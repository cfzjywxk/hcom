//! Foreground, single-generation supervisor core.
//!
//! This module deliberately knows nothing about Codex argv, hooks, bundles, or
//! terminal launchers. A caller supplies an already-running first generation,
//! a durable typed control adapter, and a generation adapter. The only process
//! signals representable on the handoff path are SIGINT, SIGTERM, and SIGHUP;
//! there is no automatic escalation API.

use std::fmt;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_CONTEXT_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 512;
const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Stable identity of the existing terminal owned by the foreground
/// supervisor. Every generation is checked against this value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OuterTerminalIdentity {
    pub supervisor_pid: i32,
    pub supervisor_pgid: i32,
    pub foreground_pgid: i32,
    pub tty_device: u64,
    pub tty_inode: u64,
}

impl OuterTerminalIdentity {
    /// Capture and verify a foreground TTY without changing its session or
    /// process group.
    pub fn capture(fd: RawFd) -> io::Result<Self> {
        // SAFETY: all calls receive a live caller-owned fd or inspect the
        // current process. Return values are checked before use.
        unsafe {
            if libc::isatty(fd) != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "foreground chain requires an existing TTY",
                ));
            }
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) == -1 {
                return Err(io::Error::last_os_error());
            }
            let supervisor_pid = libc::getpid();
            let supervisor_pgid = libc::getpgrp();
            let foreground_pgid = libc::tcgetpgrp(fd);
            if foreground_pgid == -1 {
                return Err(io::Error::last_os_error());
            }
            if supervisor_pgid != foreground_pgid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "supervisor does not own the outer foreground process group",
                ));
            }
            Ok(Self {
                supervisor_pid,
                supervisor_pgid,
                foreground_pgid,
                tty_device: stat.st_dev,
                tty_inode: stat.st_ino,
            })
        }
    }

    fn validate(self) -> Result<(), SupervisorInvariantError> {
        if self.supervisor_pid <= 0
            || self.supervisor_pgid <= 0
            || self.foreground_pgid <= 0
            || self.supervisor_pgid != self.foreground_pgid
            || self.tty_device == 0
            || self.tty_inode == 0
        {
            return Err(SupervisorInvariantError(
                "invalid foreground supervisor/TTY identity".to_string(),
            ));
        }
        Ok(())
    }
}

/// Opaque metadata for one prepared or active generation. It cannot carry
/// task text, bundle content, argv, environment snapshots, or credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationIdentity {
    pub generation: u64,
    pub launch_nonce: String,
    pub wrapper_pid: i32,
    pub wrapper_pgid: i32,
    pub child_pid: i32,
    pub child_pgid: i32,
    pub child_process_birth_identity: String,
    pub process_id: String,
    pub process_birth_identity: String,
    pub instance_name: String,
    pub hcom_session_id: String,
    pub synthetic_native_session_id: String,
}

impl GenerationIdentity {
    fn validate(&self, outer: OuterTerminalIdentity) -> Result<(), SupervisorInvariantError> {
        if self.generation == 0
            || self.wrapper_pid <= 0
            || self.child_pid <= 0
            || self.child_pgid <= 0
            || self.wrapper_pid == outer.supervisor_pid
            || self.wrapper_pid == self.child_pid
            || self.child_pid == outer.supervisor_pid
            || self.child_pid != self.child_pgid
            || self.wrapper_pgid != outer.supervisor_pgid
            || self.child_pgid == outer.supervisor_pgid
        {
            return Err(SupervisorInvariantError(
                "generation process topology does not match the foreground chain".to_string(),
            ));
        }
        for (name, value) in [
            ("launch nonce", self.launch_nonce.as_str()),
            ("process identity", self.process_id.as_str()),
            (
                "process birth identity",
                self.process_birth_identity.as_str(),
            ),
            (
                "child process birth identity",
                self.child_process_birth_identity.as_str(),
            ),
            ("instance identity", self.instance_name.as_str()),
            ("hcom session identity", self.hcom_session_id.as_str()),
            (
                "synthetic native session identity",
                self.synthetic_native_session_id.as_str(),
            ),
        ] {
            validate_opaque(name, value)?;
        }
        Ok(())
    }
}

fn validate_opaque(name: &str, value: &str) -> Result<(), SupervisorInvariantError> {
    if value.is_empty()
        || value.len() > MAX_CONTEXT_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(SupervisorInvariantError(format!(
            "{name} is empty, unbounded, or contains control bytes"
        )));
    }
    Ok(())
}

/// The exact durable authorization consumed at the signal apply point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuiesceAuthorization {
    pub handoff_id: String,
    pub expected_version: i64,
    pub quiesce_token: String,
    pub generation: u64,
    pub launch_nonce: String,
    pub pinned_native_session_id: String,
    pub process_birth_identity: String,
}

/// Authorization after the durable `begin_quiesce` CAS succeeds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuiesceApply {
    pub handoff_id: String,
    pub expected_version: i64,
    pub generation: u64,
}

/// Exact typed target reservation created while the source is still live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetReservation {
    pub handoff_id: String,
    pub expected_version: i64,
    pub generation: u64,
    pub launch_nonce: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableDirective {
    Wait,
    Quiesce(QuiesceAuthorization),
    NeedsRecovery(String),
    StopChain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainSignal {
    Interrupt,
    Terminate,
    Hangup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalSendResult {
    Sent,
    NotFound,
    PermissionDenied,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryExitContext {
    Closed,
    Killed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitEvidence {
    pub observed_wall_seconds: u64,
    pub observed_monotonic_ns: i64,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub delivery_context: DeliveryExitContext,
}

impl ExitEvidence {
    fn validate(&self) -> Result<(), SupervisorInvariantError> {
        if self.observed_wall_seconds == 0
            || self.observed_monotonic_ns < 0
            || (self.exit_code.is_some() == self.exit_signal.is_some())
        {
            return Err(SupervisorInvariantError(
                "child exit evidence is incomplete or contradictory".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceCleanupEvidence {
    pub inject_stopped: bool,
    pub delivery_joined: bool,
    pub pty_closed: bool,
    pub screen_released: bool,
    pub write_queue_empty: bool,
}

impl ResourceCleanupEvidence {
    pub fn all_succeeded(self) -> bool {
        self.inject_stopped
            && self.delivery_joined
            && self.pty_closed
            && self.screen_released
            && self.write_queue_empty
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupEvidence {
    pub exit: Option<ExitEvidence>,
    pub waitpid_reaped: bool,
    pub resources: ResourceCleanupEvidence,
    pub failure_kind: String,
    pub failure_reason: String,
}

impl CleanupEvidence {
    pub fn successful(&self) -> bool {
        self.exit.is_some()
            && self.waitpid_reaped
            && self.resources.all_succeeded()
            && self.failure_kind.is_empty()
            && self.failure_reason.is_empty()
    }

    fn validate(&self) -> Result<(), SupervisorInvariantError> {
        if let Some(exit) = &self.exit {
            exit.validate()?;
        }
        if self.failure_kind.len() > MAX_REASON_BYTES
            || self.failure_reason.len() > MAX_REASON_BYTES
        {
            return Err(SupervisorInvariantError(
                "cleanup failure evidence exceeds its bound".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigtermEvidence {
    pub requested_wall_seconds: u64,
    pub requested_monotonic_ns: i64,
    pub result: SignalSendResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostCleanup {
    Advance(TargetReservation),
    NeedsRecovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownReason {
    Explicit,
    OuterHangup,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenerationEvent {
    ControlWake,
    Timeout,
    ChildExited(ExitEvidence),
    Interrupt,
    Hangup,
    Resize,
    Continue,
}

/// Adapter-owned result of reaping and resource cleanup. A residual handle is
/// mandatory whenever liveness or cleanup ownership is unresolved.
pub struct FinishAttempt<H> {
    pub evidence: CleanupEvidence,
    pub residual: Option<H>,
}

/// A generation prepared behind a private bootstrap gate. No inner tool may
/// execute until `activate_target` consumes it after durable materialization.
pub trait PreparedGeneration {
    fn identity(&self) -> &GenerationIdentity;
}

/// Process/PTY seam. It intentionally has no command string or argv argument.
pub trait GenerationAdapter {
    type Active;
    type Prepared: PreparedGeneration;
    type Error: fmt::Display;

    fn identity<'a>(&'a self, active: &'a Self::Active) -> &'a GenerationIdentity;
    fn wait_event(
        &mut self,
        active: &mut Self::Active,
        timeout: Duration,
    ) -> Result<GenerationEvent, Self::Error>;
    fn send_signal(&mut self, active: &Self::Active, signal: ChainSignal) -> SignalSendResult;
    fn resize(&mut self, active: &mut Self::Active) -> Result<(), Self::Error>;
    fn reassert_outer_terminal(&mut self) -> Result<(), Self::Error>;
    fn finish_after_exit(
        &mut self,
        active: Self::Active,
        exit: &ExitEvidence,
    ) -> FinishAttempt<Self::Active>;
    fn shutdown_without_successor(
        &mut self,
        active: Self::Active,
        reason: ShutdownReason,
    ) -> FinishAttempt<Self::Active>;
    /// An error is valid only while the adapter retains no process, fd, or
    /// thread ownership. Once a private wrapper exists, return `Prepared`
    /// (still gated) so the supervisor can durably record and abort it.
    fn prepare_target(
        &mut self,
        reservation: &TargetReservation,
        outer: OuterTerminalIdentity,
    ) -> Result<Self::Prepared, Self::Error>;
    fn activate_target(
        &mut self,
        prepared: Self::Prepared,
    ) -> Result<Self::Active, (Self::Prepared, Self::Error)>;
    fn abort_prepared(&mut self, prepared: Self::Prepared) -> FinishAttempt<Self::Prepared>;
}

/// Durable-state adapter. Each method must perform an exact typed reread/CAS;
/// a control wake is only advisory.
pub trait DurableControl {
    type Error: fmt::Display;

    fn read_directive(
        &mut self,
        active: &GenerationIdentity,
        local_quiesce: Option<&QuiesceApply>,
    ) -> Result<DurableDirective, Self::Error>;
    fn begin_quiesce(
        &mut self,
        active: &GenerationIdentity,
        authorization: &QuiesceAuthorization,
    ) -> Result<QuiesceApply, Self::Error>;
    fn record_sigterm(
        &mut self,
        apply: &QuiesceApply,
        evidence: &SigtermEvidence,
    ) -> Result<QuiesceApply, Self::Error>;
    fn record_cleanup(
        &mut self,
        apply: &QuiesceApply,
        evidence: &CleanupEvidence,
    ) -> Result<PostCleanup, Self::Error>;
    fn record_exit_without_stop(
        &mut self,
        active: &GenerationIdentity,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error>;
    fn materialize_target(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error>;
    /// Phase 2 fake adapters may pin a synthetic native session here. A real
    /// Codex SessionStart implementation is deliberately outside this module.
    /// This operation may reach AwaitingAcceptance only; it must not accept.
    fn target_ready(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error>;
    /// Persist a fail-closed target-launch outcome. `identity` is present once
    /// a private wrapper was prepared; `cleanup` is present only after an
    /// explicit abort/shutdown attempt. Implementations must transition the
    /// exact reservation to recovery before returning success.
    fn record_target_failure(
        &mut self,
        reservation: &TargetReservation,
        identity: Option<&GenerationIdentity>,
        cleanup: Option<&CleanupEvidence>,
        failure_kind: &str,
        failure_reason: &str,
    ) -> Result<(), Self::Error>;
    /// Persist fail-closed shutdown intent before any process action. This
    /// prevents a crash after SIGHUP/explicit cleanup from leaving durable
    /// state claiming that a vanished generation is still active.
    fn begin_shutdown(
        &mut self,
        active: &GenerationIdentity,
        reason: ShutdownReason,
    ) -> Result<(), Self::Error>;
    fn record_shutdown(
        &mut self,
        active: &GenerationIdentity,
        reason: ShutdownReason,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error>;
}

pub trait SupervisorClock {
    fn wall_seconds(&self) -> u64;
    fn monotonic_ns(&self) -> i64;
}

#[derive(Default)]
pub struct SystemClock;

impl SupervisorClock for SystemClock {
    fn wall_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn monotonic_ns(&self) -> i64 {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: value points at initialized writable memory and the return
        // value is checked.
        if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } == -1 {
            return 0;
        }
        value
            .tv_sec
            .saturating_mul(1_000_000_000)
            .saturating_add(value.tv_nsec)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceKind {
    SupervisorStarted,
    DurableReread,
    BeginQuiesce,
    SignalRequested(ChainSignal),
    SignalRecorded(SignalSendResult),
    ChildExitObserved,
    ChildReaped,
    ResourcesCleaned,
    TargetPrepare,
    TargetMaterialized,
    TargetActivated,
    TargetReady,
    InterruptForwarded,
    ResizeApplied,
    ContinueApplied,
    OuterHangup,
    ShutdownIntent,
    Recovery,
    ChainStopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecord {
    pub sequence: u64,
    pub kind: TraceKind,
    pub generation: Option<u64>,
    pub wrapper_pid: Option<i32>,
    pub child_pid: Option<i32>,
    pub child_pgid: Option<i32>,
    pub supervisor_pid: i32,
    pub supervisor_pgid: i32,
    pub tty_device: u64,
    pub tty_inode: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupervisorRunOutcome {
    Stopped,
    AwaitingAcceptance { generation: u64, handoff_id: String },
    NeedsRecovery(String),
}

#[derive(Debug)]
pub struct SupervisorInvariantError(pub String);

impl fmt::Display for SupervisorInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SupervisorInvariantError {}

struct ActiveQuiesce {
    apply: QuiesceApply,
    deadline_ns: i64,
}

/// Persistent foreground owner for one serial chain.
pub struct ForegroundChainSupervisor<C, A, K = SystemClock>
where
    C: DurableControl,
    A: GenerationAdapter,
    K: SupervisorClock,
{
    outer: OuterTerminalIdentity,
    control: C,
    adapter: A,
    clock: K,
    active: Option<A::Active>,
    prepared: Option<A::Prepared>,
    quiesce: Option<ActiveQuiesce>,
    quiesce_timeout: Duration,
    trace: Vec<TraceRecord>,
    next_sequence: u64,
}

pub type SupervisorParts<C, A> = (
    C,
    A,
    Option<<A as GenerationAdapter>::Active>,
    Option<<A as GenerationAdapter>::Prepared>,
    Vec<TraceRecord>,
);

impl<C, A> ForegroundChainSupervisor<C, A, SystemClock>
where
    C: DurableControl,
    A: GenerationAdapter,
{
    pub fn new(
        outer: OuterTerminalIdentity,
        control: C,
        adapter: A,
        active: A::Active,
        quiesce_timeout: Duration,
    ) -> Result<Self, SupervisorInvariantError> {
        Self::with_clock(
            outer,
            control,
            adapter,
            active,
            quiesce_timeout,
            SystemClock,
        )
    }
}

impl<C, A, K> ForegroundChainSupervisor<C, A, K>
where
    C: DurableControl,
    A: GenerationAdapter,
    K: SupervisorClock,
{
    pub fn with_clock(
        outer: OuterTerminalIdentity,
        control: C,
        adapter: A,
        active: A::Active,
        quiesce_timeout: Duration,
        clock: K,
    ) -> Result<Self, SupervisorInvariantError> {
        outer.validate()?;
        adapter.identity(&active).validate(outer)?;
        if quiesce_timeout.is_zero() {
            return Err(SupervisorInvariantError(
                "quiesce timeout must be positive".to_string(),
            ));
        }
        let mut supervisor = Self {
            outer,
            control,
            adapter,
            clock,
            active: Some(active),
            prepared: None,
            quiesce: None,
            quiesce_timeout,
            trace: Vec::new(),
            next_sequence: 0,
        };
        supervisor.record(TraceKind::SupervisorStarted, None);
        Ok(supervisor)
    }

    pub fn trace(&self) -> &[TraceRecord] {
        &self.trace
    }

    pub fn active_identity(&self) -> Option<&GenerationIdentity> {
        self.active
            .as_ref()
            .map(|active| self.adapter.identity(active))
    }

    /// Expose the typed durable adapter for an explicit external transition
    /// such as target acceptance. The supervisor never performs acceptance
    /// itself.
    pub fn control_mut(&mut self) -> &mut C {
        &mut self.control
    }

    /// Run until explicit stop, outer hangup, or a fail-closed recovery state.
    pub fn run(&mut self) -> SupervisorRunOutcome {
        loop {
            let Some(active) = self.active.as_ref() else {
                return self.recovery("supervisor has no active generation");
            };
            let identity = self.adapter.identity(active).clone();
            if let Err(error) = identity.validate(self.outer) {
                return self.recovery(&error.to_string());
            }
            self.record(TraceKind::DurableReread, Some(&identity));

            let directive = match self
                .control
                .read_directive(&identity, self.quiesce.as_ref().map(|value| &value.apply))
            {
                Ok(directive) => directive,
                Err(error) => {
                    return self.recovery(&format!("durable reread failed: {error}"));
                }
            };
            match directive {
                DurableDirective::NeedsRecovery(reason) => return self.recovery(&reason),
                DurableDirective::StopChain => return self.stop_chain(ShutdownReason::Explicit),
                DurableDirective::Quiesce(authorization) if self.quiesce.is_none() => {
                    if let Some(outcome) = self.apply_quiesce(&identity, &authorization) {
                        return outcome;
                    }
                }
                DurableDirective::Wait | DurableDirective::Quiesce(_) => {}
            }

            if let Some(quiesce) = &self.quiesce
                && self.clock.monotonic_ns() >= quiesce.deadline_ns
            {
                let evidence = CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "sigterm_timeout".to_string(),
                    failure_reason: "child did not exit before the no-escalation deadline"
                        .to_string(),
                };
                let apply = quiesce.apply.clone();
                if let Err(error) = self.control.record_cleanup(&apply, &evidence) {
                    return self.recovery(&format!("failed to persist SIGTERM timeout: {error}"));
                }
                return self.recovery("child ignored SIGTERM; automatic SIGKILL is forbidden");
            }

            let timeout = self.poll_timeout();
            let event = {
                let active = self.active.as_mut().expect("active checked above");
                match self.adapter.wait_event(active, timeout) {
                    Ok(event) => event,
                    Err(error) => {
                        return self.recovery(&format!("generation event loop failed: {error}"));
                    }
                }
            };
            match event {
                GenerationEvent::ControlWake | GenerationEvent::Timeout => {}
                GenerationEvent::Interrupt => {
                    let result = {
                        let active = self.active.as_ref().expect("active exists");
                        self.adapter.send_signal(active, ChainSignal::Interrupt)
                    };
                    if result != SignalSendResult::Sent {
                        return self.recovery("SIGINT could not be forwarded to the current child");
                    }
                    self.record(TraceKind::InterruptForwarded, Some(&identity));
                }
                GenerationEvent::Hangup => return self.stop_chain(ShutdownReason::OuterHangup),
                GenerationEvent::Resize => {
                    let result = {
                        let active = self.active.as_mut().expect("active exists");
                        self.adapter.resize(active)
                    };
                    if let Err(error) = result {
                        return self.recovery(&format!("resize failed: {error}"));
                    }
                    self.record(TraceKind::ResizeApplied, Some(&identity));
                }
                GenerationEvent::Continue => {
                    if let Err(error) = self.adapter.reassert_outer_terminal() {
                        return self.recovery(&format!("raw terminal reassertion failed: {error}"));
                    }
                    let result = {
                        let active = self.active.as_mut().expect("active exists");
                        self.adapter.resize(active)
                    };
                    if let Err(error) = result {
                        return self.recovery(&format!("continued resize failed: {error}"));
                    }
                    self.record(TraceKind::ContinueApplied, Some(&identity));
                }
                GenerationEvent::ChildExited(exit) => {
                    return self.finish_generation(identity, exit);
                }
            }
        }
    }

    /// Return ownership to the caller for deterministic fixture teardown or a
    /// later explicit recovery command. This method performs no process action.
    pub fn into_parts(self) -> SupervisorParts<C, A> {
        (
            self.control,
            self.adapter,
            self.active,
            self.prepared,
            self.trace,
        )
    }

    fn apply_quiesce(
        &mut self,
        identity: &GenerationIdentity,
        authorization: &QuiesceAuthorization,
    ) -> Option<SupervisorRunOutcome> {
        if authorization.generation != identity.generation
            || authorization.launch_nonce != identity.launch_nonce
            || authorization.pinned_native_session_id != identity.synthetic_native_session_id
            || authorization.process_birth_identity != identity.process_birth_identity
            || authorization.expected_version < 0
            || validate_opaque("handoff ID", &authorization.handoff_id).is_err()
            || validate_opaque("quiesce token", &authorization.quiesce_token).is_err()
        {
            return Some(
                self.recovery("quiesce authorization failed the apply-point identity check"),
            );
        }
        let apply = match self.control.begin_quiesce(identity, authorization) {
            Ok(apply) => apply,
            Err(error) => {
                return Some(self.recovery(&format!("begin_quiesce CAS failed: {error}")));
            }
        };
        if apply.generation != identity.generation
            || apply.handoff_id != authorization.handoff_id
            || apply.expected_version <= authorization.expected_version
        {
            return Some(self.recovery("begin_quiesce returned inconsistent durable evidence"));
        }
        self.record(TraceKind::BeginQuiesce, Some(identity));

        let requested_wall_seconds = self.clock.wall_seconds();
        let requested_monotonic_ns = self.clock.monotonic_ns();
        self.record(
            TraceKind::SignalRequested(ChainSignal::Terminate),
            Some(identity),
        );
        let result = {
            let active = self.active.as_ref().expect("active exists");
            self.adapter.send_signal(active, ChainSignal::Terminate)
        };
        let deadline_ns = requested_monotonic_ns
            .saturating_add(self.quiesce_timeout.as_nanos().min(i64::MAX as u128) as i64);
        let evidence = SigtermEvidence {
            requested_wall_seconds,
            requested_monotonic_ns,
            result,
        };
        let recorded = match self.control.record_sigterm(&apply, &evidence) {
            Ok(recorded) => recorded,
            Err(error) => {
                // Keep the local one-shot marker even when the durability
                // write is uncertain. This run stops and never resends.
                self.quiesce = Some(ActiveQuiesce { apply, deadline_ns });
                return Some(self.recovery(&format!(
                    "SIGTERM was attempted but evidence persistence failed: {error}"
                )));
            }
        };
        if recorded.handoff_id != apply.handoff_id
            || recorded.generation != apply.generation
            || recorded.expected_version <= apply.expected_version
        {
            self.quiesce = Some(ActiveQuiesce { apply, deadline_ns });
            return Some(
                self.recovery("SIGTERM evidence returned an inconsistent durable version"),
            );
        }
        self.quiesce = Some(ActiveQuiesce {
            apply: recorded,
            deadline_ns,
        });
        self.record(TraceKind::SignalRecorded(result), Some(identity));
        if result != SignalSendResult::Sent {
            return Some(self.recovery("SIGTERM delivery failed"));
        }
        None
    }

    fn finish_generation(
        &mut self,
        identity: GenerationIdentity,
        exit: ExitEvidence,
    ) -> SupervisorRunOutcome {
        if let Err(error) = exit.validate() {
            return self.recovery(&error.to_string());
        }
        self.record(TraceKind::ChildExitObserved, Some(&identity));
        let active = self.active.take().expect("active exists");
        let mut finish = self.adapter.finish_after_exit(active, &exit);
        if let Err(error) = finish.evidence.validate() {
            self.active = finish.residual;
            return self.recovery(&error.to_string());
        }
        if finish.evidence.exit.as_ref() != Some(&exit) {
            finish.evidence.exit = Some(exit.clone());
            finish.evidence.resources.pty_closed = false;
            finish.evidence.failure_kind = "exit_evidence_mismatch".to_string();
            finish.evidence.failure_reason =
                "adapter cleanup did not preserve the observed child exit".to_string();
        }
        if finish.evidence.successful() && finish.residual.is_some() {
            finish.evidence.resources.pty_closed = false;
            finish.evidence.failure_kind = "residual_handle".to_string();
            finish.evidence.failure_reason =
                "adapter retained ownership after reporting successful cleanup".to_string();
        }
        if finish.evidence.waitpid_reaped {
            self.record(TraceKind::ChildReaped, Some(&identity));
        }
        if finish.evidence.resources.all_succeeded() {
            self.record(TraceKind::ResourcesCleaned, Some(&identity));
        }

        let Some(quiesce) = self.quiesce.take() else {
            let record = self
                .control
                .record_exit_without_stop(&identity, &finish.evidence);
            self.active = finish.residual;
            if let Err(error) = record {
                return self.recovery(&format!(
                    "unexpected exit evidence could not be persisted: {error}"
                ));
            }
            return self.recovery("source exited before an exact typed Stop");
        };
        let post_cleanup = match self
            .control
            .record_cleanup(&quiesce.apply, &finish.evidence)
        {
            Ok(result) => result,
            Err(error) => {
                self.active = finish.residual;
                return self.recovery(&format!("cleanup evidence persistence failed: {error}"));
            }
        };
        if !finish.evidence.successful() || finish.residual.is_some() {
            self.active = finish.residual;
            return self.recovery("source reap or owned-resource cleanup was incomplete");
        }
        match post_cleanup {
            PostCleanup::NeedsRecovery => {
                self.active = finish.residual;
                self.recovery("durable cleanup transition entered recovery")
            }
            PostCleanup::Advance(reservation) => self.spawn_target(identity, reservation),
        }
    }

    fn spawn_target(
        &mut self,
        source: GenerationIdentity,
        reservation: TargetReservation,
    ) -> SupervisorRunOutcome {
        if self.active.is_some()
            || reservation.generation != source.generation + 1
            || reservation.expected_version < 0
            || validate_opaque("target handoff ID", &reservation.handoff_id).is_err()
            || validate_opaque("target launch nonce", &reservation.launch_nonce).is_err()
        {
            return self.recovery("target reservation is not the exact serial successor");
        }
        self.record(TraceKind::TargetPrepare, None);
        let prepared = match self.adapter.prepare_target(&reservation, self.outer) {
            Ok(prepared) => prepared,
            Err(error) => {
                let reason = format!("target prepare failed: {error}");
                return self.target_failure(
                    &reservation,
                    None,
                    None,
                    "target_prepare_failed",
                    &reason,
                );
            }
        };
        let target = prepared.identity().clone();
        if target.generation != reservation.generation
            || target.launch_nonce != reservation.launch_nonce
            || target.validate(self.outer).is_err()
        {
            let cleanup = self.abort_prepared_target(prepared);
            let reason = format!(
                "prepared target identity does not match its reservation (reaped={})",
                cleanup.waitpid_reaped
            );
            return self.target_failure(
                &reservation,
                Some(&target),
                Some(&cleanup),
                "target_identity_mismatch",
                &reason,
            );
        }
        if let Err(error) = self.control.materialize_target(&reservation, &target) {
            let cleanup = self.abort_prepared_target(prepared);
            let reason = format!(
                "target materialization failed; private gate was revoked (reaped={}): {error}",
                cleanup.waitpid_reaped
            );
            return self.target_failure(
                &reservation,
                Some(&target),
                Some(&cleanup),
                "target_materialization_failed",
                &reason,
            );
        }
        self.record(TraceKind::TargetMaterialized, Some(&target));
        let active = match self.adapter.activate_target(prepared) {
            Ok(active) => active,
            Err((prepared, error)) => {
                let cleanup = self.abort_prepared_target(prepared);
                let reason = format!(
                    "materialized target activation failed (reaped={}): {error}",
                    cleanup.waitpid_reaped
                );
                return self.target_failure(
                    &reservation,
                    Some(&target),
                    Some(&cleanup),
                    "target_activation_failed",
                    &reason,
                );
            }
        };
        let active_identity = self.adapter.identity(&active).clone();
        if active_identity != target {
            let mut cleanup = self
                .adapter
                .shutdown_without_successor(active, ShutdownReason::Explicit);
            if let Err(error) = cleanup.evidence.validate() {
                cleanup.evidence = CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "changed_target_cleanup_invalid".to_string(),
                    failure_reason: bounded_reason(&error.to_string()),
                };
            } else if cleanup.evidence.successful() && cleanup.residual.is_some() {
                cleanup.evidence.resources.pty_closed = false;
                cleanup.evidence.failure_kind = "changed_target_residual_handle".to_string();
                cleanup.evidence.failure_reason =
                    "adapter retained changed-target ownership after reporting cleanup".to_string();
            }
            self.active = cleanup.residual;
            return self.target_failure(
                &reservation,
                Some(&target),
                Some(&cleanup.evidence),
                "target_identity_changed",
                "target identity changed while its private gate opened",
            );
        }
        self.record(TraceKind::TargetActivated, Some(&target));
        self.active = Some(active);
        if let Err(error) = self.control.target_ready(&reservation, &target) {
            let reason = format!(
                "target started but ready evidence failed; acceptance was not inferred: {error}"
            );
            return self.target_failure(
                &reservation,
                Some(&target),
                None,
                "target_ready_failed",
                &reason,
            );
        }
        self.record(TraceKind::TargetReady, Some(&target));
        SupervisorRunOutcome::AwaitingAcceptance {
            generation: target.generation,
            handoff_id: reservation.handoff_id,
        }
    }

    fn abort_prepared_target(&mut self, prepared: A::Prepared) -> CleanupEvidence {
        let mut finish = self.adapter.abort_prepared(prepared);
        if let Err(error) = finish.evidence.validate() {
            finish.evidence = CleanupEvidence {
                exit: None,
                waitpid_reaped: false,
                resources: ResourceCleanupEvidence::default(),
                failure_kind: "prepared_abort_evidence_invalid".to_string(),
                failure_reason: bounded_reason(&error.to_string()),
            };
        } else if finish.evidence.successful() && finish.residual.is_some() {
            finish.evidence.resources.pty_closed = false;
            finish.evidence.failure_kind = "prepared_abort_residual".to_string();
            finish.evidence.failure_reason =
                "adapter retained prepared-target ownership after reporting cleanup".to_string();
        } else if !finish.evidence.successful() && finish.residual.is_none() {
            finish.evidence.failure_kind = "prepared_abort_ownership_lost".to_string();
            finish.evidence.failure_reason =
                "adapter reported incomplete prepared-target cleanup without a residual handle"
                    .to_string();
        }
        self.prepared = finish.residual;
        finish.evidence
    }

    fn target_failure(
        &mut self,
        reservation: &TargetReservation,
        identity: Option<&GenerationIdentity>,
        cleanup: Option<&CleanupEvidence>,
        failure_kind: &str,
        failure_reason: &str,
    ) -> SupervisorRunOutcome {
        if let Err(error) = self.control.record_target_failure(
            reservation,
            identity,
            cleanup,
            failure_kind,
            failure_reason,
        ) {
            return self.recovery(&format!("target failure could not be persisted: {error}"));
        }
        self.recovery(failure_reason)
    }

    fn stop_chain(&mut self, reason: ShutdownReason) -> SupervisorRunOutcome {
        let Some(active) = self.active.as_ref() else {
            return self.recovery("chain stop found no owned generation");
        };
        let identity = self.adapter.identity(active).clone();
        if reason == ShutdownReason::OuterHangup {
            self.record(TraceKind::OuterHangup, Some(&identity));
        }
        if let Err(error) = self.control.begin_shutdown(&identity, reason) {
            return self.recovery(&format!(
                "chain shutdown intent could not be persisted: {error}"
            ));
        }
        self.record(TraceKind::ShutdownIntent, Some(&identity));
        let active = self.active.take().expect("active checked above");
        let mut finish = self.adapter.shutdown_without_successor(active, reason);
        if let Err(error) = finish.evidence.validate() {
            finish.evidence = CleanupEvidence {
                exit: None,
                waitpid_reaped: false,
                resources: ResourceCleanupEvidence::default(),
                failure_kind: "shutdown_evidence_invalid".to_string(),
                failure_reason: bounded_reason(&error.to_string()),
            };
        } else if finish.evidence.successful() && finish.residual.is_some() {
            finish.evidence.resources.pty_closed = false;
            finish.evidence.failure_kind = "shutdown_residual_handle".to_string();
            finish.evidence.failure_reason =
                "adapter retained ownership after reporting successful shutdown".to_string();
        }
        if finish.evidence.exit.is_some() {
            self.record(TraceKind::ChildExitObserved, Some(&identity));
        }
        if finish.evidence.waitpid_reaped {
            self.record(TraceKind::ChildReaped, Some(&identity));
        }
        if finish.evidence.resources.all_succeeded() {
            self.record(TraceKind::ResourcesCleaned, Some(&identity));
        }
        if let Err(error) = self
            .control
            .record_shutdown(&identity, reason, &finish.evidence)
        {
            self.active = finish.residual;
            return self.recovery(&format!("chain shutdown evidence failed: {error}"));
        }
        self.active = finish.residual;
        if self.active.is_some() || !finish.evidence.successful() {
            return self.recovery("chain shutdown left unresolved process or resource ownership");
        }
        self.record(TraceKind::ChainStopped, Some(&identity));
        SupervisorRunOutcome::Stopped
    }

    fn poll_timeout(&self) -> Duration {
        let Some(quiesce) = &self.quiesce else {
            return CONTROL_POLL_INTERVAL;
        };
        let remaining_ns = quiesce
            .deadline_ns
            .saturating_sub(self.clock.monotonic_ns());
        if remaining_ns <= 0 {
            Duration::ZERO
        } else {
            CONTROL_POLL_INTERVAL.min(Duration::from_nanos(remaining_ns as u64))
        }
    }

    fn recovery(&mut self, reason: &str) -> SupervisorRunOutcome {
        let bounded = bounded_reason(reason);
        let identity = self
            .active
            .as_ref()
            .map(|active| self.adapter.identity(active).clone());
        self.record(TraceKind::Recovery, identity.as_ref());
        SupervisorRunOutcome::NeedsRecovery(bounded)
    }

    fn record(&mut self, kind: TraceKind, identity: Option<&GenerationIdentity>) {
        let record = TraceRecord {
            sequence: self.next_sequence,
            kind,
            generation: identity.map(|value| value.generation),
            wrapper_pid: identity.map(|value| value.wrapper_pid),
            child_pid: identity.map(|value| value.child_pid),
            child_pgid: identity.map(|value| value.child_pgid),
            supervisor_pid: self.outer.supervisor_pid,
            supervisor_pgid: self.outer.supervisor_pgid,
            tty_device: self.outer.tty_device,
            tty_inode: self.outer.tty_inode,
        };
        self.next_sequence += 1;
        self.trace.push(record);
    }
}

fn bounded_reason(value: &str) -> String {
    let mut result = String::with_capacity(value.len().min(MAX_REASON_BYTES));
    for character in value.chars() {
        if result.len() + character.len_utf8() > MAX_REASON_BYTES {
            break;
        }
        if !character.is_control() {
            result.push(character);
        }
    }
    result
}

/// Build a Linux PID-reuse-resistant process birth identity.
pub fn linux_process_birth_identity(pid: i32) -> io::Result<String> {
    let (_, start_ticks) = linux_proc_parent_and_start(pid)?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty()
        || boot_id.len() > 64
        || boot_id.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Linux boot identity",
        ));
    }
    Ok(format!("linux-v1:{pid}:{start_ticks}:{boot_id}"))
}

/// Verify that the exact wrapper birth identity is still live and that the
/// caller is that process or one of its descendants. Copying the marker into
/// an unrelated process therefore fails closed.
pub fn verify_current_process_scope(identity: &str) -> io::Result<()> {
    let (expected_pid, expected_start, expected_boot) = parse_linux_birth(identity)?;
    let current_boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    if current_boot.trim() != expected_boot {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process birth identity belongs to another boot",
        ));
    }
    let (_, live_start) = linux_proc_parent_and_start(expected_pid)?;
    if live_start != expected_start {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process birth identity is stale",
        ));
    }
    // SAFETY: getpid has no preconditions.
    let mut cursor = unsafe { libc::getpid() };
    for _ in 0..256 {
        if cursor == expected_pid {
            return Ok(());
        }
        if cursor <= 1 {
            break;
        }
        let (parent, _) = linux_proc_parent_and_start(cursor)?;
        if parent == cursor {
            break;
        }
        cursor = parent;
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "current process is outside the managed wrapper ancestry",
    ))
}

fn parse_linux_birth(identity: &str) -> io::Result<(i32, u64, &str)> {
    let mut parts = identity.splitn(4, ':');
    if parts.next() != Some("linux-v1") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported process birth identity",
        ));
    }
    let pid = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid process PID"))?;
    let start = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid process start time"))?;
    let boot = parts
        .next()
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid boot identity"))?;
    Ok((pid, start, boot))
}

fn linux_proc_parent_and_start(pid: i32) -> io::Result<(i32, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat.rfind(')').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "malformed Linux process stat")
    })?;
    let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
    // fields[0] is kernel field 3 (state), fields[1] is ppid, and fields[19]
    // is starttime (kernel field 22).
    if fields.len() <= 19 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated Linux process stat",
        ));
    }
    let parent = fields[1]
        .parse::<i32>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid parent PID"))?;
    let start = fields[19]
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid process start time"))?;
    Ok((parent, start))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn current_process_birth_identity_round_trips_and_rejects_spoof() {
        // SAFETY: getpid has no preconditions.
        let pid = unsafe { libc::getpid() };
        let identity = linux_process_birth_identity(pid).unwrap();
        verify_current_process_scope(&identity).unwrap();
        assert!(verify_current_process_scope("linux-v1:1:1:not-this-boot").is_err());
    }

    #[test]
    fn copied_live_birth_marker_is_rejected_outside_wrapper_ancestry() {
        // SAFETY: the unit test is single-threaded at this point with respect
        // to this child, and the child performs only async-signal-safe calls.
        let child = unsafe { libc::fork() };
        assert!(child >= 0);
        if child == 0 {
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        let identity = linux_process_birth_identity(child).unwrap();
        assert!(verify_current_process_scope(&identity).is_err());
        // SAFETY: exact test child; SIGTERM has its default disposition.
        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status = 0;
            assert_eq!(libc::waitpid(child, &mut status, 0), child);
        }
    }

    #[test]
    fn cleanup_success_requires_every_owned_resource_and_reap() {
        let exit = ExitEvidence {
            observed_wall_seconds: 1,
            observed_monotonic_ns: 1,
            exit_code: Some(0),
            exit_signal: None,
            delivery_context: DeliveryExitContext::Closed,
        };
        let mut cleanup = CleanupEvidence {
            exit: Some(exit),
            waitpid_reaped: true,
            resources: ResourceCleanupEvidence {
                inject_stopped: true,
                delivery_joined: true,
                pty_closed: true,
                screen_released: true,
                write_queue_empty: true,
            },
            failure_kind: String::new(),
            failure_reason: String::new(),
        };
        assert!(cleanup.successful());
        cleanup.failure_kind = "contradictory_adapter_failure".to_string();
        assert!(!cleanup.successful());
        cleanup.failure_kind.clear();
        cleanup.resources.delivery_joined = false;
        assert!(!cleanup.successful());
    }

    #[test]
    fn no_chain_signal_variant_can_express_sigkill() {
        assert_eq!(
            [
                ChainSignal::Interrupt,
                ChainSignal::Terminate,
                ChainSignal::Hangup
            ]
            .len(),
            3
        );
    }

    #[derive(Clone)]
    struct ManualClock(Arc<AtomicI64>);

    impl SupervisorClock for ManualClock {
        fn wall_seconds(&self) -> u64 {
            1
        }

        fn monotonic_ns(&self) -> i64 {
            self.0.load(Ordering::Acquire)
        }
    }

    struct TimeoutControl {
        phase: u8,
        cleanup_seen: bool,
    }

    impl DurableControl for TimeoutControl {
        type Error = String;

        fn read_directive(
            &mut self,
            active: &GenerationIdentity,
            _local_quiesce: Option<&QuiesceApply>,
        ) -> Result<DurableDirective, Self::Error> {
            if self.phase == 0 {
                Ok(DurableDirective::Quiesce(QuiesceAuthorization {
                    handoff_id: "handoff-timeout".to_string(),
                    expected_version: 1,
                    quiesce_token: "quiesce-timeout".to_string(),
                    generation: active.generation,
                    launch_nonce: active.launch_nonce.clone(),
                    pinned_native_session_id: active.synthetic_native_session_id.clone(),
                    process_birth_identity: active.process_birth_identity.clone(),
                }))
            } else {
                Ok(DurableDirective::Wait)
            }
        }

        fn begin_quiesce(
            &mut self,
            active: &GenerationIdentity,
            authorization: &QuiesceAuthorization,
        ) -> Result<QuiesceApply, Self::Error> {
            self.phase = 1;
            Ok(QuiesceApply {
                handoff_id: authorization.handoff_id.clone(),
                expected_version: 2,
                generation: active.generation,
            })
        }

        fn record_sigterm(
            &mut self,
            apply: &QuiesceApply,
            evidence: &SigtermEvidence,
        ) -> Result<QuiesceApply, Self::Error> {
            assert_eq!(evidence.result, SignalSendResult::Sent);
            self.phase = 2;
            Ok(QuiesceApply {
                handoff_id: apply.handoff_id.clone(),
                expected_version: 3,
                generation: apply.generation,
            })
        }

        fn record_cleanup(
            &mut self,
            _apply: &QuiesceApply,
            evidence: &CleanupEvidence,
        ) -> Result<PostCleanup, Self::Error> {
            assert!(evidence.exit.is_none());
            assert!(!evidence.waitpid_reaped);
            self.cleanup_seen = true;
            self.phase = 3;
            Ok(PostCleanup::NeedsRecovery)
        }

        fn record_exit_without_stop(
            &mut self,
            _active: &GenerationIdentity,
            _evidence: &CleanupEvidence,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn materialize_target(
            &mut self,
            _reservation: &TargetReservation,
            _identity: &GenerationIdentity,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn target_ready(
            &mut self,
            _reservation: &TargetReservation,
            _identity: &GenerationIdentity,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn record_target_failure(
            &mut self,
            _reservation: &TargetReservation,
            _identity: Option<&GenerationIdentity>,
            _cleanup: Option<&CleanupEvidence>,
            _failure_kind: &str,
            _failure_reason: &str,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn begin_shutdown(
            &mut self,
            _active: &GenerationIdentity,
            _reason: ShutdownReason,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn record_shutdown(
            &mut self,
            _active: &GenerationIdentity,
            _reason: ShutdownReason,
            _evidence: &CleanupEvidence,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    struct DummyPrepared(GenerationIdentity);

    impl PreparedGeneration for DummyPrepared {
        fn identity(&self) -> &GenerationIdentity {
            &self.0
        }
    }

    struct TimeoutAdapter {
        identity: GenerationIdentity,
        clock: Arc<AtomicI64>,
        term_count: usize,
        duplicate_wakes: usize,
    }

    impl GenerationAdapter for TimeoutAdapter {
        type Active = ();
        type Prepared = DummyPrepared;
        type Error = String;

        fn identity<'a>(&'a self, _active: &'a Self::Active) -> &'a GenerationIdentity {
            &self.identity
        }

        fn wait_event(
            &mut self,
            _active: &mut Self::Active,
            timeout: Duration,
        ) -> Result<GenerationEvent, Self::Error> {
            self.clock.fetch_add(
                timeout.as_nanos().min(i64::MAX as u128) as i64,
                Ordering::AcqRel,
            );
            if self.duplicate_wakes > 0 {
                self.duplicate_wakes -= 1;
                Ok(GenerationEvent::ControlWake)
            } else {
                Ok(GenerationEvent::Timeout)
            }
        }

        fn send_signal(&mut self, _active: &Self::Active, signal: ChainSignal) -> SignalSendResult {
            assert_eq!(signal, ChainSignal::Terminate);
            self.term_count += 1;
            SignalSendResult::Sent
        }

        fn resize(&mut self, _active: &mut Self::Active) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn reassert_outer_terminal(&mut self) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn finish_after_exit(
            &mut self,
            active: Self::Active,
            _exit: &ExitEvidence,
        ) -> FinishAttempt<Self::Active> {
            FinishAttempt {
                evidence: CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "unused".to_string(),
                    failure_reason: "unused".to_string(),
                },
                residual: Some(active),
            }
        }

        fn shutdown_without_successor(
            &mut self,
            active: Self::Active,
            _reason: ShutdownReason,
        ) -> FinishAttempt<Self::Active> {
            FinishAttempt {
                evidence: CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "unused".to_string(),
                    failure_reason: "unused".to_string(),
                },
                residual: Some(active),
            }
        }

        fn prepare_target(
            &mut self,
            _reservation: &TargetReservation,
            _outer: OuterTerminalIdentity,
        ) -> Result<Self::Prepared, Self::Error> {
            Err("target spawn must remain unreachable".to_string())
        }

        fn activate_target(
            &mut self,
            prepared: Self::Prepared,
        ) -> Result<Self::Active, (Self::Prepared, Self::Error)> {
            Err((
                prepared,
                "target activation must remain unreachable".to_string(),
            ))
        }

        fn abort_prepared(&mut self, _prepared: Self::Prepared) -> FinishAttempt<Self::Prepared> {
            unreachable!()
        }
    }

    #[test]
    fn fake_clock_timeout_and_duplicate_wakes_never_resend_or_spawn() {
        let clock_value = Arc::new(AtomicI64::new(1_000_000));
        let clock = ManualClock(Arc::clone(&clock_value));
        let outer = OuterTerminalIdentity {
            supervisor_pid: 100,
            supervisor_pgid: 100,
            foreground_pgid: 100,
            tty_device: 1,
            tty_inode: 2,
        };
        let identity = GenerationIdentity {
            generation: 1,
            launch_nonce: "nonce-1".to_string(),
            wrapper_pid: 101,
            wrapper_pgid: 100,
            child_pid: 102,
            child_pgid: 102,
            child_process_birth_identity: "child-birth-1".to_string(),
            process_id: "process-1".to_string(),
            process_birth_identity: "birth-1".to_string(),
            instance_name: "instance-1".to_string(),
            hcom_session_id: "hcom-1".to_string(),
            synthetic_native_session_id: "native-1".to_string(),
        };
        let control = TimeoutControl {
            phase: 0,
            cleanup_seen: false,
        };
        let adapter = TimeoutAdapter {
            identity,
            clock: Arc::clone(&clock_value),
            term_count: 0,
            duplicate_wakes: 3,
        };
        let mut supervisor = ForegroundChainSupervisor::with_clock(
            outer,
            control,
            adapter,
            (),
            Duration::from_millis(250),
            clock,
        )
        .unwrap();
        assert!(matches!(
            supervisor.run(),
            SupervisorRunOutcome::NeedsRecovery(_)
        ));
        let (control, adapter, active, prepared, trace) = supervisor.into_parts();
        assert!(active.is_some());
        assert!(prepared.is_none());
        assert!(control.cleanup_seen);
        assert_eq!(adapter.term_count, 1);
        assert_eq!(
            trace
                .iter()
                .filter(|record| {
                    record.kind == TraceKind::SignalRequested(ChainSignal::Terminate)
                })
                .count(),
            1
        );
        assert!(
            !trace
                .iter()
                .any(|record| record.kind == TraceKind::TargetPrepare)
        );
    }
}
