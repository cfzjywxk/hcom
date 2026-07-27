//! Deterministic Linux PTY/process integration for the Phase 2 foreground
//! supervisor. This is a harness-free test executable so the worker can own a
//! real outer PTY before any test threads exist.

#![cfg(unix)]

use std::collections::VecDeque;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::time::Duration;

use hcom::chain_pty::{
    arm_parent_death_hangup, copy_winsize, observe_direct_child, reap_direct_child,
    send_process_group_signal,
};
use hcom::chain_supervisor::{
    ChainSignal, CleanupEvidence, DeliveryExitContext, DurableControl, DurableDirective,
    ExitEvidence, FinishAttempt, ForegroundChainSupervisor, GenerationAdapter, GenerationEvent,
    GenerationIdentity, OuterTerminalIdentity, PostCleanup, PreparedGeneration, QuiesceApply,
    QuiesceAuthorization, ResourceCleanupEvidence, ShutdownReason, SignalSendResult,
    SigtermEvidence, SupervisorRunOutcome, TargetReservation, TraceKind,
    linux_process_birth_identity,
};

const MSG_START: i64 = 1;
const MSG_READY: i64 = 2;
const MSG_EXIT: i64 = 3;
const MSG_REAP: i64 = 4;
const MSG_INTERRUPT: i64 = 5;
const MSG_RESIZE: i64 = 6;

static SUPERVISOR_SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static CHILD_INTERRUPT_SEEN: AtomicBool = AtomicBool::new(false);

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct WireMessage {
    kind: i64,
    a: i64,
    b: i64,
    c: i64,
    d: i64,
    e: i64,
}

extern "C" fn supervisor_signal_handler(signal: libc::c_int) {
    let fd = SUPERVISOR_SIGNAL_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = signal as u8;
        // SAFETY: async-signal-safe write to a nonblocking self-pipe.
        unsafe {
            libc::write(fd, (&byte as *const u8).cast(), 1);
        }
    }
}

extern "C" fn child_interrupt_handler(_: libc::c_int) {
    CHILD_INTERRUPT_SEEN.store(true, Ordering::Release);
}

fn main() {
    let mut outer_master = -1;
    let mut outer_slave = -1;
    let winsize = libc::winsize {
        ws_row: 37,
        ws_col: 111,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: openpty initializes both descriptors; pointers are valid.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut outer_master,
                &mut outer_slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &winsize,
            )
        },
        0
    );
    let (status_read, status_write) = pipe().unwrap();
    // SAFETY: this harness is single-threaded and forks before constructing
    // any adapter state.
    let worker = unsafe { libc::fork() };
    assert!(worker >= 0);
    if worker == 0 {
        close_fd(outer_master);
        close_fd(status_read);
        setup_outer_worker(outer_slave);
        run_all_cases(status_write);
        write_byte(status_write, u8::MAX).unwrap();
        close_fd(status_write);
        // SAFETY: worker has completed all Rust-owned cleanup.
        unsafe { libc::_exit(0) }
    }

    close_fd(outer_slave);
    close_fd(status_write);
    let mut status = 0;
    while let Ok(marker) = read_byte(status_read) {
        if marker == u8::MAX {
            status = marker;
            break;
        }
        eprintln!("fake-chain case marker: {marker}");
    }
    close_fd(status_read);
    let outer_output = drain_fd(outer_master);
    close_fd(outer_master);
    let mut wait_status = 0;
    // SAFETY: worker is a direct child and status points to writable memory.
    assert_eq!(
        unsafe { libc::waitpid(worker, &mut wait_status, 0) },
        worker
    );
    assert_eq!(status, u8::MAX, "fake-chain worker did not report success");
    assert!(libc::WIFEXITED(wait_status));
    assert_eq!(libc::WEXITSTATUS(wait_status), 0);
    let outer_text = String::from_utf8_lossy(&outer_output);
    let evidence = outer_text
        .lines()
        .find(|line| line.contains("FAKE_CHAIN_JSON "))
        .expect("worker did not emit structured fake-chain evidence")
        .trim_end_matches('\r');
    eprintln!("{evidence}");
    eprintln!(
        "FAKE_CHAIN_SUITE_JSON {}",
        serde_json::json!({
            "cases_passed": 12,
            "prepared_abort_residual_retained": true,
            "shutdown_intent_failure_preserved_child": true,
            "abrupt_supervisor_death_closed_wrapper_and_child": true,
            "generation_switch_resize_and_continue": true,
            "ordinary_child_sigkill_from_supervisor": false,
        })
    );
    eprintln!("fake-chain integration: PASS");
}

fn setup_outer_worker(slave: RawFd) {
    // The harness emulates the terminal emulator/shell boundary. The
    // supervisor code itself never calls setsid or tcsetpgrp.
    // SAFETY: all calls operate on the worker and its newly opened PTY.
    unsafe {
        assert_ne!(libc::setsid(), -1);
        assert_ne!(libc::ioctl(slave, libc::TIOCSCTTY, 0), -1);
        let pgid = libc::getpgrp();
        assert_ne!(libc::tcsetpgrp(slave, pgid), -1);
        assert_ne!(libc::dup2(slave, libc::STDIN_FILENO), -1);
        assert_ne!(libc::dup2(slave, libc::STDOUT_FILENO), -1);
        assert_ne!(libc::dup2(slave, libc::STDERR_FILENO), -1);
    }
    if slave > libc::STDERR_FILENO {
        close_fd(slave);
    }
}

fn run_all_cases(status_fd: RawFd) {
    write_byte(status_fd, 1).unwrap();
    happy_three_generation_case();
    write_byte(status_fd, 2).unwrap();
    ignore_sigterm_case();
    write_byte(status_fd, 3).unwrap();
    natural_exit_without_stop_case();
    write_byte(status_fd, 4).unwrap();
    cleanup_failure_case();
    write_byte(status_fd, 5).unwrap();
    unreaped_child_case();
    write_byte(status_fd, 6).unwrap();
    stale_authorization_case();
    write_byte(status_fd, 7).unwrap();
    materialize_crash_boundary_case();
    write_byte(status_fd, 8).unwrap();
    signal_resize_continue_case();
    write_byte(status_fd, 9).unwrap();
    outer_hangup_case();
    write_byte(status_fd, 10).unwrap();
    shutdown_intent_failure_preserves_process_ownership_case();
    write_byte(status_fd, 11).unwrap();
    abrupt_supervisor_death_closes_generation_case();
    write_byte(status_fd, 12).unwrap();
    resize_and_continue_after_generation_switch_case();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChildBehavior {
    ExitOnSignal,
    IgnoreTerm,
    ExitImmediately,
}

#[derive(Clone, Copy, Debug, Default)]
struct AdapterFaults {
    delivery_cleanup: bool,
    leave_unreaped: bool,
    abort_prepared_residual: bool,
}

#[derive(Default, Debug)]
struct AdapterStats {
    target_prepares: usize,
    target_activations: usize,
    term_signals: usize,
    int_signals: usize,
    hup_signals: usize,
    fixture_sigkills: usize,
    max_live_generations: usize,
    live_generations: usize,
    os_serial_spawn_checks: usize,
    resize_count: usize,
    reassert_count: usize,
    child_interrupt_reports: usize,
    identities: Vec<GenerationIdentity>,
    child_session_ids: Vec<i32>,
}

struct FakePrepared {
    identity: GenerationIdentity,
    gate_write: RawFd,
    command_write: RawFd,
    report_read: RawFd,
    behavior: ChildBehavior,
}

impl PreparedGeneration for FakePrepared {
    fn identity(&self) -> &GenerationIdentity {
        &self.identity
    }
}

struct FakeActive {
    identity: GenerationIdentity,
    gate_write: RawFd,
    command_write: RawFd,
    report_read: RawFd,
    exit: Option<ExitEvidence>,
}

struct FakePtyAdapter {
    outer: OuterTerminalIdentity,
    behavior_by_generation: Vec<ChildBehavior>,
    faults: AdapterFaults,
    stats: AdapterStats,
    signal_read: RawFd,
    signal_write: RawFd,
    raised_signals: VecDeque<i32>,
    generation_signals: VecDeque<(u64, i32)>,
    duplicate_wakes: usize,
    observed_events: Arc<AtomicUsize>,
    outer_termios: libc::termios,
}

impl FakePtyAdapter {
    fn new(
        outer: OuterTerminalIdentity,
        behavior_by_generation: Vec<ChildBehavior>,
        faults: AdapterFaults,
        observed_events: Arc<AtomicUsize>,
    ) -> Self {
        let (signal_read, signal_write) = pipe().unwrap();
        let mut outer_termios = MaybeUninit::<libc::termios>::uninit();
        // SAFETY: stdin is the isolated worker's live outer TTY.
        assert_eq!(
            unsafe { libc::tcgetattr(libc::STDIN_FILENO, outer_termios.as_mut_ptr()) },
            0
        );
        // SAFETY: successful tcgetattr initialized the value.
        let outer_termios = unsafe { outer_termios.assume_init() };
        set_nonblocking(signal_write);
        SUPERVISOR_SIGNAL_WRITE_FD.store(signal_write, Ordering::Release);
        for signal in [libc::SIGINT, libc::SIGHUP, libc::SIGWINCH, libc::SIGCONT] {
            // SAFETY: installs one simple self-pipe handler in the isolated
            // test worker.
            assert_ne!(
                unsafe {
                    libc::signal(
                        signal,
                        supervisor_signal_handler as *const () as libc::sighandler_t,
                    )
                },
                libc::SIG_ERR
            );
        }
        Self {
            outer,
            behavior_by_generation,
            faults,
            stats: AdapterStats::default(),
            signal_read,
            signal_write,
            raised_signals: VecDeque::new(),
            generation_signals: VecDeque::new(),
            duplicate_wakes: 0,
            observed_events,
            outer_termios,
        }
    }

    fn spawn_initial(&mut self) -> FakeActive {
        let reservation = TargetReservation {
            handoff_id: "initial-fixture".to_string(),
            expected_version: 0,
            generation: 1,
            launch_nonce: "nonce-g1".to_string(),
        };
        let prepared = self.spawn_prepared(&reservation).unwrap();
        self.activate(prepared).unwrap()
    }

    fn behavior(&self, generation: u64) -> ChildBehavior {
        self.behavior_by_generation
            .get(generation.saturating_sub(1) as usize)
            .copied()
            .unwrap_or(ChildBehavior::ExitOnSignal)
    }

    fn spawn_prepared(&mut self, reservation: &TargetReservation) -> io::Result<FakePrepared> {
        for retired in &self.stats.identities {
            // A successor spawn is the only point that can increase live
            // generation count. Check the kernel, not just fixture counters,
            // immediately before that action.
            for pid in [retired.child_pid, retired.wrapper_pid] {
                // SAFETY: signal 0 performs a liveness check only.
                if unsafe { libc::kill(pid, 0) } == 0
                    || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "retired generation is still visible before successor spawn",
                    ));
                }
            }
            self.stats.os_serial_spawn_checks += 1;
        }
        let mut master = -1;
        let mut slave = -1;
        let winsize = libc::winsize {
            ws_row: 37,
            ws_col: 111,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: openpty initializes descriptors.
        if unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &winsize,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        let (gate_read, gate_write) = pipe()?;
        let (command_read, command_write) = pipe()?;
        let (report_read, report_write) = pipe()?;
        let behavior = self.behavior(reservation.generation);
        // SAFETY: the harness worker is single-threaded.
        let wrapper = unsafe { libc::fork() };
        if wrapper == -1 {
            return Err(io::Error::last_os_error());
        }
        if wrapper == 0 {
            close_fd(gate_write);
            close_fd(command_write);
            close_fd(report_read);
            wrapper_main(
                self.outer.supervisor_pid,
                gate_read,
                command_read,
                report_write,
                master,
                slave,
                behavior,
            );
        }
        close_fd(gate_read);
        close_fd(command_read);
        close_fd(report_write);
        close_fd(master);
        close_fd(slave);
        let start = read_message(report_read)?;
        if start.kind != MSG_START || start.a != i64::from(wrapper) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrapper start evidence mismatch",
            ));
        }
        let child_pid = i32::try_from(start.c)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid child PID"))?;
        let wrapper_pgid = i32::try_from(start.b)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid wrapper PGID"))?;
        if wrapper_pgid != self.outer.supervisor_pgid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "wrapper left the outer foreground process group",
            ));
        }
        let identity = GenerationIdentity {
            generation: reservation.generation,
            launch_nonce: reservation.launch_nonce.clone(),
            wrapper_pid: wrapper,
            wrapper_pgid,
            child_pid,
            child_pgid: child_pid,
            child_process_birth_identity: linux_process_birth_identity(child_pid)?,
            process_id: format!("proc-g{}", reservation.generation),
            process_birth_identity: linux_process_birth_identity(wrapper)?,
            instance_name: format!("fake-g{}", reservation.generation),
            hcom_session_id: format!("hcom-g{}", reservation.generation),
            synthetic_native_session_id: format!("native-g{}", reservation.generation),
        };
        self.stats.live_generations += 1;
        self.stats.max_live_generations = self
            .stats
            .max_live_generations
            .max(self.stats.live_generations);
        Ok(FakePrepared {
            identity,
            gate_write,
            command_write,
            report_read,
            behavior,
        })
    }

    fn activate(&mut self, prepared: FakePrepared) -> io::Result<FakeActive> {
        write_byte(prepared.gate_write, 1)?;
        let ready = read_until_kind(prepared.report_read, MSG_READY)?;
        if ready.a != i64::from(prepared.identity.child_pid)
            || ready.b != i64::from(prepared.identity.child_pid)
            || ready.c != i64::from(prepared.identity.child_pgid)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inner child session/process-group evidence mismatch",
            ));
        }
        self.stats
            .child_session_ids
            .push(i32::try_from(ready.b).unwrap());
        self.stats.identities.push(prepared.identity.clone());
        Ok(FakeActive {
            identity: prepared.identity,
            gate_write: prepared.gate_write,
            command_write: prepared.command_write,
            report_read: prepared.report_read,
            exit: None,
        })
    }

    fn finish_real(
        &mut self,
        mut active: FakeActive,
        exit: &ExitEvidence,
    ) -> FinishAttempt<FakeActive> {
        if self.faults.leave_unreaped {
            active.exit = Some(exit.clone());
            return FinishAttempt {
                evidence: CleanupEvidence {
                    exit: Some(exit.clone()),
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "waitpid_injected".to_string(),
                    failure_reason: "fixture left the observed child unreaped".to_string(),
                },
                residual: Some(active),
            };
        }
        let cleanup = reap_wrapper(&active, exit, self.faults.delivery_cleanup);
        self.stats.live_generations = self.stats.live_generations.saturating_sub(1);
        close_active_fds(&active);
        FinishAttempt {
            evidence: cleanup,
            residual: None,
        }
    }

    fn fixture_force_teardown(&mut self, active: FakeActive) {
        // This explicit SIGKILL exists only in negative-fixture teardown. It is
        // outside the supervisor API and cannot record handoff success.
        self.stats.fixture_sigkills += 1;
        let exit = match active.exit.clone() {
            Some(exit) => exit,
            None => {
                // SAFETY: exact child process group belongs to this fixture.
                unsafe {
                    libc::kill(-active.identity.child_pgid, libc::SIGKILL);
                }
                read_until_kind(active.report_read, MSG_EXIT)
                    .map(message_to_exit)
                    .unwrap()
            }
        };
        let _ = reap_wrapper(&active, &exit, false);
        self.stats.live_generations = self.stats.live_generations.saturating_sub(1);
        close_active_fds(&active);
    }
}

impl Drop for FakePtyAdapter {
    fn drop(&mut self) {
        // SAFETY: the persistent adapter owns restoration of the outer TTY.
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.outer_termios);
        }
        SUPERVISOR_SIGNAL_WRITE_FD.store(-1, Ordering::Release);
        close_fd(self.signal_read);
        close_fd(self.signal_write);
    }
}

impl GenerationAdapter for FakePtyAdapter {
    type Active = FakeActive;
    type Prepared = FakePrepared;
    type Error = io::Error;

    fn identity<'a>(&'a self, active: &'a Self::Active) -> &'a GenerationIdentity {
        &active.identity
    }

    fn wait_event(
        &mut self,
        active: &mut Self::Active,
        timeout: Duration,
    ) -> Result<GenerationEvent, Self::Error> {
        if self.duplicate_wakes > 0 {
            self.duplicate_wakes -= 1;
            return Ok(GenerationEvent::ControlWake);
        }
        if let Some(message) = try_read_message(active.report_read)? {
            return self.handle_report(active, message);
        }
        let generation_signal = self
            .generation_signals
            .front()
            .is_some_and(|(generation, _)| *generation == active.identity.generation)
            .then(|| self.generation_signals.pop_front().unwrap().1);
        if let Some(signal) = generation_signal.or_else(|| self.raised_signals.pop_front()) {
            // SAFETY: raises a real signal in the isolated supervisor process;
            // the installed handler writes its self-pipe.
            assert_eq!(unsafe { libc::raise(signal) }, 0);
        }
        let mut poll_fds = [
            libc::pollfd {
                fd: active.report_read,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.signal_read,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: array points to initialized pollfd entries.
        let count = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, timeout_ms) };
        if count == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(GenerationEvent::ControlWake);
            }
            return Err(error);
        }
        if count == 0 {
            return Ok(GenerationEvent::Timeout);
        }
        if poll_fds[0].revents & libc::POLLIN != 0 {
            let message = read_message(active.report_read)?;
            return self.handle_report(active, message);
        }
        if poll_fds[1].revents & libc::POLLIN != 0 {
            let signal = read_byte(self.signal_read)? as i32;
            self.observed_events.fetch_add(1, Ordering::AcqRel);
            return Ok(match signal {
                libc::SIGINT => GenerationEvent::Interrupt,
                libc::SIGHUP => GenerationEvent::Hangup,
                libc::SIGWINCH => GenerationEvent::Resize,
                libc::SIGCONT => GenerationEvent::Continue,
                _ => GenerationEvent::ControlWake,
            });
        }
        Ok(GenerationEvent::ControlWake)
    }

    fn send_signal(&mut self, active: &Self::Active, signal: ChainSignal) -> SignalSendResult {
        match signal {
            ChainSignal::Interrupt => {
                self.stats.int_signals += 1;
            }
            ChainSignal::Terminate => {
                self.stats.term_signals += 1;
                self.duplicate_wakes = self.duplicate_wakes.max(2);
            }
            ChainSignal::Hangup => {
                self.stats.hup_signals += 1;
            }
        }
        send_process_group_signal(
            active.identity.child_pid,
            active.identity.child_pgid,
            &active.identity.child_process_birth_identity,
            signal,
        )
    }

    fn resize(&mut self, active: &mut Self::Active) -> Result<(), Self::Error> {
        write_byte(active.command_write, b'W')?;
        let message = read_until_kind(active.report_read, MSG_RESIZE)?;
        if message.a != 37 || message.b != 111 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid resize acknowledgement",
            ));
        }
        self.stats.resize_count += 1;
        Ok(())
    }

    fn reassert_outer_terminal(&mut self) -> Result<(), Self::Error> {
        let mut termios = MaybeUninit::<libc::termios>::uninit();
        // SAFETY: stdin is the worker's live outer TTY and termios is writable.
        if unsafe { libc::tcgetattr(libc::STDIN_FILENO, termios.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: tcgetattr initialized the value.
        let mut termios = unsafe { termios.assume_init() };
        // SAFETY: cfmakeraw/tcsetattr receive valid termios storage.
        unsafe {
            libc::cfmakeraw(&mut termios);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &termios) == -1 {
                return Err(io::Error::last_os_error());
            }
        }
        self.stats.reassert_count += 1;
        Ok(())
    }

    fn finish_after_exit(
        &mut self,
        active: Self::Active,
        exit: &ExitEvidence,
    ) -> FinishAttempt<Self::Active> {
        self.finish_real(active, exit)
    }

    fn shutdown_without_successor(
        &mut self,
        mut active: Self::Active,
        _reason: ShutdownReason,
    ) -> FinishAttempt<Self::Active> {
        self.stats.hup_signals += 1;
        if send_process_group_signal(
            active.identity.child_pid,
            active.identity.child_pgid,
            &active.identity.child_process_birth_identity,
            ChainSignal::Hangup,
        ) != SignalSendResult::Sent
        {
            return FinishAttempt {
                evidence: CleanupEvidence {
                    exit: active.exit.clone(),
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "sighup_failed".to_string(),
                    failure_reason: "fixture shutdown SIGHUP was not delivered".to_string(),
                },
                residual: Some(active),
            };
        }
        let exit = match active.exit.take() {
            Some(exit) => exit,
            None => match read_until_kind(active.report_read, MSG_EXIT) {
                Ok(message) => message_to_exit(message),
                Err(_) => {
                    return FinishAttempt {
                        evidence: CleanupEvidence {
                            exit: None,
                            waitpid_reaped: false,
                            resources: ResourceCleanupEvidence::default(),
                            failure_kind: "shutdown_exit_missing".to_string(),
                            failure_reason: "fixture child exit was not observed".to_string(),
                        },
                        residual: Some(active),
                    };
                }
            },
        };
        self.finish_real(active, &exit)
    }

    fn prepare_target(
        &mut self,
        reservation: &TargetReservation,
        outer: OuterTerminalIdentity,
    ) -> Result<Self::Prepared, Self::Error> {
        if outer != self.outer || self.stats.live_generations != 0 {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "target prepare attempted while a generation is still owned",
            ));
        }
        self.stats.target_prepares += 1;
        self.spawn_prepared(reservation)
    }

    fn activate_target(
        &mut self,
        prepared: Self::Prepared,
    ) -> Result<Self::Active, (Self::Prepared, Self::Error)> {
        let fallback = FakePrepared {
            identity: prepared.identity.clone(),
            gate_write: prepared.gate_write,
            command_write: prepared.command_write,
            report_read: prepared.report_read,
            behavior: prepared.behavior,
        };
        match self.activate(prepared) {
            Ok(active) => {
                self.stats.target_activations += 1;
                Ok(active)
            }
            Err(error) => Err((fallback, error)),
        }
    }

    fn abort_prepared(&mut self, prepared: Self::Prepared) -> FinishAttempt<Self::Prepared> {
        if self.faults.abort_prepared_residual {
            return FinishAttempt {
                evidence: CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "injected_prepared_abort_failure".to_string(),
                    failure_reason: "fixture retained the gated target for explicit teardown"
                        .to_string(),
                },
                residual: Some(prepared),
            };
        }
        close_fd(prepared.gate_write);
        let exit = read_until_kind(prepared.report_read, MSG_EXIT)
            .map(message_to_exit)
            .unwrap();
        let active = FakeActive {
            identity: prepared.identity,
            gate_write: -1,
            command_write: prepared.command_write,
            report_read: prepared.report_read,
            exit: Some(exit.clone()),
        };
        let cleanup = reap_wrapper(&active, &exit, false);
        self.stats.live_generations = self.stats.live_generations.saturating_sub(1);
        close_active_fds(&active);
        FinishAttempt {
            evidence: cleanup,
            residual: None,
        }
    }
}

impl FakePtyAdapter {
    fn handle_report(
        &mut self,
        active: &mut FakeActive,
        message: WireMessage,
    ) -> Result<GenerationEvent, io::Error> {
        match message.kind {
            MSG_EXIT => {
                let exit = message_to_exit(message);
                active.exit = Some(exit.clone());
                Ok(GenerationEvent::ChildExited(exit))
            }
            MSG_INTERRUPT => {
                self.stats.child_interrupt_reports += 1;
                Ok(GenerationEvent::ControlWake)
            }
            _ => Ok(GenerationEvent::ControlWake),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlPhase {
    Active,
    Begun,
    SigtermRecorded,
    Materialized,
    AwaitingAcceptance,
    ShuttingDown,
    Stopped,
    Recovery,
}

struct MockControl {
    generation: u64,
    max_generation: u64,
    version: i64,
    phase: ControlPhase,
    handoff_enabled: bool,
    corrupt_authorization: bool,
    materialize_commit_then_error: bool,
    shutdown_intent_error: bool,
    ready_count: usize,
    acceptance_count: usize,
    cleanup_count: usize,
    unexpected_exit_count: usize,
    observed_events: Arc<AtomicUsize>,
    stop_after_events: Option<usize>,
}

impl MockControl {
    fn serial(max_generation: u64, observed_events: Arc<AtomicUsize>) -> Self {
        Self {
            generation: 1,
            max_generation,
            version: 1,
            phase: ControlPhase::Active,
            handoff_enabled: true,
            corrupt_authorization: false,
            materialize_commit_then_error: false,
            shutdown_intent_error: false,
            ready_count: 0,
            acceptance_count: 0,
            cleanup_count: 0,
            unexpected_exit_count: 0,
            observed_events,
            stop_after_events: None,
        }
    }

    fn waiting(observed_events: Arc<AtomicUsize>) -> Self {
        let mut control = Self::serial(1, observed_events);
        control.handoff_enabled = false;
        control
    }

    fn accept_explicitly(&mut self, generation: u64) {
        assert_eq!(self.phase, ControlPhase::AwaitingAcceptance);
        assert_eq!(self.generation, generation);
        self.phase = ControlPhase::Active;
        self.version = 1;
        self.acceptance_count += 1;
    }

    fn authorization(&self, active: &GenerationIdentity) -> QuiesceAuthorization {
        QuiesceAuthorization {
            handoff_id: format!("handoff-g{}", active.generation),
            expected_version: self.version,
            quiesce_token: format!("quiesce-g{}", active.generation),
            generation: active.generation,
            launch_nonce: active.launch_nonce.clone(),
            pinned_native_session_id: active.synthetic_native_session_id.clone(),
            process_birth_identity: if self.corrupt_authorization {
                "linux-v1:1:1:stale".to_string()
            } else {
                active.process_birth_identity.clone()
            },
        }
    }
}

impl DurableControl for MockControl {
    type Error = String;

    fn read_directive(
        &mut self,
        active: &GenerationIdentity,
        _local_quiesce: Option<&QuiesceApply>,
    ) -> Result<DurableDirective, Self::Error> {
        if self.phase == ControlPhase::Recovery {
            return Ok(DurableDirective::NeedsRecovery(
                "fixture durable recovery".to_string(),
            ));
        }
        if let Some(limit) = self.stop_after_events
            && self.observed_events.load(Ordering::Acquire) >= limit
        {
            return Ok(DurableDirective::StopChain);
        }
        if !self.handoff_enabled {
            return Ok(DurableDirective::Wait);
        }
        if active.generation != self.generation {
            return Err("active generation differs from durable generation".to_string());
        }
        if self.phase == ControlPhase::Active {
            if self.generation >= self.max_generation {
                return Ok(if self.stop_after_events.is_some() {
                    DurableDirective::Wait
                } else {
                    DurableDirective::StopChain
                });
            }
            return Ok(DurableDirective::Quiesce(self.authorization(active)));
        }
        Ok(DurableDirective::Wait)
    }

    fn begin_quiesce(
        &mut self,
        active: &GenerationIdentity,
        authorization: &QuiesceAuthorization,
    ) -> Result<QuiesceApply, Self::Error> {
        if self.phase != ControlPhase::Active
            || authorization.generation != self.generation
            || authorization.launch_nonce != active.launch_nonce
        {
            return Err("begin_quiesce exact CAS failed".to_string());
        }
        self.phase = ControlPhase::Begun;
        self.version += 1;
        Ok(QuiesceApply {
            handoff_id: authorization.handoff_id.clone(),
            expected_version: self.version,
            generation: active.generation,
        })
    }

    fn record_sigterm(
        &mut self,
        apply: &QuiesceApply,
        evidence: &SigtermEvidence,
    ) -> Result<QuiesceApply, Self::Error> {
        if self.phase != ControlPhase::Begun
            || apply.expected_version != self.version
            || evidence.requested_wall_seconds == 0
            || evidence.requested_monotonic_ns <= 0
        {
            return Err("SIGTERM evidence CAS failed".to_string());
        }
        self.version += 1;
        if evidence.result == SignalSendResult::Sent {
            self.phase = ControlPhase::SigtermRecorded;
        } else {
            self.phase = ControlPhase::Recovery;
        }
        Ok(QuiesceApply {
            handoff_id: apply.handoff_id.clone(),
            expected_version: self.version,
            generation: apply.generation,
        })
    }

    fn record_cleanup(
        &mut self,
        apply: &QuiesceApply,
        evidence: &CleanupEvidence,
    ) -> Result<PostCleanup, Self::Error> {
        if self.phase != ControlPhase::SigtermRecorded || apply.expected_version != self.version {
            return Err("cleanup exact CAS failed".to_string());
        }
        self.cleanup_count += 1;
        self.version += 1;
        if !evidence.successful() {
            self.phase = ControlPhase::Recovery;
            return Ok(PostCleanup::NeedsRecovery);
        }
        Ok(PostCleanup::Advance(TargetReservation {
            handoff_id: apply.handoff_id.clone(),
            expected_version: self.version,
            generation: self.generation + 1,
            launch_nonce: format!("nonce-g{}", self.generation + 1),
        }))
    }

    fn record_exit_without_stop(
        &mut self,
        active: &GenerationIdentity,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error> {
        assert_eq!(active.generation, self.generation);
        assert!(evidence.exit.is_some());
        self.unexpected_exit_count += 1;
        self.phase = ControlPhase::Recovery;
        Ok(())
    }

    fn materialize_target(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error> {
        if reservation.generation != self.generation + 1
            || reservation.launch_nonce != identity.launch_nonce
            || self.phase != ControlPhase::SigtermRecorded
        {
            return Err("materialization reservation mismatch".to_string());
        }
        self.phase = ControlPhase::Materialized;
        self.version += 1;
        if self.materialize_commit_then_error {
            self.phase = ControlPhase::Recovery;
            return Err("injected post-commit materialization crash".to_string());
        }
        Ok(())
    }

    fn target_ready(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error> {
        if self.phase != ControlPhase::Materialized
            || reservation.generation != identity.generation
            || identity.synthetic_native_session_id == identity.hcom_session_id
        {
            return Err("target ready exact identity mismatch".to_string());
        }
        self.generation = reservation.generation;
        self.phase = ControlPhase::AwaitingAcceptance;
        self.ready_count += 1;
        Ok(())
    }

    fn record_target_failure(
        &mut self,
        reservation: &TargetReservation,
        identity: Option<&GenerationIdentity>,
        cleanup: Option<&CleanupEvidence>,
        failure_kind: &str,
        failure_reason: &str,
    ) -> Result<(), Self::Error> {
        if reservation.generation != self.generation + 1
            || identity.is_some_and(|value| {
                value.generation != reservation.generation
                    || value.launch_nonce != reservation.launch_nonce
            })
            || failure_kind.is_empty()
            || failure_reason.is_empty()
        {
            return Err("target failure evidence mismatch".to_string());
        }
        let _cleanup_completed = cleanup.is_some_and(CleanupEvidence::successful);
        self.phase = ControlPhase::Recovery;
        Ok(())
    }

    fn begin_shutdown(
        &mut self,
        active: &GenerationIdentity,
        _reason: ShutdownReason,
    ) -> Result<(), Self::Error> {
        if self.shutdown_intent_error {
            return Err("injected shutdown-intent persistence failure".to_string());
        }
        if active.generation != self.generation
            || !matches!(
                self.phase,
                ControlPhase::Active | ControlPhase::AwaitingAcceptance
            )
        {
            return Err("shutdown intent exact CAS failed".to_string());
        }
        self.phase = ControlPhase::ShuttingDown;
        Ok(())
    }

    fn record_shutdown(
        &mut self,
        active: &GenerationIdentity,
        _reason: ShutdownReason,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error> {
        if active.generation != self.generation
            || self.phase != ControlPhase::ShuttingDown
            || !evidence.successful()
        {
            return Err("shutdown evidence mismatch".to_string());
        }
        self.phase = ControlPhase::Stopped;
        Ok(())
    }
}

fn happy_three_generation_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::serial(3, Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal; 3],
        AdapterFaults::default(),
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();

    for generation in [2, 3] {
        match supervisor.run() {
            SupervisorRunOutcome::AwaitingAcceptance {
                generation: actual, ..
            } => assert_eq!(actual, generation),
            other => panic!("unexpected serial handoff outcome: {other:?}"),
        }
        supervisor.control_mut().accept_explicitly(generation);
    }
    assert_eq!(supervisor.run(), SupervisorRunOutcome::Stopped);
    let trace = supervisor.trace().to_vec();
    let (control, adapter, active, prepared, _) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(control.ready_count, 2);
    assert_eq!(control.acceptance_count, 2);
    assert_eq!(control.cleanup_count, 2);
    assert_eq!(adapter.stats.target_prepares, 2);
    assert_eq!(adapter.stats.target_activations, 2);
    assert_eq!(adapter.stats.term_signals, 2);
    assert_eq!(adapter.stats.fixture_sigkills, 0);
    assert_eq!(adapter.stats.max_live_generations, 1);
    assert_eq!(adapter.stats.os_serial_spawn_checks, 3);
    assert_eq!(adapter.stats.live_generations, 0);
    assert_eq!(adapter.stats.identities.len(), 3);
    assert_eq!(adapter.stats.child_session_ids.len(), 3);
    assert!(
        adapter
            .stats
            .child_session_ids
            .windows(2)
            .all(|pair| pair[0] != pair[1])
    );
    assert!(
        adapter
            .stats
            .identities
            .windows(2)
            .all(|pair| pair[0].wrapper_pid != pair[1].wrapper_pid
                && pair[0].child_pid != pair[1].child_pid
                && pair[0].hcom_session_id != pair[1].hcom_session_id
                && pair[0].synthetic_native_session_id != pair[1].synthetic_native_session_id)
    );
    assert!(
        trace
            .windows(2)
            .all(|pair| pair[0].sequence + 1 == pair[1].sequence)
    );
    assert!(trace.iter().all(|record| {
        record.supervisor_pid == outer.supervisor_pid
            && record.supervisor_pgid == outer.supervisor_pgid
            && record.tty_device == outer.tty_device
            && record.tty_inode == outer.tty_inode
    }));
    let reaped: Vec<u64> = trace
        .iter()
        .filter(|record| record.kind == TraceKind::ChildReaped)
        .filter_map(|record| record.generation)
        .collect();
    assert_eq!(reaped, vec![1, 2, 3]);
    for target_generation in [2, 3] {
        let source_reaped = trace
            .iter()
            .find(|record| {
                record.kind == TraceKind::ChildReaped
                    && record.generation == Some(target_generation - 1)
            })
            .unwrap()
            .sequence;
        let target_materialized = trace
            .iter()
            .find(|record| {
                record.kind == TraceKind::TargetMaterialized
                    && record.generation == Some(target_generation)
            })
            .unwrap()
            .sequence;
        assert!(source_reaped < target_materialized);
    }
    let generations: Vec<_> = adapter
        .stats
        .identities
        .iter()
        .zip(&adapter.stats.child_session_ids)
        .map(|(identity, child_sid)| {
            serde_json::json!({
                "generation": identity.generation,
                "wrapper_pid": identity.wrapper_pid,
                "wrapper_pgid": identity.wrapper_pgid,
                "child_pid": identity.child_pid,
                "child_pgid": identity.child_pgid,
                "child_sid": child_sid,
                "hcom_native_distinct":
                    identity.hcom_session_id != identity.synthetic_native_session_id,
            })
        })
        .collect();
    let structured_trace: Vec<_> = trace
        .iter()
        .map(|record| {
            serde_json::json!({
                "sequence": record.sequence,
                "kind": format!("{:?}", record.kind),
                "generation": record.generation,
                "wrapper_pid": record.wrapper_pid,
                "child_pid": record.child_pid,
                "child_pgid": record.child_pgid,
            })
        })
        .collect();
    println!(
        "FAKE_CHAIN_JSON {}",
        serde_json::json!({
            "supervisor_pid": outer.supervisor_pid,
            "supervisor_pgid": outer.supervisor_pgid,
            "foreground_pgid": outer.foreground_pgid,
            "tty_device": outer.tty_device,
            "tty_inode": outer.tty_inode,
            "generations": generations,
            "max_live_generations": adapter.stats.max_live_generations,
            "os_serial_spawn_checks": adapter.stats.os_serial_spawn_checks,
            "term_signals": adapter.stats.term_signals,
            "fixture_sigkills": adapter.stats.fixture_sigkills,
            "g1_g2_reaped_before_materialized": true,
            "successor_acceptances_explicit": control.acceptance_count,
            "trace": structured_trace,
        })
    );
}

fn ignore_sigterm_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::serial(2, Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::IgnoreTerm],
        AdapterFaults::default(),
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_millis(120))
            .unwrap();
    let outcome = supervisor.run();
    assert!(matches!(outcome, SupervisorRunOutcome::NeedsRecovery(_)));
    let (control, mut adapter, active, prepared, trace) = supervisor.into_parts();
    assert!(prepared.is_none());
    assert_eq!(control.phase, ControlPhase::Recovery);
    assert_eq!(adapter.stats.term_signals, 1);
    assert_eq!(adapter.stats.target_prepares, 0);
    assert!(
        !trace
            .iter()
            .any(|record| record.kind == TraceKind::TargetPrepare)
    );
    adapter.fixture_force_teardown(active.unwrap());
    assert_eq!(adapter.stats.fixture_sigkills, 1);
}

fn natural_exit_without_stop_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::waiting(Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitImmediately],
        AdapterFaults::default(),
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (control, adapter, active, prepared, _) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(control.unexpected_exit_count, 1);
    assert_eq!(adapter.stats.target_prepares, 0);
}

fn cleanup_failure_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::serial(2, Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults {
            delivery_cleanup: true,
            leave_unreaped: false,
            abort_prepared_residual: false,
        },
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (control, adapter, active, prepared, _) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(control.phase, ControlPhase::Recovery);
    assert_eq!(adapter.stats.target_prepares, 0);
}

fn unreaped_child_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::serial(2, Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults {
            delivery_cleanup: false,
            leave_unreaped: true,
            abort_prepared_residual: false,
        },
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (_, mut adapter, active, prepared, _) = supervisor.into_parts();
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.target_prepares, 0);
    adapter.faults.leave_unreaped = false;
    adapter.fixture_force_teardown(active.unwrap());
}

fn stale_authorization_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let mut control = MockControl::serial(2, Arc::clone(&observed));
    control.corrupt_authorization = true;
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults::default(),
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (_, mut adapter, active, prepared, _) = supervisor.into_parts();
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.term_signals, 0);
    assert_eq!(adapter.stats.target_prepares, 0);
    adapter.fixture_force_teardown(active.unwrap());
}

fn materialize_crash_boundary_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let mut control = MockControl::serial(2, Arc::clone(&observed));
    control.materialize_commit_then_error = true;
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal; 2],
        AdapterFaults {
            abort_prepared_residual: true,
            ..AdapterFaults::default()
        },
        observed,
    );
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (control, mut adapter, active, prepared, _) = supervisor.into_parts();
    assert!(active.is_none());
    assert_eq!(control.phase, ControlPhase::Recovery);
    assert_eq!(adapter.stats.target_prepares, 1);
    assert_eq!(adapter.stats.target_activations, 0);
    assert_eq!(adapter.stats.max_live_generations, 1);
    assert_eq!(adapter.stats.live_generations, 1);
    adapter.faults.abort_prepared_residual = false;
    let teardown = adapter.abort_prepared(prepared.unwrap());
    assert!(teardown.evidence.successful());
    assert!(teardown.residual.is_none());
    assert_eq!(adapter.stats.live_generations, 0);
}

fn signal_resize_continue_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let mut control = MockControl::waiting(Arc::clone(&observed));
    control.stop_after_events = Some(3);
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults::default(),
        Arc::clone(&observed),
    );
    adapter.raised_signals = VecDeque::from([libc::SIGINT, libc::SIGWINCH, libc::SIGCONT]);
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert_eq!(supervisor.run(), SupervisorRunOutcome::Stopped);
    // The foreground supervisor is still the same live process.
    // SAFETY: getpid has no preconditions.
    assert_eq!(unsafe { libc::getpid() }, outer.supervisor_pid);
    let (_, adapter, active, prepared, trace) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.int_signals, 1);
    assert_eq!(adapter.stats.resize_count, 2);
    assert_eq!(adapter.stats.reassert_count, 1);
    assert!(
        trace
            .iter()
            .any(|record| record.kind == TraceKind::InterruptForwarded)
    );
    assert!(
        trace
            .iter()
            .any(|record| record.kind == TraceKind::ResizeApplied)
    );
    assert!(
        trace
            .iter()
            .any(|record| record.kind == TraceKind::ContinueApplied)
    );
    let mut termios = MaybeUninit::<libc::termios>::uninit();
    // SAFETY: stdin is the worker's live outer TTY and termios is writable.
    assert_eq!(
        unsafe { libc::tcgetattr(libc::STDIN_FILENO, termios.as_mut_ptr()) },
        0
    );
    // SAFETY: successful tcgetattr initialized termios.
    let termios = unsafe { termios.assume_init() };
    assert_eq!(termios.c_lflag & (libc::ICANON | libc::ECHO), 0);
}

fn resize_and_continue_after_generation_switch_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let mut control = MockControl::serial(2, Arc::clone(&observed));
    control.stop_after_events = Some(2);
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal; 2],
        AdapterFaults::default(),
        observed,
    );
    adapter.generation_signals = VecDeque::from([(2, libc::SIGWINCH), (2, libc::SIGCONT)]);
    let active = adapter.spawn_initial();
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::AwaitingAcceptance { generation: 2, .. }
    ));
    supervisor.control_mut().accept_explicitly(2);
    assert_eq!(supervisor.run(), SupervisorRunOutcome::Stopped);
    let (_, adapter, active, prepared, trace) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.resize_count, 2);
    assert_eq!(adapter.stats.reassert_count, 1);
    assert!(
        trace.iter().any(|record| {
            record.kind == TraceKind::ResizeApplied && record.generation == Some(2)
        })
    );
    assert!(trace.iter().any(|record| {
        record.kind == TraceKind::ContinueApplied && record.generation == Some(2)
    }));
}

fn outer_hangup_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let control = MockControl::waiting(Arc::clone(&observed));
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults::default(),
        observed,
    );
    adapter.raised_signals.push_back(libc::SIGHUP);
    let active = adapter.spawn_initial();
    let child_pid = active.identity.child_pid;
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert_eq!(supervisor.run(), SupervisorRunOutcome::Stopped);
    let (_, adapter, active, prepared, trace) = supervisor.into_parts();
    assert!(active.is_none());
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.target_prepares, 0);
    assert!(
        trace
            .iter()
            .any(|record| record.kind == TraceKind::OuterHangup)
    );
    // SAFETY: signal 0 checks liveness without changing process state.
    assert_eq!(unsafe { libc::kill(child_pid, 0) }, -1);
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
}

fn shutdown_intent_failure_preserves_process_ownership_case() {
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let mut control = MockControl::waiting(Arc::clone(&observed));
    control.stop_after_events = Some(0);
    control.shutdown_intent_error = true;
    let mut adapter = FakePtyAdapter::new(
        outer,
        vec![ChildBehavior::ExitOnSignal],
        AdapterFaults::default(),
        observed,
    );
    let active = adapter.spawn_initial();
    let child_pid = active.identity.child_pid;
    let mut supervisor =
        ForegroundChainSupervisor::new(outer, control, adapter, active, Duration::from_secs(1))
            .unwrap();
    assert!(matches!(
        supervisor.run(),
        SupervisorRunOutcome::NeedsRecovery(_)
    ));
    let (_, mut adapter, active, prepared, trace) = supervisor.into_parts();
    assert!(prepared.is_none());
    assert_eq!(adapter.stats.hup_signals, 0);
    assert!(
        !trace
            .iter()
            .any(|record| record.kind == TraceKind::ShutdownIntent)
    );
    // SAFETY: signal 0 checks that the exact fixture child remains owned.
    assert_eq!(unsafe { libc::kill(child_pid, 0) }, 0);
    adapter.fixture_force_teardown(active.unwrap());
}

fn abrupt_supervisor_death_closes_generation_case() {
    let (report_read, report_write) = pipe().unwrap();
    // SAFETY: the isolated worker is single-threaded.
    let supervisor = unsafe { libc::fork() };
    assert!(supervisor >= 0);
    if supervisor == 0 {
        close_fd(report_read);
        let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let mut adapter = FakePtyAdapter::new(
            outer,
            vec![ChildBehavior::ExitOnSignal],
            AdapterFaults::default(),
            observed,
        );
        let active = adapter.spawn_initial();
        write_message(
            report_write,
            WireMessage {
                kind: MSG_START,
                a: i64::from(active.identity.wrapper_pid),
                b: i64::from(active.identity.child_pid),
                c: i64::from(active.identity.child_pgid),
                ..WireMessage::default()
            },
        )
        .unwrap();
        close_fd(report_write);
        // SAFETY: the parent deliberately kills this supervisor.
        unsafe {
            libc::pause();
            libc::_exit(94);
        }
    }

    close_fd(report_write);
    let report = read_message(report_read).unwrap();
    close_fd(report_read);
    assert_eq!(report.kind, MSG_START);
    let wrapper_pid = i32::try_from(report.a).unwrap();
    let child_pid = i32::try_from(report.b).unwrap();
    let child_pgid = i32::try_from(report.c).unwrap();
    let wrapper_pidfd = pidfd_open(wrapper_pid).unwrap();
    let child_pidfd = pidfd_open(child_pid).unwrap();
    // SAFETY: exact nested fixture supervisor; SIGKILL is test teardown, not
    // a chain child escalation path.
    assert_eq!(unsafe { libc::kill(supervisor, libc::SIGKILL) }, 0);
    let mut status = 0;
    // SAFETY: supervisor is this worker's exact direct child.
    assert_eq!(
        unsafe { libc::waitpid(supervisor, &mut status, 0) },
        supervisor
    );
    assert!(libc::WIFSIGNALED(status));
    assert_eq!(libc::WTERMSIG(status), libc::SIGKILL);

    let exited = wait_pidfds_readable(&[wrapper_pidfd, child_pidfd], Duration::from_secs(2));
    if !exited {
        // SAFETY: bounded fixture fallback prevents pollution on assertion
        // failure; it is outside all production supervisor APIs.
        unsafe {
            libc::kill(-child_pgid, libc::SIGKILL);
            libc::kill(wrapper_pid, libc::SIGKILL);
        }
    }
    close_fd(wrapper_pidfd);
    close_fd(child_pidfd);
    assert!(
        exited,
        "wrapper/inner child survived abrupt foreground-supervisor death"
    );
}

fn wrapper_main(
    supervisor_pid: i32,
    gate_read: RawFd,
    command_read: RawFd,
    report_write: RawFd,
    master: RawFd,
    slave: RawFd,
    behavior: ChildBehavior,
) -> ! {
    if arm_parent_death_hangup(supervisor_pid).is_err() {
        // SAFETY: the wrapper has not created the inner child yet.
        unsafe { libc::_exit(89) }
    }
    // SAFETY: single-threaded wrapper forks its deterministic inner child.
    let child = unsafe { libc::fork() };
    if child == -1 {
        unsafe { libc::_exit(90) }
    }
    if child == 0 {
        close_fd(command_read);
        close_fd(master);
        // SAFETY: getppid has no preconditions.
        let wrapper_pid = unsafe { libc::getppid() };
        inner_child_main(wrapper_pid, gate_read, report_write, slave, behavior);
    }
    close_fd(gate_read);
    close_fd(slave);
    let start = WireMessage {
        kind: MSG_START,
        a: i64::from(unsafe { libc::getpid() }),
        b: i64::from(unsafe { libc::getpgrp() }),
        c: i64::from(child),
        ..WireMessage::default()
    };
    write_message(report_write, start).unwrap();

    let mut exit_reported = false;
    loop {
        if !exit_reported && let Some(exit) = observe_child_exit_without_reap(child) {
            write_message(report_write, exit_to_message(&exit)).unwrap();
            exit_reported = true;
        }
        let mut descriptor = libc::pollfd {
            fd: command_read,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: descriptor is initialized.
        let ready = unsafe { libc::poll(&mut descriptor, 1, 20) };
        if ready > 0 && descriptor.revents & libc::POLLIN != 0 {
            let command = read_byte(command_read).unwrap_or(0);
            match command {
                b'W' => {
                    if let Ok(size) = copy_winsize(libc::STDIN_FILENO, master) {
                        write_message(
                            report_write,
                            WireMessage {
                                kind: MSG_RESIZE,
                                a: i64::from(size.ws_row),
                                b: i64::from(size.ws_col),
                                ..WireMessage::default()
                            },
                        )
                        .unwrap();
                    }
                }
                b'R' => {
                    let reaped = reap_direct_child(child).is_ok();
                    close_fd(master);
                    write_message(
                        report_write,
                        WireMessage {
                            kind: MSG_REAP,
                            a: i64::from(reaped),
                            b: 0b1_1111,
                            ..WireMessage::default()
                        },
                    )
                    .unwrap();
                    close_fd(command_read);
                    close_fd(report_write);
                    // SAFETY: wrapper owns no Rust destructors that matter.
                    unsafe { libc::_exit(if reaped { 0 } else { 91 }) }
                }
                _ => {}
            }
        }
    }
}

fn inner_child_main(
    wrapper_pid: i32,
    gate_read: RawFd,
    report_write: RawFd,
    slave: RawFd,
    behavior: ChildBehavior,
) -> ! {
    if arm_parent_death_hangup(wrapper_pid).is_err() {
        // SAFETY: the inner child has not activated the tool.
        unsafe { libc::_exit(88) }
    }
    let gate = read_byte(gate_read);
    close_fd(gate_read);
    if !matches!(gate, Ok(1)) {
        close_fd(slave);
        // SAFETY: dormant gate revocation exits before tool activation.
        unsafe { libc::_exit(73) }
    }
    // SAFETY: inner child creates its own session and controlling inner PTY.
    unsafe {
        if libc::setsid() == -1 || libc::ioctl(slave, libc::TIOCSCTTY, 0) == -1 {
            libc::_exit(92);
        }
        for fd in [0, 1, 2] {
            if libc::dup2(slave, fd) == -1 {
                libc::_exit(93);
            }
        }
    }
    if slave > 2 {
        close_fd(slave);
    }
    // The inner child must not inherit the supervisor's outer-terminal
    // handlers. In particular, SIGHUP is the chain-cancel mechanism here.
    // SAFETY: restoring standard dispositions is process-local.
    unsafe {
        libc::signal(libc::SIGHUP, libc::SIG_DFL);
        libc::signal(libc::SIGWINCH, libc::SIG_DFL);
        libc::signal(libc::SIGCONT, libc::SIG_DFL);
    }
    match behavior {
        ChildBehavior::IgnoreTerm => {
            // SAFETY: deterministic fixture behavior.
            unsafe {
                libc::signal(libc::SIGTERM, libc::SIG_IGN);
            }
        }
        ChildBehavior::ExitOnSignal | ChildBehavior::ExitImmediately => {}
    }
    // SAFETY: handler only stores an atomic flag.
    unsafe {
        libc::signal(
            libc::SIGINT,
            child_interrupt_handler as *const () as libc::sighandler_t,
        );
    }
    let ready = WireMessage {
        kind: MSG_READY,
        a: i64::from(unsafe { libc::getpid() }),
        b: i64::from(unsafe { libc::getsid(0) }),
        c: i64::from(unsafe { libc::getpgrp() }),
        ..WireMessage::default()
    };
    write_message(report_write, ready).unwrap();
    if behavior == ChildBehavior::ExitImmediately {
        unsafe { libc::_exit(17) }
    }
    loop {
        // SAFETY: pause blocks until a signal and has no memory preconditions.
        unsafe {
            libc::pause();
        }
        if CHILD_INTERRUPT_SEEN.swap(false, Ordering::AcqRel) {
            write_message(
                report_write,
                WireMessage {
                    kind: MSG_INTERRUPT,
                    ..WireMessage::default()
                },
            )
            .unwrap();
        }
    }
}

fn observe_child_exit_without_reap(pid: i32) -> Option<ExitEvidence> {
    let observed = observe_direct_child(pid).ok()??;
    Some(ExitEvidence {
        observed_wall_seconds: wall_seconds(),
        observed_monotonic_ns: monotonic_ns(),
        exit_code: observed.exit_code,
        exit_signal: observed.exit_signal,
        delivery_context: if observed.exit_signal.is_some() {
            DeliveryExitContext::Killed
        } else {
            DeliveryExitContext::Closed
        },
    })
}

fn reap_wrapper(active: &FakeActive, exit: &ExitEvidence, fail_delivery: bool) -> CleanupEvidence {
    write_byte(active.command_write, b'R').unwrap();
    let message = read_until_kind(active.report_read, MSG_REAP).unwrap();
    let mut wrapper_status = 0;
    // SAFETY: exact wrapper is a direct supervisor child.
    let wrapper_reaped =
        unsafe { libc::waitpid(active.identity.wrapper_pid, &mut wrapper_status, 0) }
            == active.identity.wrapper_pid;
    let child_reaped = message.a == 1;
    let bits = message.b;
    CleanupEvidence {
        exit: Some(exit.clone()),
        waitpid_reaped: wrapper_reaped && child_reaped,
        resources: ResourceCleanupEvidence {
            inject_stopped: bits & 0b0_0001 != 0,
            delivery_joined: bits & 0b0_0010 != 0 && !fail_delivery,
            pty_closed: bits & 0b0_0100 != 0,
            screen_released: bits & 0b0_1000 != 0,
            write_queue_empty: bits & 0b1_0000 != 0,
        },
        failure_kind: if fail_delivery {
            "delivery_join_timeout".to_string()
        } else {
            String::new()
        },
        failure_reason: if fail_delivery {
            "injected delivery cleanup failure".to_string()
        } else {
            String::new()
        },
    }
}

fn close_active_fds(active: &FakeActive) {
    if active.gate_write >= 0 {
        close_fd(active.gate_write);
    }
    close_fd(active.command_write);
    close_fd(active.report_read);
}

fn exit_to_message(exit: &ExitEvidence) -> WireMessage {
    WireMessage {
        kind: MSG_EXIT,
        a: i64::from(exit.exit_code.unwrap_or(-1)),
        b: i64::from(exit.exit_signal.unwrap_or(0)),
        c: exit.observed_monotonic_ns,
        d: exit.observed_wall_seconds as i64,
        e: i64::from(exit.delivery_context == DeliveryExitContext::Killed),
    }
}

fn message_to_exit(message: WireMessage) -> ExitEvidence {
    assert_eq!(message.kind, MSG_EXIT);
    ExitEvidence {
        observed_wall_seconds: message.d as u64,
        observed_monotonic_ns: message.c,
        exit_code: (message.a >= 0).then_some(message.a as i32),
        exit_signal: (message.b > 0).then_some(message.b as i32),
        delivery_context: if message.e == 1 {
            DeliveryExitContext::Killed
        } else {
            DeliveryExitContext::Closed
        },
    }
}

fn monotonic_ns() -> i64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) },
        0
    );
    value.tv_sec * 1_000_000_000 + value.tv_nsec
}

fn wall_seconds() -> u64 {
    // SAFETY: null requests seconds only.
    unsafe { libc::time(std::ptr::null_mut()) as u64 }
}

fn pidfd_open(pid: i32) -> io::Result<RawFd> {
    // SAFETY: pidfd_open takes scalar arguments and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd as RawFd)
    }
}

fn wait_pidfds_readable(pidfds: &[RawFd], timeout: Duration) -> bool {
    let mut descriptors: Vec<_> = pidfds
        .iter()
        .copied()
        .map(|fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    let deadline = monotonic_ns().saturating_add(
        timeout
            .as_nanos()
            .min(i64::MAX as u128)
            .try_into()
            .unwrap_or(i64::MAX),
    );
    loop {
        if descriptors
            .iter()
            .all(|descriptor| descriptor.revents & libc::POLLIN != 0)
        {
            return true;
        }
        let remaining_ns = deadline.saturating_sub(monotonic_ns());
        if remaining_ns <= 0 {
            return false;
        }
        let timeout_ms =
            ((remaining_ns + 999_999) / 1_000_000).clamp(1, i64::from(i32::MAX)) as i32;
        // SAFETY: descriptors is a live contiguous pollfd array.
        let result = unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                timeout_ms,
            )
        };
        if result == -1 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return false;
        }
    }
}

fn pipe() -> io::Result<(RawFd, RawFd)> {
    let mut descriptors = [-1; 2];
    // SAFETY: array has two writable descriptor slots.
    if unsafe { libc::pipe(descriptors.as_mut_ptr()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok((descriptors[0], descriptors[1]))
    }
}

fn set_nonblocking(fd: RawFd) {
    // SAFETY: fd is live and fcntl results are checked.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert_ne!(flags, -1);
    assert_ne!(
        unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        -1
    );
}

fn write_byte(fd: RawFd, byte: u8) -> io::Result<()> {
    write_all(fd, (&byte as *const u8).cast(), 1)
}

fn read_byte(fd: RawFd) -> io::Result<u8> {
    let mut byte = 0u8;
    read_exact(fd, (&mut byte as *mut u8).cast(), 1)?;
    Ok(byte)
}

fn write_message(fd: RawFd, message: WireMessage) -> io::Result<()> {
    write_all(
        fd,
        (&message as *const WireMessage).cast(),
        std::mem::size_of::<WireMessage>(),
    )
}

fn read_message(fd: RawFd) -> io::Result<WireMessage> {
    let mut message = MaybeUninit::<WireMessage>::uninit();
    read_exact(
        fd,
        message.as_mut_ptr().cast(),
        std::mem::size_of::<WireMessage>(),
    )?;
    // SAFETY: read_exact initialized every byte.
    Ok(unsafe { message.assume_init() })
}

fn try_read_message(fd: RawFd) -> io::Result<Option<WireMessage>> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: descriptor is initialized.
    let ready = unsafe { libc::poll(&mut descriptor, 1, 0) };
    if ready == -1 {
        return Err(io::Error::last_os_error());
    }
    if ready == 0 || descriptor.revents & libc::POLLIN == 0 {
        Ok(None)
    } else {
        read_message(fd).map(Some)
    }
}

fn read_until_kind(fd: RawFd, kind: i64) -> io::Result<WireMessage> {
    loop {
        let message = read_message(fd)?;
        if message.kind == kind {
            return Ok(message);
        }
    }
}

fn write_all(fd: RawFd, mut pointer: *const libc::c_void, mut length: usize) -> io::Result<()> {
    while length > 0 {
        // SAFETY: pointer references length readable bytes.
        let written = unsafe { libc::write(fd, pointer, length) };
        if written == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if written == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "pipe closed"));
        }
        length -= written as usize;
        // SAFETY: advances within the original readable allocation.
        pointer = unsafe { pointer.cast::<u8>().add(written as usize).cast() };
    }
    Ok(())
}

fn read_exact(fd: RawFd, mut pointer: *mut libc::c_void, mut length: usize) -> io::Result<()> {
    while length > 0 {
        // SAFETY: pointer references length writable bytes.
        let read = unsafe { libc::read(fd, pointer, length) };
        if read == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if read == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "pipe closed"));
        }
        length -= read as usize;
        // SAFETY: advances within the original writable allocation.
        pointer = unsafe { pointer.cast::<u8>().add(read as usize).cast() };
    }
    Ok(())
}

fn drain_fd(fd: RawFd) -> Vec<u8> {
    let mut buffer = [0u8; 4096];
    let mut output = Vec::new();
    loop {
        // SAFETY: buffer is writable.
        let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
        if read <= 0 {
            break;
        }
        output.extend_from_slice(&buffer[..read as usize]);
    }
    output
}

fn close_fd(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: duplicate closes are avoided by ownership discipline.
        unsafe {
            libc::close(fd);
        }
    }
}
