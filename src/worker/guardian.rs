//! Linux per-invocation process Guardian for native Claude roles.
//!
//! The Guardian is deliberately narrower than the task supervisor. It owns one
//! native process tree, uses Linux subreaper adoption to retain descendants that
//! escape their original session/process group, and reports lifecycle state
//! over one inherited `SOCK_SEQPACKET` socket. It never reads or forwards the
//! native process's stdin/stdout/stderr.

use super::environment::{ParentEnvironment, validate_claude_proxy_environment};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const INTERNAL_ENTRY: &str = "__hcom_internal_claude_guardian_v1";
const PROTOCOL_MAGIC: [u8; 4] = *b"HCG1";
const PROTOCOL_VERSION: u8 = 1;
const FRAME_BYTES: usize = 64;
const MAX_NATIVE_ARGUMENTS: usize = 4096;
const MAX_NATIVE_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_DIRECT_CHILDREN: usize = 4096;
const READY_TIMEOUT: Duration = Duration::from_secs(3);
const NATURAL_GRACE: Duration = Duration::from_millis(250);
const TERM_GRACE: Duration = Duration::from_millis(700);
const KILL_GRACE: Duration = Duration::from_millis(1200);
const DROP_CLEANUP_BUDGET: Duration = Duration::from_secs(3);
const LOOP_TICK: Duration = Duration::from_millis(10);

const EXIT_ORPHANED: i32 = 20;
const EXIT_NATIVE_FAILURE: i32 = 21;
const EXIT_CANCELED: i32 = 22;
const EXIT_TIMEOUT: i32 = 23;
const EXIT_PARENT_DEATH: i32 = 25;
const EXIT_PROTOCOL_FAILURE: i32 = 76;
const EXIT_INTERNAL_FAILURE: i32 = 90;

pub const GUARDIAN_LIFECYCLE_BOUNDARY: &str = "Guardian cleanup covers owned process descendants while the Guardian remains live; \
     external service-manager resources and unexpected Guardian death are outside this guarantee";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FrameKind {
    GuardianReady = 1,
    CleanupRequest = 2,
    NativeExited = 3,
    CleanupComplete = 4,
    CleanupFailed = 5,
    GuardianError = 6,
    CleanupAck = 7,
}

impl TryFrom<u8> for FrameKind {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::GuardianReady),
            2 => Ok(Self::CleanupRequest),
            3 => Ok(Self::NativeExited),
            4 => Ok(Self::CleanupComplete),
            5 => Ok(Self::CleanupFailed),
            6 => Ok(Self::GuardianError),
            7 => Ok(Self::CleanupAck),
            _ => bail!("Guardian frame has an unknown kind"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FailureClass {
    None = 0,
    Capability = 1,
    Protocol = 2,
    Process = 3,
    Cleanup = 4,
    Ownership = 5,
}

impl TryFrom<u8> for FailureClass {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Capability),
            2 => Ok(Self::Protocol),
            3 => Ok(Self::Process),
            4 => Ok(Self::Cleanup),
            5 => Ok(Self::Ownership),
            _ => bail!("Guardian frame has an unknown failure class"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GuardianMode {
    HeadlessWorker = 1,
    ForegroundArchitect = 2,
}

impl GuardianMode {
    fn argument(self) -> &'static str {
        match self {
            Self::HeadlessWorker => "headless",
            Self::ForegroundArchitect => "foreground",
        }
    }

    fn parse(value: &OsStr) -> Result<Self> {
        match value.as_bytes() {
            b"headless" => Ok(Self::HeadlessWorker),
            b"foreground" => Ok(Self::ForegroundArchitect),
            _ => bail!("invalid internal Guardian mode"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GuardianCleanupReason {
    NormalTeardown = 1,
    Cancel = 2,
    Timeout = 3,
    ParentDeath = 4,
    ProtocolFailure = 5,
}

impl TryFrom<u8> for GuardianCleanupReason {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::NormalTeardown),
            2 => Ok(Self::Cancel),
            3 => Ok(Self::Timeout),
            4 => Ok(Self::ParentDeath),
            5 => Ok(Self::ProtocolFailure),
            _ => bail!("Guardian frame has an invalid cleanup reason"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GuardianCleanupDisposition {
    Clean = 1,
    OrphanedDescendants = 2,
    NativeFailure = 3,
    Canceled = 4,
    TimedOut = 5,
    ParentDied = 6,
    ProtocolFailure = 7,
}

impl TryFrom<u8> for GuardianCleanupDisposition {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Clean),
            2 => Ok(Self::OrphanedDescendants),
            3 => Ok(Self::NativeFailure),
            4 => Ok(Self::Canceled),
            5 => Ok(Self::TimedOut),
            6 => Ok(Self::ParentDied),
            7 => Ok(Self::ProtocolFailure),
            _ => bail!("Guardian frame has an invalid cleanup disposition"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Frame {
    kind: FrameKind,
    sequence: u32,
    guardian_pid: u32,
    guardian_birth: u64,
    native_pid: u32,
    native_birth: u64,
    native_code: i32,
    native_signal: i32,
    disposition: u8,
    failure_class: FailureClass,
    forced_count: u32,
}

impl Frame {
    fn encode(self) -> [u8; FRAME_BYTES] {
        let mut bytes = [0u8; FRAME_BYTES];
        bytes[..4].copy_from_slice(&PROTOCOL_MAGIC);
        bytes[4] = PROTOCOL_VERSION;
        bytes[5] = self.kind as u8;
        bytes[6] = self.disposition;
        bytes[7] = self.failure_class as u8;
        put_u32(&mut bytes, 8, self.sequence);
        put_u32(&mut bytes, 12, self.guardian_pid);
        put_u64(&mut bytes, 16, self.guardian_birth);
        put_u32(&mut bytes, 24, self.native_pid);
        put_u64(&mut bytes, 32, self.native_birth);
        put_i32(&mut bytes, 40, self.native_code);
        put_i32(&mut bytes, 44, self.native_signal);
        put_u32(&mut bytes, 48, self.forced_count);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != FRAME_BYTES {
            bail!("Guardian frame has an invalid size");
        }
        if bytes[..4] != PROTOCOL_MAGIC || bytes[4] != PROTOCOL_VERSION {
            bail!("Guardian frame has an invalid protocol version");
        }
        if bytes[52..].iter().any(|byte| *byte != 0) {
            bail!("Guardian frame has nonzero reserved bytes");
        }
        Ok(Self {
            kind: FrameKind::try_from(bytes[5])?,
            sequence: get_u32(bytes, 8),
            guardian_pid: get_u32(bytes, 12),
            guardian_birth: get_u64(bytes, 16),
            native_pid: get_u32(bytes, 24),
            native_birth: get_u64(bytes, 32),
            native_code: get_i32(bytes, 40),
            native_signal: get_i32(bytes, 44),
            disposition: bytes[6],
            failure_class: FailureClass::try_from(bytes[7])?,
            forced_count: get_u32(bytes, 48),
        })
    }

    fn request(sequence: u32, reason: GuardianCleanupReason) -> Self {
        Self {
            kind: FrameKind::CleanupRequest,
            sequence,
            guardian_pid: 0,
            guardian_birth: 0,
            native_pid: 0,
            native_birth: 0,
            native_code: 0,
            native_signal: 0,
            disposition: reason as u8,
            failure_class: FailureClass::None,
            forced_count: 0,
        }
    }

    fn cleanup_ack(sequence: u32) -> Self {
        Self {
            kind: FrameKind::CleanupAck,
            sequence,
            guardian_pid: 0,
            guardian_birth: 0,
            native_pid: 0,
            native_birth: 0,
            native_code: 0,
            native_signal: 0,
            disposition: 0,
            failure_class: FailureClass::None,
            forced_count: 0,
        }
    }
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i32(bytes: &mut [u8], offset: usize, value: i32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed frame"))
}

fn get_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed frame"))
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed frame"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardianProcessIdentity {
    pub pid: u32,
    pub birth: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardianReady {
    pub guardian: GuardianProcessIdentity,
    pub native: GuardianProcessIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardianCompletion {
    pub native_code: Option<i32>,
    pub native_signal: Option<i32>,
    pub disposition: GuardianCleanupDisposition,
    pub forced_signal_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardianPoll {
    Running,
    CleanupPending,
    Complete(GuardianCompletion),
    OwnershipLost(String),
}

#[derive(Debug)]
pub enum GuardianSpawnFailure {
    Reaped(anyhow::Error),
    CleanupPending {
        detail: String,
        handle: Box<GuardianHandle>,
    },
    OwnershipLost(String),
}

impl std::fmt::Display for GuardianSpawnFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reaped(error) => write!(formatter, "{error}"),
            Self::CleanupPending { detail, .. } | Self::OwnershipLost(detail) => {
                formatter.write_str(detail)
            }
        }
    }
}

impl std::error::Error for GuardianSpawnFailure {}

/// Builder for one native command owned by a per-invocation Guardian.
pub struct GuardedCommand {
    guardian_executable: PathBuf,
    native_program: OsString,
    native_args: Vec<OsString>,
    mode: GuardianMode,
    cwd: Option<PathBuf>,
    clear_environment: bool,
    environment: Vec<(OsString, OsString)>,
    stdin: Option<Stdio>,
    stdout: Option<Stdio>,
    stderr: Option<Stdio>,
    require_claude_proxy: bool,
}

impl GuardedCommand {
    pub fn new(program: impl Into<OsString>) -> Result<Self> {
        Self::with_guardian_executable(std::env::current_exe()?, program)
    }

    #[doc(hidden)]
    pub fn with_guardian_executable(
        guardian_executable: impl Into<PathBuf>,
        program: impl Into<OsString>,
    ) -> Result<Self> {
        let guardian_executable = guardian_executable.into();
        if !guardian_executable.is_absolute() {
            bail!("Guardian executable must be an absolute path");
        }
        let native_program = program.into();
        if native_program.as_bytes().is_empty() {
            bail!("native Guardian command cannot be empty");
        }
        Ok(Self {
            guardian_executable,
            native_program,
            native_args: Vec::new(),
            mode: GuardianMode::HeadlessWorker,
            cwd: None,
            clear_environment: false,
            environment: Vec::new(),
            stdin: None,
            stdout: None,
            stderr: None,
            require_claude_proxy: false,
        })
    }

    pub fn arg(&mut self, argument: impl Into<OsString>) -> &mut Self {
        self.native_args.push(argument.into());
        self
    }

    pub fn args<I, S>(&mut self, arguments: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.native_args
            .extend(arguments.into_iter().map(Into::into));
        self
    }

    pub fn mode(&mut self, mode: GuardianMode) -> &mut Self {
        self.mode = mode;
        self
    }

    pub fn current_dir(&mut self, cwd: impl Into<PathBuf>) -> &mut Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn env_clear(&mut self) -> &mut Self {
        self.clear_environment = true;
        self
    }

    pub fn env(&mut self, name: impl Into<OsString>, value: impl Into<OsString>) -> &mut Self {
        self.environment.push((name.into(), value.into()));
        self
    }

    pub fn envs<I, K, V>(&mut self, values: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        self.environment.extend(
            values
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    pub fn stdin(&mut self, stdin: Stdio) -> &mut Self {
        self.stdin = Some(stdin);
        self
    }

    pub fn stdout(&mut self, stdout: Stdio) -> &mut Self {
        self.stdout = Some(stdout);
        self
    }

    pub fn stderr(&mut self, stderr: Stdio) -> &mut Self {
        self.stderr = Some(stderr);
        self
    }

    pub fn require_claude_proxy(&mut self) -> &mut Self {
        self.require_claude_proxy = true;
        self
    }

    pub fn spawn(&mut self) -> std::result::Result<GuardianHandle, GuardianSpawnFailure> {
        if let Err(error) = validate_command_shape(&self.native_program, &self.native_args) {
            return Err(GuardianSpawnFailure::Reaped(error));
        }
        if let Err(error) = probe_runtime_capabilities() {
            return Err(GuardianSpawnFailure::Reaped(error));
        }
        self.spawn_after_preflight()
    }

    fn spawn_after_preflight(
        &mut self,
    ) -> std::result::Result<GuardianHandle, GuardianSpawnFailure> {
        let (parent_control, child_control) =
            seqpacket_socketpair().map_err(GuardianSpawnFailure::Reaped)?;
        let control_fd = child_control.as_raw_fd();
        let expected_parent = std::process::id();
        let mut command = Command::new(&self.guardian_executable);
        command
            .arg(INTERNAL_ENTRY)
            .arg("--control-fd")
            .arg(control_fd.to_string())
            .arg("--expected-parent")
            .arg(expected_parent.to_string())
            .arg("--mode")
            .arg(self.mode.argument())
            .arg("--environment-policy")
            .arg(if self.require_claude_proxy {
                "claude-exact-proxy"
            } else {
                "inherit"
            })
            .arg("--")
            .arg(&self.native_program)
            .args(&self.native_args);
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        if self.clear_environment {
            command.env_clear();
        }
        for (name, value) in &self.environment {
            command.env(name, value);
        }
        command
            .stdin(self.stdin.take().unwrap_or_else(Stdio::inherit))
            .stdout(self.stdout.take().unwrap_or_else(Stdio::inherit))
            .stderr(self.stderr.take().unwrap_or_else(Stdio::inherit));
        // SAFETY: after fork this closure only calls fcntl on one inherited
        // descriptor. No allocation or non-async-signal-safe Rust operation is
        // performed.
        unsafe {
            command.pre_exec(move || clear_close_on_exec(control_fd));
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return Err(GuardianSpawnFailure::Reaped(
                    anyhow!(error).context("failed to spawn internal Guardian"),
                ));
            }
        };
        drop(child_control);
        let guardian_snapshot = match read_process_snapshot(child.id()) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                let _ = child.wait();
                return Err(GuardianSpawnFailure::OwnershipLost(
                    "Guardian exited before its process identity was captured".into(),
                ));
            }
            Err(error) => {
                let _ = child.wait();
                return Err(GuardianSpawnFailure::OwnershipLost(bounded_detail(
                    &error.to_string(),
                )));
            }
        };
        let guardian_pidfd = match open_pidfd(guardian_snapshot.pid) {
            Ok(pidfd) => pidfd,
            Err(error) => {
                let _ = child.wait();
                return Err(GuardianSpawnFailure::OwnershipLost(bounded_detail(
                    &error.to_string(),
                )));
            }
        };
        let mut handle = GuardianHandle {
            child: Some(child),
            pidfd: guardian_pidfd,
            control: parent_control,
            ready: None,
            guardian: GuardianProcessIdentity {
                pid: guardian_snapshot.pid,
                birth: guardian_snapshot.birth,
            },
            next_guardian_sequence: 1,
            next_request_sequence: 1,
            native_exit: None,
            cleanup: None,
            cleanup_failed: false,
            ownership_lost: None,
            pre_spawn_failure: None,
            cleanup_requested: false,
        };
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match handle.receive_ready() {
                Ok(Some(ready)) => {
                    handle.ready = Some(ready);
                    return Ok(handle);
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(LOOP_TICK);
                }
                Ok(None) => {
                    let detail = "Guardian did not publish readiness before the bounded deadline";
                    return fail_spawn_after_guardian_start(handle, detail);
                }
                Err(error) => {
                    return fail_spawn_after_guardian_start(handle, &error.to_string());
                }
            }
        }
    }
}

fn fail_spawn_after_guardian_start(
    mut handle: GuardianHandle,
    detail: &str,
) -> std::result::Result<GuardianHandle, GuardianSpawnFailure> {
    let detail = bounded_detail(detail);
    if handle.pre_spawn_failure.is_some() {
        let deadline = Instant::now() + READY_TIMEOUT;
        while !pidfd_is_ready(&handle.pidfd).unwrap_or(false) && Instant::now() < deadline {
            thread::sleep(LOOP_TICK);
        }
        if pidfd_is_ready(&handle.pidfd).unwrap_or(false) {
            if let Some(child) = handle.child.as_mut() {
                let _ = child.wait();
            }
            handle.child = None;
            return Err(GuardianSpawnFailure::Reaped(anyhow!(detail)));
        }
        return Err(GuardianSpawnFailure::CleanupPending {
            detail,
            handle: Box::new(handle),
        });
    }
    match handle.terminate_and_reap(
        GuardianCleanupReason::ProtocolFailure,
        TERM_GRACE + KILL_GRACE,
    ) {
        Ok(_) => Err(GuardianSpawnFailure::Reaped(anyhow!(detail))),
        Err(GuardianHandleFailure::CleanupPending(_)) => {
            Err(GuardianSpawnFailure::CleanupPending {
                detail,
                handle: Box::new(handle),
            })
        }
        Err(GuardianHandleFailure::OwnershipLost(lost)) => {
            Err(GuardianSpawnFailure::OwnershipLost(lost))
        }
    }
}

#[derive(Debug)]
pub enum GuardianHandleFailure {
    CleanupPending(String),
    OwnershipLost(String),
}

impl std::fmt::Display for GuardianHandleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CleanupPending(detail) | Self::OwnershipLost(detail) => {
                formatter.write_str(detail)
            }
        }
    }
}

impl std::error::Error for GuardianHandleFailure {}

pub struct GuardianHandle {
    child: Option<Child>,
    pidfd: OwnedFd,
    control: OwnedFd,
    ready: Option<GuardianReady>,
    guardian: GuardianProcessIdentity,
    next_guardian_sequence: u32,
    next_request_sequence: u32,
    native_exit: Option<(Option<i32>, Option<i32>)>,
    cleanup: Option<GuardianCompletion>,
    cleanup_failed: bool,
    ownership_lost: Option<String>,
    pre_spawn_failure: Option<String>,
    cleanup_requested: bool,
}

impl std::fmt::Debug for GuardianHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GuardianHandle")
            .field("guardian", &self.guardian)
            .field("ready", &self.ready)
            .field("cleanup_requested", &self.cleanup_requested)
            .field("cleanup_failed", &self.cleanup_failed)
            .field("ownership_lost", &self.ownership_lost)
            .finish_non_exhaustive()
    }
}

impl GuardianHandle {
    pub fn ready(&self) -> &GuardianReady {
        self.ready
            .as_ref()
            .expect("a published GuardianHandle is ready")
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.as_mut().and_then(|child| child.stdin.take())
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.as_mut().and_then(|child| child.stdout.take())
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.as_mut().and_then(|child| child.stderr.take())
    }

    pub fn request_cleanup(&mut self, reason: GuardianCleanupReason) -> Result<()> {
        if self.cleanup.is_some() || self.ownership_lost.is_some() {
            return Ok(());
        }
        let frame = Frame::request(self.next_request_sequence, reason);
        self.next_request_sequence = self
            .next_request_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("Guardian request sequence overflow"))?;
        send_frame(&self.control, frame)?;
        self.cleanup_requested = true;
        self.cleanup_failed = false;
        Ok(())
    }

    pub fn try_wait(&mut self) -> GuardianPoll {
        if let Some(detail) = &self.ownership_lost {
            return GuardianPoll::OwnershipLost(detail.clone());
        }
        if let Some(completion) = self.cleanup.clone() {
            return match self.reap_completed_guardian() {
                Ok(true) => GuardianPoll::Complete(completion),
                Ok(false) => GuardianPoll::CleanupPending,
                Err(error) => self.mark_ownership_lost(&error.to_string()),
            };
        }
        loop {
            match receive_frame(&self.control) {
                Ok(Some(frame)) => {
                    if let Err(error) = self.accept_runtime_frame(frame) {
                        let _ = self.request_cleanup(GuardianCleanupReason::ProtocolFailure);
                        return self.mark_ownership_lost(&format!(
                            "Guardian protocol identity was lost: {error}"
                        ));
                    }
                    if self.cleanup.is_some() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let exit = self
                        .child
                        .as_mut()
                        .and_then(|child| child.try_wait().ok().flatten())
                        .map(exit_status_detail);
                    if exit.is_some() {
                        self.child = None;
                    }
                    return self.mark_ownership_lost(&format!(
                        "Guardian control transport failed: {error}{}",
                        exit.map(|status| format!(" ({status})"))
                            .unwrap_or_default()
                    ));
                }
            }
        }
        if self.cleanup.is_some() {
            return self.try_wait();
        }
        match pidfd_is_ready(&self.pidfd) {
            Ok(true) => {
                let detail = if self.cleanup_failed {
                    "Guardian exited after reporting cleanup failure"
                } else {
                    "Guardian exited before validated CleanupComplete"
                };
                self.mark_ownership_lost(detail)
            }
            Ok(false) if self.cleanup_failed || self.cleanup_requested => {
                GuardianPoll::CleanupPending
            }
            Ok(false) => GuardianPoll::Running,
            Err(error) => {
                self.mark_ownership_lost(&format!("Guardian pidfd observation failed: {error}"))
            }
        }
    }

    pub fn terminate_and_reap(
        &mut self,
        reason: GuardianCleanupReason,
        budget: Duration,
    ) -> std::result::Result<GuardianCompletion, GuardianHandleFailure> {
        if self.cleanup.is_none()
            && (self.cleanup_failed || !self.cleanup_requested)
            && let Err(error) = self.request_cleanup(reason)
        {
            return Err(GuardianHandleFailure::OwnershipLost(bounded_detail(
                &error.to_string(),
            )));
        }
        let deadline = Instant::now() + budget;
        loop {
            match self.try_wait() {
                GuardianPoll::Complete(completion) => return Ok(completion),
                GuardianPoll::OwnershipLost(detail) => {
                    return Err(GuardianHandleFailure::OwnershipLost(detail));
                }
                GuardianPoll::CleanupPending | GuardianPoll::Running => {}
            }
            if Instant::now() >= deadline {
                return Err(GuardianHandleFailure::CleanupPending(
                    "Guardian cleanup remains pending after one bounded attempt".into(),
                ));
            }
            thread::sleep(LOOP_TICK);
        }
    }

    fn receive_ready(&mut self) -> Result<Option<GuardianReady>> {
        let frame = match receive_frame(&self.control) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                if pidfd_is_ready(&self.pidfd)? {
                    let status = self
                        .child
                        .as_mut()
                        .expect("Guardian child exists before ready")
                        .wait()?;
                    self.child = None;
                    bail!(
                        "Guardian exited before readiness ({})",
                        exit_status_detail(status)
                    );
                }
                return Ok(None);
            }
            Err(error) if !pidfd_is_ready(&self.pidfd)? => return Err(error),
            Err(error) => {
                let status = self
                    .child
                    .as_mut()
                    .expect("Guardian child exists before ready")
                    .wait()?;
                self.child = None;
                bail!(
                    "Guardian transport ended before readiness: {error} ({})",
                    exit_status_detail(status)
                );
            }
        };
        self.validate_frame_identity(&frame)?;
        if frame.kind == FrameKind::GuardianError {
            if frame.native_pid == 0
                && frame.native_birth == 0
                && matches!(
                    frame.failure_class,
                    FailureClass::Capability | FailureClass::Process
                )
            {
                self.pre_spawn_failure = Some(format!(
                    "Guardian rejected native spawn during {:?} preflight",
                    frame.failure_class
                ));
            }
            bail!(
                "Guardian failed its pre-spawn lifecycle capability check ({:?})",
                frame.failure_class
            );
        }
        if frame.kind != FrameKind::GuardianReady {
            bail!("Guardian first frame was not readiness");
        }
        if frame.native_pid <= 1 {
            bail!("Guardian readiness did not identify a native process");
        }
        if frame.native_birth == 0 {
            bail!("Guardian readiness omitted native birth identity");
        }
        if frame.sequence != 1
            || frame.native_code != 0
            || frame.native_signal != 0
            || frame.disposition != 0
            || frame.failure_class != FailureClass::None
            || frame.forced_count != 0
        {
            bail!("Guardian readiness frame violated the protocol");
        }
        let Some(native_snapshot) = read_process_snapshot(frame.native_pid)? else {
            // A very short-lived native child may already be a reaped process
            // while its exact ready/exit/cleanup frames remain queued. The
            // Guardian bound it before publishing readiness, so the immutable
            // frame identity is sufficient once the Guardian itself is exact.
            self.next_guardian_sequence = 2;
            return Ok(Some(GuardianReady {
                guardian: self.guardian.clone(),
                native: GuardianProcessIdentity {
                    pid: frame.native_pid,
                    birth: frame.native_birth,
                },
            }));
        };
        // SAFETY: geteuid has no preconditions.
        let expected_uid = unsafe { libc::geteuid() };
        if native_snapshot.uid != expected_uid
            || native_snapshot.ppid != self.guardian.pid
            || native_snapshot.birth != frame.native_birth
        {
            bail!("Guardian readiness did not bind an exact direct native child");
        }
        self.next_guardian_sequence = 2;
        Ok(Some(GuardianReady {
            guardian: self.guardian.clone(),
            native: GuardianProcessIdentity {
                pid: frame.native_pid,
                birth: frame.native_birth,
            },
        }))
    }

    fn accept_runtime_frame(&mut self, frame: Frame) -> Result<()> {
        self.validate_frame_identity(&frame)?;
        if frame.sequence != self.next_guardian_sequence {
            bail!("Guardian frame sequence was duplicated or skipped");
        }
        self.next_guardian_sequence = self
            .next_guardian_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("Guardian frame sequence overflow"))?;
        let ready = self
            .ready
            .as_ref()
            .ok_or_else(|| anyhow!("Guardian runtime frame preceded readiness"))?;
        if frame.native_pid != ready.native.pid || frame.native_birth != ready.native.birth {
            bail!("Guardian runtime frame changed native process identity");
        }
        match frame.kind {
            FrameKind::NativeExited => {
                if self.native_exit.is_some()
                    || self.cleanup.is_some()
                    || frame.disposition != 0
                    || frame.failure_class != FailureClass::None
                    || frame.forced_count != 0
                {
                    bail!("Guardian native-exit frame violated the protocol");
                }
                let code = (frame.native_code >= 0).then_some(frame.native_code);
                let signal = (frame.native_signal > 0).then_some(frame.native_signal);
                if code.is_none() == signal.is_none() {
                    bail!("Guardian native-exit status was ambiguous");
                }
                self.native_exit = Some((code, signal));
            }
            FrameKind::CleanupFailed => {
                if self.cleanup.is_some()
                    || frame.failure_class != FailureClass::Cleanup
                    || frame.disposition != 0
                {
                    bail!("Guardian cleanup-failed frame violated the protocol");
                }
                self.cleanup_failed = true;
                self.cleanup_requested = false;
            }
            FrameKind::CleanupComplete => {
                let (native_code, native_signal) = self
                    .native_exit
                    .ok_or_else(|| anyhow!("Guardian cleanup preceded native exit"))?;
                if self.cleanup.is_some() || frame.failure_class != FailureClass::None {
                    bail!("Guardian cleanup-complete frame was duplicated or malformed");
                }
                let disposition = GuardianCleanupDisposition::try_from(frame.disposition)?;
                self.cleanup = Some(GuardianCompletion {
                    native_code,
                    native_signal,
                    disposition,
                    forced_signal_count: frame.forced_count,
                });
                self.cleanup_failed = false;
                let acknowledgement = Frame::cleanup_ack(self.next_request_sequence);
                self.next_request_sequence = self
                    .next_request_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("Guardian request sequence overflow"))?;
                send_frame(&self.control, acknowledgement)
                    .context("failed to acknowledge Guardian cleanup completion")?;
            }
            FrameKind::GuardianError => {
                bail!("Guardian reported an internal failure after native spawn");
            }
            FrameKind::GuardianReady | FrameKind::CleanupRequest | FrameKind::CleanupAck => {
                bail!("Guardian sent an invalid runtime frame kind");
            }
        }
        Ok(())
    }

    fn validate_frame_identity(&self, frame: &Frame) -> Result<()> {
        if frame.guardian_pid != self.guardian.pid || frame.guardian_birth != self.guardian.birth {
            bail!("Guardian frame identity does not match the owned process");
        }
        Ok(())
    }

    fn reap_completed_guardian(&mut self) -> Result<bool> {
        if self.child.is_none() {
            return Ok(true);
        }
        if !pidfd_is_ready(&self.pidfd)? {
            return Ok(false);
        }
        let status = self
            .child
            .as_mut()
            .expect("checked above")
            .wait()
            .context("failed to reap completed Guardian")?;
        self.child = None;
        let expected = self
            .cleanup
            .as_ref()
            .map(expected_guardian_exit_code)
            .ok_or_else(|| anyhow!("Guardian exit preceded cleanup result"))?;
        if status.code() != Some(expected) {
            bail!(
                "Guardian exit status disagreed with cleanup disposition ({})",
                exit_status_detail(status)
            );
        }
        Ok(true)
    }

    fn mark_ownership_lost(&mut self, detail: &str) -> GuardianPoll {
        let detail = bounded_detail(detail);
        self.ownership_lost = Some(detail.clone());
        if let Some(child) = self.child.as_mut()
            && pidfd_is_ready(&self.pidfd).unwrap_or(false)
        {
            let _ = child.wait();
            self.child = None;
        }
        GuardianPoll::OwnershipLost(detail)
    }
}

impl Drop for GuardianHandle {
    fn drop(&mut self) {
        if self.child.is_none() || self.cleanup.is_some() || self.ownership_lost.is_some() {
            return;
        }
        let _ = self.terminate_and_reap(GuardianCleanupReason::NormalTeardown, DROP_CLEANUP_BUDGET);
        // Never SIGKILL the Guardian as an escalation. Closing the private
        // control socket makes a still-live Guardian take its parent-death
        // cleanup path; its own PDEATHSIG covers actual parent termination.
    }
}

fn expected_guardian_exit_code(completion: &GuardianCompletion) -> i32 {
    match completion.disposition {
        GuardianCleanupDisposition::Clean => 0,
        GuardianCleanupDisposition::OrphanedDescendants => EXIT_ORPHANED,
        GuardianCleanupDisposition::NativeFailure => EXIT_NATIVE_FAILURE,
        GuardianCleanupDisposition::Canceled => EXIT_CANCELED,
        GuardianCleanupDisposition::TimedOut => EXIT_TIMEOUT,
        GuardianCleanupDisposition::ParentDied => EXIT_PARENT_DEATH,
        GuardianCleanupDisposition::ProtocolFailure => EXIT_PROTOCOL_FAILURE,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupRegistryInterlock {
    Ready,
    Pending { claims: usize },
    Poisoned { detail: String },
}

#[derive(Clone, Default)]
pub struct GuardianCleanupRegistry {
    inner: Arc<Mutex<CleanupRegistryState>>,
}

#[derive(Default)]
struct CleanupRegistryState {
    next_claim: u64,
    claims: BTreeMap<u64, GuardianHandle>,
    poisoned: Option<String>,
    #[cfg(test)]
    synthetic_pending: usize,
}

impl GuardianCleanupRegistry {
    pub fn register(&self, mut handle: GuardianHandle) -> Result<u64> {
        let request_failure = handle
            .request_cleanup(GuardianCleanupReason::NormalTeardown)
            .err()
            .map(|error| bounded_detail(&error.to_string()));
        let mut state = self
            .inner
            .lock()
            .expect("Guardian cleanup registry poisoned");
        state.next_claim = state
            .next_claim
            .checked_add(1)
            .ok_or_else(|| anyhow!("Guardian cleanup claim sequence overflow"))?;
        let claim = state.next_claim;
        if state.claims.insert(claim, handle).is_some() {
            bail!("Guardian cleanup claim identity collided");
        }
        if state.poisoned.is_none()
            && let Some(detail) = request_failure
        {
            state.poisoned = Some(bounded_detail(&format!(
                "Guardian claim {claim} could not begin cleanup: {detail}; {GUARDIAN_LIFECYCLE_BOUNDARY}"
            )));
        }
        Ok(claim)
    }

    pub fn record_ownership_lost(&self, detail: &str) {
        let mut state = self
            .inner
            .lock()
            .expect("Guardian cleanup registry poisoned");
        if state.poisoned.is_none() {
            state.poisoned = Some(bounded_detail(&format!(
                "{}; {GUARDIAN_LIFECYCLE_BOUNDARY}",
                bounded_detail(detail)
            )));
        }
    }

    pub fn retry_pending(&self) -> CleanupRegistryInterlock {
        let mut state = self
            .inner
            .lock()
            .expect("Guardian cleanup registry poisoned");
        refresh_registry(&mut state);
        registry_interlock(&state)
    }

    pub fn interlock(&self) -> CleanupRegistryInterlock {
        self.retry_pending()
    }

    pub fn ensure_available(&self) -> Result<()> {
        match self.retry_pending() {
            CleanupRegistryInterlock::Ready => Ok(()),
            CleanupRegistryInterlock::Pending { claims } => bail!(
                "Claude lifecycle cleanup is pending for {claims} owned Guardian claim(s); a new run or worker cannot start"
            ),
            CleanupRegistryInterlock::Poisoned { detail } => bail!(
                "Claude lifecycle ownership lost; this foreground session cannot start another run or worker: {detail}"
            ),
        }
    }

    pub fn cleanup_for(&self, budget: Duration) -> CleanupRegistryInterlock {
        let deadline = Instant::now() + budget;
        loop {
            let interlock = self.retry_pending();
            if !matches!(interlock, CleanupRegistryInterlock::Pending { .. })
                || Instant::now() >= deadline
            {
                return interlock;
            }
            thread::sleep(LOOP_TICK);
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_pending_for_test(&self) {
        self.inner
            .lock()
            .expect("Guardian cleanup registry poisoned")
            .synthetic_pending += 1;
    }

    #[cfg(test)]
    pub(crate) fn complete_pending_for_test(&self) {
        let mut state = self
            .inner
            .lock()
            .expect("Guardian cleanup registry poisoned");
        state.synthetic_pending = state.synthetic_pending.saturating_sub(1);
    }

    #[cfg(test)]
    pub(crate) fn poison_for_test(&self, detail: &str) {
        self.inner
            .lock()
            .expect("Guardian cleanup registry poisoned")
            .poisoned = Some(bounded_detail(detail));
    }
}

fn refresh_registry(state: &mut CleanupRegistryState) {
    let mut complete = Vec::new();
    let mut poison = None;
    for (claim, handle) in &mut state.claims {
        if handle.cleanup_failed {
            let _ = handle.request_cleanup(GuardianCleanupReason::NormalTeardown);
        }
        match handle.try_wait() {
            GuardianPoll::Complete(_) => complete.push(*claim),
            GuardianPoll::OwnershipLost(detail) => {
                poison = Some(format!(
                    "Guardian claim {claim} lost exact lifecycle ownership: {detail}; {GUARDIAN_LIFECYCLE_BOUNDARY}"
                ));
            }
            GuardianPoll::Running | GuardianPoll::CleanupPending => {}
        }
    }
    for claim in complete {
        state.claims.remove(&claim);
    }
    if state.poisoned.is_none() {
        state.poisoned = poison.map(|detail| bounded_detail(&detail));
    }
}

fn registry_interlock(state: &CleanupRegistryState) -> CleanupRegistryInterlock {
    if let Some(detail) = &state.poisoned {
        return CleanupRegistryInterlock::Poisoned {
            detail: detail.clone(),
        };
    }
    let pending = state.claims.len() + {
        #[cfg(test)]
        {
            state.synthetic_pending
        }
        #[cfg(not(test))]
        {
            0
        }
    };
    if pending == 0 {
        CleanupRegistryInterlock::Ready
    } else {
        CleanupRegistryInterlock::Pending { claims: pending }
    }
}

/// Recognize and run the private same-binary Guardian entry.
///
/// Unknown argv returns `None`; a recognized internal entry always returns a
/// process exit code and never writes to stdout/stderr.
pub fn run_internal_entry(arguments: impl Iterator<Item = OsString>) -> Option<i32> {
    let arguments: Vec<OsString> = arguments.collect();
    if arguments
        .first()
        .map(OsString::as_os_str)
        .map(OsStr::as_bytes)
        != Some(INTERNAL_ENTRY.as_bytes())
    {
        return None;
    }
    Some(run_internal_entry_inner(&arguments[1..]).unwrap_or(EXIT_INTERNAL_FAILURE))
}

fn run_internal_entry_inner(arguments: &[OsString]) -> Result<i32> {
    let parsed = InternalArguments::parse(arguments)?;
    let control_fd = parsed.control_fd;
    // SAFETY: the hidden entry is the unique owner of the inherited descriptor.
    let control = unsafe { OwnedFd::from_raw_fd(control_fd) };
    let mut native_spawned = false;
    match guardian_main(&control, parsed, &mut native_spawned) {
        Ok(exit) => Ok(exit),
        Err(error) => {
            let identity = guardian_identity().ok();
            let _ = send_frame(
                &control,
                Frame {
                    kind: FrameKind::GuardianError,
                    sequence: 1,
                    guardian_pid: identity.as_ref().map_or(0, |value| value.pid),
                    guardian_birth: identity.as_ref().map_or(0, |value| value.birth),
                    native_pid: 0,
                    native_birth: 0,
                    native_code: 0,
                    native_signal: 0,
                    disposition: 0,
                    failure_class: if native_spawned {
                        FailureClass::Ownership
                    } else if error.to_string().contains("capability") {
                        FailureClass::Capability
                    } else {
                        FailureClass::Process
                    },
                    forced_count: 0,
                },
            );
            Ok(EXIT_INTERNAL_FAILURE)
        }
    }
}

struct InternalArguments {
    control_fd: RawFd,
    expected_parent: u32,
    mode: GuardianMode,
    require_claude_proxy: bool,
    native_program: OsString,
    native_args: Vec<OsString>,
}

impl InternalArguments {
    fn parse(arguments: &[OsString]) -> Result<Self> {
        let separator = arguments
            .iter()
            .position(|argument| argument.as_bytes() == b"--")
            .ok_or_else(|| anyhow!("internal Guardian command separator is missing"))?;
        let metadata = &arguments[..separator];
        let command = &arguments[separator + 1..];
        if command.is_empty() {
            bail!("internal Guardian native command is missing");
        }
        let mut control_fd = None;
        let mut expected_parent = None;
        let mut mode = None;
        let mut require_claude_proxy = None;
        let mut index = 0;
        while index < metadata.len() {
            let flag = metadata[index].as_bytes();
            let value = metadata
                .get(index + 1)
                .ok_or_else(|| anyhow!("internal Guardian flag value is missing"))?;
            match flag {
                b"--control-fd" if control_fd.is_none() => {
                    control_fd = Some(parse_raw_fd(value)?);
                }
                b"--expected-parent" if expected_parent.is_none() => {
                    expected_parent = Some(parse_u32(value, "expected parent")?);
                }
                b"--mode" if mode.is_none() => {
                    mode = Some(GuardianMode::parse(value)?);
                }
                b"--environment-policy" if require_claude_proxy.is_none() => {
                    require_claude_proxy = Some(match value.as_bytes() {
                        b"inherit" => false,
                        b"claude-exact-proxy" => true,
                        _ => bail!("internal Guardian environment policy is invalid"),
                    });
                }
                _ => bail!("internal Guardian metadata is invalid or duplicated"),
            }
            index += 2;
        }
        let native_program = command[0].clone();
        let native_args = command[1..].to_vec();
        validate_command_shape(&native_program, &native_args)?;
        Ok(Self {
            control_fd: control_fd.ok_or_else(|| anyhow!("internal control fd is missing"))?,
            expected_parent: expected_parent
                .ok_or_else(|| anyhow!("internal expected parent is missing"))?,
            mode: mode.ok_or_else(|| anyhow!("internal Guardian mode is missing"))?,
            require_claude_proxy: require_claude_proxy
                .ok_or_else(|| anyhow!("internal Guardian environment policy is missing"))?,
            native_program,
            native_args,
        })
    }
}

fn parse_raw_fd(value: &OsStr) -> Result<RawFd> {
    let value = std::str::from_utf8(value.as_bytes()).context("control fd is not UTF-8")?;
    let parsed = value.parse::<i32>().context("control fd is invalid")?;
    if parsed < 3 {
        bail!("control fd is outside the private inherited range");
    }
    Ok(parsed)
}

fn parse_u32(value: &OsStr, label: &str) -> Result<u32> {
    let value =
        std::str::from_utf8(value.as_bytes()).with_context(|| format!("{label} is not UTF-8"))?;
    let value = value
        .parse::<u32>()
        .with_context(|| format!("{label} is invalid"))?;
    if value <= 1 {
        bail!("{label} is outside the process range");
    }
    Ok(value)
}

fn guardian_main(
    control: &OwnedFd,
    parsed: InternalArguments,
    native_spawned: &mut bool,
) -> Result<i32> {
    configure_guardian_process(parsed.expected_parent, parsed.mode)?;
    verify_seqpacket(control)?;
    probe_runtime_capabilities()?;
    if parsed.require_claude_proxy {
        let environment = ParentEnvironment::capture_current()
            .context("Guardian could not capture the Claude launch environment")?;
        validate_claude_proxy_environment(&environment)
            .context("Guardian rejected the Claude launch environment")?;
    }
    set_close_on_exec(control.as_raw_fd())?;

    let signal_fd = create_signal_fd()?;
    let mut command = Command::new(&parsed.native_program);
    command
        .args(&parsed.native_args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let headless = parsed.mode == GuardianMode::HeadlessWorker;
    // SAFETY: the closure only resets signal state and, in headless mode,
    // creates one process group. Those are async-signal-safe libc operations.
    unsafe {
        command.pre_exec(move || configure_native_child(headless));
    }
    let mut native = command
        .spawn()
        .context("failed to spawn the native Guardian child")?;
    *native_spawned = true;
    retain_post_spawn_ownership(&mut native, |native| {
        if headless {
            // The child also calls setpgid. Repeating it closes the fork/exec race;
            // EACCES means exec won after the child established the requested group.
            // SAFETY: setpgid only targets the still-unreaped direct child.
            let result =
                unsafe { libc::setpgid(native.id() as libc::pid_t, native.id() as libc::pid_t) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EACCES) {
                return Err(std::io::Error::last_os_error())
                    .context("failed to bind native child process group");
            }
        }
        let native_snapshot = read_process_snapshot(native.id())?
            .ok_or_else(|| anyhow!("native child identity unavailable"))?;
        // SAFETY: geteuid/getpid have no preconditions.
        if native_snapshot.uid != unsafe { libc::geteuid() }
            || native_snapshot.ppid != unsafe { libc::getpid() as u32 }
        {
            bail!("native child was not an exact current-user direct child");
        }
        let native_pidfd = open_pidfd(native_snapshot.pid)?;
        let guardian = guardian_identity()?;
        let mut sequence = 1u32;
        send_frame(
            control,
            Frame {
                kind: FrameKind::GuardianReady,
                sequence,
                guardian_pid: guardian.pid,
                guardian_birth: guardian.birth,
                native_pid: native_snapshot.pid,
                native_birth: native_snapshot.birth,
                native_code: 0,
                native_signal: 0,
                disposition: 0,
                failure_class: FailureClass::None,
                forced_count: 0,
            },
        )?;
        sequence += 1;

        let mut lifecycle = GuardianLifecycle::new(native_snapshot, native_pidfd);
        let mut reason = GuardianCleanupReason::NormalTeardown;
        let mut cleanup_started = false;
        let mut native_exit_sent = false;
        let mut had_residual = false;
        let mut forced_count = 0u32;
        loop {
            if !cleanup_started {
                lifecycle.reap_available(native)?;
                if lifecycle.native_status.is_none() {
                    match wait_for_guardian_event(control, &signal_fd, Duration::from_millis(100))?
                    {
                        GuardianEvent::None | GuardianEvent::Child => {}
                        GuardianEvent::Cleanup(requested) => {
                            reason = requested;
                            cleanup_started = true;
                        }
                        GuardianEvent::ParentDeath => {
                            reason = GuardianCleanupReason::ParentDeath;
                            cleanup_started = true;
                        }
                        GuardianEvent::ProtocolFailure => {
                            reason = GuardianCleanupReason::ProtocolFailure;
                            cleanup_started = true;
                        }
                    }
                    lifecycle.reap_available(native)?;
                }
                if lifecycle.native_status.is_some() {
                    cleanup_started = true;
                }
                if !cleanup_started {
                    continue;
                }
                had_residual |= lifecycle
                    .natural_grace(native, reason == GuardianCleanupReason::NormalTeardown)?;
            }

            let cleanup = lifecycle.cleanup_attempt(native)?;
            forced_count = forced_count.saturating_add(cleanup.forced_count);
            lifecycle.reap_available(native)?;
            if let Some((native_code, native_signal)) = lifecycle.native_exit()
                && !native_exit_sent
            {
                send_frame(
                    control,
                    Frame {
                        kind: FrameKind::NativeExited,
                        sequence,
                        guardian_pid: guardian.pid,
                        guardian_birth: guardian.birth,
                        native_pid: lifecycle.native.pid,
                        native_birth: lifecycle.native.birth,
                        native_code: native_code.unwrap_or(-1),
                        native_signal: native_signal.unwrap_or(0),
                        disposition: 0,
                        failure_class: FailureClass::None,
                        forced_count: 0,
                    },
                )?;
                sequence += 1;
                native_exit_sent = true;
            }

            if !cleanup.complete {
                let (native_code, native_signal) = lifecycle.native_exit().unwrap_or((None, None));
                send_frame(
                    control,
                    Frame {
                        kind: FrameKind::CleanupFailed,
                        sequence,
                        guardian_pid: guardian.pid,
                        guardian_birth: guardian.birth,
                        native_pid: lifecycle.native.pid,
                        native_birth: lifecycle.native.birth,
                        native_code: native_code.unwrap_or(-1),
                        native_signal: native_signal.unwrap_or(0),
                        disposition: 0,
                        failure_class: FailureClass::Cleanup,
                        forced_count,
                    },
                )?;
                sequence += 1;
                if reason == GuardianCleanupReason::ParentDeath {
                    thread::sleep(TERM_GRACE);
                    continue;
                }
                loop {
                    match wait_for_guardian_event(control, &signal_fd, Duration::from_secs(1))? {
                        GuardianEvent::Cleanup(requested) => {
                            reason = requested;
                            break;
                        }
                        GuardianEvent::ParentDeath => {
                            reason = GuardianCleanupReason::ParentDeath;
                            break;
                        }
                        GuardianEvent::ProtocolFailure => {
                            reason = GuardianCleanupReason::ProtocolFailure;
                            break;
                        }
                        GuardianEvent::None | GuardianEvent::Child => {}
                    }
                }
                continue;
            }

            let (native_code, native_signal) = lifecycle
                .native_exit()
                .ok_or_else(|| anyhow!("native exit was unavailable after completed cleanup"))?;
            if !native_exit_sent {
                send_frame(
                    control,
                    Frame {
                        kind: FrameKind::NativeExited,
                        sequence,
                        guardian_pid: guardian.pid,
                        guardian_birth: guardian.birth,
                        native_pid: lifecycle.native.pid,
                        native_birth: lifecycle.native.birth,
                        native_code: native_code.unwrap_or(-1),
                        native_signal: native_signal.unwrap_or(0),
                        disposition: 0,
                        failure_class: FailureClass::None,
                        forced_count: 0,
                    },
                )?;
                sequence += 1;
            }
            let disposition = cleanup_disposition(
                reason,
                native_code,
                native_signal,
                had_residual,
                forced_count,
            );
            let completion = GuardianCompletion {
                native_code,
                native_signal,
                disposition,
                forced_signal_count: forced_count,
            };
            send_frame(
                control,
                Frame {
                    kind: FrameKind::CleanupComplete,
                    sequence,
                    guardian_pid: guardian.pid,
                    guardian_birth: guardian.birth,
                    native_pid: lifecycle.native.pid,
                    native_birth: lifecycle.native.birth,
                    native_code: native_code.unwrap_or(-1),
                    native_signal: native_signal.unwrap_or(0),
                    disposition: disposition as u8,
                    failure_class: FailureClass::None,
                    forced_count: completion.forced_signal_count,
                },
            )?;
            let _ = wait_for_cleanup_ack(control, &signal_fd, READY_TIMEOUT)?;
            return Ok(expected_guardian_exit_code(&completion));
        }
    })
}

fn retain_post_spawn_ownership<T>(
    native: &mut Child,
    operation: impl FnOnce(&mut Child) -> Result<T>,
) -> Result<T> {
    let result = operation(native);
    if result.is_err() {
        cleanup_after_post_spawn_failure(native);
    }
    result
}

/// Once the native child exists, the Guardian must not unwind out of ownership.
///
/// This path deliberately has no attempt limit: if a post-spawn protocol,
/// process-observation, or control transport operation fails, the Guardian
/// keeps the subreaper alive and repeats exact direct-child cleanup until both
/// `/proc` and `waitpid` agree that no owned child remains. Persistent kernel
/// observation failure therefore retains ownership instead of claiming clean
/// shutdown or abandoning the native tree.
fn cleanup_after_post_spawn_failure(native: &mut Child) {
    let native_pid = native.id();
    let kill_after = Instant::now() + TERM_GRACE;
    loop {
        let no_waitable_children =
            reap_after_post_spawn_failure(native, native_pid).unwrap_or(false);
        let no_direct_children = direct_children()
            .map(|children| children.is_empty())
            .unwrap_or(false);
        if no_waitable_children && no_direct_children {
            return;
        }

        let signal = if Instant::now() < kill_after {
            libc::SIGTERM
        } else {
            libc::SIGKILL
        };
        let _ = signal_direct_children(signal);
        thread::sleep(Duration::from_millis(20));
    }
}

fn reap_after_post_spawn_failure(native: &mut Child, native_pid: u32) -> Result<bool> {
    // Synchronize Child's internal status when it wins the race to reap the
    // leader; raw waitpid below is still required for adopted descendants.
    let _ = native.try_wait();
    loop {
        // SAFETY: waitpid writes one status integer and only reaps direct or
        // subreaper-adopted children of this Guardian.
        let mut status = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if pid > 0 {
            if pid as u32 == native_pid {
                let _ = native.try_wait();
            }
            continue;
        }
        if pid == 0 {
            return Ok(false);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ECHILD) {
            return Ok(true);
        }
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error).context("Guardian post-spawn cleanup waitpid failed");
    }
}

fn cleanup_disposition(
    reason: GuardianCleanupReason,
    native_code: Option<i32>,
    native_signal: Option<i32>,
    had_residual: bool,
    forced_count: u32,
) -> GuardianCleanupDisposition {
    match reason {
        GuardianCleanupReason::Cancel => GuardianCleanupDisposition::Canceled,
        GuardianCleanupReason::Timeout => GuardianCleanupDisposition::TimedOut,
        GuardianCleanupReason::ParentDeath => GuardianCleanupDisposition::ParentDied,
        GuardianCleanupReason::ProtocolFailure => GuardianCleanupDisposition::ProtocolFailure,
        GuardianCleanupReason::NormalTeardown => {
            if native_code != Some(0) || native_signal.is_some() {
                GuardianCleanupDisposition::NativeFailure
            } else if had_residual || forced_count > 0 {
                GuardianCleanupDisposition::OrphanedDescendants
            } else {
                GuardianCleanupDisposition::Clean
            }
        }
    }
}

struct GuardianLifecycle {
    native: ProcSnapshot,
    _native_pidfd: OwnedFd,
    native_status: Option<ExitStatus>,
}

struct CleanupAttempt {
    complete: bool,
    forced_count: u32,
}

impl GuardianLifecycle {
    fn new(native: ProcSnapshot, native_pidfd: OwnedFd) -> Self {
        Self {
            native,
            _native_pidfd: native_pidfd,
            native_status: None,
        }
    }

    fn reap_available(&mut self, native: &mut Child) -> Result<()> {
        loop {
            // SAFETY: waitpid writes one status integer and only reaps direct or
            // subreaper-adopted children of this Guardian.
            let mut status = 0;
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid > 0 {
                if pid as u32 == self.native.pid {
                    self.native_status = Some(exit_status_from_wait(status));
                    // Keep std::process::Child synchronized after the raw reap.
                    let _ = native.try_wait();
                }
                continue;
            }
            if pid == 0
                || (pid < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD))
            {
                return Ok(());
            }
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(std::io::Error::last_os_error()).context("Guardian waitpid failed");
        }
    }

    fn natural_grace(&mut self, native: &mut Child, enabled: bool) -> Result<bool> {
        if !enabled {
            return self.any_children(native);
        }
        let deadline = Instant::now() + NATURAL_GRACE;
        loop {
            self.reap_available(native)?;
            if !self.any_children(native)? {
                return Ok(false);
            }
            if Instant::now() >= deadline {
                return Ok(true);
            }
            thread::sleep(LOOP_TICK);
        }
    }

    fn cleanup_attempt(&mut self, native: &mut Child) -> Result<CleanupAttempt> {
        let mut forced_count = 0u32;
        for (signal, grace) in [(libc::SIGTERM, TERM_GRACE), (libc::SIGKILL, KILL_GRACE)] {
            let deadline = Instant::now() + grace;
            loop {
                self.reap_available(native)?;
                if !self.any_children(native)? {
                    return Ok(CleanupAttempt {
                        complete: true,
                        forced_count,
                    });
                }
                forced_count = forced_count.saturating_add(signal_direct_children(signal)?);
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        self.reap_available(native)?;
        Ok(CleanupAttempt {
            complete: !self.any_children(native)?,
            forced_count,
        })
    }

    fn any_children(&mut self, native: &mut Child) -> Result<bool> {
        self.reap_available(native)?;
        Ok(!direct_children()?.is_empty())
    }

    fn native_exit(&self) -> Option<(Option<i32>, Option<i32>)> {
        self.native_status.as_ref().map(|status| {
            use std::os::unix::process::ExitStatusExt;
            (status.code(), status.signal())
        })
    }
}

fn exit_status_from_wait(status: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    ExitStatus::from_raw(status)
}

#[derive(Debug, Clone, Copy)]
struct ProcSnapshot {
    pid: u32,
    uid: u32,
    ppid: u32,
    birth: u64,
}

fn read_process_snapshot(pid: u32) -> Result<Option<ProcSnapshot>> {
    if pid <= 1 {
        bail!("process identity PID is outside the supported range");
    }
    let root = PathBuf::from(format!("/proc/{pid}"));
    let metadata = match fs::metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if proc_entry_vanished(&error) => return Ok(None),
        Err(error) => return Err(error).context("failed to inspect process owner"),
    };
    let stat = match fs::read_to_string(root.join("stat")) {
        Ok(stat) => stat,
        Err(error) if proc_entry_vanished(&error) => return Ok(None),
        Err(error) => return Err(error).context("failed to inspect process stat"),
    };
    let close = stat
        .rfind(')')
        .ok_or_else(|| anyhow!("process stat is malformed"))?;
    let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
    let ppid = fields
        .get(1)
        .ok_or_else(|| anyhow!("process stat omitted parent PID"))?
        .parse::<u32>()
        .context("process parent PID is invalid")?;
    let birth = fields
        .get(19)
        .ok_or_else(|| anyhow!("process stat omitted birth identity"))?
        .parse::<u64>()
        .context("process birth identity is invalid")?;
    if birth == 0 {
        bail!("process birth identity is zero");
    }
    Ok(Some(ProcSnapshot {
        pid,
        uid: metadata.uid(),
        ppid,
        birth,
    }))
}

fn guardian_identity() -> Result<GuardianProcessIdentity> {
    let pid = std::process::id();
    let snapshot = read_process_snapshot(pid)?
        .ok_or_else(|| anyhow!("Guardian process identity is unavailable"))?;
    Ok(GuardianProcessIdentity {
        pid,
        birth: snapshot.birth,
    })
}

fn direct_children() -> Result<Vec<u32>> {
    let task_root = PathBuf::from(format!("/proc/{}/task", std::process::id()));
    let mut found = Vec::new();
    for task in fs::read_dir(&task_root).context("failed to open Guardian task directory")? {
        let task = task?;
        let Some(tid) = task
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let children_path = task_root.join(tid.to_string()).join("children");
        let children = match fs::read_to_string(children_path) {
            Ok(children) => children,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error).context("failed to read Guardian direct children"),
        };
        for value in children.split_ascii_whitespace() {
            let pid = value
                .parse::<u32>()
                .context("Guardian direct-child PID is invalid")?;
            if pid <= 1 {
                bail!("Guardian direct-child PID is outside the process range");
            }
            if !found.contains(&pid) {
                if found.len() >= MAX_DIRECT_CHILDREN {
                    bail!("Guardian direct-child bound exceeded");
                }
                found.push(pid);
            }
        }
    }
    Ok(found)
}

fn signal_direct_children(signal: i32) -> Result<u32> {
    let guardian_pid = std::process::id();
    let mut signaled = 0u32;
    for pid in direct_children()? {
        let Some(before) = read_process_snapshot(pid)? else {
            continue;
        };
        if signal_owned_snapshot(before, guardian_pid, signal)? {
            signaled = signaled.saturating_add(1);
        }
    }
    Ok(signaled)
}

fn signal_owned_snapshot(before: ProcSnapshot, guardian_pid: u32, signal: i32) -> Result<bool> {
    // SAFETY: geteuid has no preconditions.
    if before.uid != unsafe { libc::geteuid() } || before.ppid != guardian_pid {
        return Ok(false);
    }
    let pidfd = match open_pidfd(before.pid) {
        Ok(pidfd) => pidfd,
        Err(error) if error_is_process_gone(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    let Some(after) = read_process_snapshot(before.pid)? else {
        return Ok(false);
    };
    // SAFETY: geteuid has no preconditions.
    if after.pid != before.pid
        || after.uid != before.uid
        || after.ppid != before.ppid
        || after.birth != before.birth
        || after.uid != unsafe { libc::geteuid() }
        || after.ppid != guardian_pid
    {
        return Ok(false);
    }
    pidfd_send_signal(&pidfd, signal)?;
    Ok(true)
}

fn configure_guardian_process(expected_parent: u32, mode: GuardianMode) -> Result<()> {
    // SAFETY: these prctl/getppid/setsid calls use no borrowed pointers.
    unsafe {
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("Guardian capability PR_SET_CHILD_SUBREAPER unavailable");
        }
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("Guardian capability PR_SET_PDEATHSIG unavailable");
        }
        if libc::getppid() != expected_parent as libc::pid_t {
            bail!("Guardian expected parent changed before lifecycle binding");
        }
        for signal in [libc::SIGINT, libc::SIGQUIT, libc::SIGPIPE] {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = libc::SIG_IGN;
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("Guardian signal policy setup failed");
            }
        }
        if mode == GuardianMode::HeadlessWorker && libc::setsid() < 0 {
            return Err(std::io::Error::last_os_error())
                .context("Guardian headless session setup failed");
        }
    }
    Ok(())
}

fn configure_native_child(headless: bool) -> std::io::Result<()> {
    // SAFETY: sigprocmask/sigaction/setpgid only update this freshly forked
    // process and use initialized local values.
    unsafe {
        let mut empty: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut empty);
        if libc::sigprocmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        for signal in [
            libc::SIGINT,
            libc::SIGQUIT,
            libc::SIGTERM,
            libc::SIGHUP,
            libc::SIGCHLD,
        ] {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = libc::SIG_DFL;
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        if headless && libc::setpgid(0, 0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn create_signal_fd() -> Result<OwnedFd> {
    // SAFETY: signal set and signalfd arguments are initialized for the calls.
    unsafe {
        let mut blocked: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut blocked);
        for signal in [libc::SIGCHLD, libc::SIGTERM, libc::SIGHUP] {
            libc::sigaddset(&mut blocked, signal);
        }
        if libc::sigprocmask(libc::SIG_BLOCK, &blocked, std::ptr::null_mut()) != 0 {
            return Err(std::io::Error::last_os_error()).context("Guardian signal mask failed");
        }
        let fd = libc::signalfd(-1, &blocked, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("Guardian signalfd unavailable");
        }
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

enum GuardianEvent {
    None,
    Child,
    Cleanup(GuardianCleanupReason),
    ParentDeath,
    ProtocolFailure,
}

fn wait_for_guardian_event(
    control: &OwnedFd,
    signal_fd: &OwnedFd,
    timeout: Duration,
) -> Result<GuardianEvent> {
    let timeout = timeout.as_millis().min(i32::MAX as u128) as i32;
    let mut descriptors = [
        libc::pollfd {
            fd: control.as_raw_fd(),
            events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
            revents: 0,
        },
        libc::pollfd {
            fd: signal_fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    // SAFETY: descriptors points to two initialized pollfd records.
    let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, timeout) };
    if ready < 0 {
        if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            return Ok(GuardianEvent::None);
        }
        return Err(std::io::Error::last_os_error()).context("Guardian event poll failed");
    }
    if descriptors[1].revents & libc::POLLIN != 0 {
        let mut info = std::mem::MaybeUninit::<libc::signalfd_siginfo>::uninit();
        // SAFETY: info has enough writable storage for one signalfd record.
        let count = unsafe {
            libc::read(
                signal_fd.as_raw_fd(),
                info.as_mut_ptr().cast(),
                std::mem::size_of::<libc::signalfd_siginfo>(),
            )
        };
        if count == std::mem::size_of::<libc::signalfd_siginfo>() as isize {
            // SAFETY: the complete signalfd record was read above.
            let signal = unsafe { info.assume_init() }.ssi_signo as i32;
            return match signal {
                libc::SIGCHLD => Ok(GuardianEvent::Child),
                libc::SIGTERM | libc::SIGHUP => Ok(GuardianEvent::ParentDeath),
                _ => Ok(GuardianEvent::ProtocolFailure),
            };
        }
    }
    if descriptors[0].revents & libc::POLLIN != 0 {
        return match receive_frame(control) {
            Ok(Some(frame)) if frame.kind == FrameKind::CleanupRequest => Ok(
                GuardianEvent::Cleanup(GuardianCleanupReason::try_from(frame.disposition)?),
            ),
            Ok(Some(_)) | Err(_) => Ok(GuardianEvent::ProtocolFailure),
            Ok(None) => Ok(GuardianEvent::ParentDeath),
        };
    }
    if descriptors[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
        return Ok(GuardianEvent::ParentDeath);
    }
    Ok(GuardianEvent::None)
}

fn wait_for_cleanup_ack(control: &OwnedFd, signal_fd: &OwnedFd, budget: Duration) -> Result<bool> {
    let deadline = Instant::now() + budget;
    loop {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = remaining
            .min(Duration::from_millis(100))
            .as_millis()
            .min(i32::MAX as u128) as i32;
        let mut descriptors = [
            libc::pollfd {
                fd: control.as_raw_fd(),
                events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
            libc::pollfd {
                fd: signal_fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: descriptors points to two initialized pollfd records.
        let ready =
            unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, timeout) };
        if ready < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(std::io::Error::last_os_error())
                .context("Guardian cleanup acknowledgement poll failed");
        }
        if descriptors[0].revents & libc::POLLIN != 0 {
            match receive_frame(control) {
                Ok(Some(frame)) if frame.kind == FrameKind::CleanupAck => {
                    if frame.guardian_pid != 0
                        || frame.guardian_birth != 0
                        || frame.native_pid != 0
                        || frame.native_birth != 0
                        || frame.native_code != 0
                        || frame.native_signal != 0
                        || frame.disposition != 0
                        || frame.failure_class != FailureClass::None
                        || frame.forced_count != 0
                    {
                        bail!("Guardian cleanup acknowledgement was malformed");
                    }
                    return Ok(true);
                }
                // A cleanup request can race with natural native completion.
                // Cleanup is already complete, so consume it and keep waiting
                // for the acknowledgement that closes the protocol.
                Ok(Some(frame)) if frame.kind == FrameKind::CleanupRequest => continue,
                Ok(Some(_)) => bail!("Guardian received an invalid completion acknowledgement"),
                Ok(None) => {}
                Err(error) if error_is_socket_closed(&error) => return Ok(false),
                Err(error) => return Err(error),
            }
        }
        if descriptors[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            return Ok(false);
        }
        if descriptors[1].revents & libc::POLLIN != 0 {
            let mut info = std::mem::MaybeUninit::<libc::signalfd_siginfo>::uninit();
            // SAFETY: info has storage for one signalfd record.
            let count = unsafe {
                libc::read(
                    signal_fd.as_raw_fd(),
                    info.as_mut_ptr().cast(),
                    std::mem::size_of::<libc::signalfd_siginfo>(),
                )
            };
            if count == std::mem::size_of::<libc::signalfd_siginfo>() as isize {
                // SAFETY: the complete signalfd record was read above.
                let signal = unsafe { info.assume_init() }.ssi_signo as i32;
                if matches!(signal, libc::SIGTERM | libc::SIGHUP) {
                    return Ok(false);
                }
            }
        }
    }
}

fn probe_runtime_capabilities() -> Result<()> {
    let pair = seqpacket_socketpair().context("Guardian capability SOCK_SEQPACKET unavailable")?;
    verify_seqpacket(&pair.0)?;
    drop(pair);
    let mut subreaper = 0;
    let mut death_signal = 0;
    // SAFETY: PR_GET_* write one integer to initialized stack storage.
    unsafe {
        if libc::prctl(libc::PR_GET_CHILD_SUBREAPER, &mut subreaper, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("Guardian capability PR_GET_CHILD_SUBREAPER unavailable");
        }
        if libc::prctl(libc::PR_GET_PDEATHSIG, &mut death_signal, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("Guardian capability PR_GET_PDEATHSIG unavailable");
        }
    }
    let pidfd = open_pidfd(std::process::id())
        .context("Guardian capability pidfd_open unavailable or denied")?;
    pidfd_send_signal(&pidfd, 0)
        .context("Guardian capability pidfd_send_signal unavailable or denied")?;
    let _ = direct_children().context("Guardian capability /proc children unavailable")?;
    let _ = read_process_snapshot(std::process::id())?
        .ok_or_else(|| anyhow!("Guardian capability /proc birth identity unavailable"))?;
    Ok(())
}

fn seqpacket_socketpair() -> Result<(OwnedFd, OwnedFd)> {
    let mut pair = [-1; 2];
    // SAFETY: pair points to two writable integers. On success each descriptor
    // is uniquely adopted below.
    let result = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
            pair.as_mut_ptr(),
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("SOCK_SEQPACKET socketpair failed");
    }
    // SAFETY: socketpair returned two new owned descriptors.
    Ok(unsafe { (OwnedFd::from_raw_fd(pair[0]), OwnedFd::from_raw_fd(pair[1])) })
}

fn verify_seqpacket(fd: &OwnedFd) -> Result<()> {
    let mut socket_type = 0i32;
    let mut length = std::mem::size_of::<i32>() as libc::socklen_t;
    // SAFETY: socket_type and length describe writable getsockopt storage.
    let result = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut socket_type as *mut i32).cast(),
            &mut length,
        )
    };
    if result != 0
        || length as usize != std::mem::size_of::<i32>()
        || socket_type != libc::SOCK_SEQPACKET
    {
        bail!("Guardian control descriptor is not SOCK_SEQPACKET");
    }
    Ok(())
}

fn send_frame(fd: &OwnedFd, frame: Frame) -> Result<()> {
    let bytes = frame.encode();
    // SAFETY: bytes is a valid fixed-size frame for the duration of send.
    let sent = unsafe {
        libc::send(
            fd.as_raw_fd(),
            bytes.as_ptr().cast(),
            bytes.len(),
            libc::MSG_NOSIGNAL,
        )
    };
    if sent == bytes.len() as isize {
        return Ok(());
    }
    if sent < 0 {
        return Err(std::io::Error::last_os_error()).context("Guardian frame send failed");
    }
    bail!("Guardian frame send was partial")
}

fn receive_frame(fd: &OwnedFd) -> Result<Option<Frame>> {
    let mut bytes = [0u8; FRAME_BYTES + 1];
    // SAFETY: bytes is writable for the complete recv call.
    let count = unsafe {
        libc::recv(
            fd.as_raw_fd(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
            libc::MSG_DONTWAIT,
        )
    };
    if count < 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EAGAIN) {
            return Ok(None);
        }
        return Err(error).context("Guardian frame receive failed");
    }
    if count == 0 {
        bail!("Guardian control socket closed");
    }
    Frame::decode(&bytes[..count as usize]).map(Some)
}

fn open_pidfd(pid: u32) -> Result<OwnedFd> {
    // SAFETY: pidfd_open has no pointer arguments and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd_open failed");
    }
    let fd = i32::try_from(fd).context("pidfd exceeds descriptor range")?;
    // SAFETY: pidfd_open returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn pidfd_send_signal(pidfd: &OwnedFd, signal: i32) -> Result<()> {
    // SAFETY: pidfd identifies the exact process and siginfo is null.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd_send_signal failed");
    }
    Ok(())
}

fn pidfd_is_ready(pidfd: &OwnedFd) -> Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: descriptor points to one initialized pollfd.
    let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
    if result < 0 {
        return Err(std::io::Error::last_os_error()).context("Guardian pidfd poll failed");
    }
    Ok(result == 1 && descriptor.revents & (libc::POLLIN | libc::POLLHUP) != 0)
}

fn clear_close_on_exec(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl only inspects/updates the inherited descriptor flags.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn set_close_on_exec(fd: RawFd) -> Result<()> {
    // SAFETY: fcntl only inspects/updates the owned descriptor flags.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) != 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to protect Guardian control descriptor");
        }
    }
    Ok(())
}

fn validate_command_shape(program: &OsStr, arguments: &[OsString]) -> Result<()> {
    if program.as_bytes().is_empty() {
        bail!("native Guardian command cannot be empty");
    }
    if arguments.len() > MAX_NATIVE_ARGUMENTS {
        bail!("native Guardian argument count exceeds its bound");
    }
    let bytes = program.as_bytes().len()
        + arguments
            .iter()
            .map(|argument| argument.as_bytes().len())
            .sum::<usize>();
    if bytes > MAX_NATIVE_ARGUMENT_BYTES {
        bail!("native Guardian argument bytes exceed their bound");
    }
    Ok(())
}

fn proc_entry_vanished(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        || matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH))
}

fn error_is_process_gone(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<std::io::Error>()
            .is_some_and(proc_entry_vanished)
    })
}

fn error_is_socket_closed(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            .is_some_and(|code| {
                matches!(
                    code,
                    libc::EPIPE | libc::ECONNRESET | libc::ENOTCONN | libc::EBADF
                )
            })
    })
}

fn exit_status_detail(status: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (_, Some(signal)) => format!("signal {signal}"),
        _ => "unknown status".into(),
    }
}

fn bounded_detail(input: &str) -> String {
    let mut output: String = input
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if output.is_empty() {
        output.push_str("bounded Guardian lifecycle failure");
    }
    if output.len() > 512 {
        let mut boundary = 512;
        while !output.is_char_boundary(boundary) {
            boundary -= 1;
        }
        output.truncate(boundary);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::PermissionsExt;

    fn protocol_handle() -> (GuardianHandle, OwnedFd) {
        let (parent, peer) = seqpacket_socketpair().unwrap();
        let child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let snapshot = read_process_snapshot(child.id()).unwrap().unwrap();
        let pidfd = open_pidfd(snapshot.pid).unwrap();
        (
            GuardianHandle {
                child: Some(child),
                pidfd,
                control: parent,
                ready: Some(GuardianReady {
                    guardian: GuardianProcessIdentity {
                        pid: snapshot.pid,
                        birth: snapshot.birth,
                    },
                    native: GuardianProcessIdentity {
                        pid: 4242,
                        birth: 4343,
                    },
                }),
                guardian: GuardianProcessIdentity {
                    pid: snapshot.pid,
                    birth: snapshot.birth,
                },
                next_guardian_sequence: 2,
                next_request_sequence: 1,
                native_exit: None,
                cleanup: None,
                cleanup_failed: false,
                ownership_lost: None,
                pre_spawn_failure: None,
                cleanup_requested: false,
            },
            peer,
        )
    }

    fn stop_protocol_handle(handle: &mut GuardianHandle) {
        if let Some(child) = handle.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        handle.child = None;
        handle.ownership_lost = Some("test cleanup".into());
    }

    fn protocol_ownership_lost_before_deadline(handle: &mut GuardianHandle) -> bool {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if matches!(handle.try_wait(), GuardianPoll::OwnershipLost(_)) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(LOOP_TICK);
        }
    }

    #[test]
    fn frame_codec_is_fixed_bounded_and_rejects_reserved_bytes() {
        let frame = Frame {
            kind: FrameKind::GuardianReady,
            sequence: 7,
            guardian_pid: 11,
            guardian_birth: 13,
            native_pid: 17,
            native_birth: 19,
            native_code: -1,
            native_signal: 9,
            disposition: 2,
            failure_class: FailureClass::Cleanup,
            forced_count: 23,
        };
        let bytes = frame.encode();
        let decoded = Frame::decode(&bytes).unwrap();
        assert_eq!(decoded.kind, frame.kind);
        assert_eq!(decoded.sequence, 7);
        assert_eq!(decoded.guardian_pid, 11);
        assert_eq!(decoded.guardian_birth, 13);
        assert_eq!(decoded.native_pid, 17);
        assert_eq!(decoded.native_birth, 19);
        assert_eq!(decoded.native_code, -1);
        assert_eq!(decoded.native_signal, 9);
        assert_eq!(decoded.disposition, 2);
        assert_eq!(decoded.failure_class, FailureClass::Cleanup);
        assert_eq!(decoded.forced_count, 23);

        let mut malformed = bytes;
        malformed[63] = 1;
        assert!(Frame::decode(&malformed).is_err());
        assert!(Frame::decode(&bytes[..FRAME_BYTES - 1]).is_err());
        let mut invalid_kind = bytes;
        invalid_kind[5] = 255;
        assert!(Frame::decode(&invalid_kind).is_err());
        let mut invalid_version = bytes;
        invalid_version[4] = PROTOCOL_VERSION + 1;
        assert!(Frame::decode(&invalid_version).is_err());
    }

    #[test]
    fn missing_malformed_and_duplicate_runtime_frames_fail_closed() {
        let (mut missing, peer) = protocol_handle();
        drop(peer);
        let ownership_lost = protocol_ownership_lost_before_deadline(&mut missing);
        stop_protocol_handle(&mut missing);
        assert!(ownership_lost);

        let (mut malformed, peer) = protocol_handle();
        let invalid = [0xffu8; FRAME_BYTES];
        // SAFETY: invalid is a live fixed buffer and peer owns a seqpacket fd.
        assert_eq!(
            unsafe {
                libc::send(
                    peer.as_raw_fd(),
                    invalid.as_ptr().cast(),
                    invalid.len(),
                    libc::MSG_NOSIGNAL,
                )
            },
            FRAME_BYTES as isize
        );
        assert!(matches!(
            malformed.try_wait(),
            GuardianPoll::OwnershipLost(_)
        ));
        stop_protocol_handle(&mut malformed);

        let (mut duplicate, _peer) = protocol_handle();
        let exit = Frame {
            kind: FrameKind::NativeExited,
            sequence: 2,
            guardian_pid: duplicate.guardian.pid,
            guardian_birth: duplicate.guardian.birth,
            native_pid: 4242,
            native_birth: 4343,
            native_code: 0,
            native_signal: 0,
            disposition: 0,
            failure_class: FailureClass::None,
            forced_count: 0,
        };
        duplicate.accept_runtime_frame(exit).unwrap();
        assert!(duplicate.accept_runtime_frame(exit).is_err());
        stop_protocol_handle(&mut duplicate);
    }

    #[test]
    fn internal_parser_preserves_opaque_native_arguments() {
        let raw = OsString::from_vec(b"raw-\xff-argument".to_vec());
        let parsed = InternalArguments::parse(&[
            "--control-fd".into(),
            "9".into(),
            "--expected-parent".into(),
            "123".into(),
            "--mode".into(),
            "headless".into(),
            "--environment-policy".into(),
            "inherit".into(),
            "--".into(),
            "fake-native".into(),
            raw.clone(),
        ])
        .unwrap();
        assert_eq!(parsed.control_fd, 9);
        assert_eq!(parsed.expected_parent, 123);
        assert_eq!(parsed.mode, GuardianMode::HeadlessWorker);
        assert!(!parsed.require_claude_proxy);
        assert_eq!(parsed.native_program, "fake-native");
        assert_eq!(parsed.native_args, [raw]);
    }

    #[test]
    fn internal_parser_rejects_duplicate_metadata_and_unbounded_argv() {
        assert!(
            InternalArguments::parse(&[
                "--control-fd".into(),
                "9".into(),
                "--control-fd".into(),
                "10".into(),
                "--expected-parent".into(),
                "123".into(),
                "--mode".into(),
                "headless".into(),
                "--environment-policy".into(),
                "inherit".into(),
                "--".into(),
                "fake-native".into(),
            ])
            .is_err()
        );
        let arguments = vec![OsString::from("x"); MAX_NATIVE_ARGUMENTS + 1];
        assert!(validate_command_shape(OsStr::new("fake"), &arguments).is_err());
    }

    #[test]
    fn runtime_capabilities_are_available_on_the_supported_test_host() {
        probe_runtime_capabilities().unwrap();
    }

    #[test]
    fn birth_mismatch_is_never_signaled() {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("trap '' TERM; exec sleep 30")
            .spawn()
            .unwrap();
        let mut snapshot = read_process_snapshot(child.id()).unwrap().unwrap();
        snapshot.birth = snapshot.birth.saturating_add(1);
        assert!(!signal_owned_snapshot(snapshot, std::process::id(), libc::SIGTERM).unwrap());
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn post_spawn_failure_cleanup_retains_subreaper_until_escaped_children_are_reaped() {
        const HELPER: &str = "HCOM_GUARDIAN_POST_SPAWN_CLEANUP_HELPER";
        if std::env::var_os(HELPER).is_some() {
            // SAFETY: this disposable single-test helper owns all children it
            // creates and exits immediately after the cleanup assertion.
            assert_eq!(
                unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) },
                0
            );
            let native_path = std::env::var_os("HCOM_GUARDIAN_TEST_NATIVE").unwrap();
            let native_report =
                PathBuf::from(std::env::var_os("HCOM_GUARDIAN_TEST_NATIVE_REPORT").unwrap());
            let child_report =
                PathBuf::from(std::env::var_os("HCOM_GUARDIAN_TEST_CHILD_REPORT").unwrap());
            let mut native = Command::new(native_path)
                .arg(&native_report)
                .arg(&child_report)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                if native_report.is_file() && child_report.is_file() {
                    break;
                }
                thread::sleep(LOOP_TICK);
            }
            assert!(native_report.is_file());
            assert!(child_report.is_file());
            let failure: Result<()> = retain_post_spawn_ownership(&mut native, |_native| {
                bail!("injected post-spawn failure")
            });
            assert!(failure.is_err());
            return;
        }

        let root = tempfile::tempdir().unwrap();
        let native = root.path().join("escaped-tree.py");
        let native_report = root.path().join("native.identity");
        let child_report = root.path().join("child.identity");
        fs::write(
            &native,
            r#"#!/usr/bin/python3
import os
import signal
import sys
import time

def report(path):
    stat = open(f"/proc/{os.getpid()}/stat", encoding="utf-8").read()
    birth = stat.rsplit(")", 1)[1].split()[19]
    with open(path, "w", encoding="utf-8") as output:
        output.write(f"{os.getpid()} {birth}")

signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = os.fork()
if child == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    report(sys.argv[2])
    while True:
        time.sleep(1)
report(sys.argv[1])
while True:
    time.sleep(1)
"#,
        )
        .unwrap();
        fs::set_permissions(&native, fs::Permissions::from_mode(0o700)).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "worker::guardian::tests::post_spawn_failure_cleanup_retains_subreaper_until_escaped_children_are_reaped",
            )
            .arg("--nocapture")
            .env(HELPER, "1")
            .env("HCOM_GUARDIAN_TEST_NATIVE", &native)
            .env("HCOM_GUARDIAN_TEST_NATIVE_REPORT", &native_report)
            .env("HCOM_GUARDIAN_TEST_CHILD_REPORT", &child_report)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());

        for report in [native_report, child_report] {
            let fields: Vec<u64> = fs::read_to_string(report)
                .unwrap()
                .split_ascii_whitespace()
                .map(|value| value.parse().unwrap())
                .collect();
            assert_eq!(fields.len(), 2);
            assert_ne!(
                read_process_snapshot(fields[0] as u32)
                    .unwrap()
                    .map(|snapshot| snapshot.birth),
                Some(fields[1]),
                "post-spawn cleanup abandoned exact child {}/{}",
                fields[0],
                fields[1]
            );
        }
    }

    #[test]
    fn cleanup_registry_interlock_releases_and_poison_is_permanent() {
        let registry = GuardianCleanupRegistry::default();
        assert_eq!(registry.interlock(), CleanupRegistryInterlock::Ready);
        registry.inject_pending_for_test();
        assert_eq!(
            registry.interlock(),
            CleanupRegistryInterlock::Pending { claims: 1 }
        );
        assert!(registry.ensure_available().is_err());
        registry.complete_pending_for_test();
        assert_eq!(registry.interlock(), CleanupRegistryInterlock::Ready);
        registry.poison_for_test("stable ownership-lost diagnostic");
        assert_eq!(
            registry.interlock(),
            CleanupRegistryInterlock::Poisoned {
                detail: "stable ownership-lost diagnostic".into()
            }
        );
        assert!(registry.ensure_available().is_err());
        assert!(GUARDIAN_LIFECYCLE_BOUNDARY.contains("external service-manager"));
        assert!(GUARDIAN_LIFECYCLE_BOUNDARY.contains("unexpected Guardian death"));
    }

    #[test]
    fn parent_preflight_rejects_invalid_shape_before_any_spawn() {
        let result = GuardedCommand::with_guardian_executable(PathBuf::from("/does/not/run"), "");
        assert!(result.is_err());
    }
}
