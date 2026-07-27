//! Private Linux PTY adapter for one fixed, fresh Codex generation.
//!
//! This module has no clap/router entry point. The Phase 3 factory consumes an
//! already-pinned [`TerminalChain`], and every successor argv is derived from
//! that immutable profile plus one opaque handoff ID.

#![cfg(unix)]

use std::collections::VecDeque;
use std::ffi::{CString, OsStr, OsString};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hcom::chain_pty::{
    arm_parent_death_hangup, copy_winsize, observe_direct_child, reap_direct_child,
    send_process_group_signal,
};
use hcom::chain_supervisor::{
    ChainSignal, ChainTitleState, CleanupEvidence, DeliveryExitContext, ExitEvidence,
    FinishAttempt, GenerationAdapter, GenerationEvent, GenerationIdentity, OuterTerminalIdentity,
    PreparedGeneration, ResourceCleanupEvidence, ShutdownReason, SignalSendResult,
    TargetReservation, linux_process_birth_identity,
};

use crate::codex_chain::{
    CODEX_VERSION_ENV, CODEX_VERSION_PROBE_ARGS, CodexLaunchProfile, HANDOFF_ID_ENV,
    SUPPORTED_CODEX_VERSION, validate_codex_version_output,
};
use crate::handoff::TerminalChain;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/support/mock_http.rs"]
mod phase3_mock_http;

const MSG_START: i64 = 1;
const MSG_READY: i64 = 2;
const MSG_EXIT: i64 = 3;
const MSG_REAP: i64 = 4;
const MAX_PROXY_QUEUE_BYTES: usize = 1024 * 1024;
const CHILD_START_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const TITLE_CONTROL_TIMEOUT: Duration = Duration::from_millis(50);

static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueRead {
    Data,
    Pending,
    Closed,
}

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

extern "C" fn chain_signal_handler(signal: libc::c_int) {
    let fd = SIGNAL_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = signal as u8;
        // SAFETY: async-signal-safe write to a nonblocking self-pipe.
        unsafe {
            libc::write(fd, (&byte as *const u8).cast(), 1);
        }
    }
}

struct SavedSignal {
    signal: libc::c_int,
    action: libc::sigaction,
}

struct LaunchSpec {
    executable: PathBuf,
    argv: Vec<String>,
    environment: Vec<(OsString, OsString)>,
    workspace: PathBuf,
}

pub(crate) struct CodexAdapterPreflight {
    executable: PathBuf,
    profile: CodexLaunchProfile,
}

pub(crate) struct CodexPrepared {
    identity: GenerationIdentity,
    gate_write: RawFd,
    command_write: RawFd,
    report_read: RawFd,
    exec_status_read: RawFd,
    master: RawFd,
}

type CodexActivationFailure = Box<(CodexPrepared, io::Error)>;

impl PreparedGeneration for CodexPrepared {
    fn identity(&self) -> &GenerationIdentity {
        &self.identity
    }
}

pub(crate) struct CodexActive {
    identity: GenerationIdentity,
    command_write: RawFd,
    report_read: RawFd,
    master: RawFd,
    to_child: VecDeque<u8>,
    to_outer: VecDeque<u8>,
    output_filter: crate::pty::ChainTitleOutputFilter,
    exit: Option<ExitEvidence>,
}

pub(crate) struct CodexGenerationAdapter {
    outer: OuterTerminalIdentity,
    outer_fd: RawFd,
    saved_termios: libc::termios,
    signal_read: RawFd,
    signal_write: RawFd,
    saved_signals: Vec<SavedSignal>,
    executable: PathBuf,
    profile: CodexLaunchProfile,
    chain_id: String,
    title_stack_pushed: bool,
    current_title: Option<Vec<u8>>,
    pending_title: Option<Vec<u8>>,
}

impl CodexGenerationAdapter {
    /// Resolve and verify the one supported executable/profile without opening
    /// a PTY, changing terminal mode, or taking process ownership.
    pub(crate) fn preflight(chain: &TerminalChain) -> io::Result<CodexAdapterPreflight> {
        if chain.tool != "codex" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "private generation adapter requires a Codex chain",
            ));
        }
        let profile = CodexLaunchProfile::from_chain(chain)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let executable = crate::terminal::which_bin("codex")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute() && path.is_file())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "supported Codex executable is unavailable",
                )
            })?;
        let mut version_command = Command::new(&executable);
        version_command.args(CODEX_VERSION_PROBE_ARGS);
        let parent_pid = unsafe { libc::getpid() };
        // SAFETY: this pre-exec closure only applies the same async-signal-safe
        // Linux parent-death guard used by the wrapper and inner child.
        unsafe {
            version_command.pre_exec(move || arm_parent_death_hangup(parent_pid));
        }
        let version = version_command.output()?;
        if !version.status.success() {
            return Err(io::Error::other(
                "supported Codex version probe did not succeed",
            ));
        }
        let stdout = std::str::from_utf8(&version.stdout)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid Codex version"))?;
        validate_codex_version_output(stdout)
            .map_err(|error| io::Error::new(io::ErrorKind::Unsupported, error))?;
        Ok(CodexAdapterPreflight {
            executable,
            profile,
        })
    }

    /// Construct the adapter only after the public chain reservation is
    /// durable. Raw-mode and signal ownership begin here; no child exists yet.
    pub(crate) fn from_preflight(
        chain: &TerminalChain,
        outer: OuterTerminalIdentity,
        preflight: CodexAdapterPreflight,
    ) -> io::Result<Self> {
        let pinned = CodexLaunchProfile::from_chain(chain)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if pinned != preflight.profile {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Codex chain profile changed after preflight",
            ));
        }
        let outer_fd = open_exact_outer_tty(outer)?;
        // SAFETY: open_exact_outer_tty returned a fresh owned descriptor.
        let outer_fd = unsafe { OwnedFd::from_raw_fd(outer_fd) };
        let saved_termios = tcgetattr(outer_fd.as_raw_fd())?;
        set_raw(outer_fd.as_raw_fd())?;
        let setup = (|| -> io::Result<(OwnedFd, OwnedFd, Vec<SavedSignal>)> {
            let (signal_read, signal_write) = pipe_cloexec_owned()?;
            set_nonblocking(signal_read.as_raw_fd())?;
            set_nonblocking(signal_write.as_raw_fd())?;
            let saved_signals = install_signal_handlers(signal_write.as_raw_fd())?;
            Ok((signal_read, signal_write, saved_signals))
        })();
        let (signal_read, signal_write, saved_signals) = match setup {
            Ok(values) => values,
            Err(error) => {
                // SAFETY: restore the exact caller state before OwnedFd closes.
                unsafe {
                    libc::tcsetattr(outer_fd.as_raw_fd(), libc::TCSANOW, &saved_termios);
                }
                return Err(error);
            }
        };
        let title_stack_pushed = write_terminal_control(outer_fd.as_raw_fd(), b"\x1b[22;0t");
        Ok(Self {
            outer,
            outer_fd: outer_fd.into_raw_fd(),
            saved_termios,
            signal_read: signal_read.into_raw_fd(),
            signal_write: signal_write.into_raw_fd(),
            saved_signals,
            executable: preflight.executable,
            profile: preflight.profile,
            chain_id: chain.id.clone(),
            title_stack_pushed,
            current_title: None,
            pending_title: None,
        })
    }

    /// Private test/characterization convenience. The production CLI performs
    /// preflight only after its durable reservation, then calls
    /// `from_preflight`.
    #[cfg(test)]
    pub(crate) fn new(chain: &TerminalChain, outer: OuterTerminalIdentity) -> io::Result<Self> {
        let preflight = Self::preflight(chain)?;
        Self::from_preflight(chain, outer, preflight)
    }

    fn launch_spec(
        &self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> io::Result<LaunchSpec> {
        let initial_protocol = reservation.handoff_id == self.chain_id;
        let argv = if initial_protocol {
            self.profile.initial_argv(&self.chain_id)
        } else {
            self.profile.argv(&reservation.handoff_id)
        }
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if initial_protocol {
            self.profile
                .validate_exact_initial_argv(&self.chain_id, &argv)
        } else {
            self.profile
                .validate_exact_argv(&reservation.handoff_id, &argv)
        }
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let environment = exact_generation_environment(
            &self.chain_id,
            reservation,
            identity,
            SUPPORTED_CODEX_VERSION,
        );
        Ok(LaunchSpec {
            executable: self.executable.clone(),
            argv,
            environment,
            workspace: self.profile.workspace.clone(),
        })
    }

    fn spawn_prepared(&mut self, reservation: &TargetReservation) -> io::Result<CodexPrepared> {
        let process_id = opaque_id("chain-process");
        let instance_name = format!(
            "chain_g{}_{}",
            reservation.generation,
            uuid::Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(8)
                .collect::<String>()
        );
        let hcom_session_id = opaque_id("chain-session");
        let provisional = GenerationIdentity {
            generation: reservation.generation,
            launch_nonce: reservation.launch_nonce.clone(),
            wrapper_pid: -1,
            wrapper_pgid: self.outer.supervisor_pgid,
            child_pid: -1,
            child_pgid: -1,
            child_process_birth_identity: "pending".to_string(),
            process_id,
            process_birth_identity: "pending".to_string(),
            instance_name,
            hcom_session_id,
            native_session_id: None,
        };
        let spec = self.launch_spec(reservation, &provisional)?;
        let (gate_read, gate_write) = pipe_cloexec_owned()?;
        let (command_read, command_write) = pipe_cloexec_owned()?;
        let (report_read, report_write) = pipe_cloexec_owned()?;
        let (exec_status_read, exec_status_write) = pipe_cloexec_owned()?;
        let (master, slave) = openpty_owned(current_winsize(self.outer_fd)?)?;
        set_cloexec(master.as_raw_fd())?;
        set_cloexec(slave.as_raw_fd())?;
        set_nonblocking(master.as_raw_fd())?;
        set_nonblocking(report_read.as_raw_fd())?;

        // SAFETY: this private factory is called before a chain adapter owns
        // any thread. The forked wrapper never returns into the caller.
        let wrapper = unsafe { libc::fork() };
        if wrapper == -1 {
            return Err(io::Error::last_os_error());
        }
        if wrapper == 0 {
            wrapper_main(
                self.outer.supervisor_pid,
                gate_read.as_raw_fd(),
                command_read.as_raw_fd(),
                report_write.as_raw_fd(),
                exec_status_write.as_raw_fd(),
                slave.as_raw_fd(),
                spec,
            );
        }

        drop(gate_read);
        drop(command_read);
        drop(report_write);
        drop(exec_status_write);
        drop(slave);
        let gate_write = gate_write.into_raw_fd();
        let command_write = command_write.into_raw_fd();
        let report_read = report_read.into_raw_fd();
        let exec_status_read = exec_status_read.into_raw_fd();
        let master = master.into_raw_fd();

        let setup = (|| -> io::Result<(String, i32, String)> {
            let wrapper_birth = linux_process_birth_identity(wrapper)?;
            let start = read_message_with_timeout(report_read, CHILD_START_TIMEOUT)?;
            if start.kind != MSG_START
                || start.a != i64::from(wrapper)
                || start.b != i64::from(self.outer.supervisor_pgid)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Codex wrapper start evidence is inconsistent",
                ));
            }
            let child_pid = i32::try_from(start.c).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Codex child PID")
            })?;
            if child_pid <= 1 || child_pid == wrapper {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Codex child process identity",
                ));
            }
            let child_birth = linux_process_birth_identity(child_pid)?;
            Ok((wrapper_birth, child_pid, child_birth))
        })();
        let (wrapper_birth, child_pid, child_birth) = match setup {
            Ok(values) => values,
            Err(error) => {
                abort_failed_prepare(
                    wrapper,
                    gate_write,
                    command_write,
                    report_read,
                    exec_status_read,
                    master,
                );
                return Err(error);
            }
        };
        Ok(CodexPrepared {
            identity: GenerationIdentity {
                wrapper_pid: wrapper,
                child_pid,
                child_pgid: child_pid,
                child_process_birth_identity: child_birth,
                process_birth_identity: wrapper_birth,
                ..provisional
            },
            gate_write,
            command_write,
            report_read,
            exec_status_read,
            master,
        })
    }

    fn activate(
        &mut self,
        mut prepared: CodexPrepared,
    ) -> Result<CodexActive, CodexActivationFailure> {
        if let Err(error) = write_byte(prepared.gate_write, 1) {
            return Err(Box::new((prepared, error)));
        }
        close_fd(prepared.gate_write);
        prepared.gate_write = -1;
        let ready = match read_message_with_timeout(prepared.report_read, CHILD_START_TIMEOUT) {
            Ok(ready) => ready,
            Err(error) => return Err(Box::new((prepared, error))),
        };
        if ready.kind != MSG_READY
            || ready.a != i64::from(prepared.identity.child_pid)
            || ready.b != i64::from(prepared.identity.child_pid)
            || ready.c != i64::from(prepared.identity.child_pgid)
        {
            return Err(Box::new((
                prepared,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Codex inner session/process-group evidence is inconsistent",
                ),
            )));
        }
        if let Err(error) = verify_exec_success(prepared.exec_status_read, CHILD_START_TIMEOUT) {
            return Err(Box::new((prepared, error)));
        }
        close_fd(prepared.exec_status_read);
        prepared.exec_status_read = -1;
        Ok(CodexActive {
            identity: prepared.identity,
            command_write: prepared.command_write,
            report_read: prepared.report_read,
            master: prepared.master,
            to_child: VecDeque::new(),
            to_outer: VecDeque::new(),
            output_filter: crate::pty::ChainTitleOutputFilter::new(),
            exit: None,
        })
    }

    fn handle_report(
        &mut self,
        active: &mut CodexActive,
        message: WireMessage,
    ) -> io::Result<GenerationEvent> {
        if message.kind != MSG_EXIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected Codex wrapper report",
            ));
        }
        let exit = message_to_exit(message)?;
        active.exit = Some(exit.clone());
        Ok(GenerationEvent::ChildExited(exit))
    }

    fn proxy_once(
        &mut self,
        active: &mut CodexActive,
        timeout: Duration,
    ) -> io::Result<GenerationEvent> {
        flush_queue(active.master, &mut active.to_child)?;
        flush_queue(self.outer_fd, &mut active.to_outer)?;
        self.queue_pending_title(active);
        flush_queue(self.outer_fd, &mut active.to_outer)?;
        if let Some(message) = try_read_message(active.report_read)? {
            return self.handle_report(active, message);
        }

        let mut descriptors = [
            libc::pollfd {
                fd: active.report_read,
                events: libc::POLLIN | libc::POLLHUP,
                revents: 0,
            },
            libc::pollfd {
                fd: self.signal_read,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.outer_fd,
                events: if active.to_child.len() < MAX_PROXY_QUEUE_BYTES {
                    libc::POLLIN
                } else {
                    0
                },
                revents: 0,
            },
            libc::pollfd {
                fd: active.master,
                events: if active.to_outer.len() < MAX_PROXY_QUEUE_BYTES {
                    libc::POLLIN
                } else {
                    0
                } | if active.to_child.is_empty() {
                    0
                } else {
                    libc::POLLOUT
                },
                revents: 0,
            },
            libc::pollfd {
                fd: self.outer_fd,
                events: if active.to_outer.is_empty() {
                    0
                } else {
                    libc::POLLOUT
                },
                revents: 0,
            },
        ];
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: descriptors is a live initialized pollfd array.
        let ready = unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                timeout_ms,
            )
        };
        if ready == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(GenerationEvent::ControlWake);
            }
            return Err(error);
        }
        if ready == 0 {
            return Ok(GenerationEvent::Timeout);
        }
        if descriptors[0].revents & libc::POLLIN != 0 {
            return self.handle_report(active, read_message(active.report_read)?);
        }
        if descriptors[0].revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Codex wrapper closed before reporting child exit",
            ));
        }
        if descriptors[1].revents & libc::POLLIN != 0 {
            let signal = read_byte(self.signal_read)? as i32;
            return Ok(match signal {
                libc::SIGINT => GenerationEvent::Interrupt,
                libc::SIGHUP => GenerationEvent::Hangup,
                libc::SIGWINCH => GenerationEvent::Resize,
                libc::SIGCONT => GenerationEvent::Continue,
                _ => GenerationEvent::ControlWake,
            });
        }
        if descriptors[2].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            return Ok(GenerationEvent::Hangup);
        }
        if descriptors[2].revents & libc::POLLIN != 0
            && read_into_queue(self.outer_fd, &mut active.to_child)? == QueueRead::Closed
        {
            return Ok(GenerationEvent::Hangup);
        }
        if descriptors[3].revents & libc::POLLIN != 0 {
            let _ = read_child_into_queue(active)?;
        }
        if descriptors[3].revents & libc::POLLOUT != 0 {
            flush_queue(active.master, &mut active.to_child)?;
        }
        if descriptors[4].revents & libc::POLLOUT != 0 {
            flush_queue(self.outer_fd, &mut active.to_outer)?;
        }
        if active.to_child.len() > MAX_PROXY_QUEUE_BYTES
            || active.to_outer.len() > MAX_PROXY_QUEUE_BYTES
        {
            return Err(io::Error::other("Codex PTY proxy queue exceeded its bound"));
        }
        Ok(GenerationEvent::ControlWake)
    }

    fn queue_pending_title(&mut self, active: &mut CodexActive) {
        if !self.title_stack_pushed
            || !active.to_outer.is_empty()
            || !active.output_filter.title_write_safe()
        {
            return;
        }
        if let Some(title) = self.pending_title.take() {
            active.to_outer.extend(title);
        }
    }

    fn finish_owned(
        &mut self,
        mut active: CodexActive,
        exit: &ExitEvidence,
    ) -> FinishAttempt<CodexActive> {
        let drained = drain_after_exit(self.outer_fd, &mut active);
        let reaped = reap_wrapper(&active);
        match (drained, reaped) {
            (Ok(()), Ok(())) => {
                close_active_fds(&mut active);
                FinishAttempt {
                    evidence: CleanupEvidence {
                        exit: Some(exit.clone()),
                        waitpid_reaped: true,
                        resources: ResourceCleanupEvidence {
                            // This chain-only adapter never creates an inject
                            // server, delivery thread, or screen ownership.
                            inject_stopped: true,
                            delivery_joined: true,
                            pty_closed: true,
                            screen_released: true,
                            write_queue_empty: true,
                        },
                        failure_kind: String::new(),
                        failure_reason: String::new(),
                    },
                    residual: None,
                }
            }
            (drain, reap) => FinishAttempt {
                evidence: CleanupEvidence {
                    exit: Some(exit.clone()),
                    waitpid_reaped: reap.is_ok(),
                    resources: ResourceCleanupEvidence {
                        inject_stopped: true,
                        delivery_joined: true,
                        pty_closed: false,
                        screen_released: true,
                        write_queue_empty: active.to_child.is_empty()
                            && active.to_outer.is_empty()
                            && drain.is_ok(),
                    },
                    failure_kind: "codex_adapter_cleanup".to_string(),
                    failure_reason: bounded_io_failure(drain.err().or_else(|| reap.err())),
                },
                residual: Some(active),
            },
        }
    }

    fn abort_owned(&mut self, mut prepared: CodexPrepared) -> FinishAttempt<CodexPrepared> {
        if prepared.gate_write >= 0 {
            close_fd(prepared.gate_write);
            prepared.gate_write = -1;
        }
        let cleanup = abort_prepared_protocol(&prepared);
        match cleanup {
            Ok(exit) => {
                close_prepared_fds(&mut prepared);
                FinishAttempt {
                    evidence: CleanupEvidence {
                        exit: Some(exit),
                        waitpid_reaped: true,
                        resources: all_resources_clean(),
                        failure_kind: String::new(),
                        failure_reason: String::new(),
                    },
                    residual: None,
                }
            }
            Err(error) => FinishAttempt {
                evidence: CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "prepared_abort_failed".to_string(),
                    failure_reason: bounded_io_failure(Some(error)),
                },
                residual: Some(prepared),
            },
        }
    }
}

impl Drop for CodexGenerationAdapter {
    fn drop(&mut self) {
        SIGNAL_WRITE_FD.store(-1, Ordering::Release);
        for saved in self.saved_signals.iter().rev() {
            // SAFETY: restores the exact dispositions captured by this owner.
            unsafe {
                libc::sigaction(saved.signal, &saved.action, std::ptr::null_mut());
            }
        }
        // SAFETY: the adapter owns raw-mode restoration for the existing TTY.
        unsafe {
            libc::tcsetattr(self.outer_fd, libc::TCSANOW, &self.saved_termios);
        }
        if self.title_stack_pushed {
            let _ = write_terminal_control(self.outer_fd, b"\x1b[23;0t");
        }
        close_many(&[self.signal_read, self.signal_write, self.outer_fd]);
    }
}

impl GenerationAdapter for CodexGenerationAdapter {
    type Active = CodexActive;
    type Prepared = CodexPrepared;
    type Error = io::Error;

    fn identity<'a>(&'a self, active: &'a Self::Active) -> &'a GenerationIdentity {
        &active.identity
    }

    fn wait_event(
        &mut self,
        active: &mut Self::Active,
        timeout: Duration,
    ) -> Result<GenerationEvent, Self::Error> {
        self.proxy_once(active, timeout)
    }

    fn send_signal(&mut self, active: &Self::Active, signal: ChainSignal) -> SignalSendResult {
        send_process_group_signal(
            active.identity.child_pid,
            active.identity.child_pgid,
            &active.identity.child_process_birth_identity,
            signal,
        )
    }

    fn resize(&mut self, active: &mut Self::Active) -> Result<(), Self::Error> {
        copy_winsize(self.outer_fd, active.master).map(|_| ())
    }

    fn reassert_outer_terminal(&mut self) -> Result<(), Self::Error> {
        verify_outer_tty(self.outer_fd, self.outer)?;
        set_raw(self.outer_fd)?;
        self.pending_title.clone_from(&self.current_title);
        Ok(())
    }

    fn set_chain_title(
        &mut self,
        generation: u64,
        state: ChainTitleState,
    ) -> Result<(), Self::Error> {
        if !self.title_stack_pushed {
            return Ok(());
        }
        let title = chain_title_sequence(generation, state);
        if self.current_title.as_deref() == Some(title.as_slice()) {
            return Ok(());
        }
        self.current_title = Some(title.clone());
        self.pending_title = Some(title);
        Ok(())
    }

    fn flush_chain_title(&mut self, active: &mut Self::Active) -> Result<(), Self::Error> {
        flush_queue(self.outer_fd, &mut active.to_outer)?;
        self.queue_pending_title(active);
        flush_queue(self.outer_fd, &mut active.to_outer)
    }

    fn finish_after_exit(
        &mut self,
        active: Self::Active,
        exit: &ExitEvidence,
    ) -> FinishAttempt<Self::Active> {
        self.finish_owned(active, exit)
    }

    fn shutdown_without_successor(
        &mut self,
        mut active: Self::Active,
        _reason: ShutdownReason,
    ) -> FinishAttempt<Self::Active> {
        let signal = send_process_group_signal(
            active.identity.child_pid,
            active.identity.child_pgid,
            &active.identity.child_process_birth_identity,
            ChainSignal::Hangup,
        );
        let exit = active
            .exit
            .take()
            .map(Ok)
            .unwrap_or_else(|| wait_for_exit(active.report_read, SHUTDOWN_EXIT_TIMEOUT));
        match exit {
            Ok(exit) => self.finish_owned(active, &exit),
            Err(error) => FinishAttempt {
                evidence: CleanupEvidence {
                    exit: None,
                    waitpid_reaped: false,
                    resources: ResourceCleanupEvidence::default(),
                    failure_kind: "codex_shutdown_failed".to_string(),
                    failure_reason: format!(
                        "SIGHUP result={signal:?}; {}",
                        bounded_io_failure(Some(error))
                    ),
                },
                residual: Some(active),
            },
        }
    }

    fn prepare_target(
        &mut self,
        reservation: &TargetReservation,
        outer: OuterTerminalIdentity,
    ) -> Result<Self::Prepared, Self::Error> {
        if outer != self.outer {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "outer terminal identity changed before Codex prepare",
            ));
        }
        verify_outer_tty(self.outer_fd, self.outer)?;
        self.spawn_prepared(reservation)
    }

    fn activate_target(
        &mut self,
        prepared: Self::Prepared,
    ) -> Result<Self::Active, (Self::Prepared, Self::Error)> {
        self.activate(prepared).map_err(|failure| *failure)
    }

    fn bind_native_session(
        &mut self,
        active: &mut Self::Active,
        native_session_id: &str,
    ) -> Result<(), Self::Error> {
        if native_session_id.is_empty()
            || native_session_id.len() > 256
            || native_session_id.chars().any(char::is_control)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native Codex session identity",
            ));
        }
        match active.identity.native_session_id.as_deref() {
            None => {
                active.identity.native_session_id = Some(native_session_id.to_string());
                Ok(())
            }
            Some(existing) if existing == native_session_id => Ok(()),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "native Codex session identity is immutable",
            )),
        }
    }

    fn abort_prepared(&mut self, prepared: Self::Prepared) -> FinishAttempt<Self::Prepared> {
        self.abort_owned(prepared)
    }
}

fn exact_generation_environment(
    chain_id: &str,
    reservation: &TargetReservation,
    identity: &GenerationIdentity,
    version: &str,
) -> Vec<(OsString, OsString)> {
    exact_generation_environment_from(
        std::env::vars_os(),
        chain_id,
        reservation,
        identity,
        version,
    )
}

fn exact_generation_environment_from<I>(
    parent: I,
    chain_id: &str,
    reservation: &TargetReservation,
    identity: &GenerationIdentity,
    version: &str,
) -> Vec<(OsString, OsString)>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let generation = reservation.generation.to_string();
    let mut environment: Vec<(OsString, OsString)> = parent
        .into_iter()
        .filter(|(key, _)| {
            let bytes = key.as_os_str().as_bytes();
            !bytes.starts_with(b"HCOM_") || bytes == b"HCOM_DIR"
        })
        .filter(|(key, _)| key != "ANTIGRAVITY_AGENT" && key != "CODEX_THREAD_ID")
        .collect();
    for (key, value) in [
        ("HCOM_PROCESS_ID", identity.process_id.as_str()),
        ("HCOM_INSTANCE_NAME", identity.instance_name.as_str()),
        ("HCOM_LAUNCHED", "1"),
        ("HCOM_PTY_MODE", "1"),
        ("HCOM_TOOL", "codex"),
        ("HCOM_CHAIN_ID", chain_id),
        ("HCOM_CHAIN_GENERATION", generation.as_str()),
        ("HCOM_CHAIN_LAUNCH_NONCE", &reservation.launch_nonce),
        (CODEX_VERSION_ENV, version),
    ] {
        environment.push((OsString::from(key), OsString::from(value)));
    }
    if reservation.handoff_id != chain_id {
        environment.push((
            OsString::from(HANDOFF_ID_ENV),
            OsString::from(&reservation.handoff_id),
        ));
    }
    environment
}

fn wrapper_main(
    supervisor_pid: i32,
    gate_read: RawFd,
    command_read: RawFd,
    report_write: RawFd,
    exec_status_write: RawFd,
    slave: RawFd,
    spec: LaunchSpec,
) -> ! {
    if arm_parent_death_hangup(supervisor_pid).is_err() {
        // SAFETY: freshly forked wrapper has no relevant Rust cleanup.
        unsafe { libc::_exit(80) }
    }
    let process_birth = match linux_process_birth_identity(unsafe { libc::getpid() }) {
        Ok(value) => value,
        Err(_) => unsafe { libc::_exit(81) },
    };
    close_fds_except(&[
        gate_read,
        command_read,
        report_write,
        exec_status_write,
        slave,
    ]);
    // SAFETY: wrapper is single-threaded and forks exactly one gated child.
    let child = unsafe { libc::fork() };
    if child == -1 {
        unsafe { libc::_exit(82) }
    }
    if child == 0 {
        close_fd(command_read);
        inner_child_main(
            unsafe { libc::getppid() },
            gate_read,
            report_write,
            exec_status_write,
            slave,
            spec,
            process_birth,
        );
    }
    close_many(&[gate_read, exec_status_write, slave]);
    let wrapper_pgid = unsafe { libc::getpgrp() };
    if write_message(
        report_write,
        WireMessage {
            kind: MSG_START,
            a: i64::from(unsafe { libc::getpid() }),
            b: i64::from(wrapper_pgid),
            c: i64::from(child),
            ..WireMessage::default()
        },
    )
    .is_err()
    {
        unsafe { libc::_exit(83) }
    }

    let mut exit_reported = false;
    loop {
        if !exit_reported && let Some(exit) = observe_child_exit_without_reap(child) {
            if write_message(report_write, exit_to_message(&exit)).is_err() {
                unsafe { libc::_exit(84) }
            }
            exit_reported = true;
        }
        let mut descriptor = libc::pollfd {
            fd: command_read,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: descriptor points to one initialized entry.
        let ready = unsafe { libc::poll(&mut descriptor, 1, 10) };
        if ready == -1 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            unsafe { libc::_exit(85) }
        }
        if descriptor.revents & libc::POLLHUP != 0 {
            abort_wrapper_child(child, report_write, exit_reported, 86)
        }
        if descriptor.revents & libc::POLLIN != 0 {
            let command = match read_byte(command_read) {
                Ok(command) => command,
                Err(_) => unsafe { libc::_exit(87) },
            };
            if command == b'A' {
                abort_wrapper_child(child, report_write, exit_reported, 87)
            }
            if command == b'R' && exit_reported {
                let reaped = reap_direct_child(child).is_ok();
                let _ = write_message(
                    report_write,
                    WireMessage {
                        kind: MSG_REAP,
                        a: i64::from(reaped),
                        b: 0b1_1111,
                        ..WireMessage::default()
                    },
                );
                close_many(&[command_read, report_write]);
                unsafe { libc::_exit(if reaped { 0 } else { 88 }) }
            }
        }
    }
}

fn abort_wrapper_child(
    child: i32,
    report_write: RawFd,
    exit_already_reported: bool,
    failure_exit: i32,
) -> ! {
    // This path is used only to revoke a private prepared/failed generation.
    // PID-directed SIGHUP is safe both before setsid and after it; SIGKILL is
    // intentionally not representable.
    unsafe {
        if libc::kill(child, libc::SIGHUP) == -1
            && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        {
            libc::_exit(failure_exit);
        }
    }
    let reaped = reap_direct_child(child);
    if !exit_already_reported && let Ok(reaped) = &reaped {
        let exit = ExitEvidence {
            observed_wall_seconds: wall_seconds(),
            observed_monotonic_ns: monotonic_ns(),
            exit_code: reaped.exit_code,
            exit_signal: reaped.exit_signal,
            delivery_context: if reaped.exit_signal.is_some() {
                DeliveryExitContext::Killed
            } else {
                DeliveryExitContext::Closed
            },
        };
        let _ = write_message(report_write, exit_to_message(&exit));
    }
    let reaped = reaped.is_ok();
    let _ = write_message(
        report_write,
        WireMessage {
            kind: MSG_REAP,
            a: i64::from(reaped),
            b: 0b1_1111,
            ..WireMessage::default()
        },
    );
    close_fd(report_write);
    unsafe { libc::_exit(if reaped { 0 } else { failure_exit }) }
}

fn inner_child_main(
    wrapper_pid: i32,
    gate_read: RawFd,
    report_write: RawFd,
    exec_status_write: RawFd,
    slave: RawFd,
    spec: LaunchSpec,
    process_birth: String,
) -> ! {
    if arm_parent_death_hangup(wrapper_pid).is_err() {
        unsafe { libc::_exit(89) }
    }
    let gate = read_byte(gate_read);
    close_fd(gate_read);
    if !matches!(gate, Ok(1)) {
        unsafe { libc::_exit(73) }
    }
    // SAFETY: the inner child creates one new session and controlling PTY.
    unsafe {
        if libc::setsid() == -1 || libc::ioctl(slave, libc::TIOCSCTTY, 0) == -1 {
            libc::_exit(90);
        }
        for fd in [0, 1, 2] {
            if libc::dup2(slave, fd) == -1 {
                libc::_exit(91);
            }
        }
    }
    if slave > 2 {
        close_fd(slave);
    }
    // SAFETY: restore child-local default dispositions.
    unsafe {
        for signal in [
            libc::SIGINT,
            libc::SIGTERM,
            libc::SIGHUP,
            libc::SIGWINCH,
            libc::SIGCONT,
            libc::SIGUSR1,
        ] {
            libc::signal(signal, libc::SIG_DFL);
        }
    }
    let _ = set_cloexec(report_write);
    let ready = WireMessage {
        kind: MSG_READY,
        a: i64::from(unsafe { libc::getpid() }),
        b: i64::from(unsafe { libc::getsid(0) }),
        c: i64::from(unsafe { libc::getpgrp() }),
        ..WireMessage::default()
    };
    if write_message(report_write, ready).is_err() {
        unsafe { libc::_exit(92) }
    }
    let errno = exec_codex(spec, &process_birth)
        .err()
        .and_then(|error| error.raw_os_error())
        .unwrap_or(libc::EINVAL);
    let bytes = errno.to_ne_bytes();
    let _ = write_all(exec_status_write, bytes.as_ptr().cast(), bytes.len());
    unsafe { libc::_exit(127) }
}

fn exec_codex(spec: LaunchSpec, process_birth: &str) -> io::Result<()> {
    let executable = cstring(spec.executable.as_os_str())?;
    let mut argv = Vec::with_capacity(spec.argv.len() + 1);
    argv.push(executable.clone());
    for argument in &spec.argv {
        argv.push(cstring(OsStr::new(argument))?);
    }
    let mut environment = spec.environment;
    environment.push((
        OsString::from("HCOM_CHAIN_PROCESS_BIRTH_IDENTITY"),
        OsString::from(process_birth),
    ));
    let env: Vec<CString> = environment
        .iter()
        .map(|(key, value)| {
            let mut bytes = key.as_os_str().as_bytes().to_vec();
            bytes.push(b'=');
            bytes.extend_from_slice(value.as_os_str().as_bytes());
            CString::new(bytes)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid environment"))
        })
        .collect::<io::Result<_>>()?;
    let mut argv_ptrs: Vec<*const libc::c_char> = argv.iter().map(|value| value.as_ptr()).collect();
    argv_ptrs.push(std::ptr::null());
    let mut env_ptrs: Vec<*const libc::c_char> = env.iter().map(|value| value.as_ptr()).collect();
    env_ptrs.push(std::ptr::null());
    let workspace = cstring(spec.workspace.as_os_str())?;
    // SAFETY: all C strings and pointer arrays remain live across the calls.
    unsafe {
        if libc::chdir(workspace.as_ptr()) == -1 {
            return Err(io::Error::last_os_error());
        }
        libc::execve(executable.as_ptr(), argv_ptrs.as_ptr(), env_ptrs.as_ptr());
    }
    Err(io::Error::last_os_error())
}

fn open_exact_outer_tty(outer: OuterTerminalIdentity) -> io::Result<RawFd> {
    // The private foreground factory captures stdin as its outer terminal.
    // Reopening that exact procfd gives the proxy an independent file
    // description while preserving the captured PTY device/inode. `/dev/tty`
    // is not sufficient here: fstat may describe the controlling-tty clone
    // device rather than the exact captured slave.
    let path = CString::new("/proc/self/fd/0").unwrap();
    // SAFETY: path is a fixed valid C string and flags create no file.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    if let Err(error) = verify_outer_tty(fd, outer) {
        close_fd(fd);
        return Err(error);
    }
    Ok(fd)
}

fn verify_outer_tty(fd: RawFd, outer: OuterTerminalIdentity) -> io::Result<()> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    // SAFETY: stat is writable and fd is caller-owned.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    let foreground = unsafe { libc::tcgetpgrp(fd) };
    if foreground == -1 {
        return Err(io::Error::last_os_error());
    }
    if stat.st_dev != outer.tty_device
        || stat.st_ino != outer.tty_inode
        || foreground != outer.foreground_pgid
        || unsafe { libc::getpid() } != outer.supervisor_pid
        || unsafe { libc::getpgrp() } != outer.supervisor_pgid
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "outer terminal identity no longer matches the supervisor",
        ));
    }
    Ok(())
}

fn install_signal_handlers(write_fd: RawFd) -> io::Result<Vec<SavedSignal>> {
    let mut saved: Vec<SavedSignal> = Vec::new();
    SIGNAL_WRITE_FD.store(write_fd, Ordering::Release);
    for signal in [
        libc::SIGINT,
        libc::SIGHUP,
        libc::SIGWINCH,
        libc::SIGCONT,
        libc::SIGUSR1,
    ] {
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = chain_signal_handler as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
        // SAFETY: both action pointers are initialized.
        if unsafe { libc::sigaction(signal, &action, &mut old) } == -1 {
            for entry in saved.iter().rev() {
                unsafe {
                    libc::sigaction(entry.signal, &entry.action, std::ptr::null_mut());
                }
            }
            SIGNAL_WRITE_FD.store(-1, Ordering::Release);
            return Err(io::Error::last_os_error());
        }
        saved.push(SavedSignal {
            signal,
            action: old,
        });
    }
    Ok(saved)
}

fn tcgetattr(fd: RawFd) -> io::Result<libc::termios> {
    let mut termios = MaybeUninit::<libc::termios>::uninit();
    if unsafe { libc::tcgetattr(fd, termios.as_mut_ptr()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { termios.assume_init() })
    }
}

fn set_raw(fd: RawFd) -> io::Result<()> {
    let mut termios = tcgetattr(fd)?;
    unsafe {
        libc::cfmakeraw(&mut termios);
        if libc::tcsetattr(fd, libc::TCSANOW, &termios) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn current_winsize(fd: RawFd) -> io::Result<libc::winsize> {
    let mut size = MaybeUninit::<libc::winsize>::zeroed();
    if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, size.as_mut_ptr()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { size.assume_init() })
    }
}

fn opaque_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(24)
            .collect::<String>()
    )
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

fn message_to_exit(message: WireMessage) -> io::Result<ExitEvidence> {
    if message.kind != MSG_EXIT || message.d <= 0 || message.c < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Codex exit evidence",
        ));
    }
    let exit_code = (message.a >= 0).then_some(message.a as i32);
    let exit_signal = (message.b > 0).then_some(message.b as i32);
    if exit_code.is_some() == exit_signal.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "contradictory Codex exit evidence",
        ));
    }
    Ok(ExitEvidence {
        observed_wall_seconds: message.d as u64,
        observed_monotonic_ns: message.c,
        exit_code,
        exit_signal,
        delivery_context: if message.e == 1 {
            DeliveryExitContext::Killed
        } else {
            DeliveryExitContext::Closed
        },
    })
}

fn wait_for_exit(fd: RawFd, timeout: Duration) -> io::Result<ExitEvidence> {
    let message = read_message_with_timeout(fd, timeout)?;
    message_to_exit(message)
}

fn abort_prepared_protocol(prepared: &CodexPrepared) -> io::Result<ExitEvidence> {
    write_byte(prepared.command_write, b'A')?;
    let deadline = Instant::now() + SHUTDOWN_EXIT_TIMEOUT;
    let mut exit = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "prepared Codex abort timed out",
            ));
        }
        let message = read_message_with_timeout(prepared.report_read, remaining)?;
        match message.kind {
            MSG_READY => {}
            MSG_EXIT => {
                let observed = message_to_exit(message)?;
                if exit.as_ref().is_some_and(|existing| existing != &observed) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "prepared Codex exit evidence changed",
                    ));
                }
                exit = Some(observed);
            }
            MSG_REAP if message.a == 1 => {
                let exit = exit.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "prepared Codex was reaped without exit evidence",
                    )
                })?;
                wait_wrapper(prepared.identity.wrapper_pid)?;
                return Ok(exit);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected prepared Codex abort report",
                ));
            }
        }
    }
}

fn reap_wrapper(active: &CodexActive) -> io::Result<()> {
    write_byte(active.command_write, b'R')?;
    let message = read_message_with_timeout(active.report_read, SHUTDOWN_EXIT_TIMEOUT)?;
    if message.kind != MSG_REAP || message.a != 1 {
        return Err(io::Error::other("Codex wrapper did not confirm child reap"));
    }
    wait_wrapper(active.identity.wrapper_pid)
}

fn wait_wrapper(pid: i32) -> io::Result<()> {
    let mut status = 0;
    let reaped = unsafe { libc::waitpid(pid, &mut status, 0) };
    if reaped != pid {
        return Err(io::Error::last_os_error());
    }
    if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
        return Err(io::Error::other("Codex wrapper exited unsuccessfully"));
    }
    Ok(())
}

fn drain_after_exit(outer_fd: RawFd, active: &mut CodexActive) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut master_closed = false;
    let mut filter_flushed = false;
    loop {
        while !master_closed && active.to_outer.len() < MAX_PROXY_QUEUE_BYTES {
            match read_child_into_queue(active)? {
                QueueRead::Data => continue,
                QueueRead::Pending => break,
                QueueRead::Closed => master_closed = true,
            }
        }
        if master_closed && !filter_flushed {
            active.to_outer.extend(active.output_filter.flush());
            filter_flushed = true;
        }
        flush_queue(outer_fd, &mut active.to_outer)?;
        flush_queue(active.master, &mut active.to_child)?;
        if master_closed && active.to_outer.is_empty() && active.to_child.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Codex PTY queues did not drain",
            ));
        }
        let mut descriptors = [
            libc::pollfd {
                fd: outer_fd,
                events: if active.to_outer.is_empty() {
                    0
                } else {
                    libc::POLLOUT
                },
                revents: 0,
            },
            libc::pollfd {
                fd: active.master,
                events: if master_closed { 0 } else { libc::POLLIN }
                    | if active.to_child.is_empty() {
                        0
                    } else {
                        libc::POLLOUT
                    },
                revents: 0,
            },
        ];
        let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, 10) };
        if ready == -1 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

fn close_active_fds(active: &mut CodexActive) {
    close_many(&[active.command_write, active.report_read, active.master]);
    active.command_write = -1;
    active.report_read = -1;
    active.master = -1;
}

fn close_prepared_fds(prepared: &mut CodexPrepared) {
    close_many(&[
        prepared.gate_write,
        prepared.command_write,
        prepared.report_read,
        prepared.exec_status_read,
        prepared.master,
    ]);
    prepared.gate_write = -1;
    prepared.command_write = -1;
    prepared.report_read = -1;
    prepared.exec_status_read = -1;
    prepared.master = -1;
}

fn all_resources_clean() -> ResourceCleanupEvidence {
    ResourceCleanupEvidence {
        inject_stopped: true,
        delivery_joined: true,
        pty_closed: true,
        screen_released: true,
        write_queue_empty: true,
    }
}

fn read_child_into_queue(active: &mut CodexActive) -> io::Result<QueueRead> {
    let available = MAX_PROXY_QUEUE_BYTES.saturating_sub(active.to_outer.len());
    if available == 0 {
        return Ok(QueueRead::Pending);
    }
    let mut buffer = [0u8; 8192];
    let limit = available.min(buffer.len());
    // SAFETY: buffer is writable for `limit` bytes and the active generation
    // owns its master descriptor.
    let read = unsafe { libc::read(active.master, buffer.as_mut_ptr().cast(), limit) };
    if read == -1 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(QueueRead::Pending);
        }
        if error.raw_os_error() == Some(libc::EIO) {
            return Ok(QueueRead::Closed);
        }
        return Err(error);
    }
    if read == 0 {
        return Ok(QueueRead::Closed);
    }
    let filtered = active.output_filter.filter(&buffer[..read as usize]);
    active.to_outer.extend(filtered);
    Ok(QueueRead::Data)
}

fn read_into_queue(fd: RawFd, queue: &mut VecDeque<u8>) -> io::Result<QueueRead> {
    let available = MAX_PROXY_QUEUE_BYTES.saturating_sub(queue.len());
    if available == 0 {
        return Ok(QueueRead::Pending);
    }
    let mut buffer = [0u8; 8192];
    let limit = available.min(buffer.len());
    let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), limit) };
    if read == -1 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(QueueRead::Pending);
        }
        if error.raw_os_error() == Some(libc::EIO) {
            return Ok(QueueRead::Closed);
        }
        return Err(error);
    }
    if read == 0 {
        return Ok(QueueRead::Closed);
    }
    queue.extend(&buffer[..read as usize]);
    Ok(QueueRead::Data)
}

fn write_terminal_control(fd: RawFd, bytes: &[u8]) -> bool {
    if fd < 0 || bytes.is_empty() {
        return false;
    }
    // These controls are shorter than a TTY's atomic write bound, but the
    // exact outer descriptor is intentionally nonblocking. Give a transient
    // EAGAIN a small bounded POLLOUT window so a successful title-stack push
    // is paired with a reliable pop during teardown. Failure still only
    // disables title management; it never affects durable state or ownership.
    let deadline = Instant::now() + TITLE_CONTROL_TIMEOUT;
    let mut offset = 0;
    while offset < bytes.len() {
        let written =
            unsafe { libc::write(fd, bytes[offset..].as_ptr().cast(), bytes.len() - offset) };
        if written > 0 {
            offset += written as usize;
            continue;
        }
        if written == 0 {
            return false;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() != io::ErrorKind::WouldBlock {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let timeout_ms = remaining.as_millis().clamp(1, libc::c_int::MAX as u128) as libc::c_int;
        let mut descriptor = libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
        if ready == 0 {
            return false;
        }
        if ready == -1 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        if descriptor.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return false;
        }
    }
    true
}

fn chain_title_sequence(generation: u64, state: ChainTitleState) -> Vec<u8> {
    let title = if state == ChainTitleState::NeedsRecovery {
        "hcom codex needs-recovery".to_string()
    } else {
        format!("hcom codex g{} {}", generation.max(1), state.as_str())
    };
    format!("\x1b]0;{title}\x07").into_bytes()
}

fn flush_queue(fd: RawFd, queue: &mut VecDeque<u8>) -> io::Result<()> {
    if queue.is_empty() {
        return Ok(());
    }
    let (first, _) = queue.as_slices();
    let written = unsafe { libc::write(fd, first.as_ptr().cast(), first.len()) };
    if written == -1 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(());
        }
        return Err(error);
    }
    queue.drain(..written as usize);
    Ok(())
}

fn verify_exec_success(fd: RawFd, timeout: Duration) -> io::Result<()> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLHUP,
        revents: 0,
    };
    let ready = unsafe {
        libc::poll(
            &mut descriptor,
            1,
            timeout.as_millis().min(i32::MAX as u128) as i32,
        )
    };
    if ready <= 0 {
        return if ready == 0 {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Codex exec status timed out",
            ))
        } else {
            Err(io::Error::last_os_error())
        };
    }
    let mut bytes = [0u8; 4];
    let read = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
    if read == 0 {
        Ok(())
    } else if read == 4 {
        Err(io::Error::from_raw_os_error(i32::from_ne_bytes(bytes)))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Codex exec status",
        ))
    }
}

fn pipe_cloexec() -> io::Result<(RawFd, RawFd)> {
    let mut descriptors = [-1; 2];
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok((descriptors[0], descriptors[1]))
    }
}

fn pipe_cloexec_owned() -> io::Result<(OwnedFd, OwnedFd)> {
    let (read, write) = pipe_cloexec()?;
    // SAFETY: pipe_cloexec returned two fresh, independently owned fds.
    Ok(unsafe { (OwnedFd::from_raw_fd(read), OwnedFd::from_raw_fd(write)) })
}

fn openpty_owned(winsize: libc::winsize) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master = -1;
    let mut slave = -1;
    // SAFETY: openpty initializes both descriptor slots on success.
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
    // SAFETY: openpty returned two fresh, independently owned fds.
    Ok(unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) })
}

fn abort_failed_prepare(
    wrapper_pid: i32,
    gate_write: RawFd,
    command_write: RawFd,
    report_read: RawFd,
    exec_status_read: RawFd,
    master: RawFd,
) {
    // Gate closure makes exec impossible. The wrapper's abort command
    // PID-signals and reaps its exact gated child; command POLLHUP is the
    // fallback when the write races wrapper startup.
    close_fd(gate_write);
    let _ = write_byte(command_write, b'A');
    close_fd(command_write);
    wait_wrapper_termination(wrapper_pid);
    close_many(&[report_read, exec_status_read, master]);
}

fn wait_wrapper_termination(pid: i32) {
    loop {
        let mut status = 0;
        let result = unsafe { libc::waitpid(pid, &mut status, 0) };
        if result == pid
            || (result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD))
        {
            return;
        }
        if result == -1 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return;
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
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
    Ok(unsafe { message.assume_init() })
}

fn try_read_message(fd: RawFd) -> io::Result<Option<WireMessage>> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = unsafe { libc::poll(&mut descriptor, 1, 0) };
    if ready == -1 {
        return Err(io::Error::last_os_error());
    }
    if ready == 0 || descriptor.revents & libc::POLLIN == 0 {
        return Ok(None);
    }
    read_message(fd).map(Some)
}

fn read_message_with_timeout(fd: RawFd, timeout: Duration) -> io::Result<WireMessage> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLHUP,
        revents: 0,
    };
    let ready = unsafe {
        libc::poll(
            &mut descriptor,
            1,
            timeout.as_millis().min(i32::MAX as u128) as i32,
        )
    };
    if ready == 0 {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Codex wrapper report timed out",
        ));
    }
    if ready == -1 {
        return Err(io::Error::last_os_error());
    }
    read_message(fd)
}

fn write_all(fd: RawFd, mut data: *const libc::c_void, mut len: usize) -> io::Result<()> {
    while len > 0 {
        let written = unsafe { libc::write(fd, data, len) };
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
        data = unsafe { data.cast::<u8>().add(written as usize).cast() };
        len -= written as usize;
    }
    Ok(())
}

fn read_exact(fd: RawFd, mut data: *mut libc::c_void, mut len: usize) -> io::Result<()> {
    while len > 0 {
        let read = unsafe { libc::read(fd, data, len) };
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
        data = unsafe { data.cast::<u8>().add(read as usize).cast() };
        len -= read as usize;
    }
    Ok(())
}

fn close_fds_except(keep: &[RawFd]) {
    let mut keep: Vec<u32> = keep
        .iter()
        .copied()
        .filter(|fd| *fd >= 3)
        .map(|fd| fd as u32)
        .collect();
    keep.sort_unstable();
    keep.dedup();
    let mut start = 3u32;
    for fd in keep {
        if start < fd {
            unsafe {
                libc::syscall(libc::SYS_close_range, start, fd - 1, 0);
            }
        }
        start = fd.saturating_add(1);
    }
    unsafe {
        libc::syscall(libc::SYS_close_range, start, u32::MAX, 0);
    }
}

fn close_fd(fd: RawFd) {
    if fd >= 0 {
        unsafe {
            libc::close(fd);
        }
    }
}

fn close_many(fds: &[RawFd]) {
    for fd in fds {
        close_fd(*fd);
    }
}

fn cstring(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value contains NUL"))
}

fn monotonic_ns() -> i64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } == -1 {
        return 0;
    }
    value
        .tv_sec
        .saturating_mul(1_000_000_000)
        .saturating_add(value.tv_nsec)
}

fn wall_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn bounded_io_failure(error: Option<io::Error>) -> String {
    let value = error
        .map(|error| error.kind().to_string())
        .unwrap_or_else(|| "unknown cleanup failure".to_string());
    value.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{Read as _, Write as _};
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::process::Stdio;
    use std::time::Duration;

    use hcom::chain_supervisor::{
        ForegroundChainSupervisor, GenerationEvent, SupervisorRunOutcome, TraceKind,
    };
    use rusqlite::{OptionalExtension, params};

    use super::phase3_mock_http::{MockHttp, RecordedRequest, Reply};
    use crate::chain_control::HcomChainControl;
    use crate::db::HcomDb;
    use crate::handoff::{
        self, ChainSpec, ChainState, HandoffActor, HandoffState, SupervisorActor, TerminalChain,
        create_chain_with_id, prepare_handoff,
    };

    #[test]
    fn exit_drain_consumes_more_than_one_read_before_reporting_clean() {
        let payload: Vec<u8> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
        let (source, mut source_peer) = UnixStream::pair().unwrap();
        let (sink, mut sink_peer) = UnixStream::pair().unwrap();
        source.set_nonblocking(true).unwrap();
        sink.set_nonblocking(true).unwrap();

        let expected = payload.clone();
        let writer = std::thread::spawn(move || {
            source_peer.write_all(&payload).unwrap();
            source_peer.shutdown(Shutdown::Write).unwrap();
        });
        let reader = std::thread::spawn(move || {
            let mut output = Vec::new();
            sink_peer.read_to_end(&mut output).unwrap();
            output
        });
        let mut active = CodexActive {
            identity: GenerationIdentity {
                generation: 1,
                launch_nonce: "nonce".to_string(),
                wrapper_pid: 100,
                wrapper_pgid: 99,
                child_pid: 101,
                child_pgid: 101,
                child_process_birth_identity: "birth-child".to_string(),
                process_id: "process".to_string(),
                process_birth_identity: "birth-wrapper".to_string(),
                instance_name: "instance".to_string(),
                hcom_session_id: "hcom-session".to_string(),
                native_session_id: Some("native-session".to_string()),
            },
            command_write: -1,
            report_read: -1,
            master: source.as_raw_fd(),
            to_child: VecDeque::new(),
            to_outer: VecDeque::new(),
            output_filter: crate::pty::ChainTitleOutputFilter::new(),
            exit: None,
        };

        drain_after_exit(sink.as_raw_fd(), &mut active).unwrap();
        assert!(active.to_outer.is_empty());
        writer.join().unwrap();
        sink.shutdown(Shutdown::Write).unwrap();
        assert_eq!(reader.join().unwrap(), expected);
    }

    #[test]
    fn exact_environment_contains_only_opaque_chain_metadata() {
        let secret = "PHASE3_RAW_SECRET_SENTINEL";
        let reservation = TargetReservation {
            handoff_id: "ho-opaque".to_string(),
            expected_version: 7,
            generation: 2,
            launch_nonce: "nonce-opaque".to_string(),
        };
        let identity = GenerationIdentity {
            generation: 2,
            launch_nonce: reservation.launch_nonce.clone(),
            wrapper_pid: 10,
            wrapper_pgid: 11,
            child_pid: 12,
            child_pgid: 12,
            child_process_birth_identity: "child-birth".to_string(),
            process_id: "process-opaque".to_string(),
            process_birth_identity: "wrapper-birth".to_string(),
            instance_name: "chain_g2_test".to_string(),
            hcom_session_id: "hcom-opaque".to_string(),
            native_session_id: None,
        };
        let environment = exact_generation_environment_from(
            [
                (OsString::from("PATH"), OsString::from("/usr/bin")),
                (OsString::from("HCOM_SOURCE_SECRET"), OsString::from(secret)),
                (OsString::from("ANTIGRAVITY_AGENT"), OsString::from(secret)),
                (OsString::from("CODEX_THREAD_ID"), OsString::from(secret)),
                (OsString::from("HCOM_DIR"), OsString::from("/tmp/hcom")),
            ],
            "tc-opaque",
            &reservation,
            &identity,
            SUPPORTED_CODEX_VERSION,
        );
        assert!(!environment.iter().any(|(_, value)| value == secret));
        assert!(environment.iter().any(|(key, value)| {
            key == HANDOFF_ID_ENV && value == reservation.handoff_id.as_str()
        }));
        assert!(
            !environment
                .iter()
                .any(|(key, _)| key == "HCOM_SOURCE_SECRET")
        );
        assert!(
            !environment
                .iter()
                .any(|(key, _)| key == "ANTIGRAVITY_AGENT")
        );
        assert!(!environment.iter().any(|(key, _)| key == "CODEX_THREAD_ID"));
        assert!(environment.iter().any(|(key, _)| key == "HCOM_DIR"));
    }

    #[test]
    fn initial_environment_and_titles_are_bounded_and_private() {
        let chain_id = "tc-initial-opaque";
        let reservation = TargetReservation {
            handoff_id: chain_id.to_string(),
            expected_version: 0,
            generation: 1,
            launch_nonce: "initial-launch-nonce".to_string(),
        };
        let identity = GenerationIdentity {
            generation: 1,
            launch_nonce: reservation.launch_nonce.clone(),
            wrapper_pid: 10,
            wrapper_pgid: 11,
            child_pid: 12,
            child_pgid: 12,
            child_process_birth_identity: "child-birth".to_string(),
            process_id: "initial-process".to_string(),
            process_birth_identity: "wrapper-birth".to_string(),
            instance_name: "chain-g1".to_string(),
            hcom_session_id: "initial-hcom-session".to_string(),
            native_session_id: None,
        };
        let environment = exact_generation_environment_from(
            [(OsString::from("PATH"), OsString::from("/usr/bin"))],
            chain_id,
            &reservation,
            &identity,
            SUPPORTED_CODEX_VERSION,
        );
        assert!(!environment.iter().any(|(key, _)| key == HANDOFF_ID_ENV));
        for (key, value) in [
            ("HCOM_CHAIN_ID", chain_id),
            ("HCOM_CHAIN_GENERATION", "1"),
            ("HCOM_CHAIN_LAUNCH_NONCE", "initial-launch-nonce"),
            (CODEX_VERSION_ENV, SUPPORTED_CODEX_VERSION),
        ] {
            assert!(
                environment.iter().any(|(actual_key, actual_value)| {
                    actual_key == key && actual_value == value
                })
            );
        }

        let secret = "PRIVATE_NATIVE_OR_BUNDLE_SENTINEL";
        for (generation, state, expected) in [
            (
                1,
                ChainTitleState::Active,
                "\u{1b}]0;hcom codex g1 active\u{7}",
            ),
            (
                2,
                ChainTitleState::AwaitingAcceptance,
                "\u{1b}]0;hcom codex g2 awaiting-acceptance\u{7}",
            ),
            (
                0,
                ChainTitleState::NeedsRecovery,
                "\u{1b}]0;hcom codex needs-recovery\u{7}",
            ),
        ] {
            let title = chain_title_sequence(generation, state);
            assert_eq!(title, expected.as_bytes());
            assert!(title.len() < 128);
            assert!(!String::from_utf8_lossy(&title).contains(secret));
            assert!(!String::from_utf8_lossy(&title).contains(chain_id));
        }
    }

    #[test]
    #[serial_test::serial]
    fn adapter_drop_restores_the_terminal_title_stack() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = std::fs::canonicalize(directory.path()).unwrap();
        let chain = TerminalChain {
            id: "tc-title".to_string(),
            workspace: workspace.to_string_lossy().into_owned(),
            tool: "codex".to_string(),
            model_ref: "gpt-test".to_string(),
            reasoning_ref: "high".to_string(),
            permission_policy_ref: "approval=never;sandbox=read-only".to_string(),
            policy_ref: "codex-0.145.0-foreground-v1".to_string(),
            supervisor_process_id: "supervisor".to_string(),
            supervisor_process_birth_identity: "birth".to_string(),
            supervisor_pid: Some(10),
            supervisor_pgid: Some(10),
            outer_foreground_pgid: Some(10),
            outer_tty_device: Some(7),
            outer_tty_inode: Some(11),
            current_generation: 1,
            state: ChainState::Active,
            version: 0,
            created_at: 0.0,
            updated_at: 0.0,
        };
        let profile = CodexLaunchProfile::from_chain(&chain).unwrap();
        let size = libc::winsize {
            ws_row: 24,
            ws_col: 80,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let (master, slave) = openpty_owned(size).unwrap();
        set_nonblocking(master.as_raw_fd()).unwrap();
        let saved_termios = tcgetattr(slave.as_raw_fd()).unwrap();
        assert!(write_terminal_control(slave.as_raw_fd(), b"\x1b[22;0t"));
        let adapter = CodexGenerationAdapter {
            outer: OuterTerminalIdentity {
                supervisor_pid: 10,
                supervisor_pgid: 10,
                foreground_pgid: 10,
                tty_device: 7,
                tty_inode: 11,
            },
            outer_fd: slave.into_raw_fd(),
            saved_termios,
            signal_read: -1,
            signal_write: -1,
            saved_signals: Vec::new(),
            executable: PathBuf::from("/usr/bin/false"),
            profile,
            chain_id: chain.id,
            title_stack_pushed: true,
            current_title: None,
            pending_title: None,
        };
        drop(adapter);

        let mut output = Vec::new();
        let mut buffer = [0u8; 128];
        loop {
            let read =
                unsafe { libc::read(master.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            if read > 0 {
                output.extend_from_slice(&buffer[..read as usize]);
            } else {
                break;
            }
        }
        assert!(output.windows(7).any(|window| window == b"\x1b[22;0t"));
        assert!(output.windows(7).any(|window| window == b"\x1b[23;0t"));
    }

    #[derive(Clone, Debug)]
    struct DbHandoff {
        id: String,
        state: String,
        version: i64,
        validated: bool,
    }

    fn read_db_handoff(db_path: &Path) -> Option<DbHandoff> {
        let connection = rusqlite::Connection::open(db_path).ok()?;
        connection
            .query_row(
                "SELECT id, state, version, target_validated_at IS NOT NULL
                 FROM terminal_handoffs ORDER BY created_at DESC LIMIT 1",
                [],
                |row| {
                    Ok(DbHandoff {
                        id: row.get(0)?,
                        state: row.get(1)?,
                        version: row.get(2)?,
                        validated: row.get(3)?,
                    })
                },
            )
            .optional()
            .ok()
            .flatten()
    }

    fn await_db_handoff<F>(db_path: &Path, predicate: F) -> DbHandoff
    where
        F: Fn(&DbHandoff) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(value) = read_db_handoff(db_path)
                && predicate(&value)
            {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for real-adapter durable handoff state"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn response_sse(id: &str, item: serde_json::Value) -> Vec<u8> {
        let created = serde_json::json!({
            "type": "response.created",
            "response": {"id": id}
        });
        let done = serde_json::json!({
            "type": "response.output_item.done",
            "item": item
        });
        let completed = serde_json::json!({
            "type": "response.completed",
            "response": {
                "id": id,
                "usage": {
                    "input_tokens": 0,
                    "input_tokens_details": null,
                    "output_tokens": 0,
                    "output_tokens_details": null,
                    "total_tokens": 0
                }
            }
        });
        let mut output = String::new();
        for (event, value) in [
            ("response.created", created),
            ("response.output_item.done", done),
            ("response.completed", completed),
        ] {
            output.push_str("event: ");
            output.push_str(event);
            output.push_str("\ndata: ");
            output.push_str(&serde_json::to_string(&value).unwrap());
            output.push_str("\n\n");
        }
        output.into_bytes()
    }

    fn shell_call_sse(response_id: &str, call_id: &str, command: &str) -> Vec<u8> {
        response_sse(
            response_id,
            serde_json::json!({
                "type": "function_call",
                "call_id": call_id,
                "name": "exec_command",
                "arguments": serde_json::json!({"cmd": command}).to_string()
            }),
        )
    }

    fn message_sse(response_id: &str, item_id: &str, text: &str) -> Vec<u8> {
        response_sse(
            response_id,
            serde_json::json!({
                "type": "message",
                "role": "assistant",
                "id": item_id,
                "content": [{"type": "output_text", "text": text}]
            }),
        )
    }

    fn shell_quote(value: &Path) -> String {
        format!("'{}'", value.to_string_lossy().replace('\'', "'\\''"))
    }

    fn configure_real_fixture(
        root: &Path,
        workspace: &Path,
        hcom_bin: &Path,
        base_url: &str,
        path: &OsStr,
    ) {
        let home = root.join("home");
        let hcom_dir = root.join("hcom");
        let codex_home = root.join("codex");
        for directory in [&home, &hcom_dir, &codex_home] {
            fs::create_dir_all(directory).unwrap();
        }
        let workspace_key = workspace.to_string_lossy().replace('\\', "\\\\");
        let config = format!(
            "model = \"gpt-5.5\"\n\
             model_provider = \"mock_local\"\n\
             \n\
             [model_providers.mock_local]\n\
             name = \"Local Mock\"\n\
             base_url = \"{base_url}\"\n\
             env_key = \"DUMMY_KEY\"\n\
             wire_api = \"responses\"\n\
             requires_openai_auth = false\n\
             \n\
             [projects.\"{workspace_key}\"]\n\
             trust_level = \"trusted\"\n"
        );
        fs::write(codex_home.join("config.toml"), config).unwrap();
        let output = Command::new(hcom_bin)
            .args(["hooks", "add", "codex"])
            .env_clear()
            .env("PATH", path)
            .env("HOME", &home)
            .env("HCOM_DIR", &hcom_dir)
            .env("CODEX_HOME", &codex_home)
            .env("DUMMY_KEY", "phase3-localhost-only")
            .env("TERM", "xterm-256color")
            .env("LANG", "C.UTF-8")
            .output()
            .expect("install isolated Codex hooks");
        assert!(
            output.status.success(),
            "isolated hook install failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn production_hcom_for_test() -> PathBuf {
        let build = Command::new(env!("CARGO"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(["build", "--quiet", "--bin", "hcom"])
            .output()
            .expect("build production hcom for isolated real probe");
        assert!(
            build.status.success(),
            "production hcom build failed: {}",
            String::from_utf8_lossy(&build.stderr)
        );
        let current = std::env::current_exe().unwrap();
        let debug = current
            .parent()
            .and_then(Path::parent)
            .expect("test executable must be below target/debug/deps");
        let binary = debug.join("hcom");
        assert!(
            binary.is_file(),
            "production hcom build did not create target/debug/hcom"
        );
        binary
    }

    fn setup_git_workspace(workspace: &Path) {
        fs::create_dir_all(workspace).unwrap();
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.name", "hcom phase3"],
            vec!["config", "user.email", "phase3@example.invalid"],
        ] {
            let output = Command::new("git")
                .arg("-C")
                .arg(workspace)
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success());
        }
        fs::write(
            workspace.join("AGENTS.md"),
            "Phase 3 fixture instructions.\n",
        )
        .unwrap();
        fs::write(workspace.join("README.md"), "phase3\n").unwrap();
        let output = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["add", "AGENTS.md", "README.md"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let output = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["commit", "-m", "fixture"])
            .output()
            .unwrap();
        assert!(output.status.success());
    }

    fn start_real_mock(root: &Path, hcom_bin: &Path) -> MockHttp {
        let db_path = root.join("hcom/hcom.db");
        let accepted_marker = root.join("accepted-output-observed");
        let hcom = shell_quote(hcom_bin);
        MockHttp::start(move |request: &RecordedRequest| {
            let body = &request.body;
            let has_output = |id: &str| body.contains("function_call_output") && body.contains(id);
            if has_output("CALL_ACCEPT") {
                if body.contains("not_managed") || body.contains("handoff_conflict") {
                    return Reply::Status(500);
                }
                fs::write(&accepted_marker, b"1").unwrap();
                return Reply::Sse(message_sse(
                    "RESP_B_DONE",
                    "ITEM_B_DONE",
                    "PHASE3_TARGET_ACCEPTED",
                ));
            }
            if has_output("CALL_INSPECT") {
                if body.contains("not_managed") || body.contains("handoff_conflict") {
                    return Reply::Status(500);
                }
                let value = await_db_handoff(&db_path, |value| {
                    value.state == "awaiting_acceptance" && value.validated
                });
                let command = format!(
                    "{hcom} handoff accept {} --version {} --json",
                    value.id, value.version
                );
                return Reply::Sse(shell_call_sse("RESP_B_ACCEPT", "CALL_ACCEPT", &command));
            }
            if has_output("CALL_COMMIT") {
                if body.contains("not_managed") || body.contains("handoff_conflict") {
                    return Reply::Status(500);
                }
                let _ = await_db_handoff(&db_path, |value| {
                    matches!(value.state.as_str(), "committed" | "stop_observed")
                });
                return Reply::Sse(message_sse(
                    "RESP_A_DONE",
                    "ITEM_A_DONE",
                    "PHASE3_SOURCE_COMMITTED",
                ));
            }
            let value = await_db_handoff(&db_path, |value| {
                matches!(value.state.as_str(), "prepared" | "awaiting_acceptance")
            });
            if value.state == "prepared" {
                let command = format!(
                    "{hcom} handoff commit {} --version {} --json",
                    value.id, value.version
                );
                Reply::Sse(shell_call_sse("RESP_A_COMMIT", "CALL_COMMIT", &command))
            } else {
                let command = format!(
                    "{hcom} handoff inspect {} --version {} --json",
                    value.id, value.version
                );
                Reply::Sse(shell_call_sse("RESP_B_INSPECT", "CALL_INSPECT", &command))
            }
        })
        .unwrap()
    }

    fn child_environment(command: &mut Command, root: &Path, path: &OsStr, bundle_secret: &str) {
        command
            .env_clear()
            .env("PATH", path)
            .env("HOME", root.join("home"))
            .env("HCOM_DIR", root.join("hcom"))
            .env("CODEX_HOME", root.join("codex"))
            .env("DUMMY_KEY", "phase3-localhost-only")
            .env("HCOM_PHASE3_REAL_INNER", "1")
            .env("HCOM_SOURCE_SECRET", format!("ENV_{bundle_secret}"))
            .env("CODEX_THREAD_ID", format!("STALE_NATIVE_{bundle_secret}"))
            .env("TERM", "xterm-256color")
            .env("LANG", "C.UTF-8");
    }

    fn run_real_outer() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        setup_git_workspace(&workspace);
        let hcom_bin = production_hcom_for_test();
        let mut path_entries = vec![hcom_bin.parent().unwrap().to_path_buf()];
        path_entries.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let path = std::env::join_paths(path_entries).unwrap();
        let mock = start_real_mock(root.path(), &hcom_bin);
        configure_real_fixture(
            root.path(),
            &workspace,
            &hcom_bin,
            &format!("http://127.0.0.1:{}/v1", mock.port()),
            &path,
        );
        let bundle_secret = format!("PHASE3_RAW_SECRET_{}", std::process::id());
        fs::write(root.path().join("bundle-secret"), &bundle_secret).unwrap();

        let mut master = -1;
        let mut slave = -1;
        let size = libc::winsize {
            ws_row: 40,
            ws_col: 120,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    &size,
                )
            },
            0
        );
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "codex_chain_adapter::tests::bounded_real_fresh_a_to_b_adapter_probe",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        child_environment(&mut command, root.path(), &path, &bundle_secret);
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1
                    || libc::ioctl(slave, libc::TIOCSCTTY, 0) == -1
                    || libc::dup2(slave, 0) == -1
                    || libc::dup2(slave, 1) == -1
                    || libc::dup2(slave, 2) == -1
                {
                    return Err(io::Error::last_os_error());
                }
                if slave > 2 {
                    libc::close(slave);
                }
                libc::close(master);
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let child_pid = child.id() as i32;
        close_fd(slave);
        let output_reader = std::thread::spawn(move || {
            let mut file = unsafe { fs::File::from_raw_fd(master) };
            let mut output = Vec::new();
            let mut buffer = [0u8; 8192];
            while let Ok(count) = file.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                if output.len() < 1024 * 1024 {
                    let keep = count.min(1024 * 1024 - output.len());
                    output.extend_from_slice(&buffer[..keep]);
                }
            }
            output
        });
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut timed_out = false;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(-child_pid, libc::SIGHUP);
                }
                timed_out = true;
                break child.wait().unwrap();
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let terminal_output = output_reader.join().unwrap();
        let db_diagnostic = real_db_diagnostic(&root.path().join("hcom/hcom.db"));
        assert!(
            !timed_out,
            "bounded real Phase 3 child timed out:\ndb={db_diagnostic}\n{}",
            String::from_utf8_lossy(&terminal_output)
        );
        assert!(
            status.success(),
            "real Phase 3 child failed:\n{}",
            String::from_utf8_lossy(&terminal_output)
        );
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(root.path().join("phase3-report.json")).unwrap())
                .unwrap();
        assert_ne!(report["source_native"], report["target_native"]);
        assert_eq!(report["max_live_codex_children"], 1);
        assert_eq!(report["automatic_sigkill_count"], 0);
        assert!(mock.unexpected().is_empty());
        assert!(mock.transport_errors().is_empty());
        let requests = mock.request_bodies();
        let validation_token: String = rusqlite::Connection::open(root.path().join("hcom/hcom.db"))
            .unwrap()
            .query_row(
                "SELECT target_validation_token
                 FROM terminal_handoffs ORDER BY created_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            requests
                .iter()
                .all(|request| !request.contains(&validation_token)),
            "DB-internal validation authorization leaked into a model request"
        );
        let target_initial = requests
            .iter()
            .find(|body| {
                body.contains("Continue hcom handoff ho-") && !body.contains("function_call_output")
            })
            .expect("fresh target request");
        assert!(!target_initial.contains(&bundle_secret));
        assert!(!target_initial.contains(&format!("ENV_{bundle_secret}")));
        assert!(!target_initial.contains(&format!("STALE_NATIVE_{bundle_secret}")));
        assert!(
            !target_initial.contains(
                report["source_native"]
                    .as_str()
                    .expect("source native session report")
            )
        );
        assert!(!target_initial.contains("Continue hcom handoff initial-tc-"));
        assert!(!target_initial.contains("PHASE3_SOURCE_COMMITTED"));
        assert!(!target_initial.contains("When done, send your result back"));
        println!(
            "PHASE3_REAL_JSON {}",
            serde_json::json!({
                "fresh_native_sessions_distinct": true,
                "sigterm_to_target_ready_ms": report["sigterm_to_target_ready_ms"],
                "max_live_codex_children": report["max_live_codex_children"],
                "automatic_sigkill_count": report["automatic_sigkill_count"],
                "private_gate_revoke_reaped": true,
                "trace": report["trace"],
            })
        );
    }

    fn real_db_diagnostic(path: &Path) -> String {
        let Ok(connection) = rusqlite::Connection::open(path) else {
            return "db-unavailable".to_string();
        };
        let instances: Vec<(String, Option<String>)> = connection
            .prepare("SELECT name, session_id FROM instances ORDER BY name")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect()
            })
            .unwrap_or_default();
        let bindings: Vec<(String, Option<String>, Option<String>)> = connection
            .prepare(
                "SELECT process_id, session_id, instance_name
                 FROM process_bindings ORDER BY process_id",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect()
            })
            .unwrap_or_default();
        format!("instances={instances:?} bindings={bindings:?}")
    }

    fn actor_from_identity(identity: &GenerationIdentity) -> HandoffActor {
        HandoffActor {
            instance_name: identity.instance_name.clone(),
            hcom_session_id: identity.hcom_session_id.clone(),
            native_session_id: identity.native_session_id.clone(),
            process_id: identity.process_id.clone(),
            process_birth_identity: identity.process_birth_identity.clone(),
            generation: identity.generation as i64,
        }
    }

    fn pump_until_native(
        db: &HcomDb,
        chain_id: &str,
        adapter: &mut CodexGenerationAdapter,
        active: &mut CodexActive,
    ) -> String {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(native) =
                handoff::get_generation(db, chain_id, active.identity.generation as i64)
                    .unwrap()
                    .and_then(|generation| generation.native_session_id)
            {
                return native;
            }
            if let GenerationEvent::ChildExited(exit) = adapter
                .wait_event(active, Duration::from_millis(100))
                .unwrap()
            {
                panic!("Codex exited before SessionStart: {exit:?}")
            }
            assert!(Instant::now() < deadline, "SessionStart timed out");
        }
    }

    fn transcript_has_task_complete(path: &str) -> bool {
        fs::read_to_string(path).is_ok_and(|body| {
            body.lines().any(|line| {
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|value| {
                        value
                            .pointer("/payload/type")
                            .and_then(serde_json::Value::as_str)
                            .map(|kind| kind == "task_complete")
                    })
                    .unwrap_or(false)
            })
        })
    }

    fn run_real_inner() {
        let home = PathBuf::from(std::env::var_os("HOME").unwrap());
        let root = home.parent().unwrap().to_path_buf();
        let workspace = fs::canonicalize(root.join("workspace")).unwrap();
        let hcom_dir = root.join("hcom");
        let db_path = hcom_dir.join("hcom.db");
        let bundle_secret = fs::read_to_string(root.join("bundle-secret")).unwrap();
        let environment_secret = std::env::var("HCOM_SOURCE_SECRET").unwrap();
        let stale_native_secret = std::env::var("CODEX_THREAD_ID").unwrap();
        let outer = OuterTerminalIdentity::capture(0).unwrap();
        let supervisor = SupervisorActor {
            process_id: opaque_id("supervisor"),
            process_birth_identity: linux_process_birth_identity(outer.supervisor_pid).unwrap(),
        };
        let db = HcomDb::open_at(&db_path).unwrap();
        let chain_id = handoff::allocate_chain_id();
        let launch_nonce = opaque_id("launch-source");
        let profile = TerminalChain {
            id: chain_id.clone(),
            workspace: workspace.to_string_lossy().into_owned(),
            tool: "codex".to_string(),
            model_ref: "gpt-5.5".to_string(),
            reasoning_ref: "high".to_string(),
            permission_policy_ref: "approval=never;sandbox=danger-full-access".to_string(),
            policy_ref: "phase3-fixed-profile-v1".to_string(),
            supervisor_process_id: supervisor.process_id.clone(),
            supervisor_process_birth_identity: supervisor.process_birth_identity.clone(),
            supervisor_pid: Some(i64::from(outer.supervisor_pid)),
            supervisor_pgid: Some(i64::from(outer.supervisor_pgid)),
            outer_foreground_pgid: Some(i64::from(outer.foreground_pgid)),
            outer_tty_device: Some(outer.tty_device as i64),
            outer_tty_inode: Some(outer.tty_inode as i64),
            current_generation: 1,
            state: ChainState::Active,
            version: 0,
            created_at: 0.0,
            updated_at: 0.0,
        };
        let mut adapter = CodexGenerationAdapter::new(&profile, outer).unwrap();
        eprintln!("phase3-real stage=adapter-ready");
        let revoked = adapter
            .spawn_prepared(&TargetReservation {
                handoff_id: "ho-private-gate-revoke-probe".to_string(),
                expected_version: 0,
                generation: 99,
                launch_nonce: "nonce-private-gate-revoke".to_string(),
            })
            .unwrap();
        let revoked_wrapper = revoked.identity.wrapper_pid;
        let revoked_child = revoked.identity.child_pid;
        let revoked = adapter.abort_owned(revoked);
        assert!(revoked.evidence.successful(), "{:?}", revoked.evidence);
        assert!(revoked.residual.is_none());
        assert_eq!(unsafe { libc::kill(revoked_wrapper, 0) }, -1);
        assert_eq!(unsafe { libc::kill(revoked_child, 0) }, -1);
        eprintln!("phase3-real stage=private-gate-revoked");
        let source_reservation = TargetReservation {
            handoff_id: format!("initial-{chain_id}"),
            expected_version: 0,
            generation: 1,
            launch_nonce: launch_nonce.clone(),
        };
        let prepared = adapter.spawn_prepared(&source_reservation).unwrap();
        eprintln!("phase3-real stage=source-prepared");
        let source_before_native = prepared.identity.clone();
        let source_actor = actor_from_identity(&source_before_native);
        db.conn()
            .execute(
                "INSERT INTO instances (name, session_id, status, tool, created_at)
                 VALUES (?1, ?2, 'launching', 'codex', 1.0)",
                params![source_actor.instance_name, source_actor.hcom_session_id],
            )
            .unwrap();
        db.set_process_binding(
            &source_actor.process_id,
            &source_actor.hcom_session_id,
            &source_actor.instance_name,
        )
        .unwrap();
        let chain = create_chain_with_id(
            &db,
            &source_actor,
            &ChainSpec {
                workspace: workspace.clone(),
                tool: "codex".to_string(),
                model_ref: profile.model_ref.clone(),
                reasoning_ref: profile.reasoning_ref.clone(),
                permission_policy_ref: profile.permission_policy_ref.clone(),
                policy_ref: profile.policy_ref.clone(),
                supervisor_process_id: supervisor.process_id.clone(),
                supervisor_process_birth_identity: supervisor.process_birth_identity.clone(),
                supervisor_pid: i64::from(outer.supervisor_pid),
                supervisor_pgid: i64::from(outer.supervisor_pgid),
                outer_foreground_pgid: i64::from(outer.foreground_pgid),
                outer_tty_device: outer.tty_device as i64,
                outer_tty_inode: outer.tty_inode as i64,
                launch_nonce,
            },
            &chain_id,
        )
        .unwrap();
        eprintln!("phase3-real stage=chain-created");
        let mut source = match adapter.activate_target(prepared) {
            Ok(source) => source,
            Err((_prepared, error)) => panic!("initial Codex activation failed: {error}"),
        };
        eprintln!("phase3-real stage=source-activated");
        let source_native = pump_until_native(&db, &chain_id, &mut adapter, &mut source);
        eprintln!("phase3-real stage=source-native");
        adapter
            .bind_native_session(&mut source, &source_native)
            .unwrap();
        let source_actor = actor_from_identity(&source.identity);

        let bundle = serde_json::json!({
            "bundle_id": "bundle-phase3-real",
            "created_by": source_actor.instance_name,
            "title": "opaque handoff",
            "description": bundle_secret,
            "refs": {"events": [], "files": ["README.md"], "transcript": []},
        });
        db.log_event("bundle", &source_actor.instance_name, &bundle)
            .unwrap();
        let bundle_event: i64 = db
            .conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap();
        let prepared_handoff =
            prepare_handoff(&db, &source_actor, bundle_event, &workspace).unwrap();
        eprintln!("phase3-real stage=handoff-prepared");

        let control = HcomChainControl::new(
            HcomDb::open_at(&db_path).unwrap(),
            chain.id.clone(),
            supervisor.clone(),
            outer,
        )
        .unwrap();
        let mut supervisor_loop =
            ForegroundChainSupervisor::new(outer, control, adapter, source, Duration::from_secs(5))
                .unwrap();
        let started = Instant::now();
        let outcome = supervisor_loop.run();
        assert_eq!(
            outcome,
            SupervisorRunOutcome::AwaitingAcceptance {
                generation: 2,
                handoff_id: prepared_handoff.handoff.id.clone(),
            }
        );
        eprintln!("phase3-real stage=target-awaiting-acceptance");
        let (_control, mut adapter, active, prepared, trace) = supervisor_loop.into_parts();
        assert!(prepared.is_none());
        let mut target = active.expect("target remains owned");
        let target_native = target
            .identity
            .native_session_id
            .clone()
            .expect("target native ID");
        assert_ne!(source_native, target_native);
        assert_ne!(
            source_before_native.hcom_session_id,
            target.identity.hcom_session_id
        );
        assert_ne!(source_before_native.process_id, target.identity.process_id);

        let cmdline = fs::read(format!("/proc/{}/cmdline", target.identity.child_pid)).unwrap();
        let environ = fs::read(format!("/proc/{}/environ", target.identity.child_pid)).unwrap();
        for secret in [&bundle_secret, &environment_secret, &stale_native_secret] {
            assert!(
                !cmdline
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes())
            );
            assert!(
                !environ
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes())
            );
        }
        let args: Vec<&[u8]> = cmdline
            .split(|byte| *byte == 0)
            .filter(|value| !value.is_empty())
            .collect();
        for forbidden in [b"resume".as_slice(), b"fork", b"--last"] {
            assert!(!args.contains(&forbidden));
        }
        assert!(
            args.iter()
                .any(|argument| { argument.starts_with(b"Continue hcom handoff ho-") })
        );
        assert_eq!(
            unsafe { libc::tcgetpgrp(0) },
            outer.foreground_pgid,
            "outer foreground PGID changed"
        );

        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            let accepted = handoff::get_handoff(&db, &prepared_handoff.handoff.id)
                .unwrap()
                .unwrap();
            let instance = db
                .get_instance_full(&target.identity.instance_name)
                .unwrap()
                .unwrap();
            if accepted.state == HandoffState::Accepted
                && root.join("accepted-output-observed").is_file()
                && !instance.transcript_path.is_empty()
                && transcript_has_task_complete(&instance.transcript_path)
            {
                break;
            }
            if let GenerationEvent::ChildExited(exit) = adapter
                .wait_event(&mut target, Duration::from_millis(100))
                .unwrap()
            {
                panic!("target exited before explicit acceptance: {exit:?}")
            }
            assert!(Instant::now() < deadline, "target acceptance timed out");
        }
        eprintln!("phase3-real stage=target-accepted");

        let retired = db
            .get_instance_full(&source_before_native.instance_name)
            .unwrap();
        assert!(retired.is_none());
        let stopped_events: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE instance = ?1 AND type = 'life'
                   AND json_extract(data, '$.action') = 'stopped'
                   AND json_extract(data, '$.by') = 'chain-supervisor'
                   AND json_extract(data, '$.reason') = 'handoff'",
                [&source_before_native.instance_name],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stopped_events, 1);
        let audit_text: String = db
            .conn()
            .query_row(
                "SELECT COALESCE(group_concat(
                     chain_id || object_kind || object_id || from_state ||
                     to_state || actor_instance_name || actor_hcom_session_id ||
                     actor_process_id || actor_process_birth_identity ||
                     actor_role || action || request_hash, '|'
                 ), '') FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!audit_text.contains(&bundle_secret));
        assert!(!audit_text.contains(&environment_secret));
        assert!(!audit_text.contains(&stale_native_secret));
        let handoff_row = handoff::get_handoff(&db, &prepared_handoff.handoff.id)
            .unwrap()
            .unwrap();
        let validation_token = handoff_row
            .target_validation_token
            .as_deref()
            .expect("durable target validation token");
        assert!(!format!("{handoff_row:?}").contains(&bundle_secret));
        assert!(!audit_text.contains(validation_token));
        let logs = collect_text_files(&hcom_dir);
        assert!(!logs.contains(&bundle_secret));
        assert!(!logs.contains(&environment_secret));
        assert!(!logs.contains(&stale_native_secret));
        assert!(!logs.contains(validation_token));

        let child_pid = target.identity.child_pid;
        let wrapper_pid = target.identity.wrapper_pid;
        let finish = adapter.shutdown_without_successor(target, ShutdownReason::Explicit);
        assert!(finish.evidence.successful(), "{:?}", finish.evidence);
        assert!(finish.residual.is_none());
        assert_eq!(unsafe { libc::kill(child_pid, 0) }, -1);
        assert_eq!(unsafe { libc::kill(wrapper_pid, 0) }, -1);

        let positions: Vec<TraceKind> = trace.iter().map(|record| record.kind.clone()).collect();
        let reaped = positions
            .iter()
            .position(|kind| *kind == TraceKind::ChildReaped)
            .unwrap();
        let prepared = positions
            .iter()
            .position(|kind| *kind == TraceKind::TargetPrepare)
            .unwrap();
        assert!(reaped < prepared);
        let report = serde_json::json!({
            "source_native": source_native,
            "target_native": target_native,
            "source_child_pid": source_before_native.child_pid,
            "target_child_pid": child_pid,
            "sigterm_to_target_ready_ms": started.elapsed().as_millis(),
            "max_live_codex_children": 1,
            "automatic_sigkill_count": 0,
            "trace": positions.iter().map(|kind| format!("{kind:?}")).collect::<Vec<_>>(),
        });
        fs::write(
            root.join("phase3-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }

    fn collect_text_files(root: &Path) -> String {
        fn visit(path: &Path, output: &mut String) {
            let Ok(metadata) = fs::symlink_metadata(path) else {
                return;
            };
            if metadata.is_dir() {
                if let Ok(entries) = fs::read_dir(path) {
                    for entry in entries.flatten() {
                        visit(&entry.path(), output);
                    }
                }
            } else if metadata.is_file()
                && metadata.len() <= 1024 * 1024
                && let Ok(value) = fs::read_to_string(path)
            {
                output.push_str(&value);
            }
        }
        let mut output = String::new();
        visit(root, &mut output);
        output
    }

    #[test]
    #[ignore = "bounded installed Codex 0.145.0 A-to-B adapter probe"]
    #[serial_test::serial]
    fn bounded_real_fresh_a_to_b_adapter_probe() {
        if std::env::var_os("HCOM_PHASE3_REAL_INNER").is_some() {
            run_real_inner();
        } else {
            run_real_outer();
        }
    }
}
