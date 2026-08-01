//! Linux no-TTY process lifecycle for one foreground-session worker turn.

use super::contract::NativeOutputKind;
use super::{MaterializedWorkerEnvironment, NativeArtifacts, PreparedTurn, SchemaTransport};
use crate::artifact::{
    ActivityLogWriter, ArtifactAttempt, ArtifactKind, ArtifactWriteStatus, BoundedArtifactWriter,
    MAX_ACTIVITY_ARTIFACT_BYTES, MAX_NATIVE_ARTIFACT_BYTES,
};
use crate::control_api::WorkerRole;
use crate::control_api::peer::process_birth_identity;
use anyhow::{Context, Result, anyhow, bail};
use std::fs;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const PIPE_BUFFER_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub process_group: u32,
    pub process_birth: String,
}

pub(crate) struct ProcessGroupBinding {
    pidfd: OwnedFd,
    identity: ProcessIdentity,
}

impl ProcessGroupBinding {
    pub(crate) fn capture(child: &mut Child) -> Result<Self> {
        let spawned_pid = child.id();
        let identity = match capture_process_identity(spawned_pid) {
            Ok(identity) => identity,
            Err(error) => {
                terminate_spawn_failure(child, spawned_pid);
                return Err(error).context("failed to bind spawned process identity");
            }
        };
        let pidfd = match open_pidfd(identity.pid) {
            Ok(pidfd) => pidfd,
            Err(error) => {
                terminate_spawn_failure(child, identity.process_group);
                return Err(error).context("failed to bind spawned process pidfd");
            }
        };
        Ok(Self { pidfd, identity })
    }

    /// Settle the group after the leader has already exited on its own: any
    /// surviving descendant is signalled and reaped. Without this a background
    /// child keeps the leader's pipes open (blocking the drain threads) or
    /// simply outlives the run as an orphan.
    ///
    /// Returns true when descendants had to be killed.
    pub(crate) fn settle_after_exit(&self, grace: Duration) -> Result<bool> {
        settle_reaped_group(self.identity.process_group, grace)
    }

    pub(crate) fn terminate_and_reap(&self, child: &mut Child, grace: Duration) -> Result<()> {
        if !pidfd_is_ready(&self.pidfd)? && process_identity_is_live(&self.identity)? {
            signal_owned_group(&self.identity, libc::SIGTERM)?;
            let deadline = Instant::now() + grace;
            while !pidfd_is_ready(&self.pidfd)? && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            if !pidfd_is_ready(&self.pidfd)? && process_identity_is_live(&self.identity)? {
                signal_owned_group(&self.identity, libc::SIGKILL)?;
            }
        }
        let kill_deadline = Instant::now() + grace;
        while !pidfd_is_ready(&self.pidfd)? && Instant::now() < kill_deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if !pidfd_is_ready(&self.pidfd)? {
            bail!("owned process group did not exit after SIGKILL");
        }
        let _ = settle_descendants_after_leader_exit(&self.identity, grace)?;
        child.wait()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatControl {
    Continue,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerTermination {
    Exited,
    Canceled,
    OutputLimit,
    TransportFailure,
    OrphanedDescendants,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerExit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub termination: WorkerTermination,
    pub heartbeat_count: u64,
}

pub struct ProcessCompletion {
    pub exit: WorkerExit,
    pub artifacts: NativeArtifacts,
    pub artifact_attempt: ArtifactAttempt,
}

pub struct ProcessRunner {
    heartbeat_interval: Duration,
    terminate_grace: Duration,
}

impl Default for ProcessRunner {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            terminate_grace: Duration::from_secs(2),
        }
    }
}

impl ProcessRunner {
    pub fn new(heartbeat_interval: Duration, terminate_grace: Duration) -> Result<Self> {
        if heartbeat_interval.is_zero()
            || heartbeat_interval > Duration::from_secs(60)
            || terminate_grace.is_zero()
            || terminate_grace > Duration::from_secs(30)
        {
            bail!("worker process timing contract is outside its bounds");
        }
        Ok(Self {
            heartbeat_interval,
            terminate_grace,
        })
    }

    pub fn spawn(
        &self,
        role: WorkerRole,
        prepared: PreparedTurn,
        environment: &MaterializedWorkerEnvironment,
        attempt: ArtifactAttempt,
    ) -> Result<RunningWorker> {
        if attempt.scope().role != role {
            bail!("worker role does not match its artifact attempt");
        }
        let (command_spec, prompt) = prepared.into_parts();
        command_spec.validate()?;
        environment.require_exact(&command_spec.exact_environment)?;
        let inside_artifact_dir = command_spec
            .outer_launch
            .as_ref()
            .map(|outer| outer.inside_artifact_dir.as_path());
        let mut native_argv = command_spec.fixed_argv.clone();
        match &command_spec.schema_transport {
            SchemaTransport::None => {}
            SchemaTransport::InlineArgument { flag, json } => {
                native_argv.push(flag.clone());
                native_argv.push(json.clone());
            }
            SchemaTransport::File {
                argument,
                relative_path,
                contents,
            } => {
                let host_path = attempt.write_control_file(relative_path, contents)?;
                let path = inside_artifact_dir
                    .map(|root| root.join(relative_path))
                    .unwrap_or(host_path);
                let path = path
                    .to_str()
                    .ok_or_else(|| anyhow!("adapter control path is not valid UTF-8"))?;
                native_argv.push(argument.clone());
                native_argv.push(path.to_owned());
            }
        }
        let stdout_cap = command_spec
            .expected_outputs
            .iter()
            .find(|output| output.kind == NativeOutputKind::StdoutEnvelope)
            .map(|output| output.max_bytes as u64)
            .unwrap_or(MAX_NATIVE_ARTIFACT_BYTES);
        let stdout_cap = usize::try_from(stdout_cap).context("stdout cap does not fit usize")?;
        let stderr_cap =
            usize::try_from(MAX_NATIVE_ARTIFACT_BYTES).context("stderr cap does not fit usize")?;
        let final_output = command_spec
            .expected_outputs
            .iter()
            .find(|output| output.kind == NativeOutputKind::FinalFile)
            .map(|output| {
                (
                    output.relative_path.clone(),
                    output
                        .output_argument
                        .clone()
                        .expect("validated final output has a path argument"),
                    output.max_bytes,
                )
            });
        if let Some((relative_path, argument, _)) = &final_output {
            let output_path = inside_artifact_dir
                .map(|root| root.join(relative_path))
                .unwrap_or_else(|| attempt.directory_path().join(relative_path));
            let output_path = output_path
                .to_str()
                .ok_or_else(|| anyhow!("native final output path is not valid UTF-8"))?;
            native_argv.push(argument.clone());
            native_argv.push(output_path.to_owned());
        }
        if let Some(argument) = &command_spec.stdin_prompt_argument {
            native_argv.push(argument.clone());
        }
        let (launch_executable, argv) = match &command_spec.outer_launch {
            Some(outer) => {
                if outer.expected_artifact_dir != attempt.directory_path()
                    || fs::canonicalize(&outer.expected_artifact_dir)?
                        != outer.expected_artifact_dir
                {
                    bail!("outer launch artifact mount does not match the exact attempt");
                }
                let mut argv = outer.fixed_argv.clone();
                argv.push("--".into());
                argv.push(
                    outer
                        .inside_executable
                        .to_str()
                        .ok_or_else(|| anyhow!("native executable path is not valid UTF-8"))?
                        .to_owned(),
                );
                argv.extend(native_argv);
                (&outer.executable, argv)
            }
            None => (&command_spec.executable, native_argv),
        };

        let stdout_writer =
            attempt.start_native_stream(ArtifactKind::NativeStdout, stdout_cap as u64)?;
        let stderr_writer =
            attempt.start_native_stream(ArtifactKind::NativeStderr, MAX_NATIVE_ARTIFACT_BYTES)?;
        let mut activity = attempt.start_activity_log(MAX_ACTIVITY_ARTIFACT_BYTES)?;
        activity.record("lifecycle", b"worker spawn starting after in-memory claim")?;

        let expected_parent = std::process::id();
        let mut command = Command::new(&launch_executable.canonical_path);
        command
            .args(&argv)
            .current_dir(&command_spec.workspace_cwd)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in environment.iter() {
            command.env(name, value);
        }
        // SAFETY: the closure only invokes async-signal-safe Linux syscalls and
        // reads the captured parent PID. It performs no allocation after fork.
        unsafe {
            command.pre_exec(move || configure_worker_child(expected_parent));
        }
        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to spawn session worker {}",
                launch_executable.canonical_path.display()
            )
        })?;
        let spawned_pid = child.id();
        let identity = match capture_process_identity(spawned_pid) {
            Ok(identity) => identity,
            Err(error) => {
                terminate_spawn_failure(&mut child, spawned_pid);
                return Err(error).context("failed to bind spawned worker identity");
            }
        };
        let pidfd = match open_pidfd(identity.pid) {
            Ok(pidfd) => pidfd,
            Err(error) => {
                terminate_spawn_failure(&mut child, identity.process_group);
                return Err(error).context("failed to bind spawned worker pidfd");
            }
        };
        let Some(stdin) = child.stdin.take() else {
            terminate_spawn_failure(&mut child, identity.process_group);
            bail!("worker stdin pipe is unavailable");
        };
        let Some(stdout) = child.stdout.take() else {
            terminate_spawn_failure(&mut child, identity.process_group);
            bail!("worker stdout pipe is unavailable");
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_spawn_failure(&mut child, identity.process_group);
            bail!("worker stderr pipe is unavailable");
        };
        if let Err(error) = activity.record("lifecycle", b"worker process identity bound") {
            terminate_spawn_failure(&mut child, identity.process_group);
            return Err(error).context("failed to record spawned worker identity");
        }
        Ok(RunningWorker {
            child,
            pidfd,
            identity,
            stdin: Some(stdin),
            stdout: Some(stdout),
            stderr: Some(stderr),
            prompt,
            stdout_writer: Some(stdout_writer),
            stderr_writer: Some(stderr_writer),
            stdout_cap,
            stderr_cap,
            final_output,
            attempt: Some(attempt),
            activity: Some(activity),
            heartbeat_interval: self.heartbeat_interval,
            terminate_grace: self.terminate_grace,
        })
    }
}

pub struct RunningWorker {
    child: Child,
    pidfd: OwnedFd,
    identity: ProcessIdentity,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    prompt: Vec<u8>,
    stdout_writer: Option<BoundedArtifactWriter>,
    stderr_writer: Option<BoundedArtifactWriter>,
    stdout_cap: usize,
    stderr_cap: usize,
    final_output: Option<(String, String, usize)>,
    attempt: Option<ArtifactAttempt>,
    activity: Option<ActivityLogWriter>,
    heartbeat_interval: Duration,
    terminate_grace: Duration,
}

impl RunningWorker {
    pub fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    pub fn wait<F>(self, mut heartbeat: F) -> Result<ProcessCompletion>
    where
        F: FnMut(&ProcessIdentity) -> Result<HeartbeatControl>,
    {
        self.wait_with_cancel(Arc::new(AtomicBool::new(false)), &mut heartbeat)
    }

    pub fn wait_with_cancel<F>(
        mut self,
        cancel: Arc<AtomicBool>,
        mut heartbeat: F,
    ) -> Result<ProcessCompletion>
    where
        F: FnMut(&ProcessIdentity) -> Result<HeartbeatControl>,
    {
        let overflow = Arc::new(AtomicBool::new(false));
        let transport_failure = Arc::new(AtomicBool::new(false));
        let stdin = self.stdin.take().expect("spawned worker owns stdin");
        let prompt = std::mem::take(&mut self.prompt);
        let stdin_failure = transport_failure.clone();
        let stdin_thread = thread::spawn(move || {
            let result = write_prompt(stdin, prompt);
            if result.is_err() {
                stdin_failure.store(true, Ordering::Release);
            }
            result
        });

        let stdout = self.stdout.take().expect("spawned worker owns stdout");
        let stdout_writer = self
            .stdout_writer
            .take()
            .expect("spawned worker owns stdout artifact");
        let stdout_overflow = overflow.clone();
        let stdout_failure = transport_failure.clone();
        let stdout_cap = self.stdout_cap;
        let stdout_thread = thread::spawn(move || {
            let result = drain_pipe(stdout, stdout_writer, stdout_cap, stdout_overflow);
            if result.is_err() {
                stdout_failure.store(true, Ordering::Release);
            }
            result
        });

        let stderr = self.stderr.take().expect("spawned worker owns stderr");
        let stderr_writer = self
            .stderr_writer
            .take()
            .expect("spawned worker owns stderr artifact");
        let stderr_overflow = overflow.clone();
        let stderr_failure = transport_failure.clone();
        let stderr_cap = self.stderr_cap;
        let stderr_thread = thread::spawn(move || {
            let result = drain_pipe(stderr, stderr_writer, stderr_cap, stderr_overflow);
            if result.is_err() {
                stderr_failure.store(true, Ordering::Release);
            }
            result
        });

        let mut heartbeat_count = 0u64;
        let mut next_heartbeat = Instant::now();
        let mut termination = WorkerTermination::Exited;
        let mut termination_started = None;
        let status = loop {
            if pidfd_is_ready(&self.pidfd)? {
                if settle_descendants_after_leader_exit(&self.identity, self.terminate_grace)? {
                    termination = WorkerTermination::OrphanedDescendants;
                }
                let status = self.child.wait()?;
                break status;
            }
            if cancel.load(Ordering::Acquire) && termination == WorkerTermination::Exited {
                signal_owned_group(&self.identity, libc::SIGTERM)?;
                termination = WorkerTermination::Canceled;
                termination_started = Some(Instant::now());
            }
            if transport_failure.load(Ordering::Acquire) && termination == WorkerTermination::Exited
            {
                signal_owned_group(&self.identity, libc::SIGTERM)?;
                termination = WorkerTermination::TransportFailure;
                termination_started = Some(Instant::now());
            }
            if overflow.load(Ordering::Acquire) && termination == WorkerTermination::Exited {
                signal_owned_group(&self.identity, libc::SIGTERM)?;
                termination = WorkerTermination::OutputLimit;
                termination_started = Some(Instant::now());
            }
            if Instant::now() >= next_heartbeat {
                heartbeat_count = heartbeat_count.saturating_add(1);
                if heartbeat(&self.identity)? == HeartbeatControl::Cancel
                    && termination == WorkerTermination::Exited
                {
                    signal_owned_group(&self.identity, libc::SIGTERM)?;
                    termination = WorkerTermination::Canceled;
                    termination_started = Some(Instant::now());
                }
                next_heartbeat = Instant::now() + self.heartbeat_interval;
            }
            if termination != WorkerTermination::Exited
                && termination_started
                    .is_some_and(|started| started.elapsed() >= self.terminate_grace)
            {
                if process_identity_is_live(&self.identity)? {
                    signal_owned_group(&self.identity, libc::SIGKILL)?;
                }
                termination_started = None;
            }
            thread::sleep(self.heartbeat_interval.min(Duration::from_millis(20)));
        };

        let stdin = join_worker_thread(stdin_thread, "stdin")?;
        let stdout = join_worker_thread(stdout_thread, "stdout")?;
        let stderr = join_worker_thread(stderr_thread, "stderr")?;
        if overflow.load(Ordering::Acquire) && termination == WorkerTermination::Exited {
            termination = WorkerTermination::OutputLimit;
        }
        let (stdout, stderr) = if matches!(
            termination,
            WorkerTermination::Canceled | WorkerTermination::OutputLimit
        ) {
            (
                stdout.unwrap_or_else(|_| DrainedPipe { bytes: Vec::new() }),
                stderr.unwrap_or_else(|_| DrainedPipe { bytes: Vec::new() }),
            )
        } else {
            stdin?;
            (stdout?, stderr?)
        };
        let mut activity = self
            .activity
            .take()
            .expect("spawned worker owns activity artifact");
        activity.record("lifecycle", b"worker process exited and pipes drained")?;
        activity.finish()?;

        let final_output = match self
            .final_output
            .take()
            .filter(|_| termination == WorkerTermination::Exited)
        {
            Some((relative_path, _, max_bytes)) => {
                let attempt = self
                    .attempt
                    .as_ref()
                    .expect("spawned worker owns artifact attempt");
                match fs::symlink_metadata(attempt.directory_path().join(&relative_path)) {
                    Ok(_) => Some(attempt.ingest_native_final(&relative_path, max_bytes as u64)?),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => {
                        return Err(error).context("failed to inspect native final output");
                    }
                }
            }
            None => None,
        };
        let attempt = self
            .attempt
            .take()
            .expect("spawned worker owns artifact attempt");
        let exit = WorkerExit {
            code: status.code(),
            signal: status.signal(),
            termination,
            heartbeat_count,
        };
        Ok(ProcessCompletion {
            exit,
            artifacts: NativeArtifacts::new(
                attempt.scope().role,
                stdout.bytes,
                stderr.bytes,
                final_output,
            )?,
            artifact_attempt: attempt,
        })
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        if pidfd_is_ready(&self.pidfd).unwrap_or(false) {
            let _ = kill_descendants_after_leader_exit(&self.identity);
        } else if process_identity_is_live(&self.identity).unwrap_or(false) {
            let _ = signal_owned_group(&self.identity, libc::SIGKILL);
        } else {
            // The unreaped Child handle still names the exact child; its
            // PID cannot be reused before wait. This fallback never scans
            // or targets an unrelated process group.
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

struct DrainedPipe {
    bytes: Vec<u8>,
}

fn write_prompt(mut stdin: ChildStdin, prompt: Vec<u8>) -> Result<()> {
    stdin
        .write_all(&prompt)
        .context("failed to write bounded worker prompt")?;
    stdin.flush()?;
    drop(stdin);
    Ok(())
}

fn drain_pipe<R: Read>(
    mut pipe: R,
    mut writer: BoundedArtifactWriter,
    memory_cap: usize,
    overflow: Arc<AtomicBool>,
) -> Result<DrainedPipe> {
    let mut bytes = Vec::with_capacity(memory_cap.min(PIPE_BUFFER_BYTES));
    let mut buffer = [0u8; PIPE_BUFFER_BYTES];
    loop {
        let count = pipe.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if writer.write_chunk(&buffer[..count])? == ArtifactWriteStatus::Truncated {
            overflow.store(true, Ordering::Release);
        }
        let remaining = memory_cap.saturating_sub(bytes.len());
        let accepted = count.min(remaining);
        bytes.extend_from_slice(&buffer[..accepted]);
        if accepted < count {
            overflow.store(true, Ordering::Release);
        }
    }
    writer.finish()?;
    Ok(DrainedPipe { bytes })
}

fn join_worker_thread<T>(handle: thread::JoinHandle<Result<T>>, label: &str) -> Result<Result<T>> {
    handle
        .join()
        .map_err(|_| anyhow!("worker {label} pipe thread panicked"))
}

fn capture_process_identity(pid: u32) -> Result<ProcessIdentity> {
    let process_group = process_group_id(pid)?;
    if process_group != pid {
        bail!("worker did not enter its isolated process group");
    }
    Ok(ProcessIdentity {
        pid,
        process_group,
        process_birth: process_birth_identity(pid)?,
    })
}

fn terminate_spawn_failure(child: &mut Child, process_group: u32) {
    if let Ok(process_group) = i32::try_from(process_group) {
        // The exact Child handle is still unreaped, so its PID cannot be
        // reused. configure_worker_child established this PID as the process
        // group before exec; signaling that group cannot target another job.
        // SAFETY: the negative PID targets that exact unreaped child group.
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn open_pidfd(pid: u32) -> Result<OwnedFd> {
    // SAFETY: pidfd_open takes a numeric PID and zero flags and returns a new
    // descriptor on success.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd_open failed");
    }
    let fd = i32::try_from(fd).context("pidfd exceeds a file descriptor")?;
    // SAFETY: pidfd_open returned a newly owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn pidfd_is_ready(pidfd: &OwnedFd) -> Result<bool> {
    let mut pollfd = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pollfd points to one initialized entry for the duration of the call.
    let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if ready < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to poll worker pidfd");
    }
    Ok(ready == 1 && pollfd.revents & (libc::POLLIN | libc::POLLHUP) != 0)
}

fn process_group_id(pid: u32) -> Result<u32> {
    // SAFETY: getpgid has no pointer arguments and pid was range checked by the caller.
    let process_group = unsafe { libc::getpgid(pid as libc::pid_t) };
    if process_group < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to read worker process group");
    }
    u32::try_from(process_group).context("worker process group is out of range")
}

fn settle_descendants_after_leader_exit(
    identity: &ProcessIdentity,
    grace: Duration,
) -> Result<bool> {
    if wait_for_owned_descendants(identity, grace)? {
        return Ok(false);
    }
    signal_owned_group(identity, libc::SIGTERM)?;
    if !wait_for_owned_descendants(identity, grace)? {
        signal_owned_group(identity, libc::SIGKILL)?;
        if !wait_for_owned_descendants(identity, grace)? {
            bail!("owned worker descendants did not exit after SIGKILL");
        }
    }
    Ok(true)
}

fn kill_descendants_after_leader_exit(identity: &ProcessIdentity) -> Result<()> {
    if owned_group_has_live_descendants(identity)? {
        signal_owned_group(identity, libc::SIGKILL)?;
    }
    Ok(())
}

fn wait_for_owned_descendants(identity: &ProcessIdentity, grace: Duration) -> Result<bool> {
    let deadline = Instant::now() + grace;
    loop {
        if !owned_group_has_live_descendants(identity)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn owned_group_has_live_descendants(identity: &ProcessIdentity) -> Result<bool> {
    if !process_identity_is_live(identity)? {
        bail!("worker leader identity changed before descendant inspection");
    }
    // SAFETY: geteuid has no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == identity.pid {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error).context("failed to inspect /proc process owner"),
        };
        if metadata.uid() != expected_uid {
            continue;
        }
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error).context("failed to inspect owned /proc process"),
        };
        let (_, fields) = stat
            .rsplit_once(") ")
            .ok_or_else(|| anyhow!("owned /proc stat has an invalid shape"))?;
        let mut fields = fields.split_ascii_whitespace();
        let state = fields
            .next()
            .ok_or_else(|| anyhow!("owned /proc stat omitted process state"))?;
        let _parent = fields
            .next()
            .ok_or_else(|| anyhow!("owned /proc stat omitted parent PID"))?;
        let process_group = fields
            .next()
            .ok_or_else(|| anyhow!("owned /proc stat omitted process group"))?
            .parse::<u32>()
            .context("owned /proc process group is invalid")?;
        let session = fields
            .next()
            .ok_or_else(|| anyhow!("owned /proc stat omitted session"))?
            .parse::<u32>()
            .context("owned /proc session is invalid")?;
        if state != "Z"
            && process_group == identity.process_group
            && session == identity.process_group
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn proc_entry_vanished(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        || matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH))
}

fn process_identity_is_live(identity: &ProcessIdentity) -> Result<bool> {
    if !std::path::Path::new(&format!("/proc/{}", identity.pid)).exists() {
        return Ok(false);
    }
    let birth = match process_birth_identity(identity.pid) {
        Ok(birth) => birth,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    Ok(birth == identity.process_birth
        && process_group_id(identity.pid)? == identity.process_group
        && identity.process_group == identity.pid)
}

/// Descendants of an already-reaped leader, matched by the group id the
/// leader owned. The leader itself is gone, so its identity cannot be
/// re-verified; the group id is still exact because `configure_worker_child`
/// made the leader its own session and group leader, and only its children
/// can carry that id.
fn reaped_group_has_live_descendants(process_group: u32) -> Result<bool> {
    // SAFETY: geteuid has no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error).context("failed to inspect /proc process owner"),
        };
        if metadata.uid() != expected_uid {
            continue;
        }
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error).context("failed to inspect owned /proc process"),
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            continue;
        };
        let mut fields = fields.split_ascii_whitespace();
        let Some(state) = fields.next() else { continue };
        let _parent = fields.next();
        let Some(group) = fields.next().and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };
        // Zombies hold no resources and are reaped by init.
        if group == process_group && pid != process_group && state != "Z" {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Kill and reap descendants left behind by a leader that already exited.
/// Returns true when anything had to be killed.
pub(crate) fn settle_reaped_group(process_group: u32, grace: Duration) -> Result<bool> {
    if !reaped_group_has_live_descendants(process_group)? {
        return Ok(false);
    }
    let group = i32::try_from(process_group).context("worker process group exceeds pid_t")?;
    for signal in [libc::SIGTERM, libc::SIGKILL] {
        // SAFETY: the negative PID targets the exact worker process group.
        unsafe { libc::kill(-group, signal) };
        let deadline = Instant::now() + grace;
        loop {
            if !reaped_group_has_live_descendants(process_group)? {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    bail!("worker descendants survived SIGKILL")
}

fn signal_owned_group(identity: &ProcessIdentity, signal: i32) -> Result<()> {
    if !process_identity_is_live(identity)? {
        bail!("worker process identity changed before signal");
    }
    let process_group =
        i32::try_from(identity.process_group).context("worker process group exceeds pid_t")?;
    // SAFETY: the negative PID targets the exact validated worker process group.
    if unsafe { libc::kill(-process_group, signal) } != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to signal worker group");
    }
    Ok(())
}

pub(crate) fn configure_worker_child(expected_parent: u32) -> std::io::Result<()> {
    // SAFETY: prctl, getppid, getpid, and setsid have no pointer arguments here.
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::getppid() != expected_parent as libc::pid_t {
            return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
        }
        libc::umask(0o077);
        if libc::setsid() < 0 || libc::getpgrp() != libc::getpid() {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{ArtifactRoot, ArtifactScope};
    use crate::worker::environment::{
        EnvironmentPolicy, ExecutionEnvironmentLease, ParentEnvironment, WorkerEnvironmentIdentity,
    };
    use crate::worker::fake::FakeWorkerAdapter;
    use crate::worker::{ExecutableIdentity, TurnControl, WorkerAdapter, prepare_create_turn};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn fake_script(temp: &tempfile::TempDir, body: &str) -> ExecutableIdentity {
        let path = temp.path().join("fake-worker");
        fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableIdentity::capture(fs::canonicalize(path).unwrap()).unwrap()
    }

    fn lease(epoch: &str) -> ExecutionEnvironmentLease {
        ExecutionEnvironmentLease::capture(
            "lease-1",
            epoch,
            &EnvironmentPolicy::baseline(),
            vec![("PATH".into(), "/usr/bin:/bin".into())],
        )
        .unwrap()
    }

    fn spawn_fake(body: &str) -> (tempfile::TempDir, RunningWorker, FakeWorkerAdapter, Vec<u8>) {
        spawn_fake_mode(body, false)
    }

    fn spawn_discovered_fake(
        body: &str,
    ) -> (tempfile::TempDir, RunningWorker, FakeWorkerAdapter, Vec<u8>) {
        spawn_fake_mode(body, true)
    }

    fn spawn_fake_mode(
        body: &str,
        discovered: bool,
    ) -> (tempfile::TempDir, RunningWorker, FakeWorkerAdapter, Vec<u8>) {
        spawn_fake_mode_with_prompt(body, discovered, b"bounded private prompt".to_vec())
    }

    fn spawn_fake_mode_with_prompt(
        body: &str,
        discovered: bool,
        prompt: Vec<u8>,
    ) -> (tempfile::TempDir, RunningWorker, FakeWorkerAdapter, Vec<u8>) {
        spawn_fake_mode_with_prompt_and_environment(body, discovered, prompt, lease("epoch-1"))
    }

    fn spawn_fake_mode_with_prompt_and_environment(
        body: &str,
        discovered: bool,
        prompt: Vec<u8>,
        environment: ExecutionEnvironmentLease,
    ) -> (tempfile::TempDir, RunningWorker, FakeWorkerAdapter, Vec<u8>) {
        let temp = tempfile::tempdir().unwrap();
        let workspace = fs::canonicalize(temp.path()).unwrap();
        let executable = fake_script(&temp, body);
        let adapter = if discovered {
            FakeWorkerAdapter::discovered(executable, workspace.clone()).unwrap()
        } else {
            FakeWorkerAdapter::preassigned(executable, workspace.clone()).unwrap()
        };
        let profile = adapter.profile(WorkerRole::Developer);
        let control = TurnControl {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            role: WorkerRole::Developer,
            logical_session_id: "session-1".into(),
            native_session_id: (!discovered).then(|| "native-1".into()),
            turn_sequence: 1,
            attempt: 1,
            task_version: 1,
            review_round: 0,
            base_revision: "a".repeat(40),
            head_revision: None,
            artifact_dir: "run-1/task-1/developer/session-1/turn-1".into(),
        };
        let prepared = prepare_create_turn(&adapter, &profile, &control, prompt.clone()).unwrap();
        let artifact_root_path = temp.path().join("artifacts");
        fs::create_dir(&artifact_root_path).unwrap();
        fs::set_permissions(&artifact_root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = ArtifactRoot::open(fs::canonicalize(artifact_root_path).unwrap()).unwrap();
        let attempt = ArtifactAttempt::create(
            &root,
            ArtifactScope {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                role: WorkerRole::Developer,
                logical_session_id: "session-1".into(),
                turn_sequence: 1,
                attempt: 1,
            },
            &environment,
            &prompt,
        )
        .unwrap();
        let materialized = environment
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        let runner =
            ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(50)).unwrap();
        let worker = runner
            .spawn(WorkerRole::Developer, prepared, &materialized, attempt)
            .unwrap();
        (temp, worker, adapter, prompt)
    }

    #[test]
    fn spawned_worker_receives_complete_parent_environment_and_identity_overlays() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let raw_name = OsString::from("RAW_PARENT_VALUE");
        let raw_value = OsString::from_vec(b"value-\xfe".to_vec());
        let parent = ParentEnvironment::from_os(vec![
            (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
            (
                OsString::from("ARBITRARY_PARENT_VALUE"),
                OsString::from("arbitrary-value"),
            ),
            (
                OsString::from("SERVICE_ACCESS_TOKEN"),
                OsString::from("secret-shaped-value"),
            ),
            (OsString::from("EMPTY_PARENT_VALUE"), OsString::new()),
            (
                OsString::from("HCOM_WORKER_ROLE"),
                OsString::from("wrong-parent-role"),
            ),
            (raw_name, raw_value),
        ]);
        let environment = ExecutionEnvironmentLease::capture_complete(
            "lease-complete-parent",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            &parent,
            Vec::new(),
        )
        .unwrap();
        let (_temp, worker, adapter, _prompt) = spawn_fake_mode_with_prompt_and_environment(
            r#"
python3 -c 'import os; assert os.environ["ARBITRARY_PARENT_VALUE"] == "arbitrary-value"; assert os.environ["SERVICE_ACCESS_TOKEN"] == "secret-shaped-value"; assert os.environ["EMPTY_PARENT_VALUE"] == ""; assert os.environ["HCOM_WORKER_ROLE"] == "developer"; assert os.environb[b"RAW_PARENT_VALUE"] == b"value-\xfe"'
sed -n '1,$p' >/dev/null
printf '%s' '{"session_id":"native-1","role":"developer","result":{"decision":"blocked","summary":"complete environment","head_revision":null,"commits":[],"checks":[],"questions":[],"risks":[],"changed_paths":[]}}'
"#,
            false,
            b"bounded private prompt".to_vec(),
            environment,
        );
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        assert_eq!(completion.exit.termination, WorkerTermination::Exited);
        assert_eq!(
            completion.exit.code,
            Some(0),
            "worker stderr: {}",
            String::from_utf8_lossy(completion.artifacts.stderr())
        );
        adapter
            .extract_result(&developer_control(false), &completion.artifacts)
            .unwrap();
    }

    #[test]
    fn no_tty_prompt_eof_concurrent_drain_and_silent_heartbeat() {
        let (_temp, worker, adapter, prompt) = spawn_fake(
            r#"
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
prompt=$(sed -n '1,$p')
sleep 0.08
printf '%s' "$prompt" >&2
printf '%s' '{"session_id":"native-1","role":"developer","result":{"decision":"blocked","summary":"bounded","head_revision":null,"commits":[],"checks":[],"questions":[],"risks":[],"changed_paths":[]}}'
"#,
        );
        assert_eq!(worker.identity().pid, worker.identity().process_group);
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        assert!(completion.exit.heartbeat_count >= 2);
        assert_eq!(completion.exit.termination, WorkerTermination::Exited);
        assert!(completion.exit.code == Some(0));
        assert_eq!(completion.artifacts.stderr(), prompt);
        adapter
            .extract_result(&developer_control(false), &completion.artifacts)
            .unwrap();
    }

    #[test]
    fn cancel_signals_only_the_exact_owned_process_group() {
        let (_temp, worker, _adapter, _prompt) = spawn_fake(
            r#"
sed -n '1,$p' >/dev/null
sleep 5
"#,
        );
        let identity = worker.identity().clone();
        let mut wrong = identity.clone();
        wrong.process_birth.push_str("-wrong");
        let completion = worker.wait(|live| {
            assert_eq!(live, &identity);
            assert!(signal_owned_group(&wrong, libc::SIGTERM).is_err());
            Ok(HeartbeatControl::Cancel)
        });
        let completion = completion.unwrap();
        assert_eq!(completion.exit.termination, WorkerTermination::Canceled);
        assert!(completion.exit.signal.is_some() || completion.exit.code.is_some());
        assert!(
            process_birth_identity(identity.pid)
                .map(|birth| birth != identity.process_birth)
                .unwrap_or(true),
            "the exact owned worker must be gone after cancellation"
        );
    }

    #[test]
    fn prompt_transport_failure_terminates_a_silent_worker() {
        let (_temp, worker, _adapter, _prompt) =
            spawn_fake_mode_with_prompt("exec 0<&-; sleep 30", false, vec![b'x'; 128 * 1024]);
        let identity = worker.identity().clone();
        let started = Instant::now();
        assert!(worker.wait(|_| Ok(HeartbeatControl::Continue)).is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(
            process_birth_identity(identity.pid).is_err(),
            "transport failure must reap the exact worker"
        );
    }

    #[test]
    fn dropping_unbound_worker_reaps_the_exact_spawned_process() {
        let (_temp, worker, _adapter, _prompt) = spawn_fake(
            r#"
sed -n '1,$p' >/dev/null
sleep 5
"#,
        );
        let identity = worker.identity().clone();
        drop(worker);
        assert!(
            process_birth_identity(identity.pid)
                .map(|birth| birth != identity.process_birth)
                .unwrap_or(true),
            "dropping a spawned-but-unbound worker must reap the exact child"
        );
    }

    #[test]
    fn heartbeat_failure_cleans_up_worker_before_returning_error() {
        let (_temp, worker, _adapter, _prompt) = spawn_fake(
            r#"
sed -n '1,$p' >/dev/null
sleep 5
"#,
        );
        let identity = worker.identity().clone();
        let result = worker.wait(|_| Err(anyhow!("injected session heartbeat failure")));
        assert!(result.is_err());
        assert!(
            process_birth_identity(identity.pid)
                .map(|birth| birth != identity.process_birth)
                .unwrap_or(true),
            "heartbeat failure must not leave the worker process alive"
        );
    }

    #[test]
    fn final_file_path_is_parent_materialized_and_securely_ingested() {
        let (_temp, worker, adapter, _prompt) = spawn_discovered_fake(
            r#"
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--output-file" ]; then
        shift
        output="$1"
    fi
    shift
done
[ -n "$output" ]
sed -n '1,$p' >/dev/null
printf '%s' '{"session_id":"native-discovered-1","role":"developer","result":{"decision":"blocked","summary":"bounded","head_revision":null,"commits":[],"checks":[],"questions":[],"risks":[],"changed_paths":[]}}' >"$output"
"#,
        );
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        let native = adapter
            .extract_result(&developer_control(true), &completion.artifacts)
            .unwrap();
        assert_eq!(native.native_session_id(), "native-discovered-1");
        let final_path = completion
            .artifact_attempt
            .artifact_path(ArtifactKind::NativeFinal);
        let metadata = fs::metadata(&final_path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            fs::read(final_path).unwrap(),
            completion.artifacts.final_output().unwrap()
        );
    }

    #[test]
    fn nonzero_final_file_worker_preserves_streams_without_requiring_a_result_file() {
        let (_temp, worker, _adapter, _prompt) = spawn_discovered_fake(
            r#"
sed -n '1,$p' >/dev/null
printf '%s' '{"type":"session_started","session_id":"native-discovered-1"}'
exit 42
"#,
        );
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        assert_eq!(completion.exit.termination, WorkerTermination::Exited);
        assert_eq!(completion.exit.code, Some(42));
        assert_eq!(completion.exit.signal, None);
        assert_eq!(
            completion.artifacts.stdout(),
            br#"{"type":"session_started","session_id":"native-discovered-1"}"#
        );
        assert_eq!(completion.artifacts.final_output(), None);
    }

    #[test]
    fn final_file_symlink_is_rejected_without_following_it() {
        let (temp, worker, _adapter, _prompt) = spawn_discovered_fake(
            r#"
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--output-file" ]; then
        shift
        output="$1"
    fi
    shift
done
sed -n '1,$p' >/dev/null
printf 'do-not-ingest' > victim
ln -s "$PWD/victim" "$output"
"#,
        );
        assert!(worker.wait(|_| Ok(HeartbeatControl::Continue)).is_err());
        assert_eq!(
            fs::read(temp.path().join("victim")).unwrap(),
            b"do-not-ingest"
        );
    }

    fn developer_control(discovered: bool) -> TurnControl {
        TurnControl {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            role: WorkerRole::Developer,
            logical_session_id: "session-1".into(),
            native_session_id: (!discovered).then(|| "native-1".into()),
            turn_sequence: 1,
            attempt: 1,
            task_version: 1,
            review_round: 0,
            base_revision: "a".repeat(40),
            head_revision: None,
            artifact_dir: "run-1/task-1/developer/session-1/turn-1/attempt-1".into(),
        }
    }

    #[test]
    fn native_output_cap_terminates_group_and_still_drains_pipes() {
        let (_temp, worker, _adapter, _prompt) = spawn_fake(
            r#"
sed -n '1,$p' >/dev/null
while :; do
    printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
done
"#,
        );
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        assert_eq!(completion.exit.termination, WorkerTermination::OutputLimit);
        assert!(completion.artifacts.stdout().len() <= 256 * 1024);
    }

    #[test]
    fn exited_leader_cannot_leave_a_live_process_group_behind() {
        let (_temp, worker, _adapter, _prompt) = spawn_fake(
            "sed -n '1,$p' >/dev/null; sleep 30 & descendant=$!; printf '%s\\n' \"$descendant\"",
        );
        let completion = worker.wait(|_| Ok(HeartbeatControl::Continue)).unwrap();
        assert_eq!(
            completion.exit.termination,
            WorkerTermination::OrphanedDescendants
        );
        let descendant = std::str::from_utf8(completion.artifacts.stdout())
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while std::path::Path::new(&format!("/proc/{descendant}")).exists()
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!std::path::Path::new(&format!("/proc/{descendant}")).exists());
    }

    #[test]
    fn child_setup_binds_pdeathsig_session_and_no_tty_contract() {
        const FLAG: &str = "HCOM_PROCESS_SETUP_CHILD_TEST";
        if std::env::var_os(FLAG).is_some() {
            configure_worker_child(unsafe { libc::getppid() as u32 }).unwrap();
            let mut signal = 0;
            // SAFETY: PR_GET_PDEATHSIG writes one integer to the supplied live pointer.
            assert_eq!(
                unsafe { libc::prctl(libc::PR_GET_PDEATHSIG, &mut signal) },
                0
            );
            assert_eq!(signal, libc::SIGKILL);
            // SAFETY: getpid/getpgrp have no preconditions.
            assert_eq!(unsafe { libc::getpid() }, unsafe { libc::getpgrp() });
            assert!(!fd_is_tty(libc::STDIN_FILENO));
            assert!(!fd_is_tty(libc::STDOUT_FILENO));
            assert!(!fd_is_tty(libc::STDERR_FILENO));
            return;
        }
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("worker::process::tests::child_setup_binds_pdeathsig_session_and_no_tty_contract")
            .env(FLAG, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn worker_dies_when_its_session_parent_exits() {
        const FLAG: &str = "HCOM_PDEATHSIG_PARENT_TEST";
        if std::env::var_os(FLAG).is_some() {
            let (_temp, worker, _adapter, _prompt) = spawn_fake(
                r#"
sed -n '1,$p' >/dev/null
sleep 30
"#,
            );
            let report = std::env::var_os("HCOM_PDEATHSIG_REPORT").unwrap();
            fs::write(
                report,
                format!(
                    "{} {}",
                    worker.identity().pid,
                    worker.identity().process_birth
                ),
            )
            .unwrap();
            std::process::exit(0);
        }

        let isolated_tmp = tempfile::tempdir().unwrap();
        let report = isolated_tmp.path().join("worker-identity");
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("worker::process::tests::worker_dies_when_its_session_parent_exits")
            .env(FLAG, "1")
            .env("HCOM_PDEATHSIG_REPORT", &report)
            .env("TMPDIR", isolated_tmp.path())
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .unwrap();
        assert!(output.status.success());
        let identity = fs::read_to_string(report).unwrap();
        let (pid, birth) = identity
            .trim()
            .split_once(' ')
            .map(|(pid, birth)| (pid.parse::<u32>().unwrap(), birth.to_owned()))
            .expect("nested parent test did not report its worker PID");
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline
            && process_birth_identity(pid).is_ok_and(|current| current == birth)
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            process_birth_identity(pid)
                .map(|current| current != birth)
                .unwrap_or(true),
            "PDEATHSIG must terminate the worker after its session parent exits"
        );
    }

    fn fd_is_tty(fd: i32) -> bool {
        // SAFETY: isatty only inspects an integer file descriptor.
        unsafe { libc::isatty(fd) == 1 }
    }
}
