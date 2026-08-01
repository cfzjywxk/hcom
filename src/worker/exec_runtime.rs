//! Exec-seam Codex task worker runtime: one pinned `codex exec` process per
//! turn, no protocol conversation.
//!
//! The runtime gives a turn its capabilities up front (argv, environment,
//! mounts) and afterwards observes the world: process exit status, the
//! `thread.started` line on stdout (the only JSON parsed, as the session
//! identity proof), and the native `--output-last-message` file. Model output
//! is payload, never protocol: the reviewer verdict is classified leniently
//! from the final message's text, and every other byte is drained into
//! redacted evidence artifacts without interpretation.

use super::codex::{BWRAP_EXECUTABLE, DISABLED_CODEX_FEATURES};
use super::environment::{ExecutionEnvironmentLease, SecretRedactor};
use super::process::{ProcessGroupBinding, configure_worker_child};
use super::profile::validate_cli_help_contract;
use super::runtime::{
    CODEX_EXECUTABLE_SHA256, DeveloperOutcomeStatus, DeveloperOutcomeV1, MAX_OUTCOME_SUMMARY_CHARS,
    MAX_REVIEW_FINDING_MESSAGE_CHARS, ReviewFindingSeverity, ReviewFindingV1, ReviewerOutcomeV1,
    ReviewerVerdict, RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeOutcome,
    RuntimeSessionKey, RuntimeTelemetry, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnSpec,
    SanitizedRuntimeFailure, TaskWorkerRuntime,
};
use super::sandbox::{HostRootAccess, HostRootContract, HostRootMounts};
use super::verdict::{Verdict, VerdictClassification, classify_verdict};
use crate::artifact::{ArtifactAttempt, ArtifactKind, ArtifactRoot, ArtifactScope};
use crate::control_api::WorkerRole;
use crate::worker::runtime::RuntimeFailureClass;
use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::JoinHandle;
use std::time::Instant;

/// Pinned Codex 0.146 standalone build: the only executable this lane runs.
pub const CODEX_EXECUTABLE: &str =
    "/home/ywxk/.codex/packages/standalone/releases/0.146.0-x86_64-unknown-linux-musl/bin/codex";

pub const CODEX_EXEC_ADAPTER: &str = "codex-exec-0.146.0";
pub const CODEX_EXEC_VERSION: &str = "0.146.0";

/// Flags the closed exec invocation actually uses; the startup preflight
/// fails closed when any of them disappears from `codex exec --help`.
const EXEC_HELP_REQUIREMENTS: &[&str] = &[
    "resume",
    "--sandbox",
    "--skip-git-repo-check",
    "--json",
    "--strict-config",
    "--model",
    "--config",
    "--ignore-user-config",
    "--ignore-rules",
    "--disable",
    "--cd",
    "--output-last-message",
];
const RESUME_HELP_REQUIREMENTS: &[&str] = &[
    "--json",
    "--strict-config",
    "--model",
    "--config",
    "--output-last-message",
];

const THREAD_STARTED_LINE_CAP: usize = 8 * 1024;
const PIPE_CHUNK: usize = 32 * 1024;
const NATIVE_STREAM_CAP: u64 = 1024 * 1024 * 1024;
const RAW_FINAL_CHUNK: usize = 64 * 1024;
/// How much of a final message the supervisor may quote onward. The full text
/// is always sealed into the artifacts regardless of this bound.
const RELAY_WINDOW_BYTES: usize = 256 * 1024;
const MAX_HELP_OUTPUT: usize = 128 * 1024;
const STDERR_TAIL_BYTES: usize = 320;
const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const REAPER_ATTEMPTS: usize = 30;
const REAPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

const CLARIFICATION_PROMPT: &str = "Your previous final message did not contain a usable \
verdict. Reply once more: the FIRST line of your final message must be exactly \
`VERDICT: LGTM` or `VERDICT: REQUEST_CHANGES` (a single line, no decoration, no other \
text on that line), followed by your findings in free-form markdown.";

/// Task-private directories prepared by the driver for one task runtime.
#[derive(Debug, Clone)]
pub struct ExecTaskPaths {
    pub home: PathBuf,
    pub codex_home: PathBuf,
    pub temp: PathBuf,
    pub runtime: PathBuf,
}

/// Everything a task's exec runtime needs, assembled by the driver-side
/// factory. `artifact_root_path` points at the durable `hcom-tasks` tree;
/// `raw output` targets live under the private `runtime` directory instead.
pub struct ExecRuntimeConfig {
    pub codex: PathBuf,
    pub bwrap: Option<PathBuf>,
    pub repository_root: PathBuf,
    pub paths: ExecTaskPaths,
    pub auth_source: PathBuf,
    pub cargo_bin_source: PathBuf,
    pub rustup_home_source: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
    pub lease: ExecutionEnvironmentLease,
    pub artifact_root_path: PathBuf,
    pub run_id: String,
    pub task_id: String,
}

/// Startup preflight over the pinned executable: byte identity plus the help
/// contract for exactly the flags the closed shape uses.
#[derive(Debug, Clone)]
pub struct ExecPreflight {
    codex: PathBuf,
    bwrap: PathBuf,
}

impl ExecPreflight {
    pub fn verify_pinned() -> Result<Self, RuntimeError> {
        let codex = PathBuf::from(CODEX_EXECUTABLE);
        let digest = sha256_file(&codex)
            .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
        if digest != CODEX_EXECUTABLE_SHA256 {
            return Err(RuntimeError::invalid_contract(
                "pinned codex executable bytes changed",
            ));
        }
        let help = bounded_help_output(&codex, &["exec", "--help"])
            .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
        validate_cli_help_contract("codex exec", &help, EXEC_HELP_REQUIREMENTS)
            .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
        let help = bounded_help_output(
            &codex,
            &["exec", "--sandbox", "read-only", "resume", "--help"],
        )
        .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
        validate_cli_help_contract("codex exec resume", &help, RESUME_HELP_REQUIREMENTS)
            .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
        let bwrap = PathBuf::from(BWRAP_EXECUTABLE);
        if !bwrap.is_file() {
            return Err(RuntimeError::invalid_contract(
                "pinned bubblewrap executable is missing",
            ));
        }
        Ok(Self { codex, bwrap })
    }

    pub fn codex(&self) -> &Path {
        &self.codex
    }

    pub fn bwrap(&self) -> &Path {
        &self.bwrap
    }
}

pub fn codex_exec_contract_identity() -> RuntimeContractIdentity {
    let argv_contract: Vec<String> = EXEC_HELP_REQUIREMENTS
        .iter()
        .map(|flag| (*flag).to_owned())
        .collect();
    let mut hasher = Sha256::new();
    hasher.update(b"hcom-codex-exec-argv-contract-v1\n");
    for flag in &argv_contract {
        hasher.update(flag.as_bytes());
        hasher.update(b"\n");
    }
    RuntimeContractIdentity {
        adapter: CODEX_EXEC_ADAPTER.into(),
        cli_version: CODEX_EXEC_VERSION.into(),
        executable_sha256: CODEX_EXECUTABLE_SHA256.into(),
        schema_canonical_sha256: hex_digest(hasher),
        selected_methods: argv_contract,
        selected_fields: vec!["thread.started.thread_id".into()],
    }
}

struct ExecSession {
    role: WorkerRole,
    /// Native working root: the Architect's project directory.
    cwd: PathBuf,
    /// The task's repository. Equal to `cwd` when the project itself is the
    /// repository; otherwise an extra scope the worker must still reach.
    task_repository: PathBuf,
    instructions: String,
    label: String,
    thread_id: Option<String>,
    turn_sequence: u32,
}

/// What a poll observed about the child.
enum PollOutcome {
    Exited(ExitStatus),
    /// Timed out; carries the kill failure when the group survived SIGKILL.
    TimedOut(Option<String>),
}

enum TurnState {
    Running(Box<RunningTurn>),
    Done(RuntimeTurnPoll),
}

struct ExecTurn {
    session: RuntimeSessionKey,
    state: TurnState,
}

/// Bytes seen, the tail kept for diagnostics, and any evidence I/O failure.
type StderrDrained = (u64, Vec<u8>, Option<String>);

struct StdoutDrained {
    thread_id: Option<String>,
    bytes: u64,
    /// Set when the pipe read or the artifact write failed. Losing evidence
    /// must stop the run instead of silently routing an incomplete record.
    io_error: Option<String>,
}

struct RunningTurn {
    child: Child,
    group: ProcessGroupBinding,
    started: Instant,
    spec: RuntimeTurnSpec,
    attempt: ArtifactAttempt,
    raw_final: PathBuf,
    raw_final_identity: (u64, u64),
    redactor: SecretRedactor,
    expected_thread: Option<String>,
    stdout_thread: Option<JoinHandle<StdoutDrained>>,
    stderr_thread: Option<JoinHandle<StderrDrained>>,
    stdin_thread: Option<JoinHandle<Option<String>>>,
    clarification_used: bool,
    /// The pre-clarification final message, carried forward so the relayed
    /// outcome keeps the reviewer's original findings alongside the verdict.
    prior_text: Option<String>,
    attempt_no: u32,
}

/// One task's exec runtime. Sessions are identities plus a captured
/// `thread_id`; every turn is a fresh pinned process.
pub struct ExecTaskWorkerRuntime {
    contract: RuntimeContractIdentity,
    config: ExecRuntimeConfig,
    artifact_root: ArtifactRoot,
    host_contract: Option<HostRootContract>,
    sessions: BTreeMap<RuntimeSessionKey, ExecSession>,
    turns: BTreeMap<RuntimeTurnKey, ExecTurn>,
    next_session: u64,
    next_turn: u64,
    active_turn: Option<RuntimeTurnKey>,
    shut_down: bool,
}

impl ExecTaskWorkerRuntime {
    pub fn open(config: ExecRuntimeConfig) -> Result<Self, RuntimeError> {
        let artifact_root = ArtifactRoot::open(&config.artifact_root_path)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let host_contract = if config.bwrap.is_some() {
            Some(
                HostRootContract::capture(&config.cargo_bin_source, &config.rustup_home_source)
                    .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?,
            )
        } else {
            None
        };
        Ok(Self {
            contract: codex_exec_contract_identity(),
            config,
            artifact_root,
            host_contract,
            sessions: BTreeMap::new(),
            turns: BTreeMap::new(),
            next_session: 0,
            next_turn: 0,
            active_turn: None,
            shut_down: false,
        })
    }

    fn build_native_argv(
        &self,
        session: &ExecSession,
        spec: &RuntimeTurnSpec,
        resume_thread: Option<&str>,
        raw_final: &Path,
    ) -> Vec<OsString> {
        let profile = &spec.profile;
        let mut argv: Vec<OsString> = vec![
            "exec".into(),
            "--sandbox".into(),
            profile.sandbox.as_str().into(),
            "--skip-git-repo-check".into(),
        ];
        // The developer writes in the task repository; when that is not the
        // project directory it needs an explicit extra writable scope.
        if session.role == WorkerRole::Developer && session.task_repository != session.cwd {
            argv.push("--add-dir".into());
            argv.push(session.task_repository.as_os_str().to_owned());
        }
        if let Some(thread) = resume_thread {
            argv.push("resume".into());
            argv.push(thread.into());
        }
        argv.push("--json".into());
        argv.push("--strict-config".into());
        argv.push("--model".into());
        argv.push(profile.model.as_str().into());
        argv.push("--config".into());
        argv.push(format!("model_reasoning_effort=\"{}\"", profile.reasoning_effort).into());
        argv.push("--config".into());
        argv.push(format!("approval_policy=\"{}\"", profile.approval_policy.as_str()).into());
        argv.push("--config".into());
        argv.push("mcp_servers={}".into());
        // W0-proven: these two --config entries keep complete environment
        // inheritance effective at the tool-command layer even under
        // --ignore-user-config (which skips the private CODEX_HOME config).
        argv.push("--config".into());
        argv.push("shell_environment_policy.inherit=\"all\"".into());
        argv.push("--config".into());
        argv.push("shell_environment_policy.ignore_default_excludes=true".into());
        argv.push("--ignore-user-config".into());
        argv.push("--ignore-rules".into());
        for feature in DISABLED_CODEX_FEATURES {
            argv.push("--disable".into());
            argv.push((*feature).into());
        }
        if resume_thread.is_none() {
            argv.push("--cd".into());
            argv.push(session.cwd.as_os_str().to_owned());
        }
        argv.push("--output-last-message".into());
        argv.push(raw_final.as_os_str().to_owned());
        argv.push("-".into());
        argv
    }

    fn outer_command(
        &self,
        session: &ExecSession,
        attempt_dir: &Path,
        native_argv: Vec<OsString>,
    ) -> Result<Command> {
        let Some(bwrap) = &self.config.bwrap else {
            let mut command = Command::new(&self.config.codex);
            command.args(&native_argv);
            command.current_dir(&session.cwd);
            return Ok(command);
        };
        let contract = self
            .host_contract
            .as_ref()
            .ok_or_else(|| anyhow!("bwrap configured without a host root contract"))?;
        contract.revalidate()?;
        let auth_target = self.config.paths.codex_home.join("auth.json");
        let repo: &Path = &session.task_repository;
        let project: &Path = &session.cwd;
        let repo_is_project = repo == project;
        let (writable_roots, readable_roots): (Vec<&Path>, Vec<&Path>) =
            if session.role == WorkerRole::Developer {
                // Developer writes the repository; the project is readable so
                // task notes and plans stay reachable.
                let readable = if repo_is_project {
                    Vec::new()
                } else {
                    vec![project]
                };
                (vec![repo], readable)
            } else {
                // Reviewer reads both and writes neither.
                let readable = if repo_is_project {
                    vec![repo]
                } else {
                    vec![repo, project]
                };
                (Vec::new(), readable)
            };
        let extra: Vec<&Path> = vec![&self.config.paths.temp, &self.config.paths.runtime];
        let outer_argv = contract.host_root_argv(HostRootMounts {
            isolated_home: &self.config.paths.home,
            native_config: &self.config.paths.codex_home,
            launch_cwd: &session.cwd,
            artifact_dir: attempt_dir,
            auth_source: &self.config.auth_source,
            auth_target: &auth_target,
            readable_roots: &readable_roots,
            writable_roots: &writable_roots,
            read_only_files: &[&self.config.codex],
            extra_writable_dirs: &extra,
            host_root_access: HostRootAccess::Hidden,
            masked_dirs: &[],
        })?;
        let mut command = Command::new(bwrap);
        command.args(&outer_argv);
        command.arg("--");
        command.arg(&self.config.codex);
        command.args(&native_argv);
        Ok(command)
    }

    fn spawn_invocation(
        &mut self,
        session_key: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
        attempt_no: u32,
        prompt_override: Option<String>,
        clarification_used: bool,
        prior_text: Option<String>,
    ) -> Result<RunningTurn, RuntimeError> {
        let session = self
            .sessions
            .get(&session_key)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime session"))?;
        let resume_thread = session.thread_id.clone();
        let is_create = resume_thread.is_none();

        let prompt_body = prompt_override.unwrap_or_else(|| spec.prompt.clone());
        let full_prompt = if is_create && !session.instructions.is_empty() {
            format!("{}\n\n{}", session.instructions, prompt_body)
        } else {
            prompt_body
        };

        let scope = ArtifactScope {
            run_id: self.config.run_id.clone(),
            task_id: self.config.task_id.clone(),
            role: session.role,
            logical_session_id: session.label.clone(),
            turn_sequence: session.turn_sequence,
            attempt: attempt_no,
        };
        // The prompt is evidence, not a credential: seeding the redactor with
        // it would turn prompt.md into "[REDACTED]".
        let attempt = ArtifactAttempt::create_with_environment_secrets_only(
            &self.artifact_root,
            scope,
            &self.config.lease,
            utf8_prefix(&full_prompt, 200 * 1024).as_bytes(),
        )
        .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let mut prompt_writer = attempt
            .start_native_stream(ArtifactKind::NativePrompt, NATIVE_STREAM_CAP)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        prompt_writer
            .write_chunk(full_prompt.as_bytes())
            .and_then(|_| prompt_writer.finish())
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let redactor = self.config.lease.redactor();

        // Raw --output-last-message target lives in the private runtime dir,
        // never inside the durable tree; identity is pinned before spawn.
        let raw_final = self.config.paths.runtime.join(format!(
            "raw-final-{}-t{}-a{}.md",
            session.label, session.turn_sequence, attempt_no
        ));
        let raw_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600)
            .open(&raw_final)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let raw_metadata = raw_file
            .metadata()
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let raw_final_identity = (raw_metadata.dev(), raw_metadata.ino());
        drop(raw_file);

        // Evidence writers are opened before the child exists so a failure
        // here can never leak a running process.
        let stdout_writer = attempt
            .start_native_stream(ArtifactKind::NativeStdout, NATIVE_STREAM_CAP)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let stderr_writer = attempt
            .start_native_stream(ArtifactKind::NativeStderr, NATIVE_STREAM_CAP)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;

        let native_argv =
            self.build_native_argv(session, &spec, resume_thread.as_deref(), &raw_final);
        let mut command = self
            .outer_command(session, attempt.directory_path(), native_argv)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.env_clear();
        for (name, value) in &self.config.environment {
            command.env(name, value);
        }
        let parent = std::process::id();
        // SAFETY: pre_exec only calls async-signal-safe syscalls.
        unsafe {
            command.pre_exec(move || configure_worker_child(parent));
        }
        let mut child = command
            .spawn()
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let group = ProcessGroupBinding::capture(&mut child)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;

        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // A partially delivered prompt would make the worker answer a
        // different question, so the transport result is checked, never
        // discarded.
        let stdin_thread = stdin.map(|mut pipe| {
            let bytes = full_prompt.into_bytes();
            std::thread::spawn(move || -> Option<String> {
                use std::io::Write;
                if let Err(error) = pipe.write_all(&bytes) {
                    return Some(format!("prompt delivery failed: {error}"));
                }
                if let Err(error) = pipe.flush() {
                    return Some(format!("prompt flush failed: {error}"));
                }
                None
            })
        });
        let stdout_thread =
            stdout.map(|pipe| std::thread::spawn(move || drain_stdout(pipe, stdout_writer)));
        let stderr_thread =
            stderr.map(|pipe| std::thread::spawn(move || drain_stderr(pipe, stderr_writer)));

        Ok(RunningTurn {
            child,
            group,
            started: Instant::now(),
            spec,
            attempt,
            raw_final,
            raw_final_identity,
            redactor,
            expected_thread: resume_thread,
            stdout_thread,
            stderr_thread,
            stdin_thread,
            clarification_used,
            prior_text,
            attempt_no,
        })
    }

    fn finalize(
        &mut self,
        turn_key: RuntimeTurnKey,
        status: ExitStatus,
        mut running: Box<RunningTurn>,
    ) -> Result<RuntimeTurnPoll, RuntimeError> {
        // The leader exited; any descendant still holding the pipes must go
        // first, or the drain joins below never return.
        let settled = running.group.settle_after_exit(CANCEL_GRACE);
        let prompt_error = running
            .stdin_thread
            .take()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Some("prompt delivery thread panicked".into()))
            })
            .unwrap_or(None);
        let stdout = running
            .stdout_thread
            .take()
            .map(|handle| {
                handle.join().unwrap_or(StdoutDrained {
                    thread_id: None,
                    bytes: 0,
                    io_error: Some("stdout drain thread panicked".into()),
                })
            })
            .unwrap_or(StdoutDrained {
                thread_id: None,
                bytes: 0,
                io_error: None,
            });
        let (stderr_bytes, stderr_tail, stderr_io_error) = running
            .stderr_thread
            .take()
            .map(|handle| handle.join().unwrap_or((0, Vec::new(), None)))
            .unwrap_or((0, Vec::new(), None));
        let telemetry = RuntimeTelemetry {
            protocol_bytes: stdout.bytes,
            stderr_bytes,
            notification_count: 0,
        };

        // Ingest the raw final message: pin identity, bound the read, seal a
        // redacted copy into the durable artifacts, and remove the raw file.
        let mut final_writer = running
            .attempt
            .start_native_stream(ArtifactKind::NativeFinal, NATIVE_STREAM_CAP)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let raw = ingest_raw_final(
            &running.raw_final,
            running.raw_final_identity,
            running.redactor.trailing_guard_bytes(),
            &running.redactor,
            &mut final_writer,
        );
        let sealed_result = final_writer.finish();
        let sealed = match (&raw, sealed_result) {
            (Ok(Some(text)), Ok(_)) => Some(text.clone()),
            (Ok(_), Err(error)) => {
                return fail(
                    RuntimeFailureClass::Process,
                    single_line(&format!("final message evidence seal failed: {error}")),
                    telemetry,
                );
            }
            _ => None,
        };

        let session_key = {
            let turn = self
                .turns
                .get(&turn_key)
                .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
            turn.session
        };

        // Routing preconditions: exit 0 AND session proof AND non-empty final
        // message. Anything else is a process-level failure; artifacts stay as
        // evidence but never route.
        if let Some(error) = prompt_error
            .or_else(|| stdout.io_error.clone())
            .or(stderr_io_error)
        {
            return fail(RuntimeFailureClass::Process, single_line(&error), telemetry);
        }
        if !status.success() {
            let detail = exit_failure_detail(status, &stderr_tail, &running.redactor);
            let class = RuntimeFailureClass::Process;
            return fail(class, detail, telemetry);
        }
        match settled {
            Ok(false) => {}
            Ok(true) => {
                return fail(
                    RuntimeFailureClass::Process,
                    "codex exec left background descendants that had to be killed".into(),
                    telemetry,
                );
            }
            Err(error) => {
                return fail(
                    RuntimeFailureClass::Process,
                    single_line(&format!("worker descendants could not be settled: {error}")),
                    telemetry,
                );
            }
        }
        let Some(thread_id) = stdout.thread_id else {
            return fail(
                RuntimeFailureClass::Protocol,
                "codex exec never emitted thread.started on stdout".into(),
                telemetry,
            );
        };
        if let Some(expected) = &running.expected_thread
            && expected != &thread_id
        {
            return fail(
                RuntimeFailureClass::Protocol,
                "codex exec resume returned a different thread id".into(),
                telemetry,
            );
        }
        if let Err(error) = &raw {
            return fail(
                RuntimeFailureClass::Protocol,
                single_line(&format!("raw final message ingestion failed: {error}")),
                telemetry,
            );
        }
        let Some(final_text) = sealed.filter(|text| !text.trim().is_empty()) else {
            return fail(
                RuntimeFailureClass::Process,
                "codex exec produced an empty final message".into(),
                telemetry,
            );
        };

        let session = self
            .sessions
            .get_mut(&session_key)
            .ok_or_else(|| RuntimeError::invalid_identity("session disappeared"))?;
        if session.thread_id.is_none() {
            session.thread_id = Some(thread_id);
        }
        let role = session.role;

        // After a format clarification the relayed text keeps BOTH turns: the
        // original findings and the clarified verdict. Relaying only the
        // second (usually a bare `VERDICT:` line) would drop the substance the
        // developer needs.
        let relay_text = match &running.prior_text {
            Some(prior) => format!(
                "{}\n\n---\n[hcom: verdict clarification follow-up]\n\n{}",
                prior.trim_end(),
                final_text.trim_start()
            ),
            None => final_text.clone(),
        };

        let outcome = match role {
            WorkerRole::Developer => RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
                summary: truncate_chars(&relay_text, MAX_OUTCOME_SUMMARY_CHARS),
                questions: Vec::new(),
            }),
            WorkerRole::Reviewer => match classify_verdict(&final_text) {
                VerdictClassification::Determined(Verdict::Lgtm) => {
                    RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                        verdict: ReviewerVerdict::Lgtm,
                        summary: truncate_chars(&relay_text, MAX_OUTCOME_SUMMARY_CHARS),
                        findings: Vec::new(),
                    })
                }
                VerdictClassification::Determined(Verdict::RequestChanges) => {
                    RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                        verdict: ReviewerVerdict::RequestChanges,
                        summary: truncate_chars(&relay_text, MAX_OUTCOME_SUMMARY_CHARS),
                        findings: vec![ReviewFindingV1 {
                            severity: ReviewFindingSeverity::Major,
                            path: None,
                            line: None,
                            message: truncate_chars(&relay_text, MAX_REVIEW_FINDING_MESSAGE_CHARS),
                        }],
                    })
                }
                VerdictClassification::Undetermined(reason) => {
                    if running.clarification_used {
                        return fail(
                            RuntimeFailureClass::Contract,
                            single_line(&format!(
                                "reviewer verdict undetermined after clarification ({reason:?}): {}",
                                truncate_chars(&final_text, 200)
                            )),
                            telemetry,
                        );
                    }
                    // One format clarification turn inside the same logical
                    // turn: a fresh native invocation (attempt + 1) resuming
                    // the same thread. It never consumes a review round and
                    // both attempts' artifacts stay on disk.
                    let spec = running.spec.clone();
                    let next_attempt = running.attempt_no + 1;
                    let clarification = self.spawn_invocation(
                        session_key,
                        spec,
                        next_attempt,
                        Some(CLARIFICATION_PROMPT.to_string()),
                        true,
                        Some(final_text.clone()),
                    )?;
                    let turn = self
                        .turns
                        .get_mut(&turn_key)
                        .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
                    turn.state = TurnState::Running(Box::new(clarification));
                    return Ok(RuntimeTurnPoll::Pending { telemetry });
                }
            },
        };
        outcome.validate()?;
        Ok(RuntimeTurnPoll::Completed { outcome, telemetry })
    }
}

fn fail(
    class: RuntimeFailureClass,
    detail: String,
    telemetry: RuntimeTelemetry,
) -> Result<RuntimeTurnPoll, RuntimeError> {
    Ok(RuntimeTurnPoll::Failed {
        failure: SanitizedRuntimeFailure::new(class, detail, false)?,
        telemetry,
    })
}

/// Keep trying to kill a process group that survived the inline attempt.
///
/// The supervisor must neither block on it nor forget it: an abandoned worker
/// keeps running and burning tokens.
fn spawn_detached_reaper(running: Box<RunningTurn>) {
    std::thread::spawn(move || {
        let mut running = running;
        for _ in 0..REAPER_ATTEMPTS {
            std::thread::sleep(REAPER_INTERVAL);
            if running
                .group
                .terminate_and_reap(&mut running.child, CANCEL_GRACE)
                .is_ok()
            {
                return;
            }
        }
        // Give up only after a bounded series of attempts; Drop runs last.
    });
}

impl Drop for RunningTurn {
    /// Backstop for every path that drops a live turn without finalizing it
    /// (an error return, a panic, or a supervisor teardown): the process group
    /// dies with the run rather than becoming an orphan.
    fn drop(&mut self) {
        if self
            .group
            .terminate_and_reap(&mut self.child, CANCEL_GRACE)
            .is_ok()
        {
            // Joining is only bounded once the group is actually gone.
            if let Some(handle) = self.stdin_thread.take() {
                let _ = handle.join();
            }
            if let Some(handle) = self.stdout_thread.take() {
                let _ = handle.join();
            }
            if let Some(handle) = self.stderr_thread.take() {
                let _ = handle.join();
            }
        }
        let _ = fs::remove_file(&self.raw_final);
    }
}

impl TaskWorkerRuntime for ExecTaskWorkerRuntime {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError> {
        if self.shut_down {
            return Err(RuntimeError::invalid_transition("runtime is shut down"));
        }
        spec.validate()?;
        self.next_session += 1;
        let key = RuntimeSessionKey::from_counter(self.next_session)?;
        self.sessions.insert(
            key,
            ExecSession {
                role: spec.role,
                cwd: spec.cwd,
                task_repository: spec.task_repository,
                instructions: spec.developer_instructions,
                label: format!("session-{}", self.next_session),
                thread_id: None,
                turn_sequence: 0,
            },
        );
        Ok(key)
    }

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError> {
        if self.shut_down {
            return Err(RuntimeError::invalid_transition("runtime is shut down"));
        }
        spec.validate()?;
        if self.active_turn.is_some() {
            return Err(RuntimeError::invalid_transition(
                "another exec turn is still running",
            ));
        }
        {
            let entry = self
                .sessions
                .get_mut(&session)
                .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime session"))?;
            if entry.role != spec.role {
                return Err(RuntimeError::invalid_identity(
                    "turn role does not match its session role",
                ));
            }
            entry.turn_sequence = entry
                .turn_sequence
                .checked_add(1)
                .ok_or_else(|| RuntimeError::internal("turn sequence overflow"))?;
        }
        let running = self.spawn_invocation(session, spec, 1, None, false, None)?;
        self.next_turn += 1;
        let key = RuntimeTurnKey::from_counter(self.next_turn)?;
        self.turns.insert(
            key,
            ExecTurn {
                session,
                state: TurnState::Running(Box::new(running)),
            },
        );
        self.active_turn = Some(key);
        Ok(key)
    }

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
        let state = {
            let entry = self
                .turns
                .get_mut(&turn)
                .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
            match &mut entry.state {
                TurnState::Done(poll) => return Ok(poll.clone()),
                TurnState::Running(running) => {
                    if running.started.elapsed() >= running.spec.timeout {
                        // Record whether the group actually died: a failed kill
                        // must not lead into unbounded waits below.
                        Some(PollOutcome::TimedOut(
                            running
                                .group
                                .terminate_and_reap(&mut running.child, CANCEL_GRACE)
                                .err()
                                .map(|error| single_line(&error.to_string())),
                        ))
                    } else {
                        match running.child.try_wait() {
                            Ok(Some(status)) => Some(PollOutcome::Exited(status)),
                            Ok(None) => None,
                            Err(error) => {
                                return Err(RuntimeError::internal(single_line(&format!(
                                    "failed to poll exec child: {error}"
                                ))));
                            }
                        }
                    }
                }
            }
        };
        let Some(exit) = state else {
            return Ok(RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            });
        };

        let entry = self
            .turns
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
        let running = match std::mem::replace(
            &mut entry.state,
            TurnState::Done(RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            }),
        ) {
            TurnState::Running(running) => running,
            TurnState::Done(_) => unreachable!("running state checked above"),
        };

        let poll = match exit {
            PollOutcome::Exited(status) => self.finalize(turn, status, running)?,
            PollOutcome::TimedOut(kill_error) => {
                let mut running = running;
                let detail = match &kill_error {
                    // The group is gone, so the drains are guaranteed to end;
                    // joining them keeps the evidence complete.
                    None => {
                        if let Some(handle) = running.stdin_thread.take() {
                            let _ = handle.join();
                        }
                        if let Some(handle) = running.stdout_thread.take() {
                            let _ = handle.join();
                        }
                        if let Some(handle) = running.stderr_thread.take() {
                            let _ = handle.join();
                        }
                        format!(
                            "exec turn exceeded its {}s wall-clock limit",
                            running.spec.timeout.as_secs()
                        )
                    }
                    // The group survived SIGKILL. Joining could block forever,
                    // so report and leave the threads detached rather than
                    // hanging the supervisor's own watchdog.
                    Some(error) => single_line(&format!(
                        "exec turn exceeded its {}s wall-clock limit and its process group \
                         could not be terminated: {error}",
                        running.spec.timeout.as_secs()
                    )),
                };
                let _ = fs::remove_file(&running.raw_final);
                if kill_error.is_some() {
                    // The group survived SIGKILL. Hand the still-owned handle
                    // to a detached reaper that keeps retrying instead of
                    // abandoning the process (mem::forget) or blocking the
                    // supervisor on an unbounded join.
                    spawn_detached_reaper(running);
                }
                RuntimeTurnPoll::Failed {
                    failure: SanitizedRuntimeFailure::new(
                        RuntimeFailureClass::Timeout,
                        detail,
                        false,
                    )?,
                    telemetry: RuntimeTelemetry::default(),
                }
            }
        };

        let entry = self
            .turns
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
        if matches!(poll, RuntimeTurnPoll::Pending { .. }) {
            // A clarification invocation replaced the running state; keep it.
            return Ok(poll);
        }
        entry.state = TurnState::Done(poll.clone());
        self.active_turn = None;
        Ok(poll)
    }

    fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
        let entry = self
            .turns
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime turn"))?;
        if let TurnState::Running(running) = &mut entry.state {
            let killed = running
                .group
                .terminate_and_reap(&mut running.child, CANCEL_GRACE);
            if killed.is_ok() {
                // Only safe to join once the group is gone.
                if let Some(handle) = running.stdin_thread.take() {
                    let _ = handle.join();
                }
                if let Some(handle) = running.stdout_thread.take() {
                    let _ = handle.join();
                }
                if let Some(handle) = running.stderr_thread.take() {
                    let _ = handle.join();
                }
            }
            let _ = fs::remove_file(&running.raw_final);
            let detail = match &killed {
                Ok(_) => "exec turn canceled by the supervisor".to_string(),
                Err(error) => single_line(&format!(
                    "exec turn canceled but its process group could not be terminated: {error}"
                )),
            };
            entry.state = TurnState::Done(RuntimeTurnPoll::Failed {
                failure: SanitizedRuntimeFailure::new(
                    RuntimeFailureClass::Canceled,
                    detail,
                    false,
                )?,
                telemetry: RuntimeTelemetry::default(),
            });
        }
        self.active_turn = None;
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if let Some(turn) = self.active_turn {
            let _ = self.cancel_turn(turn);
        }
        self.shut_down = true;
        Ok(())
    }
}

impl Drop for ExecTaskWorkerRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn drain_stdout(
    mut pipe: impl Read,
    mut writer: crate::artifact::BoundedArtifactWriter,
) -> StdoutDrained {
    let mut first_line: Vec<u8> = Vec::new();
    let mut thread_id: Option<String> = None;
    let mut saw_newline = false;
    let mut total: u64 = 0;
    let mut io_error: Option<String> = None;
    let mut buffer = vec![0_u8; PIPE_CHUNK];
    loop {
        let read = match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                io_error = Some(format!("stdout read failed: {error}"));
                break;
            }
        };
        let chunk = &buffer[..read];
        total += read as u64;
        if let Err(error) = writer.write_chunk(chunk) {
            io_error = Some(format!("stdout evidence write failed: {error}"));
            break;
        }
        if !saw_newline {
            for (index, byte) in chunk.iter().enumerate() {
                if *byte == b'\n' {
                    saw_newline = true;
                    thread_id = parse_thread_started(&first_line);
                    let _ = index;
                    break;
                }
                if first_line.len() < THREAD_STARTED_LINE_CAP {
                    first_line.push(*byte);
                }
            }
        }
    }
    if !saw_newline && thread_id.is_none() {
        thread_id = parse_thread_started(&first_line);
    }
    if let Err(error) = writer.finish() {
        io_error.get_or_insert(format!("stdout evidence seal failed: {error}"));
    }
    StdoutDrained {
        thread_id,
        bytes: total,
        io_error,
    }
}

fn drain_stderr(
    mut pipe: impl Read,
    mut writer: crate::artifact::BoundedArtifactWriter,
) -> StderrDrained {
    let mut total: u64 = 0;
    let mut tail: Vec<u8> = Vec::new();
    let mut io_error: Option<String> = None;
    let mut buffer = vec![0_u8; PIPE_CHUNK];
    loop {
        let read = match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                io_error = Some(format!("stderr read failed: {error}"));
                break;
            }
        };
        let chunk = &buffer[..read];
        total += read as u64;
        if let Err(error) = writer.write_chunk(chunk) {
            io_error = Some(format!("stderr evidence write failed: {error}"));
            break;
        }
        tail.extend_from_slice(chunk);
        if tail.len() > STDERR_TAIL_BYTES {
            let cut = tail.len() - STDERR_TAIL_BYTES;
            tail.drain(..cut);
        }
    }
    if let Err(error) = writer.finish() {
        io_error.get_or_insert(format!("stderr evidence seal failed: {error}"));
    }
    (total, tail, io_error)
}

/// Parse exactly one documented event: `{"type":"thread.started","thread_id":...}`.
fn parse_thread_started(line: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("thread.started") {
        return None;
    }
    let thread = value.get("thread_id").and_then(serde_json::Value::as_str)?;
    if thread.is_empty() || thread.len() > 256 {
        return None;
    }
    Some(thread.to_owned())
}

/// Stream the CLI's final-message file through the redactor, then delete it.
///
/// Identity is pinned (same inode, still a regular file). Memory is bounded,
/// not the file: the whole message is redacted and sealed, so a legal long
/// final message keeps its tail. Chunks overlap by `guard` bytes so a
/// credential straddling a read boundary is still recognized.
fn ingest_raw_final(
    path: &Path,
    expected: (u64, u64),
    guard: usize,
    redactor: &SecretRedactor,
    sink: &mut crate::artifact::BoundedArtifactWriter,
) -> Result<Option<String>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("raw final message target is not a regular file");
    }
    if (metadata.dev(), metadata.ino()) != expected {
        bail!("raw final message file identity changed since it was created");
    }
    // Read in bounded chunks, carrying `guard` bytes forward so a secret split
    // across a boundary is still matched. `relayed` keeps only the leading
    // window the supervisor may quote; the sealed artifact gets everything.
    let mut reader = std::io::BufReader::new(file);
    let mut carry: Vec<u8> = Vec::new();
    let mut chunk = vec![0_u8; RAW_FINAL_CHUNK];
    let mut relayed = String::new();
    let mut total: u64 = 0;
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        carry.extend_from_slice(&chunk[..read]);
        // Hold back the guard window; it may be the head of a secret whose
        // tail arrives in the next chunk.
        let emit_len = carry.len().saturating_sub(guard);
        if emit_len == 0 {
            continue;
        }
        let mut boundary = emit_len;
        while boundary > 0 && !is_utf8_boundary(&carry, boundary) {
            boundary -= 1;
        }
        if boundary == 0 {
            continue;
        }
        let piece: Vec<u8> = carry.drain(..boundary).collect();
        let redacted = redactor.redact(&String::from_utf8_lossy(&piece));
        sink.write_chunk(redacted.as_bytes())?;
        if relayed.len() < RELAY_WINDOW_BYTES {
            relayed.push_str(&redacted);
        }
    }
    if !carry.is_empty() {
        let redacted = redactor.redact(&String::from_utf8_lossy(&carry));
        sink.write_chunk(redacted.as_bytes())?;
        if relayed.len() < RELAY_WINDOW_BYTES {
            relayed.push_str(&redacted);
        }
    }
    let _ = fs::remove_file(path);
    if total == 0 {
        Ok(None)
    } else {
        Ok(Some(relayed))
    }
}

fn is_utf8_boundary(bytes: &[u8], index: usize) -> bool {
    index == bytes.len() || (bytes[index] & 0xC0) != 0x80
}

fn exit_failure_detail(
    status: ExitStatus,
    stderr_tail: &[u8],
    redactor: &SecretRedactor,
) -> String {
    let cause = match (status.code(), status.signal()) {
        (Some(code), _) => format!("codex exec exited with status {code}"),
        (None, Some(signal)) => format!("codex exec was killed by signal {signal}"),
        (None, None) => "codex exec ended without a status".to_string(),
    };
    let tail = redactor.redact(&String::from_utf8_lossy(stderr_tail));
    let tail: String = tail
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let tail = tail.trim();
    if tail.is_empty() {
        cause
    } else {
        single_line(&format!("{cause}; stderr tail: {tail}"))
    }
}

/// Cut a relayed message to `limit` characters, always leaving a visible
/// marker: the next role must be able to tell that it is reading a prefix.
fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    const MARKER: &str =
        "\n\n[hcom: message truncated for relay; the full text is in this run's artifacts]";
    let keep = limit.saturating_sub(MARKER.chars().count());
    let mut out: String = text.chars().take(keep).collect();
    out.push_str(MARKER);
    out
}

fn utf8_prefix(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn single_line(text: &str) -> String {
    let mut out: String = text
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if out.len() > 900 {
        let mut end = 900;
        while end > 0 && !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_digest(hasher))
}

fn hex_digest(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn bounded_help_output(executable: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(executable)
        .args(args)
        .env_clear()
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("failed to run {} {:?}", executable.display(), args))?;
    if !output.status.success() {
        bail!(
            "{} {:?} exited with {}",
            executable.display(),
            args,
            output.status
        );
    }
    if output.stdout.len() > MAX_HELP_OUTPUT {
        bail!("help output exceeds its bound");
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::environment::EnvironmentPolicy;
    use crate::worker::runtime::{OutcomeContract, RuntimeProfile, RuntimeTurnPurpose};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    struct Fixture {
        _temp: tempfile::TempDir,
        capture: PathBuf,
        runtime_dir: PathBuf,
        artifacts: PathBuf,
        repo: PathBuf,
        runtime: ExecTaskWorkerRuntime,
    }

    const SECRET_VALUE: &str = "hushhush-secret-123456";

    fn fixture(script_body: &str) -> Fixture {
        let temp = tempfile::tempdir().expect("temp");
        let root = fs::canonicalize(temp.path()).expect("canonical temp");
        let artifacts = root.join("artifacts");
        let home = root.join("home");
        let codex_home = home.join(".codex");
        let tmp = root.join("tmp");
        let runtime_dir = root.join("run");
        let repo = root.join("repo");
        let capture = root.join("capture");
        for dir in [
            &artifacts,
            &home,
            &codex_home,
            &tmp,
            &runtime_dir,
            &repo,
            &capture,
        ] {
            fs::create_dir_all(dir).unwrap();
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let auth = root.join("auth.json");
        fs::write(&auth, b"{}").unwrap();

        let script = root.join("fake-codex");
        let prelude = r#"
OUT=""
RESUME=""
prev=""
for a in "$@"; do
  case "$prev" in
    --output-last-message) OUT="$a";;
    resume) RESUME="$a";;
  esac
  prev="$a"
done
{ echo ===INVOCATION===; printf '%s\n' "$@"; } >> "$CAPTURE/args.log"
pwd >> "$CAPTURE/cwd.log"
cat >> "$CAPTURE/stdin.log"
echo ===STDIN-END=== >> "$CAPTURE/stdin.log"
"#;
        fs::write(
            &script,
            format!("#!/bin/sh\nset -eu\n{prelude}\n{script_body}\n"),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

        let values = vec![
            (
                "PATH".to_string(),
                std::env::var("PATH").unwrap_or_default(),
            ),
            (
                "CAPTURE".to_string(),
                capture.to_string_lossy().into_owned(),
            ),
            ("FAKE_SECRET_TOKEN".to_string(), SECRET_VALUE.to_string()),
        ];
        let lease = ExecutionEnvironmentLease::capture(
            "lease-exec-test",
            "epoch-exec-test",
            &EnvironmentPolicy::baseline(),
            values.clone(),
        )
        .expect("lease");
        let environment: Vec<(OsString, OsString)> = values
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();

        let config = ExecRuntimeConfig {
            codex: script,
            bwrap: None,
            repository_root: repo.clone(),
            paths: ExecTaskPaths {
                home,
                codex_home,
                temp: tmp,
                runtime: runtime_dir.clone(),
            },
            auth_source: auth,
            cargo_bin_source: root.join("cargo-bin"),
            rustup_home_source: root.join("rustup"),
            environment,
            lease,
            artifact_root_path: artifacts.clone(),
            run_id: "run-1".into(),
            task_id: "task-1".into(),
        };
        let runtime = ExecTaskWorkerRuntime::open(config).expect("open runtime");
        Fixture {
            _temp: temp,
            capture,
            runtime_dir,
            artifacts,
            repo,
            runtime,
        }
    }

    fn session(fixture: &mut Fixture, role: WorkerRole) -> RuntimeSessionKey {
        let project = fixture.repo.clone();
        session_with_repository(fixture, role, project)
    }

    fn session_with_repository(
        fixture: &mut Fixture,
        role: WorkerRole,
        task_repository: PathBuf,
    ) -> RuntimeSessionKey {
        fixture
            .runtime
            .open_session(RoleSessionSpec {
                role,
                task_key: "task-1".into(),
                cwd: fixture.repo.clone(),
                task_repository,
                profile: RuntimeProfile::codex_exec_default(),
                developer_instructions: "You are the worker.".into(),
            })
            .expect("open session")
    }

    fn spec(
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        timeout: Duration,
        repo: &Path,
    ) -> RuntimeTurnSpec {
        RuntimeTurnSpec {
            role,
            task_key: "task-1".into(),
            purpose,
            cwd: repo.to_path_buf(),
            task_repository: repo.to_path_buf(),
            prompt: "do the task".into(),
            profile: RuntimeProfile::codex_exec_default(),
            outcome_contract: match role {
                WorkerRole::Developer => OutcomeContract::DeveloperV1,
                WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
            },
            timeout,
        }
    }

    fn poll_terminal(fixture: &mut Fixture, turn: RuntimeTurnKey) -> RuntimeTurnPoll {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let poll = fixture.runtime.poll_turn(turn).expect("poll");
            if poll.is_terminal() {
                return poll;
            }
            if Instant::now() > deadline {
                panic!("turn did not reach a terminal state in time");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn start(
        fixture: &mut Fixture,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
    ) -> (RuntimeSessionKey, RuntimeTurnKey) {
        let key = session(fixture, role);
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(key, spec(role, purpose, Duration::from_secs(30), &repo))
            .expect("start turn");
        (key, turn)
    }

    const HAPPY_DEVELOPER: &str = r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
echo '{"type":"turn.completed"}'
printf 'implemented the change' > "$OUT"
"#;

    #[test]
    fn happy_developer_turn_completes_and_captures_thread_id() {
        let mut fixture = fixture(HAPPY_DEVELOPER);
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, telemetry } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Developer(outcome) = outcome else {
            panic!("expected developer outcome");
        };
        assert_eq!(outcome.status, DeveloperOutcomeStatus::Ready);
        assert!(outcome.summary.contains("implemented the change"));
        assert!(telemetry.protocol_bytes > 0);
        // Raw target removed from the private runtime dir; sealed artifacts exist.
        let leftovers: Vec<_> = fs::read_dir(&fixture.runtime_dir).unwrap().collect();
        assert!(leftovers.is_empty(), "raw final target must be removed");
        let attempt_dir = fixture
            .artifacts
            .join("run-1/task-1/developer/session-1/turn-1/attempt-1");
        assert!(attempt_dir.join("prompt.md").is_file());
        assert!(attempt_dir.join("native.stdout.partial").is_file());
        assert!(attempt_dir.join("native-final.partial").is_file());
        // Create turn used --cd; instructions prepended to stdin prompt.
        let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
        assert!(args.contains("--cd"));
        assert!(args.contains("shell_environment_policy.inherit=\"all\""));
        assert!(args.contains("shell_environment_policy.ignore_default_excludes=true"));
        assert!(args.contains("--ignore-user-config"));
        let stdin = fs::read_to_string(fixture.capture.join("stdin.log")).unwrap();
        assert!(stdin.contains("You are the worker."));
        assert!(stdin.contains("do the task"));
    }

    #[test]
    fn nonzero_exit_with_valid_looking_message_never_routes() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
printf 'looks like a valid final message' > "$OUT"
exit 7
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Process);
        assert!(failure.detail.contains("status 7"), "{}", failure.detail);
    }

    #[test]
    fn empty_final_message_is_a_process_failure() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Process);
        assert!(
            failure.detail.contains("empty final message"),
            "{}",
            failure.detail
        );
    }

    #[test]
    fn missing_thread_started_is_a_protocol_failure() {
        let mut fixture = fixture(
            r#"
echo 'not-json-at-all'
printf 'work happened' > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Protocol);
        assert!(
            failure.detail.contains("thread.started"),
            "{}",
            failure.detail
        );
    }

    #[test]
    fn resume_reuses_the_thread_and_drops_cd() {
        let mut fixture = fixture(
            r#"
TID="thread-fake-1"
if [ -n "$RESUME" ]; then TID="$RESUME"; fi
printf '{"type":"thread.started","thread_id":"%s"}\n' "$TID"
printf 'turn done' > "$OUT"
"#,
        );
        let key = session(&mut fixture, WorkerRole::Developer);
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
        let invocations: Vec<&str> = args
            .split("===INVOCATION===")
            .filter(|s| !s.trim().is_empty())
            .collect();
        assert_eq!(invocations.len(), 2);
        assert!(invocations[0].contains("--cd"));
        assert!(
            !invocations[1].contains("--cd"),
            "resume must not carry --cd"
        );
        assert!(invocations[1].contains("resume\nthread-fake-1"));
        // Second turn skips the instructions preamble.
        let stdin = fs::read_to_string(fixture.capture.join("stdin.log")).unwrap();
        assert_eq!(stdin.matches("You are the worker.").count(), 1);
        // A resume carries no --cd, so Codex takes the *process* working
        // directory. Both invocations must therefore be launched from the
        // project directory, or the resumed turn would silently work
        // somewhere else.
        let cwds = fs::read_to_string(fixture.capture.join("cwd.log")).unwrap();
        let observed: Vec<&str> = cwds.lines().collect();
        assert_eq!(observed.len(), 2);
        assert!(
            observed.iter().all(|cwd| *cwd == repo.to_str().unwrap()),
            "every invocation must run from the project directory: {observed:?}"
        );
    }

    #[test]
    fn resume_with_a_different_thread_id_fails_closed() {
        let mut fixture = fixture(
            r#"
TID="thread-fake-1"
if [ -n "$RESUME" ]; then TID="thread-imposter"; fi
printf '{"type":"thread.started","thread_id":"%s"}\n' "$TID"
printf 'turn done' > "$OUT"
"#,
        );
        let key = session(&mut fixture, WorkerRole::Developer);
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Protocol);
        assert!(
            failure.detail.contains("different thread id"),
            "{}",
            failure.detail
        );
    }

    #[test]
    fn wall_clock_timeout_kills_the_process_group() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
sleep 30
printf 'too late' > "$OUT"
"#,
        );
        let key = session(&mut fixture, WorkerRole::Developer);
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    Duration::from_millis(300),
                    &repo,
                ),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(400));
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Timeout);
    }

    #[test]
    fn giant_single_line_event_after_thread_started_still_completes() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
head -c 4194304 /dev/zero | tr '\0' 'a'
echo
printf 'done after huge output' > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, telemetry } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        assert!(matches!(outcome, RuntimeOutcome::Developer(_)));
        assert!(telemetry.protocol_bytes > 4_000_000);
    }

    #[test]
    fn reviewer_lgtm_and_request_changes_classify() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
printf 'VERDICT: LGTM\nlooks solid' > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Reviewer(outcome) = outcome else {
            panic!("expected reviewer outcome");
        };
        assert_eq!(outcome.verdict, ReviewerVerdict::Lgtm);
        assert!(outcome.findings.is_empty());

        let mut fixture = fixture2(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
printf 'VERDICT: REQUEST_CHANGES\n- fix the frobnicator' > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Reviewer(outcome) = outcome else {
            panic!("expected reviewer outcome");
        };
        assert_eq!(outcome.verdict, ReviewerVerdict::RequestChanges);
        assert_eq!(outcome.findings.len(), 1);
        assert!(outcome.findings[0].message.contains("frobnicator"));
    }

    fn fixture2(body: &str) -> Fixture {
        fixture(body)
    }

    #[test]
    fn ambiguous_verdict_gets_one_clarification_then_succeeds() {
        let mut fixture = fixture(
            r#"
TID="thread-fake-1"
if [ -n "$RESUME" ]; then TID="$RESUME"; fi
printf '{"type":"thread.started","thread_id":"%s"}\n' "$TID"
if [ -f "$CAPTURE/first-done" ]; then
  printf 'VERDICT: LGTM\nafter clarification' > "$OUT"
else
  touch "$CAPTURE/first-done"
  printf 'I think it is probably fine overall' > "$OUT"
fi
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Reviewer(outcome) = outcome else {
            panic!("expected reviewer outcome");
        };
        assert_eq!(outcome.verdict, ReviewerVerdict::Lgtm);
        // Clarification was a resume of the same thread with the fixed re-ask
        // prompt, and both attempts kept their artifacts.
        let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
        let invocations: Vec<&str> = args
            .split("===INVOCATION===")
            .filter(|s| !s.trim().is_empty())
            .collect();
        assert_eq!(invocations.len(), 2);
        assert!(invocations[1].contains("resume\nthread-fake-1"));
        let stdin = fs::read_to_string(fixture.capture.join("stdin.log")).unwrap();
        assert!(stdin.contains("did not contain a usable"));
        let turn_dir = fixture
            .artifacts
            .join("run-1/task-1/reviewer/session-1/turn-1");
        assert!(turn_dir.join("attempt-1/native-final.partial").is_file());
        assert!(turn_dir.join("attempt-2/native-final.partial").is_file());
    }

    #[test]
    fn ambiguous_verdict_twice_fails_the_turn() {
        let mut fixture = fixture(
            r#"
TID="thread-fake-1"
if [ -n "$RESUME" ]; then TID="$RESUME"; fi
printf '{"type":"thread.started","thread_id":"%s"}\n' "$TID"
printf 'still no clear verdict here' > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Contract);
        assert!(
            failure.detail.contains("undetermined after clarification"),
            "{}",
            failure.detail
        );
    }

    #[test]
    fn secrets_never_reach_outcomes_or_sealed_artifacts() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
printf 'work done; leaked value: %s' "$FAKE_SECRET_TOKEN" > "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Developer(outcome) = outcome else {
            panic!("expected developer outcome");
        };
        assert!(
            !outcome.summary.contains(SECRET_VALUE),
            "summary leaked the secret"
        );
        assert!(outcome.summary.contains("work done"));
        let sealed = fs::read_to_string(
            fixture
                .artifacts
                .join("run-1/task-1/developer/session-1/turn-1/attempt-1/native-final.partial"),
        )
        .unwrap();
        assert!(
            !sealed.contains(SECRET_VALUE),
            "sealed artifact leaked the secret"
        );
    }

    #[test]
    fn external_task_repository_is_exposed_with_add_dir_for_the_developer() {
        let mut fixture = fixture(HAPPY_DEVELOPER);
        let external = fixture.repo.parent().unwrap().join("external-repo");
        fs::create_dir_all(&external).unwrap();
        let external = fs::canonicalize(&external).unwrap();

        let key = session_with_repository(&mut fixture, WorkerRole::Developer, external.clone());
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));

        let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
        // --add-dir belongs to the exec parent, ahead of --json, and the
        // native working root stays the project directory.
        let add_dir = args
            .find("--add-dir")
            .expect("developer must receive the external repository scope");
        assert!(add_dir < args.find("--json").unwrap());
        assert!(args.contains(external.to_str().unwrap()));
        assert!(args.contains(&format!(
            "--cd
{}",
            repo.display()
        )));
    }

    #[test]
    fn reviewer_never_receives_a_writable_repository_scope() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
printf 'VERDICT: LGTM\nfine' > "$OUT"
"#,
        );
        let external = fixture.repo.parent().unwrap().join("reviewer-external");
        fs::create_dir_all(&external).unwrap();
        let external = fs::canonicalize(&external).unwrap();
        let key = session_with_repository(&mut fixture, WorkerRole::Reviewer, external);
        let repo = fixture.repo.clone();
        let turn = fixture
            .runtime
            .start_turn(
                key,
                spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    Duration::from_secs(30),
                    &repo,
                ),
            )
            .unwrap();
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
        assert!(
            !args.contains("--add-dir"),
            "reviewer must not get a writable scope: {args}"
        );
    }

    #[test]
    fn background_descendant_holding_pipes_does_not_hang_or_orphan() {
        // The leader exits immediately but leaves a child holding stdout for
        // 30s. Without settling descendants the drain join would block for
        // the full sleep; the turn must instead finish promptly and report it.
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}
'
printf 'leader done' > "$OUT"
sleep 30 &
echo "$!" > "$CAPTURE/descendant.pid"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let started = Instant::now();
        let poll = poll_terminal(&mut fixture, turn);
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "settling descendants must not wait for the background sleep"
        );
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("a leftover descendant must be reported, got {poll:?}");
        };
        assert!(
            failure.detail.contains("background descendants"),
            "{}",
            failure.detail
        );
        // Deliberately no bare-pid liveness probe here: pids are recycled, so
        // `kill(pid, 0)` can succeed for an unrelated process. The two
        // assertions above are the real subject — the turn did not hang, and
        // the leftover descendant was reported rather than silently ignored.
    }

    #[test]
    fn a_long_final_message_keeps_its_tail_and_hides_a_straddling_secret() {
        // The final message is padded so the synthetic secret lands exactly on
        // the truncation boundary; the guard must drop the surviving prefix.
        let mut fixture = fixture(&format!(
            r#"
printf '{{"type":"thread.started","thread_id":"thread-fake-1"}}\n'
head -c {pad} /dev/zero | tr '\0' 'a' > "$OUT"
printf '%s' "$FAKE_SECRET_TOKEN" >> "$OUT"
head -c 4096 /dev/zero | tr '\0' 'b' >> "$OUT"
"#,
            pad = RAW_FINAL_CHUNK - 8
        ));
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Developer(outcome) = outcome else {
            panic!("expected developer outcome");
        };
        for fragment_len in [SECRET_VALUE.len(), SECRET_VALUE.len() / 2] {
            let fragment = &SECRET_VALUE[..fragment_len];
            assert!(
                !outcome.summary.contains(fragment),
                "summary leaked {fragment:?}"
            );
        }
        let sealed = fs::read_to_string(
            fixture
                .artifacts
                .join("run-1/task-1/developer/session-1/turn-1/attempt-1/native-final.partial"),
        )
        .unwrap();
        assert!(!sealed.contains(&SECRET_VALUE[..SECRET_VALUE.len() / 2]));
        // The message continues past the chunk boundary; the tail must survive
        // rather than being silently dropped.
        assert!(
            sealed.contains("bbbb"),
            "the tail after the secret was lost"
        );
    }

    #[test]
    fn the_prompt_artifact_stays_readable() {
        let mut fixture = fixture(HAPPY_DEVELOPER);
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        assert!(matches!(
            poll_terminal(&mut fixture, turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let prompt = fs::read_to_string(
            fixture
                .artifacts
                .join("run-1/task-1/developer/session-1/turn-1/attempt-1/prompt.md"),
        )
        .unwrap();
        // The prompt is the reproducibility record: it must survive as text.
        assert!(prompt.contains("You are the worker."), "{prompt}");
        assert!(prompt.contains("do the task"), "{prompt}");
        assert!(!prompt.contains("[REDACTED"), "{prompt}");
    }

    #[test]
    fn a_final_message_larger_than_the_old_cap_is_persisted_whole() {
        // 3 MiB: comfortably past the previous 1 MiB durable cap, and past
        // the relay window, so both behaviours are exercised at once.
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}
'
yes zzzzzzzzzzzzzzzz | head -c 3145728 > "$OUT"
printf 'TAILMARK' >> "$OUT"
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let poll = poll_terminal(&mut fixture, turn);
        let RuntimeTurnPoll::Completed { outcome, .. } = poll else {
            panic!("expected completion, got {poll:?}");
        };
        let RuntimeOutcome::Developer(outcome) = outcome else {
            panic!("expected developer outcome");
        };
        // Relayed onward: bounded, and the cut is explicitly marked.
        assert!(
            outcome.summary.len() <= 300 * 1024,
            "{}",
            outcome.summary.len()
        );
        assert!(
            outcome
                .summary
                .contains("[hcom: message truncated for relay")
        );
        // Persisted: the whole message, tail included.
        let sealed = fs::read_to_string(
            fixture
                .artifacts
                .join("run-1/task-1/developer/session-1/turn-1/attempt-1/native-final.partial"),
        )
        .unwrap();
        assert!(
            sealed.len() > 3 * 1024 * 1024,
            "sealed {} bytes",
            sealed.len()
        );
        assert!(sealed.ends_with("TAILMARK"), "the tail was dropped");
    }

    #[test]
    fn cancel_terminates_the_turn() {
        let mut fixture = fixture(
            r#"
printf '{"type":"thread.started","thread_id":"thread-fake-1"}\n'
sleep 30
"#,
        );
        let (_key, turn) = start(
            &mut fixture,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        std::thread::sleep(Duration::from_millis(100));
        fixture.runtime.cancel_turn(turn).unwrap();
        let poll = fixture.runtime.poll_turn(turn).unwrap();
        let RuntimeTurnPoll::Failed { failure, .. } = poll else {
            panic!("expected canceled failure, got {poll:?}");
        };
        assert_eq!(failure.class, RuntimeFailureClass::Canceled);
    }
}
