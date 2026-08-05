//! Direct-native Claude task worker runtime.
//!
//! One native `claude -p` process implements each turn. Claude's stream-json
//! stdout proves the preassigned UUID and carries one terminal logical final;
//! the per-invocation Guardian independently proves lifecycle cleanup before
//! that final can be sealed and routed.

use super::developer_status::classify_developer_status;
use super::environment::{ExecutionEnvironmentLease, ParentEnvironment, SecretRedactor};
use super::guardian::{
    GuardedCommand, GuardianCleanupDisposition, GuardianCleanupReason, GuardianCleanupRegistry,
    GuardianCompletion, GuardianHandle, GuardianHandleFailure, GuardianMode, GuardianPoll,
    GuardianSpawnFailure,
};
use super::runtime::{
    DeveloperOutcomeV1, ReviewerOutcomeV1, ReviewerVerdict, RoleSessionSpec,
    RuntimeContractIdentity, RuntimeError, RuntimeFailureClass, RuntimeOutcome, RuntimeProvider,
    RuntimeSessionKey, RuntimeTelemetry, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnSpec,
    SanitizedRuntimeFailure, TaskWorkerRuntime,
};
use super::verdict::{Verdict, VerdictClassification, classify_verdict};
use crate::artifact::{
    ArtifactAttempt, ArtifactKind, ArtifactRoot, ArtifactScope, MAX_NATIVE_ARTIFACT_BYTES,
};
use crate::control_api::WorkerRole;
use crate::worker::profile::ReviewerId;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use uuid::Uuid;

const PIPE_CHUNK: usize = 32 * 1024;
const STDERR_TAIL_BYTES: usize = 320;
const INLINE_CLEANUP_BUDGET: Duration = Duration::from_secs(3);

pub struct ClaudeExecRuntimeConfig {
    /// Production uses the bare name `claude`; tests may use another bare name
    /// selected from the frozen inherited PATH.
    pub claude: OsString,
    /// Exact running hcom executable used only for the private same-binary
    /// Guardian entry. This does not pin the native Claude executable.
    pub guardian_executable: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
    pub lease: ExecutionEnvironmentLease,
    pub artifact_root_path: PathBuf,
    pub run_id: String,
    pub task_id: String,
    pub reviewer_id: Option<ReviewerId>,
    pub cleanup_registry: GuardianCleanupRegistry,
}

pub fn claude_exec_contract_identity() -> RuntimeContractIdentity {
    RuntimeContractIdentity::claude_exec()
}

struct ClaudeSession {
    role: WorkerRole,
    cwd: PathBuf,
    task_repository: PathBuf,
    profile: super::runtime::RuntimeProfile,
    instructions: String,
    label: String,
    native_session_id: String,
    turn_sequence: u32,
}

enum TurnState {
    Running(Box<RunningTurn>),
    Done(RuntimeTurnPoll),
}

struct ClaudeTurn {
    session: RuntimeSessionKey,
    state: TurnState,
}

type StderrDrained = (u64, Vec<u8>, Option<String>);

struct ParsedClaudeStream {
    final_message: String,
    event_count: u32,
}

struct StdoutDrained {
    parsed: Result<ParsedClaudeStream, String>,
    bytes: u64,
    io_error: Option<String>,
}

struct RunningTurn {
    guardian: Option<GuardianHandle>,
    started: Instant,
    spec: RuntimeTurnSpec,
    attempt: ArtifactAttempt,
    redactor: SecretRedactor,
    stdout_thread: Option<JoinHandle<StdoutDrained>>,
    stderr_thread: Option<JoinHandle<StderrDrained>>,
    stdin_thread: Option<JoinHandle<Option<String>>>,
    cleanup_registry: GuardianCleanupRegistry,
    clarification_used: bool,
    preceding_final_message_path: Option<PathBuf>,
    attempt_no: u32,
}

pub struct ClaudeExecTaskWorkerRuntime {
    contract: RuntimeContractIdentity,
    config: ClaudeExecRuntimeConfig,
    artifact_root: ArtifactRoot,
    sessions: BTreeMap<RuntimeSessionKey, ClaudeSession>,
    turns: BTreeMap<RuntimeTurnKey, ClaudeTurn>,
    next_session: u64,
    next_turn: u64,
    active_turn: Option<RuntimeTurnKey>,
    shut_down: bool,
}

impl ClaudeExecTaskWorkerRuntime {
    pub fn open(config: ClaudeExecRuntimeConfig) -> Result<Self, RuntimeError> {
        validate_claude_environment(&config.environment)?;
        if !config.guardian_executable.is_absolute() {
            return Err(RuntimeError::invalid_contract(
                "Claude Guardian executable must be absolute",
            ));
        }
        let artifact_root = ArtifactRoot::open(&config.artifact_root_path)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        Ok(Self {
            contract: claude_exec_contract_identity(),
            config,
            artifact_root,
            sessions: BTreeMap::new(),
            turns: BTreeMap::new(),
            next_session: 0,
            next_turn: 0,
            active_turn: None,
            shut_down: false,
        })
    }

    fn build_native_argv(
        session: &ClaudeSession,
        spec: &RuntimeTurnSpec,
        resume: bool,
    ) -> Vec<OsString> {
        let mut argv: Vec<OsString> = vec![
            "-p".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--verbose".into(),
            "--model".into(),
            spec.profile.model.as_str().into(),
            "--effort".into(),
            spec.profile.reasoning_effort.as_str().into(),
            "--name".into(),
            role_name(session.role).into(),
            "--prompt-suggestions".into(),
            "false".into(),
        ];
        if session.task_repository != session.cwd {
            argv.push("--add-dir".into());
            argv.push(session.task_repository.as_os_str().to_owned());
        }
        if resume {
            argv.push("--resume".into());
        } else {
            argv.push("--session-id".into());
        }
        argv.push(session.native_session_id.as_str().into());
        if spec
            .profile
            .claude_permissions
            .as_ref()
            .is_some_and(|permissions| permissions.dangerously_skip_permissions)
        {
            argv.push("--dangerously-skip-permissions".into());
        }
        argv
    }

    fn spawn_invocation(
        &mut self,
        session_key: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
        attempt_no: u32,
        prompt_override: Option<String>,
        clarification_used: bool,
        preceding_final_message_path: Option<PathBuf>,
    ) -> Result<RunningTurn, RuntimeError> {
        validate_claude_environment(&self.config.environment)?;
        let session = self
            .sessions
            .get(&session_key)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime session"))?;
        if spec.cwd != session.cwd
            || spec.task_repository != session.task_repository
            || spec.profile != session.profile
        {
            return Err(RuntimeError::invalid_identity(
                "Claude turn differs from its frozen session cwd, repository, or profile",
            ));
        }
        let resume = session.turn_sequence > 1 || attempt_no > 1;
        let prompt_body = prompt_override.unwrap_or_else(|| spec.prompt.clone());
        let full_prompt = if !resume && !session.instructions.is_empty() {
            format!("{}\n\n{}", session.instructions, prompt_body)
        } else {
            prompt_body
        };
        let scope = ArtifactScope {
            run_id: self.config.run_id.clone(),
            task_id: self.config.task_id.clone(),
            role: session.role,
            reviewer_id: (session.role == WorkerRole::Reviewer)
                .then_some(self.config.reviewer_id.unwrap_or(ReviewerId::Reviewer1)),
            logical_session_id: session.label.clone(),
            turn_sequence: session.turn_sequence,
            attempt: attempt_no,
        };
        let attempt = ArtifactAttempt::create_with_environment_secrets_only(
            &self.artifact_root,
            scope,
            &self.config.lease,
            utf8_prefix(&full_prompt, 200 * 1024).as_bytes(),
        )
        .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let mut prompt_writer = attempt
            .start_native_stream(ArtifactKind::NativePrompt, MAX_NATIVE_ARTIFACT_BYTES)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        prompt_writer
            .write_chunk(full_prompt.as_bytes())
            .and_then(|_| prompt_writer.finish().map(|_| ()))
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let stdout_writer = attempt
            .start_native_stream(ArtifactKind::NativeStdout, MAX_NATIVE_ARTIFACT_BYTES)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        let stderr_writer = attempt
            .start_native_stream(ArtifactKind::NativeStderr, MAX_NATIVE_ARTIFACT_BYTES)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;

        let native_argv = Self::build_native_argv(session, &spec, resume);
        let expected_session = session.native_session_id.clone();
        let mut command = GuardedCommand::with_guardian_executable(
            &self.config.guardian_executable,
            self.config.claude.clone(),
        )
        .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        command
            .args(native_argv)
            .mode(GuardianMode::HeadlessWorker)
            .current_dir(&session.cwd)
            .env_clear()
            .envs(self.config.environment.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .require_claude_proxy();
        let mut guardian = match command.spawn() {
            Ok(handle) => handle,
            Err(failure) => return Err(self.guardian_spawn_failure(failure)),
        };
        let stdin = guardian.take_stdin();
        let stdout = guardian.take_stdout();
        let stderr = guardian.take_stderr();
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
        let stdout_thread = stdout.map(|pipe| {
            std::thread::spawn(move || drain_stdout(pipe, stdout_writer, expected_session))
        });
        let stderr_thread =
            stderr.map(|pipe| std::thread::spawn(move || drain_stderr(pipe, stderr_writer)));
        Ok(RunningTurn {
            guardian: Some(guardian),
            started: Instant::now(),
            spec,
            attempt,
            redactor: self.config.lease.redactor(),
            stdout_thread,
            stderr_thread,
            stdin_thread,
            cleanup_registry: self.config.cleanup_registry.clone(),
            clarification_used,
            preceding_final_message_path,
            attempt_no,
        })
    }

    fn guardian_spawn_failure(&self, failure: GuardianSpawnFailure) -> RuntimeError {
        match failure {
            GuardianSpawnFailure::Reaped(error) => RuntimeError::internal(single_line(&format!(
                "failed to launch bare Claude executable through Guardian: {error}"
            ))),
            GuardianSpawnFailure::CleanupPending { detail, handle } => {
                match self.config.cleanup_registry.register(*handle) {
                    Ok(_) => RuntimeError::internal(single_line(&format!(
                        "Claude Guardian launch failed and exact cleanup ownership was transferred: {detail}"
                    ))),
                    Err(error) => {
                        self.config.cleanup_registry.record_ownership_lost(&format!(
                            "Claude Guardian launch cleanup transfer failed: {error}"
                        ));
                        RuntimeError::internal(
                            "Claude Guardian launch failed and cleanup ownership could not be transferred",
                        )
                    }
                }
            }
            GuardianSpawnFailure::OwnershipLost(detail) => {
                self.config.cleanup_registry.record_ownership_lost(&detail);
                RuntimeError::internal(single_line(&format!(
                    "Claude Guardian lost lifecycle ownership during launch: {detail}"
                )))
            }
        }
    }

    fn finalize(
        &mut self,
        turn_key: RuntimeTurnKey,
        completion: GuardianCompletion,
        mut running: Box<RunningTurn>,
    ) -> Result<RuntimeTurnPoll, RuntimeError> {
        let (stdout, stderr_bytes, stderr_tail, transport_error) = running.join_drains();
        let telemetry = RuntimeTelemetry {
            protocol_bytes: stdout.bytes,
            stderr_bytes,
            notification_count: stdout
                .parsed
                .as_ref()
                .map_or(0, |parsed| parsed.event_count),
        };
        if let Some(error) = transport_error.or(stdout.io_error) {
            return fail(RuntimeFailureClass::Process, single_line(&error), telemetry);
        }
        if completion.native_code != Some(0) || completion.native_signal.is_some() {
            return fail(
                RuntimeFailureClass::Process,
                exit_failure_detail(&completion, &stderr_tail, &running.redactor),
                telemetry,
            );
        }
        if completion.disposition != GuardianCleanupDisposition::Clean
            || completion.forced_signal_count != 0
        {
            return fail(
                RuntimeFailureClass::Process,
                lifecycle_failure_detail(&completion),
                telemetry,
            );
        }
        let parsed = stdout.parsed.map_err(|detail| {
            RuntimeError::invalid_outcome(single_line(&format!(
                "Claude stream-json protocol failed: {detail}"
            )))
        });
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                return fail(RuntimeFailureClass::Protocol, error.detail, telemetry);
            }
        };
        let final_message_path = running.attempt.artifact_path(ArtifactKind::NativeFinal);
        let mut final_writer = running
            .attempt
            .start_exact_native_final(MAX_NATIVE_ARTIFACT_BYTES)
            .map_err(|error| RuntimeError::internal(single_line(&error.to_string())))?;
        if let Err(error) = final_writer
            .write_chunk(parsed.final_message.as_bytes())
            .and_then(|_| final_writer.finish().map(|_| ()))
        {
            return fail(
                RuntimeFailureClass::Protocol,
                single_line(&format!("Claude final message seal failed: {error}")),
                telemetry,
            );
        }

        let session_key = self
            .turns
            .get(&turn_key)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime turn"))?
            .session;
        let session = self
            .sessions
            .get(&session_key)
            .ok_or_else(|| RuntimeError::invalid_identity("Claude session disappeared"))?;
        let first_line = parsed
            .final_message
            .split_once('\n')
            .map_or(parsed.final_message.as_str(), |(line, _)| line);
        let outcome = match session.role {
            WorkerRole::Developer => RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: classify_developer_status(first_line),
            }),
            WorkerRole::Reviewer => match classify_verdict(first_line) {
                VerdictClassification::Determined(Verdict::Lgtm) => {
                    RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                        verdict: ReviewerVerdict::Lgtm,
                        preceding_final_message_paths: running
                            .preceding_final_message_path
                            .clone()
                            .into_iter()
                            .collect(),
                    })
                }
                VerdictClassification::Determined(Verdict::RequestChanges) => {
                    RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                        verdict: ReviewerVerdict::RequestChanges,
                        preceding_final_message_paths: running
                            .preceding_final_message_path
                            .clone()
                            .into_iter()
                            .collect(),
                    })
                }
                VerdictClassification::Undetermined(reason) => {
                    if running.clarification_used {
                        return fail(
                            RuntimeFailureClass::Contract,
                            single_line(&format!(
                                "reviewer verdict undetermined after clarification ({reason:?})"
                            )),
                            telemetry,
                        );
                    }
                    let spec = running.spec.clone();
                    let next_attempt = running.attempt_no + 1;
                    let clarification = self.spawn_invocation(
                        session_key,
                        spec,
                        next_attempt,
                        Some(verdict_clarification_prompt(&final_message_path)),
                        true,
                        Some(final_message_path),
                    )?;
                    let turn = self.turns.get_mut(&turn_key).ok_or_else(|| {
                        RuntimeError::invalid_identity("unknown Claude runtime turn")
                    })?;
                    turn.state = TurnState::Running(Box::new(clarification));
                    return Ok(RuntimeTurnPoll::Pending { telemetry });
                }
            },
        };
        outcome.validate()?;
        Ok(RuntimeTurnPoll::Completed {
            outcome,
            final_message_path,
            telemetry,
        })
    }
}

impl RunningTurn {
    fn join_drains(&mut self) -> (StdoutDrained, u64, Vec<u8>, Option<String>) {
        let prompt_error = self
            .stdin_thread
            .take()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Some("prompt delivery thread panicked".into()))
            })
            .unwrap_or(None);
        let stdout = self
            .stdout_thread
            .take()
            .map(|handle| {
                handle.join().unwrap_or(StdoutDrained {
                    parsed: Err("stdout drain thread panicked".into()),
                    bytes: 0,
                    io_error: Some("stdout drain thread panicked".into()),
                })
            })
            .unwrap_or(StdoutDrained {
                parsed: Err("Claude stdout pipe was unavailable".into()),
                bytes: 0,
                io_error: None,
            });
        let (stderr_bytes, stderr_tail, stderr_error) = self
            .stderr_thread
            .take()
            .map(|handle| {
                handle.join().unwrap_or((
                    0,
                    Vec::new(),
                    Some("stderr drain thread panicked".into()),
                ))
            })
            .unwrap_or((0, Vec::new(), None));
        (
            stdout,
            stderr_bytes,
            stderr_tail,
            prompt_error.or(stderr_error),
        )
    }

    fn transfer_cleanup(&mut self, detail: &str) -> Result<(), RuntimeError> {
        let handle = self
            .guardian
            .take()
            .ok_or_else(|| RuntimeError::internal("Claude Guardian handle disappeared"))?;
        self.cleanup_registry.register(handle).map_err(|error| {
            self.cleanup_registry.record_ownership_lost(&format!(
                "Claude Guardian cleanup transfer failed: {error}"
            ));
            RuntimeError::internal(single_line(&format!(
                "{detail}; exact cleanup ownership transfer failed"
            )))
        })?;
        Ok(())
    }
}

impl Drop for RunningTurn {
    fn drop(&mut self) {
        let Some(mut guardian) = self.guardian.take() else {
            return;
        };
        match guardian
            .terminate_and_reap(GuardianCleanupReason::NormalTeardown, INLINE_CLEANUP_BUDGET)
        {
            Ok(_) => {
                self.guardian = None;
                let _ = self.join_drains();
            }
            Err(GuardianHandleFailure::CleanupPending(_)) => {
                if let Err(error) = self.cleanup_registry.register(guardian) {
                    self.cleanup_registry.record_ownership_lost(&format!(
                        "Claude Guardian drop cleanup transfer failed: {error}"
                    ));
                }
            }
            Err(GuardianHandleFailure::OwnershipLost(detail)) => {
                self.cleanup_registry.record_ownership_lost(&detail);
            }
        }
    }
}

impl TaskWorkerRuntime for ClaudeExecTaskWorkerRuntime {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError> {
        if self.shut_down {
            return Err(RuntimeError::invalid_transition(
                "Claude runtime is shut down",
            ));
        }
        spec.validate()?;
        if spec.profile.provider != RuntimeProvider::ClaudeExec {
            return Err(RuntimeError::invalid_profile(
                "Claude session requires a claude-exec runtime profile",
            ));
        }
        self.next_session = self
            .next_session
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("Claude session key overflow"))?;
        let key = RuntimeSessionKey::from_counter(self.next_session)?;
        self.sessions.insert(
            key,
            ClaudeSession {
                role: spec.role,
                cwd: spec.cwd,
                task_repository: spec.task_repository,
                profile: spec.profile,
                instructions: spec.developer_instructions,
                label: format!("session-{}", self.next_session),
                native_session_id: Uuid::new_v4().hyphenated().to_string(),
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
            return Err(RuntimeError::invalid_transition(
                "Claude runtime is shut down",
            ));
        }
        spec.validate()?;
        if spec.profile.provider != RuntimeProvider::ClaudeExec {
            return Err(RuntimeError::invalid_profile(
                "Claude turn requires a claude-exec runtime profile",
            ));
        }
        if self.active_turn.is_some() {
            return Err(RuntimeError::invalid_transition(
                "another Claude turn is still active",
            ));
        }
        {
            let entry = self
                .sessions
                .get_mut(&session)
                .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime session"))?;
            if entry.role != spec.role {
                return Err(RuntimeError::invalid_identity(
                    "Claude turn role does not match its session role",
                ));
            }
            entry.turn_sequence = entry
                .turn_sequence
                .checked_add(1)
                .ok_or_else(|| RuntimeError::internal("Claude turn sequence overflow"))?;
        }
        let running = self.spawn_invocation(session, spec, 1, None, false, None)?;
        self.next_turn = self
            .next_turn
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("Claude turn key overflow"))?;
        let key = RuntimeTurnKey::from_counter(self.next_turn)?;
        self.turns.insert(
            key,
            ClaudeTurn {
                session,
                state: TurnState::Running(Box::new(running)),
            },
        );
        self.active_turn = Some(key);
        Ok(key)
    }

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
        enum Observed {
            Complete(GuardianCompletion),
            CleanupPending(String),
            TimeoutComplete,
            TimeoutPending(String),
            OwnershipLost(String),
        }
        let observed = {
            let entry = self
                .turns
                .get_mut(&turn)
                .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime turn"))?;
            match &mut entry.state {
                TurnState::Done(poll) => return Ok(poll.clone()),
                TurnState::Running(running) => {
                    let guardian = running.guardian.as_mut().ok_or_else(|| {
                        RuntimeError::internal("active Claude Guardian handle disappeared")
                    })?;
                    if running.started.elapsed() >= running.spec.timeout {
                        match guardian.terminate_and_reap(
                            GuardianCleanupReason::Timeout,
                            INLINE_CLEANUP_BUDGET,
                        ) {
                            Ok(_) => Some(Observed::TimeoutComplete),
                            Err(GuardianHandleFailure::CleanupPending(detail)) => {
                                Some(Observed::TimeoutPending(detail))
                            }
                            Err(GuardianHandleFailure::OwnershipLost(detail)) => {
                                Some(Observed::OwnershipLost(detail))
                            }
                        }
                    } else {
                        match guardian.try_wait() {
                            GuardianPoll::Complete(completion) => {
                                Some(Observed::Complete(completion))
                            }
                            GuardianPoll::CleanupPending => {
                                match guardian.terminate_and_reap(
                                    GuardianCleanupReason::NormalTeardown,
                                    INLINE_CLEANUP_BUDGET,
                                ) {
                                    Ok(completion) => Some(Observed::Complete(completion)),
                                    Err(GuardianHandleFailure::CleanupPending(detail)) => {
                                        Some(Observed::CleanupPending(detail))
                                    }
                                    Err(GuardianHandleFailure::OwnershipLost(detail)) => {
                                        Some(Observed::OwnershipLost(detail))
                                    }
                                }
                            }
                            GuardianPoll::OwnershipLost(detail) => {
                                Some(Observed::OwnershipLost(detail))
                            }
                            GuardianPoll::Running => None,
                        }
                    }
                }
            }
        };
        let Some(observed) = observed else {
            return Ok(RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            });
        };
        let entry = self
            .turns
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime turn"))?;
        let mut running = match std::mem::replace(
            &mut entry.state,
            TurnState::Done(RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            }),
        ) {
            TurnState::Running(running) => running,
            TurnState::Done(_) => unreachable!("running state checked above"),
        };
        let poll = match observed {
            Observed::Complete(completion) => self.finalize(turn, completion, running)?,
            Observed::CleanupPending(detail) => {
                running.transfer_cleanup(&detail)?;
                RuntimeTurnPoll::Failed {
                    failure: SanitizedRuntimeFailure::new(
                        RuntimeFailureClass::Process,
                        "Claude Guardian cleanup remains pending under foreground ownership",
                        false,
                    )?,
                    telemetry: RuntimeTelemetry::default(),
                }
            }
            Observed::TimeoutComplete => {
                let _ = running.join_drains();
                RuntimeTurnPoll::Failed {
                    failure: SanitizedRuntimeFailure::new(
                        RuntimeFailureClass::Timeout,
                        format!(
                            "Claude turn exceeded its {}s wall-clock limit",
                            running.spec.timeout.as_secs()
                        ),
                        false,
                    )?,
                    telemetry: RuntimeTelemetry::default(),
                }
            }
            Observed::TimeoutPending(detail) => {
                running.transfer_cleanup(&detail)?;
                RuntimeTurnPoll::Failed {
                    failure: SanitizedRuntimeFailure::new(
                        RuntimeFailureClass::Timeout,
                        "Claude turn timed out; Guardian cleanup is pending under foreground ownership",
                        false,
                    )?,
                    telemetry: RuntimeTelemetry::default(),
                }
            }
            Observed::OwnershipLost(detail) => {
                self.config.cleanup_registry.record_ownership_lost(&detail);
                running.guardian.take();
                RuntimeTurnPoll::Failed {
                    failure: SanitizedRuntimeFailure::new(
                        RuntimeFailureClass::Process,
                        "Claude Guardian lost exact lifecycle ownership",
                        false,
                    )?,
                    telemetry: RuntimeTelemetry::default(),
                }
            }
        };
        let entry = self
            .turns
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime turn"))?;
        if matches!(poll, RuntimeTurnPoll::Pending { .. }) {
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
            .ok_or_else(|| RuntimeError::invalid_identity("unknown Claude runtime turn"))?;
        if matches!(entry.state, TurnState::Running(_)) {
            let placeholder = TurnState::Done(RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            });
            let TurnState::Running(mut running) = std::mem::replace(&mut entry.state, placeholder)
            else {
                unreachable!("running state checked above");
            };
            let guardian = running.guardian.as_mut().ok_or_else(|| {
                RuntimeError::internal("active Claude Guardian handle disappeared")
            })?;
            let result =
                guardian.terminate_and_reap(GuardianCleanupReason::Cancel, INLINE_CLEANUP_BUDGET);
            let detail = match result {
                Ok(_) => {
                    let _ = running.join_drains();
                    "Claude turn canceled by the supervisor".to_string()
                }
                Err(GuardianHandleFailure::CleanupPending(detail)) => {
                    running.transfer_cleanup(&detail)?;
                    "Claude turn canceled; Guardian cleanup is pending under foreground ownership"
                        .to_string()
                }
                Err(GuardianHandleFailure::OwnershipLost(detail)) => {
                    self.config.cleanup_registry.record_ownership_lost(&detail);
                    running.guardian.take();
                    "Claude turn canceled after Guardian lifecycle ownership was lost".to_string()
                }
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
        let result = match self.active_turn {
            Some(turn) => self.cancel_turn(turn),
            None => Ok(()),
        };
        self.shut_down = true;
        result
    }
}

impl Drop for ClaudeExecTaskWorkerRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn validate_claude_environment(entries: &[(OsString, OsString)]) -> Result<(), RuntimeError> {
    let environment = ParentEnvironment::from_raw_entries(entries.iter().cloned())
        .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))?;
    environment
        .validate_claude_role()
        .map_err(|error| RuntimeError::invalid_contract(single_line(&error.to_string())))
}

#[derive(Deserialize)]
struct ClaudeEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    subtype: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    is_error: Option<bool>,
    #[serde(default)]
    result: Option<String>,
}

struct ClaudeStreamParser {
    expected_session: String,
    line_cap: usize,
    pending: Vec<u8>,
    init_seen: bool,
    final_message: Option<String>,
    event_count: u32,
}

impl ClaudeStreamParser {
    fn new(expected_session: String, line_cap: usize) -> Self {
        Self {
            expected_session,
            line_cap,
            pending: Vec::new(),
            init_seen: false,
            final_message: None,
            event_count: 0,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.pending.extend_from_slice(bytes);
        loop {
            let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') else {
                if self.pending.len() > self.line_cap {
                    return Err("Claude stream-json event exceeds its bound".into());
                }
                return Ok(());
            };
            if newline > self.line_cap {
                return Err("Claude stream-json event exceeds its bound".into());
            }
            let mut line: Vec<u8> = self.pending.drain(..=newline).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.accept_line(&line)?;
        }
    }

    fn accept_line(&mut self, line: &[u8]) -> Result<(), String> {
        if line.is_empty() {
            return Err("Claude stream-json contained an empty event".into());
        }
        if self.final_message.is_some() {
            return Err("Claude stream-json emitted data after its terminal result".into());
        }
        let event: ClaudeEvent = serde_json::from_slice(line)
            .map_err(|_| "Claude stream-json event is malformed or invalid UTF-8".to_string())?;
        self.event_count = self
            .event_count
            .checked_add(1)
            .ok_or_else(|| "Claude stream-json event count overflow".to_string())?;
        if event.kind == "system" && event.subtype.as_deref() == Some("init") {
            if self.init_seen {
                return Err("Claude stream-json duplicated system init".into());
            }
            if event.session_id.as_deref() != Some(self.expected_session.as_str()) {
                return Err("Claude stream-json init returned a different session id".into());
            }
            self.init_seen = true;
            return Ok(());
        }
        if event.kind == "result" {
            if !self.init_seen {
                return Err("Claude stream-json result preceded system init".into());
            }
            if event.subtype.as_deref() != Some("success") || event.is_error != Some(false) {
                return Err("Claude stream-json terminal result was not successful".into());
            }
            if event.session_id.as_deref() != Some(self.expected_session.as_str()) {
                return Err("Claude stream-json result returned a different session id".into());
            }
            let final_message = event.result.ok_or_else(|| {
                "Claude stream-json result omitted logical final text".to_string()
            })?;
            if final_message.len() as u64 > MAX_NATIVE_ARTIFACT_BYTES {
                return Err("Claude logical final exceeds the native artifact bound".into());
            }
            self.final_message = Some(final_message);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ParsedClaudeStream, String> {
        if !self.pending.is_empty() {
            if self.pending.len() > self.line_cap {
                return Err("Claude stream-json event exceeds its bound".into());
            }
            let line = std::mem::take(&mut self.pending);
            self.accept_line(&line)?;
        }
        if !self.init_seen {
            return Err("Claude stream-json omitted system init".into());
        }
        let final_message = self
            .final_message
            .ok_or_else(|| "Claude stream-json omitted a terminal result".to_string())?;
        Ok(ParsedClaudeStream {
            final_message,
            event_count: self.event_count,
        })
    }
}

fn drain_stdout(
    mut pipe: impl Read,
    mut writer: crate::artifact::BoundedArtifactWriter,
    expected_session: String,
) -> StdoutDrained {
    let mut parser = Some(ClaudeStreamParser::new(
        expected_session,
        MAX_NATIVE_ARTIFACT_BYTES as usize,
    ));
    let mut parse_error = None;
    let mut bytes = 0_u64;
    let mut io_error = None;
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
        bytes = bytes.saturating_add(read as u64);
        if let Err(error) = writer.write_chunk(chunk) {
            io_error.get_or_insert(format!("stdout evidence write failed: {error}"));
        }
        if parse_error.is_none()
            && let Some(active) = parser.as_mut()
            && let Err(error) = active.push(chunk)
        {
            parse_error = Some(error);
            parser = None;
        }
    }
    if let Err(error) = writer.finish() {
        io_error.get_or_insert(format!("stdout evidence seal failed: {error}"));
    }
    let parsed = match (parse_error, parser) {
        (Some(error), _) => Err(error),
        (None, Some(parser)) => parser.finish(),
        (None, None) => Err("Claude stream-json parser disappeared".into()),
    };
    StdoutDrained {
        parsed,
        bytes,
        io_error,
    }
}

fn drain_stderr(
    mut pipe: impl Read,
    mut writer: crate::artifact::BoundedArtifactWriter,
) -> StderrDrained {
    let mut total = 0_u64;
    let mut tail = Vec::new();
    let mut io_error = None;
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
        total = total.saturating_add(read as u64);
        if let Err(error) = writer.write_chunk(chunk) {
            io_error.get_or_insert(format!("stderr evidence write failed: {error}"));
        }
        tail.extend_from_slice(chunk);
        if tail.len() > STDERR_TAIL_BYTES {
            tail.drain(..tail.len() - STDERR_TAIL_BYTES);
        }
    }
    if let Err(error) = writer.finish() {
        io_error.get_or_insert(format!("stderr evidence seal failed: {error}"));
    }
    (total, tail, io_error)
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

fn role_name(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => "hcom-task-developer",
        WorkerRole::Reviewer => "hcom-task-reviewer",
    }
}

fn lifecycle_failure_detail(completion: &GuardianCompletion) -> String {
    match completion.disposition {
        GuardianCleanupDisposition::OrphanedDescendants => {
            "Claude exited successfully but Guardian cleaned residual descendants".into()
        }
        _ => format!(
            "Claude Guardian completed with non-clean lifecycle disposition {:?}",
            completion.disposition
        ),
    }
}

fn exit_failure_detail(
    completion: &GuardianCompletion,
    stderr_tail: &[u8],
    redactor: &SecretRedactor,
) -> String {
    let cause = match (completion.native_code, completion.native_signal) {
        (Some(code), _) => format!("claude -p exited with status {code}"),
        (None, Some(signal)) => format!("claude -p was killed by signal {signal}"),
        _ => "claude -p ended without an exact status".to_string(),
    };
    let tail = redactor.redact(&String::from_utf8_lossy(stderr_tail));
    let tail: String = tail
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    let tail = tail.trim();
    if tail.is_empty() {
        cause
    } else {
        single_line(&format!("{cause}; stderr tail: {tail}"))
    }
}

fn verdict_clarification_prompt(previous_final: &Path) -> String {
    format!(
        "Your previous final message is stored at:\n{}\n\nRead that file. Its first line did not \
         contain a usable verdict. Reply once more: the FIRST line of your final message must be \
         exactly `VERDICT: LGTM` or `VERDICT: REQUEST_CHANGES` (a single line, no decoration, no \
         other text on that line). Do not repeat the previous findings; they remain in that file.",
        previous_final.display()
    )
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
    let mut output: String = text
        .chars()
        .map(|character| {
            if matches!(character, '\n' | '\r') {
                ' '
            } else {
                character
            }
        })
        .collect();
    if output.len() > 900 {
        let mut end = 900;
        while end > 0 && !output.is_char_boundary(end) {
            end -= 1;
        }
        output.truncate(end);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: &str = "11111111-2222-4333-8444-555555555555";

    fn event(value: serde_json::Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn stream_parser_requires_exact_init_and_single_terminal_result() {
        let mut parser = ClaudeStreamParser::new(SESSION.into(), 4096);
        parser
            .push(&event(serde_json::json!({
                "type":"system", "subtype":"init", "session_id":SESSION
            })))
            .unwrap();
        parser
            .push(&event(serde_json::json!({
                "type":"assistant", "message":{"content":[]}
            })))
            .unwrap();
        parser
            .push(&event(serde_json::json!({
                "type":"result", "subtype":"success", "is_error":false,
                "session_id":SESSION, "result":"STATUS: READY\n原样 final"
            })))
            .unwrap();
        let parsed = parser.finish().unwrap();
        assert_eq!(parsed.final_message, "STATUS: READY\n原样 final");
        assert_eq!(parsed.event_count, 3);
    }

    #[test]
    fn stream_parser_fails_closed_on_identity_shape_and_bounds() {
        let init = event(serde_json::json!({
            "type":"system", "subtype":"init", "session_id":SESSION
        }));
        let result = event(serde_json::json!({
            "type":"result", "subtype":"success", "is_error":false,
            "session_id":SESSION, "result":"done"
        }));

        let mut missing_init = ClaudeStreamParser::new(SESSION.into(), 4096);
        assert!(missing_init.push(&result).is_err());

        let mut duplicate = ClaudeStreamParser::new(SESSION.into(), 4096);
        duplicate.push(&init).unwrap();
        duplicate.push(&result).unwrap();
        assert!(duplicate.push(&result).is_err());

        let mut wrong = ClaudeStreamParser::new(SESSION.into(), 4096);
        assert!(
            wrong
                .push(&event(serde_json::json!({
                    "type":"system", "subtype":"init",
                    "session_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
                })))
                .is_err()
        );

        let mut invalid = ClaudeStreamParser::new(SESSION.into(), 4096);
        assert!(invalid.push(b"{not-json}\n").is_err());

        let mut oversized = ClaudeStreamParser::new(SESSION.into(), 16);
        assert!(oversized.push(&[b'x'; 17]).is_err());

        let mut empty = ClaudeStreamParser::new(SESSION.into(), 4096);
        empty.push(&init).unwrap();
        empty
            .push(&event(serde_json::json!({
                "type":"result", "subtype":"success", "is_error":false,
                "session_id":SESSION, "result":""
            })))
            .unwrap();
        assert_eq!(empty.finish().unwrap().final_message, "");
    }
}
