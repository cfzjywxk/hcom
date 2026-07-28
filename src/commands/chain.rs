//! `hcom chain` — the one public foreground Codex-chain entry point.

use std::io::Write as _;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use hcom::chain_supervisor::{
    ChainSignal, ChainTitleState, DurableControl, ForegroundChainSupervisor, GenerationAdapter,
    GenerationEvent, OuterTerminalIdentity, PreparedGeneration, ShutdownReason, SignalSendResult,
    SupervisorRunOutcome, TargetReservation, linux_process_birth_identity,
};
use serde_json::json;

use crate::chain_control::HcomChainControl;
use crate::codex_chain_adapter::{CodexActive, CodexAdapterPreflight, CodexGenerationAdapter};
use crate::commands::handoff::{bounded_json, managed_actor_from_ctx, print_error};
use crate::db::HcomDb;
use crate::handoff::{
    self, ChainSpec, ChainState, HandoffError, MAX_MODEL_REF_BYTES, MAX_STATUS_HUMAN_BYTES,
    RecoveryOutcome, RecoveryPlanCode, RecoveryReservation, SupervisorActor, TargetMaterialization,
    TerminalChain, TerminalOwnerEvidence, chain_status_for_actor, chain_status_for_terminal_owner,
    create_public_chain_reservation,
};
use crate::shared::CommandContext;

const CHAIN_AFTER_HELP: &str = "\
`hcom chain codex` creates a fresh Codex 0.145.0 session in this exact foreground
terminal. It never resumes/forks a Codex session, opens another terminal, runs
in the background, or performs a heuristic handoff. A successor is launched
only after an explicit typed handoff completes its Stop/task_complete gates.

Use `hcom chain status [CHAIN_ID]` to inspect bounded product state. After a
supervisor crash, use the exact version printed by status:
  hcom chain recover CHAIN_ID --version VERSION";

const QUIESCE_TIMEOUT: Duration = Duration::from_secs(5);
const POLICY_REF: &str = "codex-0.145.0-foreground-v1";

#[derive(Parser, Debug)]
#[command(
    name = "chain",
    about = "Run or inspect a foreground same-terminal Codex chain",
    after_help = CHAIN_AFTER_HELP
)]
pub struct ChainArgs {
    #[command(subcommand)]
    pub command: ChainCommand,
}

#[derive(Subcommand, Debug)]
pub enum ChainCommand {
    /// Start one fresh, foreground-only Codex chain
    Codex(ChainCodexArgs),
    /// Show the current or selected handoff chain
    Status(ChainStatusArgs),
    /// Re-enter a crashed chain after exact process-absence proof
    Recover(ChainRecoverArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ChainReasoning {
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ChainReasoning {
    fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ChainSandbox {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl ChainSandbox {
    fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ChainApproval {
    Never,
    OnRequest,
    Untrusted,
}

impl ChainApproval {
    fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::OnRequest => "on-request",
            Self::Untrusted => "untrusted",
        }
    }
}

#[derive(Args, Debug)]
pub struct ChainCodexArgs {
    /// Immutable group tag inherited by every fresh generation
    #[arg(long)]
    pub tag: Option<String>,
    /// Exact model reference; passed as one bounded argv value
    #[arg(long)]
    pub model: String,
    #[arg(long, value_enum)]
    pub reasoning: ChainReasoning,
    #[arg(long, value_enum)]
    pub sandbox: ChainSandbox,
    #[arg(long, value_enum)]
    pub approval: ChainApproval,
}

#[derive(Args, Debug)]
pub struct ChainStatusArgs {
    pub id: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct ChainRecoverArgs {
    pub id: String,
    #[arg(long)]
    pub version: i64,
    #[arg(long)]
    pub json: bool,
}

pub(crate) struct HumanTerminalContext {
    pub outer: OuterTerminalIdentity,
    pub owner: TerminalOwnerEvidence,
}

fn typed_runtime(code: &'static str, message: &'static str) -> HandoffError {
    HandoffError::TypedConflict { code, message }
}

fn opaque(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..24]
    )
}

fn validate_model(model: &str) -> Result<String, HandoffError> {
    if model.is_empty()
        || model.len() > MAX_MODEL_REF_BYTES
        || !model
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !model.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':')
        })
    {
        return Err(HandoffError::Invalid(
            "model must be a bounded model reference containing only letters, numbers, .-_/:"
                .to_string(),
        ));
    }
    Ok(model.to_string())
}

fn same_tty_stdio() -> Result<(u64, u64), HandoffError> {
    let mut identity = None;
    for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // SAFETY: fd is one of the caller's standard descriptors and stat is
        // initialized before it is read.
        let mut stat = MaybeUninit::<libc::stat>::zeroed();
        if unsafe { libc::isatty(fd) } != 1 || unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == -1 {
            return Err(typed_runtime(
                "foreground_terminal_required",
                "chain mode requires stdin, stdout, and stderr on one foreground terminal",
            ));
        }
        // SAFETY: fstat succeeded.
        let stat = unsafe { stat.assume_init() };
        let current = (stat.st_dev, stat.st_ino);
        match identity {
            None => identity = Some(current),
            Some(expected) if expected == current => {}
            Some(_) => {
                return Err(typed_runtime(
                    "foreground_terminal_required",
                    "chain mode requires stdin, stdout, and stderr on one foreground terminal",
                ));
            }
        }
    }
    identity.ok_or_else(|| {
        typed_runtime(
            "foreground_terminal_required",
            "chain mode requires one foreground terminal",
        )
    })
}

fn nested_chain_environment() -> bool {
    crate::shared::is_inside_ai_tool()
        || [
            "HCOM_PROCESS_ID",
            "HCOM_LAUNCHED",
            "HCOM_PTY_MODE",
            "HCOM_BACKGROUND",
            "HCOM_LAUNCHED_BY",
            "HCOM_LAUNCHED_PRESET",
            "HCOM_CHAIN_ID",
            "HCOM_CHAIN_GENERATION",
            "HCOM_CHAIN_LAUNCH_NONCE",
            "HCOM_CHAIN_PROCESS_BIRTH_IDENTITY",
            "HCOM_CHAIN_CODEX_VERSION",
            "HCOM_CHAIN_HANDOFF_ID",
            "CODEX_THREAD_ID",
            "CLAUDECODE",
            "CLAUDE_CODE_ENTRYPOINT",
        ]
        .iter()
        .any(|key| std::env::var_os(key).is_some_and(|value| !value.is_empty()))
}

pub(crate) fn capture_human_terminal() -> Result<HumanTerminalContext, HandoffError> {
    if nested_chain_environment() {
        return Err(typed_runtime(
            "nested_chain_forbidden",
            "chain commands that own a terminal must be run from an un-managed human shell",
        ));
    }
    let (tty_device, tty_inode) = same_tty_stdio()?;
    let outer = OuterTerminalIdentity::capture(libc::STDIN_FILENO).map_err(|_| {
        typed_runtime(
            "foreground_terminal_required",
            "caller must own the current foreground terminal process group",
        )
    })?;
    if outer.tty_device != tty_device || outer.tty_inode != tty_inode {
        return Err(typed_runtime(
            "foreground_terminal_required",
            "terminal identity changed while it was captured",
        ));
    }
    let workspace = std::env::current_dir()
        .ok()
        .and_then(|path| std::fs::canonicalize(path).ok())
        .ok_or_else(|| {
            typed_runtime(
                "workspace_unavailable",
                "current workspace is unavailable or not canonical",
            )
        })?;
    let supervisor = SupervisorActor {
        process_id: opaque("chain-supervisor"),
        process_birth_identity: linux_process_birth_identity(outer.supervisor_pid).map_err(
            |_| {
                typed_runtime(
                    "process_identity_unavailable",
                    "foreground supervisor process identity is unavailable",
                )
            },
        )?,
    };
    Ok(HumanTerminalContext {
        outer,
        owner: TerminalOwnerEvidence {
            workspace,
            supervisor,
            supervisor_pid: i64::from(outer.supervisor_pid),
            supervisor_pgid: i64::from(outer.supervisor_pgid),
            outer_foreground_pgid: i64::from(outer.foreground_pgid),
            outer_tty_device: i64::try_from(outer.tty_device).map_err(|_| {
                typed_runtime(
                    "foreground_terminal_required",
                    "terminal identity exceeds the supported bound",
                )
            })?,
            outer_tty_inode: i64::try_from(outer.tty_inode).map_err(|_| {
                typed_runtime(
                    "foreground_terminal_required",
                    "terminal identity exceeds the supported bound",
                )
            })?,
        },
    })
}

fn build_spec(
    terminal: &HumanTerminalContext,
    args: &ChainCodexArgs,
) -> Result<ChainSpec, HandoffError> {
    let model = validate_model(&args.model)?;
    let tag = handoff::validate_chain_tag(args.tag.as_deref().unwrap_or(""))?;
    Ok(ChainSpec {
        workspace: terminal.owner.workspace.clone(),
        tool: "codex".to_string(),
        tag,
        model_ref: model,
        reasoning_ref: args.reasoning.as_str().to_string(),
        permission_policy_ref: format!(
            "approval={};sandbox={}",
            args.approval.as_str(),
            args.sandbox.as_str()
        ),
        policy_ref: POLICY_REF.to_string(),
        supervisor_process_id: terminal.owner.supervisor.process_id.clone(),
        supervisor_process_birth_identity: terminal.owner.supervisor.process_birth_identity.clone(),
        supervisor_pid: terminal.owner.supervisor_pid,
        supervisor_pgid: terminal.owner.supervisor_pgid,
        outer_foreground_pgid: terminal.owner.outer_foreground_pgid,
        outer_tty_device: terminal.owner.outer_tty_device,
        outer_tty_inode: terminal.owner.outer_tty_inode,
        launch_nonce: opaque("launch"),
    })
}

fn profile_from_spec(spec: &ChainSpec) -> TerminalChain {
    TerminalChain {
        id: "tc-preflight".to_string(),
        workspace: spec.workspace.to_string_lossy().into_owned(),
        tool: spec.tool.clone(),
        tag: spec.tag.clone(),
        model_ref: spec.model_ref.clone(),
        reasoning_ref: spec.reasoning_ref.clone(),
        permission_policy_ref: spec.permission_policy_ref.clone(),
        policy_ref: spec.policy_ref.clone(),
        supervisor_process_id: spec.supervisor_process_id.clone(),
        supervisor_process_birth_identity: spec.supervisor_process_birth_identity.clone(),
        supervisor_pid: Some(spec.supervisor_pid),
        supervisor_pgid: Some(spec.supervisor_pgid),
        outer_foreground_pgid: Some(spec.outer_foreground_pgid),
        outer_tty_device: Some(spec.outer_tty_device),
        outer_tty_inode: Some(spec.outer_tty_inode),
        current_generation: 1,
        state: ChainState::LaunchingTarget,
        version: 0,
        created_at: 0.0,
        updated_at: 0.0,
    }
}

fn target_materialization(
    expected_version: i64,
    reservation: &TargetReservation,
    identity: &hcom::chain_supervisor::GenerationIdentity,
) -> TargetMaterialization {
    TargetMaterialization {
        expected_version,
        launch_nonce: reservation.launch_nonce.clone(),
        instance_name: identity.instance_name.clone(),
        hcom_session_id: identity.hcom_session_id.clone(),
        process_id: identity.process_id.clone(),
        process_birth_identity: identity.process_birth_identity.clone(),
        wrapper_pid: i64::from(identity.wrapper_pid),
        wrapper_pgid: i64::from(identity.wrapper_pgid),
        child_pid: i64::from(identity.child_pid),
        child_pgid: i64::from(identity.child_pgid),
        child_process_birth_identity: identity.child_process_birth_identity.clone(),
    }
}

fn persist_initial_failure(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    chain_id: &str,
    kind: &'static str,
    reason: &'static str,
) {
    let Ok(Some(chain)) = handoff::get_chain(db, chain_id) else {
        return;
    };
    let Ok(Some(generation)) = handoff::get_generation(db, chain_id, chain.current_generation)
    else {
        return;
    };
    let _ = handoff::fail_initial_generation(
        db,
        supervisor,
        chain_id,
        chain.version,
        generation.version,
        kind,
        reason,
    );
}

struct InitialActiveFailure {
    kind: &'static str,
    reason: &'static str,
    shutdown: ShutdownReason,
}

struct ChainBootstrapFailure {
    error: HandoffError,
    active: Option<Box<CodexActive>>,
    prepared: Option<Box<crate::codex_chain_adapter::CodexPrepared>>,
    must_exit: bool,
}

impl ChainBootstrapFailure {
    fn plain(error: HandoffError) -> Self {
        Self {
            error,
            active: None,
            prepared: None,
            must_exit: false,
        }
    }

    fn with_active_cleanup(
        error: HandoffError,
        cleanup: hcom::chain_supervisor::FinishAttempt<CodexActive>,
    ) -> Self {
        let must_exit = cleanup.residual.is_some() || !cleanup.evidence.successful();
        Self {
            error,
            active: cleanup.residual.map(Box::new),
            prepared: None,
            must_exit,
        }
    }

    fn with_prepared_cleanup(
        error: HandoffError,
        cleanup: hcom::chain_supervisor::FinishAttempt<crate::codex_chain_adapter::CodexPrepared>,
    ) -> Self {
        let must_exit = cleanup.residual.is_some() || !cleanup.evidence.successful();
        Self {
            error,
            active: None,
            prepared: cleanup.residual.map(Box::new),
            must_exit,
        }
    }
}

impl From<HandoffError> for ChainBootstrapFailure {
    fn from(error: HandoffError) -> Self {
        Self::plain(error)
    }
}

fn finish_bootstrap_failure(
    adapter: CodexGenerationAdapter,
    failure: ChainBootstrapFailure,
    json: bool,
) -> i32 {
    let ChainBootstrapFailure {
        error,
        active,
        prepared,
        must_exit,
    } = failure;
    // Restore the human terminal before reporting. If cleanup retained an
    // owned process/PTY handle, keep that handle live until process exit so
    // kernel fd closure plus the two validated PDEATHSIG edges perform the
    // crash-safe teardown. Do not drop it, forge cleanup success, retry a
    // signal, or permit a second target.
    drop(adapter);
    let code = print_error(error, json);
    if must_exit {
        let residual = ManuallyDrop::new((active, prepared));
        std::hint::black_box(&residual);
        std::process::exit(code);
    }
    code
}

fn fail_initial_active(
    db: &HcomDb,
    adapter: &mut CodexGenerationAdapter,
    supervisor: &SupervisorActor,
    chain_id: &str,
    active: CodexActive,
    failure: InitialActiveFailure,
) -> ChainBootstrapFailure {
    persist_initial_failure(db, supervisor, chain_id, failure.kind, failure.reason);
    let cleanup = adapter.shutdown_without_successor(active, failure.shutdown);
    ChainBootstrapFailure::with_active_cleanup(typed_runtime(failure.kind, failure.reason), cleanup)
}

fn bootstrap_initial(
    db: &HcomDb,
    adapter: &mut CodexGenerationAdapter,
    supervisor: &SupervisorActor,
    outer: OuterTerminalIdentity,
    chain: &TerminalChain,
    generation: &handoff::TerminalGeneration,
) -> Result<CodexActive, ChainBootstrapFailure> {
    let reservation = TargetReservation {
        handoff_id: chain.id.clone(),
        expected_version: chain.version,
        generation: u64::try_from(generation.generation).map_err(|_| HandoffError::Storage)?,
        launch_nonce: generation.launch_nonce.clone(),
    };
    handoff::begin_generation_prepare(
        db,
        supervisor,
        &chain.id,
        generation.generation,
        chain.version,
        &generation.launch_nonce,
    )?;
    let _ = adapter.set_chain_title(reservation.generation, ChainTitleState::Launching);
    let prepared = match adapter.prepare_target(&reservation, outer) {
        Ok(prepared) => prepared,
        Err(_) => {
            persist_initial_failure(
                db,
                supervisor,
                &chain.id,
                "initial_prepare_failed",
                "initial Codex preparation failed",
            );
            return Err(ChainBootstrapFailure::plain(typed_runtime(
                "initial_prepare_failed",
                "initial Codex preparation failed",
            )));
        }
    };
    let identity = prepared.identity().clone();
    if identity.generation != reservation.generation
        || identity.launch_nonce != reservation.launch_nonce
    {
        persist_initial_failure(
            db,
            supervisor,
            &chain.id,
            "initial_identity_mismatch",
            "initial Codex identity did not match its reservation",
        );
        let cleanup = adapter.abort_prepared(prepared);
        return Err(ChainBootstrapFailure::with_prepared_cleanup(
            typed_runtime(
                "initial_identity_mismatch",
                "initial Codex identity did not match its reservation",
            ),
            cleanup,
        ));
    }
    if handoff::materialize_initial_generation(
        db,
        supervisor,
        &chain.id,
        chain.version,
        &target_materialization(generation.version, &reservation, &identity),
    )
    .is_err()
    {
        persist_initial_failure(
            db,
            supervisor,
            &chain.id,
            "initial_materialization_failed",
            "initial Codex durable materialization failed",
        );
        let cleanup = adapter.abort_prepared(prepared);
        return Err(ChainBootstrapFailure::with_prepared_cleanup(
            typed_runtime(
                "initial_materialization_failed",
                "initial Codex durable materialization failed",
            ),
            cleanup,
        ));
    }
    let mut active = match adapter.activate_target(prepared) {
        Ok(active) => active,
        Err((prepared, _)) => {
            persist_initial_failure(
                db,
                supervisor,
                &chain.id,
                "initial_activation_failed",
                "initial Codex activation failed",
            );
            let cleanup = adapter.abort_prepared(prepared);
            return Err(ChainBootstrapFailure::with_prepared_cleanup(
                typed_runtime(
                    "initial_activation_failed",
                    "initial Codex activation failed",
                ),
                cleanup,
            ));
        }
    };

    loop {
        let durable = match handoff::get_generation(db, &chain.id, generation.generation) {
            Ok(Some(durable)) => durable,
            _ => {
                return Err(fail_initial_active(
                    db,
                    adapter,
                    supervisor,
                    &chain.id,
                    active,
                    InitialActiveFailure {
                        kind: "initial_native_read_failed",
                        reason: "initial Codex native identity could not be read",
                        shutdown: ShutdownReason::Explicit,
                    },
                ));
            }
        };
        if let Some(native) = durable.native_session_id {
            if adapter.bind_native_session(&mut active, &native).is_err() {
                return Err(fail_initial_active(
                    db,
                    adapter,
                    supervisor,
                    &chain.id,
                    active,
                    InitialActiveFailure {
                        kind: "initial_native_bind_failed",
                        reason: "initial Codex native identity could not be bound",
                        shutdown: ShutdownReason::Explicit,
                    },
                ));
            }
            let durable_chain = match handoff::get_chain(db, &chain.id) {
                Ok(Some(chain)) => chain,
                _ => {
                    return Err(fail_initial_active(
                        db,
                        adapter,
                        supervisor,
                        &chain.id,
                        active,
                        InitialActiveFailure {
                            kind: "initial_chain_read_failed",
                            reason: "initial Codex chain state could not be read",
                            shutdown: ShutdownReason::Explicit,
                        },
                    ));
                }
            };
            if durable.state != handoff::GenerationState::Active
                || durable_chain.state != ChainState::Active
            {
                return Err(fail_initial_active(
                    db,
                    adapter,
                    supervisor,
                    &chain.id,
                    active,
                    InitialActiveFailure {
                        kind: "initial_session_state_mismatch",
                        reason: "initial Codex SessionStart state was inconsistent",
                        shutdown: ShutdownReason::Explicit,
                    },
                ));
            }
            return Ok(active);
        }

        match adapter.wait_event(&mut active, Duration::from_millis(100)) {
            Ok(GenerationEvent::ControlWake | GenerationEvent::Timeout) => {}
            Ok(GenerationEvent::Resize) => {
                if adapter.resize(&mut active).is_err() {
                    return Err(fail_initial_active(
                        db,
                        adapter,
                        supervisor,
                        &chain.id,
                        active,
                        InitialActiveFailure {
                            kind: "initial_resize_failed",
                            reason: "initial Codex terminal resize failed",
                            shutdown: ShutdownReason::Explicit,
                        },
                    ));
                }
            }
            Ok(GenerationEvent::Continue) => {
                if adapter.reassert_outer_terminal().is_err()
                    || adapter.resize(&mut active).is_err()
                {
                    return Err(fail_initial_active(
                        db,
                        adapter,
                        supervisor,
                        &chain.id,
                        active,
                        InitialActiveFailure {
                            kind: "initial_continue_failed",
                            reason: "initial Codex terminal restoration failed",
                            shutdown: ShutdownReason::Explicit,
                        },
                    ));
                }
            }
            Ok(GenerationEvent::Interrupt) => {
                if adapter.send_signal(&active, ChainSignal::Interrupt) != SignalSendResult::Sent {
                    return Err(fail_initial_active(
                        db,
                        adapter,
                        supervisor,
                        &chain.id,
                        active,
                        InitialActiveFailure {
                            kind: "initial_interrupt_failed",
                            reason: "SIGINT could not be forwarded to initial Codex",
                            shutdown: ShutdownReason::Explicit,
                        },
                    ));
                }
            }
            Ok(GenerationEvent::Hangup) => {
                return Err(fail_initial_active(
                    db,
                    adapter,
                    supervisor,
                    &chain.id,
                    active,
                    InitialActiveFailure {
                        kind: "initial_outer_hangup",
                        reason: "outer terminal hung up before initial SessionStart",
                        shutdown: ShutdownReason::OuterHangup,
                    },
                ));
            }
            Ok(GenerationEvent::ChildExited(exit)) => {
                persist_initial_failure(
                    db,
                    supervisor,
                    &chain.id,
                    "initial_exited_before_session_start",
                    "initial Codex exited before exact SessionStart",
                );
                let finish = adapter.finish_after_exit(active, &exit);
                return Err(ChainBootstrapFailure::with_active_cleanup(
                    typed_runtime(
                        "initial_exited_before_session_start",
                        "initial Codex exited before exact SessionStart",
                    ),
                    finish,
                ));
            }
            Err(_) => {
                return Err(fail_initial_active(
                    db,
                    adapter,
                    supervisor,
                    &chain.id,
                    active,
                    InitialActiveFailure {
                        kind: "initial_event_loop_failed",
                        reason: "initial Codex event loop failed",
                        shutdown: ShutdownReason::Explicit,
                    },
                ));
            }
        }
    }
}

fn fail_recovery_target(
    control: &mut HcomChainControl,
    adapter: &mut CodexGenerationAdapter,
    reservation: &TargetReservation,
    active: Option<CodexActive>,
    identity: Option<&hcom::chain_supervisor::GenerationIdentity>,
    kind: &'static str,
    reason: &'static str,
) -> ChainBootstrapFailure {
    let cleanup =
        active.map(|active| adapter.shutdown_without_successor(active, ShutdownReason::Explicit));
    let _ = control.record_target_failure(
        reservation,
        identity,
        cleanup.as_ref().map(|value| &value.evidence),
        kind,
        reason,
    );
    cleanup.map_or_else(
        || ChainBootstrapFailure::plain(typed_runtime(kind, reason)),
        |cleanup| ChainBootstrapFailure::with_active_cleanup(typed_runtime(kind, reason), cleanup),
    )
}

fn recovery_target_reservation(
    recovery: &RecoveryReservation,
) -> Result<TargetReservation, HandoffError> {
    Ok(TargetReservation {
        handoff_id: recovery.handoff_id.clone().ok_or(HandoffError::Storage)?,
        expected_version: recovery.handoff_version.ok_or(HandoffError::Storage)?,
        generation: u64::try_from(recovery.generation.generation)
            .map_err(|_| HandoffError::Storage)?,
        launch_nonce: recovery.generation.launch_nonce.clone(),
    })
}

fn bootstrap_recovery_target(
    control: &mut HcomChainControl,
    adapter: &mut CodexGenerationAdapter,
    outer: OuterTerminalIdentity,
    recovery: &RecoveryReservation,
) -> Result<CodexActive, ChainBootstrapFailure> {
    let reservation = recovery_target_reservation(recovery)?;
    if control.begin_target_prepare(&reservation).is_err() {
        let _ = control.record_target_failure(
            &reservation,
            None,
            None,
            "recovery_target_prepare_intent_failed",
            "recovery target prepare intent could not be persisted",
        );
        return Err(ChainBootstrapFailure::plain(typed_runtime(
            "recovery_target_prepare_intent_failed",
            "recovery target prepare intent could not be persisted",
        )));
    }
    let _ = adapter.set_chain_title(reservation.generation, ChainTitleState::Launching);
    let prepared = match adapter.prepare_target(&reservation, outer) {
        Ok(prepared) => prepared,
        Err(_) => {
            let _ = control.record_target_failure(
                &reservation,
                None,
                None,
                "recovery_target_prepare_failed",
                "recovery target preparation failed",
            );
            return Err(ChainBootstrapFailure::plain(typed_runtime(
                "recovery_target_prepare_failed",
                "recovery target preparation failed",
            )));
        }
    };
    let identity = prepared.identity().clone();
    if identity.generation != reservation.generation
        || identity.launch_nonce != reservation.launch_nonce
    {
        let cleanup = adapter.abort_prepared(prepared);
        let _ = control.record_target_failure(
            &reservation,
            Some(&identity),
            Some(&cleanup.evidence),
            "recovery_target_identity_mismatch",
            "recovery target identity did not match its reservation",
        );
        return Err(ChainBootstrapFailure::with_prepared_cleanup(
            typed_runtime(
                "recovery_target_identity_mismatch",
                "recovery target identity did not match its reservation",
            ),
            cleanup,
        ));
    }
    if control.materialize_target(&reservation, &identity).is_err() {
        let cleanup = adapter.abort_prepared(prepared);
        let _ = control.record_target_failure(
            &reservation,
            Some(&identity),
            Some(&cleanup.evidence),
            "recovery_target_materialization_failed",
            "recovery target durable materialization failed",
        );
        return Err(ChainBootstrapFailure::with_prepared_cleanup(
            typed_runtime(
                "recovery_target_materialization_failed",
                "recovery target durable materialization failed",
            ),
            cleanup,
        ));
    }
    let mut active = match adapter.activate_target(prepared) {
        Ok(active) => active,
        Err((prepared, _)) => {
            let cleanup = adapter.abort_prepared(prepared);
            let _ = control.record_target_failure(
                &reservation,
                Some(&identity),
                Some(&cleanup.evidence),
                "recovery_target_activation_failed",
                "recovery target activation failed",
            );
            return Err(ChainBootstrapFailure::with_prepared_cleanup(
                typed_runtime(
                    "recovery_target_activation_failed",
                    "recovery target activation failed",
                ),
                cleanup,
            ));
        }
    };
    loop {
        let native = match control.target_native_session(&reservation, &identity) {
            Ok(native) => native,
            Err(_) => {
                return Err(fail_recovery_target(
                    control,
                    adapter,
                    &reservation,
                    Some(active),
                    Some(&identity),
                    "recovery_target_native_read_failed",
                    "recovery target native identity could not be read",
                ));
            }
        };
        if let Some(native) = native {
            if adapter.bind_native_session(&mut active, &native).is_err() {
                return Err(fail_recovery_target(
                    control,
                    adapter,
                    &reservation,
                    Some(active),
                    Some(&identity),
                    "recovery_target_native_bind_failed",
                    "recovery target native identity could not be bound",
                ));
            }
            let ready_identity = adapter.identity(&active).clone();
            if let Err(_error) = control.target_ready(&reservation, &ready_identity) {
                return Err(fail_recovery_target(
                    control,
                    adapter,
                    &reservation,
                    Some(active),
                    Some(&ready_identity),
                    "recovery_target_ready_failed",
                    "recovery target ready evidence failed",
                ));
            }
            return Ok(active);
        }
        match adapter.wait_event(&mut active, Duration::from_millis(100)) {
            Ok(GenerationEvent::ControlWake | GenerationEvent::Timeout) => {}
            Ok(GenerationEvent::Resize) => {
                if adapter.resize(&mut active).is_err() {
                    return Err(fail_recovery_target(
                        control,
                        adapter,
                        &reservation,
                        Some(active),
                        Some(&identity),
                        "recovery_target_resize_failed",
                        "recovery target terminal resize failed",
                    ));
                }
            }
            Ok(GenerationEvent::Continue) => {
                if adapter.reassert_outer_terminal().is_err()
                    || adapter.resize(&mut active).is_err()
                {
                    return Err(fail_recovery_target(
                        control,
                        adapter,
                        &reservation,
                        Some(active),
                        Some(&identity),
                        "recovery_target_continue_failed",
                        "recovery target terminal restoration failed",
                    ));
                }
            }
            Ok(GenerationEvent::Interrupt) => {
                if adapter.send_signal(&active, ChainSignal::Interrupt) != SignalSendResult::Sent {
                    return Err(fail_recovery_target(
                        control,
                        adapter,
                        &reservation,
                        Some(active),
                        Some(&identity),
                        "recovery_target_interrupt_failed",
                        "SIGINT could not be forwarded to recovery target",
                    ));
                }
            }
            Ok(GenerationEvent::Hangup) => {
                return Err(fail_recovery_target(
                    control,
                    adapter,
                    &reservation,
                    Some(active),
                    Some(&identity),
                    "recovery_target_outer_hangup",
                    "outer terminal hung up before recovery target SessionStart",
                ));
            }
            Ok(GenerationEvent::ChildExited(exit)) => {
                let finish = adapter.finish_after_exit(active, &exit);
                let _ = control.record_target_failure(
                    &reservation,
                    Some(&identity),
                    Some(&finish.evidence),
                    "recovery_target_exited_before_session_start",
                    "recovery target exited before exact SessionStart",
                );
                return Err(ChainBootstrapFailure::with_active_cleanup(
                    typed_runtime(
                        "recovery_target_exited_before_session_start",
                        "recovery target exited before exact SessionStart",
                    ),
                    finish,
                ));
            }
            Err(_) => {
                return Err(fail_recovery_target(
                    control,
                    adapter,
                    &reservation,
                    Some(active),
                    Some(&identity),
                    "recovery_target_event_loop_failed",
                    "recovery target event loop failed",
                ));
            }
        }
    }
}

fn run_supervisor(
    db_path: &std::path::Path,
    chain_id: &str,
    outer: OuterTerminalIdentity,
    control: HcomChainControl,
    adapter: CodexGenerationAdapter,
    active: CodexActive,
) -> i32 {
    let mut supervisor = match ForegroundChainSupervisor::new_preserving_ownership(
        outer,
        control,
        adapter,
        active,
        QUIESCE_TIMEOUT,
    ) {
        Ok(supervisor) => supervisor,
        Err((_error, control, adapter, active)) => {
            drop(control);
            drop(adapter);
            let code = print_error(
                typed_runtime(
                    "supervisor_invariant_failed",
                    "foreground supervisor initialization failed",
                ),
                false,
            );
            // Do not drop the sole live generation handle and continue
            // after a constructor invariant error. Process exit closes the
            // retained descriptors and drives the already-validated
            // wrapper/child parent-death cascade without another signal.
            let residual = ManuallyDrop::new(active);
            std::hint::black_box(&residual);
            std::process::exit(code);
        }
    };
    loop {
        match supervisor.run() {
            SupervisorRunOutcome::Stopped => return 0,
            SupervisorRunOutcome::AwaitingAcceptance {
                generation,
                handoff_id,
            } => {
                let version = HcomDb::open_at(db_path)
                    .ok()
                    .and_then(|db| handoff::get_handoff(&db, &handoff_id).ok().flatten())
                    .map(|handoff| handoff.version)
                    .unwrap_or_default();
                println!(
                    "handoff {handoff_id} generation={generation} state=awaiting_acceptance \
                     version={version}\nnext: hcom handoff inspect {handoff_id} --version {version}"
                );
            }
            SupervisorRunOutcome::NeedsRecovery(_) => {
                let version = HcomDb::open_at(db_path)
                    .ok()
                    .and_then(|db| handoff::get_chain(&db, chain_id).ok().flatten())
                    .map(|chain| chain.version)
                    .unwrap_or_default();
                eprintln!(
                    "chain {chain_id} needs recovery; inspect with `hcom chain status {chain_id}` \
                     and, only after old processes are absent, run \
                     `hcom chain recover {chain_id} --version {version}`"
                );
                let (_control, adapter, active, prepared, _trace) = supervisor.into_parts();
                let has_residual = active.is_some() || prepared.is_some();
                drop(adapter);
                if has_residual {
                    // A public chain command exits immediately after a
                    // fail-closed recovery outcome. Keep exact local handles
                    // open until kernel process teardown drives the validated
                    // PDEATHSIG cascade; never drop ownership and continue in
                    // this process, and never retry a signal or spawn.
                    let residual = ManuallyDrop::new((active, prepared));
                    std::hint::black_box(&residual);
                    std::process::exit(2);
                }
                return 2;
            }
        }
    }
}

fn open_chain_control(
    db_path: &std::path::Path,
    chain_id: &str,
    supervisor: &SupervisorActor,
    outer: OuterTerminalIdentity,
) -> Result<HcomChainControl, HandoffError> {
    let control_db = HcomDb::open_at(db_path).map_err(|_| HandoffError::Storage)?;
    HcomChainControl::new(control_db, chain_id.to_string(), supervisor.clone(), outer)
}

fn persist_recovery_prelaunch_failure(
    db: &HcomDb,
    control: &mut HcomChainControl,
    supervisor: &SupervisorActor,
    recovery: &RecoveryReservation,
    kind: &'static str,
    reason: &'static str,
) {
    if recovery.handoff_id.is_none() {
        persist_initial_failure(db, supervisor, &recovery.chain.id, kind, reason);
    } else if let Ok(reservation) = recovery_target_reservation(recovery) {
        let _ = control.record_target_failure(&reservation, None, None, kind, reason);
    }
}

fn recovery_code(db: &HcomDb, chain: &TerminalChain) -> &'static str {
    if chain.state != ChainState::NeedsRecovery {
        return "none";
    }
    if handoff::public_chain_claim_released(db, &chain.id).unwrap_or(false) {
        return RecoveryPlanCode::SourceDeadBeforeCommit.as_str();
    }
    let failure = db
        .conn()
        .query_row(
            "SELECT failure_kind FROM terminal_handoffs
             WHERE chain_id = ?1 ORDER BY created_at DESC LIMIT 1",
            rusqlite::params![chain.id],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_default();
    match failure.as_str() {
        "exit_without_stop" => "source_exit_without_stop",
        "sigterm_timeout" => "sigterm_timeout",
        "target_rejected" => "target_rejected",
        "supervisor_shutdown" | "outer_hangup" => "supervisor_shutdown",
        value if value.starts_with("target_") => "target_launch_failed",
        _ => "manual_intervention_required",
    }
}

fn fresh_start_command(chain: &TerminalChain) -> String {
    let mut approval = None;
    let mut sandbox = None;
    for field in chain.permission_policy_ref.split(';') {
        if let Some(value) = field.strip_prefix("approval=") {
            approval = Some(value);
        } else if let Some(value) = field.strip_prefix("sandbox=") {
            sandbox = Some(value);
        }
    }
    match (approval, sandbox) {
        (Some(approval), Some(sandbox))
            if validate_model(&chain.model_ref).is_ok()
                && handoff::validate_chain_tag(&chain.tag).is_ok()
                && matches!(
                    chain.reasoning_ref.as_str(),
                    "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                )
                && matches!(approval, "never" | "on-request" | "untrusted")
                && matches!(
                    sandbox,
                    "read-only" | "workspace-write" | "danger-full-access"
                ) =>
        {
            let tag = if chain.tag.is_empty() {
                String::new()
            } else {
                format!(" --tag {}", chain.tag)
            };
            format!(
                "hcom chain codex{tag} --model {} --reasoning {} --sandbox {} --approval {}",
                chain.model_ref, chain.reasoning_ref, sandbox, approval,
            )
        }
        _ => "hcom chain codex [--tag TAG] --model MODEL --reasoning LEVEL --sandbox MODE --approval MODE"
            .to_string(),
    }
}

fn latest_handoff(db: &HcomDb, chain_id: &str) -> Option<(String, String, i64)> {
    db.conn()
        .query_row(
            "SELECT id, state, version FROM terminal_handoffs
             WHERE chain_id = ?1 ORDER BY created_at DESC LIMIT 1",
            rusqlite::params![chain_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok()
}

fn next_command(db: &HcomDb, chain: &TerminalChain) -> String {
    if handoff::public_chain_claim_released(db, &chain.id).unwrap_or(false) {
        return fresh_start_command(chain);
    }
    if chain.state == ChainState::NeedsRecovery {
        return format!(
            "hcom chain recover {} --version {}",
            chain.id, chain.version
        );
    }
    if let Some((id, state, version)) = latest_handoff(db, &chain.id)
        && state == "awaiting_acceptance"
    {
        return format!("hcom handoff inspect {id} --version {version}");
    }
    format!("hcom chain status {}", chain.id)
}

fn chain_json(db: &HcomDb, chain: &TerminalChain) -> serde_json::Value {
    let handoff = latest_handoff(db, &chain.id).map(|(id, state, version)| {
        json!({
            "id": id,
            "state": state,
            "version": version,
        })
    });
    json!({
        "id": chain.id,
        "state": chain.state.as_str(),
        "generation": chain.current_generation,
        "transition": chain.state.as_str(),
        "handoff": handoff,
        "workspace": chain.workspace,
        "policy": {
            "tool": chain.tool,
            "tag": chain.tag,
            "model": chain.model_ref,
            "reasoning": chain.reasoning_ref,
            "permission": chain.permission_policy_ref,
            "profile": chain.policy_ref,
        },
        "recovery": {
            "required": chain.state == ChainState::NeedsRecovery,
            "reason_code": recovery_code(db, chain),
        },
        "next_command": next_command(db, chain),
        "version": chain.version,
    })
}

fn chain_human(db: &HcomDb, chain: &TerminalChain) -> Result<String, HandoffError> {
    let handoff = latest_handoff(db, &chain.id)
        .map(|(id, state, version)| format!("{id} state={state} version={version}"))
        .unwrap_or_else(|| "none".to_string());
    let output = format!(
        "{} state={} version={} generation={} transition={}\n\
         handoff={}\nworkspace={}\n\
         tool={} tag={} model={} reasoning={} permission={} profile={}\n\
         recovery_required={} recovery_reason={}\nnext={}",
        chain.id,
        chain.state,
        chain.version,
        chain.current_generation,
        chain.state,
        handoff,
        chain.workspace,
        chain.tool,
        chain.tag,
        chain.model_ref,
        chain.reasoning_ref,
        chain.permission_policy_ref,
        chain.policy_ref,
        chain.state == ChainState::NeedsRecovery,
        recovery_code(db, chain),
        next_command(db, chain),
    );
    if output.len() > MAX_STATUS_HUMAN_BYTES {
        return Err(HandoffError::Storage);
    }
    Ok(output)
}

fn print_chain(db: &HcomDb, chain: &TerminalChain, json_mode: bool) -> i32 {
    if json_mode {
        match bounded_json(&chain_json(db, chain)) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, true),
        }
    } else {
        match chain_human(db, chain) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, false),
        }
    }
}

fn ensure_codex_hooks_ready() -> Result<(), HandoffError> {
    let hooks_ready = crate::hooks::codex::verify_codex_hooks_installed(false)
        && crate::hooks::codex::codex_current_feature_enabled();
    if !hooks_ready {
        crate::hooks::codex::try_setup_codex_hooks(false).map_err(|_| {
            typed_runtime(
                "codex_hook_setup_failed",
                "required Codex hooks could not be installed or verified",
            )
        })?;
    }
    Ok(())
}

fn preflight(spec: &ChainSpec) -> Result<CodexAdapterPreflight, HandoffError> {
    ensure_codex_hooks_ready()?;
    CodexGenerationAdapter::preflight(&profile_from_spec(spec)).map_err(|_| {
        typed_runtime(
            "unsupported_codex_profile",
            "the exact supported Codex 0.145.0 profile is unavailable",
        )
    })
}

fn cmd_codex(db: &HcomDb, args: &ChainCodexArgs) -> i32 {
    let terminal = match capture_human_terminal() {
        Ok(terminal) => terminal,
        Err(error) => return print_error(error, false),
    };
    let spec = match build_spec(&terminal, args) {
        Ok(spec) => spec,
        Err(error) => return print_error(error, false),
    };
    let reservation = match create_public_chain_reservation(db, &spec) {
        Ok(reservation) => reservation,
        Err(error) => return print_error(error, false),
    };
    let control = match open_chain_control(
        db.path(),
        &reservation.chain.id,
        &terminal.owner.supervisor,
        terminal.outer,
    ) {
        Ok(control) => control,
        Err(error) => {
            persist_initial_failure(
                db,
                &terminal.owner.supervisor,
                &reservation.chain.id,
                "initial_control_failed",
                "initial durable supervisor binding failed",
            );
            return print_error(error, false);
        }
    };
    // The durable public claim and exact supervisor identity must exist before
    // hook verification or the Codex version probe starts any helper process.
    let adapter_preflight = match preflight(&spec) {
        Ok(preflight) => preflight,
        Err(error) => {
            persist_initial_failure(
                db,
                &terminal.owner.supervisor,
                &reservation.chain.id,
                "initial_preflight_failed",
                "required Codex hooks or exact profile are unavailable",
            );
            return print_error(error, false);
        }
    };
    println!(
        "chain {} state=launching_target version={} generation=1",
        reservation.chain.id, reservation.chain.version
    );
    let _ = std::io::stdout().flush();
    let mut adapter = match CodexGenerationAdapter::from_preflight(
        &reservation.chain,
        terminal.outer,
        adapter_preflight,
    ) {
        Ok(adapter) => adapter,
        Err(_) => {
            persist_initial_failure(
                db,
                &terminal.owner.supervisor,
                &reservation.chain.id,
                "initial_adapter_failed",
                "initial Codex terminal adapter setup failed",
            );
            return print_error(
                typed_runtime(
                    "initial_adapter_failed",
                    "initial Codex terminal adapter setup failed",
                ),
                false,
            );
        }
    };
    let active = match bootstrap_initial(
        db,
        &mut adapter,
        &terminal.owner.supervisor,
        terminal.outer,
        &reservation.chain,
        &reservation.generation,
    ) {
        Ok(active) => active,
        Err(failure) => return finish_bootstrap_failure(adapter, failure, false),
    };
    println!(
        "chain {} state=active version={} generation=1",
        reservation.chain.id,
        handoff::get_chain(db, &reservation.chain.id)
            .ok()
            .flatten()
            .map(|chain| chain.version)
            .unwrap_or_default()
    );
    run_supervisor(
        db.path(),
        &reservation.chain.id,
        terminal.outer,
        control,
        adapter,
        active,
    )
}

fn cmd_status(db: &HcomDb, args: &ChainStatusArgs, ctx: Option<&CommandContext>) -> i32 {
    let chain = match managed_actor_from_ctx(db, ctx) {
        Ok(actor) => chain_status_for_actor(db, &actor, args.id.as_deref()),
        Err(HandoffError::NotManaged)
            if ctx.and_then(|value| value.identity.as_ref()).is_none() =>
        {
            match capture_human_terminal() {
                Ok(terminal) => {
                    chain_status_for_terminal_owner(db, args.id.as_deref(), &terminal.owner)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    };
    match chain {
        Ok(chain) => print_chain(db, &chain, args.json),
        Err(error) => print_error(error, args.json),
    }
}

fn cmd_recover(db: &HcomDb, args: &ChainRecoverArgs) -> i32 {
    let terminal = match capture_human_terminal() {
        Ok(terminal) => terminal,
        Err(error) => return print_error(error, args.json),
    };
    let chain = match chain_status_for_terminal_owner(db, Some(&args.id), &terminal.owner) {
        Ok(chain) => chain,
        Err(error) => return print_error(error, args.json),
    };
    let recovery = match handoff::begin_public_recovery(db, &args.id, args.version, &terminal.owner)
    {
        Ok(recovery) => recovery,
        Err(error) => return print_error(error, args.json),
    };
    let RecoveryOutcome::Launch(recovery) = recovery else {
        let RecoveryOutcome::Manual { chain, reason } = recovery else {
            unreachable!()
        };
        if args.json {
            let next_command = next_command(db, &chain);
            let value = json!({
                "id": chain.id,
                "state": chain.state.as_str(),
                "version": chain.version,
                "recovery": {
                    "started": false,
                    "reason_code": reason.as_str(),
                    "old_processes_absent": true,
                },
                "next_command": next_command,
            });
            return match bounded_json(&value) {
                Ok(output) => {
                    println!("{output}");
                    2
                }
                Err(error) => print_error(error, true),
            };
        }
        eprintln!(
            "chain {} state={} version={} recovery={} automatic continuation forbidden\n\
             next: {}",
            chain.id,
            chain.state,
            chain.version,
            reason.as_str(),
            next_command(db, &chain),
        );
        return 2;
    };
    let mut control = match open_chain_control(
        db.path(),
        &recovery.chain.id,
        &terminal.owner.supervisor,
        terminal.outer,
    ) {
        Ok(control) => control,
        Err(error) => return print_error(error, args.json),
    };
    // Recovery intent, append-only generation identity, new supervisor
    // identity, and absence evidence are durable before any external Codex
    // helper or target process can start.
    let adapter_preflight = match preflight(&ChainSpec {
        workspace: PathBuf::from(&chain.workspace),
        tool: chain.tool.clone(),
        tag: chain.tag.clone(),
        model_ref: chain.model_ref.clone(),
        reasoning_ref: chain.reasoning_ref.clone(),
        permission_policy_ref: chain.permission_policy_ref.clone(),
        policy_ref: chain.policy_ref.clone(),
        supervisor_process_id: terminal.owner.supervisor.process_id.clone(),
        supervisor_process_birth_identity: terminal.owner.supervisor.process_birth_identity.clone(),
        supervisor_pid: terminal.owner.supervisor_pid,
        supervisor_pgid: terminal.owner.supervisor_pgid,
        outer_foreground_pgid: terminal.owner.outer_foreground_pgid,
        outer_tty_device: terminal.owner.outer_tty_device,
        outer_tty_inode: terminal.owner.outer_tty_inode,
        launch_nonce: opaque("recovery-preflight"),
    }) {
        Ok(preflight) => preflight,
        Err(error) => {
            persist_recovery_prelaunch_failure(
                db,
                &mut control,
                &terminal.owner.supervisor,
                &recovery,
                "recovery_preflight_failed",
                "required Codex hooks or exact profile are unavailable",
            );
            return print_error(error, args.json);
        }
    };
    let mut adapter = match CodexGenerationAdapter::from_preflight(
        &recovery.chain,
        terminal.outer,
        adapter_preflight,
    ) {
        Ok(adapter) => adapter,
        Err(_) => {
            persist_recovery_prelaunch_failure(
                db,
                &mut control,
                &terminal.owner.supervisor,
                &recovery,
                "recovery_adapter_failed",
                "recovery Codex terminal adapter setup failed",
            );
            return print_error(
                typed_runtime(
                    "recovery_adapter_failed",
                    "recovery Codex terminal adapter setup failed",
                ),
                args.json,
            );
        }
    };
    if let Err(error) =
        handoff::revalidate_recovery_absence(db, &recovery.attempt_id, &terminal.owner.supervisor)
    {
        persist_recovery_prelaunch_failure(
            db,
            &mut control,
            &terminal.owner.supervisor,
            &recovery,
            "recovery_absence_revalidation_failed",
            "recovery process absence changed before preparation",
        );
        return print_error(error, args.json);
    }
    let active = if recovery.handoff_id.is_none() {
        match bootstrap_initial(
            db,
            &mut adapter,
            &terminal.owner.supervisor,
            terminal.outer,
            &recovery.chain,
            &recovery.generation,
        ) {
            Ok(active) => active,
            Err(failure) => return finish_bootstrap_failure(adapter, failure, args.json),
        }
    } else {
        match bootstrap_recovery_target(&mut control, &mut adapter, terminal.outer, &recovery) {
            Ok(active) => active,
            Err(failure) => return finish_bootstrap_failure(adapter, failure, args.json),
        }
    };
    let current = handoff::get_chain(db, &recovery.chain.id)
        .ok()
        .flatten()
        .unwrap_or_else(|| recovery.chain.clone());
    if args.json {
        println!(
            "{}",
            bounded_json(&json!({
                "id": current.id,
                "state": current.state.as_str(),
                "version": current.version,
                "generation": current.current_generation,
                "recovery": {
                    "started": true,
                    "reason_code": recovery.plan.as_str(),
                    "old_processes_absent": true,
                },
            }))
            .unwrap_or_else(|_| "{}".to_string())
        );
    } else {
        println!(
            "chain {} recovery={} state={} version={} generation={}",
            current.id,
            recovery.plan.as_str(),
            current.state,
            current.version,
            current.current_generation,
        );
    }
    let _ = std::io::stdout().flush();
    run_supervisor(
        db.path(),
        &recovery.chain.id,
        terminal.outer,
        control,
        adapter,
        active,
    )
}

pub fn cmd_chain(db: &HcomDb, args: &ChainArgs, ctx: Option<&CommandContext>) -> i32 {
    match &args.command {
        ChainCommand::Codex(args) => cmd_codex(db, args),
        ChainCommand::Status(args) => cmd_status(db, args, ctx),
        ChainCommand::Recover(args) => cmd_recover(db, args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[cfg(unix)]
    #[test]
    fn ready_codex_hooks_are_verified_without_rewriting_user_files() {
        use std::os::unix::fs::MetadataExt;

        let (_tmp, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        unsafe {
            std::env::set_var("HCOM_TEST_CODEX_CLI_VERSION", "codex-cli 0.145.0");
        }
        crate::hooks::codex::try_setup_codex_hooks(false).unwrap();
        let config_path = crate::hooks::codex::get_codex_config_path();
        let hooks_path = crate::hooks::codex::get_codex_hooks_path();
        let config_before = std::fs::metadata(&config_path).unwrap();
        let hooks_before = std::fs::metadata(&hooks_path).unwrap();

        ensure_codex_hooks_ready().unwrap();

        let config_after = std::fs::metadata(&config_path).unwrap();
        let hooks_after = std::fs::metadata(&hooks_path).unwrap();
        assert_eq!(config_after.ino(), config_before.ino());
        assert_eq!(hooks_after.ino(), hooks_before.ino());
        assert_eq!(
            config_after.modified().unwrap(),
            config_before.modified().unwrap()
        );
        assert_eq!(
            hooks_after.modified().unwrap(),
            hooks_before.modified().unwrap()
        );
    }

    #[test]
    fn parser_exposes_only_codex_status_and_recover() {
        let start = ChainArgs::try_parse_from([
            "chain",
            "codex",
            "--tag",
            "dev1",
            "--model",
            "gpt-5.5",
            "--reasoning",
            "max",
            "--sandbox",
            "workspace-write",
            "--approval",
            "on-request",
        ])
        .unwrap();
        let ChainCommand::Codex(start) = start.command else {
            panic!("expected chain codex");
        };
        assert_eq!(start.tag.as_deref(), Some("dev1"));
        assert!(matches!(start.reasoning, ChainReasoning::Max));
        assert!(ChainArgs::try_parse_from(["chain", "start"]).is_err());
        assert!(ChainArgs::try_parse_from(["chain", "resume"]).is_err());
        assert!(ChainArgs::try_parse_from(["chain", "fork"]).is_err());
        for unsupported in [
            "claude",
            "gemini",
            "opencode",
            "kilo",
            "pi",
            "omp",
            "cursor",
            "copilot",
            "kimi",
            "antigravity",
        ] {
            assert!(
                ChainArgs::try_parse_from(["chain", unsupported]).is_err(),
                "{unsupported} must remain unsupported by chain mode"
            );
        }
        assert!(validate_model("-invalid-leading-character").is_err());
        assert!(validate_model(&"x".repeat(MAX_MODEL_REF_BYTES + 1)).is_err());
        assert!(handoff::validate_chain_tag("dev1").is_ok());
        assert!(handoff::validate_chain_tag("bad tag").is_err());
        assert!(
            handoff::validate_chain_tag(&"x".repeat(crate::handoff::MAX_CHAIN_TAG_BYTES + 1))
                .is_err()
        );

        let status = ChainArgs::try_parse_from(["chain", "status", "tc-123", "--json"]).unwrap();
        assert!(matches!(status.command, ChainCommand::Status(_)));
        let recover =
            ChainArgs::try_parse_from(["chain", "recover", "tc-123", "--version", "7", "--json"])
                .unwrap();
        assert!(matches!(recover.command, ChainCommand::Recover(_)));
    }

    #[test]
    fn model_reference_rejects_config_or_argv_injection() {
        for invalid in [
            "",
            "--dangerously-bypass-approvals-and-sandbox",
            "model\nsandbox=danger-full-access",
            "model=value",
            &"x".repeat(MAX_MODEL_REF_BYTES + 1),
        ] {
            assert!(validate_model(invalid).is_err(), "{invalid:?}");
        }
        assert_eq!(validate_model("gpt-5.5/codex").unwrap(), "gpt-5.5/codex");
    }

    #[test]
    fn status_outputs_omit_private_identity_and_process_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&directory.path().join("hcom.db")).unwrap();
        let chain = TerminalChain {
            id: "tc-sample".to_string(),
            workspace: "/workspace".to_string(),
            tool: "codex".to_string(),
            tag: "dev1".to_string(),
            model_ref: "gpt-5.6-sol".to_string(),
            reasoning_ref: "max".to_string(),
            permission_policy_ref: "approval=never;sandbox=danger-full-access".to_string(),
            policy_ref: POLICY_REF.to_string(),
            supervisor_process_id: "secret-supervisor-process".to_string(),
            supervisor_process_birth_identity: "secret-supervisor-birth".to_string(),
            supervisor_pid: Some(41001),
            supervisor_pgid: Some(41001),
            outer_foreground_pgid: Some(41001),
            outer_tty_device: Some(7),
            outer_tty_inode: Some(11),
            current_generation: 1,
            state: ChainState::Active,
            version: 0,
            created_at: 1.0,
            updated_at: 1.0,
        };
        let json = bounded_json(&chain_json(&db, &chain)).unwrap();
        let human = chain_human(&db, &chain).unwrap();
        assert_eq!(
            fresh_start_command(&chain),
            "hcom chain codex --tag dev1 --model gpt-5.6-sol --reasoning max --sandbox danger-full-access --approval never"
        );
        assert!(json.contains("\"tag\":\"dev1\""));
        assert!(human.contains("tag=dev1"));
        assert!(json.len() <= crate::handoff::MAX_STATUS_JSON_BYTES);
        assert!(human.len() <= MAX_STATUS_HUMAN_BYTES);
        for secret in ["secret-supervisor-process", "secret-supervisor-birth"] {
            assert!(!json.contains(secret));
            assert!(!human.contains(secret));
        }
    }
}
