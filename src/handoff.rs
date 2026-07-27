//! Durable, typed control plane for same-terminal Codex handoffs.
//!
//! This module deliberately has no PTY, hook, launcher, signal, or process
//! control integration. The private Phase 3 Codex adapter and exact hooks call
//! these services, while every state transition remains deterministic and
//! durable.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use crate::db::HcomDb;
use crate::shared::time::now_epoch_f64;

pub const MAX_OPAQUE_ID_BYTES: usize = 64;
pub const MAX_INSTANCE_NAME_BYTES: usize = 128;
pub const MAX_PROCESS_ID_BYTES: usize = 128;
pub const MAX_IDENTITY_BYTES: usize = 256;
pub const MAX_WORKSPACE_BYTES: usize = 4096;
pub const MAX_MODEL_REF_BYTES: usize = 128;
pub const MAX_REASONING_REF_BYTES: usize = 64;
pub const MAX_POLICY_REF_BYTES: usize = 512;
pub const MAX_REVISION_BYTES: usize = 128;
pub const MAX_BRANCH_BYTES: usize = 1024;
pub const MAX_DIRTY_SUMMARY_BYTES: usize = 512;
pub const MAX_FAILURE_KIND_BYTES: usize = 64;
pub const MAX_FAILURE_REASON_BYTES: usize = 1024;
pub const MAX_HANDOFF_BUNDLE_BYTES: usize = 1024 * 1024;
pub const MAX_INSTRUCTION_FILE_BYTES: usize = 256 * 1024;
pub const MAX_INSTRUCTIONS_BYTES: usize = 1024 * 1024;
pub const MAX_INSTRUCTION_FILES: usize = 64;
pub const MAX_STATUS_JSON_BYTES: usize = 16 * 1024;
pub const MAX_STATUS_HUMAN_BYTES: usize = 16 * 1024;
pub const MAX_QUIESCE_ELAPSED_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainState {
    Active,
    Prepared,
    Committed,
    StopObserved,
    QuiescingSource,
    LaunchingTarget,
    AwaitingAcceptance,
    NeedsRecovery,
}

impl ChainState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Prepared => "prepared",
            Self::Committed => "committed",
            Self::StopObserved => "stop_observed",
            Self::QuiescingSource => "quiescing_source",
            Self::LaunchingTarget => "launching_target",
            Self::AwaitingAcceptance => "awaiting_acceptance",
            Self::NeedsRecovery => "needs_recovery",
        }
    }
}

impl fmt::Display for ChainState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ChainState {
    type Err = HandoffError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "prepared" => Ok(Self::Prepared),
            "committed" => Ok(Self::Committed),
            "stop_observed" => Ok(Self::StopObserved),
            "quiescing_source" => Ok(Self::QuiescingSource),
            "launching_target" => Ok(Self::LaunchingTarget),
            "awaiting_acceptance" => Ok(Self::AwaitingAcceptance),
            "needs_recovery" => Ok(Self::NeedsRecovery),
            _ => Err(HandoffError::Storage),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationState {
    Active,
    HandoffPrepared,
    HandoffCommitted,
    StopObserved,
    Quiescing,
    Retired,
    Reserved,
    Launching,
    AwaitingAcceptance,
    NeedsRecovery,
}

impl GenerationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::HandoffPrepared => "handoff_prepared",
            Self::HandoffCommitted => "handoff_committed",
            Self::StopObserved => "stop_observed",
            Self::Quiescing => "quiescing",
            Self::Retired => "retired",
            Self::Reserved => "reserved",
            Self::Launching => "launching",
            Self::AwaitingAcceptance => "awaiting_acceptance",
            Self::NeedsRecovery => "needs_recovery",
        }
    }
}

impl fmt::Display for GenerationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GenerationState {
    type Err = HandoffError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "handoff_prepared" => Ok(Self::HandoffPrepared),
            "handoff_committed" => Ok(Self::HandoffCommitted),
            "stop_observed" => Ok(Self::StopObserved),
            "quiescing" => Ok(Self::Quiescing),
            "retired" => Ok(Self::Retired),
            "reserved" => Ok(Self::Reserved),
            "launching" => Ok(Self::Launching),
            "awaiting_acceptance" => Ok(Self::AwaitingAcceptance),
            "needs_recovery" => Ok(Self::NeedsRecovery),
            _ => Err(HandoffError::Storage),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffState {
    Prepared,
    Committed,
    StopObserved,
    QuiescingSource,
    LaunchingTarget,
    AwaitingAcceptance,
    Accepted,
    Aborted,
    NeedsRecovery,
}

impl HandoffState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Committed => "committed",
            Self::StopObserved => "stop_observed",
            Self::QuiescingSource => "quiescing_source",
            Self::LaunchingTarget => "launching_target",
            Self::AwaitingAcceptance => "awaiting_acceptance",
            Self::Accepted => "accepted",
            Self::Aborted => "aborted",
            Self::NeedsRecovery => "needs_recovery",
        }
    }

    pub fn is_final(self) -> bool {
        matches!(self, Self::Accepted | Self::Aborted)
    }
}

impl fmt::Display for HandoffState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HandoffState {
    type Err = HandoffError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "committed" => Ok(Self::Committed),
            "stop_observed" => Ok(Self::StopObserved),
            "quiescing_source" => Ok(Self::QuiescingSource),
            "launching_target" => Ok(Self::LaunchingTarget),
            "awaiting_acceptance" => Ok(Self::AwaitingAcceptance),
            "accepted" => Ok(Self::Accepted),
            "aborted" => Ok(Self::Aborted),
            "needs_recovery" => Ok(Self::NeedsRecovery),
            _ => Err(HandoffError::Storage),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TerminalChain {
    pub id: String,
    pub workspace: String,
    pub tool: String,
    pub model_ref: String,
    pub reasoning_ref: String,
    pub permission_policy_ref: String,
    pub policy_ref: String,
    pub supervisor_process_id: String,
    pub supervisor_process_birth_identity: String,
    pub supervisor_pid: Option<i64>,
    pub supervisor_pgid: Option<i64>,
    pub outer_foreground_pgid: Option<i64>,
    pub outer_tty_device: Option<i64>,
    pub outer_tty_inode: Option<i64>,
    pub current_generation: i64,
    pub state: ChainState,
    pub version: i64,
    pub created_at: f64,
    pub updated_at: f64,
}

impl TerminalChain {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let state: String = row.get("state")?;
        let state = state.parse().map_err(sql_conversion_error)?;
        Ok(Self {
            id: row.get("id")?,
            workspace: row.get("workspace")?,
            tool: row.get("tool")?,
            model_ref: row.get("model_ref")?,
            reasoning_ref: row.get("reasoning_ref")?,
            permission_policy_ref: row.get("permission_policy_ref")?,
            policy_ref: row.get("policy_ref")?,
            supervisor_process_id: row.get("supervisor_process_id")?,
            supervisor_process_birth_identity: row.get("supervisor_process_birth_identity")?,
            supervisor_pid: row.get("supervisor_pid")?,
            supervisor_pgid: row.get("supervisor_pgid")?,
            outer_foreground_pgid: row.get("outer_foreground_pgid")?,
            outer_tty_device: row.get("outer_tty_device")?,
            outer_tty_inode: row.get("outer_tty_inode")?,
            current_generation: row.get("current_generation")?,
            state,
            version: row.get("version")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TerminalGeneration {
    pub chain_id: String,
    pub generation: i64,
    pub launch_nonce: String,
    pub wrapper_process_id: Option<String>,
    pub process_birth_identity: Option<String>,
    pub instance_name: Option<String>,
    pub hcom_session_id: Option<String>,
    pub native_session_id: Option<String>,
    pub state: GenerationState,
    pub version: i64,
    pub created_at: f64,
    pub updated_at: f64,
}

impl TerminalGeneration {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let state: String = row.get("state")?;
        let state = state.parse().map_err(sql_conversion_error)?;
        Ok(Self {
            chain_id: row.get("chain_id")?,
            generation: row.get("generation")?,
            launch_nonce: row.get("launch_nonce")?,
            wrapper_process_id: row.get("wrapper_process_id")?,
            process_birth_identity: row.get("process_birth_identity")?,
            instance_name: row.get("instance_name")?,
            hcom_session_id: row.get("hcom_session_id")?,
            native_session_id: row.get("native_session_id")?,
            state,
            version: row.get("version")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TerminalHandoff {
    pub id: String,
    pub chain_id: String,
    pub source_generation: i64,
    pub target_generation: i64,
    pub source_launch_nonce: String,
    pub source_instance_name: String,
    pub source_hcom_session_id: String,
    pub source_native_session_id: String,
    pub source_wrapper_process_id: String,
    pub source_process_birth_identity: String,
    pub bundle_event_id: i64,
    pub bundle_digest: String,
    pub bundle_size_bytes: i64,
    pub workspace: String,
    pub revision: String,
    pub branch: String,
    pub dirty_summary: String,
    pub policy_ref: String,
    pub state: HandoffState,
    pub version: i64,
    pub quiesce_token: Option<String>,
    pub quiesce_generation: Option<i64>,
    pub quiesce_native_session_id: Option<String>,
    pub quiesce_process_id: Option<String>,
    pub quiesce_process_birth_identity: Option<String>,
    pub quiesce_committed_version: Option<i64>,
    pub stop_observed_at: Option<f64>,
    pub stop_turn_id: Option<String>,
    pub sigterm_requested_wall_at: Option<f64>,
    pub sigterm_requested_monotonic_ns: Option<i64>,
    pub sigterm_request_result: String,
    pub child_exit_observed_wall_at: Option<f64>,
    pub child_exit_observed_monotonic_ns: Option<i64>,
    pub child_exit_code: Option<i64>,
    pub child_exit_signal: Option<i64>,
    pub sigterm_to_exit_ms: Option<i64>,
    pub delivery_exit_context: String,
    pub waitpid_reaped: Option<bool>,
    pub inject_cleanup_succeeded: Option<bool>,
    pub delivery_cleanup_succeeded: Option<bool>,
    pub pty_cleanup_succeeded: Option<bool>,
    pub screen_cleanup_succeeded: Option<bool>,
    pub write_queue_cleanup_succeeded: Option<bool>,
    pub cleanup_completed_at: Option<f64>,
    pub target_validation_token: Option<String>,
    pub target_instructions_digest: Option<String>,
    pub target_validated_at: Option<f64>,
    pub failure_kind: String,
    pub failure_reason: String,
    pub created_at: f64,
    pub updated_at: f64,
    pub committed_at: Option<f64>,
    pub accepted_at: Option<f64>,
}

impl TerminalHandoff {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let state: String = row.get("state")?;
        let state = state.parse().map_err(sql_conversion_error)?;
        Ok(Self {
            id: row.get("id")?,
            chain_id: row.get("chain_id")?,
            source_generation: row.get("source_generation")?,
            target_generation: row.get("target_generation")?,
            source_launch_nonce: row.get("source_launch_nonce")?,
            source_instance_name: row.get("source_instance_name")?,
            source_hcom_session_id: row.get("source_hcom_session_id")?,
            source_native_session_id: row.get("source_native_session_id")?,
            source_wrapper_process_id: row.get("source_wrapper_process_id")?,
            source_process_birth_identity: row.get("source_process_birth_identity")?,
            bundle_event_id: row.get("bundle_event_id")?,
            bundle_digest: row.get("bundle_digest")?,
            bundle_size_bytes: row.get("bundle_size_bytes")?,
            workspace: row.get("workspace")?,
            revision: row.get("revision")?,
            branch: row.get("branch")?,
            dirty_summary: row.get("dirty_summary")?,
            policy_ref: row.get("policy_ref")?,
            state,
            version: row.get("version")?,
            quiesce_token: row.get("quiesce_token")?,
            quiesce_generation: row.get("quiesce_generation")?,
            quiesce_native_session_id: row.get("quiesce_native_session_id")?,
            quiesce_process_id: row.get("quiesce_process_id")?,
            quiesce_process_birth_identity: row.get("quiesce_process_birth_identity")?,
            quiesce_committed_version: row.get("quiesce_committed_version")?,
            stop_observed_at: row.get("stop_observed_at")?,
            stop_turn_id: row.get("stop_turn_id")?,
            sigterm_requested_wall_at: row.get("sigterm_requested_wall_at")?,
            sigterm_requested_monotonic_ns: row.get("sigterm_requested_monotonic_ns")?,
            sigterm_request_result: row.get("sigterm_request_result")?,
            child_exit_observed_wall_at: row.get("child_exit_observed_wall_at")?,
            child_exit_observed_monotonic_ns: row.get("child_exit_observed_monotonic_ns")?,
            child_exit_code: row.get("child_exit_code")?,
            child_exit_signal: row.get("child_exit_signal")?,
            sigterm_to_exit_ms: row.get("sigterm_to_exit_ms")?,
            delivery_exit_context: row.get("delivery_exit_context")?,
            waitpid_reaped: row.get("waitpid_reaped")?,
            inject_cleanup_succeeded: row.get("inject_cleanup_succeeded")?,
            delivery_cleanup_succeeded: row.get("delivery_cleanup_succeeded")?,
            pty_cleanup_succeeded: row.get("pty_cleanup_succeeded")?,
            screen_cleanup_succeeded: row.get("screen_cleanup_succeeded")?,
            write_queue_cleanup_succeeded: row.get("write_queue_cleanup_succeeded")?,
            cleanup_completed_at: row.get("cleanup_completed_at")?,
            target_validation_token: row.get("target_validation_token")?,
            target_instructions_digest: row.get("target_instructions_digest")?,
            target_validated_at: row.get("target_validated_at")?,
            failure_kind: row.get("failure_kind")?,
            failure_reason: row.get("failure_reason")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
            committed_at: row.get("committed_at")?,
            accepted_at: row.get("accepted_at")?,
        })
    }
}

fn sql_conversion_error(error: HandoffError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

#[derive(Debug, Clone)]
pub struct HandoffActor {
    pub instance_name: String,
    pub hcom_session_id: String,
    pub native_session_id: Option<String>,
    pub process_id: String,
    pub process_birth_identity: String,
    pub generation: i64,
}

#[derive(Debug, Clone)]
pub struct SupervisorActor {
    pub process_id: String,
    pub process_birth_identity: String,
}

/// Opaque supervisor-owned metadata inherited by one generation wrapper.
///
/// The values identify an already materialized typed generation; they never
/// contain a bundle, task, argv snapshot, environment snapshot, or auth data.
#[derive(Debug, Clone)]
pub struct ManagedActorMarkers {
    pub chain_id: String,
    pub generation: i64,
    pub launch_nonce: String,
    pub process_birth_identity: String,
}

#[derive(Debug, Clone)]
pub struct ChainSpec {
    pub workspace: PathBuf,
    pub tool: String,
    pub model_ref: String,
    pub reasoning_ref: String,
    pub permission_policy_ref: String,
    pub policy_ref: String,
    pub supervisor_process_id: String,
    pub supervisor_process_birth_identity: String,
    pub supervisor_pid: i64,
    pub supervisor_pgid: i64,
    pub outer_foreground_pgid: i64,
    pub outer_tty_device: i64,
    pub outer_tty_inode: i64,
    pub launch_nonce: String,
}

#[derive(Debug, Clone)]
pub struct PublicChainReservation {
    pub chain: TerminalChain,
    pub generation: TerminalGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentSupervisorBinding {
    pub process_id: String,
    pub process_birth_identity: String,
    pub pid: i64,
    pub pgid: i64,
    pub outer_foreground_pgid: i64,
    pub outer_tty_device: i64,
    pub outer_tty_inode: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenerationProcessEvidence {
    pub chain_id: String,
    pub generation: i64,
    pub wrapper_pid: i64,
    pub wrapper_pgid: i64,
    pub wrapper_birth_identity: String,
    pub child_pid: i64,
    pub child_pgid: i64,
    pub child_birth_identity: String,
    pub materialized_at: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GenerationPrepareIntent {
    launch_nonce: String,
    supervisor_process_id: String,
    supervisor_process_birth_identity: String,
    control_object_kind: String,
    control_object_id: String,
    control_version: i64,
    generation_version: i64,
}

#[derive(Debug, Clone)]
pub struct TerminalOwnerEvidence {
    pub workspace: PathBuf,
    pub supervisor: SupervisorActor,
    pub supervisor_pid: i64,
    pub supervisor_pgid: i64,
    pub outer_foreground_pgid: i64,
    pub outer_tty_device: i64,
    pub outer_tty_inode: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPlanCode {
    RetryInitialGeneration,
    ContinueAfterSourceAbsence,
    RetryUnmaterializedTarget,
    ReplaceDeadTarget,
    ReplaceDeadAwaitingAcceptance,
    SourceDeadBeforeCommit,
    LiveProcessConflict,
    ProcessIdentityReused,
    AbsenceUnknown,
    UnsupportedRecoveryState,
}

impl RecoveryPlanCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetryInitialGeneration => "retry_initial_generation",
            Self::ContinueAfterSourceAbsence => "continue_after_source_absence",
            Self::RetryUnmaterializedTarget => "retry_unmaterialized_target",
            Self::ReplaceDeadTarget => "replace_dead_target",
            Self::ReplaceDeadAwaitingAcceptance => "replace_dead_awaiting_acceptance",
            Self::SourceDeadBeforeCommit => "source_dead_before_commit",
            Self::LiveProcessConflict => "old_process_still_live",
            Self::ProcessIdentityReused => "process_identity_reused",
            Self::AbsenceUnknown => "process_absence_unknown",
            Self::UnsupportedRecoveryState => "manual_intervention_required",
        }
    }

    pub fn permits_spawn(self) -> bool {
        matches!(
            self,
            Self::RetryInitialGeneration
                | Self::ContinueAfterSourceAbsence
                | Self::RetryUnmaterializedTarget
                | Self::ReplaceDeadTarget
                | Self::ReplaceDeadAwaitingAcceptance
        )
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryReservation {
    pub attempt_id: String,
    pub chain: TerminalChain,
    pub plan: RecoveryPlanCode,
    pub handoff_id: Option<String>,
    pub handoff_version: Option<i64>,
    pub generation: TerminalGeneration,
}

#[derive(Debug, Clone)]
pub enum RecoveryOutcome {
    Launch(Box<RecoveryReservation>),
    Manual {
        chain: Box<TerminalChain>,
        reason: RecoveryPlanCode,
    },
}

impl GenerationProcessEvidence {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            chain_id: row.get("chain_id")?,
            generation: row.get("generation")?,
            wrapper_pid: row.get("wrapper_pid")?,
            wrapper_pgid: row.get("wrapper_pgid")?,
            wrapper_birth_identity: row.get("wrapper_birth_identity")?,
            child_pid: row.get("child_pid")?,
            child_pgid: row.get("child_pgid")?,
            child_birth_identity: row.get("child_birth_identity")?,
            materialized_at: row.get("materialized_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct HandoffOutcome {
    pub handoff: TerminalHandoff,
    pub replayed: bool,
}

#[derive(Debug, Clone)]
pub struct ProjectInstruction {
    pub scope: String,
    pub path: String,
    pub content: String,
    pub digest: String,
}

#[derive(Debug, Clone)]
pub struct HandoffInspection {
    pub handoff: TerminalHandoff,
    pub bundle: serde_json::Value,
    pub instructions: Vec<ProjectInstruction>,
    pub instructions_digest: String,
    pub replayed: bool,
}

#[derive(Debug, Clone)]
pub struct GenerationOutcome {
    pub generation: TerminalGeneration,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorShutdownReason {
    Explicit,
    OuterHangup,
}

impl SupervisorShutdownReason {
    fn failure(self) -> (&'static str, &'static str) {
        match self {
            Self::Explicit => (
                "supervisor_shutdown",
                "foreground supervisor stopped by explicit local control",
            ),
            Self::OuterHangup => (
                "outer_hangup",
                "outer terminal hangup stopped the foreground chain",
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChainShutdownObservation {
    pub expected_chain_version: i64,
    pub expected_generation_version: i64,
    pub reason: SupervisorShutdownReason,
}

#[derive(Debug, Clone)]
pub struct StopObservation {
    pub expected_version: i64,
    pub quiesce_token: String,
    pub committed_version: i64,
    pub hook_native_session_id: String,
    pub launch_nonce: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigtermRequestResult {
    Sent,
    NotFound,
    PermissionDenied,
    Error,
}

impl SigtermRequestResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SigtermObservation {
    pub expected_version: i64,
    pub requested_wall_at: f64,
    pub requested_monotonic_ns: i64,
    pub result: SigtermRequestResult,
}

#[derive(Debug, Clone)]
pub struct TargetMaterialization {
    pub expected_version: i64,
    pub launch_nonce: String,
    pub instance_name: String,
    pub hcom_session_id: String,
    pub process_id: String,
    pub process_birth_identity: String,
    pub wrapper_pid: i64,
    pub wrapper_pgid: i64,
    pub child_pid: i64,
    pub child_pgid: i64,
    pub child_process_birth_identity: String,
}

#[derive(Debug, Clone)]
pub struct TargetFailureIdentity {
    pub instance_name: String,
    pub hcom_session_id: String,
    pub process_id: String,
    pub process_birth_identity: String,
}

#[derive(Debug, Clone)]
pub struct TargetLaunchFailure {
    pub expected_version: i64,
    pub launch_nonce: String,
    pub identity: Option<TargetFailureIdentity>,
    pub cleanup_completed: bool,
    pub failure_kind: String,
    pub failure_reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryExitContext {
    Closed,
    Killed,
}

impl DeliveryExitContext {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "exit:closed",
            Self::Killed => "exit:killed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChildExitEvidence {
    pub observed_wall_at: f64,
    pub observed_monotonic_ns: i64,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub delivery_context: DeliveryExitContext,
}

#[derive(Debug, Clone, Copy)]
pub struct ResourceCleanupEvidence {
    pub inject_succeeded: bool,
    pub delivery_succeeded: bool,
    pub pty_succeeded: bool,
    pub screen_succeeded: bool,
    pub write_queue_succeeded: bool,
}

impl ResourceCleanupEvidence {
    fn all_succeeded(self) -> bool {
        self.inject_succeeded
            && self.delivery_succeeded
            && self.pty_succeeded
            && self.screen_succeeded
            && self.write_queue_succeeded
    }
}

#[derive(Debug, Clone)]
pub struct CleanupObservation {
    pub expected_version: i64,
    pub exit: Option<ChildExitEvidence>,
    pub reaped: bool,
    pub resources: ResourceCleanupEvidence,
    pub failure_kind: String,
    pub failure_reason: String,
}

#[derive(Debug, thiserror::Error)]
pub enum HandoffError {
    #[error("not managed by a handoff chain")]
    NotManaged,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{message}")]
    TypedConflict {
        code: &'static str,
        message: &'static str,
    },
    #[error("database operation failed")]
    Storage,
}

impl HandoffError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Invalid(_) | Self::Storage => 1,
            Self::Conflict(_) | Self::TypedConflict { .. } => 2,
            Self::NotManaged => 3,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid_request",
            Self::Conflict(_) => "conflict",
            Self::TypedConflict { code, .. } => code,
            Self::NotManaged => "not_managed",
            Self::Storage => "storage_error",
        }
    }
}

fn typed_conflict(code: &'static str, message: &'static str) -> HandoffError {
    HandoffError::TypedConflict { code, message }
}

impl From<rusqlite::Error> for HandoffError {
    fn from(_value: rusqlite::Error) -> Self {
        Self::Storage
    }
}

fn validate_text(
    value: &str,
    field: &'static str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<String, HandoffError> {
    if (!allow_empty && value.is_empty()) || value.len() > max_bytes {
        return Err(HandoffError::Invalid(format!(
            "{field} must contain {}..={max_bytes} bytes",
            usize::from(!allow_empty)
        )));
    }
    if value
        .chars()
        .any(|character| character == '\0' || character.is_control())
    {
        return Err(HandoffError::Invalid(format!(
            "{field} contains unsupported control characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_opaque_id(value: &str, field: &'static str) -> Result<String, HandoffError> {
    let value = validate_text(value, field, MAX_OPAQUE_ID_BYTES, false)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
    {
        return Err(HandoffError::Invalid(format!(
            "{field} is not a valid opaque identifier"
        )));
    }
    Ok(value)
}

fn sanitize_reason(value: &str) -> Result<String, HandoffError> {
    let value = validate_text(value.trim(), "reason", MAX_FAILURE_REASON_BYTES, false)?;
    Ok(value)
}

fn validate_expected_version(expected_version: i64) -> Result<(), HandoffError> {
    if !(0..i64::MAX).contains(&expected_version) {
        return Err(HandoffError::Invalid(
            "expected version must be non-negative and incrementable".to_string(),
        ));
    }
    Ok(())
}

fn validate_observation_timestamp(value: f64, field: &'static str) -> Result<(), HandoffError> {
    if !value.is_finite() || value < 0.0 {
        return Err(HandoffError::Invalid(format!(
            "{field} must be a finite non-negative timestamp"
        )));
    }
    Ok(())
}

fn validate_monotonic_ns(value: i64, field: &'static str) -> Result<(), HandoffError> {
    if value < 0 {
        return Err(HandoffError::Invalid(format!(
            "{field} must be non-negative"
        )));
    }
    Ok(())
}

fn validate_child_exit(exit: &ChildExitEvidence) -> Result<(), HandoffError> {
    validate_observation_timestamp(exit.observed_wall_at, "child exit wall timestamp")?;
    validate_monotonic_ns(exit.observed_monotonic_ns, "child exit monotonic timestamp")?;
    if (exit.exit_code.is_some() as u8 + exit.exit_signal.is_some() as u8) != 1 {
        return Err(HandoffError::Invalid(
            "child exit evidence must contain exactly one exit code or signal".to_string(),
        ));
    }
    if exit
        .exit_signal
        .is_some_and(|signal| !(1..=64).contains(&signal))
    {
        return Err(HandoffError::Invalid(
            "child exit signal is outside the supported range".to_string(),
        ));
    }
    Ok(())
}

fn hash_parts(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.len().to_le_bytes());
        hasher.update(part);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hash_request(
    action: &str,
    object_id: &str,
    expected_version: i64,
    actor: &HandoffActor,
    payload: &[&[u8]],
) -> String {
    let expected_version = expected_version.to_string();
    let generation = actor.generation.to_string();
    let native_session = actor.native_session_id.as_deref().unwrap_or("");
    let mut parts: Vec<&[u8]> = vec![
        action.as_bytes(),
        object_id.as_bytes(),
        expected_version.as_bytes(),
        actor.instance_name.as_bytes(),
        actor.hcom_session_id.as_bytes(),
        native_session.as_bytes(),
        actor.process_id.as_bytes(),
        actor.process_birth_identity.as_bytes(),
        generation.as_bytes(),
    ];
    parts.extend_from_slice(payload);
    hash_parts(&parts)
}

fn validate_actor(actor: &HandoffActor) -> Result<(), HandoffError> {
    validate_text(
        &actor.instance_name,
        "instance identity",
        MAX_INSTANCE_NAME_BYTES,
        false,
    )?;
    validate_text(
        &actor.hcom_session_id,
        "hcom session identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    if let Some(native) = actor.native_session_id.as_deref() {
        validate_text(native, "native session identity", MAX_IDENTITY_BYTES, false)?;
    }
    validate_text(
        &actor.process_id,
        "process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    validate_text(
        &actor.process_birth_identity,
        "process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    if actor.generation < 1 {
        return Err(HandoffError::Invalid(
            "generation must be a positive integer".to_string(),
        ));
    }
    Ok(())
}

fn validate_supervisor_actor(actor: &SupervisorActor) -> Result<(), HandoffError> {
    validate_text(
        &actor.process_id,
        "supervisor process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    validate_text(
        &actor.process_birth_identity,
        "supervisor process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    Ok(())
}

fn load_chain(conn: &Connection, id: &str) -> Result<Option<TerminalChain>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_chains WHERE id = ?1",
        params![id],
        TerminalChain::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn load_generation(
    conn: &Connection,
    chain_id: &str,
    generation: i64,
) -> Result<Option<TerminalGeneration>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_generations
         WHERE chain_id = ?1 AND generation = ?2",
        params![chain_id, generation],
        TerminalGeneration::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn load_generation_process(
    conn: &Connection,
    chain_id: &str,
    generation: i64,
) -> Result<Option<GenerationProcessEvidence>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_generation_processes
         WHERE chain_id = ?1 AND generation = ?2",
        params![chain_id, generation],
        GenerationProcessEvidence::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn load_generation_prepare_intent(
    conn: &Connection,
    chain_id: &str,
    generation: i64,
) -> Result<Option<GenerationPrepareIntent>, HandoffError> {
    conn.query_row(
        "SELECT launch_nonce, supervisor_process_id,
                supervisor_process_birth_identity, control_object_kind,
                control_object_id, control_version, generation_version
         FROM terminal_generation_prepare_intents
         WHERE chain_id = ?1 AND generation = ?2",
        params![chain_id, generation],
        |row| {
            Ok(GenerationPrepareIntent {
                launch_nonce: row.get(0)?,
                supervisor_process_id: row.get(1)?,
                supervisor_process_birth_identity: row.get(2)?,
                control_object_kind: row.get(3)?,
                control_object_id: row.get(4)?,
                control_version: row.get(5)?,
                generation_version: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn load_handoff(conn: &Connection, id: &str) -> Result<Option<TerminalHandoff>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_handoffs WHERE id = ?1",
        params![id],
        TerminalHandoff::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn load_handoff_for_target_generation(
    conn: &Connection,
    chain_id: &str,
    generation: i64,
) -> Result<Option<TerminalHandoff>, HandoffError> {
    conn.query_row(
        "SELECT h.*
         FROM terminal_handoffs h
         WHERE h.chain_id = ?1
           AND h.state NOT IN ('accepted', 'aborted')
           AND (
               h.target_generation = ?2
               OR EXISTS(
                   SELECT 1 FROM terminal_recovery_attempts r
                   WHERE r.handoff_id = h.id AND r.target_generation = ?2
               )
           )",
        params![chain_id, generation],
        TerminalHandoff::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn effective_target_generation(
    conn: &Connection,
    handoff: &TerminalHandoff,
) -> Result<i64, HandoffError> {
    conn.query_row(
        "SELECT target_generation
         FROM terminal_recovery_attempts
         WHERE handoff_id = ?1 AND target_generation IS NOT NULL
         ORDER BY sequence DESC LIMIT 1",
        params![handoff.id],
        |row| row.get(0),
    )
    .optional()
    .map(|value| value.unwrap_or(handoff.target_generation))
    .map_err(Into::into)
}

pub fn effective_handoff_target_generation(
    db: &HcomDb,
    handoff_id: &str,
) -> Result<i64, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    let handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::NotManaged)?;
    effective_target_generation(db.conn(), &handoff)
}

fn is_initial_protocol_generation(
    conn: &Connection,
    chain: &TerminalChain,
    generation: &TerminalGeneration,
) -> Result<bool, HandoffError> {
    let public_claim: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM terminal_chain_claims
             WHERE chain_id = ?1 AND state = 'active'
         )",
        params![chain.id],
        |row| row.get(0),
    )?;
    if !public_claim || get_open_handoff_for_chain_tx(conn, &chain.id)?.is_some() {
        return Ok(false);
    }
    if generation.generation == 1 {
        return Ok(true);
    }
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM terminal_recovery_attempts
             WHERE chain_id = ?1 AND target_generation = ?2
               AND handoff_id IS NULL
               AND state IN ('intent', 'authorized', 'materialized')
         )",
        params![chain.id, generation.generation],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

pub fn get_chain(db: &HcomDb, id: &str) -> Result<Option<TerminalChain>, HandoffError> {
    let id = validate_opaque_id(id, "chain ID")?;
    load_chain(db.conn(), &id)
}

pub fn get_generation(
    db: &HcomDb,
    chain_id: &str,
    generation: i64,
) -> Result<Option<TerminalGeneration>, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    if generation < 1 {
        return Err(HandoffError::Invalid(
            "generation must be a positive integer".to_string(),
        ));
    }
    load_generation(db.conn(), &chain_id, generation)
}

pub fn get_generation_process(
    db: &HcomDb,
    chain_id: &str,
    generation: i64,
) -> Result<Option<GenerationProcessEvidence>, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    if generation < 1 {
        return Err(HandoffError::Invalid(
            "generation must be a positive integer".to_string(),
        ));
    }
    load_generation_process(db.conn(), &chain_id, generation)
}

fn load_current_supervisor_binding(
    conn: &Connection,
    chain: &TerminalChain,
) -> Result<CurrentSupervisorBinding, HandoffError> {
    if let Some(binding) = conn
        .query_row(
            "SELECT supervisor_process_id, supervisor_process_birth_identity,
                    supervisor_pid, supervisor_pgid, outer_foreground_pgid,
                    outer_tty_device, outer_tty_inode
             FROM terminal_recovery_attempts
             WHERE chain_id = ?1 AND state != 'manual'
             ORDER BY sequence DESC LIMIT 1",
            params![chain.id],
            |row| {
                Ok(CurrentSupervisorBinding {
                    process_id: row.get(0)?,
                    process_birth_identity: row.get(1)?,
                    pid: row.get(2)?,
                    pgid: row.get(3)?,
                    outer_foreground_pgid: row.get(4)?,
                    outer_tty_device: row.get(5)?,
                    outer_tty_inode: row.get(6)?,
                })
            },
        )
        .optional()?
    {
        return Ok(binding);
    }
    Ok(CurrentSupervisorBinding {
        process_id: chain.supervisor_process_id.clone(),
        process_birth_identity: chain.supervisor_process_birth_identity.clone(),
        pid: chain.supervisor_pid.ok_or(HandoffError::Storage)?,
        pgid: chain.supervisor_pgid.ok_or(HandoffError::Storage)?,
        outer_foreground_pgid: chain.outer_foreground_pgid.ok_or(HandoffError::Storage)?,
        outer_tty_device: chain.outer_tty_device.ok_or(HandoffError::Storage)?,
        outer_tty_inode: chain.outer_tty_inode.ok_or(HandoffError::Storage)?,
    })
}

pub fn current_supervisor_binding(
    db: &HcomDb,
    chain_id: &str,
) -> Result<CurrentSupervisorBinding, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    let chain = load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::NotManaged)?;
    load_current_supervisor_binding(db.conn(), &chain)
}

pub fn get_handoff(db: &HcomDb, id: &str) -> Result<Option<TerminalHandoff>, HandoffError> {
    let id = validate_opaque_id(id, "handoff ID")?;
    load_handoff(db.conn(), &id)
}

pub fn get_open_handoff_for_chain(
    db: &HcomDb,
    chain_id: &str,
) -> Result<Option<TerminalHandoff>, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    get_open_handoff_for_chain_tx(db.conn(), &chain_id)
}

fn get_open_handoff_for_chain_tx(
    conn: &Connection,
    chain_id: &str,
) -> Result<Option<TerminalHandoff>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_handoffs
         WHERE chain_id = ?1 AND state NOT IN ('accepted', 'aborted')",
        params![chain_id],
        TerminalHandoff::from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn live_binding_matches(conn: &Connection, actor: &HandoffActor) -> Result<bool, HandoffError> {
    let binding = conn
        .query_row(
            "SELECT session_id, instance_name
             FROM process_bindings WHERE process_id = ?1",
            params![actor.process_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?;
    if binding
        != Some((
            Some(actor.hcom_session_id.clone()),
            Some(actor.instance_name.clone()),
        ))
    {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM instances
             WHERE name = ?1 AND session_id = ?2 AND tool = 'codex'
               AND COALESCE(parent_name, '') = ''
               AND COALESCE(origin_device_id, '') = ''
         )",
        params![actor.instance_name, actor.hcom_session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn generation_matches_actor(
    generation: &TerminalGeneration,
    actor: &HandoffActor,
    require_native: bool,
) -> bool {
    generation.generation == actor.generation
        && generation.wrapper_process_id.as_deref() == Some(actor.process_id.as_str())
        && generation.process_birth_identity.as_deref()
            == Some(actor.process_birth_identity.as_str())
        && generation.instance_name.as_deref() == Some(actor.instance_name.as_str())
        && generation.hcom_session_id.as_deref() == Some(actor.hcom_session_id.as_str())
        && (!require_native
            || generation.native_session_id.as_deref() == actor.native_session_id.as_deref())
}

fn authorize_generation(
    conn: &Connection,
    chain: &TerminalChain,
    generation: &TerminalGeneration,
    actor: &HandoffActor,
    require_native: bool,
    require_live: bool,
    require_current: bool,
) -> Result<(), HandoffError> {
    validate_actor(actor)?;
    if (require_current && chain.current_generation != actor.generation)
        || generation.chain_id != chain.id
        || !generation_matches_actor(generation, actor, require_native)
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller identity does not match current generation".to_string(),
        ));
    }
    if require_live && !live_binding_matches(conn, actor)? {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT exact live process/session binding is missing".to_string(),
        ));
    }
    Ok(())
}

fn find_actor_chain(
    conn: &Connection,
    actor: &HandoffActor,
) -> Result<Option<(TerminalChain, TerminalGeneration)>, HandoffError> {
    validate_actor(actor)?;
    let generation = conn
        .query_row(
            "SELECT g.*
             FROM terminal_generations g
             JOIN terminal_chains c
               ON c.id = g.chain_id AND c.current_generation = g.generation
             WHERE g.wrapper_process_id = ?1",
            params![actor.process_id],
            TerminalGeneration::from_row,
        )
        .optional()?;
    let Some(generation) = generation else {
        return Ok(None);
    };
    let Some(chain) = load_chain(conn, &generation.chain_id)? else {
        return Err(HandoffError::Storage);
    };
    authorize_generation(conn, &chain, &generation, actor, false, true, true)?;
    Ok(Some((chain, generation)))
}

pub fn current_chain_for_actor(
    db: &HcomDb,
    actor: &HandoffActor,
) -> Result<Option<(TerminalChain, TerminalGeneration)>, HandoffError> {
    find_actor_chain(db.conn(), actor)
}

/// Resolve CLI identity from exact live hcom identity plus immutable typed
/// generation metadata. In particular, the native session is read only from
/// `terminal_generations.native_session_id`; callers cannot supply or derive it
/// from the mutable instance/session lifecycle rows.
pub fn resolve_managed_actor(
    db: &HcomDb,
    instance_name: &str,
    hcom_session_id: &str,
    process_id: &str,
    markers: &ManagedActorMarkers,
) -> Result<HandoffActor, HandoffError> {
    let chain_id =
        validate_opaque_id(&markers.chain_id, "chain ID").map_err(|_| HandoffError::NotManaged)?;
    if markers.generation < 1 {
        return Err(HandoffError::NotManaged);
    }
    let launch_nonce = validate_opaque_id(&markers.launch_nonce, "launch nonce")
        .map_err(|_| HandoffError::NotManaged)?;
    let process_birth_identity = validate_text(
        &markers.process_birth_identity,
        "process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(
        instance_name,
        "instance identity",
        MAX_INSTANCE_NAME_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(
        hcom_session_id,
        "hcom session identity",
        MAX_IDENTITY_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(process_id, "process identity", MAX_PROCESS_ID_BYTES, false)
        .map_err(|_| HandoffError::NotManaged)?;

    let chain = load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let generation = load_generation(db.conn(), &chain_id, markers.generation)?
        .ok_or(HandoffError::NotManaged)?;
    let native_session_id = generation
        .native_session_id
        .clone()
        .ok_or(HandoffError::NotManaged)?;
    let actor = HandoffActor {
        instance_name: instance_name.to_string(),
        hcom_session_id: hcom_session_id.to_string(),
        native_session_id: Some(native_session_id),
        process_id: process_id.to_string(),
        process_birth_identity,
        generation: markers.generation,
    };
    if chain.current_generation != markers.generation
        || generation.launch_nonce != launch_nonce
        || !generation_matches_actor(&generation, &actor, true)
        || !live_binding_matches(db.conn(), &actor)?
    {
        return Err(HandoffError::NotManaged);
    }
    Ok(actor)
}

/// Resolve the exact materialized generation before its native Codex session
/// has been pinned. This is intentionally narrower than the ordinary CLI
/// resolver and is only suitable for a verified SessionStart hook.
pub fn resolve_managed_actor_for_session_start(
    db: &HcomDb,
    instance_name: &str,
    hcom_session_id: &str,
    process_id: &str,
    markers: &ManagedActorMarkers,
) -> Result<HandoffActor, HandoffError> {
    let chain_id =
        validate_opaque_id(&markers.chain_id, "chain ID").map_err(|_| HandoffError::NotManaged)?;
    if markers.generation < 1 {
        return Err(HandoffError::NotManaged);
    }
    let launch_nonce = validate_opaque_id(&markers.launch_nonce, "launch nonce")
        .map_err(|_| HandoffError::NotManaged)?;
    let process_birth_identity = validate_text(
        &markers.process_birth_identity,
        "process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(
        instance_name,
        "instance identity",
        MAX_INSTANCE_NAME_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(
        hcom_session_id,
        "hcom session identity",
        MAX_IDENTITY_BYTES,
        false,
    )
    .map_err(|_| HandoffError::NotManaged)?;
    validate_text(process_id, "process identity", MAX_PROCESS_ID_BYTES, false)
        .map_err(|_| HandoffError::NotManaged)?;

    let chain = load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let generation = load_generation(db.conn(), &chain_id, markers.generation)?
        .ok_or(HandoffError::NotManaged)?;
    let actor = HandoffActor {
        instance_name: instance_name.to_string(),
        hcom_session_id: hcom_session_id.to_string(),
        native_session_id: None,
        process_id: process_id.to_string(),
        process_birth_identity,
        generation: markers.generation,
    };
    if chain.current_generation != markers.generation
        || generation.launch_nonce != launch_nonce
        || !generation_matches_actor(&generation, &actor, false)
        || !live_binding_matches(db.conn(), &actor)?
    {
        return Err(HandoffError::NotManaged);
    }
    Ok(actor)
}

fn canonical_workspace(path: &Path) -> Result<String, HandoffError> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|_| HandoffError::Invalid("workspace cannot be canonicalized".to_string()))?;
    let value = canonical
        .to_str()
        .ok_or_else(|| HandoffError::Invalid("workspace must be valid UTF-8".to_string()))?;
    validate_text(value, "workspace", MAX_WORKSPACE_BYTES, false)
}

#[derive(Debug, Clone)]
struct WorkspaceSnapshot {
    workspace: String,
    revision: String,
    branch: String,
    dirty_summary: String,
}

fn git_output(workspace: &str, args: &[&str]) -> Result<Vec<u8>, HandoffError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .map_err(|_| HandoffError::Invalid("git metadata is unavailable".to_string()))?;
    if !output.status.success() {
        return Err(HandoffError::Invalid(
            "workspace git metadata cannot be resolved".to_string(),
        ));
    }
    Ok(output.stdout)
}

fn clean_git_text(
    bytes: Vec<u8>,
    field: &'static str,
    max_bytes: usize,
) -> Result<String, HandoffError> {
    let value = String::from_utf8(bytes)
        .map_err(|_| HandoffError::Invalid(format!("{field} must be valid UTF-8")))?;
    validate_text(
        value.trim_end_matches(['\r', '\n']),
        field,
        max_bytes,
        false,
    )
}

fn snapshot_workspace(path: &Path) -> Result<WorkspaceSnapshot, HandoffError> {
    let workspace = canonical_workspace(path)?;
    let revision = clean_git_text(
        git_output(&workspace, &["rev-parse", "--verify", "HEAD"])?,
        "revision",
        MAX_REVISION_BYTES,
    )?;
    let branch_output = Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .output()
        .map_err(|_| HandoffError::Invalid("git metadata is unavailable".to_string()))?;
    let branch = if branch_output.status.success() {
        clean_git_text(branch_output.stdout, "branch", MAX_BRANCH_BYTES)?
    } else if branch_output.status.code() == Some(1) {
        "(detached)".to_string()
    } else {
        return Err(HandoffError::Invalid(
            "workspace git branch cannot be resolved".to_string(),
        ));
    };
    let status = git_output(
        &workspace,
        &["status", "--porcelain=v1", "-z", "--untracked-files=normal"],
    )?;
    if status.len() > MAX_HANDOFF_BUNDLE_BYTES {
        return Err(HandoffError::Invalid(
            "workspace status exceeds the handoff metadata bound".to_string(),
        ));
    }
    let mut staged = 0usize;
    let mut unstaged = 0usize;
    let mut untracked = 0usize;
    let mut conflicted = 0usize;
    let mut records = status
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        if record.len() < 3 || record[2] != b' ' {
            return Err(HandoffError::Invalid(
                "workspace git status output is malformed".to_string(),
            ));
        }
        let x = record[0];
        let y = record[1];
        if x == b'?' && y == b'?' {
            untracked += 1;
            continue;
        }
        if matches!(
            (x, y),
            (b'D', b'D')
                | (b'A', b'U')
                | (b'U', b'D')
                | (b'U', b'A')
                | (b'D', b'U')
                | (b'A', b'A')
                | (b'U', b'U')
        ) {
            conflicted += 1;
            continue;
        }
        if x != b' ' {
            staged += 1;
        }
        if y != b' ' {
            unstaged += 1;
        }
        // Porcelain v1 -z represents a rename/copy with a second NUL-delimited
        // path record. It contains no XY prefix and must never be interpreted
        // as another status entry (nor persisted in the snapshot).
        if matches!(x, b'R' | b'C') || matches!(y, b'R' | b'C') {
            let Some(path_record) = records.next() else {
                return Err(HandoffError::Invalid(
                    "workspace git rename status is incomplete".to_string(),
                ));
            };
            if path_record.is_empty() {
                return Err(HandoffError::Invalid(
                    "workspace git rename status is incomplete".to_string(),
                ));
            }
        }
    }
    let dirty_summary = format!(
        "staged={staged},unstaged={unstaged},untracked={untracked},conflicted={conflicted}"
    );
    validate_text(
        &dirty_summary,
        "dirty summary",
        MAX_DIRTY_SUMMARY_BYTES,
        false,
    )?;
    Ok(WorkspaceSnapshot {
        workspace,
        revision,
        branch,
        dirty_summary,
    })
}

#[derive(Debug)]
struct BundleSnapshot {
    event_id: i64,
    digest: String,
    size_bytes: i64,
    value: serde_json::Value,
}

fn load_bundle_snapshot(
    conn: &Connection,
    event_id: i64,
    source_instance: &str,
) -> Result<BundleSnapshot, HandoffError> {
    if event_id <= 0 {
        return Err(HandoffError::Invalid(
            "bundle event ID must be an exact positive integer".to_string(),
        ));
    }
    let row = conn
        .query_row(
            "SELECT instance, data FROM events
             WHERE id = ?1 AND type = 'bundle'",
            params![event_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((instance, raw)) = row else {
        return Err(HandoffError::Invalid(
            "bundle event is missing or has the wrong type".to_string(),
        ));
    };
    if instance != source_instance || raw.len() > MAX_HANDOFF_BUNDLE_BYTES {
        return Err(HandoffError::Invalid(
            "bundle event ownership or size validation failed".to_string(),
        ));
    }
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|_| HandoffError::Invalid("bundle event payload is not valid JSON".to_string()))?;
    let created_by = value
        .get("created_by")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if created_by != source_instance
        || value
            .get("bundle_id")
            .and_then(|value| value.as_str())
            .is_none_or(|value| value.is_empty())
    {
        return Err(HandoffError::Invalid(
            "bundle event ownership metadata is invalid".to_string(),
        ));
    }
    let canonical = serde_json::to_vec(&value)
        .map_err(|_| HandoffError::Invalid("bundle event cannot be serialized".to_string()))?;
    if canonical.len() > MAX_HANDOFF_BUNDLE_BYTES {
        return Err(HandoffError::Invalid(
            "bundle event exceeds the handoff byte bound".to_string(),
        ));
    }
    Ok(BundleSnapshot {
        event_id,
        digest: hash_parts(&[&canonical]),
        size_bytes: canonical.len() as i64,
        value,
    })
}

fn verify_pinned_bundle(
    conn: &Connection,
    handoff: &TerminalHandoff,
) -> Result<BundleSnapshot, HandoffError> {
    let current =
        load_bundle_snapshot(conn, handoff.bundle_event_id, &handoff.source_instance_name)
            .map_err(|_| {
                HandoffError::Conflict(
                    "HANDOFF_CONFLICT pinned bundle event is no longer valid".to_string(),
                )
            })?;
    if current.digest != handoff.bundle_digest || current.size_bytes != handoff.bundle_size_bytes {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT pinned bundle event changed after prepare".to_string(),
        ));
    }
    Ok(current)
}

fn load_current_instructions(
    workspace: &str,
    bundle: &serde_json::Value,
) -> Result<(Vec<ProjectInstruction>, String), HandoffError> {
    #[cfg(test)]
    let _env_read = crate::hooks::test_helpers::process_env_read();
    let workspace = PathBuf::from(workspace);
    let canonical_workspace = std::fs::canonicalize(&workspace).map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT target workspace is no longer available".to_string(),
        )
    })?;
    if canonical_workspace != workspace {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target workspace is no longer canonical".to_string(),
        ));
    }

    let mut candidates: Vec<(String, PathBuf, PathBuf)> = Vec::new();
    if let Some(codex_home) = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| crate::runtime_env::user_home().map(|home| home.join(".codex")))
        && let Some(path) = preferred_instruction_file(&codex_home)
    {
        candidates.push(("global".to_string(), codex_home, path));
    }

    let mut project_directories = vec![workspace.clone()];
    if let Some(files) = bundle.pointer("/refs/files") {
        let files = files.as_array().ok_or_else(|| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT bundle refs.files is not a bounded path list".to_string(),
            )
        })?;
        if files.len() > MAX_INSTRUCTION_FILES {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT bundle references too many instruction scopes".to_string(),
            ));
        }
        for value in files {
            let raw = value.as_str().ok_or_else(|| {
                HandoffError::Conflict(
                    "HANDOFF_CONFLICT bundle refs.files contains a non-path value".to_string(),
                )
            })?;
            let relative = bounded_relative_path(raw)?;
            let referenced = workspace.join(relative);
            let directory = if referenced.is_dir() {
                referenced
            } else {
                referenced
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| workspace.clone())
            };
            let relative_directory = directory.strip_prefix(&workspace).map_err(|_| {
                HandoffError::Conflict(
                    "HANDOFF_CONFLICT bundle file escapes the pinned workspace".to_string(),
                )
            })?;
            let mut current = workspace.clone();
            for component in relative_directory.components() {
                current.push(component);
                project_directories.push(current.clone());
            }
        }
    }
    project_directories.sort();
    project_directories.dedup();
    for directory in project_directories {
        if let Some(path) = preferred_instruction_file(&directory) {
            candidates.push(("workspace".to_string(), workspace.clone(), path));
        }
    }

    let mut instructions = Vec::with_capacity(candidates.len());
    let mut total = 0usize;
    for (scope, root, path) in candidates {
        if instructions.len() >= MAX_INSTRUCTION_FILES {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT current project instructions exceed the file bound".to_string(),
            ));
        }
        let canonical_root = std::fs::canonicalize(&root).map_err(|_| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT instruction root is no longer available".to_string(),
            )
        })?;
        let canonical_path = std::fs::canonicalize(&path).map_err(|_| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT project instruction file changed during validation".to_string(),
            )
        })?;
        if !canonical_path.starts_with(&canonical_root) {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT project instruction file escapes its scope".to_string(),
            ));
        }
        let metadata = std::fs::metadata(&canonical_path).map_err(|_| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT project instruction metadata is unavailable".to_string(),
            )
        })?;
        if !metadata.is_file() || metadata.len() as usize > MAX_INSTRUCTION_FILE_BYTES {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT project instruction file is not bounded".to_string(),
            ));
        }
        let content = std::fs::read_to_string(&canonical_path).map_err(|_| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT project instruction file is not valid UTF-8".to_string(),
            )
        })?;
        total = total.checked_add(content.len()).ok_or_else(|| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT current project instructions exceed the byte bound".to_string(),
            )
        })?;
        if total > MAX_INSTRUCTIONS_BYTES {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT current project instructions exceed the byte bound".to_string(),
            ));
        }
        let display_path = canonical_path
            .strip_prefix(&canonical_root)
            .ok()
            .and_then(Path::to_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                HandoffError::Conflict(
                    "HANDOFF_CONFLICT project instruction path is not representable".to_string(),
                )
            })?
            .to_string();
        let digest = hash_parts(&[
            scope.as_bytes(),
            display_path.as_bytes(),
            content.as_bytes(),
        ]);
        instructions.push(ProjectInstruction {
            scope,
            path: display_path,
            content,
            digest,
        });
    }
    instructions.sort_by(|left, right| (&left.scope, &left.path).cmp(&(&right.scope, &right.path)));
    let instructions_digest = {
        let mut digest_parts: Vec<&[u8]> = Vec::with_capacity(instructions.len() * 4);
        for instruction in &instructions {
            digest_parts.push(instruction.scope.as_bytes());
            digest_parts.push(instruction.path.as_bytes());
            digest_parts.push(instruction.digest.as_bytes());
            digest_parts.push(instruction.content.as_bytes());
        }
        hash_parts(&digest_parts)
    };
    Ok((instructions, instructions_digest))
}

fn preferred_instruction_file(directory: &Path) -> Option<PathBuf> {
    for name in ["AGENTS.override.md", "AGENTS.md"] {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn bounded_relative_path(raw: &str) -> Result<PathBuf, HandoffError> {
    let raw = validate_text(raw, "bundle file reference", MAX_WORKSPACE_BYTES, false)?;
    let path = Path::new(&raw);
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => relative.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(HandoffError::Conflict(
                    "HANDOFF_CONFLICT bundle file reference is outside the workspace".to_string(),
                ));
            }
        }
    }
    if relative.as_os_str().is_empty() {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT bundle file reference is empty".to_string(),
        ));
    }
    Ok(relative)
}

fn generate_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..24]
    )
}

fn generation_object_id(chain_id: &str, generation: i64) -> String {
    format!("{chain_id}:{generation}")
}

#[allow(clippy::too_many_arguments)]
fn insert_audit(
    tx: &Transaction<'_>,
    chain_id: &str,
    object_kind: &str,
    object_id: &str,
    from_version: i64,
    from_state: Option<&str>,
    to_state: &str,
    actor: &HandoffActor,
    actor_role: &str,
    action: &str,
    request_hash: &str,
    now: f64,
) -> Result<(), HandoffError> {
    tx.execute(
        "INSERT INTO terminal_transition_audit (
             chain_id, object_kind, object_id, from_version, to_version,
             from_state, to_state, actor_instance_name, actor_hcom_session_id,
             actor_process_id, actor_process_birth_identity, actor_generation,
             actor_role, action, request_hash, created_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16
         )",
        params![
            chain_id,
            object_kind,
            object_id,
            from_version,
            from_version + 1,
            from_state,
            to_state,
            actor.instance_name,
            actor.hcom_session_id,
            actor.process_id,
            actor.process_birth_identity,
            actor.generation,
            actor_role,
            action,
            request_hash,
            now,
        ],
    )?;
    Ok(())
}

fn transition_is_replay(
    conn: &Connection,
    object: (&str, &str),
    expected_version: i64,
    current_version: i64,
    current_state: &str,
    action: &str,
    request_hash: &str,
) -> Result<bool, HandoffError> {
    let (object_kind, object_id) = object;
    if current_version != expected_version + 1 {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM terminal_transition_audit
             WHERE object_kind = ?1 AND object_id = ?2
               AND from_version = ?3 AND to_version = ?4
               AND to_state = ?5 AND action = ?6 AND request_hash = ?7
         )",
        params![
            object_kind,
            object_id,
            expected_version,
            current_version,
            current_state,
            action,
            request_hash
        ],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn conflict(
    id: &str,
    expected_version: i64,
    actual_state: &str,
    actual_version: i64,
) -> HandoffError {
    HandoffError::Conflict(format!(
        "HANDOFF_CONFLICT {id} expected_version={expected_version} state={actual_state} version={actual_version}"
    ))
}

#[allow(clippy::too_many_arguments)]
fn update_chain_state(
    tx: &Transaction<'_>,
    chain: &mut TerminalChain,
    state: ChainState,
    current_generation: i64,
    actor: &HandoffActor,
    role: &str,
    action: &str,
    request_hash: &str,
    now: f64,
) -> Result<(), HandoffError> {
    let from_state = chain.state;
    let from_version = chain.version;
    let updated = tx.execute(
        "UPDATE terminal_chains
         SET state = ?1, current_generation = ?2, version = ?3, updated_at = ?4
         WHERE id = ?5 AND state = ?6 AND version = ?7",
        params![
            state.as_str(),
            current_generation,
            from_version + 1,
            now,
            chain.id,
            from_state.as_str(),
            from_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &chain.id,
            from_version,
            from_state.as_str(),
            from_version,
        ));
    }
    insert_audit(
        tx,
        &chain.id,
        "chain",
        &chain.id,
        from_version,
        Some(from_state.as_str()),
        state.as_str(),
        actor,
        role,
        action,
        request_hash,
        now,
    )?;
    chain.state = state;
    chain.current_generation = current_generation;
    chain.version += 1;
    chain.updated_at = now;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_generation_state(
    tx: &Transaction<'_>,
    generation: &mut TerminalGeneration,
    state: GenerationState,
    actor: &HandoffActor,
    role: &str,
    action: &str,
    request_hash: &str,
    now: f64,
) -> Result<(), HandoffError> {
    let from_state = generation.state;
    let from_version = generation.version;
    let updated = tx.execute(
        "UPDATE terminal_generations
         SET state = ?1, version = ?2, updated_at = ?3
         WHERE chain_id = ?4 AND generation = ?5
           AND state = ?6 AND version = ?7",
        params![
            state.as_str(),
            from_version + 1,
            now,
            generation.chain_id,
            generation.generation,
            from_state.as_str(),
            from_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &generation_object_id(&generation.chain_id, generation.generation),
            from_version,
            from_state.as_str(),
            from_version,
        ));
    }
    insert_audit(
        tx,
        &generation.chain_id,
        "generation",
        &generation_object_id(&generation.chain_id, generation.generation),
        from_version,
        Some(from_state.as_str()),
        state.as_str(),
        actor,
        role,
        action,
        request_hash,
        now,
    )?;
    generation.state = state;
    generation.version += 1;
    generation.updated_at = now;
    Ok(())
}

pub fn create_chain(
    db: &HcomDb,
    actor: &HandoffActor,
    spec: &ChainSpec,
) -> Result<TerminalChain, HandoffError> {
    create_chain_with_id(db, actor, spec, &generate_id("tc"))
}

/// Reserve the only public foreground chain before the adapter opens a PTY or
/// forks either wrapper process. The public claim indexes make workspace and
/// outer-TTY acquisition single-winner across concurrent callers.
pub fn create_public_chain_reservation(
    db: &HcomDb,
    spec: &ChainSpec,
) -> Result<PublicChainReservation, HandoffError> {
    if spec.tool != "codex" {
        return Err(HandoffError::Invalid(
            "same-terminal handoff chains only support codex".to_string(),
        ));
    }
    let workspace = canonical_workspace(&spec.workspace)?;
    let model_ref = validate_text(
        &spec.model_ref,
        "model reference",
        MAX_MODEL_REF_BYTES,
        false,
    )?;
    let reasoning_ref = validate_text(
        &spec.reasoning_ref,
        "reasoning reference",
        MAX_REASONING_REF_BYTES,
        false,
    )?;
    let permission_policy_ref = validate_text(
        &spec.permission_policy_ref,
        "permission policy reference",
        MAX_POLICY_REF_BYTES,
        false,
    )?;
    let policy_ref = validate_text(
        &spec.policy_ref,
        "policy reference",
        MAX_POLICY_REF_BYTES,
        false,
    )?;
    let supervisor_process_id = validate_text(
        &spec.supervisor_process_id,
        "supervisor process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    let supervisor_process_birth_identity = validate_text(
        &spec.supervisor_process_birth_identity,
        "supervisor process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    if spec.supervisor_pid <= 1
        || spec.supervisor_pgid <= 0
        || spec.outer_foreground_pgid <= 0
        || spec.outer_tty_device <= 0
        || spec.outer_tty_inode <= 0
        || spec.supervisor_pgid != spec.outer_foreground_pgid
    {
        return Err(HandoffError::Invalid(
            "supervisor must have exact foreground PID/PGID/TTY evidence".to_string(),
        ));
    }
    let launch_nonce = validate_opaque_id(&spec.launch_nonce, "launch nonce")?;
    let chain_id = generate_id("tc");
    let actor = HandoffActor {
        instance_name: "chain-supervisor".to_string(),
        hcom_session_id: chain_id.clone(),
        native_session_id: None,
        process_id: supervisor_process_id.clone(),
        process_birth_identity: supervisor_process_birth_identity.clone(),
        generation: 1,
    };
    let request_hash = hash_request(
        "reserve_public_chain",
        &chain_id,
        -1,
        &actor,
        &[
            workspace.as_bytes(),
            model_ref.as_bytes(),
            reasoning_ref.as_bytes(),
            permission_policy_ref.as_bytes(),
            policy_ref.as_bytes(),
            launch_nonce.as_bytes(),
        ],
    );
    let now = now_epoch_f64();
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO terminal_chains (
             id, workspace, tool, model_ref, reasoning_ref,
             permission_policy_ref, policy_ref, supervisor_process_id,
             supervisor_process_birth_identity, supervisor_pid,
             supervisor_pgid, outer_foreground_pgid, outer_tty_device,
             outer_tty_inode, current_generation, state, version,
             created_at, updated_at
         ) VALUES (
             ?1, ?2, 'codex', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
             ?12, ?13, 1, 'launching_target', 0, ?14, ?14
         )",
        params![
            chain_id,
            workspace,
            model_ref,
            reasoning_ref,
            permission_policy_ref,
            policy_ref,
            supervisor_process_id,
            supervisor_process_birth_identity,
            spec.supervisor_pid,
            spec.supervisor_pgid,
            spec.outer_foreground_pgid,
            spec.outer_tty_device,
            spec.outer_tty_inode,
            now,
        ],
    )?;
    tx.execute(
        "INSERT INTO terminal_generations (
             chain_id, generation, launch_nonce, state, version,
             created_at, updated_at
         ) VALUES (?1, 1, ?2, 'reserved', 0, ?3, ?3)",
        params![chain_id, launch_nonce, now],
    )?;
    if tx
        .execute(
            "INSERT INTO terminal_chain_claims (
                 chain_id, workspace, outer_tty_device, outer_tty_inode,
                 state, version, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, 'active', 0, ?5, ?5)",
            params![
                chain_id,
                workspace,
                spec.outer_tty_device,
                spec.outer_tty_inode,
                now,
            ],
        )
        .is_err()
    {
        return Err(HandoffError::Conflict(
            "CHAIN_START_CONFLICT an active public chain already owns this workspace or terminal"
                .to_string(),
        ));
    }
    insert_audit(
        &tx,
        &chain_id,
        "chain",
        &chain_id,
        -1,
        None,
        ChainState::LaunchingTarget.as_str(),
        &actor,
        "supervisor",
        "reserve_public_chain",
        &request_hash,
        now,
    )?;
    insert_audit(
        &tx,
        &chain_id,
        "generation",
        &generation_object_id(&chain_id, 1),
        -1,
        None,
        GenerationState::Reserved.as_str(),
        &actor,
        "supervisor",
        "reserve_public_generation",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    Ok(PublicChainReservation {
        chain: load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::Storage)?,
        generation: load_generation(db.conn(), &chain_id, 1)?.ok_or(HandoffError::Storage)?,
    })
}

/// Allocate the opaque identity consumed by the private Phase 3 factory before
/// its initial Codex is released from the bootstrap gate.
#[cfg(test)]
pub(crate) fn allocate_chain_id() -> String {
    generate_id("tc")
}

/// Private create variant used only while the foreground factory already owns
/// a gated initial wrapper. There is deliberately no CLI/router path for a
/// caller-supplied chain identity.
pub(crate) fn create_chain_with_id(
    db: &HcomDb,
    actor: &HandoffActor,
    spec: &ChainSpec,
    requested_chain_id: &str,
) -> Result<TerminalChain, HandoffError> {
    validate_actor(actor)?;
    if actor.generation != 1 {
        return Err(HandoffError::Invalid(
            "a new chain must start at generation 1".to_string(),
        ));
    }
    if !live_binding_matches(db.conn(), actor)? {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT exact live process/session binding is missing".to_string(),
        ));
    }
    if spec.tool != "codex" {
        return Err(HandoffError::Invalid(
            "same-terminal handoff chains only support codex".to_string(),
        ));
    }
    let workspace = canonical_workspace(&spec.workspace)?;
    let model_ref = validate_text(
        &spec.model_ref,
        "model reference",
        MAX_MODEL_REF_BYTES,
        false,
    )?;
    let reasoning_ref = validate_text(
        &spec.reasoning_ref,
        "reasoning reference",
        MAX_REASONING_REF_BYTES,
        false,
    )?;
    let permission_policy_ref = validate_text(
        &spec.permission_policy_ref,
        "permission policy reference",
        MAX_POLICY_REF_BYTES,
        false,
    )?;
    let policy_ref = validate_text(
        &spec.policy_ref,
        "policy reference",
        MAX_POLICY_REF_BYTES,
        false,
    )?;
    let supervisor_process_id = validate_text(
        &spec.supervisor_process_id,
        "supervisor process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    let supervisor_process_birth_identity = validate_text(
        &spec.supervisor_process_birth_identity,
        "supervisor process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    if spec.supervisor_pid <= 0
        || spec.supervisor_pgid <= 0
        || spec.outer_foreground_pgid <= 0
        || spec.outer_tty_device <= 0
        || spec.outer_tty_inode <= 0
        || spec.supervisor_pgid != spec.outer_foreground_pgid
    {
        return Err(HandoffError::Invalid(
            "supervisor must have positive PID/PGID/TTY evidence and own the outer foreground process group"
                .to_string(),
        ));
    }
    let launch_nonce = validate_opaque_id(&spec.launch_nonce, "launch nonce")?;
    let supervisor_pid = spec.supervisor_pid.to_string();
    let supervisor_pgid = spec.supervisor_pgid.to_string();
    let outer_foreground_pgid = spec.outer_foreground_pgid.to_string();
    let outer_tty_device = spec.outer_tty_device.to_string();
    let outer_tty_inode = spec.outer_tty_inode.to_string();
    let chain_id = validate_opaque_id(requested_chain_id, "chain ID")?;
    let request_hash = hash_request(
        "create_chain",
        &chain_id,
        -1,
        actor,
        &[
            workspace.as_bytes(),
            spec.tool.as_bytes(),
            model_ref.as_bytes(),
            reasoning_ref.as_bytes(),
            permission_policy_ref.as_bytes(),
            policy_ref.as_bytes(),
            supervisor_process_id.as_bytes(),
            supervisor_process_birth_identity.as_bytes(),
            supervisor_pid.as_bytes(),
            supervisor_pgid.as_bytes(),
            outer_foreground_pgid.as_bytes(),
            outer_tty_device.as_bytes(),
            outer_tty_inode.as_bytes(),
            launch_nonce.as_bytes(),
        ],
    );
    let now = now_epoch_f64();
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO terminal_chains (
             id, workspace, tool, model_ref, reasoning_ref,
             permission_policy_ref, policy_ref, supervisor_process_id,
             supervisor_process_birth_identity, supervisor_pid,
             supervisor_pgid, outer_foreground_pgid, outer_tty_device,
             outer_tty_inode, current_generation, state, version,
             created_at, updated_at
         ) VALUES (
             ?1, ?2, 'codex', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
             ?12, ?13, 1, 'active', 0, ?14, ?14
         )",
        params![
            chain_id,
            workspace,
            model_ref,
            reasoning_ref,
            permission_policy_ref,
            policy_ref,
            supervisor_process_id,
            supervisor_process_birth_identity,
            spec.supervisor_pid,
            spec.supervisor_pgid,
            spec.outer_foreground_pgid,
            spec.outer_tty_device,
            spec.outer_tty_inode,
            now
        ],
    )?;
    tx.execute(
        "INSERT INTO terminal_generations (
             chain_id, generation, launch_nonce, wrapper_process_id,
             process_birth_identity, instance_name, hcom_session_id,
             native_session_id, state, version, created_at, updated_at
         ) VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', 0, ?8, ?8)",
        params![
            chain_id,
            launch_nonce,
            actor.process_id,
            actor.process_birth_identity,
            actor.instance_name,
            actor.hcom_session_id,
            actor.native_session_id,
            now
        ],
    )?;
    insert_audit(
        &tx,
        &chain_id,
        "chain",
        &chain_id,
        -1,
        None,
        ChainState::Active.as_str(),
        actor,
        "supervisor",
        "create_chain",
        &request_hash,
        now,
    )?;
    insert_audit(
        &tx,
        &chain_id,
        "generation",
        &generation_object_id(&chain_id, 1),
        -1,
        None,
        GenerationState::Active.as_str(),
        actor,
        "supervisor",
        "create_generation",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::Storage)
}

fn validate_process_materialization(
    materialization: &TargetMaterialization,
    supervisor: &CurrentSupervisorBinding,
) -> Result<(), HandoffError> {
    if materialization.wrapper_pid <= 1
        || materialization.wrapper_pgid <= 0
        || materialization.child_pid <= 1
        || materialization.child_pgid <= 1
        || materialization.wrapper_pid == materialization.child_pid
        || materialization.wrapper_pgid != supervisor.pgid
        || materialization.child_pid != materialization.child_pgid
        || materialization.child_pgid == supervisor.pgid
    {
        return Err(HandoffError::Invalid(
            "generation process topology does not match the foreground chain".to_string(),
        ));
    }
    validate_text(
        &materialization.child_process_birth_identity,
        "child process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    Ok(())
}

fn process_evidence_matches_materialization(
    evidence: &GenerationProcessEvidence,
    materialization: &TargetMaterialization,
) -> bool {
    evidence.wrapper_pid == materialization.wrapper_pid
        && evidence.wrapper_pgid == materialization.wrapper_pgid
        && evidence.wrapper_birth_identity == materialization.process_birth_identity
        && evidence.child_pid == materialization.child_pid
        && evidence.child_pgid == materialization.child_pgid
        && evidence.child_birth_identity == materialization.child_process_birth_identity
}

fn insert_generation_process(
    tx: &Transaction<'_>,
    chain_id: &str,
    generation: i64,
    materialization: &TargetMaterialization,
    now: f64,
) -> Result<(), HandoffError> {
    tx.execute(
        "INSERT INTO terminal_generation_processes (
             chain_id, generation, wrapper_pid, wrapper_pgid,
             wrapper_birth_identity, child_pid, child_pgid,
             child_birth_identity, materialized_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            chain_id,
            generation,
            materialization.wrapper_pid,
            materialization.wrapper_pgid,
            materialization.process_birth_identity,
            materialization.child_pid,
            materialization.child_pgid,
            materialization.child_process_birth_identity,
            now,
        ],
    )
    .map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT immutable generation process evidence already exists".to_string(),
        )
    })?;
    Ok(())
}

/// Record the irreversible boundary immediately before the adapter may fork a
/// wrapper. An absent process row is safe evidence of "never materialized"
/// only when this append-only intent is also absent.
pub fn begin_generation_prepare(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    chain_id: &str,
    generation_number: i64,
    expected_control_version: i64,
    launch_nonce: &str,
) -> Result<(), HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(expected_control_version)?;
    if generation_number < 1 {
        return Err(HandoffError::Invalid(
            "generation must be a positive integer".to_string(),
        ));
    }
    let launch_nonce = validate_opaque_id(launch_nonce, "launch nonce")?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let generation =
        load_generation(&tx, &chain_id, generation_number)?.ok_or(HandoffError::NotManaged)?;
    let binding = load_current_supervisor_binding(&tx, &chain)?;
    if binding.process_id != supervisor.process_id
        || binding.process_birth_identity != supervisor.process_birth_identity
        || chain.current_generation != generation_number
        || generation.launch_nonce != launch_nonce
        || generation.wrapper_process_id.is_some()
        || generation.process_birth_identity.is_some()
        || generation.instance_name.is_some()
        || generation.hcom_session_id.is_some()
        || generation.native_session_id.is_some()
        || load_generation_process(&tx, &chain_id, generation_number)?.is_some()
        || load_generation_prepare_intent(&tx, &chain_id, generation_number)?.is_some()
    {
        return Err(typed_conflict(
            "prepare_intent_conflict",
            "generation process preparation is not an unused exact reservation",
        ));
    }

    let handoff = get_open_handoff_for_chain_tx(&tx, &chain_id)?;
    let (control_kind, control_id, control_version) = if let Some(handoff) = handoff {
        if handoff.state != HandoffState::LaunchingTarget
            || handoff.version != expected_control_version
            || chain.state != ChainState::LaunchingTarget
            || generation.state != GenerationState::Launching
            || effective_target_generation(&tx, &handoff)? != generation_number
        {
            return Err(typed_conflict(
                "wrong_expected_version_or_state",
                "handoff state or expected version does not permit target preparation",
            ));
        }
        ("handoff", handoff.id, handoff.version)
    } else {
        if chain.state != ChainState::LaunchingTarget
            || chain.version != expected_control_version
            || generation.state != GenerationState::Reserved
            || !is_initial_protocol_generation(&tx, &chain, &generation)?
        {
            return Err(typed_conflict(
                "wrong_expected_version_or_state",
                "chain state or expected version does not permit initial preparation",
            ));
        }
        ("chain", chain.id.clone(), chain.version)
    };

    let recovery_state = tx
        .query_row(
            "SELECT state FROM terminal_recovery_attempts
             WHERE chain_id = ?1 AND target_generation = ?2
             ORDER BY sequence DESC LIMIT 1",
            params![chain_id, generation_number],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if recovery_state
        .as_deref()
        .is_some_and(|state| state != "authorized")
    {
        return Err(typed_conflict(
            "recovery_intent_changed",
            "recovery must be revalidated before target preparation",
        ));
    }

    let now = now_epoch_f64();
    tx.execute(
        "INSERT INTO terminal_generation_prepare_intents (
             chain_id, generation, launch_nonce, supervisor_process_id,
             supervisor_process_birth_identity, control_object_kind,
             control_object_id, control_version, generation_version,
             started_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            chain_id,
            generation_number,
            launch_nonce,
            supervisor.process_id,
            supervisor.process_birth_identity,
            control_kind,
            control_id,
            control_version,
            generation.version,
            now,
        ],
    )
    .map_err(|_| {
        typed_conflict(
            "prepare_intent_conflict",
            "generation process preparation was already authorized",
        )
    })?;
    tx.commit()?;
    Ok(())
}

/// Bind a gated public source or initial-recovery wrapper before its child is
/// released. The chain remains non-active until the exact SessionStart hook
/// pins the native identity.
pub fn materialize_initial_generation(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    chain_id: &str,
    expected_chain_version: i64,
    materialization: &TargetMaterialization,
) -> Result<GenerationOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(expected_chain_version)?;
    validate_expected_version(materialization.expected_version)?;
    let launch_nonce = validate_opaque_id(&materialization.launch_nonce, "launch nonce")?;
    let instance_name = validate_text(
        &materialization.instance_name,
        "instance identity",
        MAX_INSTANCE_NAME_BYTES,
        false,
    )?;
    let hcom_session_id = validate_text(
        &materialization.hcom_session_id,
        "hcom session identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    let process_id = validate_text(
        &materialization.process_id,
        "process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    let process_birth_identity = validate_text(
        &materialization.process_birth_identity,
        "process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let supervisor_binding = load_current_supervisor_binding(&tx, &chain)?;
    if supervisor_binding.process_id != supervisor.process_id
        || supervisor_binding.process_birth_identity != supervisor.process_birth_identity
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT current recovery supervisor identity changed".to_string(),
        ));
    }
    validate_process_materialization(materialization, &supervisor_binding)?;
    let mut generation =
        load_generation(&tx, &chain_id, chain.current_generation)?.ok_or(HandoffError::Storage)?;
    let prepare_intent = load_generation_prepare_intent(&tx, &chain_id, generation.generation)?
        .ok_or_else(|| {
            typed_conflict(
                "prepare_intent_required",
                "initial process materialization requires a durable prepare intent",
            )
        })?;
    if chain.state != ChainState::LaunchingTarget
        || chain.version != expected_chain_version
        || generation.state != GenerationState::Reserved
        || generation.version != materialization.expected_version
        || generation.launch_nonce != launch_nonce
        || generation.wrapper_process_id.is_some()
        || get_open_handoff_for_chain_tx(&tx, &chain_id)?.is_some()
        || prepare_intent.launch_nonce != launch_nonce
        || prepare_intent.supervisor_process_id != supervisor.process_id
        || prepare_intent.supervisor_process_birth_identity != supervisor.process_birth_identity
        || prepare_intent.control_object_kind != "chain"
        || prepare_intent.control_object_id != chain_id
        || prepare_intent.control_version != expected_chain_version
        || prepare_intent.generation_version != generation.version
    {
        return Err(conflict(
            &chain_id,
            expected_chain_version,
            chain.state.as_str(),
            chain.version,
        ));
    }
    let now = now_epoch_f64();
    tx.execute(
        "INSERT INTO instances (
             name, session_id, status, tool, created_at, parent_name,
             origin_device_id, launch_context
         ) VALUES (?1, ?2, 'launching', 'codex', ?3, '', '', ?4)",
        params![
            instance_name,
            hcom_session_id,
            now,
            serde_json::json!({
                "chain_id": chain_id,
                "generation": generation.generation,
                "launch_nonce": launch_nonce,
            })
            .to_string(),
        ],
    )
    .map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT exact initial instance materialization already exists".to_string(),
        )
    })?;
    tx.execute(
        "INSERT INTO process_bindings (
             process_id, session_id, instance_name, updated_at
         ) VALUES (?1, ?2, ?3, ?4)",
        params![process_id, hcom_session_id, instance_name, now],
    )
    .map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT exact initial process materialization already exists".to_string(),
        )
    })?;
    let from_version = generation.version;
    let updated = tx.execute(
        "UPDATE terminal_generations
         SET wrapper_process_id = ?1, process_birth_identity = ?2,
             instance_name = ?3, hcom_session_id = ?4, state = 'launching',
             version = ?5, updated_at = ?6
         WHERE chain_id = ?7 AND generation = ?8
           AND state = 'reserved' AND version = ?9
           AND wrapper_process_id IS NULL
           AND process_birth_identity IS NULL
           AND instance_name IS NULL AND hcom_session_id IS NULL
           AND native_session_id IS NULL AND launch_nonce = ?10",
        params![
            process_id,
            process_birth_identity,
            instance_name,
            hcom_session_id,
            from_version + 1,
            now,
            chain_id,
            generation.generation,
            from_version,
            launch_nonce,
        ],
    )?;
    if updated != 1 {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT initial reservation changed during materialization".to_string(),
        ));
    }
    insert_generation_process(&tx, &chain_id, generation.generation, materialization, now)?;
    let audit_actor = HandoffActor {
        instance_name: instance_name.clone(),
        hcom_session_id: hcom_session_id.clone(),
        native_session_id: None,
        process_id: supervisor.process_id.clone(),
        process_birth_identity: supervisor.process_birth_identity.clone(),
        generation: generation.generation,
    };
    let request_hash = hash_request(
        "materialize_initial",
        &generation_object_id(&chain_id, generation.generation),
        from_version,
        &audit_actor,
        &[
            launch_nonce.as_bytes(),
            process_id.as_bytes(),
            process_birth_identity.as_bytes(),
            materialization.wrapper_pid.to_string().as_bytes(),
            materialization.wrapper_pgid.to_string().as_bytes(),
            materialization.child_pid.to_string().as_bytes(),
            materialization.child_pgid.to_string().as_bytes(),
            materialization.child_process_birth_identity.as_bytes(),
        ],
    );
    insert_audit(
        &tx,
        &chain_id,
        "generation",
        &generation_object_id(&chain_id, generation.generation),
        from_version,
        Some(GenerationState::Reserved.as_str()),
        GenerationState::Launching.as_str(),
        &audit_actor,
        "supervisor",
        "materialize_initial",
        &request_hash,
        now,
    )?;
    let recovery_authorized = tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'materialized', version = version + 1, updated_at = ?1
         WHERE chain_id = ?2 AND target_generation = ?3
           AND handoff_id IS NULL AND state = 'authorized'",
        params![now, chain_id, generation.generation],
    )?;
    if generation.generation > 1 && recovery_authorized != 1 {
        return Err(typed_conflict(
            "recovery_intent_changed",
            "initial recovery was not revalidated before process materialization",
        ));
    }
    tx.commit()?;
    generation = load_generation(db.conn(), &chain_id, generation.generation)?
        .ok_or(HandoffError::Storage)?;
    Ok(GenerationOutcome {
        generation,
        replayed: false,
    })
}

/// Persist fail-closed initial-generation intent before aborting or shutting
/// down any process that escaped the private bootstrap gate. Recovery never
/// turns this into normal waitpid/resource-cleanup evidence.
pub fn fail_initial_generation(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    chain_id: &str,
    expected_chain_version: i64,
    expected_generation_version: i64,
    failure_kind: &str,
    failure_reason: &str,
) -> Result<GenerationOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(expected_chain_version)?;
    validate_expected_version(expected_generation_version)?;
    let failure_kind = validate_text(failure_kind, "failure kind", MAX_FAILURE_KIND_BYTES, false)?;
    let failure_reason = validate_text(
        failure_reason,
        "failure reason",
        MAX_FAILURE_REASON_BYTES,
        false,
    )?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let mut chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let binding = load_current_supervisor_binding(&tx, &chain)?;
    if binding.process_id != supervisor.process_id
        || binding.process_birth_identity != supervisor.process_birth_identity
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT current supervisor identity changed".to_string(),
        ));
    }
    let mut generation =
        load_generation(&tx, &chain_id, chain.current_generation)?.ok_or(HandoffError::Storage)?;
    if chain.state != ChainState::LaunchingTarget
        || chain.version != expected_chain_version
        || generation.version != expected_generation_version
        || !matches!(
            generation.state,
            GenerationState::Reserved | GenerationState::Launching
        )
        || get_open_handoff_for_chain_tx(&tx, &chain_id)?.is_some()
    {
        return Err(typed_conflict(
            "wrong_expected_version_or_state",
            "initial chain state or expected version changed",
        ));
    }
    let actor = HandoffActor {
        instance_name: "chain-supervisor".to_string(),
        hcom_session_id: chain_id.clone(),
        native_session_id: None,
        process_id: supervisor.process_id.clone(),
        process_birth_identity: supervisor.process_birth_identity.clone(),
        generation: generation.generation,
    };
    let request_hash = hash_request(
        "fail_initial_generation",
        &generation_object_id(&chain_id, generation.generation),
        expected_generation_version,
        &actor,
        &[failure_kind.as_bytes(), failure_reason.as_bytes()],
    );
    let now = now_epoch_f64();
    update_generation_state(
        &tx,
        &mut generation,
        GenerationState::NeedsRecovery,
        &actor,
        "supervisor",
        "fail_initial_generation",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::NeedsRecovery,
        current_generation,
        &actor,
        "supervisor",
        "fail_initial_generation",
        &request_hash,
        now,
    )?;
    tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'failed', version = version + 1, updated_at = ?1
         WHERE chain_id = ?2 AND target_generation = ?3
           AND state IN ('intent', 'authorized', 'materialized')",
        params![now, chain_id, generation.generation],
    )?;
    tx.commit()?;
    Ok(GenerationOutcome {
        generation,
        replayed: false,
    })
}

pub fn pin_native_session(
    db: &HcomDb,
    chain_id: &str,
    actor: &HandoffActor,
    expected_version: i64,
    native_session_id: &str,
) -> Result<GenerationOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_actor(actor)?;
    if actor.native_session_id.is_some() {
        return Err(HandoffError::Invalid(
            "SessionStart actor must not supply a native session identity".to_string(),
        ));
    }
    validate_expected_version(expected_version)?;
    let native_session_id = validate_text(
        native_session_id,
        "native session identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    let request_hash = hash_request(
        "pin_native_session",
        &generation_object_id(&chain_id, actor.generation),
        expected_version,
        actor,
        &[native_session_id.as_bytes()],
    );
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let mut generation =
        load_generation(&tx, &chain_id, actor.generation)?.ok_or(HandoffError::NotManaged)?;
    authorize_generation(&tx, &chain, &generation, actor, false, true, true)?;

    if generation.native_session_id.as_deref() == Some(native_session_id.as_str())
        && (generation.version == expected_version
            || transition_is_replay(
                &tx,
                (
                    "generation",
                    &generation_object_id(&chain_id, actor.generation),
                ),
                expected_version,
                generation.version,
                generation.state.as_str(),
                "pin_native_session",
                &request_hash,
            )?)
    {
        tx.commit()?;
        return Ok(GenerationOutcome {
            generation,
            replayed: true,
        });
    }
    if generation.version != expected_version {
        return Err(conflict(
            &generation_object_id(&chain_id, actor.generation),
            expected_version,
            generation.state.as_str(),
            generation.version,
        ));
    }
    if generation.native_session_id.is_some() {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT native session is already pinned".to_string(),
        ));
    }
    let valid_initial = actor.generation == 1
        && generation.state == GenerationState::Active
        && chain.state == ChainState::Active;
    let valid_starting_initial = generation.state == GenerationState::Launching
        && chain.state == ChainState::LaunchingTarget
        && is_initial_protocol_generation(&tx, &chain, &generation)?;
    let valid_target = generation.state == GenerationState::Launching
        && chain.state == ChainState::LaunchingTarget
        && load_handoff_for_target_generation(&tx, &chain_id, actor.generation)?
            .is_some_and(|handoff| handoff.state == HandoffState::LaunchingTarget);
    if !valid_initial && !valid_starting_initial && !valid_target {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT SessionStart does not match an active initial or launching target generation"
                .to_string(),
        ));
    }
    let historical_collision: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM terminal_generations
             WHERE chain_id = ?1 AND generation != ?2
               AND native_session_id = ?3
         )",
        params![chain_id, actor.generation, native_session_id],
        |row| row.get(0),
    )?;
    if historical_collision {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT native session is not fresh within the chain".to_string(),
        ));
    }
    let now = now_epoch_f64();
    let next_generation_state = if valid_starting_initial {
        GenerationState::Active
    } else {
        generation.state
    };
    let updated = tx.execute(
        "UPDATE terminal_generations
         SET native_session_id = ?1, state = ?2, version = ?3, updated_at = ?4
         WHERE chain_id = ?5 AND generation = ?6
           AND native_session_id IS NULL AND state = ?7 AND version = ?8",
        params![
            native_session_id,
            next_generation_state.as_str(),
            expected_version + 1,
            now,
            chain_id,
            actor.generation,
            generation.state.as_str(),
            expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &generation_object_id(&chain_id, actor.generation),
            expected_version,
            generation.state.as_str(),
            generation.version,
        ));
    }
    insert_audit(
        &tx,
        &chain_id,
        "generation",
        &generation_object_id(&chain_id, actor.generation),
        expected_version,
        Some(generation.state.as_str()),
        next_generation_state.as_str(),
        actor,
        if valid_target { "target" } else { "source" },
        "pin_native_session",
        &request_hash,
        now,
    )?;
    if valid_starting_initial {
        let mut starting_chain = chain;
        let current_generation = starting_chain.current_generation;
        update_chain_state(
            &tx,
            &mut starting_chain,
            ChainState::Active,
            current_generation,
            actor,
            "supervisor",
            "pin_initial_native_session",
            &request_hash,
            now,
        )?;
        tx.execute(
            "UPDATE terminal_recovery_attempts
             SET state = 'active', version = version + 1, updated_at = ?1
             WHERE chain_id = ?2 AND target_generation = ?3
               AND state IN ('intent', 'authorized', 'materialized')",
            params![now, chain_id, actor.generation],
        )?;
    }
    generation.native_session_id = Some(native_session_id);
    generation.state = next_generation_state;
    generation.version += 1;
    generation.updated_at = now;
    tx.commit()?;
    Ok(GenerationOutcome {
        generation,
        replayed: false,
    })
}

/// Durably fail a foreground chain closed before the supervisor starts local
/// SIGHUP or explicit-shutdown cleanup.
///
/// This intent transition deliberately precedes process action. A crash after
/// it can leave only `NeedsRecovery`, never a durable `Active` row whose
/// generation has already disappeared. The current live binding is required
/// for the first transition; an exact replay does not depend on that binding
/// surviving cleanup.
pub fn begin_chain_shutdown(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    chain_id: &str,
    actor: &HandoffActor,
    observation: &ChainShutdownObservation,
) -> Result<GenerationOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_actor(actor)?;
    validate_expected_version(observation.expected_chain_version)?;
    validate_expected_version(observation.expected_generation_version)?;
    let (failure_kind, failure_reason) = observation.reason.failure();

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let mut chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
    let mut generation =
        load_generation(&tx, &chain_id, actor.generation)?.ok_or(HandoffError::NotManaged)?;
    let supervisor_binding = load_current_supervisor_binding(&tx, &chain)?;
    if chain.current_generation != actor.generation
        || supervisor_binding.process_id != supervisor.process_id
        || supervisor_binding.process_birth_identity != supervisor.process_birth_identity
        || !generation_matches_actor(&generation, actor, true)
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT shutdown actor does not match the immutable current generation"
                .to_string(),
        ));
    }
    let mut handoff = tx
        .query_row(
            "SELECT * FROM terminal_handoffs
             WHERE chain_id = ?1 AND state NOT IN ('accepted', 'aborted')
             LIMIT 1",
            [&chain_id],
            TerminalHandoff::from_row,
        )
        .optional()?;
    let audit_actor = HandoffActor {
        instance_name: generation
            .instance_name
            .clone()
            .ok_or(HandoffError::Storage)?,
        hcom_session_id: generation
            .hcom_session_id
            .clone()
            .ok_or(HandoffError::Storage)?,
        native_session_id: generation.native_session_id.clone(),
        process_id: supervisor.process_id.clone(),
        process_birth_identity: supervisor.process_birth_identity.clone(),
        generation: generation.generation,
    };
    let expected_generation_version = observation.expected_generation_version.to_string();
    let handoff_id = handoff
        .as_ref()
        .map(|value| value.id.as_str())
        .unwrap_or("");
    let request_hash = hash_request(
        "begin_chain_shutdown",
        &chain_id,
        observation.expected_chain_version,
        &audit_actor,
        &[
            expected_generation_version.as_bytes(),
            actor.process_id.as_bytes(),
            actor.process_birth_identity.as_bytes(),
            generation.launch_nonce.as_bytes(),
            handoff_id.as_bytes(),
            failure_kind.as_bytes(),
        ],
    );
    let chain_replay = transition_is_replay(
        &tx,
        ("chain", &chain_id),
        observation.expected_chain_version,
        chain.version,
        chain.state.as_str(),
        "begin_chain_shutdown",
        &request_hash,
    )?;
    if chain_replay {
        let generation_id = generation_object_id(&chain_id, actor.generation);
        let generation_replay = transition_is_replay(
            &tx,
            ("generation", &generation_id),
            observation.expected_generation_version,
            generation.version,
            generation.state.as_str(),
            "begin_chain_shutdown",
            &request_hash,
        )?;
        let handoff_replay = if let Some(value) = handoff.as_ref() {
            value.state == HandoffState::NeedsRecovery
                && tx.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM terminal_transition_audit
                         WHERE object_kind = 'handoff' AND object_id = ?1
                           AND to_version = ?2 AND to_state = 'needs_recovery'
                           AND action = 'begin_chain_shutdown'
                           AND request_hash = ?3
                     )",
                    params![value.id, value.version, request_hash],
                    |row| row.get::<_, bool>(0),
                )?
        } else {
            true
        };
        if chain.state != ChainState::NeedsRecovery
            || generation.state != GenerationState::NeedsRecovery
            || !generation_replay
            || !handoff_replay
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT shutdown replay does not match every durable object".to_string(),
            ));
        }
        tx.commit()?;
        return Ok(GenerationOutcome {
            generation,
            replayed: true,
        });
    }
    if chain.version != observation.expected_chain_version
        || generation.version != observation.expected_generation_version
        || chain.state == ChainState::NeedsRecovery
        || generation.state == GenerationState::NeedsRecovery
        || handoff
            .as_ref()
            .is_some_and(|value| value.state == HandoffState::NeedsRecovery)
    {
        return Err(conflict(
            &chain_id,
            observation.expected_chain_version,
            chain.state.as_str(),
            chain.version,
        ));
    }
    authorize_generation(&tx, &chain, &generation, actor, true, true, true)?;

    let now = now_epoch_f64();
    if let Some(current) = handoff.as_mut() {
        let from_state = current.state;
        let from_version = current.version;
        let updated = tx.execute(
            "UPDATE terminal_handoffs
             SET state = 'needs_recovery', version = ?1,
                 failure_kind = ?2, failure_reason = ?3, updated_at = ?4
             WHERE id = ?5 AND state = ?6 AND version = ?7",
            params![
                from_version + 1,
                failure_kind,
                failure_reason,
                now,
                current.id,
                from_state.as_str(),
                from_version
            ],
        )?;
        if updated != 1 {
            return Err(conflict(
                &current.id,
                from_version,
                from_state.as_str(),
                from_version,
            ));
        }
        insert_audit(
            &tx,
            &chain_id,
            "handoff",
            &current.id,
            from_version,
            Some(from_state.as_str()),
            HandoffState::NeedsRecovery.as_str(),
            &audit_actor,
            "supervisor",
            "begin_chain_shutdown",
            &request_hash,
            now,
        )?;
        current.state = HandoffState::NeedsRecovery;
        current.version += 1;
    }
    update_generation_state(
        &tx,
        &mut generation,
        GenerationState::NeedsRecovery,
        &audit_actor,
        "supervisor",
        "begin_chain_shutdown",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::NeedsRecovery,
        current_generation,
        &audit_actor,
        "supervisor",
        "begin_chain_shutdown",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    Ok(GenerationOutcome {
        generation,
        replayed: false,
    })
}

fn handoff_matches_source(
    handoff: &TerminalHandoff,
    generation: &TerminalGeneration,
    actor: &HandoffActor,
) -> bool {
    handoff.source_generation == actor.generation
        && handoff.source_generation == generation.generation
        && handoff.source_launch_nonce == generation.launch_nonce
        && handoff.source_instance_name == actor.instance_name
        && handoff.source_hcom_session_id == actor.hcom_session_id
        && handoff.source_native_session_id == actor.native_session_id.as_deref().unwrap_or("")
        && handoff.source_wrapper_process_id == actor.process_id
        && handoff.source_process_birth_identity == actor.process_birth_identity
}

fn ensure_workspace_matches(chain: &TerminalChain, cwd: &Path) -> Result<String, HandoffError> {
    let workspace = canonical_workspace(cwd)?;
    if workspace != chain.workspace {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller workspace does not match the pinned chain workspace"
                .to_string(),
        ));
    }
    Ok(workspace)
}

fn prepare_request_hash(
    handoff_id: &str,
    actor: &HandoffActor,
    source_generation: &TerminalGeneration,
    target_generation: &TerminalGeneration,
    bundle: &BundleSnapshot,
    workspace: &WorkspaceSnapshot,
    policy_ref: &str,
) -> String {
    let source = source_generation.generation.to_string();
    let target = target_generation.generation.to_string();
    let event_id = bundle.event_id.to_string();
    let size = bundle.size_bytes.to_string();
    hash_request(
        "prepare",
        handoff_id,
        -1,
        actor,
        &[
            source.as_bytes(),
            target.as_bytes(),
            source_generation.launch_nonce.as_bytes(),
            target_generation.launch_nonce.as_bytes(),
            event_id.as_bytes(),
            bundle.digest.as_bytes(),
            size.as_bytes(),
            workspace.workspace.as_bytes(),
            workspace.revision.as_bytes(),
            workspace.branch.as_bytes(),
            workspace.dirty_summary.as_bytes(),
            policy_ref.as_bytes(),
        ],
    )
}

pub fn prepare_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    bundle_event_id: i64,
    cwd: &Path,
) -> Result<HandoffOutcome, HandoffError> {
    prepare_handoff_with_snapshot_provider(db, actor, bundle_event_id, cwd, snapshot_workspace)
}

fn prepare_handoff_with_snapshot_provider<F>(
    db: &HcomDb,
    actor: &HandoffActor,
    bundle_event_id: i64,
    cwd: &Path,
    snapshot_provider: F,
) -> Result<HandoffOutcome, HandoffError>
where
    F: FnOnce(&Path) -> Result<WorkspaceSnapshot, HandoffError>,
{
    validate_actor(actor)?;
    // Canonicalization and every git subprocess run before BEGIN IMMEDIATE.
    // The transaction below only validates this deterministic snapshot
    // against the exact typed chain/generation/handoff state.
    let workspace = snapshot_provider(cwd)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let Some((mut chain, mut source_generation)) = find_actor_chain(&tx, actor)? else {
        return Err(HandoffError::NotManaged);
    };
    authorize_generation(&tx, &chain, &source_generation, actor, true, true, true)?;
    if source_generation.native_session_id.is_none() {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT source native session is not pinned".to_string(),
        ));
    }
    if workspace.workspace != chain.workspace {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller workspace does not match the pinned chain workspace"
                .to_string(),
        ));
    }
    let bundle = load_bundle_snapshot(&tx, bundle_event_id, &actor.instance_name)?;
    let target_number = source_generation.generation + 1;

    let existing = tx
        .query_row(
            "SELECT * FROM terminal_handoffs
             WHERE chain_id = ?1 AND state NOT IN ('accepted', 'aborted')
             LIMIT 1",
            params![chain.id],
            TerminalHandoff::from_row,
        )
        .optional()?;
    if let Some(existing) = existing {
        let target_generation = load_generation(&tx, &chain.id, existing.target_generation)?
            .ok_or(HandoffError::Storage)?;
        let request_hash = prepare_request_hash(
            &existing.id,
            actor,
            &source_generation,
            &target_generation,
            &bundle,
            &workspace,
            &chain.policy_ref,
        );
        let replay: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND from_version = -1 AND action = 'prepare'
                   AND request_hash = ?2
             )",
            params![existing.id, request_hash],
            |row| row.get(0),
        )?;
        if replay {
            tx.commit()?;
            return Ok(HandoffOutcome {
                handoff: existing,
                replayed: true,
            });
        }
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT a different non-final handoff already exists for this chain"
                .to_string(),
        ));
    }
    if chain.state != ChainState::Active || source_generation.state != GenerationState::Active {
        return Err(HandoffError::Conflict(format!(
            "HANDOFF_CONFLICT chain state={} generation_state={}",
            chain.state, source_generation.state
        )));
    }

    let mut target_generation =
        if let Some(existing) = load_generation(&tx, &chain.id, target_number)? {
            if existing.state != GenerationState::Reserved
                || existing.wrapper_process_id.is_some()
                || existing.native_session_id.is_some()
            {
                return Err(HandoffError::Conflict(
                    "HANDOFF_CONFLICT target generation reservation is not reusable".to_string(),
                ));
            }
            existing
        } else {
            TerminalGeneration {
                chain_id: chain.id.clone(),
                generation: target_number,
                launch_nonce: generate_id("ln"),
                wrapper_process_id: None,
                process_birth_identity: None,
                instance_name: None,
                hcom_session_id: None,
                native_session_id: None,
                state: GenerationState::Reserved,
                version: 0,
                created_at: 0.0,
                updated_at: 0.0,
            }
        };
    let handoff_id = generate_id("ho");
    let request_hash = prepare_request_hash(
        &handoff_id,
        actor,
        &source_generation,
        &target_generation,
        &bundle,
        &workspace,
        &chain.policy_ref,
    );
    let now = now_epoch_f64();
    if target_generation.created_at == 0.0 {
        target_generation.created_at = now;
        target_generation.updated_at = now;
        tx.execute(
            "INSERT INTO terminal_generations (
                 chain_id, generation, launch_nonce, state, version, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'reserved', 0, ?4, ?4)",
            params![
                target_generation.chain_id,
                target_generation.generation,
                target_generation.launch_nonce,
                now
            ],
        )?;
        insert_audit(
            &tx,
            &chain.id,
            "generation",
            &generation_object_id(&chain.id, target_number),
            -1,
            None,
            GenerationState::Reserved.as_str(),
            actor,
            "source",
            "reserve_target",
            &request_hash,
            now,
        )?;
    }
    tx.execute(
        "INSERT INTO terminal_handoffs (
             id, chain_id, source_generation, target_generation,
             source_launch_nonce, source_instance_name, source_hcom_session_id,
             source_native_session_id, source_wrapper_process_id,
             source_process_birth_identity, bundle_event_id, bundle_digest,
             bundle_size_bytes, workspace, revision, branch, dirty_summary,
             policy_ref, state, version, created_at, updated_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
             ?14, ?15, ?16, ?17, ?18, 'prepared', 0, ?19, ?19
         )",
        params![
            handoff_id,
            chain.id,
            source_generation.generation,
            target_number,
            source_generation.launch_nonce,
            actor.instance_name,
            actor.hcom_session_id,
            source_generation.native_session_id,
            actor.process_id,
            actor.process_birth_identity,
            bundle.event_id,
            bundle.digest,
            bundle.size_bytes,
            workspace.workspace,
            workspace.revision,
            workspace.branch,
            workspace.dirty_summary,
            chain.policy_ref,
            now,
        ],
    )?;
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        -1,
        None,
        HandoffState::Prepared.as_str(),
        actor,
        "source",
        "prepare",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source_generation,
        GenerationState::HandoffPrepared,
        actor,
        "source",
        "prepare",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::Prepared,
        current_generation,
        actor,
        "source",
        "prepare",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    let handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

fn load_source_context(
    tx: &Transaction<'_>,
    handoff_id: &str,
    actor: &HandoffActor,
    require_live: bool,
    require_current: bool,
) -> Result<(TerminalHandoff, TerminalChain, TerminalGeneration), HandoffError> {
    let handoff = load_handoff(tx, handoff_id)?
        .ok_or_else(|| HandoffError::Invalid("handoff was not found".to_string()))?;
    let chain = load_chain(tx, &handoff.chain_id)?.ok_or(HandoffError::Storage)?;
    let generation = load_generation(tx, &handoff.chain_id, handoff.source_generation)?
        .ok_or(HandoffError::Storage)?;
    authorize_generation(
        tx,
        &chain,
        &generation,
        actor,
        true,
        require_live,
        require_current,
    )?;
    if !handoff_matches_source(&handoff, &generation, actor) {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller does not match the immutable source identity".to_string(),
        ));
    }
    Ok((handoff, chain, generation))
}

fn load_supervisor_source_context(
    tx: &Transaction<'_>,
    handoff_id: &str,
    supervisor: &SupervisorActor,
    require_current: bool,
) -> Result<
    (
        TerminalHandoff,
        TerminalChain,
        TerminalGeneration,
        HandoffActor,
    ),
    HandoffError,
> {
    validate_supervisor_actor(supervisor)?;
    let handoff = load_handoff(tx, handoff_id)?
        .ok_or_else(|| HandoffError::Invalid("handoff was not found".to_string()))?;
    let chain = load_chain(tx, &handoff.chain_id)?.ok_or(HandoffError::Storage)?;
    let supervisor_binding = load_current_supervisor_binding(tx, &chain)?;
    let source = load_generation(tx, &handoff.chain_id, handoff.source_generation)?
        .ok_or(HandoffError::Storage)?;
    let exact_source_snapshot = source.generation == handoff.source_generation
        && source.launch_nonce == handoff.source_launch_nonce
        && source.wrapper_process_id.as_deref() == Some(handoff.source_wrapper_process_id.as_str())
        && source.process_birth_identity.as_deref()
            == Some(handoff.source_process_birth_identity.as_str())
        && source.instance_name.as_deref() == Some(handoff.source_instance_name.as_str())
        && source.hcom_session_id.as_deref() == Some(handoff.source_hcom_session_id.as_str())
        && source.native_session_id.as_deref() == Some(handoff.source_native_session_id.as_str());
    if (require_current && chain.current_generation != handoff.source_generation)
        || !exact_source_snapshot
        || supervisor_binding.process_id != supervisor.process_id
        || supervisor_binding.process_birth_identity != supervisor.process_birth_identity
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller does not match the immutable chain supervisor".to_string(),
        ));
    }
    let audit_actor = HandoffActor {
        instance_name: handoff.source_instance_name.clone(),
        hcom_session_id: handoff.source_hcom_session_id.clone(),
        native_session_id: Some(handoff.source_native_session_id.clone()),
        process_id: supervisor.process_id.clone(),
        process_birth_identity: supervisor.process_birth_identity.clone(),
        generation: handoff.source_generation,
    };
    Ok((handoff, chain, source, audit_actor))
}

pub fn commit_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
) -> Result<HandoffOutcome, HandoffError> {
    commit_handoff_with_snapshot_provider(
        db,
        actor,
        handoff_id,
        expected_version,
        cwd,
        snapshot_workspace,
    )
}

fn commit_handoff_with_snapshot_provider<F>(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
    snapshot_provider: F,
) -> Result<HandoffOutcome, HandoffError>
where
    F: FnOnce(&Path) -> Result<WorkspaceSnapshot, HandoffError>,
{
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    let workspace = snapshot_provider(cwd)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source) =
        load_source_context(&tx, &handoff_id, actor, true, true)?;
    if workspace.workspace != chain.workspace
        || workspace.workspace != handoff.workspace
        || workspace.revision != handoff.revision
        || workspace.branch != handoff.branch
        || workspace.dirty_summary != handoff.dirty_summary
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT workspace snapshot changed after prepare".to_string(),
        ));
    }
    let bundle = verify_pinned_bundle(&tx, &handoff)?;
    let bundle_event_id = bundle.event_id.to_string();
    let bundle_size = bundle.size_bytes.to_string();
    let request_hash = hash_request(
        "commit",
        &handoff_id,
        expected_version,
        actor,
        &[
            workspace.workspace.as_bytes(),
            workspace.revision.as_bytes(),
            workspace.branch.as_bytes(),
            workspace.dirty_summary.as_bytes(),
            handoff.policy_ref.as_bytes(),
            bundle_event_id.as_bytes(),
            bundle.digest.as_bytes(),
            bundle_size.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "commit",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::Prepared
        || handoff.version != expected_version
        || chain.state != ChainState::Prepared
        || source.state != GenerationState::HandoffPrepared
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let to_version = expected_version + 1;
    let token = generate_id("qa");
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'committed', version = ?1, quiesce_token = ?2,
             quiesce_generation = source_generation,
             quiesce_native_session_id = source_native_session_id,
             quiesce_process_id = source_wrapper_process_id,
             quiesce_process_birth_identity = source_process_birth_identity,
             quiesce_committed_version = ?1, committed_at = ?3, updated_at = ?3
         WHERE id = ?4 AND state = 'prepared' AND version = ?5",
        params![to_version, token, now, handoff_id, expected_version],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::Prepared.as_str()),
        HandoffState::Committed.as_str(),
        actor,
        "source",
        "commit",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source,
        GenerationState::HandoffCommitted,
        actor,
        "source",
        "commit",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::Committed,
        current_generation,
        actor,
        "source",
        "commit",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn abort_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    reason: &str,
    cwd: &Path,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    let reason = sanitize_reason(reason)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source) =
        load_source_context(&tx, &handoff_id, actor, true, true)?;
    let workspace = ensure_workspace_matches(&chain, cwd)?;
    let request_hash = hash_request(
        "abort",
        &handoff_id,
        expected_version,
        actor,
        &[reason.as_bytes(), workspace.as_bytes()],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "abort",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::Prepared
        || handoff.version != expected_version
        || chain.state != ChainState::Prepared
        || source.state != GenerationState::HandoffPrepared
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'aborted', version = ?1, failure_kind = 'aborted',
             failure_reason = ?2, updated_at = ?3
         WHERE id = ?4 AND state = 'prepared' AND version = ?5",
        params![
            expected_version + 1,
            reason,
            now,
            handoff_id,
            expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::Prepared.as_str()),
        HandoffState::Aborted.as_str(),
        actor,
        "source",
        "abort",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source,
        GenerationState::Active,
        actor,
        "source",
        "abort",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::Active,
        current_generation,
        actor,
        "source",
        "abort",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn observe_stop(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    observation: &StopObservation,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(observation.expected_version)?;
    let token = validate_opaque_id(&observation.quiesce_token, "quiesce authorization")?;
    let hook_session = validate_text(
        &observation.hook_native_session_id,
        "hook native session identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    let launch_nonce = validate_opaque_id(&observation.launch_nonce, "launch nonce")?;
    let turn_id = validate_text(
        &observation.turn_id,
        "Stop turn identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source) =
        load_source_context(&tx, &handoff_id, actor, true, true)?;
    let committed_version = observation.committed_version.to_string();
    let request_hash = hash_request(
        "observe_stop",
        &handoff_id,
        observation.expected_version,
        actor,
        &[
            token.as_bytes(),
            committed_version.as_bytes(),
            hook_session.as_bytes(),
            launch_nonce.as_bytes(),
            turn_id.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        observation.expected_version,
        handoff.version,
        handoff.state.as_str(),
        "observe_stop",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::Committed
        || handoff.version != observation.expected_version
        || chain.state != ChainState::Committed
        || source.state != GenerationState::HandoffCommitted
    {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let exact_authorization = handoff.quiesce_token.as_deref() == Some(token.as_str())
        && handoff.quiesce_generation == Some(actor.generation)
        && handoff.quiesce_native_session_id.as_deref() == Some(hook_session.as_str())
        && handoff.quiesce_native_session_id.as_deref() == actor.native_session_id.as_deref()
        && handoff.quiesce_process_id.as_deref() == Some(actor.process_id.as_str())
        && handoff.quiesce_process_birth_identity.as_deref()
            == Some(actor.process_birth_identity.as_str())
        && handoff.quiesce_committed_version == Some(observation.committed_version)
        && observation.committed_version == handoff.version
        && source.launch_nonce == launch_nonce;
    if !exact_authorization {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT typed Stop does not match the committed authorization".to_string(),
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'stop_observed', version = ?1,
             stop_observed_at = ?2, stop_turn_id = ?3, updated_at = ?2
         WHERE id = ?4 AND state = 'committed' AND version = ?5",
        params![
            observation.expected_version + 1,
            now,
            turn_id,
            handoff_id,
            observation.expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        observation.expected_version,
        Some(HandoffState::Committed.as_str()),
        HandoffState::StopObserved.as_str(),
        actor,
        "source",
        "observe_stop",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source,
        GenerationState::StopObserved,
        actor,
        "source",
        "observe_stop",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::StopObserved,
        current_generation,
        actor,
        "source",
        "observe_stop",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn begin_quiesce(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    expected_version: i64,
    quiesce_token: &str,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(expected_version)?;
    let token = validate_opaque_id(quiesce_token, "quiesce authorization")?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, true)?;
    let request_hash = hash_request(
        "begin_quiesce",
        &handoff_id,
        expected_version,
        &audit_actor,
        &[token.as_bytes()],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "begin_quiesce",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::StopObserved
        || handoff.version != expected_version
        || chain.state != ChainState::StopObserved
        || source.state != GenerationState::StopObserved
        || handoff.stop_observed_at.is_none()
        || handoff.quiesce_token.as_deref() != Some(token.as_str())
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'quiescing_source', version = ?1, updated_at = ?2
         WHERE id = ?3 AND state = 'stop_observed' AND version = ?4
           AND stop_observed_at IS NOT NULL AND quiesce_token = ?5",
        params![
            expected_version + 1,
            now,
            handoff_id,
            expected_version,
            token
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::StopObserved.as_str()),
        HandoffState::QuiescingSource.as_str(),
        &audit_actor,
        "supervisor",
        "begin_quiesce",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source,
        GenerationState::Quiescing,
        &audit_actor,
        "supervisor",
        "begin_quiesce",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::QuiescingSource,
        current_generation,
        &audit_actor,
        "supervisor",
        "begin_quiesce",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

/// Persist the outcome of the supervisor's single SIGTERM request.
///
/// The OS action happens immediately before this call. If the process crashes
/// or this write fails, later code must not guess or resend: a quiescing row
/// without this evidence is a recovery case, never a target-launch permit.
pub fn record_sigterm_request(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    observation: &SigtermObservation,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(observation.expected_version)?;
    validate_observation_timestamp(
        observation.requested_wall_at,
        "SIGTERM request wall timestamp",
    )?;
    validate_monotonic_ns(
        observation.requested_monotonic_ns,
        "SIGTERM request monotonic timestamp",
    )?;

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, false)?;
    let wall_at = observation.requested_wall_at.to_bits().to_string();
    let monotonic_ns = observation.requested_monotonic_ns.to_string();
    let request_hash = hash_request(
        "record_sigterm_request",
        &handoff_id,
        observation.expected_version,
        &audit_actor,
        &[
            wall_at.as_bytes(),
            monotonic_ns.as_bytes(),
            observation.result.as_str().as_bytes(),
            handoff.quiesce_token.as_deref().unwrap_or("").as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        observation.expected_version,
        handoff.version,
        handoff.state.as_str(),
        "record_sigterm_request",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::QuiescingSource
        || handoff.version != observation.expected_version
        || source.state != GenerationState::Quiescing
        || chain.state != ChainState::QuiescingSource
        || handoff.stop_observed_at.is_none()
        || !handoff.sigterm_request_result.is_empty()
    {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }

    let sent = observation.result == SigtermRequestResult::Sent;
    let next_handoff = if sent {
        HandoffState::QuiescingSource
    } else {
        HandoffState::NeedsRecovery
    };
    let failure_kind = if sent { "" } else { "sigterm_failed" };
    let failure_reason = if sent {
        ""
    } else {
        "SIGTERM request could not be delivered"
    };
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = ?1, version = ?2,
             sigterm_requested_wall_at = ?3,
             sigterm_requested_monotonic_ns = ?4,
             sigterm_request_result = ?5,
             failure_kind = ?6, failure_reason = ?7, updated_at = ?8
         WHERE id = ?9 AND state = 'quiescing_source' AND version = ?10
           AND stop_observed_at IS NOT NULL
           AND sigterm_request_result = ''",
        params![
            next_handoff.as_str(),
            observation.expected_version + 1,
            observation.requested_wall_at,
            observation.requested_monotonic_ns,
            observation.result.as_str(),
            failure_kind,
            failure_reason,
            now,
            handoff_id,
            observation.expected_version,
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        observation.expected_version,
        Some(HandoffState::QuiescingSource.as_str()),
        next_handoff.as_str(),
        &audit_actor,
        "supervisor",
        "record_sigterm_request",
        &request_hash,
        now,
    )?;
    if !sent {
        update_generation_state(
            &tx,
            &mut source,
            GenerationState::NeedsRecovery,
            &audit_actor,
            "supervisor",
            "record_sigterm_request",
            &request_hash,
            now,
        )?;
        let current_generation = chain.current_generation;
        update_chain_state(
            &tx,
            &mut chain,
            ChainState::NeedsRecovery,
            current_generation,
            &audit_actor,
            "supervisor",
            "record_sigterm_request",
            &request_hash,
            now,
        )?;
    }
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn observe_source_exit_without_stop(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    observation: &CleanupObservation,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(observation.expected_version)?;
    let exit = observation.exit.as_ref().ok_or_else(|| {
        HandoffError::Invalid("source exit observation is missing exit evidence".to_string())
    })?;
    validate_child_exit(exit)?;
    let failure_reason = if observation.failure_reason.trim().is_empty() {
        "source exited before a verified typed Stop".to_string()
    } else {
        sanitize_reason(&observation.failure_reason)?
    };
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, true)?;
    let wall_at = exit.observed_wall_at.to_bits().to_string();
    let monotonic_ns = exit.observed_monotonic_ns.to_string();
    let exit_code = exit
        .exit_code
        .map_or_else(String::new, |value| value.to_string());
    let exit_signal = exit
        .exit_signal
        .map_or_else(String::new, |value| value.to_string());
    let reaped = u8::from(observation.reaped).to_string();
    let inject = u8::from(observation.resources.inject_succeeded).to_string();
    let delivery = u8::from(observation.resources.delivery_succeeded).to_string();
    let pty = u8::from(observation.resources.pty_succeeded).to_string();
    let screen = u8::from(observation.resources.screen_succeeded).to_string();
    let write_queue = u8::from(observation.resources.write_queue_succeeded).to_string();
    let request_hash = hash_request(
        "source_exit_without_stop",
        &handoff_id,
        observation.expected_version,
        &audit_actor,
        &[
            wall_at.as_bytes(),
            monotonic_ns.as_bytes(),
            exit_code.as_bytes(),
            exit_signal.as_bytes(),
            exit.delivery_context.as_str().as_bytes(),
            reaped.as_bytes(),
            inject.as_bytes(),
            delivery.as_bytes(),
            pty.as_bytes(),
            screen.as_bytes(),
            write_queue.as_bytes(),
            failure_reason.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        observation.expected_version,
        handoff.version,
        handoff.state.as_str(),
        "source_exit_without_stop",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::Committed
        || handoff.version != observation.expected_version
        || handoff.stop_observed_at.is_some()
        || chain.state != ChainState::Committed
        || source.state != GenerationState::HandoffCommitted
    {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'needs_recovery', version = ?1,
             failure_kind = 'exit_without_stop', failure_reason = ?2,
             child_exit_observed_wall_at = ?3,
             child_exit_observed_monotonic_ns = ?4,
             child_exit_code = ?5, child_exit_signal = ?6,
             delivery_exit_context = ?7, waitpid_reaped = ?8,
             inject_cleanup_succeeded = ?9,
             delivery_cleanup_succeeded = ?10,
             pty_cleanup_succeeded = ?11,
             screen_cleanup_succeeded = ?12,
             write_queue_cleanup_succeeded = ?13,
             cleanup_completed_at = ?14, updated_at = ?14
         WHERE id = ?15 AND state = 'committed' AND version = ?16
           AND stop_observed_at IS NULL",
        params![
            observation.expected_version + 1,
            failure_reason,
            exit.observed_wall_at,
            exit.observed_monotonic_ns,
            exit.exit_code,
            exit.exit_signal,
            exit.delivery_context.as_str(),
            observation.reaped,
            observation.resources.inject_succeeded,
            observation.resources.delivery_succeeded,
            observation.resources.pty_succeeded,
            observation.resources.screen_succeeded,
            observation.resources.write_queue_succeeded,
            now,
            handoff_id,
            observation.expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        observation.expected_version,
        Some(HandoffState::Committed.as_str()),
        HandoffState::NeedsRecovery.as_str(),
        &audit_actor,
        "supervisor",
        "source_exit_without_stop",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut source,
        GenerationState::NeedsRecovery,
        &audit_actor,
        "supervisor",
        "source_exit_without_stop",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::NeedsRecovery,
        current_generation,
        &audit_actor,
        "supervisor",
        "source_exit_without_stop",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

struct RetiredInstanceSnapshot {
    session_id: Option<String>,
    tool: String,
    directory: Option<String>,
    transcript_path: Option<String>,
    pid: Option<i64>,
    created_at: Option<f64>,
}

pub fn complete_source_cleanup(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    observation: &CleanupObservation,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(observation.expected_version)?;
    if let Some(exit) = observation.exit.as_ref() {
        validate_child_exit(exit)?;
    }
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, false)?;
    let sigterm_monotonic_ns = handoff.sigterm_requested_monotonic_ns.ok_or_else(|| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT successful SIGTERM evidence is missing".to_string(),
        )
    })?;
    if handoff.sigterm_request_result != SigtermRequestResult::Sent.as_str() {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT source cleanup cannot advance without a delivered SIGTERM"
                .to_string(),
        ));
    }
    let elapsed_ms = match observation.exit.as_ref() {
        Some(exit) if exit.observed_monotonic_ns >= sigterm_monotonic_ns => {
            let elapsed = (exit.observed_monotonic_ns - sigterm_monotonic_ns) / 1_000_000;
            if elapsed > MAX_QUIESCE_ELAPSED_MS {
                return Err(HandoffError::Invalid(
                    "SIGTERM-to-exit elapsed time exceeds the evidence bound".to_string(),
                ));
            }
            Some(elapsed)
        }
        Some(_) => {
            return Err(HandoffError::Invalid(
                "child exit monotonic timestamp precedes SIGTERM".to_string(),
            ));
        }
        None => None,
    };
    let success =
        observation.exit.is_some() && observation.reaped && observation.resources.all_succeeded();
    if success
        && (!observation.failure_kind.trim().is_empty()
            || !observation.failure_reason.trim().is_empty())
    {
        return Err(HandoffError::Invalid(
            "successful cleanup cannot contain failure evidence".to_string(),
        ));
    }
    let failure_kind = if success {
        String::new()
    } else if observation.failure_kind.trim().is_empty() {
        "cleanup_failed".to_string()
    } else {
        validate_text(
            observation.failure_kind.trim(),
            "failure kind",
            MAX_FAILURE_KIND_BYTES,
            false,
        )?
    };
    let failure_reason = if success {
        String::new()
    } else if observation.failure_reason.trim().is_empty() {
        "source cleanup did not complete".to_string()
    } else {
        sanitize_reason(&observation.failure_reason)?
    };
    let exit_wall_at = observation.exit.as_ref().map(|exit| exit.observed_wall_at);
    let exit_monotonic_ns = observation
        .exit
        .as_ref()
        .map(|exit| exit.observed_monotonic_ns);
    let exit_code = observation.exit.as_ref().and_then(|exit| exit.exit_code);
    let exit_signal = observation.exit.as_ref().and_then(|exit| exit.exit_signal);
    let exit_context = observation
        .exit
        .as_ref()
        .map(|exit| exit.delivery_context.as_str())
        .unwrap_or("");
    let exit_wall_hash = exit_wall_at
        .map(f64::to_bits)
        .map_or_else(String::new, |value| value.to_string());
    let exit_monotonic_hash = exit_monotonic_ns.map_or_else(String::new, |value| value.to_string());
    let exit_code_hash = exit_code.map_or_else(String::new, |value| value.to_string());
    let exit_signal_hash = exit_signal.map_or_else(String::new, |value| value.to_string());
    let elapsed_hash = elapsed_ms.map_or_else(String::new, |value| value.to_string());
    let reaped = u8::from(observation.reaped).to_string();
    let inject = u8::from(observation.resources.inject_succeeded).to_string();
    let delivery = u8::from(observation.resources.delivery_succeeded).to_string();
    let pty = u8::from(observation.resources.pty_succeeded).to_string();
    let screen = u8::from(observation.resources.screen_succeeded).to_string();
    let write_queue = u8::from(observation.resources.write_queue_succeeded).to_string();
    let request_hash = hash_request(
        "complete_source_cleanup",
        &handoff_id,
        observation.expected_version,
        &audit_actor,
        &[
            exit_wall_hash.as_bytes(),
            exit_monotonic_hash.as_bytes(),
            exit_code_hash.as_bytes(),
            exit_signal_hash.as_bytes(),
            elapsed_hash.as_bytes(),
            exit_context.as_bytes(),
            reaped.as_bytes(),
            inject.as_bytes(),
            delivery.as_bytes(),
            pty.as_bytes(),
            screen.as_bytes(),
            write_queue.as_bytes(),
            failure_kind.as_bytes(),
            failure_reason.as_bytes(),
            handoff.quiesce_token.as_deref().unwrap_or("").as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        observation.expected_version,
        handoff.version,
        handoff.state.as_str(),
        "complete_source_cleanup",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::QuiescingSource
        || handoff.version != observation.expected_version
        || handoff.stop_observed_at.is_none()
        || source.state != GenerationState::Quiescing
        || handoff.cleanup_completed_at.is_some()
    {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    if success {
        let binding: Option<(Option<String>, Option<String>)> = tx
            .query_row(
                "SELECT session_id, instance_name
                 FROM process_bindings WHERE process_id = ?1",
                [&handoff.source_wrapper_process_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if binding.as_ref().is_some_and(|(session, instance)| {
            session.as_deref() != Some(handoff.source_hcom_session_id.as_str())
                || instance.as_deref() != Some(handoff.source_instance_name.as_str())
        }) {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT source process binding changed before cleanup".to_string(),
            ));
        }
        let instance: Option<RetiredInstanceSnapshot> = tx
            .query_row(
                "SELECT session_id, tool, directory, transcript_path, pid, created_at
                 FROM instances WHERE name = ?1",
                [&handoff.source_instance_name],
                |row| {
                    Ok(RetiredInstanceSnapshot {
                        session_id: row.get(0)?,
                        tool: row.get(1)?,
                        directory: row.get(2)?,
                        transcript_path: row.get(3)?,
                        pid: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                },
            )
            .optional()?;
        if instance.as_ref().is_some_and(|instance| {
            instance.session_id.as_deref() != Some(handoff.source_hcom_session_id.as_str())
                || instance.tool != "codex"
        }) {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT source instance changed before cleanup".to_string(),
            ));
        }
        let directory = instance
            .as_ref()
            .and_then(|value| value.directory.as_deref())
            .filter(|value| !value.is_empty())
            .unwrap_or(handoff.workspace.as_str());
        let transcript_path = instance
            .as_ref()
            .and_then(|value| value.transcript_path.as_deref())
            .unwrap_or("");
        validate_text(
            directory,
            "retired instance directory",
            MAX_WORKSPACE_BYTES,
            false,
        )?;
        validate_text(
            transcript_path,
            "retired instance transcript path",
            MAX_WORKSPACE_BYTES,
            true,
        )?;
        let lifecycle = serde_json::json!({
            "action": "stopped",
            "by": "chain-supervisor",
            "reason": "handoff",
            "snapshot": {
                "tool": "codex",
                "session_id": handoff.source_hcom_session_id,
                "directory": directory,
                "transcript_path": transcript_path,
                "pid": instance.as_ref().and_then(|value| value.pid),
                "created_at": instance.as_ref().and_then(|value| value.created_at),
            },
        });
        let lifecycle = serde_json::to_string(&lifecycle).map_err(|_| HandoffError::Storage)?;
        if lifecycle.len() > MAX_STATUS_JSON_BYTES {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT retired lifecycle snapshot exceeds the byte bound".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO events (timestamp, type, instance, data)
             VALUES (?1, 'life', ?2, ?3)",
            params![
                crate::shared::time::now_iso(),
                handoff.source_instance_name,
                lifecycle
            ],
        )?;
        tx.execute(
            "DELETE FROM process_bindings
             WHERE process_id = ?1 AND session_id = ?2 AND instance_name = ?3",
            params![
                handoff.source_wrapper_process_id,
                handoff.source_hcom_session_id,
                handoff.source_instance_name
            ],
        )?;
        tx.execute(
            "DELETE FROM instances
             WHERE name = ?1 AND session_id = ?2 AND tool = 'codex'",
            params![handoff.source_instance_name, handoff.source_hcom_session_id],
        )?;
    }
    let next_handoff = if success {
        HandoffState::LaunchingTarget
    } else {
        HandoffState::NeedsRecovery
    };
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = ?1, version = ?2, failure_kind = ?3,
             failure_reason = ?4,
             child_exit_observed_wall_at = ?5,
             child_exit_observed_monotonic_ns = ?6,
             child_exit_code = ?7, child_exit_signal = ?8,
             sigterm_to_exit_ms = ?9, delivery_exit_context = ?10,
             waitpid_reaped = ?11, inject_cleanup_succeeded = ?12,
             delivery_cleanup_succeeded = ?13,
             pty_cleanup_succeeded = ?14,
             screen_cleanup_succeeded = ?15,
             write_queue_cleanup_succeeded = ?16,
             cleanup_completed_at = ?17, updated_at = ?17
         WHERE id = ?18 AND state = 'quiescing_source' AND version = ?19
           AND stop_observed_at IS NOT NULL
           AND sigterm_request_result = 'sent'
           AND cleanup_completed_at IS NULL",
        params![
            next_handoff.as_str(),
            observation.expected_version + 1,
            failure_kind,
            failure_reason,
            exit_wall_at,
            exit_monotonic_ns,
            exit_code,
            exit_signal,
            elapsed_ms,
            exit_context,
            observation.reaped,
            observation.resources.inject_succeeded,
            observation.resources.delivery_succeeded,
            observation.resources.pty_succeeded,
            observation.resources.screen_succeeded,
            observation.resources.write_queue_succeeded,
            now,
            handoff_id,
            observation.expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        observation.expected_version,
        Some(HandoffState::QuiescingSource.as_str()),
        next_handoff.as_str(),
        &audit_actor,
        "supervisor",
        "complete_source_cleanup",
        &request_hash,
        now,
    )?;
    let source_state = if success {
        GenerationState::Retired
    } else {
        GenerationState::NeedsRecovery
    };
    update_generation_state(
        &tx,
        &mut source,
        source_state,
        &audit_actor,
        "supervisor",
        "complete_source_cleanup",
        &request_hash,
        now,
    )?;
    if success {
        let mut target = load_generation(&tx, &chain.id, handoff.target_generation)?
            .ok_or(HandoffError::Storage)?;
        if target.state != GenerationState::Reserved {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT target generation is not reserved".to_string(),
            ));
        }
        update_generation_state(
            &tx,
            &mut target,
            GenerationState::Launching,
            &audit_actor,
            "supervisor",
            "complete_source_cleanup",
            &request_hash,
            now,
        )?;
        let target_number = handoff.target_generation;
        update_chain_state(
            &tx,
            &mut chain,
            ChainState::LaunchingTarget,
            target_number,
            &audit_actor,
            "supervisor",
            "complete_source_cleanup",
            &request_hash,
            now,
        )?;
    } else {
        let current_generation = chain.current_generation;
        update_chain_state(
            &tx,
            &mut chain,
            ChainState::NeedsRecovery,
            current_generation,
            &audit_actor,
            "supervisor",
            "complete_source_cleanup",
            &request_hash,
            now,
        )?;
    }
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

/// Bind the already prepared target wrapper to its exact typed reservation.
///
/// The wrapper must still be held behind its private bootstrap gate; callers
/// release it only after this transaction commits. The instance is created
/// directly as `launching`, never as a generic pending placeholder, so an old
/// source hook cannot adopt it.
pub fn materialize_target_generation(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    materialization: &TargetMaterialization,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(materialization.expected_version)?;
    let launch_nonce = validate_opaque_id(&materialization.launch_nonce, "launch nonce")?;
    let instance_name = validate_text(
        &materialization.instance_name,
        "instance identity",
        MAX_INSTANCE_NAME_BYTES,
        false,
    )?;
    let hcom_session_id = validate_text(
        &materialization.hcom_session_id,
        "hcom session identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;
    let process_id = validate_text(
        &materialization.process_id,
        "process identity",
        MAX_PROCESS_ID_BYTES,
        false,
    )?;
    let process_birth_identity = validate_text(
        &materialization.process_birth_identity,
        "process birth identity",
        MAX_IDENTITY_BYTES,
        false,
    )?;

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, _source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, false)?;
    let supervisor_binding = load_current_supervisor_binding(&tx, &chain)?;
    validate_process_materialization(materialization, &supervisor_binding)?;
    let effective_target = effective_target_generation(&tx, &handoff)?;
    let mut target =
        load_generation(&tx, &chain.id, effective_target)?.ok_or(HandoffError::Storage)?;
    let process_evidence = load_generation_process(&tx, &chain.id, effective_target)?;
    let prepare_intent = load_generation_prepare_intent(&tx, &chain.id, effective_target)?
        .ok_or_else(|| {
            typed_conflict(
                "prepare_intent_required",
                "target process materialization requires a durable prepare intent",
            )
        })?;
    let target_generation = effective_target.to_string();
    let request_hash = hash_request(
        "materialize_target",
        &handoff_id,
        materialization.expected_version,
        &audit_actor,
        &[
            target_generation.as_bytes(),
            launch_nonce.as_bytes(),
            instance_name.as_bytes(),
            hcom_session_id.as_bytes(),
            process_id.as_bytes(),
            process_birth_identity.as_bytes(),
            materialization.wrapper_pid.to_string().as_bytes(),
            materialization.wrapper_pgid.to_string().as_bytes(),
            materialization.child_pid.to_string().as_bytes(),
            materialization.child_pgid.to_string().as_bytes(),
            materialization.child_process_birth_identity.as_bytes(),
        ],
    );
    let exact_materialization = target.launch_nonce == launch_nonce
        && target.wrapper_process_id.as_deref() == Some(process_id.as_str())
        && target.process_birth_identity.as_deref() == Some(process_birth_identity.as_str())
        && target.instance_name.as_deref() == Some(instance_name.as_str())
        && target.hcom_session_id.as_deref() == Some(hcom_session_id.as_str())
        && target.native_session_id.is_none()
        && process_evidence.as_ref().is_some_and(|evidence| {
            process_evidence_matches_materialization(evidence, materialization)
        });
    if exact_materialization
        && transition_is_replay(
            &tx,
            ("handoff", &handoff_id),
            materialization.expected_version,
            handoff.version,
            handoff.state.as_str(),
            "materialize_target",
            &request_hash,
        )?
    {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    let normal_cleanup_proved = handoff.sigterm_request_result
        == SigtermRequestResult::Sent.as_str()
        && handoff.child_exit_observed_wall_at.is_some()
        && handoff.waitpid_reaped == Some(true)
        && handoff.inject_cleanup_succeeded == Some(true)
        && handoff.delivery_cleanup_succeeded == Some(true)
        && handoff.pty_cleanup_succeeded == Some(true)
        && handoff.screen_cleanup_succeeded == Some(true)
        && handoff.write_queue_cleanup_succeeded == Some(true)
        && handoff.cleanup_completed_at.is_some();
    let recovery_absence_proved = tx
        .query_row(
            "SELECT r.replaced_generation,
                    (SELECT COUNT(*)
                     FROM terminal_recovery_absence_evidence e
                     WHERE e.recovery_attempt_id = r.id)
             FROM terminal_recovery_attempts r
             WHERE r.handoff_id = ?1 AND r.target_generation = ?2
               AND r.state IN ('authorized', 'materialized')
             ORDER BY r.sequence DESC LIMIT 1",
            params![handoff_id, effective_target],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .is_some_and(|(replaced_generation, evidence_count)| {
            let process_exists = load_generation_process(&tx, &chain.id, replaced_generation)
                .ok()
                .flatten()
                .is_some();
            evidence_count == if process_exists { 5 } else { 2 }
        });
    if handoff.state != HandoffState::LaunchingTarget
        || handoff.version != materialization.expected_version
        || chain.state != ChainState::LaunchingTarget
        || chain.current_generation != effective_target
        || target.state != GenerationState::Launching
        || target.launch_nonce != launch_nonce
        || target.wrapper_process_id.is_some()
        || target.process_birth_identity.is_some()
        || target.instance_name.is_some()
        || target.hcom_session_id.is_some()
        || target.native_session_id.is_some()
        || prepare_intent.launch_nonce != launch_nonce
        || prepare_intent.supervisor_process_id != supervisor.process_id
        || prepare_intent.supervisor_process_birth_identity != supervisor.process_birth_identity
        || prepare_intent.control_object_kind != "handoff"
        || prepare_intent.control_object_id != handoff_id
        || prepare_intent.control_version != materialization.expected_version
        || prepare_intent.generation_version != target.version
        || (!normal_cleanup_proved && !recovery_absence_proved)
    {
        return Err(conflict(
            &handoff_id,
            materialization.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }

    let now = now_epoch_f64();
    tx.execute(
        "INSERT INTO instances (
             name, session_id, status, tool, created_at, parent_name,
             origin_device_id, launch_context
         ) VALUES (?1, ?2, 'launching', 'codex', ?3, '', '', ?4)",
        params![
            instance_name,
            hcom_session_id,
            now,
            serde_json::json!({
                "chain_id": handoff.chain_id,
                "generation": effective_target,
                "launch_nonce": launch_nonce,
            })
            .to_string(),
        ],
    )
    .map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT exact target instance materialization already exists".to_string(),
        )
    })?;
    tx.execute(
        "INSERT INTO process_bindings (
             process_id, session_id, instance_name, updated_at
         ) VALUES (?1, ?2, ?3, ?4)",
        params![process_id, hcom_session_id, instance_name, now],
    )
    .map_err(|_| {
        HandoffError::Conflict(
            "HANDOFF_CONFLICT exact target process materialization already exists".to_string(),
        )
    })?;

    let target_from_version = target.version;
    let updated_target = tx.execute(
        "UPDATE terminal_generations
         SET wrapper_process_id = ?1, process_birth_identity = ?2,
             instance_name = ?3, hcom_session_id = ?4,
             version = ?5, updated_at = ?6
         WHERE chain_id = ?7 AND generation = ?8
           AND state = 'launching' AND version = ?9
           AND wrapper_process_id IS NULL
           AND process_birth_identity IS NULL
           AND instance_name IS NULL AND hcom_session_id IS NULL
           AND native_session_id IS NULL AND launch_nonce = ?10",
        params![
            process_id,
            process_birth_identity,
            instance_name,
            hcom_session_id,
            target_from_version + 1,
            now,
            target.chain_id,
            target.generation,
            target_from_version,
            launch_nonce,
        ],
    )?;
    if updated_target != 1 {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target reservation changed during materialization".to_string(),
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "generation",
        &generation_object_id(&chain.id, target.generation),
        target_from_version,
        Some(GenerationState::Launching.as_str()),
        GenerationState::Launching.as_str(),
        &audit_actor,
        "supervisor",
        "materialize_target",
        &request_hash,
        now,
    )?;
    target.wrapper_process_id = Some(process_id);
    target.process_birth_identity = Some(process_birth_identity);
    target.instance_name = Some(instance_name);
    target.hcom_session_id = Some(hcom_session_id);
    target.version += 1;
    target.updated_at = now;

    let updated_handoff = tx.execute(
        "UPDATE terminal_handoffs
         SET version = ?1, updated_at = ?2
         WHERE id = ?3 AND state = 'launching_target' AND version = ?4",
        params![
            materialization.expected_version + 1,
            now,
            handoff_id,
            materialization.expected_version
        ],
    )?;
    if updated_handoff != 1 {
        return Err(conflict(
            &handoff_id,
            materialization.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        materialization.expected_version,
        Some(HandoffState::LaunchingTarget.as_str()),
        HandoffState::LaunchingTarget.as_str(),
        &audit_actor,
        "supervisor",
        "materialize_target",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::LaunchingTarget,
        current_generation,
        &audit_actor,
        "supervisor",
        "materialize_target",
        &request_hash,
        now,
    )?;
    insert_generation_process(&tx, &chain.id, target.generation, materialization, now)?;
    tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'materialized', version = version + 1, updated_at = ?1
         WHERE handoff_id = ?2 AND target_generation = ?3 AND state = 'authorized'",
        params![now, handoff_id, effective_target],
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

/// Fail one exact target reservation closed after prepare, materialization,
/// activation, or ready evidence fails.
///
/// A successfully cleaned materialized process has its generic instance and
/// process binding removed atomically with the typed recovery transition. If
/// cleanup is incomplete, those rows remain as recovery evidence; no caller
/// may treat the target as accepted or active.
pub fn fail_target_launch(
    db: &HcomDb,
    supervisor: &SupervisorActor,
    handoff_id: &str,
    observation: &TargetLaunchFailure,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_supervisor_actor(supervisor)?;
    validate_expected_version(observation.expected_version)?;
    let launch_nonce = validate_opaque_id(&observation.launch_nonce, "launch nonce")?;
    let failure_kind = validate_text(
        &observation.failure_kind,
        "failure kind",
        MAX_FAILURE_KIND_BYTES,
        false,
    )?;
    let failure_reason = sanitize_reason(&observation.failure_reason)?;
    if observation.cleanup_completed && observation.identity.is_none() {
        return Err(HandoffError::Invalid(
            "target cleanup cannot complete without an exact prepared identity".to_string(),
        ));
    }
    let identity = observation
        .identity
        .as_ref()
        .map(|identity| {
            Ok::<_, HandoffError>(TargetFailureIdentity {
                instance_name: validate_text(
                    &identity.instance_name,
                    "instance identity",
                    MAX_INSTANCE_NAME_BYTES,
                    false,
                )?,
                hcom_session_id: validate_text(
                    &identity.hcom_session_id,
                    "hcom session identity",
                    MAX_IDENTITY_BYTES,
                    false,
                )?,
                process_id: validate_text(
                    &identity.process_id,
                    "process identity",
                    MAX_PROCESS_ID_BYTES,
                    false,
                )?,
                process_birth_identity: validate_text(
                    &identity.process_birth_identity,
                    "process birth identity",
                    MAX_IDENTITY_BYTES,
                    false,
                )?,
            })
        })
        .transpose()?;

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, _source, audit_actor) =
        load_supervisor_source_context(&tx, &handoff_id, supervisor, false)?;
    let effective_target = effective_target_generation(&tx, &handoff)?;
    let mut target =
        load_generation(&tx, &chain.id, effective_target)?.ok_or(HandoffError::Storage)?;
    let target_unmaterialized = target.wrapper_process_id.is_none()
        && target.process_birth_identity.is_none()
        && target.instance_name.is_none()
        && target.hcom_session_id.is_none()
        && target.native_session_id.is_none();
    let target_matches_identity = identity.as_ref().is_some_and(|identity| {
        target.wrapper_process_id.as_deref() == Some(identity.process_id.as_str())
            && target.process_birth_identity.as_deref()
                == Some(identity.process_birth_identity.as_str())
            && target.instance_name.as_deref() == Some(identity.instance_name.as_str())
            && target.hcom_session_id.as_deref() == Some(identity.hcom_session_id.as_str())
            && target.native_session_id.is_none()
    });
    if identity.is_none() && !target_unmaterialized
        || identity.is_some() && !target_unmaterialized && !target_matches_identity
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target failure identity does not match the reservation".to_string(),
        ));
    }

    let identity_instance = identity
        .as_ref()
        .map(|value| value.instance_name.as_str())
        .unwrap_or("");
    let identity_session = identity
        .as_ref()
        .map(|value| value.hcom_session_id.as_str())
        .unwrap_or("");
    let identity_process = identity
        .as_ref()
        .map(|value| value.process_id.as_str())
        .unwrap_or("");
    let identity_birth = identity
        .as_ref()
        .map(|value| value.process_birth_identity.as_str())
        .unwrap_or("");
    let cleanup_completed = u8::from(observation.cleanup_completed).to_string();
    let request_hash = hash_request(
        "target_launch_failure",
        &handoff_id,
        observation.expected_version,
        &audit_actor,
        &[
            launch_nonce.as_bytes(),
            identity_instance.as_bytes(),
            identity_session.as_bytes(),
            identity_process.as_bytes(),
            identity_birth.as_bytes(),
            cleanup_completed.as_bytes(),
            failure_kind.as_bytes(),
            failure_reason.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        observation.expected_version,
        handoff.version,
        handoff.state.as_str(),
        "target_launch_failure",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::LaunchingTarget
        || handoff.version != observation.expected_version
        || chain.state != ChainState::LaunchingTarget
        || chain.current_generation != effective_target
        || target.state != GenerationState::Launching
        || target.launch_nonce != launch_nonce
    {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }

    if observation.cleanup_completed
        && target_matches_identity
        && let Some(identity) = identity.as_ref()
    {
        let binding: Option<(String, String)> = tx
            .query_row(
                "SELECT session_id, instance_name FROM process_bindings WHERE process_id = ?1",
                [&identity.process_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if binding.as_ref().is_some_and(|(session, instance)| {
            session != &identity.hcom_session_id || instance != &identity.instance_name
        }) {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT target process binding changed before cleanup".to_string(),
            ));
        }
        let instance: Option<(String, String)> = tx
            .query_row(
                "SELECT session_id, tool FROM instances WHERE name = ?1",
                [&identity.instance_name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if instance
            .as_ref()
            .is_some_and(|(session, tool)| session != &identity.hcom_session_id || tool != "codex")
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT target instance changed before cleanup".to_string(),
            ));
        }
        tx.execute(
            "DELETE FROM process_bindings
             WHERE process_id = ?1 AND session_id = ?2 AND instance_name = ?3",
            params![
                identity.process_id,
                identity.hcom_session_id,
                identity.instance_name
            ],
        )?;
        tx.execute(
            "DELETE FROM instances WHERE name = ?1 AND session_id = ?2 AND tool = 'codex'",
            params![identity.instance_name, identity.hcom_session_id],
        )?;
    }

    let now = now_epoch_f64();
    let handoff_updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'needs_recovery', version = ?1,
             failure_kind = ?2, failure_reason = ?3, updated_at = ?4
         WHERE id = ?5 AND state = 'launching_target' AND version = ?6",
        params![
            observation.expected_version + 1,
            failure_kind,
            failure_reason,
            now,
            handoff_id,
            observation.expected_version
        ],
    )?;
    if handoff_updated != 1 {
        return Err(conflict(
            &handoff_id,
            observation.expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        observation.expected_version,
        Some(HandoffState::LaunchingTarget.as_str()),
        HandoffState::NeedsRecovery.as_str(),
        &audit_actor,
        "supervisor",
        "target_launch_failure",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut target,
        GenerationState::NeedsRecovery,
        &audit_actor,
        "supervisor",
        "target_launch_failure",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::NeedsRecovery,
        current_generation,
        &audit_actor,
        "supervisor",
        "target_launch_failure",
        &request_hash,
        now,
    )?;
    tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'failed', version = version + 1, updated_at = ?1
         WHERE handoff_id = ?2 AND target_generation = ?3
           AND state IN ('intent', 'authorized', 'materialized')",
        params![now, handoff_id, effective_target],
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

fn load_target_context(
    tx: &Transaction<'_>,
    handoff_id: &str,
    actor: &HandoffActor,
    require_live: bool,
) -> Result<(TerminalHandoff, TerminalChain, TerminalGeneration), HandoffError> {
    let handoff = load_handoff(tx, handoff_id)?
        .ok_or_else(|| HandoffError::Invalid("handoff was not found".to_string()))?;
    let chain = load_chain(tx, &handoff.chain_id)?.ok_or(HandoffError::Storage)?;
    let effective_target = effective_target_generation(tx, &handoff)?;
    let generation =
        load_generation(tx, &handoff.chain_id, effective_target)?.ok_or(HandoffError::Storage)?;
    if let Err(error) =
        authorize_generation(tx, &chain, &generation, actor, true, require_live, true)
    {
        return match error {
            HandoffError::Storage => Err(HandoffError::Storage),
            _ => Err(typed_conflict(
                "wrong_target_actor",
                "target actor does not match the exact current generation",
            )),
        };
    }
    if effective_target != actor.generation {
        return Err(typed_conflict(
            "wrong_target_actor",
            "target actor does not match the exact current generation",
        ));
    }
    Ok((handoff, chain, generation))
}

pub fn target_ready(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    launch_nonce: &str,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    let launch_nonce = validate_opaque_id(launch_nonce, "launch nonce")?;
    let native_session = actor.native_session_id.as_deref().ok_or_else(|| {
        HandoffError::Invalid("target native session must be pinned before ready".to_string())
    })?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let mut handoff = load_handoff(&tx, &handoff_id)?
        .ok_or_else(|| HandoffError::Invalid("handoff was not found".to_string()))?;
    let mut chain = load_chain(&tx, &handoff.chain_id)?.ok_or(HandoffError::Storage)?;
    let effective_target = effective_target_generation(&tx, &handoff)?;
    let mut target =
        load_generation(&tx, &handoff.chain_id, effective_target)?.ok_or(HandoffError::Storage)?;
    if actor.generation != effective_target
        || chain.current_generation != effective_target
        || target.launch_nonce != launch_nonce
        || native_session == handoff.source_native_session_id
        || actor.hcom_session_id == handoff.source_hcom_session_id
        || actor.process_id == handoff.source_wrapper_process_id
        || actor.process_birth_identity == handoff.source_process_birth_identity
        || !live_binding_matches(&tx, actor)?
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target ready identity is not a fresh match for the reservation"
                .to_string(),
        ));
    }
    let request_hash = hash_request(
        "target_ready",
        &handoff_id,
        expected_version,
        actor,
        &[launch_nonce.as_bytes(), native_session.as_bytes()],
    );
    if generation_matches_actor(&target, actor, true)
        && transition_is_replay(
            &tx,
            ("handoff", &handoff_id),
            expected_version,
            handoff.version,
            handoff.state.as_str(),
            "target_ready",
            &request_hash,
        )?
    {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::LaunchingTarget
        || handoff.version != expected_version
        || chain.state != ChainState::LaunchingTarget
        || target.state != GenerationState::Launching
        || target.wrapper_process_id.as_deref() != Some(actor.process_id.as_str())
        || target.process_birth_identity.as_deref() != Some(actor.process_birth_identity.as_str())
        || target.instance_name.as_deref() != Some(actor.instance_name.as_str())
        || target.hcom_session_id.as_deref() != Some(actor.hcom_session_id.as_str())
        || target.native_session_id.as_deref() != Some(native_session)
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let target_from_state = target.state;
    let target_from_version = target.version;
    let target_updated = tx.execute(
        "UPDATE terminal_generations
         SET state = 'awaiting_acceptance', version = ?1, updated_at = ?2
         WHERE chain_id = ?3 AND generation = ?4
           AND state = 'launching' AND version = ?5
           AND wrapper_process_id = ?6
           AND process_birth_identity = ?7
           AND instance_name = ?8
           AND hcom_session_id = ?9
           AND native_session_id = ?10
           AND launch_nonce = ?11",
        params![
            target_from_version + 1,
            now,
            target.chain_id,
            target.generation,
            target_from_version,
            actor.process_id,
            actor.process_birth_identity,
            actor.instance_name,
            actor.hcom_session_id,
            native_session,
            launch_nonce,
        ],
    )?;
    if target_updated != 1 {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target identity binding changed concurrently".to_string(),
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "generation",
        &generation_object_id(&chain.id, target.generation),
        target_from_version,
        Some(target_from_state.as_str()),
        GenerationState::AwaitingAcceptance.as_str(),
        actor,
        "target",
        "target_ready",
        &request_hash,
        now,
    )?;
    target.state = GenerationState::AwaitingAcceptance;
    target.version += 1;
    target.updated_at = now;

    let handoff_updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'awaiting_acceptance', version = ?1, updated_at = ?2
         WHERE id = ?3 AND state = 'launching_target' AND version = ?4",
        params![expected_version + 1, now, handoff_id, expected_version],
    )?;
    if handoff_updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::LaunchingTarget.as_str()),
        HandoffState::AwaitingAcceptance.as_str(),
        actor,
        "target",
        "target_ready",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::AwaitingAcceptance,
        current_generation,
        actor,
        "target",
        "target_ready",
        &request_hash,
        now,
    )?;
    tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'awaiting_acceptance', version = version + 1, updated_at = ?1
         WHERE handoff_id = ?2 AND target_generation = ?3
           AND state = 'materialized'",
        params![now, handoff_id, effective_target],
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn accept_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
) -> Result<HandoffOutcome, HandoffError> {
    accept_handoff_with_snapshot_provider(
        db,
        actor,
        handoff_id,
        expected_version,
        cwd,
        snapshot_workspace,
    )
}

pub fn inspect_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
) -> Result<HandoffInspection, HandoffError> {
    inspect_handoff_with_snapshot_provider(
        db,
        actor,
        handoff_id,
        expected_version,
        cwd,
        snapshot_workspace,
    )
}

#[derive(Debug, Clone)]
struct TargetValidationSnapshot {
    token: String,
    instructions_digest: String,
    validated_at: f64,
}

fn load_target_validation(
    conn: &Connection,
    handoff: &TerminalHandoff,
    target_generation: i64,
) -> Result<Option<TargetValidationSnapshot>, HandoffError> {
    if target_generation == handoff.target_generation
        && let (Some(token), Some(instructions_digest), Some(validated_at)) = (
            handoff.target_validation_token.clone(),
            handoff.target_instructions_digest.clone(),
            handoff.target_validated_at,
        )
    {
        return Ok(Some(TargetValidationSnapshot {
            token,
            instructions_digest,
            validated_at,
        }));
    }
    conn.query_row(
        "SELECT validation_token, instructions_digest, validated_at
         FROM terminal_target_validations
         WHERE handoff_id = ?1 AND target_generation = ?2",
        params![handoff.id, target_generation],
        |row| {
            Ok(TargetValidationSnapshot {
                token: row.get(0)?,
                instructions_digest: row.get(1)?,
                validated_at: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn map_target_actor_error(error: HandoffError) -> HandoffError {
    match error {
        HandoffError::Storage => HandoffError::Storage,
        HandoffError::Invalid(message) => HandoffError::Invalid(message),
        HandoffError::NotManaged
        | HandoffError::Conflict(_)
        | HandoffError::TypedConflict { .. } => typed_conflict(
            "wrong_target_actor",
            "caller does not match the exact current target generation",
        ),
    }
}

fn inspect_handoff_with_snapshot_provider<F>(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
    snapshot_provider: F,
) -> Result<HandoffInspection, HandoffError>
where
    F: FnOnce(&Path) -> Result<WorkspaceSnapshot, HandoffError>,
{
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    {
        let auth = Transaction::new_unchecked(db.conn(), TransactionBehavior::Deferred)?;
        let (handoff, chain, target) =
            load_target_context(&auth, &handoff_id, actor, true).map_err(map_target_actor_error)?;
        let validation = load_target_validation(&auth, &handoff, target.generation)?;
        let fresh_inspection = handoff.version == expected_version && validation.is_none();
        let replay_candidate = handoff.version == expected_version + 1 && validation.is_some();
        if handoff.state != HandoffState::AwaitingAcceptance
            || chain.state != ChainState::AwaitingAcceptance
            || target.state != GenerationState::AwaitingAcceptance
            || (!fresh_inspection && !replay_candidate)
        {
            return Err(typed_conflict(
                "wrong_expected_version_or_state",
                "handoff state or expected version does not permit inspection",
            ));
        }
        auth.commit()?;
    }
    let workspace = snapshot_provider(cwd).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    let preliminary = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::NotManaged)?;
    if !workspace_matches_handoff(&workspace, &preliminary) {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target workspace does not match the prepared snapshot".to_string(),
        ));
    }
    let preliminary_bundle = verify_pinned_bundle(db.conn(), &preliminary).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    let (instructions, instructions_digest) =
        load_current_instructions(&preliminary.workspace, &preliminary_bundle.value).map_err(
            |error| {
                if matches!(error, HandoffError::Storage) {
                    HandoffError::Storage
                } else {
                    typed_conflict(
                        "target_validation_changed",
                        "bundle, workspace, or project instructions changed after inspection",
                    )
                }
            },
        )?;

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, chain, target) =
        load_target_context(&tx, &handoff_id, actor, true).map_err(map_target_actor_error)?;
    let target_generation = target.generation;
    let existing_validation = load_target_validation(&tx, &handoff, target_generation)?;
    if !workspace_matches_handoff(&workspace, &handoff) {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target workspace changed during validation".to_string(),
        ));
    }
    let bundle = verify_pinned_bundle(&tx, &handoff).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    if bundle.digest != preliminary_bundle.digest
        || bundle.size_bytes != preliminary_bundle.size_bytes
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT pinned bundle changed during target validation".to_string(),
        ));
    }
    let request_hash = hash_request(
        "inspect_target",
        &handoff_id,
        expected_version,
        actor,
        &[
            workspace.workspace.as_bytes(),
            workspace.revision.as_bytes(),
            workspace.branch.as_bytes(),
            workspace.dirty_summary.as_bytes(),
            bundle.digest.as_bytes(),
            instructions_digest.as_bytes(),
            handoff.policy_ref.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "inspect_target",
        &request_hash,
    )? {
        if existing_validation
            .as_ref()
            .is_none_or(|validation| validation.instructions_digest != instructions_digest)
        {
            return Err(typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            ));
        }
        tx.commit()?;
        return Ok(HandoffInspection {
            handoff,
            bundle: bundle.value,
            instructions,
            instructions_digest,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::AwaitingAcceptance
        || handoff.version != expected_version
        || chain.state != ChainState::AwaitingAcceptance
        || target.state != GenerationState::AwaitingAcceptance
        || existing_validation.is_some()
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let validation_token = generate_id("validation");
    let now = now_epoch_f64();
    let updated = if target_generation == handoff.target_generation {
        tx.execute(
            "UPDATE terminal_handoffs
             SET version = ?1, target_validation_token = ?2,
                 target_instructions_digest = ?3, target_validated_at = ?4,
                 updated_at = ?4
             WHERE id = ?5 AND state = 'awaiting_acceptance' AND version = ?6
               AND target_validation_token IS NULL
               AND target_instructions_digest IS NULL
               AND target_validated_at IS NULL",
            params![
                expected_version + 1,
                validation_token,
                instructions_digest,
                now,
                handoff_id,
                expected_version,
            ],
        )?
    } else {
        tx.execute(
            "INSERT INTO terminal_target_validations (
                 handoff_id, target_generation, validation_token,
                 instructions_digest, validated_at
             ) SELECT ?1, ?2, ?3, ?4, ?5
               WHERE EXISTS(
                   SELECT 1 FROM terminal_handoffs
                   WHERE id = ?1 AND state = 'awaiting_acceptance'
                     AND version = ?6
               )",
            params![
                handoff_id,
                target_generation,
                validation_token,
                instructions_digest,
                now,
                expected_version,
            ],
        )?;
        tx.execute(
            "UPDATE terminal_handoffs
             SET version = ?1, updated_at = ?2
             WHERE id = ?3 AND state = 'awaiting_acceptance' AND version = ?4",
            params![expected_version + 1, now, handoff_id, expected_version],
        )?
    };
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &handoff.chain_id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::AwaitingAcceptance.as_str()),
        HandoffState::AwaitingAcceptance.as_str(),
        actor,
        "target",
        "inspect_target",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffInspection {
        handoff,
        bundle: bundle.value,
        instructions,
        instructions_digest,
        replayed: false,
    })
}

fn accept_handoff_with_snapshot_provider<F>(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    cwd: &Path,
    snapshot_provider: F,
) -> Result<HandoffOutcome, HandoffError>
where
    F: FnOnce(&Path) -> Result<WorkspaceSnapshot, HandoffError>,
{
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    {
        let auth = Transaction::new_unchecked(db.conn(), TransactionBehavior::Deferred)?;
        let (handoff, chain, target) =
            load_target_context(&auth, &handoff_id, actor, true).map_err(map_target_actor_error)?;
        if handoff.state == HandoffState::AwaitingAcceptance {
            if handoff.version != expected_version
                || chain.state != ChainState::AwaitingAcceptance
                || target.state != GenerationState::AwaitingAcceptance
            {
                return Err(typed_conflict(
                    "wrong_expected_version_or_state",
                    "handoff state or expected version does not permit acceptance",
                ));
            }
        } else if handoff.state != HandoffState::Accepted
            || chain.state != ChainState::Active
            || target.state != GenerationState::Active
        {
            return Err(typed_conflict(
                "wrong_expected_version_or_state",
                "handoff state or expected version does not permit acceptance",
            ));
        }
        if load_target_validation(&auth, &handoff, target.generation)?
            .is_none_or(|value| value.validated_at <= 0.0)
        {
            return Err(typed_conflict(
                "durable_inspection_required",
                "target must inspect the durable bundle and instructions before acceptance",
            ));
        }
        auth.commit()?;
    }
    let workspace = snapshot_provider(cwd).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    let preliminary = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::NotManaged)?;
    let preliminary_bundle = verify_pinned_bundle(db.conn(), &preliminary).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    let (_, instructions_digest) =
        load_current_instructions(&preliminary.workspace, &preliminary_bundle.value).map_err(
            |error| {
                if matches!(error, HandoffError::Storage) {
                    HandoffError::Storage
                } else {
                    typed_conflict(
                        "target_validation_changed",
                        "bundle, workspace, or project instructions changed after inspection",
                    )
                }
            },
        )?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut target) =
        load_target_context(&tx, &handoff_id, actor, true).map_err(map_target_actor_error)?;
    let target_generation = target.generation;
    let validation = load_target_validation(&tx, &handoff, target_generation)?;
    if !workspace_matches_handoff(&workspace, &handoff) || workspace.workspace != chain.workspace {
        return Err(typed_conflict(
            "target_validation_changed",
            "bundle, workspace, or project instructions changed after inspection",
        ));
    }
    let bundle = verify_pinned_bundle(&tx, &handoff).map_err(|error| {
        if matches!(error, HandoffError::Storage) {
            HandoffError::Storage
        } else {
            typed_conflict(
                "target_validation_changed",
                "bundle, workspace, or project instructions changed after inspection",
            )
        }
    })?;
    let bundle_event_id = bundle.event_id.to_string();
    let bundle_size = bundle.size_bytes.to_string();
    let request_hash = hash_request(
        "accept",
        &handoff_id,
        expected_version,
        actor,
        &[
            workspace.workspace.as_bytes(),
            workspace.revision.as_bytes(),
            workspace.branch.as_bytes(),
            workspace.dirty_summary.as_bytes(),
            bundle_event_id.as_bytes(),
            bundle.digest.as_bytes(),
            bundle_size.as_bytes(),
            handoff.policy_ref.as_bytes(),
            validation
                .as_ref()
                .map(|value| value.token.as_str())
                .unwrap_or("")
                .as_bytes(),
            instructions_digest.as_bytes(),
        ],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "accept",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::AwaitingAcceptance
        || handoff.version != expected_version
        || chain.state != ChainState::AwaitingAcceptance
        || target.state != GenerationState::AwaitingAcceptance
    {
        return Err(typed_conflict(
            "wrong_expected_version_or_state",
            "handoff state or expected version does not permit acceptance",
        ));
    }
    if validation
        .as_ref()
        .is_none_or(|value| value.validated_at <= 0.0)
    {
        return Err(typed_conflict(
            "durable_inspection_required",
            "target must inspect the durable bundle and instructions before acceptance",
        ));
    }
    if validation
        .as_ref()
        .is_none_or(|value| value.instructions_digest != instructions_digest)
    {
        return Err(typed_conflict(
            "target_validation_changed",
            "bundle, workspace, or project instructions changed after inspection",
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'accepted', version = ?1, accepted_at = ?2, updated_at = ?2
         WHERE id = ?3 AND state = 'awaiting_acceptance' AND version = ?4",
        params![expected_version + 1, now, handoff_id, expected_version],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::AwaitingAcceptance.as_str()),
        HandoffState::Accepted.as_str(),
        actor,
        "target",
        "accept",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut target,
        GenerationState::Active,
        actor,
        "target",
        "accept",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::Active,
        current_generation,
        actor,
        "target",
        "accept",
        &request_hash,
        now,
    )?;
    tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'active', version = version + 1, updated_at = ?1
         WHERE handoff_id = ?2 AND target_generation = ?3
           AND state = 'awaiting_acceptance'",
        params![now, handoff_id, target_generation],
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

fn workspace_matches_handoff(workspace: &WorkspaceSnapshot, handoff: &TerminalHandoff) -> bool {
    workspace.workspace == handoff.workspace
        && workspace.revision == handoff.revision
        && workspace.branch == handoff.branch
        && workspace.dirty_summary == handoff.dirty_summary
}

pub fn reject_handoff(
    db: &HcomDb,
    actor: &HandoffActor,
    handoff_id: &str,
    expected_version: i64,
    reason: &str,
    cwd: &Path,
) -> Result<HandoffOutcome, HandoffError> {
    let handoff_id = validate_opaque_id(handoff_id, "handoff ID")?;
    validate_actor(actor)?;
    validate_expected_version(expected_version)?;
    let reason = sanitize_reason(reason)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut target) = load_target_context(&tx, &handoff_id, actor, true)?;
    let workspace = ensure_workspace_matches(&chain, cwd)?;
    let request_hash = hash_request(
        "reject",
        &handoff_id,
        expected_version,
        actor,
        &[reason.as_bytes(), workspace.as_bytes()],
    );
    if transition_is_replay(
        &tx,
        ("handoff", &handoff_id),
        expected_version,
        handoff.version,
        handoff.state.as_str(),
        "reject",
        &request_hash,
    )? {
        tx.commit()?;
        return Ok(HandoffOutcome {
            handoff,
            replayed: true,
        });
    }
    if handoff.state != HandoffState::AwaitingAcceptance
        || handoff.version != expected_version
        || chain.state != ChainState::AwaitingAcceptance
        || target.state != GenerationState::AwaitingAcceptance
    {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE terminal_handoffs
         SET state = 'needs_recovery', version = ?1,
             failure_kind = 'target_rejected', failure_reason = ?2,
             updated_at = ?3
         WHERE id = ?4 AND state = 'awaiting_acceptance' AND version = ?5",
        params![
            expected_version + 1,
            reason,
            now,
            handoff_id,
            expected_version
        ],
    )?;
    if updated != 1 {
        return Err(conflict(
            &handoff_id,
            expected_version,
            handoff.state.as_str(),
            handoff.version,
        ));
    }
    insert_audit(
        &tx,
        &chain.id,
        "handoff",
        &handoff_id,
        expected_version,
        Some(HandoffState::AwaitingAcceptance.as_str()),
        HandoffState::NeedsRecovery.as_str(),
        actor,
        "target",
        "reject",
        &request_hash,
        now,
    )?;
    update_generation_state(
        &tx,
        &mut target,
        GenerationState::NeedsRecovery,
        actor,
        "target",
        "reject",
        &request_hash,
        now,
    )?;
    let current_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::NeedsRecovery,
        current_generation,
        actor,
        "target",
        "reject",
        &request_hash,
        now,
    )?;
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
}

pub fn chain_status_for_actor(
    db: &HcomDb,
    actor: &HandoffActor,
    requested_id: Option<&str>,
) -> Result<TerminalChain, HandoffError> {
    let Some((chain, _generation)) = find_actor_chain(db.conn(), actor)? else {
        return Err(HandoffError::NotManaged);
    };
    if let Some(requested_id) = requested_id {
        let requested_id = validate_opaque_id(requested_id, "chain ID")?;
        if requested_id != chain.id {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT requested chain does not match caller identity".to_string(),
            ));
        }
    }
    Ok(chain)
}

pub fn handoff_status_for_actor(
    db: &HcomDb,
    actor: &HandoffActor,
    requested_id: Option<&str>,
) -> Result<TerminalHandoff, HandoffError> {
    let chain = chain_status_for_actor(db, actor, None)?;
    let handoff = if let Some(requested_id) = requested_id {
        let requested_id = validate_opaque_id(requested_id, "handoff ID")?;
        load_handoff(db.conn(), &requested_id)?.filter(|handoff| handoff.chain_id == chain.id)
    } else {
        db.conn()
            .query_row(
                "SELECT * FROM terminal_handoffs
                 WHERE chain_id = ?1
                 ORDER BY created_at DESC LIMIT 1",
                params![chain.id],
                TerminalHandoff::from_row,
            )
            .optional()?
    };
    handoff.ok_or_else(|| HandoffError::Invalid("handoff was not found".to_string()))
}

fn terminal_owner_error() -> HandoffError {
    typed_conflict(
        "not_found_or_not_owner",
        "chain was not found or is not owned by this foreground terminal",
    )
}

fn public_chain_for_terminal_owner(
    db: &HcomDb,
    requested_id: Option<&str>,
    owner: &TerminalOwnerEvidence,
    require_old_supervisor_absent: bool,
) -> Result<TerminalChain, HandoffError> {
    if owner.supervisor_pid <= 1
        || owner.supervisor_pgid <= 0
        || owner.outer_foreground_pgid <= 0
        || owner.supervisor_pgid != owner.outer_foreground_pgid
        || owner.outer_tty_device <= 0
        || owner.outer_tty_inode <= 0
    {
        return Err(terminal_owner_error());
    }
    let workspace = canonical_workspace(&owner.workspace).map_err(|_| terminal_owner_error())?;
    let chain = if let Some(id) = requested_id {
        let id = validate_opaque_id(id, "chain ID").map_err(|_| terminal_owner_error())?;
        db.conn()
            .query_row(
                "SELECT c.*
                 FROM terminal_chains c
                 JOIN terminal_chain_claims p ON p.chain_id = c.id
                 WHERE c.id = ?1 AND p.state IN ('active', 'released')
                   AND p.workspace = ?2
                   AND p.outer_tty_device = ?3
                   AND p.outer_tty_inode = ?4",
                params![id, workspace, owner.outer_tty_device, owner.outer_tty_inode,],
                TerminalChain::from_row,
            )
            .optional()?
    } else {
        db.conn()
            .query_row(
                "SELECT c.*
                 FROM terminal_chains c
                 JOIN terminal_chain_claims p ON p.chain_id = c.id
                 WHERE p.state IN ('active', 'released') AND p.workspace = ?1
                   AND p.outer_tty_device = ?2 AND p.outer_tty_inode = ?3
                 ORDER BY CASE p.state WHEN 'active' THEN 0 ELSE 1 END,
                          p.updated_at DESC
                 LIMIT 1",
                params![workspace, owner.outer_tty_device, owner.outer_tty_inode,],
                TerminalChain::from_row,
            )
            .optional()?
    }
    .ok_or_else(terminal_owner_error)?;
    if chain.workspace != workspace {
        return Err(terminal_owner_error());
    }
    let binding = load_current_supervisor_binding(db.conn(), &chain)?;
    if binding.outer_tty_device != owner.outer_tty_device
        || binding.outer_tty_inode != owner.outer_tty_inode
    {
        return Err(terminal_owner_error());
    }
    if require_old_supervisor_absent {
        match hcom::chain_supervisor::exact_process_status(
            i32::try_from(binding.pid).map_err(|_| terminal_owner_error())?,
            &binding.process_birth_identity,
        ) {
            hcom::chain_supervisor::ExactProcessStatus::Absent
            | hcom::chain_supervisor::ExactProcessStatus::Reused => {}
            _ => return Err(terminal_owner_error()),
        }
    }
    Ok(chain)
}

pub fn public_chain_claim_released(db: &HcomDb, chain_id: &str) -> Result<bool, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    db.conn()
        .query_row(
            "SELECT state = 'released' FROM terminal_chain_claims WHERE chain_id = ?1",
            params![chain_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(HandoffError::NotManaged)
}

pub fn chain_status_for_terminal_owner(
    db: &HcomDb,
    requested_id: Option<&str>,
    owner: &TerminalOwnerEvidence,
) -> Result<TerminalChain, HandoffError> {
    public_chain_for_terminal_owner(db, requested_id, owner, true)
}

pub fn handoff_status_for_terminal_owner(
    db: &HcomDb,
    requested_id: Option<&str>,
    owner: &TerminalOwnerEvidence,
) -> Result<TerminalHandoff, HandoffError> {
    if let Some(id) = requested_id {
        let id = validate_opaque_id(id, "handoff ID").map_err(|_| terminal_owner_error())?;
        let handoff = load_handoff(db.conn(), &id)?.ok_or_else(terminal_owner_error)?;
        let chain = public_chain_for_terminal_owner(db, Some(&handoff.chain_id), owner, true)?;
        if handoff.chain_id != chain.id {
            return Err(terminal_owner_error());
        }
        return Ok(handoff);
    }
    let chain = public_chain_for_terminal_owner(db, None, owner, true)?;
    db.conn()
        .query_row(
            "SELECT * FROM terminal_handoffs
             WHERE chain_id = ?1 ORDER BY created_at DESC LIMIT 1",
            params![chain.id],
            TerminalHandoff::from_row,
        )
        .optional()?
        .ok_or_else(terminal_owner_error)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryAbsenceEntry {
    subject: &'static str,
    generation: Option<i64>,
    pid: Option<i64>,
    pgid: Option<i64>,
    process_birth_identity: Option<String>,
    method: &'static str,
}

fn recovery_process_error(status: hcom::chain_supervisor::ExactProcessStatus) -> HandoffError {
    use hcom::chain_supervisor::ExactProcessStatus;
    match status {
        ExactProcessStatus::LiveExact => typed_conflict(
            RecoveryPlanCode::LiveProcessConflict.as_str(),
            "old supervisor or generation process is still live; recovery performed no action",
        ),
        ExactProcessStatus::Reused => typed_conflict(
            RecoveryPlanCode::ProcessIdentityReused.as_str(),
            "a durable PID or process group identity was reused; manual intervention is required",
        ),
        ExactProcessStatus::Unknown | ExactProcessStatus::Absent => typed_conflict(
            RecoveryPlanCode::AbsenceUnknown.as_str(),
            "old process absence could not be proved; manual intervention is required",
        ),
    }
}

fn observe_exact_recovery_absence(
    conn: &Connection,
    chain: &TerminalChain,
    generation: &TerminalGeneration,
) -> Result<Vec<RecoveryAbsenceEntry>, HandoffError> {
    use hcom::chain_supervisor::{ExactProcessStatus, exact_process_status, process_group_status};

    let supervisor = load_current_supervisor_binding(conn, chain)?;
    let supervisor_pid = i32::try_from(supervisor.pid)
        .map_err(|_| recovery_process_error(ExactProcessStatus::Unknown))?;
    match exact_process_status(supervisor_pid, &supervisor.process_birth_identity) {
        ExactProcessStatus::Absent => {}
        status => return Err(recovery_process_error(status)),
    }
    let mut evidence = vec![RecoveryAbsenceEntry {
        subject: "supervisor",
        generation: None,
        pid: Some(supervisor.pid),
        pgid: None,
        process_birth_identity: Some(supervisor.process_birth_identity),
        method: "proc_birth_missing",
    }];
    let supervisor_pgid = i32::try_from(supervisor.pgid)
        .map_err(|_| recovery_process_error(ExactProcessStatus::Unknown))?;
    match process_group_status(supervisor_pgid) {
        ExactProcessStatus::Absent => evidence.push(RecoveryAbsenceEntry {
            subject: "supervisor_process_group",
            generation: None,
            pid: None,
            pgid: Some(supervisor.pgid),
            process_birth_identity: None,
            method: "process_group_missing",
        }),
        status => return Err(recovery_process_error(status)),
    }

    let identities_absent = generation.wrapper_process_id.is_none()
        && generation.process_birth_identity.is_none()
        && generation.instance_name.is_none()
        && generation.hcom_session_id.is_none()
        && generation.native_session_id.is_none();
    let process = load_generation_process(conn, &chain.id, generation.generation)?;
    if identities_absent {
        if process.is_some() {
            return Err(recovery_process_error(ExactProcessStatus::Unknown));
        }
        return Ok(evidence);
    }
    let process = process.ok_or_else(|| recovery_process_error(ExactProcessStatus::Unknown))?;
    if generation.process_birth_identity.as_deref() != Some(process.wrapper_birth_identity.as_str())
        || process.chain_id != chain.id
        || process.generation != generation.generation
    {
        return Err(recovery_process_error(ExactProcessStatus::Unknown));
    }
    if let Some((session_id, instance_name)) = conn
        .query_row(
            "SELECT session_id, instance_name
             FROM process_bindings WHERE process_id = ?1",
            params![generation.wrapper_process_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?
        && (session_id.as_deref() != generation.hcom_session_id.as_deref()
            || instance_name.as_deref() != generation.instance_name.as_deref())
    {
        return Err(recovery_process_error(ExactProcessStatus::Unknown));
    }

    for (subject, pid, birth) in [
        (
            "wrapper",
            process.wrapper_pid,
            process.wrapper_birth_identity.as_str(),
        ),
        (
            "child",
            process.child_pid,
            process.child_birth_identity.as_str(),
        ),
    ] {
        let pid32 =
            i32::try_from(pid).map_err(|_| recovery_process_error(ExactProcessStatus::Unknown))?;
        match exact_process_status(pid32, birth) {
            ExactProcessStatus::Absent => evidence.push(RecoveryAbsenceEntry {
                subject,
                generation: Some(generation.generation),
                pid: Some(pid),
                pgid: None,
                process_birth_identity: Some(birth.to_string()),
                method: "proc_birth_missing",
            }),
            status => return Err(recovery_process_error(status)),
        }
    }
    let child_pgid = i32::try_from(process.child_pgid)
        .map_err(|_| recovery_process_error(ExactProcessStatus::Unknown))?;
    match process_group_status(child_pgid) {
        ExactProcessStatus::Absent => evidence.push(RecoveryAbsenceEntry {
            subject: "child_process_group",
            generation: Some(generation.generation),
            pid: None,
            pgid: Some(process.child_pgid),
            process_birth_identity: None,
            method: "process_group_missing",
        }),
        status => return Err(recovery_process_error(status)),
    }
    Ok(evidence)
}

fn recovery_plan(
    chain: &TerminalChain,
    generation: &TerminalGeneration,
    handoff: Option<&TerminalHandoff>,
    prepare_started: bool,
) -> RecoveryPlanCode {
    match (chain.state, handoff.map(|value| value.state)) {
        (ChainState::Active | ChainState::Prepared, _) | (_, Some(HandoffState::Prepared)) => {
            RecoveryPlanCode::SourceDeadBeforeCommit
        }
        (
            ChainState::Committed | ChainState::StopObserved | ChainState::QuiescingSource,
            Some(
                HandoffState::Committed
                | HandoffState::StopObserved
                | HandoffState::QuiescingSource,
            ),
        ) => RecoveryPlanCode::ContinueAfterSourceAbsence,
        (ChainState::LaunchingTarget, Some(HandoffState::LaunchingTarget)) => {
            if generation.wrapper_process_id.is_none() && !prepare_started {
                RecoveryPlanCode::RetryUnmaterializedTarget
            } else if generation.wrapper_process_id.is_some() && prepare_started {
                RecoveryPlanCode::ReplaceDeadTarget
            } else {
                RecoveryPlanCode::AbsenceUnknown
            }
        }
        (ChainState::AwaitingAcceptance, Some(HandoffState::AwaitingAcceptance)) => {
            if generation.wrapper_process_id.is_some() && prepare_started {
                RecoveryPlanCode::ReplaceDeadAwaitingAcceptance
            } else {
                RecoveryPlanCode::AbsenceUnknown
            }
        }
        (ChainState::LaunchingTarget | ChainState::NeedsRecovery, None)
            if matches!(
                generation.state,
                GenerationState::Reserved
                    | GenerationState::Launching
                    | GenerationState::NeedsRecovery
            ) && generation.native_session_id.is_none()
                && generation.wrapper_process_id.is_none()
                && !prepare_started =>
        {
            RecoveryPlanCode::RetryInitialGeneration
        }
        (ChainState::LaunchingTarget | ChainState::NeedsRecovery, None)
            if matches!(
                generation.state,
                GenerationState::Reserved
                    | GenerationState::Launching
                    | GenerationState::NeedsRecovery
            ) =>
        {
            RecoveryPlanCode::AbsenceUnknown
        }
        (ChainState::NeedsRecovery, Some(_))
            if handoff.is_some_and(|handoff| {
                handoff.failure_kind == "exit_without_stop"
                    || handoff.failure_kind == "sigterm_timeout"
                    || handoff.failure_kind == "supervisor_shutdown"
                    || handoff.failure_kind == "outer_hangup"
                    || handoff.failure_kind == "target_rejected"
                    || handoff.failure_kind.starts_with("target_")
            }) =>
        {
            let handoff = handoff.expect("guard requires a handoff");
            if generation.generation == handoff.source_generation {
                RecoveryPlanCode::ContinueAfterSourceAbsence
            } else if generation.wrapper_process_id.is_none() && !prepare_started {
                RecoveryPlanCode::RetryUnmaterializedTarget
            } else if generation.wrapper_process_id.is_some()
                && prepare_started
                && generation.state == GenerationState::AwaitingAcceptance
            {
                RecoveryPlanCode::ReplaceDeadAwaitingAcceptance
            } else if generation.wrapper_process_id.is_some() && prepare_started {
                RecoveryPlanCode::ReplaceDeadTarget
            } else {
                RecoveryPlanCode::AbsenceUnknown
            }
        }
        _ => RecoveryPlanCode::UnsupportedRecoveryState,
    }
}

fn insert_recovery_absence_evidence(
    tx: &Transaction<'_>,
    attempt_id: &str,
    evidence: &[RecoveryAbsenceEntry],
    observed_at: f64,
) -> Result<(), HandoffError> {
    for entry in evidence {
        tx.execute(
            "INSERT INTO terminal_recovery_absence_evidence (
                 recovery_attempt_id, subject, generation, pid, pgid,
                 process_birth_identity, observation, method, observed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'absent', ?7, ?8)",
            params![
                attempt_id,
                entry.subject,
                entry.generation,
                entry.pid,
                entry.pgid,
                entry.process_birth_identity,
                entry.method,
                observed_at,
            ],
        )?;
    }
    Ok(())
}

fn delete_exact_absent_generic_binding(
    tx: &Transaction<'_>,
    generation: &TerminalGeneration,
) -> Result<(), HandoffError> {
    let (Some(process_id), Some(session_id), Some(instance_name)) = (
        generation.wrapper_process_id.as_deref(),
        generation.hcom_session_id.as_deref(),
        generation.instance_name.as_deref(),
    ) else {
        return Ok(());
    };
    tx.execute(
        "DELETE FROM process_bindings
         WHERE process_id = ?1 AND session_id = ?2 AND instance_name = ?3",
        params![process_id, session_id, instance_name],
    )?;
    tx.execute(
        "DELETE FROM instances
         WHERE name = ?1 AND session_id = ?2 AND tool = 'codex'",
        params![instance_name, session_id],
    )?;
    Ok(())
}

pub fn begin_public_recovery(
    db: &HcomDb,
    chain_id: &str,
    expected_chain_version: i64,
    owner: &TerminalOwnerEvidence,
) -> Result<RecoveryOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_expected_version(expected_chain_version)?;
    validate_supervisor_actor(&owner.supervisor)?;
    let preliminary = public_chain_for_terminal_owner(db, Some(&chain_id), owner, false)?;
    if preliminary.version != expected_chain_version {
        return Err(typed_conflict(
            "wrong_expected_version_or_state",
            "chain state or expected version does not permit recovery",
        ));
    }
    if public_chain_claim_released(db, &chain_id)? {
        return Ok(RecoveryOutcome::Manual {
            chain: Box::new(preliminary),
            reason: RecoveryPlanCode::SourceDeadBeforeCommit,
        });
    }
    let preliminary_generation =
        load_generation(db.conn(), &chain_id, preliminary.current_generation)?
            .ok_or(HandoffError::Storage)?;
    let preliminary_handoff = get_open_handoff_for_chain_tx(db.conn(), &chain_id)?;
    let preliminary_prepare_started =
        load_generation_prepare_intent(db.conn(), &chain_id, preliminary.current_generation)?
            .is_some();
    let plan = recovery_plan(
        &preliminary,
        &preliminary_generation,
        preliminary_handoff.as_ref(),
        preliminary_prepare_started,
    );
    let absence = observe_exact_recovery_absence(db.conn(), &preliminary, &preliminary_generation)?;
    let now = now_epoch_f64();
    let attempt_id = generate_id("recovery");
    let launch_nonce = generate_id("launch");

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let mut chain = load_chain(&tx, &chain_id)?.ok_or_else(terminal_owner_error)?;
    let mut generation =
        load_generation(&tx, &chain_id, chain.current_generation)?.ok_or(HandoffError::Storage)?;
    let mut handoff = get_open_handoff_for_chain_tx(&tx, &chain_id)?;
    let transaction_prepare_started =
        load_generation_prepare_intent(&tx, &chain_id, chain.current_generation)?.is_some();
    let binding = load_current_supervisor_binding(&tx, &chain)?;
    let preliminary_binding = load_current_supervisor_binding(db.conn(), &preliminary)?;
    if chain.version != expected_chain_version
        || chain.state != preliminary.state
        || chain.current_generation != preliminary.current_generation
        || generation.version != preliminary_generation.version
        || transaction_prepare_started != preliminary_prepare_started
        || binding != preliminary_binding
        || handoff
            .as_ref()
            .map(|value| (&value.id, value.version, value.state))
            != preliminary_handoff
                .as_ref()
                .map(|value| (&value.id, value.version, value.state))
    {
        return Err(typed_conflict(
            "wrong_expected_version_or_state",
            "chain state or expected version does not permit recovery",
        ));
    }
    if recovery_plan(
        &chain,
        &generation,
        handoff.as_ref(),
        transaction_prepare_started,
    ) != plan
    {
        return Err(typed_conflict(
            "wrong_expected_version_or_state",
            "chain recovery plan changed during authorization",
        ));
    }
    let transaction_absence = observe_exact_recovery_absence(&tx, &chain, &generation)?;
    if transaction_absence != absence {
        return Err(typed_conflict(
            RecoveryPlanCode::AbsenceUnknown.as_str(),
            "old process absence changed during recovery authorization",
        ));
    }
    let sequence: i64 = tx.query_row(
        "SELECT COALESCE(MAX(sequence), 0) + 1
         FROM terminal_recovery_attempts WHERE chain_id = ?1",
        params![chain_id],
        |row| row.get(0),
    )?;
    let audit_actor = HandoffActor {
        instance_name: "chain-recovery".to_string(),
        hcom_session_id: chain_id.clone(),
        native_session_id: None,
        process_id: owner.supervisor.process_id.clone(),
        process_birth_identity: owner.supervisor.process_birth_identity.clone(),
        generation: generation.generation,
    };
    let request_hash = hash_request(
        "begin_public_recovery",
        &chain_id,
        expected_chain_version,
        &audit_actor,
        &[
            plan.as_str().as_bytes(),
            attempt_id.as_bytes(),
            generation.launch_nonce.as_bytes(),
        ],
    );

    if !plan.permits_spawn() {
        tx.execute(
            "INSERT INTO terminal_recovery_attempts (
                 id, chain_id, sequence, requested_chain_version, handoff_id,
                 replaced_generation, target_generation, plan_code, state,
                 version, supervisor_process_id,
                 supervisor_process_birth_identity, supervisor_pid,
                 supervisor_pgid, outer_foreground_pgid, outer_tty_device,
                 outer_tty_inode, created_at, updated_at
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'manual', 0,
                 ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?15
             )",
            params![
                attempt_id,
                chain_id,
                sequence,
                expected_chain_version,
                handoff.as_ref().map(|value| value.id.as_str()),
                generation.generation,
                plan.as_str(),
                owner.supervisor.process_id,
                owner.supervisor.process_birth_identity,
                owner.supervisor_pid,
                owner.supervisor_pgid,
                owner.outer_foreground_pgid,
                owner.outer_tty_device,
                owner.outer_tty_inode,
                now,
            ],
        )?;
        insert_recovery_absence_evidence(&tx, &attempt_id, &transaction_absence, now)?;
        if let Some(current) = handoff.as_mut()
            && current.state != HandoffState::NeedsRecovery
        {
            let from_state = current.state;
            let from_version = current.version;
            tx.execute(
                "UPDATE terminal_handoffs
                 SET state = 'needs_recovery', version = ?1,
                     failure_kind = 'source_absent_before_commit',
                     failure_reason = 'source absence requires explicit operator action',
                     updated_at = ?2
                 WHERE id = ?3 AND state = ?4 AND version = ?5",
                params![
                    from_version + 1,
                    now,
                    current.id,
                    from_state.as_str(),
                    from_version,
                ],
            )?;
            insert_audit(
                &tx,
                &chain_id,
                "handoff",
                &current.id,
                from_version,
                Some(from_state.as_str()),
                HandoffState::NeedsRecovery.as_str(),
                &audit_actor,
                "supervisor",
                "begin_public_recovery_manual",
                &request_hash,
                now,
            )?;
        }
        if generation.state != GenerationState::NeedsRecovery {
            update_generation_state(
                &tx,
                &mut generation,
                GenerationState::NeedsRecovery,
                &audit_actor,
                "supervisor",
                "begin_public_recovery_manual",
                &request_hash,
                now,
            )?;
        }
        let current_generation = chain.current_generation;
        update_chain_state(
            &tx,
            &mut chain,
            ChainState::NeedsRecovery,
            current_generation,
            &audit_actor,
            "supervisor",
            "begin_public_recovery_manual",
            &request_hash,
            now,
        )?;
        if plan == RecoveryPlanCode::SourceDeadBeforeCommit {
            let claim_version: i64 = tx.query_row(
                "SELECT version FROM terminal_chain_claims
                 WHERE chain_id = ?1 AND state = 'active'",
                params![chain_id],
                |row| row.get(0),
            )?;
            let released = tx.execute(
                "UPDATE terminal_chain_claims
                 SET state = 'released', version = ?1,
                     updated_at = ?2, released_at = ?2
                 WHERE chain_id = ?3 AND state = 'active' AND version = ?4",
                params![claim_version + 1, now, chain_id, claim_version],
            )?;
            if released != 1 {
                return Err(typed_conflict(
                    "recovery_intent_changed",
                    "public chain claim changed before explicit abandonment",
                ));
            }
            insert_audit(
                &tx,
                &chain_id,
                "chain",
                &format!("{chain_id}:claim"),
                claim_version,
                Some("active"),
                "released",
                &audit_actor,
                "supervisor",
                "release_public_chain_claim",
                &request_hash,
                now,
            )?;
        }
        delete_exact_absent_generic_binding(&tx, &generation)?;
        tx.commit()?;
        return Ok(RecoveryOutcome::Manual {
            chain: Box::new(load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::Storage)?),
            reason: plan,
        });
    }

    let target_generation: i64 = tx.query_row(
        "SELECT COALESCE(MAX(generation), 0) + 1
         FROM terminal_generations WHERE chain_id = ?1",
        params![chain_id],
        |row| row.get(0),
    )?;
    let target_state = if plan == RecoveryPlanCode::RetryInitialGeneration {
        GenerationState::Reserved
    } else {
        GenerationState::Launching
    };
    tx.execute(
        "INSERT INTO terminal_generations (
             chain_id, generation, launch_nonce, state, version,
             created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5)",
        params![
            chain_id,
            target_generation,
            launch_nonce,
            target_state.as_str(),
            now,
        ],
    )?;
    tx.execute(
        "INSERT INTO terminal_recovery_attempts (
             id, chain_id, sequence, requested_chain_version, handoff_id,
             replaced_generation, target_generation, plan_code, state,
             version, supervisor_process_id,
             supervisor_process_birth_identity, supervisor_pid,
             supervisor_pgid, outer_foreground_pgid, outer_tty_device,
             outer_tty_inode, created_at, updated_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'intent', 0,
             ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16
         )",
        params![
            attempt_id,
            chain_id,
            sequence,
            expected_chain_version,
            handoff.as_ref().map(|value| value.id.as_str()),
            generation.generation,
            target_generation,
            plan.as_str(),
            owner.supervisor.process_id,
            owner.supervisor.process_birth_identity,
            owner.supervisor_pid,
            owner.supervisor_pgid,
            owner.outer_foreground_pgid,
            owner.outer_tty_device,
            owner.outer_tty_inode,
            now,
        ],
    )?;
    insert_recovery_absence_evidence(&tx, &attempt_id, &transaction_absence, now)?;
    if generation.state != GenerationState::NeedsRecovery {
        update_generation_state(
            &tx,
            &mut generation,
            GenerationState::NeedsRecovery,
            &audit_actor,
            "supervisor",
            "begin_public_recovery",
            &request_hash,
            now,
        )?;
    }
    if let Some(current) = handoff.as_mut() {
        let from_state = current.state;
        let from_version = current.version;
        let updated = tx.execute(
            "UPDATE terminal_handoffs
             SET state = 'launching_target', version = ?1, updated_at = ?2
             WHERE id = ?3 AND state = ?4 AND version = ?5",
            params![
                from_version + 1,
                now,
                current.id,
                from_state.as_str(),
                from_version,
            ],
        )?;
        if updated != 1 {
            return Err(typed_conflict(
                "wrong_expected_version_or_state",
                "chain state or expected version does not permit recovery",
            ));
        }
        insert_audit(
            &tx,
            &chain_id,
            "handoff",
            &current.id,
            from_version,
            Some(from_state.as_str()),
            HandoffState::LaunchingTarget.as_str(),
            &audit_actor,
            "supervisor",
            "begin_public_recovery",
            &request_hash,
            now,
        )?;
        current.state = HandoffState::LaunchingTarget;
        current.version += 1;
    }
    let replaced_generation = chain.current_generation;
    update_chain_state(
        &tx,
        &mut chain,
        ChainState::LaunchingTarget,
        target_generation,
        &audit_actor,
        "supervisor",
        "begin_public_recovery",
        &request_hash,
        now,
    )?;
    let target_audit_actor = HandoffActor {
        generation: target_generation,
        ..audit_actor.clone()
    };
    insert_audit(
        &tx,
        &chain_id,
        "generation",
        &generation_object_id(&chain_id, target_generation),
        -1,
        None,
        target_state.as_str(),
        &target_audit_actor,
        "supervisor",
        "reserve_recovery_generation",
        &request_hash,
        now,
    )?;
    delete_exact_absent_generic_binding(&tx, &generation)?;
    tx.commit()?;

    let chain = load_chain(db.conn(), &chain_id)?.ok_or(HandoffError::Storage)?;
    debug_assert_eq!(replaced_generation, preliminary.current_generation);
    let generation =
        load_generation(db.conn(), &chain_id, target_generation)?.ok_or(HandoffError::Storage)?;
    let handoff = get_open_handoff_for_chain_tx(db.conn(), &chain_id)?;
    Ok(RecoveryOutcome::Launch(Box::new(RecoveryReservation {
        attempt_id,
        chain,
        plan,
        handoff_id: handoff.as_ref().map(|value| value.id.clone()),
        handoff_version: handoff.as_ref().map(|value| value.version),
        generation,
    })))
}

/// Recheck the append-only absence proof immediately before a recovery caller
/// prepares any wrapper. The durable intent is necessary but not sufficient:
/// a reused PID/process group or contradictory generic binding that appears
/// after the intent still blocks all process action.
pub fn revalidate_recovery_absence(
    db: &HcomDb,
    attempt_id: &str,
    supervisor: &SupervisorActor,
) -> Result<(), HandoffError> {
    use hcom::chain_supervisor::{ExactProcessStatus, exact_process_status, process_group_status};

    let attempt_id = validate_opaque_id(attempt_id, "recovery attempt ID")?;
    validate_supervisor_actor(supervisor)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (
        chain_id,
        replaced_generation,
        target_generation,
        state,
        process_id,
        process_birth,
        supervisor_pid,
    ): (String, i64, i64, String, String, String, i64) = tx
        .query_row(
            "SELECT chain_id, replaced_generation, target_generation, state,
                    supervisor_process_id, supervisor_process_birth_identity,
                    supervisor_pid
             FROM terminal_recovery_attempts WHERE id = ?1",
            params![attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(terminal_owner_error)?;
    if state != "intent"
        || process_id != supervisor.process_id
        || process_birth != supervisor.process_birth_identity
        || exact_process_status(
            i32::try_from(supervisor_pid).map_err(|_| terminal_owner_error())?,
            &process_birth,
        ) != ExactProcessStatus::LiveExact
    {
        return Err(typed_conflict(
            "recovery_intent_changed",
            "recovery intent or new supervisor identity changed before process preparation",
        ));
    }
    let chain = load_chain(&tx, &chain_id)?.ok_or_else(terminal_owner_error)?;
    let binding = load_current_supervisor_binding(&tx, &chain)?;
    if chain.current_generation != target_generation
        || binding.process_id != supervisor.process_id
        || binding.process_birth_identity != supervisor.process_birth_identity
    {
        return Err(typed_conflict(
            "recovery_intent_changed",
            "recovery intent or current generation changed before process preparation",
        ));
    }
    let replaced =
        load_generation(&tx, &chain_id, replaced_generation)?.ok_or(HandoffError::Storage)?;
    let expected_process = load_generation_process(&tx, &chain_id, replaced_generation)?;
    let mut subjects = std::collections::BTreeSet::new();
    {
        let mut statement = tx.prepare(
            "SELECT subject, pid, pgid, process_birth_identity
             FROM terminal_recovery_absence_evidence
             WHERE recovery_attempt_id = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map(params![attempt_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (subject, pid, pgid, birth) = row?;
            if !subjects.insert(subject.clone()) {
                return Err(typed_conflict(
                    RecoveryPlanCode::AbsenceUnknown.as_str(),
                    "recovery absence evidence is incomplete or contradictory",
                ));
            }
            let status = if subject.ends_with("_process_group") {
                process_group_status(
                    i32::try_from(pgid.ok_or(HandoffError::Storage)?)
                        .map_err(|_| HandoffError::Storage)?,
                )
            } else {
                exact_process_status(
                    i32::try_from(pid.ok_or(HandoffError::Storage)?)
                        .map_err(|_| HandoffError::Storage)?,
                    birth.as_deref().ok_or(HandoffError::Storage)?,
                )
            };
            if status != ExactProcessStatus::Absent {
                return Err(recovery_process_error(status));
            }
        }
    }
    let expected_subjects: std::collections::BTreeSet<String> = if expected_process.is_some() {
        [
            "supervisor",
            "supervisor_process_group",
            "wrapper",
            "child",
            "child_process_group",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    } else {
        ["supervisor", "supervisor_process_group"]
            .into_iter()
            .map(str::to_string)
            .collect()
    };
    if subjects != expected_subjects {
        return Err(typed_conflict(
            RecoveryPlanCode::AbsenceUnknown.as_str(),
            "recovery absence evidence is incomplete or contradictory",
        ));
    }
    if let Some(process_id) = replaced.wrapper_process_id.as_deref() {
        let generic_binding_exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM process_bindings WHERE process_id = ?1)",
            params![process_id],
            |row| row.get(0),
        )?;
        if generic_binding_exists {
            return Err(typed_conflict(
                RecoveryPlanCode::AbsenceUnknown.as_str(),
                "generic process binding changed after recovery authorization",
            ));
        }
    }
    if let (Some(instance), Some(session)) = (
        replaced.instance_name.as_deref(),
        replaced.hcom_session_id.as_deref(),
    ) {
        let generic_instance_exists: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM instances WHERE name = ?1 AND session_id = ?2
             )",
            params![instance, session],
            |row| row.get(0),
        )?;
        if generic_instance_exists {
            return Err(typed_conflict(
                RecoveryPlanCode::AbsenceUnknown.as_str(),
                "generic instance binding changed after recovery authorization",
            ));
        }
    }
    let updated = tx.execute(
        "UPDATE terminal_recovery_attempts
         SET state = 'authorized', version = version + 1, updated_at = ?1
         WHERE id = ?2 AND state = 'intent'",
        params![now_epoch_f64(), attempt_id],
    )?;
    if updated != 1 {
        return Err(typed_conflict(
            "recovery_intent_changed",
            "recovery intent changed before process preparation",
        ));
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    struct Fixture {
        _dir: TempDir,
        db: HcomDb,
        workspace: PathBuf,
        source: HandoffActor,
        chain: TerminalChain,
    }

    struct PublicFixture {
        _dir: TempDir,
        db: HcomDb,
        workspace: PathBuf,
        chain_id: String,
        source: HandoffActor,
        supervisor: SupervisorActor,
        tty_device: i64,
        tty_inode: i64,
    }

    fn test_boot_id() -> String {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap()
            .trim()
            .to_string()
    }

    fn absent_birth(pid: i32) -> String {
        format!("linux-v1:{pid}:1:{}", test_boot_id())
    }

    fn absent_pid_and_group(seed: i32) -> i32 {
        for pid in seed..seed + 10_000 {
            if hcom::chain_supervisor::exact_process_status(pid, &absent_birth(pid))
                == hcom::chain_supervisor::ExactProcessStatus::Absent
                && hcom::chain_supervisor::process_group_status(pid)
                    == hcom::chain_supervisor::ExactProcessStatus::Absent
            {
                return pid;
            }
        }
        panic!("could not locate an absent PID/process-group test identity");
    }

    fn public_spec(
        workspace: &Path,
        supervisor_pid: i32,
        supervisor_pgid: i32,
        supervisor_birth: String,
        suffix: &str,
        tty_device: i64,
        tty_inode: i64,
    ) -> ChainSpec {
        ChainSpec {
            workspace: workspace.to_path_buf(),
            tool: "codex".to_string(),
            model_ref: "gpt-test".to_string(),
            reasoning_ref: "high".to_string(),
            permission_policy_ref: "approval=never;sandbox=read-only".to_string(),
            policy_ref: "codex-0.145.0-foreground-v1".to_string(),
            supervisor_process_id: format!("supervisor-{suffix}"),
            supervisor_process_birth_identity: supervisor_birth,
            supervisor_pid: i64::from(supervisor_pid),
            supervisor_pgid: i64::from(supervisor_pgid),
            outer_foreground_pgid: i64::from(supervisor_pgid),
            outer_tty_device: tty_device,
            outer_tty_inode: tty_inode,
            launch_nonce: format!("launch-{suffix}"),
        }
    }

    fn public_active_fixture() -> PublicFixture {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        run_git(&workspace, &["init", "-b", "main"]);
        run_git(&workspace, &["config", "user.name", "hcom test"]);
        run_git(
            &workspace,
            &["config", "user.email", "hcom-test@example.invalid"],
        );
        std::fs::write(workspace.join("README.md"), "public fixture\n").unwrap();
        run_git(&workspace, &["add", "README.md"]);
        run_git(&workspace, &["commit", "-m", "fixture"]);

        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        let old_supervisor_pid = absent_pid_and_group(1_400_000);
        let old_supervisor_pgid = absent_pid_and_group(1_420_000);
        let tty_device = 71;
        let tty_inode = 73;
        let spec = public_spec(
            &workspace,
            old_supervisor_pid,
            old_supervisor_pgid,
            absent_birth(old_supervisor_pid),
            "old",
            tty_device,
            tty_inode,
        );
        let supervisor = SupervisorActor {
            process_id: spec.supervisor_process_id.clone(),
            process_birth_identity: spec.supervisor_process_birth_identity.clone(),
        };
        let reservation = create_public_chain_reservation(&db, &spec).unwrap();
        let wrapper_pid = absent_pid_and_group(1_440_000);
        let child_pid = absent_pid_and_group(1_460_000);
        begin_generation_prepare(
            &db,
            &supervisor,
            &reservation.chain.id,
            reservation.generation.generation,
            reservation.chain.version,
            &reservation.generation.launch_nonce,
        )
        .unwrap();
        let materialized = materialize_initial_generation(
            &db,
            &supervisor,
            &reservation.chain.id,
            reservation.chain.version,
            &TargetMaterialization {
                expected_version: reservation.generation.version,
                launch_nonce: reservation.generation.launch_nonce.clone(),
                instance_name: "public-source".to_string(),
                hcom_session_id: "public-source-session".to_string(),
                process_id: "public-source-process".to_string(),
                process_birth_identity: absent_birth(wrapper_pid),
                wrapper_pid: i64::from(wrapper_pid),
                wrapper_pgid: i64::from(old_supervisor_pgid),
                child_pid: i64::from(child_pid),
                child_pgid: i64::from(child_pid),
                child_process_birth_identity: absent_birth(child_pid),
            },
        )
        .unwrap();
        let mut source = HandoffActor {
            instance_name: "public-source".to_string(),
            hcom_session_id: "public-source-session".to_string(),
            native_session_id: None,
            process_id: "public-source-process".to_string(),
            process_birth_identity: absent_birth(wrapper_pid),
            generation: 1,
        };
        pin_native_session(
            &db,
            &reservation.chain.id,
            &source,
            materialized.generation.version,
            "public-source-native",
        )
        .unwrap();
        source.native_session_id = Some("public-source-native".to_string());
        PublicFixture {
            _dir: dir,
            db,
            workspace,
            chain_id: reservation.chain.id,
            source,
            supervisor,
            tty_device,
            tty_inode,
        }
    }

    fn current_terminal_owner(fixture: &PublicFixture, suffix: &str) -> TerminalOwnerEvidence {
        // SAFETY: these libc calls have no preconditions.
        let pid = unsafe { libc::getpid() };
        let pgid = unsafe { libc::getpgrp() };
        TerminalOwnerEvidence {
            workspace: fixture.workspace.clone(),
            supervisor: SupervisorActor {
                process_id: format!("recovery-supervisor-{suffix}"),
                process_birth_identity: hcom::chain_supervisor::linux_process_birth_identity(pid)
                    .unwrap(),
            },
            supervisor_pid: i64::from(pid),
            supervisor_pgid: i64::from(pgid),
            outer_foreground_pgid: i64::from(pgid),
            outer_tty_device: fixture.tty_device,
            outer_tty_inode: fixture.tty_inode,
        }
    }

    fn run_git(workspace: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn add_live_actor(db: &HcomDb, actor: &HandoffActor) {
        db.conn()
            .execute(
                "INSERT INTO instances (
                     name, session_id, status, tool, created_at,
                     parent_name, origin_device_id
                 ) VALUES (?1, ?2, 'listening', 'codex', 1.0, '', '')",
                params![actor.instance_name, actor.hcom_session_id],
            )
            .unwrap();
        db.set_process_binding(
            &actor.process_id,
            &actor.hcom_session_id,
            &actor.instance_name,
        )
        .unwrap();
    }

    fn remove_live_actor(db: &HcomDb, actor: &HandoffActor) {
        db.conn()
            .execute(
                "DELETE FROM process_bindings WHERE process_id = ?1",
                params![actor.process_id],
            )
            .unwrap();
        db.conn()
            .execute(
                "DELETE FROM instances WHERE name = ?1",
                params![actor.instance_name],
            )
            .unwrap();
    }

    fn create_bundle(db: &HcomDb, actor: &HandoffActor, padding: usize) -> i64 {
        let data = serde_json::json!({
            "bundle_id": generate_id("bundle"),
            "created_by": actor.instance_name,
            "title": "handoff",
            "description": "x".repeat(padding),
            "refs": {"events": [], "files": [], "transcript": []},
        });
        db.log_event("bundle", &actor.instance_name, &data).unwrap();
        db.conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn create_bundle_with_serialized_size(
        db: &HcomDb,
        actor: &HandoffActor,
        target_size: usize,
    ) -> i64 {
        let mut data = serde_json::json!({
            "bundle_id": "bundle:bounded",
            "created_by": actor.instance_name,
            "description": "",
        });
        let base = serde_json::to_vec(&data).unwrap().len();
        assert!(target_size >= base);
        data["description"] = serde_json::Value::String("x".repeat(target_size - base));
        assert_eq!(serde_json::to_vec(&data).unwrap().len(), target_size);
        db.log_event("bundle", &actor.instance_name, &data).unwrap();
        db.conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn fixture(native_pinned: bool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        run_git(&workspace, &["init", "-b", "main"]);
        run_git(&workspace, &["config", "user.name", "hcom test"]);
        run_git(
            &workspace,
            &["config", "user.email", "hcom-test@example.invalid"],
        );
        std::fs::write(workspace.join("README.md"), "fixture\n").unwrap();
        run_git(&workspace, &["add", "README.md"]);
        run_git(&workspace, &["commit", "-m", "fixture"]);

        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        let source = HandoffActor {
            instance_name: "source".to_string(),
            hcom_session_id: "hcom-source".to_string(),
            native_session_id: native_pinned.then(|| "native-source".to_string()),
            process_id: "process-source".to_string(),
            process_birth_identity: "birth-source".to_string(),
            generation: 1,
        };
        add_live_actor(&db, &source);
        let chain = create_chain(
            &db,
            &source,
            &ChainSpec {
                workspace: workspace.clone(),
                tool: "codex".to_string(),
                model_ref: "model-pinned".to_string(),
                reasoning_ref: "reasoning-pinned".to_string(),
                permission_policy_ref: "permission-policy-pinned".to_string(),
                policy_ref: "policy-digest-pinned".to_string(),
                supervisor_process_id: "supervisor-process".to_string(),
                supervisor_process_birth_identity: "supervisor-birth".to_string(),
                supervisor_pid: 41001,
                supervisor_pgid: 41001,
                outer_foreground_pgid: 41001,
                outer_tty_device: 7,
                outer_tty_inode: 11,
                launch_nonce: "launch-source".to_string(),
            },
        )
        .unwrap();
        Fixture {
            _dir: dir,
            db,
            workspace,
            source,
            chain,
        }
    }

    fn supervisor_actor(fixture: &Fixture) -> SupervisorActor {
        SupervisorActor {
            process_id: fixture.chain.supervisor_process_id.clone(),
            process_birth_identity: fixture.chain.supervisor_process_birth_identity.clone(),
        }
    }

    fn record_test_sigterm(fixture: &Fixture, quiescing: &HandoffOutcome) -> HandoffOutcome {
        record_sigterm_request(
            &fixture.db,
            &supervisor_actor(fixture),
            &quiescing.handoff.id,
            &SigtermObservation {
                expected_version: quiescing.handoff.version,
                requested_wall_at: 1000.0,
                requested_monotonic_ns: 1_000_000_000,
                result: SigtermRequestResult::Sent,
            },
        )
        .unwrap()
    }

    fn successful_cleanup(expected_version: i64) -> CleanupObservation {
        CleanupObservation {
            expected_version,
            exit: Some(ChildExitEvidence {
                observed_wall_at: 1000.025,
                observed_monotonic_ns: 1_025_000_000,
                exit_code: None,
                exit_signal: Some(15),
                delivery_context: DeliveryExitContext::Killed,
            }),
            reaped: true,
            resources: ResourceCleanupEvidence {
                inject_succeeded: true,
                delivery_succeeded: true,
                pty_succeeded: true,
                screen_succeeded: true,
                write_queue_succeeded: true,
            },
            failure_kind: String::new(),
            failure_reason: String::new(),
        }
    }

    fn prepare_and_commit(fixture: &Fixture) -> HandoffOutcome {
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        assert_eq!(prepared.handoff.state, HandoffState::Prepared);
        commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            prepared.handoff.version,
            &fixture.workspace,
        )
        .unwrap()
    }

    fn prepare_public_and_commit(fixture: &PublicFixture) -> HandoffOutcome {
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            prepared.handoff.version,
            &fixture.workspace,
        )
        .unwrap()
    }

    fn advance_public_to_unmaterialized_target(fixture: &PublicFixture) -> HandoffOutcome {
        let committed = prepare_public_and_commit(fixture);
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = observe_stop(
            &fixture.db,
            &fixture.source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: committed.handoff.version,
                quiesce_token: token.clone(),
                committed_version: committed.handoff.version,
                hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
                launch_nonce: committed.handoff.source_launch_nonce.clone(),
                turn_id: "turn-public-source".to_string(),
            },
        )
        .unwrap();
        let quiescing = begin_quiesce(
            &fixture.db,
            &fixture.supervisor,
            &stopped.handoff.id,
            stopped.handoff.version,
            &token,
        )
        .unwrap();
        let sigterm = record_sigterm_request(
            &fixture.db,
            &fixture.supervisor,
            &quiescing.handoff.id,
            &SigtermObservation {
                expected_version: quiescing.handoff.version,
                requested_wall_at: 2000.0,
                requested_monotonic_ns: 2_000_000_000,
                result: SigtermRequestResult::Sent,
            },
        )
        .unwrap();
        remove_live_actor(&fixture.db, &fixture.source);
        complete_source_cleanup(
            &fixture.db,
            &fixture.supervisor,
            &sigterm.handoff.id,
            &CleanupObservation {
                expected_version: sigterm.handoff.version,
                exit: Some(ChildExitEvidence {
                    observed_wall_at: 2000.010,
                    observed_monotonic_ns: 2_010_000_000,
                    exit_code: None,
                    exit_signal: Some(libc::SIGTERM),
                    delivery_context: DeliveryExitContext::Killed,
                }),
                reaped: true,
                resources: ResourceCleanupEvidence {
                    inject_succeeded: true,
                    delivery_succeeded: true,
                    pty_succeeded: true,
                    screen_succeeded: true,
                    write_queue_succeeded: true,
                },
                failure_kind: String::new(),
                failure_reason: String::new(),
            },
        )
        .unwrap()
    }

    fn materialize_public_target(
        fixture: &PublicFixture,
        handoff: &TerminalHandoff,
        supervisor: &SupervisorActor,
        wrapper_pgid: i32,
        suffix: &str,
    ) -> HandoffActor {
        let generation = effective_handoff_target_generation(&fixture.db, &handoff.id).unwrap();
        let durable = get_generation(&fixture.db, &fixture.chain_id, generation)
            .unwrap()
            .unwrap();
        let wrapper_pid = absent_pid_and_group(1_500_000 + generation as i32 * 100);
        let child_pid = absent_pid_and_group(1_520_000 + generation as i32 * 100);
        let actor = HandoffActor {
            instance_name: format!("public-target-{suffix}"),
            hcom_session_id: format!("public-target-session-{suffix}"),
            native_session_id: None,
            process_id: format!("public-target-process-{suffix}"),
            process_birth_identity: absent_birth(wrapper_pid),
            generation,
        };
        begin_generation_prepare(
            &fixture.db,
            supervisor,
            &fixture.chain_id,
            generation,
            handoff.version,
            &durable.launch_nonce,
        )
        .unwrap();
        materialize_target_generation(
            &fixture.db,
            supervisor,
            &handoff.id,
            &TargetMaterialization {
                expected_version: handoff.version,
                launch_nonce: durable.launch_nonce,
                instance_name: actor.instance_name.clone(),
                hcom_session_id: actor.hcom_session_id.clone(),
                process_id: actor.process_id.clone(),
                process_birth_identity: actor.process_birth_identity.clone(),
                wrapper_pid: i64::from(wrapper_pid),
                wrapper_pgid: i64::from(wrapper_pgid),
                child_pid: i64::from(child_pid),
                child_pgid: i64::from(child_pid),
                child_process_birth_identity: absent_birth(child_pid),
            },
        )
        .unwrap();
        actor
    }

    fn advance_to_sigterm(fixture: &Fixture) -> HandoffOutcome {
        let committed = prepare_and_commit(fixture);
        let supervisor = supervisor_actor(fixture);
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = observe_stop(
            &fixture.db,
            &fixture.source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: committed.handoff.version,
                quiesce_token: token.clone(),
                committed_version: committed.handoff.version,
                hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
                launch_nonce: committed.handoff.source_launch_nonce.clone(),
                turn_id: "turn-source".to_string(),
            },
        )
        .unwrap();
        let quiescing = begin_quiesce(
            &fixture.db,
            &supervisor,
            &stopped.handoff.id,
            stopped.handoff.version,
            &token,
        )
        .unwrap();
        record_test_sigterm(fixture, &quiescing)
    }

    fn advance_to_unmaterialized_launching(fixture: &Fixture) -> HandoffOutcome {
        let sigterm = advance_to_sigterm(fixture);
        let supervisor = supervisor_actor(fixture);
        remove_live_actor(&fixture.db, &fixture.source);
        complete_source_cleanup(
            &fixture.db,
            &supervisor,
            &sigterm.handoff.id,
            &successful_cleanup(sigterm.handoff.version),
        )
        .unwrap()
    }

    fn advance_to_launching(fixture: &Fixture) -> HandoffOutcome {
        let launching = advance_to_unmaterialized_launching(fixture);
        let target = target_actor();
        let materialized = materialize_target_fixture(fixture, &launching, &target);
        pin_target_fixture(fixture, &target);
        materialized
    }

    fn materialize_target_fixture(
        fixture: &Fixture,
        launching: &HandoffOutcome,
        target: &HandoffActor,
    ) -> HandoffOutcome {
        let generation = load_generation(fixture.db.conn(), &fixture.chain.id, target.generation)
            .unwrap()
            .unwrap();
        begin_generation_prepare(
            &fixture.db,
            &supervisor_actor(fixture),
            &fixture.chain.id,
            target.generation,
            launching.handoff.version,
            &generation.launch_nonce,
        )
        .unwrap();
        materialize_target_generation(
            &fixture.db,
            &supervisor_actor(fixture),
            &launching.handoff.id,
            &TargetMaterialization {
                expected_version: launching.handoff.version,
                launch_nonce: generation.launch_nonce,
                instance_name: target.instance_name.clone(),
                hcom_session_id: target.hcom_session_id.clone(),
                process_id: target.process_id.clone(),
                process_birth_identity: target.process_birth_identity.clone(),
                wrapper_pid: 41_002,
                wrapper_pgid: 41_001,
                child_pid: 41_003,
                child_pgid: 41_003,
                child_process_birth_identity: "child-birth-target".to_string(),
            },
        )
        .unwrap()
    }

    fn target_actor() -> HandoffActor {
        HandoffActor {
            instance_name: "target".to_string(),
            hcom_session_id: "hcom-target".to_string(),
            native_session_id: Some("native-target".to_string()),
            process_id: "process-target".to_string(),
            process_birth_identity: "birth-target".to_string(),
            generation: 2,
        }
    }

    fn pin_target_fixture(fixture: &Fixture, target: &HandoffActor) {
        let generation = get_generation(&fixture.db, &fixture.chain.id, target.generation)
            .unwrap()
            .unwrap();
        let mut session_start_actor = target.clone();
        session_start_actor.native_session_id = None;
        pin_native_session(
            &fixture.db,
            &fixture.chain.id,
            &session_start_actor,
            generation.version,
            target.native_session_id.as_deref().unwrap(),
        )
        .unwrap();
    }

    fn assert_audit_continuity(db: &HcomDb, object_kind: &str, object_id: &str) {
        let versions: Vec<(i64, i64)> = db
            .conn()
            .prepare(
                "SELECT from_version, to_version
                 FROM terminal_transition_audit
                 WHERE object_kind = ?1 AND object_id = ?2
                 ORDER BY from_version",
            )
            .unwrap()
            .query_map(params![object_kind, object_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(!versions.is_empty());
        assert_eq!(versions[0].0, -1);
        for (index, (from, to)) in versions.iter().enumerate() {
            assert_eq!(*to, *from + 1);
            if let Some((next_from, _)) = versions.get(index + 1) {
                assert_eq!(*next_from, *to);
            }
        }
    }

    fn audit_count(db: &HcomDb) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn advance_to_awaiting_acceptance(fixture: &Fixture) -> (HandoffOutcome, HandoffActor) {
        let launching = advance_to_launching(fixture);
        let target = target_actor();
        let generation = load_generation(fixture.db.conn(), &fixture.chain.id, target.generation)
            .unwrap()
            .unwrap();
        let ready = target_ready(
            &fixture.db,
            &target,
            &launching.handoff.id,
            launching.handoff.version,
            &generation.launch_nonce,
        )
        .unwrap();
        (ready, target)
    }

    fn inspect_target_fixture(
        fixture: &Fixture,
        ready: &HandoffOutcome,
        target: &HandoffActor,
    ) -> HandoffInspection {
        inspect_handoff(
            &fixture.db,
            target,
            &ready.handoff.id,
            ready.handoff.version,
            &fixture.workspace,
        )
        .unwrap()
    }

    #[test]
    fn shutdown_intent_precedes_cleanup_and_replays_without_live_binding() {
        let fixture = fixture(true);
        let chain = get_chain(&fixture.db, &fixture.chain.id).unwrap().unwrap();
        let generation = get_generation(&fixture.db, &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        let observation = ChainShutdownObservation {
            expected_chain_version: chain.version,
            expected_generation_version: generation.version,
            reason: SupervisorShutdownReason::Explicit,
        };
        let outcome = begin_chain_shutdown(
            &fixture.db,
            &supervisor_actor(&fixture),
            &fixture.chain.id,
            &fixture.source,
            &observation,
        )
        .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(outcome.generation.state, GenerationState::NeedsRecovery);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
        );

        remove_live_actor(&fixture.db, &fixture.source);
        let audit_before = audit_count(&fixture.db);
        assert!(
            begin_chain_shutdown(
                &fixture.db,
                &supervisor_actor(&fixture),
                &fixture.chain.id,
                &fixture.source,
                &observation,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), audit_before);
    }

    #[test]
    fn outer_hangup_marks_open_handoff_and_current_generation_recovery() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let chain = get_chain(&fixture.db, &fixture.chain.id).unwrap().unwrap();
        let generation = get_generation(&fixture.db, &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        begin_chain_shutdown(
            &fixture.db,
            &supervisor_actor(&fixture),
            &fixture.chain.id,
            &fixture.source,
            &ChainShutdownObservation {
                expected_chain_version: chain.version,
                expected_generation_version: generation.version,
                reason: SupervisorShutdownReason::OuterHangup,
            },
        )
        .unwrap();

        let failed_handoff = get_handoff(&fixture.db, &prepared.handoff.id)
            .unwrap()
            .unwrap();
        assert_eq!(failed_handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(failed_handoff.failure_kind, "outer_hangup");
        assert_eq!(
            get_generation(&fixture.db, &fixture.chain.id, 1)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::NeedsRecovery
        );
        assert_eq!(
            get_generation(&fixture.db, &fixture.chain.id, 2)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::Reserved
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = 'target'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn shutdown_audit_failure_rolls_back_chain_and_generation_intent() {
        let fixture = fixture(true);
        fixture
            .db
            .conn()
            .execute_batch(
                "CREATE TRIGGER fail_shutdown_audit
                 BEFORE INSERT ON terminal_transition_audit
                 WHEN NEW.action = 'begin_chain_shutdown'
                      AND NEW.object_kind = 'chain'
                 BEGIN
                     SELECT RAISE(ABORT, 'injected shutdown audit failure');
                 END;",
            )
            .unwrap();
        let chain = get_chain(&fixture.db, &fixture.chain.id).unwrap().unwrap();
        let generation = get_generation(&fixture.db, &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        assert!(matches!(
            begin_chain_shutdown(
                &fixture.db,
                &supervisor_actor(&fixture),
                &fixture.chain.id,
                &fixture.source,
                &ChainShutdownObservation {
                    expected_chain_version: chain.version,
                    expected_generation_version: generation.version,
                    reason: SupervisorShutdownReason::Explicit,
                },
            ),
            Err(HandoffError::Storage)
        ));
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::Active
        );
        assert_eq!(
            get_generation(&fixture.db, &fixture.chain.id, 1)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::Active
        );
    }

    #[test]
    fn full_state_machine_is_serial_and_repeatable() {
        let fixture = fixture(true);
        let instances_before: i64 = fixture
            .db
            .conn()
            .query_row("SELECT COUNT(*) FROM instances", [], |row| row.get(0))
            .unwrap();
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let instances_after_reservation: i64 = fixture
            .db
            .conn()
            .query_row("SELECT COUNT(*) FROM instances", [], |row| row.get(0))
            .unwrap();
        assert_eq!(instances_before, instances_after_reservation);
        assert_eq!(prepared.handoff.target_generation, 2);

        let committed = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            0,
            &fixture.workspace,
        )
        .unwrap();
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = observe_stop(
            &fixture.db,
            &fixture.source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: 1,
                quiesce_token: token.clone(),
                committed_version: 1,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "launch-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
        )
        .unwrap();
        let quiescing = begin_quiesce(
            &fixture.db,
            &supervisor_actor(&fixture),
            &stopped.handoff.id,
            2,
            &token,
        )
        .unwrap();
        let sigterm = record_test_sigterm(&fixture, &quiescing);
        remove_live_actor(&fixture.db, &fixture.source);
        let launching = complete_source_cleanup(
            &fixture.db,
            &supervisor_actor(&fixture),
            &sigterm.handoff.id,
            &successful_cleanup(sigterm.handoff.version),
        )
        .unwrap();
        assert_eq!(launching.handoff.state, HandoffState::LaunchingTarget);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .current_generation,
            2
        );

        let target = target_actor();
        let launching = materialize_target_fixture(&fixture, &launching, &target);
        pin_target_fixture(&fixture, &target);
        let target_generation = load_generation(fixture.db.conn(), &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        let ready = target_ready(
            &fixture.db,
            &target,
            &launching.handoff.id,
            launching.handoff.version,
            &target_generation.launch_nonce,
        )
        .unwrap();
        assert_eq!(ready.handoff.state, HandoffState::AwaitingAcceptance);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::AwaitingAcceptance
        );
        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            inspection.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(accepted.handoff.state, HandoffState::Accepted);

        let event_id = create_bundle(&fixture.db, &target, 0);
        let next = prepare_handoff(&fixture.db, &target, event_id, &fixture.workspace).unwrap();
        assert_eq!(next.handoff.source_generation, 2);
        assert_eq!(next.handoff.target_generation, 3);
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT MAX(generation) FROM terminal_generations WHERE chain_id = ?1",
                    params![fixture.chain.id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            3
        );
    }

    #[test]
    fn every_successful_state_transition_has_an_exact_zero_write_replay() {
        let fixture = fixture(true);
        let supervisor = supervisor_actor(&fixture);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace)
                .unwrap()
                .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);

        let committed = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            prepared.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            commit_handoff(
                &fixture.db,
                &fixture.source,
                &prepared.handoff.id,
                prepared.handoff.version,
                &fixture.workspace,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);

        let stop = StopObservation {
            expected_version: committed.handoff.version,
            quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
            committed_version: committed.handoff.version,
            hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
            launch_nonce: committed.handoff.source_launch_nonce.clone(),
            turn_id: "turn-source".to_string(),
        };
        let stopped =
            observe_stop(&fixture.db, &fixture.source, &committed.handoff.id, &stop).unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            observe_stop(&fixture.db, &fixture.source, &committed.handoff.id, &stop)
                .unwrap()
                .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);

        let quiescing = begin_quiesce(
            &fixture.db,
            &supervisor,
            &stopped.handoff.id,
            stopped.handoff.version,
            stop.quiesce_token.as_str(),
        )
        .unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            begin_quiesce(
                &fixture.db,
                &supervisor,
                &stopped.handoff.id,
                stopped.handoff.version,
                stop.quiesce_token.as_str(),
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);
        let stop_actor: (String, String) = fixture
            .db
            .conn()
            .query_row(
                "SELECT actor_process_id, actor_role
                 FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND action = 'observe_stop'",
                params![stopped.handoff.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            stop_actor,
            (fixture.source.process_id.clone(), "source".to_string())
        );
        let quiesce_actor: (String, String) = fixture
            .db
            .conn()
            .query_row(
                "SELECT actor_process_id, actor_role
                 FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND action = 'begin_quiesce'",
                params![stopped.handoff.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            quiesce_actor,
            (supervisor.process_id.clone(), "supervisor".to_string())
        );

        let sigterm = record_test_sigterm(&fixture, &quiescing);
        remove_live_actor(&fixture.db, &fixture.source);
        let cleanup = successful_cleanup(sigterm.handoff.version);
        let launching =
            complete_source_cleanup(&fixture.db, &supervisor, &sigterm.handoff.id, &cleanup)
                .unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            complete_source_cleanup(&fixture.db, &supervisor, &sigterm.handoff.id, &cleanup,)
                .unwrap()
                .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);

        let target = target_actor();
        let launch_nonce = load_generation(fixture.db.conn(), &fixture.chain.id, target.generation)
            .unwrap()
            .unwrap()
            .launch_nonce;
        let materialized = materialize_target_fixture(&fixture, &launching, &target);
        let before = audit_count(&fixture.db);
        assert!(
            materialize_target_generation(
                &fixture.db,
                &supervisor,
                &launching.handoff.id,
                &TargetMaterialization {
                    expected_version: launching.handoff.version,
                    launch_nonce: launch_nonce.clone(),
                    instance_name: target.instance_name.clone(),
                    hcom_session_id: target.hcom_session_id.clone(),
                    process_id: target.process_id.clone(),
                    process_birth_identity: target.process_birth_identity.clone(),
                    wrapper_pid: 41_002,
                    wrapper_pgid: 41_001,
                    child_pid: 41_003,
                    child_pgid: 41_003,
                    child_process_birth_identity: "child-birth-target".to_string(),
                },
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);
        pin_target_fixture(&fixture, &target);
        let ready = target_ready(
            &fixture.db,
            &target,
            &materialized.handoff.id,
            materialized.handoff.version,
            &launch_nonce,
        )
        .unwrap();
        let before = audit_count(&fixture.db);
        assert!(
            target_ready(
                &fixture.db,
                &target,
                &materialized.handoff.id,
                materialized.handoff.version,
                &launch_nonce,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);

        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            inspection.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(accepted.handoff.state, HandoffState::Accepted);
        let before = audit_count(&fixture.db);
        assert!(
            accept_handoff(
                &fixture.db,
                &target,
                &ready.handoff.id,
                inspection.handoff.version,
                &fixture.workspace,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);
    }

    #[test]
    fn native_session_pin_is_one_way_and_conflict_is_zero_write() {
        let fixture = fixture(false);
        let pinned = pin_native_session(
            &fixture.db,
            &fixture.chain.id,
            &fixture.source,
            0,
            "native-source",
        )
        .unwrap();
        assert_eq!(
            pinned.generation.native_session_id.as_deref(),
            Some("native-source")
        );
        let replay = pin_native_session(
            &fixture.db,
            &fixture.chain.id,
            &fixture.source,
            0,
            "native-source",
        )
        .unwrap();
        assert!(replay.replayed);

        let conflict = pin_native_session(
            &fixture.db,
            &fixture.chain.id,
            &fixture.source,
            1,
            "different-native",
        );
        assert!(matches!(conflict, Err(HandoffError::Conflict(_))));
        let generation = load_generation(fixture.db.conn(), &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        assert_eq!(
            generation.native_session_id.as_deref(),
            Some("native-source")
        );
        assert_eq!(generation.state, GenerationState::Active);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::Active
        );
    }

    #[test]
    fn abort_is_atomic_replayable_and_returns_source_to_active() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let aborted = abort_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            prepared.handoff.version,
            "user kept the current generation",
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(aborted.handoff.state, HandoffState::Aborted);
        assert_eq!(aborted.handoff.failure_kind, "aborted");
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::Active
        );
        assert_eq!(
            load_generation(fixture.db.conn(), &fixture.chain.id, 1)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::Active
        );
        let replay = abort_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            prepared.handoff.version,
            "user kept the current generation",
            &fixture.workspace,
        )
        .unwrap();
        assert!(replay.replayed);
        assert!(matches!(
            abort_handoff(
                &fixture.db,
                &fixture.source,
                &prepared.handoff.id,
                prepared.handoff.version,
                "different reason",
                &fixture.workspace
            ),
            Err(HandoffError::Conflict(_))
        ));

        let next_event = create_bundle(&fixture.db, &fixture.source, 0);
        let next =
            prepare_handoff(&fixture.db, &fixture.source, next_event, &fixture.workspace).unwrap();
        assert_eq!(next.handoff.target_generation, 2);
    }

    #[test]
    fn commit_replay_and_conflicting_payload_have_zero_partial_writes() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let committed = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            0,
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(committed.handoff.version, 1);
        let replay = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            0,
            &fixture.workspace,
        )
        .unwrap();
        assert!(replay.replayed);
        let audit_before: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(matches!(
            commit_handoff(
                &fixture.db,
                &fixture.source,
                &prepared.handoff.id,
                committed.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::Conflict(_))
        ));
        assert_eq!(audit_count(&fixture.db), audit_before);

        std::fs::write(fixture.workspace.join("new.txt"), "changed\n").unwrap();
        let conflict = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            0,
            &fixture.workspace,
        );
        assert!(matches!(conflict, Err(HandoffError::Conflict(_))));
        let audit_after: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_before, audit_after);
        assert_eq!(
            get_handoff(&fixture.db, &prepared.handoff.id)
                .unwrap()
                .unwrap()
                .version,
            1
        );
    }

    #[test]
    fn commit_and_accept_reject_a_mutated_pinned_bundle() {
        let commit_fixture = fixture(true);
        let event_id = create_bundle(&commit_fixture.db, &commit_fixture.source, 0);
        let prepared = prepare_handoff(
            &commit_fixture.db,
            &commit_fixture.source,
            event_id,
            &commit_fixture.workspace,
        )
        .unwrap();
        let audit_before = audit_count(&commit_fixture.db);
        commit_fixture
            .db
            .conn()
            .execute(
                "UPDATE events SET data = ?1 WHERE id = ?2",
                params![
                    serde_json::json!({
                        "bundle_id": "bundle:mutated",
                        "created_by": commit_fixture.source.instance_name,
                        "description": "changed after prepare",
                    })
                    .to_string(),
                    event_id,
                ],
            )
            .unwrap();
        assert!(matches!(
            commit_handoff(
                &commit_fixture.db,
                &commit_fixture.source,
                &prepared.handoff.id,
                prepared.handoff.version,
                &commit_fixture.workspace,
            ),
            Err(HandoffError::Conflict(_))
        ));
        assert_eq!(audit_count(&commit_fixture.db), audit_before);
        assert_eq!(
            get_handoff(&commit_fixture.db, &prepared.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::Prepared
        );

        let fixture = fixture(true);
        let (ready, target) = advance_to_awaiting_acceptance(&fixture);
        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        let audit_before = audit_count(&fixture.db);
        fixture
            .db
            .conn()
            .execute(
                "UPDATE events SET data = ?1 WHERE id = ?2",
                params![
                    serde_json::json!({
                        "bundle_id": "bundle:mutated",
                        "created_by": ready.handoff.source_instance_name,
                        "description": "changed before target acceptance",
                    })
                    .to_string(),
                    ready.handoff.bundle_event_id,
                ],
            )
            .unwrap();
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &target,
                &ready.handoff.id,
                inspection.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::TypedConflict {
                code: "target_validation_changed",
                ..
            })
        ));
        assert_eq!(audit_count(&fixture.db), audit_before);
        assert_eq!(
            get_handoff(&fixture.db, &ready.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::AwaitingAcceptance
        );
    }

    #[test]
    fn stop_requires_exact_committed_authorization() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let before_commit = observe_stop(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            &StopObservation {
                expected_version: 0,
                quiesce_token: "qa-wrong".to_string(),
                committed_version: 0,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "launch-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
        );
        assert!(matches!(before_commit, Err(HandoffError::Conflict(_))));

        let committed = commit_handoff(
            &fixture.db,
            &fixture.source,
            &prepared.handoff.id,
            0,
            &fixture.workspace,
        )
        .unwrap();
        for observation in [
            StopObservation {
                expected_version: 1,
                quiesce_token: "qa-wrong".to_string(),
                committed_version: 1,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "launch-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
            StopObservation {
                expected_version: 1,
                quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
                committed_version: 1,
                hook_native_session_id: "wrong-native".to_string(),
                launch_nonce: "launch-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
            StopObservation {
                expected_version: 1,
                quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
                committed_version: 0,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "launch-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
        ] {
            assert!(matches!(
                observe_stop(
                    &fixture.db,
                    &fixture.source,
                    &committed.handoff.id,
                    &observation
                ),
                Err(HandoffError::Conflict(_))
            ));
        }
        assert_eq!(
            get_handoff(&fixture.db, &committed.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::Committed
        );

        let mut wrong_identity = fixture.source.clone();
        wrong_identity.process_birth_identity = "wrong-birth".to_string();
        let exact = StopObservation {
            expected_version: committed.handoff.version,
            quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
            committed_version: committed.handoff.version,
            hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
            launch_nonce: committed.handoff.source_launch_nonce.clone(),
            turn_id: "turn-source".to_string(),
        };
        assert!(matches!(
            observe_stop(&fixture.db, &wrong_identity, &committed.handoff.id, &exact),
            Err(HandoffError::Conflict(_))
        ));

        let stopped =
            observe_stop(&fixture.db, &fixture.source, &committed.handoff.id, &exact).unwrap();
        assert_eq!(stopped.handoff.state, HandoffState::StopObserved);
        let replay =
            observe_stop(&fixture.db, &fixture.source, &committed.handoff.id, &exact).unwrap();
        assert!(replay.replayed);
        let mut stale = exact;
        stale.quiesce_token = "qa-stale".to_string();
        assert!(matches!(
            observe_stop(&fixture.db, &fixture.source, &committed.handoff.id, &stale),
            Err(HandoffError::Conflict(_))
        ));
        let stop_audits: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND action = 'observe_stop'",
                params![committed.handoff.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stop_audits, 1);

        let mut wrong_supervisor = supervisor_actor(&fixture);
        wrong_supervisor.process_birth_identity = "wrong-supervisor-birth".to_string();
        assert!(matches!(
            begin_quiesce(
                &fixture.db,
                &wrong_supervisor,
                &committed.handoff.id,
                stopped.handoff.version,
                committed.handoff.quiesce_token.as_deref().unwrap(),
            ),
            Err(HandoffError::Conflict(_))
        ));
        assert_eq!(
            get_handoff(&fixture.db, &committed.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::StopObserved
        );
    }

    #[test]
    fn exit_without_verified_stop_needs_recovery() {
        let fixture = fixture(true);
        let committed = prepare_and_commit(&fixture);
        remove_live_actor(&fixture.db, &fixture.source);
        let outcome = observe_source_exit_without_stop(
            &fixture.db,
            &supervisor_actor(&fixture),
            &committed.handoff.id,
            &CleanupObservation {
                expected_version: committed.handoff.version,
                exit: Some(ChildExitEvidence {
                    observed_wall_at: 1000.0,
                    observed_monotonic_ns: 1_000_000_000,
                    exit_code: Some(0),
                    exit_signal: None,
                    delivery_context: DeliveryExitContext::Closed,
                }),
                reaped: true,
                resources: ResourceCleanupEvidence {
                    inject_succeeded: true,
                    delivery_succeeded: true,
                    pty_succeeded: true,
                    screen_succeeded: true,
                    write_queue_succeeded: true,
                },
                failure_kind: "exit_without_stop".to_string(),
                failure_reason: String::new(),
            },
        )
        .unwrap();
        assert_eq!(outcome.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(outcome.handoff.failure_kind, "exit_without_stop");
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
        );
    }

    #[test]
    fn cleanup_failure_and_target_rejection_enter_recovery() {
        let cleanup_fixture = fixture(true);
        let committed = prepare_and_commit(&cleanup_fixture);
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = observe_stop(
            &cleanup_fixture.db,
            &cleanup_fixture.source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: committed.handoff.version,
                quiesce_token: token.clone(),
                committed_version: committed.handoff.version,
                hook_native_session_id: cleanup_fixture.source.native_session_id.clone().unwrap(),
                launch_nonce: committed.handoff.source_launch_nonce.clone(),
                turn_id: "turn-source".to_string(),
            },
        )
        .unwrap();
        let quiescing = begin_quiesce(
            &cleanup_fixture.db,
            &supervisor_actor(&cleanup_fixture),
            &stopped.handoff.id,
            stopped.handoff.version,
            &token,
        )
        .unwrap();
        let sigterm = record_test_sigterm(&cleanup_fixture, &quiescing);
        remove_live_actor(&cleanup_fixture.db, &cleanup_fixture.source);
        let mut cleanup = successful_cleanup(sigterm.handoff.version);
        cleanup.reaped = false;
        cleanup.failure_kind = "waitpid_failed".to_string();
        cleanup.failure_reason = "waitpid did not reap the source".to_string();
        let failed = complete_source_cleanup(
            &cleanup_fixture.db,
            &supervisor_actor(&cleanup_fixture),
            &sigterm.handoff.id,
            &cleanup,
        )
        .unwrap();
        assert_eq!(failed.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(failed.handoff.failure_kind, "waitpid_failed");
        assert_eq!(
            get_chain(&cleanup_fixture.db, &cleanup_fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
        );

        let fixture = fixture(true);
        let (ready, target) = advance_to_awaiting_acceptance(&fixture);
        let rejected = reject_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
            "target rejected the pinned snapshot",
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(rejected.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(rejected.handoff.failure_kind, "target_rejected");
        let replay = reject_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
            "target rejected the pinned snapshot",
            &fixture.workspace,
        )
        .unwrap();
        assert!(replay.replayed);
    }

    #[test]
    fn illegal_transition_order_and_negative_versions_write_nothing() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let audit_before: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(matches!(
            begin_quiesce(
                &fixture.db,
                &supervisor_actor(&fixture),
                &prepared.handoff.id,
                prepared.handoff.version,
                "qa-before-stop"
            ),
            Err(HandoffError::Conflict(_))
        ));
        assert!(matches!(
            commit_handoff(
                &fixture.db,
                &fixture.source,
                &prepared.handoff.id,
                -1,
                &fixture.workspace
            ),
            Err(HandoffError::Invalid(_))
        ));
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &fixture.source,
                &prepared.handoff.id,
                prepared.handoff.version,
                &fixture.workspace
            ),
            Err(HandoffError::TypedConflict {
                code: "wrong_target_actor",
                ..
            })
        ));
        assert_eq!(
            get_handoff(&fixture.db, &prepared.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::Prepared
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_transition_audit",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .unwrap(),
            audit_before
        );
    }

    #[test]
    fn exact_actor_ownership_and_workspace_checks_fail_closed() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let baseline_audits: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let mut wrong_session = fixture.source.clone();
        wrong_session.hcom_session_id = "wrong-session".to_string();
        let mut wrong_native = fixture.source.clone();
        wrong_native.native_session_id = Some("wrong-native".to_string());
        let mut wrong_birth = fixture.source.clone();
        wrong_birth.process_birth_identity = "wrong-birth".to_string();
        let mut wrong_generation = fixture.source.clone();
        wrong_generation.generation = 2;
        for actor in [wrong_session, wrong_native, wrong_birth, wrong_generation] {
            assert!(matches!(
                prepare_handoff(&fixture.db, &actor, event_id, &fixture.workspace),
                Err(HandoffError::Conflict(_))
            ));
        }

        let spoof = HandoffActor {
            instance_name: fixture.source.instance_name.clone(),
            hcom_session_id: fixture.source.hcom_session_id.clone(),
            native_session_id: fixture.source.native_session_id.clone(),
            process_id: "attacker-process".to_string(),
            process_birth_identity: "attacker-birth".to_string(),
            generation: fixture.source.generation,
        };
        assert!(matches!(
            prepare_handoff(&fixture.db, &spoof, event_id, &fixture.workspace),
            Err(HandoffError::NotManaged)
        ));

        fixture
            .db
            .conn()
            .execute(
                "DELETE FROM process_bindings WHERE process_id = ?1",
                params![fixture.source.process_id],
            )
            .unwrap();
        assert!(matches!(
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace),
            Err(HandoffError::Conflict(_))
        ));
        fixture
            .db
            .set_process_binding(
                &fixture.source.process_id,
                &fixture.source.hcom_session_id,
                &fixture.source.instance_name,
            )
            .unwrap();

        let other_dir = tempfile::tempdir().unwrap();
        let other_workspace = other_dir.path().join("other");
        std::fs::create_dir(&other_workspace).unwrap();
        run_git(&other_workspace, &["init", "-b", "main"]);
        run_git(&other_workspace, &["config", "user.name", "hcom test"]);
        run_git(
            &other_workspace,
            &["config", "user.email", "hcom-test@example.invalid"],
        );
        std::fs::write(other_workspace.join("README.md"), "other\n").unwrap();
        run_git(&other_workspace, &["add", "README.md"]);
        run_git(&other_workspace, &["commit", "-m", "other"]);
        assert!(matches!(
            prepare_handoff(&fixture.db, &fixture.source, event_id, &other_workspace),
            Err(HandoffError::Conflict(_))
        ));

        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_transition_audit",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .unwrap(),
            baseline_audits
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_handoffs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn managed_actor_uses_exact_markers_and_typed_native_pin() {
        let fixture = fixture(true);
        let markers = ManagedActorMarkers {
            chain_id: fixture.chain.id.clone(),
            generation: fixture.source.generation,
            launch_nonce: "launch-source".to_string(),
            process_birth_identity: fixture.source.process_birth_identity.clone(),
        };
        let resolved = resolve_managed_actor(
            &fixture.db,
            &fixture.source.instance_name,
            &fixture.source.hcom_session_id,
            &fixture.source.process_id,
            &markers,
        )
        .unwrap();
        assert_eq!(resolved.native_session_id.as_deref(), Some("native-source"));
        assert_ne!(
            resolved.native_session_id.as_deref(),
            Some(resolved.hcom_session_id.as_str())
        );

        let mut wrong = markers.clone();
        wrong.launch_nonce = "wrong-launch".to_string();
        assert!(matches!(
            resolve_managed_actor(
                &fixture.db,
                &fixture.source.instance_name,
                &fixture.source.hcom_session_id,
                &fixture.source.process_id,
                &wrong,
            ),
            Err(HandoffError::NotManaged)
        ));
        wrong = markers.clone();
        wrong.process_birth_identity = "wrong-birth".to_string();
        assert!(matches!(
            resolve_managed_actor(
                &fixture.db,
                &fixture.source.instance_name,
                &fixture.source.hcom_session_id,
                &fixture.source.process_id,
                &wrong,
            ),
            Err(HandoffError::NotManaged)
        ));
        assert!(matches!(
            resolve_managed_actor(
                &fixture.db,
                &fixture.source.instance_name,
                "wrong-hcom-session",
                &fixture.source.process_id,
                &markers,
            ),
            Err(HandoffError::NotManaged)
        ));
        assert!(matches!(
            resolve_managed_actor(
                &fixture.db,
                &fixture.source.instance_name,
                &fixture.source.hcom_session_id,
                "wrong-process",
                &markers,
            ),
            Err(HandoffError::NotManaged)
        ));
    }

    #[test]
    fn ready_does_not_accept_and_wrong_target_cannot_advance() {
        let fixture = fixture(true);
        let launching = advance_to_launching(&fixture);
        let target = target_actor();
        let generation = load_generation(fixture.db.conn(), &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        assert!(matches!(
            target_ready(
                &fixture.db,
                &target,
                &launching.handoff.id,
                launching.handoff.version,
                "ln-wrong"
            ),
            Err(HandoffError::Conflict(_))
        ));
        let mut resumed_source_session = target.clone();
        resumed_source_session.native_session_id = fixture.source.native_session_id.clone();
        assert!(matches!(
            target_ready(
                &fixture.db,
                &resumed_source_session,
                &launching.handoff.id,
                launching.handoff.version,
                &generation.launch_nonce,
            ),
            Err(HandoffError::Conflict(_))
        ));

        let ready = target_ready(
            &fixture.db,
            &target,
            &launching.handoff.id,
            launching.handoff.version,
            &generation.launch_nonce,
        )
        .unwrap();
        assert_eq!(ready.handoff.state, HandoffState::AwaitingAcceptance);
        assert_ne!(
            load_generation(fixture.db.conn(), &fixture.chain.id, 2)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::Active
        );
        let mut spoof = target.clone();
        spoof.instance_name = "spoof".to_string();
        spoof.process_id = "process-spoof".to_string();
        spoof.process_birth_identity = "birth-spoof".to_string();
        spoof.hcom_session_id = "hcom-spoof".to_string();
        spoof.native_session_id = Some("native-spoof".to_string());
        add_live_actor(&fixture.db, &spoof);
        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &spoof,
                &ready.handoff.id,
                inspection.handoff.version,
                &fixture.workspace
            ),
            Err(HandoffError::TypedConflict {
                code: "wrong_target_actor",
                ..
            })
        ));
    }

    #[test]
    fn exact_prepare_replays_but_different_bundle_conflicts() {
        let fixture = fixture(true);
        let first_event = create_bundle(&fixture.db, &fixture.source, 0);
        let first = prepare_handoff(
            &fixture.db,
            &fixture.source,
            first_event,
            &fixture.workspace,
        )
        .unwrap();
        let replay = prepare_handoff(
            &fixture.db,
            &fixture.source,
            first_event,
            &fixture.workspace,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(first.handoff.id, replay.handoff.id);

        let second_event = create_bundle(&fixture.db, &fixture.source, 1);
        assert!(matches!(
            prepare_handoff(
                &fixture.db,
                &fixture.source,
                second_event,
                &fixture.workspace
            ),
            Err(HandoffError::Conflict(_))
        ));
    }

    #[test]
    fn bundle_event_must_be_exact_local_owned_and_bounded() {
        let fixture = fixture(true);
        assert!(matches!(
            prepare_handoff(&fixture.db, &fixture.source, 0, &fixture.workspace),
            Err(HandoffError::Invalid(_))
        ));
        assert!(matches!(
            prepare_handoff(&fixture.db, &fixture.source, i64::MAX, &fixture.workspace),
            Err(HandoffError::Invalid(_))
        ));
        fixture
            .db
            .log_event(
                "message",
                &fixture.source.instance_name,
                &serde_json::json!({}),
            )
            .unwrap();
        let wrong_type: i64 = fixture
            .db
            .conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(
            prepare_handoff(&fixture.db, &fixture.source, wrong_type, &fixture.workspace),
            Err(HandoffError::Invalid(_))
        ));

        let other = HandoffActor {
            instance_name: "other".to_string(),
            ..fixture.source.clone()
        };
        let wrong_owner = create_bundle(&fixture.db, &other, 0);
        assert!(matches!(
            prepare_handoff(
                &fixture.db,
                &fixture.source,
                wrong_owner,
                &fixture.workspace
            ),
            Err(HandoffError::Invalid(_))
        ));

        for size in [
            MAX_HANDOFF_BUNDLE_BYTES - 1,
            MAX_HANDOFF_BUNDLE_BYTES,
            MAX_HANDOFF_BUNDLE_BYTES + 1,
        ] {
            let event_id = create_bundle_with_serialized_size(&fixture.db, &fixture.source, size);
            let snapshot =
                load_bundle_snapshot(fixture.db.conn(), event_id, &fixture.source.instance_name);
            assert_eq!(snapshot.is_ok(), size <= MAX_HANDOFF_BUNDLE_BYTES);
            if let Ok(snapshot) = snapshot {
                assert_eq!(snapshot.size_bytes, size as i64);
            }
        }

        let foreign_dir = tempfile::tempdir().unwrap();
        let foreign_db = HcomDb::open_at(&foreign_dir.path().join("foreign.db")).unwrap();
        foreign_db
            .conn()
            .execute(
                "INSERT INTO events (id, timestamp, type, instance, data)
                 VALUES (
                     9000001, '2026-07-27T00:00:00Z', 'bundle', ?1,
                     '{\"bundle_id\":\"foreign\",\"created_by\":\"source\"}'
                 )",
                params![fixture.source.instance_name],
            )
            .unwrap();
        assert!(matches!(
            load_bundle_snapshot(fixture.db.conn(), 9_000_001, &fixture.source.instance_name),
            Err(HandoffError::Invalid(_))
        ));
    }

    #[test]
    fn every_named_text_bound_covers_limit_minus_one_limit_and_plus_one() {
        for (field, limit) in [
            ("opaque identifier", MAX_OPAQUE_ID_BYTES),
            ("instance identity", MAX_INSTANCE_NAME_BYTES),
            ("process identity", MAX_PROCESS_ID_BYTES),
            ("session identity", MAX_IDENTITY_BYTES),
            ("workspace", MAX_WORKSPACE_BYTES),
            ("model reference", MAX_MODEL_REF_BYTES),
            ("reasoning reference", MAX_REASONING_REF_BYTES),
            ("policy reference", MAX_POLICY_REF_BYTES),
            ("revision", MAX_REVISION_BYTES),
            ("branch", MAX_BRANCH_BYTES),
            ("dirty summary", MAX_DIRTY_SUMMARY_BYTES),
            ("failure kind", MAX_FAILURE_KIND_BYTES),
            ("failure reason", MAX_FAILURE_REASON_BYTES),
        ] {
            for size in [limit - 1, limit, limit + 1] {
                let result = validate_text(&"x".repeat(size), field, limit, false);
                assert_eq!(
                    result.is_ok(),
                    size <= limit,
                    "{field} size={size} limit={limit}"
                );
            }
        }
        assert!(validate_text("", "required", 1, false).is_err());
        assert!(validate_text("x\n", "control", 8, false).is_err());
        assert!(validate_opaque_id(&"x".repeat(MAX_OPAQUE_ID_BYTES), "ID").is_ok());
        assert!(validate_opaque_id(&"x".repeat(MAX_OPAQUE_ID_BYTES + 1), "ID").is_err());
        assert!(validate_opaque_id("not/opaque", "ID").is_err());
        let multibyte_overflow = format!("{}é", "x".repeat(MAX_IDENTITY_BYTES - 1));
        assert_eq!(multibyte_overflow.chars().count(), MAX_IDENTITY_BYTES);
        assert_eq!(multibyte_overflow.len(), MAX_IDENTITY_BYTES + 1);
        assert!(validate_text(&multibyte_overflow, "identity", MAX_IDENTITY_BYTES, false).is_err());
        assert!(sanitize_reason(&"x".repeat(MAX_FAILURE_REASON_BYTES)).is_ok());
        assert!(sanitize_reason(&"x".repeat(MAX_FAILURE_REASON_BYTES + 1)).is_err());
        assert_ne!(
            hash_parts(&[b"ab".as_slice(), b"c".as_slice()]),
            hash_parts(&[b"a".as_slice(), b"bc".as_slice()])
        );
    }

    #[test]
    fn workspace_snapshot_counts_rename_records_without_persisting_paths() {
        let fixture = fixture(true);
        let renamed = fixture.workspace.join("sensitive-renamed-file.txt");
        std::fs::rename(fixture.workspace.join("README.md"), &renamed).unwrap();
        run_git(&fixture.workspace, &["add", "-A"]);
        std::fs::write(&renamed, "unstaged after rename\n").unwrap();
        std::fs::write(
            fixture.workspace.join("sensitive-untracked-file.txt"),
            "untracked\n",
        )
        .unwrap();

        let snapshot = snapshot_workspace(&fixture.workspace).unwrap();
        assert_eq!(
            snapshot.dirty_summary,
            "staged=1,unstaged=1,untracked=1,conflicted=0"
        );
        assert!(!snapshot.dirty_summary.contains("sensitive"));
        assert!(!snapshot.dirty_summary.contains("README"));
    }

    #[test]
    fn slow_workspace_snapshot_does_not_hold_writer_transaction() {
        use std::sync::mpsc;
        use std::time::Duration;

        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let db_path = fixture.db.path().to_path_buf();
        let actor = fixture.source.clone();
        let workspace = fixture.workspace.clone();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);

        let handle = std::thread::spawn(move || {
            let db = HcomDb::open_at(&db_path).unwrap();
            prepare_handoff_with_snapshot_provider(&db, &actor, event_id, &workspace, |path| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                snapshot_workspace(path)
            })
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("snapshot provider did not start");
        fixture
            .db
            .conn()
            .execute(
                "INSERT INTO kv (key, value) VALUES ('slow-git-writer', 'ok')",
                [],
            )
            .expect("slow workspace snapshot held the SQLite writer lock");
        release_tx.send(()).unwrap();
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn target_reservation_neither_creates_nor_adopts_generic_pending_instance() {
        let fixture = fixture(true);
        fixture
            .db
            .conn()
            .execute(
                "INSERT INTO instances (
                     name, session_id, status, tool, created_at,
                     parent_name, origin_device_id
                 ) VALUES (
                     'pending-generic', 'pending-session', 'pending',
                     'codex', 1.0, '', ''
                 )",
                [],
            )
            .unwrap();
        let instances_before: Vec<(String, Option<String>, String)> = fixture
            .db
            .conn()
            .prepare("SELECT name, session_id, status FROM instances ORDER BY name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();

        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let instances_after: Vec<(String, Option<String>, String)> = fixture
            .db
            .conn()
            .prepare("SELECT name, session_id, status FROM instances ORDER BY name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(instances_before, instances_after);
        let target = load_generation(fixture.db.conn(), &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        assert_eq!(target.state, GenerationState::Reserved);
        assert!(target.wrapper_process_id.is_none());
        assert!(target.instance_name.is_none());
        assert_eq!(prepared.handoff.target_generation, 2);
    }

    #[test]
    fn exact_target_materialization_is_replayable_and_ready_does_not_accept() {
        let fixture = fixture(true);
        let launching = advance_to_unmaterialized_launching(&fixture);
        let target = target_actor();
        let generation = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        assert!(generation.wrapper_process_id.is_none());
        let materialization = TargetMaterialization {
            expected_version: launching.handoff.version,
            launch_nonce: generation.launch_nonce.clone(),
            instance_name: target.instance_name.clone(),
            hcom_session_id: target.hcom_session_id.clone(),
            process_id: target.process_id.clone(),
            process_birth_identity: target.process_birth_identity.clone(),
            wrapper_pid: 41_002,
            wrapper_pgid: 41_001,
            child_pid: 41_003,
            child_pgid: 41_003,
            child_process_birth_identity: "child-birth-target".to_string(),
        };
        begin_generation_prepare(
            &fixture.db,
            &supervisor_actor(&fixture),
            &fixture.chain.id,
            target.generation,
            launching.handoff.version,
            &generation.launch_nonce,
        )
        .unwrap();
        let materialized = materialize_target_generation(
            &fixture.db,
            &supervisor_actor(&fixture),
            &launching.handoff.id,
            &materialization,
        )
        .unwrap();
        assert!(!materialized.replayed);
        let audit_before = audit_count(&fixture.db);
        assert!(
            materialize_target_generation(
                &fixture.db,
                &supervisor_actor(&fixture),
                &launching.handoff.id,
                &materialization,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), audit_before);

        let mut conflicting = materialization.clone();
        conflicting.process_id = "process-second-target".to_string();
        conflicting.process_birth_identity = "birth-second-target".to_string();
        assert!(matches!(
            materialize_target_generation(
                &fixture.db,
                &supervisor_actor(&fixture),
                &launching.handoff.id,
                &conflicting,
            ),
            Err(HandoffError::Conflict(_))
        ));
        let mut topology_conflicting = materialization.clone();
        topology_conflicting.child_pid = 41_004;
        topology_conflicting.child_pgid = 41_004;
        topology_conflicting.child_process_birth_identity = "child-birth-second-target".to_string();
        assert!(matches!(
            materialize_target_generation(
                &fixture.db,
                &supervisor_actor(&fixture),
                &launching.handoff.id,
                &topology_conflicting,
            ),
            Err(HandoffError::Conflict(_))
        ));
        let target_instances: Vec<(String, String, String)> = fixture
            .db
            .conn()
            .prepare(
                "SELECT name, session_id, status FROM instances
                 WHERE name IN ('target', 'pending-generic')
                 ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            target_instances,
            vec![(
                "target".to_string(),
                "hcom-target".to_string(),
                "launching".to_string(),
            )]
        );

        pin_target_fixture(&fixture, &target);
        let ready = target_ready(
            &fixture.db,
            &target,
            &materialized.handoff.id,
            materialized.handoff.version,
            &generation.launch_nonce,
        )
        .unwrap();
        assert_eq!(ready.handoff.state, HandoffState::AwaitingAcceptance);
        assert_ne!(ready.handoff.state, HandoffState::Accepted);
        assert_eq!(
            get_generation(&fixture.db, &fixture.chain.id, 2)
                .unwrap()
                .unwrap()
                .state,
            GenerationState::AwaitingAcceptance
        );
    }

    #[test]
    fn materialized_target_failure_removes_exact_live_rows_but_retains_typed_identity() {
        let fixture = fixture(true);
        let launching = advance_to_unmaterialized_launching(&fixture);
        let target = target_actor();
        let materialized = materialize_target_fixture(&fixture, &launching, &target);
        let generation = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        let observation = TargetLaunchFailure {
            expected_version: materialized.handoff.version,
            launch_nonce: generation.launch_nonce.clone(),
            identity: Some(TargetFailureIdentity {
                instance_name: target.instance_name.clone(),
                hcom_session_id: target.hcom_session_id.clone(),
                process_id: target.process_id.clone(),
                process_birth_identity: target.process_birth_identity.clone(),
            }),
            cleanup_completed: true,
            failure_kind: "activation_failed".to_string(),
            failure_reason: "target private gate did not open".to_string(),
        };

        let failed = fail_target_launch(
            &fixture.db,
            &supervisor_actor(&fixture),
            &materialized.handoff.id,
            &observation,
        )
        .unwrap();
        assert!(!failed.replayed);
        assert_eq!(failed.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
        );
        let failed_generation = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        assert_eq!(failed_generation.state, GenerationState::NeedsRecovery);
        assert_eq!(
            failed_generation.wrapper_process_id.as_deref(),
            Some(target.process_id.as_str())
        );
        assert_eq!(
            failed_generation.process_birth_identity.as_deref(),
            Some(target.process_birth_identity.as_str())
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = ?1",
                    [&target.instance_name],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM process_bindings WHERE process_id = ?1",
                    [&target.process_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );

        let audit_before = audit_count(&fixture.db);
        assert!(
            fail_target_launch(
                &fixture.db,
                &supervisor_actor(&fixture),
                &materialized.handoff.id,
                &observation,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), audit_before);
    }

    #[test]
    fn unmaterialized_target_prepare_failure_enters_recovery_without_generic_instance() {
        let fixture = fixture(true);
        let launching = advance_to_unmaterialized_launching(&fixture);
        let generation = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        let failed = fail_target_launch(
            &fixture.db,
            &supervisor_actor(&fixture),
            &launching.handoff.id,
            &TargetLaunchFailure {
                expected_version: launching.handoff.version,
                launch_nonce: generation.launch_nonce,
                identity: None,
                cleanup_completed: false,
                failure_kind: "prepare_failed".to_string(),
                failure_reason: "fake adapter rejected the target reservation".to_string(),
            },
        )
        .unwrap();

        assert_eq!(failed.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
        );
        let target_generation = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        assert_eq!(target_generation.state, GenerationState::NeedsRecovery);
        assert!(target_generation.wrapper_process_id.is_none());
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = 'target'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn each_incomplete_cleanup_component_blocks_target_materialization() {
        for failure in [
            "unreaped",
            "inject",
            "delivery",
            "pty",
            "screen",
            "write_queue",
            "missing_exit",
        ] {
            let fixture = fixture(true);
            let sigterm = advance_to_sigterm(&fixture);
            remove_live_actor(&fixture.db, &fixture.source);
            let mut cleanup = successful_cleanup(sigterm.handoff.version);
            match failure {
                "unreaped" => cleanup.reaped = false,
                "inject" => cleanup.resources.inject_succeeded = false,
                "delivery" => cleanup.resources.delivery_succeeded = false,
                "pty" => cleanup.resources.pty_succeeded = false,
                "screen" => cleanup.resources.screen_succeeded = false,
                "write_queue" => cleanup.resources.write_queue_succeeded = false,
                "missing_exit" => cleanup.exit = None,
                _ => unreachable!(),
            }
            cleanup.failure_kind = failure.to_string();
            cleanup.failure_reason = format!("injected {failure} failure");
            let outcome = complete_source_cleanup(
                &fixture.db,
                &supervisor_actor(&fixture),
                &sigterm.handoff.id,
                &cleanup,
            )
            .unwrap();
            assert_eq!(
                outcome.handoff.state,
                HandoffState::NeedsRecovery,
                "failure={failure}"
            );
            assert_eq!(
                get_chain(&fixture.db, &fixture.chain.id)
                    .unwrap()
                    .unwrap()
                    .state,
                ChainState::NeedsRecovery,
                "failure={failure}"
            );
            let target = get_generation(&fixture.db, &fixture.chain.id, 2)
                .unwrap()
                .unwrap();
            assert_eq!(target.state, GenerationState::Reserved);
            assert!(target.wrapper_process_id.is_none());
            let target_instance_count: i64 = fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = 'target'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(target_instance_count, 0);
        }
    }

    #[test]
    fn successful_cleanup_rejects_contradictory_failure_evidence() {
        let fixture = fixture(true);
        let sigterm = advance_to_sigterm(&fixture);
        let mut cleanup = successful_cleanup(sigterm.handoff.version);
        cleanup.failure_kind = "contradictory_failure".to_string();
        assert!(matches!(
            complete_source_cleanup(
                &fixture.db,
                &supervisor_actor(&fixture),
                &sigterm.handoff.id,
                &cleanup,
            ),
            Err(HandoffError::Invalid(_))
        ));
        assert_eq!(
            get_handoff(&fixture.db, &sigterm.handoff.id)
                .unwrap()
                .unwrap()
                .state,
            HandoffState::QuiescingSource
        );
    }

    #[test]
    fn failed_sigterm_delivery_is_one_shot_and_enters_recovery() {
        let fixture = fixture(true);
        let committed = prepare_and_commit(&fixture);
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = observe_stop(
            &fixture.db,
            &fixture.source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: committed.handoff.version,
                quiesce_token: token.clone(),
                committed_version: committed.handoff.version,
                hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
                launch_nonce: committed.handoff.source_launch_nonce.clone(),
                turn_id: "turn-source".to_string(),
            },
        )
        .unwrap();
        let quiescing = begin_quiesce(
            &fixture.db,
            &supervisor_actor(&fixture),
            &stopped.handoff.id,
            stopped.handoff.version,
            &token,
        )
        .unwrap();
        let observation = SigtermObservation {
            expected_version: quiescing.handoff.version,
            requested_wall_at: 2000.0,
            requested_monotonic_ns: 2_000_000_000,
            result: SigtermRequestResult::NotFound,
        };
        let failed = record_sigterm_request(
            &fixture.db,
            &supervisor_actor(&fixture),
            &quiescing.handoff.id,
            &observation,
        )
        .unwrap();
        assert_eq!(failed.handoff.state, HandoffState::NeedsRecovery);
        assert_eq!(failed.handoff.sigterm_request_result, "not_found");
        let audit_before = audit_count(&fixture.db);
        assert!(
            record_sigterm_request(
                &fixture.db,
                &supervisor_actor(&fixture),
                &quiescing.handoff.id,
                &observation,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), audit_before);
        let mut second_attempt = observation;
        second_attempt.result = SigtermRequestResult::Sent;
        assert!(matches!(
            record_sigterm_request(
                &fixture.db,
                &supervisor_actor(&fixture),
                &quiescing.handoff.id,
                &second_attempt,
            ),
            Err(HandoffError::Conflict(_))
        ));
    }

    #[test]
    fn source_instance_deletion_does_not_delete_handoff_or_audit() {
        let fixture = fixture(true);
        let committed = prepare_and_commit(&fixture);
        let audit_before: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit WHERE chain_id = ?1",
                params![fixture.chain.id],
                |row| row.get(0),
            )
            .unwrap();
        remove_live_actor(&fixture.db, &fixture.source);
        assert!(
            get_handoff(&fixture.db, &committed.handoff.id)
                .unwrap()
                .is_some()
        );
        let audit_after: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit WHERE chain_id = ?1",
                params![fixture.chain.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_before, audit_after);
    }

    #[test]
    fn successful_retirement_logs_one_bounded_stop_and_replay_preserves_name_reuse() {
        let fixture = fixture(true);
        let sigterm = advance_to_sigterm(&fixture);
        let observation = successful_cleanup(sigterm.handoff.version);
        let retired = complete_source_cleanup(
            &fixture.db,
            &supervisor_actor(&fixture),
            &sigterm.handoff.id,
            &observation,
        )
        .unwrap();
        assert_eq!(retired.handoff.state, HandoffState::LaunchingTarget);
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = ?1",
                    [&fixture.source.instance_name],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let lifecycle: String = fixture
            .db
            .conn()
            .query_row(
                "SELECT data FROM events
                 WHERE type = 'life' AND instance = ?1
                   AND json_extract(data, '$.action') = 'stopped'",
                [&fixture.source.instance_name],
                |row| row.get(0),
            )
            .unwrap();
        assert!(lifecycle.len() <= MAX_STATUS_JSON_BYTES);
        let lifecycle: serde_json::Value = serde_json::from_str(&lifecycle).unwrap();
        assert_eq!(lifecycle["by"], "chain-supervisor");
        assert_eq!(lifecycle["reason"], "handoff");
        assert_eq!(lifecycle["snapshot"]["tool"], "codex");
        assert_eq!(
            lifecycle["snapshot"]["session_id"],
            fixture.source.hcom_session_id
        );

        let mut reused = fixture.source.clone();
        reused.hcom_session_id = "hcom-reused".to_string();
        reused.native_session_id = Some("native-reused".to_string());
        reused.process_id = "process-reused".to_string();
        reused.process_birth_identity = "birth-reused".to_string();
        add_live_actor(&fixture.db, &reused);
        assert!(
            complete_source_cleanup(
                &fixture.db,
                &supervisor_actor(&fixture),
                &sigterm.handoff.id,
                &observation,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE type = 'life' AND instance = ?1
                       AND json_extract(data, '$.action') = 'stopped'",
                    [&fixture.source.instance_name],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT session_id FROM instances WHERE name = ?1",
                    [&fixture.source.instance_name],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            reused.hcom_session_id
        );
    }

    #[test]
    fn audit_failure_rolls_back_main_chain_and_generation_rows() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        fixture
            .db
            .conn()
            .execute_batch(
                "CREATE TRIGGER fail_handoff_audit
                 BEFORE INSERT ON terminal_transition_audit
                 WHEN NEW.action = 'prepare' AND NEW.object_kind = 'chain'
                 BEGIN
                     SELECT RAISE(ABORT, 'injected audit failure');
                 END;",
            )
            .unwrap();
        let result = prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace);
        assert!(matches!(result, Err(HandoffError::Storage)));
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_handoffs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_generations WHERE chain_id = ?1",
                    params![fixture.chain.id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        let source = load_generation(fixture.db.conn(), &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        assert_eq!(source.state, GenerationState::Active);
        assert_eq!(source.version, 0);
        let chain = get_chain(&fixture.db, &fixture.chain.id).unwrap().unwrap();
        assert_eq!(chain.state, ChainState::Active);
        assert_eq!(chain.version, 0);
        assert_eq!(audit_count(&fixture.db), 2);
    }

    #[test]
    fn concurrent_commit_has_one_transition_and_one_replay() {
        let fixture = fixture(true);
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let db_path = fixture.db.path().to_path_buf();
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&barrier);
                let actor = fixture.source.clone();
                let handoff_id = prepared.handoff.id.clone();
                let workspace = fixture.workspace.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&db_path).unwrap();
                    barrier.wait();
                    commit_handoff(&db, &actor, &handoff_id, 0, &workspace)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.replayed).count(),
            1
        );
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.replayed).count(),
            1
        );
        let commits: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1 AND action = 'commit'",
                params![prepared.handoff.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(commits, 1);
        assert_audit_continuity(&fixture.db, "handoff", &prepared.handoff.id);
        assert_audit_continuity(&fixture.db, "chain", &fixture.chain.id);
        assert_audit_continuity(
            &fixture.db,
            "generation",
            &generation_object_id(&fixture.chain.id, 1),
        );
    }

    #[test]
    fn concurrent_stop_has_one_transition_and_one_replay() {
        let fixture = fixture(true);
        let committed = prepare_and_commit(&fixture);
        let observation = StopObservation {
            expected_version: committed.handoff.version,
            quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
            committed_version: committed.handoff.version,
            hook_native_session_id: fixture.source.native_session_id.clone().unwrap(),
            launch_nonce: committed.handoff.source_launch_nonce.clone(),
            turn_id: "turn-source".to_string(),
        };
        let db_path = fixture.db.path().to_path_buf();
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&barrier);
                let actor = fixture.source.clone();
                let handoff_id = committed.handoff.id.clone();
                let observation = observation.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&db_path).unwrap();
                    barrier.wait();
                    observe_stop(&db, &actor, &handoff_id, &observation)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.replayed).count(),
            1
        );
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.replayed).count(),
            1
        );
        let transitions: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND action = 'observe_stop'",
                params![committed.handoff.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(transitions, 1);
        assert_audit_continuity(&fixture.db, "handoff", &committed.handoff.id);
    }

    #[test]
    fn concurrent_accept_has_one_transition_and_one_replay() {
        let fixture = fixture(true);
        let (ready, target) = advance_to_awaiting_acceptance(&fixture);
        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        let expected_version = inspection.handoff.version;
        let db_path = fixture.db.path().to_path_buf();
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&barrier);
                let actor = target.clone();
                let handoff_id = ready.handoff.id.clone();
                let workspace = fixture.workspace.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&db_path).unwrap();
                    barrier.wait();
                    accept_handoff(&db, &actor, &handoff_id, expected_version, &workspace)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.replayed).count(),
            1
        );
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.replayed).count(),
            1
        );
        let transitions: i64 = fixture
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_transition_audit
                 WHERE object_kind = 'handoff' AND object_id = ?1
                   AND action = 'accept'",
                params![ready.handoff.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(transitions, 1);
        assert_audit_continuity(&fixture.db, "handoff", &ready.handoff.id);
    }

    #[test]
    fn target_accept_and_reject_are_mutually_exclusive() {
        let fixture = fixture(true);
        let (ready, target) = advance_to_awaiting_acceptance(&fixture);
        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            inspection.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(accepted.handoff.state, HandoffState::Accepted);
        assert!(matches!(
            reject_handoff(
                &fixture.db,
                &target,
                &ready.handoff.id,
                ready.handoff.version,
                "late rejection",
                &fixture.workspace
            ),
            Err(HandoffError::Conflict(_))
        ));
    }

    #[test]
    fn target_must_inspect_bundle_and_current_instructions_before_accepting() {
        let fixture = fixture(true);
        std::fs::write(
            fixture.workspace.join("AGENTS.md"),
            "phase3 instruction sentinel one\n",
        )
        .unwrap();
        let (ready, target) = advance_to_awaiting_acceptance(&fixture);
        let audit_before = audit_count(&fixture.db);
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &target,
                &ready.handoff.id,
                ready.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::TypedConflict {
                code: "durable_inspection_required",
                ..
            })
        ));
        assert_eq!(audit_count(&fixture.db), audit_before);

        let inspection = inspect_target_fixture(&fixture, &ready, &target);
        assert_eq!(inspection.handoff.state, HandoffState::AwaitingAcceptance);
        assert_eq!(inspection.handoff.version, ready.handoff.version + 1);
        assert_eq!(
            inspection
                .bundle
                .get("created_by")
                .and_then(serde_json::Value::as_str),
            Some(fixture.source.instance_name.as_str())
        );
        assert!(inspection.instructions.iter().any(|instruction| {
            instruction.scope == "workspace"
                && instruction.path == "AGENTS.md"
                && instruction.content == "phase3 instruction sentinel one\n"
        }));
        let replay = inspect_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.instructions_digest, inspection.instructions_digest);
        std::fs::write(
            fixture.workspace.join("AGENTS.md"),
            "phase3 instruction sentinel two\n",
        )
        .unwrap();
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &target,
                &ready.handoff.id,
                inspection.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::TypedConflict {
                code: "target_validation_changed",
                ..
            })
        ));
        let rejected = reject_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            inspection.handoff.version,
            "project instructions changed after validation",
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(rejected.handoff.state, HandoffState::NeedsRecovery);
    }

    #[test]
    fn public_reservation_is_single_winner_and_session_start_activates() {
        let fixture = public_active_fixture();
        let chain = get_chain(&fixture.db, &fixture.chain_id).unwrap().unwrap();
        let generation = get_generation(&fixture.db, &fixture.chain_id, 1)
            .unwrap()
            .unwrap();
        assert_eq!(chain.state, ChainState::Active);
        assert_eq!(generation.state, GenerationState::Active);
        assert_eq!(
            generation.native_session_id.as_deref(),
            Some("public-source-native")
        );
        assert!(
            get_generation_process(&fixture.db, &fixture.chain_id, 1)
                .unwrap()
                .is_some()
        );

        let other_pid = absent_pid_and_group(1_600_000);
        let other_pgid = absent_pid_and_group(1_620_000);
        let conflict = create_public_chain_reservation(
            &fixture.db,
            &public_spec(
                &fixture.workspace,
                other_pid,
                other_pgid,
                absent_birth(other_pid),
                "loser",
                fixture.tty_device + 2,
                fixture.tty_inode + 2,
            ),
        )
        .unwrap_err();
        assert!(matches!(conflict, HandoffError::Conflict(_)));
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_chains", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn concurrent_public_start_and_recover_each_have_one_durable_winner() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let db_path = directory.path().join("hcom.db");
        let db = HcomDb::open_at(&db_path).unwrap();
        let old_pid = absent_pid_and_group(1_640_000);
        let old_pgid = absent_pid_and_group(1_660_000);
        let base = public_spec(
            &workspace,
            old_pid,
            old_pgid,
            absent_birth(old_pid),
            "concurrent",
            81,
            83,
        );
        drop(db);

        let barrier = Arc::new(Barrier::new(2));
        let starts: Vec<_> = (0..2)
            .map(|index| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&barrier);
                let mut spec = base.clone();
                spec.supervisor_process_id = format!("start-supervisor-{index}");
                spec.launch_nonce = format!("start-launch-{index}");
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&db_path).unwrap();
                    barrier.wait();
                    create_public_chain_reservation(&db, &spec)
                })
            })
            .collect();
        let outcomes: Vec<_> = starts
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(HandoffError::Conflict(_))))
                .count(),
            1
        );

        let db = HcomDb::open_at(&db_path).unwrap();
        let chain: TerminalChain = db
            .conn()
            .query_row(
                "SELECT * FROM terminal_chains LIMIT 1",
                [],
                TerminalChain::from_row,
            )
            .unwrap();
        let owner_base =
            {
                // SAFETY: these libc calls have no preconditions.
                let pid = unsafe { libc::getpid() };
                let pgid = unsafe { libc::getpgrp() };
                TerminalOwnerEvidence {
                    workspace: workspace.clone(),
                    supervisor: SupervisorActor {
                        process_id: "recover-owner".to_string(),
                        process_birth_identity:
                            hcom::chain_supervisor::linux_process_birth_identity(pid).unwrap(),
                    },
                    supervisor_pid: i64::from(pid),
                    supervisor_pgid: i64::from(pgid),
                    outer_foreground_pgid: i64::from(pgid),
                    outer_tty_device: 81,
                    outer_tty_inode: 83,
                }
            };
        let recover_barrier = Arc::new(Barrier::new(2));
        let recovers: Vec<_> = (0..2)
            .map(|index| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&recover_barrier);
                let mut owner = owner_base.clone();
                owner.supervisor.process_id = format!("recover-owner-{index}");
                let chain_id = chain.id.clone();
                let version = chain.version;
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&db_path).unwrap();
                    barrier.wait();
                    begin_public_recovery(&db, &chain_id, version, &owner)
                })
            })
            .collect();
        let outcomes: Vec<_> = recovers
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Ok(RecoveryOutcome::Launch(_))))
                .count(),
            1
        );
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_recovery_attempts",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .unwrap(),
            1
        );
        assert_eq!(
            db.conn()
                .query_row("SELECT COUNT(*) FROM terminal_generations", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
    }

    #[test]
    fn recovery_rejects_live_reused_and_unknown_supervisor_without_mutation() {
        enum Case {
            Live,
            Reused,
            Unknown,
        }
        for (index, case, expected_code) in [
            (0, Case::Live, "old_process_still_live"),
            (1, Case::Reused, "process_identity_reused"),
            (2, Case::Unknown, "process_absence_unknown"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let workspace = directory.path().join("workspace");
            std::fs::create_dir(&workspace).unwrap();
            let db = HcomDb::open_at(&directory.path().join("hcom.db")).unwrap();
            // SAFETY: these libc calls have no preconditions.
            let pid = unsafe { libc::getpid() };
            let pgid = unsafe { libc::getpgrp() };
            let exact = hcom::chain_supervisor::linux_process_birth_identity(pid).unwrap();
            let birth = match case {
                Case::Live => exact,
                Case::Reused => {
                    let mut parts = exact.splitn(4, ':');
                    let prefix = parts.next().unwrap();
                    let parsed_pid = parts.next().unwrap();
                    let start = parts.next().unwrap().parse::<u64>().unwrap() + 1;
                    let boot = parts.next().unwrap();
                    format!("{prefix}:{parsed_pid}:{start}:{boot}")
                }
                Case::Unknown => "not-a-process-birth-identity".to_string(),
            };
            let reservation = create_public_chain_reservation(
                &db,
                &public_spec(
                    &workspace,
                    pid,
                    pgid,
                    birth,
                    &format!("blocked-{index}"),
                    91 + index,
                    101 + index,
                ),
            )
            .unwrap();
            let owner =
                TerminalOwnerEvidence {
                    workspace: workspace.clone(),
                    supervisor: SupervisorActor {
                        process_id: format!("new-owner-{index}"),
                        process_birth_identity:
                            hcom::chain_supervisor::linux_process_birth_identity(pid).unwrap(),
                    },
                    supervisor_pid: i64::from(pid),
                    supervisor_pgid: i64::from(pgid),
                    outer_foreground_pgid: i64::from(pgid),
                    outer_tty_device: 91 + index,
                    outer_tty_inode: 101 + index,
                };
            let result = begin_public_recovery(
                &db,
                &reservation.chain.id,
                reservation.chain.version,
                &owner,
            );
            assert!(
                matches!(
                    &result,
                    Err(HandoffError::TypedConflict { code, .. }) if *code == expected_code
                ),
                "case={index} expected={expected_code} result={result:?}"
            );
            assert_eq!(
                db.conn()
                    .query_row(
                        "SELECT COUNT(*) FROM terminal_recovery_attempts",
                        [],
                        |row| { row.get::<_, i64>(0) }
                    )
                    .unwrap(),
                0
            );
            assert_eq!(
                db.conn()
                    .query_row("SELECT COUNT(*) FROM terminal_generations", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn recovery_planner_covers_each_phase4_state_class() {
        let fixture = fixture(true);
        let committed = prepare_and_commit(&fixture);
        let base_chain = get_chain(&fixture.db, &fixture.chain.id).unwrap().unwrap();
        let source = get_generation(&fixture.db, &fixture.chain.id, 1)
            .unwrap()
            .unwrap();
        let target = get_generation(&fixture.db, &fixture.chain.id, 2)
            .unwrap()
            .unwrap();
        let base_handoff = committed.handoff;

        let mut chain = base_chain.clone();
        chain.state = ChainState::Active;
        assert_eq!(
            recovery_plan(&chain, &source, None, true),
            RecoveryPlanCode::SourceDeadBeforeCommit
        );
        chain.state = ChainState::Prepared;
        let mut handoff = base_handoff.clone();
        handoff.state = HandoffState::Prepared;
        assert_eq!(
            recovery_plan(&chain, &source, Some(&handoff), true),
            RecoveryPlanCode::SourceDeadBeforeCommit
        );

        for (chain_state, handoff_state) in [
            (ChainState::Committed, HandoffState::Committed),
            (ChainState::StopObserved, HandoffState::StopObserved),
            (ChainState::QuiescingSource, HandoffState::QuiescingSource),
        ] {
            chain.state = chain_state;
            handoff.state = handoff_state;
            assert_eq!(
                recovery_plan(&chain, &source, Some(&handoff), true),
                RecoveryPlanCode::ContinueAfterSourceAbsence
            );
        }

        chain.state = ChainState::LaunchingTarget;
        handoff.state = HandoffState::LaunchingTarget;
        assert_eq!(
            recovery_plan(&chain, &target, Some(&handoff), false),
            RecoveryPlanCode::RetryUnmaterializedTarget
        );
        assert_eq!(
            recovery_plan(&chain, &target, Some(&handoff), true),
            RecoveryPlanCode::AbsenceUnknown
        );
        let mut materialized = target.clone();
        materialized.wrapper_process_id = Some("target-process".to_string());
        assert_eq!(
            recovery_plan(&chain, &materialized, Some(&handoff), false),
            RecoveryPlanCode::AbsenceUnknown
        );
        assert_eq!(
            recovery_plan(&chain, &materialized, Some(&handoff), true),
            RecoveryPlanCode::ReplaceDeadTarget
        );

        chain.state = ChainState::AwaitingAcceptance;
        handoff.state = HandoffState::AwaitingAcceptance;
        assert_eq!(
            recovery_plan(&chain, &materialized, Some(&handoff), true),
            RecoveryPlanCode::ReplaceDeadAwaitingAcceptance
        );

        chain.state = ChainState::LaunchingTarget;
        let mut initial = source.clone();
        initial.state = GenerationState::Reserved;
        initial.wrapper_process_id = None;
        initial.process_birth_identity = None;
        initial.instance_name = None;
        initial.hcom_session_id = None;
        initial.native_session_id = None;
        assert_eq!(
            recovery_plan(&chain, &initial, None, false),
            RecoveryPlanCode::RetryInitialGeneration
        );
        initial.native_session_id = Some("old-native".to_string());
        assert_eq!(
            recovery_plan(&chain, &initial, None, false),
            RecoveryPlanCode::AbsenceUnknown
        );
        initial.native_session_id = None;
        assert_eq!(
            recovery_plan(&chain, &initial, None, true),
            RecoveryPlanCode::AbsenceUnknown
        );
        chain.state = ChainState::NeedsRecovery;
        initial.state = GenerationState::NeedsRecovery;
        assert_eq!(
            recovery_plan(&chain, &initial, None, false),
            RecoveryPlanCode::RetryInitialGeneration
        );

        handoff.state = HandoffState::NeedsRecovery;
        handoff.failure_kind = "unclassified_failure".to_string();
        assert_eq!(
            recovery_plan(&chain, &materialized, Some(&handoff), true),
            RecoveryPlanCode::UnsupportedRecoveryState
        );
        handoff.failure_kind = "target_launch_failed".to_string();
        assert_eq!(
            recovery_plan(&chain, &materialized, Some(&handoff), true),
            RecoveryPlanCode::ReplaceDeadTarget
        );
    }

    #[test]
    fn dead_active_source_becomes_manual_and_unmaterialized_target_appends() {
        let active = public_active_fixture();
        let active_chain = get_chain(&active.db, &active.chain_id).unwrap().unwrap();
        let active_owner = current_terminal_owner(&active, "manual");
        let RecoveryOutcome::Manual { chain, reason } = begin_public_recovery(
            &active.db,
            &active_chain.id,
            active_chain.version,
            &active_owner,
        )
        .unwrap() else {
            panic!("dead active source must not be automatically continued")
        };
        assert_eq!(reason, RecoveryPlanCode::SourceDeadBeforeCommit);
        assert_eq!(chain.state, ChainState::NeedsRecovery);
        assert_eq!(
            active
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_generations", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert_eq!(
            active
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_recovery_absence_evidence",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            5
        );
        assert_eq!(
            active
                .db
                .conn()
                .query_row(
                    "SELECT state FROM terminal_chain_claims WHERE chain_id = ?1",
                    params![active.chain_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "released"
        );
        assert_eq!(
            active
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_transition_audit
                     WHERE chain_id = ?1 AND object_id = ?2
                       AND from_state = 'active' AND to_state = 'released'
                       AND action = 'release_public_chain_claim'",
                    params![active.chain_id, format!("{}:claim", active.chain_id)],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(
            active
                .db
                .conn()
                .execute(
                    "UPDATE terminal_chain_claims
                     SET state = 'active', version = version + 1, released_at = NULL
                     WHERE chain_id = ?1",
                    params![active.chain_id],
                )
                .is_err(),
            "a released public claim must not be reactivated"
        );
        let second =
            begin_public_recovery(&active.db, &active_chain.id, chain.version, &active_owner)
                .unwrap();
        let RecoveryOutcome::Manual {
            reason: second_reason,
            ..
        } = second
        else {
            panic!("a manual dead-source decision must never become an automatic retry")
        };
        assert_eq!(second_reason, RecoveryPlanCode::SourceDeadBeforeCommit);
        assert_eq!(
            active
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_generations", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert_eq!(
            active
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_recovery_attempts WHERE chain_id = ?1",
                    params![active.chain_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );

        let replacement_supervisor_pid = absent_pid_and_group(1_480_000);
        let replacement_supervisor_pgid = absent_pid_and_group(1_500_000);
        let replacement = create_public_chain_reservation(
            &active.db,
            &public_spec(
                &active.workspace,
                replacement_supervisor_pid,
                replacement_supervisor_pgid,
                absent_birth(replacement_supervisor_pid),
                "replacement",
                active.tty_device,
                active.tty_inode,
            ),
        )
        .unwrap();
        assert_ne!(replacement.chain.id, active.chain_id);
        assert_eq!(
            chain_status_for_terminal_owner(&active.db, None, &active_owner)
                .unwrap()
                .id,
            replacement.chain.id
        );
        assert_eq!(
            chain_status_for_terminal_owner(&active.db, Some(&active.chain_id), &active_owner)
                .unwrap()
                .id,
            active.chain_id
        );

        let launching = public_active_fixture();
        let original_target = advance_public_to_unmaterialized_target(&launching);
        let chain = get_chain(&launching.db, &launching.chain_id)
            .unwrap()
            .unwrap();
        let owner = current_terminal_owner(&launching, "unmaterialized");
        let RecoveryOutcome::Launch(recovery) =
            begin_public_recovery(&launching.db, &chain.id, chain.version, &owner).unwrap()
        else {
            panic!("unmaterialized target must have one append-only retry")
        };
        assert_eq!(recovery.plan, RecoveryPlanCode::RetryUnmaterializedTarget);
        assert!(recovery.generation.generation > original_target.handoff.target_generation);
        assert_eq!(
            launching
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_recovery_absence_evidence
                 WHERE recovery_attempt_id = ?1",
                    params![recovery.attempt_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn explicit_terminal_owner_status_can_read_a_released_chain_handoff() {
        let fixture = public_active_fixture();
        let event_id = create_bundle(&fixture.db, &fixture.source, 0);
        let prepared =
            prepare_handoff(&fixture.db, &fixture.source, event_id, &fixture.workspace).unwrap();
        let owner = current_terminal_owner(&fixture, "released-handoff");
        let chain = get_chain(&fixture.db, &fixture.chain_id).unwrap().unwrap();
        let RecoveryOutcome::Manual { reason, .. } =
            begin_public_recovery(&fixture.db, &chain.id, chain.version, &owner).unwrap()
        else {
            panic!("a dead prepared source must require explicit reconstruction")
        };
        assert_eq!(reason, RecoveryPlanCode::SourceDeadBeforeCommit);

        let replacement_supervisor_pid = absent_pid_and_group(1_520_000);
        let replacement_supervisor_pgid = absent_pid_and_group(1_540_000);
        create_public_chain_reservation(
            &fixture.db,
            &public_spec(
                &fixture.workspace,
                replacement_supervisor_pid,
                replacement_supervisor_pgid,
                absent_birth(replacement_supervisor_pid),
                "status-replacement",
                fixture.tty_device,
                fixture.tty_inode,
            ),
        )
        .unwrap();
        assert_eq!(
            handoff_status_for_terminal_owner(&fixture.db, Some(&prepared.handoff.id), &owner)
                .unwrap()
                .id,
            prepared.handoff.id
        );
    }

    #[test]
    fn prepare_intent_without_process_evidence_never_spawns_a_second_target() {
        let fixture = public_active_fixture();
        let launching = advance_public_to_unmaterialized_target(&fixture);
        let target_generation =
            effective_handoff_target_generation(&fixture.db, &launching.handoff.id).unwrap();
        let target = get_generation(&fixture.db, &fixture.chain_id, target_generation)
            .unwrap()
            .unwrap();
        begin_generation_prepare(
            &fixture.db,
            &fixture.supervisor,
            &fixture.chain_id,
            target_generation,
            launching.handoff.version,
            &target.launch_nonce,
        )
        .unwrap();

        let chain = get_chain(&fixture.db, &fixture.chain_id).unwrap().unwrap();
        let owner = current_terminal_owner(&fixture, "prepare-gap");
        let RecoveryOutcome::Manual {
            chain: recovered,
            reason,
        } = begin_public_recovery(&fixture.db, &chain.id, chain.version, &owner).unwrap()
        else {
            panic!("an interrupted process preparation must require manual recovery")
        };
        assert_eq!(reason, RecoveryPlanCode::AbsenceUnknown);
        assert_eq!(recovered.state, ChainState::NeedsRecovery);
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row("SELECT COUNT(*) FROM terminal_generations", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_generation_prepare_intents
                     WHERE chain_id = ?1 AND generation = ?2",
                    params![fixture.chain_id, target_generation],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn committed_recovery_uses_authorized_absence_without_forging_cleanup() {
        let fixture = public_active_fixture();
        let committed = prepare_public_and_commit(&fixture);
        let chain = get_chain(&fixture.db, &fixture.chain_id).unwrap().unwrap();
        let owner = current_terminal_owner(&fixture, "committed");
        let RecoveryOutcome::Launch(recovery) =
            begin_public_recovery(&fixture.db, &chain.id, chain.version, &owner).unwrap()
        else {
            panic!("committed dead source should have a launch recovery plan")
        };
        assert_eq!(recovery.plan, RecoveryPlanCode::ContinueAfterSourceAbsence);
        assert!(recovery.generation.generation > committed.handoff.target_generation);
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_recovery_absence_evidence
                     WHERE recovery_attempt_id = ?1",
                    params![recovery.attempt_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            5
        );
        revalidate_recovery_absence(&fixture.db, &recovery.attempt_id, &owner.supervisor).unwrap();
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT state FROM terminal_recovery_attempts WHERE id = ?1",
                    params![recovery.attempt_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "authorized"
        );
        let recovered_handoff = get_handoff(&fixture.db, &committed.handoff.id)
            .unwrap()
            .unwrap();
        assert!(recovered_handoff.waitpid_reaped.is_none());
        assert!(recovered_handoff.cleanup_completed_at.is_none());
        assert!(recovered_handoff.sigterm_request_result.is_empty());

        let actor = materialize_public_target(
            &fixture,
            &recovered_handoff,
            &owner.supervisor,
            i32::try_from(owner.supervisor_pgid).unwrap(),
            "absence",
        );
        assert_eq!(actor.generation, recovery.generation.generation);
        assert_eq!(
            fixture
                .db
                .conn()
                .query_row(
                    "SELECT state FROM terminal_recovery_attempts WHERE id = ?1",
                    params![recovery.attempt_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "materialized"
        );
        assert!(
            fixture
                .db
                .conn()
                .execute(
                    "UPDATE terminal_recovery_absence_evidence
                     SET observed_at = observed_at + 1
                     WHERE recovery_attempt_id = ?1",
                    params![recovery.attempt_id],
                )
                .is_err()
        );
        assert!(
            fixture
                .db
                .conn()
                .execute(
                    "DELETE FROM terminal_generation_processes
                     WHERE chain_id = ?1 AND generation = 1",
                    params![fixture.chain_id],
                )
                .is_err()
        );
        assert!(matches!(
            revalidate_recovery_absence(&fixture.db, &recovery.attempt_id, &owner.supervisor,),
            Err(HandoffError::TypedConflict {
                code: "recovery_intent_changed",
                ..
            })
        ));
    }

    #[test]
    fn recovered_awaiting_target_gets_new_generation_and_requires_reinspection() {
        let fixture = public_active_fixture();
        let launching = advance_public_to_unmaterialized_target(&fixture);
        let old_pgid = i32::try_from(
            get_chain(&fixture.db, &fixture.chain_id)
                .unwrap()
                .unwrap()
                .supervisor_pgid
                .unwrap(),
        )
        .unwrap();
        let mut old_target = materialize_public_target(
            &fixture,
            &launching.handoff,
            &fixture.supervisor,
            old_pgid,
            "old",
        );
        let old_generation = get_generation(&fixture.db, &fixture.chain_id, old_target.generation)
            .unwrap()
            .unwrap();
        pin_native_session(
            &fixture.db,
            &fixture.chain_id,
            &old_target,
            old_generation.version,
            "public-target-native-old",
        )
        .unwrap();
        old_target.native_session_id = Some("public-target-native-old".to_string());
        let materialized_handoff = get_handoff(&fixture.db, &launching.handoff.id)
            .unwrap()
            .unwrap();
        let ready = target_ready(
            &fixture.db,
            &old_target,
            &materialized_handoff.id,
            materialized_handoff.version,
            &old_generation.launch_nonce,
        )
        .unwrap();
        let inspected = inspect_handoff(
            &fixture.db,
            &old_target,
            &ready.handoff.id,
            ready.handoff.version,
            &fixture.workspace,
        )
        .unwrap();

        let chain = get_chain(&fixture.db, &fixture.chain_id).unwrap().unwrap();
        let owner = current_terminal_owner(&fixture, "awaiting");
        let RecoveryOutcome::Launch(recovery) =
            begin_public_recovery(&fixture.db, &chain.id, chain.version, &owner).unwrap()
        else {
            panic!("dead awaiting target should have a launch recovery plan")
        };
        assert_eq!(
            recovery.plan,
            RecoveryPlanCode::ReplaceDeadAwaitingAcceptance
        );
        assert!(recovery.generation.generation > old_target.generation);
        revalidate_recovery_absence(&fixture.db, &recovery.attempt_id, &owner.supervisor).unwrap();
        let recovered_handoff = get_handoff(&fixture.db, &ready.handoff.id)
            .unwrap()
            .unwrap();
        let mut new_target = materialize_public_target(
            &fixture,
            &recovered_handoff,
            &owner.supervisor,
            i32::try_from(owner.supervisor_pgid).unwrap(),
            "new",
        );
        let new_generation = get_generation(&fixture.db, &fixture.chain_id, new_target.generation)
            .unwrap()
            .unwrap();
        pin_native_session(
            &fixture.db,
            &fixture.chain_id,
            &new_target,
            new_generation.version,
            "public-target-native-new",
        )
        .unwrap();
        new_target.native_session_id = Some("public-target-native-new".to_string());
        let after_materialize = get_handoff(&fixture.db, &ready.handoff.id)
            .unwrap()
            .unwrap();
        let new_ready = target_ready(
            &fixture.db,
            &new_target,
            &after_materialize.id,
            after_materialize.version,
            &new_generation.launch_nonce,
        )
        .unwrap();
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &new_target,
                &new_ready.handoff.id,
                new_ready.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::TypedConflict {
                code: "durable_inspection_required",
                ..
            })
        ));
        let reinspection = inspect_handoff(
            &fixture.db,
            &new_target,
            &new_ready.handoff.id,
            new_ready.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert!(!reinspection.replayed);
        assert_ne!(new_target.generation, old_target.generation);
        assert_eq!(
            reinspection.instructions_digest,
            inspected.instructions_digest
        );
        let accepted = accept_handoff(
            &fixture.db,
            &new_target,
            &new_ready.handoff.id,
            reinspection.handoff.version,
            &fixture.workspace,
        )
        .unwrap();
        assert_eq!(accepted.handoff.state, HandoffState::Accepted);
    }

    #[test]
    fn instruction_snapshot_follows_bounded_bundle_subtree_scopes() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(workspace.join("src/nested")).unwrap();
        std::fs::write(workspace.join("AGENTS.md"), "root\n").unwrap();
        std::fs::write(workspace.join("src/AGENTS.md"), "src ordinary\n").unwrap();
        std::fs::write(workspace.join("src/AGENTS.override.md"), "src override\n").unwrap();
        std::fs::write(workspace.join("src/nested/file.rs"), "fn main() {}\n").unwrap();
        let canonical = std::fs::canonicalize(&workspace).unwrap();
        let bundle = serde_json::json!({
            "refs": {"files": ["src/nested/file.rs"]}
        });
        let (instructions, digest) =
            load_current_instructions(canonical.to_str().unwrap(), &bundle).unwrap();
        let workspace_instructions: Vec<_> = instructions
            .iter()
            .filter(|instruction| instruction.scope == "workspace")
            .collect();
        assert!(workspace_instructions.iter().any(|instruction| {
            instruction.path == "AGENTS.md" && instruction.content == "root\n"
        }));
        assert!(workspace_instructions.iter().any(|instruction| {
            instruction.path == "src/AGENTS.override.md" && instruction.content == "src override\n"
        }));
        assert!(
            !workspace_instructions
                .iter()
                .any(|instruction| instruction.content == "src ordinary\n")
        );
        assert_eq!(digest.len(), 64);
    }
}
