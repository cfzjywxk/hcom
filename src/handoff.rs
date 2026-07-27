//! Durable, typed control plane for same-terminal Codex handoffs.
//!
//! This module deliberately has no PTY, hook, launcher, signal, or process
//! control integration. Later phases may call these service APIs, but Phase 1
//! only persists and validates deterministic state transitions.

use std::fmt;
use std::path::{Path, PathBuf};
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
pub struct HandoffOutcome {
    pub handoff: TerminalHandoff,
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
    #[error("database operation failed")]
    Storage,
}

impl HandoffError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Invalid(_) | Self::Storage => 1,
            Self::Conflict(_) => 2,
            Self::NotManaged => 3,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid_request",
            Self::Conflict(_) => "conflict",
            Self::NotManaged => "not_managed",
            Self::Storage => "storage_error",
        }
    }
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
    if expected_version < 0 {
        return Err(HandoffError::Invalid(
            "expected version must be non-negative".to_string(),
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

fn load_handoff(conn: &Connection, id: &str) -> Result<Option<TerminalHandoff>, HandoffError> {
    conn.query_row(
        "SELECT * FROM terminal_handoffs WHERE id = ?1",
        params![id],
        TerminalHandoff::from_row,
    )
    .optional()
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

pub fn get_handoff(db: &HcomDb, id: &str) -> Result<Option<TerminalHandoff>, HandoffError> {
    let id = validate_opaque_id(id, "handoff ID")?;
    load_handoff(db.conn(), &id)
}

pub fn get_open_handoff_for_chain(
    db: &HcomDb,
    chain_id: &str,
) -> Result<Option<TerminalHandoff>, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    db.conn()
        .query_row(
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
    let chain_id = generate_id("tc");
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

pub fn pin_native_session(
    db: &HcomDb,
    chain_id: &str,
    actor: &HandoffActor,
    expected_version: i64,
    native_session_id: &str,
) -> Result<GenerationOutcome, HandoffError> {
    let chain_id = validate_opaque_id(chain_id, "chain ID")?;
    validate_actor(actor)?;
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
    let mut chain = load_chain(&tx, &chain_id)?.ok_or(HandoffError::NotManaged)?;
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
    let now = now_epoch_f64();
    if generation.native_session_id.is_some() {
        update_generation_state(
            &tx,
            &mut generation,
            GenerationState::NeedsRecovery,
            actor,
            "supervisor",
            "pin_native_session_conflict",
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
            "supervisor",
            "pin_native_session_conflict",
            &request_hash,
            now,
        )?;
        tx.commit()?;
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT native session is already pinned; chain needs recovery".to_string(),
        ));
    }
    let updated = tx.execute(
        "UPDATE terminal_generations
         SET native_session_id = ?1, version = ?2, updated_at = ?3
         WHERE chain_id = ?4 AND generation = ?5
           AND native_session_id IS NULL AND state = ?6 AND version = ?7",
        params![
            native_session_id,
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
        generation.state.as_str(),
        actor,
        "supervisor",
        "pin_native_session",
        &request_hash,
        now,
    )?;
    generation.native_session_id = Some(native_session_id);
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
    if chain.current_generation != actor.generation
        || chain.supervisor_process_id != supervisor.process_id
        || chain.supervisor_process_birth_identity != supervisor.process_birth_identity
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
        || chain.supervisor_process_id != supervisor.process_id
        || chain.supervisor_process_birth_identity != supervisor.process_birth_identity
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
             stop_observed_at = ?2, updated_at = ?2
         WHERE id = ?3 AND state = 'committed' AND version = ?4",
        params![
            observation.expected_version + 1,
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
        let instance: Option<(Option<String>, String)> = tx
            .query_row(
                "SELECT session_id, tool FROM instances WHERE name = ?1",
                [&handoff.source_instance_name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if instance.as_ref().is_some_and(|(session, tool)| {
            session.as_deref() != Some(handoff.source_hcom_session_id.as_str()) || tool != "codex"
        }) {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT source instance changed before cleanup".to_string(),
            ));
        }
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
    let now = now_epoch_f64();
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
    let mut target =
        load_generation(&tx, &chain.id, handoff.target_generation)?.ok_or(HandoffError::Storage)?;
    let target_generation = handoff.target_generation.to_string();
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
        ],
    );
    let exact_materialization = target.launch_nonce == launch_nonce
        && target.wrapper_process_id.as_deref() == Some(process_id.as_str())
        && target.process_birth_identity.as_deref() == Some(process_birth_identity.as_str())
        && target.instance_name.as_deref() == Some(instance_name.as_str())
        && target.hcom_session_id.as_deref() == Some(hcom_session_id.as_str())
        && target.native_session_id.is_none();
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
    let cleanup_proved = handoff.sigterm_request_result == SigtermRequestResult::Sent.as_str()
        && handoff.child_exit_observed_wall_at.is_some()
        && handoff.waitpid_reaped == Some(true)
        && handoff.inject_cleanup_succeeded == Some(true)
        && handoff.delivery_cleanup_succeeded == Some(true)
        && handoff.pty_cleanup_succeeded == Some(true)
        && handoff.screen_cleanup_succeeded == Some(true)
        && handoff.write_queue_cleanup_succeeded == Some(true)
        && handoff.cleanup_completed_at.is_some();
    if handoff.state != HandoffState::LaunchingTarget
        || handoff.version != materialization.expected_version
        || chain.state != ChainState::LaunchingTarget
        || chain.current_generation != handoff.target_generation
        || target.state != GenerationState::Launching
        || target.launch_nonce != launch_nonce
        || target.wrapper_process_id.is_some()
        || target.process_birth_identity.is_some()
        || target.instance_name.is_some()
        || target.hcom_session_id.is_some()
        || target.native_session_id.is_some()
        || !cleanup_proved
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
                "generation": handoff.target_generation,
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
    let mut target =
        load_generation(&tx, &chain.id, handoff.target_generation)?.ok_or(HandoffError::Storage)?;
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
        || chain.current_generation != handoff.target_generation
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
    let generation = load_generation(tx, &handoff.chain_id, handoff.target_generation)?
        .ok_or(HandoffError::Storage)?;
    authorize_generation(tx, &chain, &generation, actor, true, require_live, true)?;
    if handoff.target_generation != actor.generation {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT caller does not match the immutable target identity".to_string(),
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
    let mut target = load_generation(&tx, &handoff.chain_id, handoff.target_generation)?
        .ok_or(HandoffError::Storage)?;
    if actor.generation != handoff.target_generation
        || chain.current_generation != handoff.target_generation
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
        || target.native_session_id.is_some()
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
         SET native_session_id = ?1, state = 'awaiting_acceptance',
             version = ?2, updated_at = ?3
         WHERE chain_id = ?4 AND generation = ?5
           AND state = 'launching' AND version = ?6
           AND wrapper_process_id = ?7
           AND process_birth_identity = ?8
           AND instance_name = ?9
           AND hcom_session_id = ?10
           AND native_session_id IS NULL
           AND launch_nonce = ?11",
        params![
            native_session,
            target_from_version + 1,
            now,
            target.chain_id,
            target.generation,
            target_from_version,
            actor.process_id,
            actor.process_birth_identity,
            actor.instance_name,
            actor.hcom_session_id,
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
    target.native_session_id = Some(native_session.to_string());
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
    let workspace = snapshot_provider(cwd)?;
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let (mut handoff, mut chain, mut target) = load_target_context(&tx, &handoff_id, actor, true)?;
    if workspace.workspace != chain.workspace
        || workspace.workspace != handoff.workspace
        || workspace.revision != handoff.revision
        || workspace.branch != handoff.branch
        || workspace.dirty_summary != handoff.dirty_summary
    {
        return Err(HandoffError::Conflict(
            "HANDOFF_CONFLICT target workspace does not match the prepared snapshot".to_string(),
        ));
    }
    let bundle = verify_pinned_bundle(&tx, &handoff)?;
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
    tx.commit()?;
    handoff = load_handoff(db.conn(), &handoff_id)?.ok_or(HandoffError::Storage)?;
    Ok(HandoffOutcome {
        handoff,
        replayed: false,
    })
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
        materialize_target_fixture(fixture, &launching, &target_actor())
    }

    fn materialize_target_fixture(
        fixture: &Fixture,
        launching: &HandoffOutcome,
        target: &HandoffActor,
    ) -> HandoffOutcome {
        let generation = load_generation(fixture.db.conn(), &fixture.chain.id, target.generation)
            .unwrap()
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
        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
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
                },
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);
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

        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
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
                ready.handoff.version,
                &fixture.workspace,
            )
            .unwrap()
            .replayed
        );
        assert_eq!(audit_count(&fixture.db), before);
    }

    #[test]
    fn native_session_pin_is_one_way_and_conflict_enters_recovery() {
        let mut fixture = fixture(false);
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

        fixture.source.native_session_id = Some("different-native".to_string());
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
        assert_eq!(generation.state, GenerationState::NeedsRecovery);
        assert_eq!(
            get_chain(&fixture.db, &fixture.chain.id)
                .unwrap()
                .unwrap()
                .state,
            ChainState::NeedsRecovery
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
                ready.handoff.version,
                &fixture.workspace,
            ),
            Err(HandoffError::Conflict(_))
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
            },
            StopObservation {
                expected_version: 1,
                quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
                committed_version: 1,
                hook_native_session_id: "wrong-native".to_string(),
                launch_nonce: "launch-source".to_string(),
            },
            StopObservation {
                expected_version: 1,
                quiesce_token: committed.handoff.quiesce_token.clone().unwrap(),
                committed_version: 0,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "launch-source".to_string(),
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
            Err(HandoffError::Conflict(_))
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
        assert!(matches!(
            accept_handoff(
                &fixture.db,
                &spoof,
                &ready.handoff.id,
                ready.handoff.version,
                &fixture.workspace
            ),
            Err(HandoffError::Conflict(_))
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
        };
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
        let expected_version = ready.handoff.version;
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
        let accepted = accept_handoff(
            &fixture.db,
            &target,
            &ready.handoff.id,
            ready.handoff.version,
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
}
