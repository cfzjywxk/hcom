//! Provider-neutral contracts for one task-local worker runtime.
//!
//! The supervisor owns logical sessions, turns, scheduling, and terminal
//! decisions. A runtime owns only the provider process/transport needed to
//! implement those logical operations.

use crate::control_api::WorkerRole;
use crate::worker::profile::{
    CodexApprovalPolicy, CodexInvocationProfile, CodexSandbox, DeveloperInvocationProfile,
    ReviewerInvocationProfile, SessionInvocationProfiles,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

pub const CODEX_TASK_WORKER_ADAPTER: &str = "codex-exec-0.146.0";
pub const CODEX_TASK_WORKER_VERSION: &str = "0.146.0";
pub const CODEX_EXECUTABLE_SHA256: &str =
    "2e863156ed35ecc5253b1e2f907a9143077b9f7cb51942070c61996471ff6e04";
/// Raw hash captured from the design-time generated stable v2 schema bundle.
///
/// The 0.146 generator emits semantically identical `$defs` in nondeterministic
/// map order, so runtime drift checks use [`CODEX_SCHEMA_CANONICAL_SHA256`].
pub const CODEX_SCHEMA_EXEMPLAR_SHA256: &str =
    "8a1e451c6244f9d954cc2b19aeef2cb33b03fbcd33d21002bd8875e4ead4bd40";
pub const CODEX_SCHEMA_CANONICAL_SHA256: &str =
    "2f402b7d1356adccc1a4785c0656db457578ca9ea5d5b08953487a410c630ce8";

pub const MAX_RUNTIME_KEY_BYTES: usize = 128;
pub const MAX_RUNTIME_PROMPT_BYTES: usize = 256 * 1024;
pub const MAX_RUNTIME_INSTRUCTIONS_BYTES: usize = 64 * 1024;
pub const MAX_RUNTIME_OUTCOME_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_RUNTIME_DIAGNOSTIC_BYTES: usize = 1024;
pub const MAX_OUTCOME_SUMMARY_CHARS: usize = 256 * 1024;
pub const MAX_OUTCOME_QUESTIONS: usize = 8;
pub const MAX_OUTCOME_QUESTION_CHARS: usize = 2048;
pub const MAX_REVIEW_FINDINGS: usize = 32;
pub const MAX_REVIEW_FINDING_PATH_CHARS: usize = 4096;
pub const MAX_REVIEW_FINDING_MESSAGE_CHARS: usize = 256 * 1024;

const DEFAULT_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_REASONING_EFFORT: &str = "xhigh";
const MAX_TASK_KEY_BYTES: usize = 128;
const MAX_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeProvider {
    CodexExec,
}

impl RuntimeProvider {
    pub fn parse(value: &str) -> Result<Self, RuntimeError> {
        match value {
            "codex-exec" => Ok(Self::CodexExec),
            "codex" | "claude" => Err(RuntimeError::unsupported(format!(
                "{value} CLI workers are unsupported in the Codex exec worker lane"
            ))),
            _ => Err(RuntimeError::unsupported(format!(
                "unknown worker runtime provider {value:?}; expected codex-exec"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CodexExec => "codex-exec",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSandbox {
    DangerFullAccess,
}

impl RuntimeSandbox {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeApprovalPolicy {
    Never,
}

impl RuntimeApprovalPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProfile {
    pub provider: RuntimeProvider,
    pub model: String,
    pub reasoning_effort: String,
    pub sandbox: RuntimeSandbox,
    pub approval_policy: RuntimeApprovalPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeThreadProfileFields<'a> {
    pub model: &'a str,
    pub sandbox: &'static str,
    pub approval_policy: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTurnProfileFields<'a> {
    pub model: &'a str,
    pub reasoning_effort: &'a str,
    pub sandbox_policy_type: &'static str,
    pub approval_policy: &'static str,
}

impl RuntimeProfile {
    pub fn codex_exec_default() -> Self {
        Self {
            provider: RuntimeProvider::CodexExec,
            model: DEFAULT_MODEL.into(),
            reasoning_effort: DEFAULT_REASONING_EFFORT.into(),
            sandbox: RuntimeSandbox::DangerFullAccess,
            approval_policy: RuntimeApprovalPolicy::Never,
        }
    }

    pub fn from_codex(label: &str, profile: &CodexInvocationProfile) -> Result<Self, RuntimeError> {
        profile
            .validate(label)
            .map_err(|error| RuntimeError::invalid_profile(error.to_string()))?;
        if profile.sandbox != CodexSandbox::DangerFullAccess {
            return Err(RuntimeError::invalid_profile(format!(
                "{label} must use danger-full-access in the Codex exec worker lane"
            )));
        }
        if profile.approval_policy != CodexApprovalPolicy::Never {
            return Err(RuntimeError::invalid_profile(format!(
                "{label} must use ask_for_approval=never because task runtimes have no human approval channel"
            )));
        }
        Ok(Self {
            provider: RuntimeProvider::CodexExec,
            model: profile.model.clone(),
            reasoning_effort: profile.reasoning_effort.clone(),
            sandbox: RuntimeSandbox::DangerFullAccess,
            approval_policy: RuntimeApprovalPolicy::Never,
        })
    }

    pub fn validate(&self, label: &str) -> Result<(), RuntimeError> {
        validate_model(label, &self.model)?;
        if !matches!(
            self.reasoning_effort.as_str(),
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        ) {
            return Err(RuntimeError::invalid_profile(format!(
                "{label} reasoning_effort must be one of none, minimal, low, medium, high, xhigh, or max"
            )));
        }
        Ok(())
    }

    /// The native CLI options whose semantics the exec invocation must
    /// preserve. This is diagnostic/test evidence, never a process argv.
    pub fn cli_equivalent_arguments(&self) -> Vec<String> {
        vec![
            "--sandbox".into(),
            self.sandbox.as_str().into(),
            "--ask-for-approval".into(),
            self.approval_policy.as_str().into(),
            "--model".into(),
            self.model.clone(),
            "--config".into(),
            format!("model_reasoning_effort=\"{}\"", self.reasoning_effort),
        ]
    }

    pub fn canonical_hash(&self) -> String {
        canonical_hash(&("hcom-runtime-profile-v1", self))
    }

    pub fn thread_fields(&self) -> RuntimeThreadProfileFields<'_> {
        RuntimeThreadProfileFields {
            model: &self.model,
            sandbox: self.sandbox.as_str(),
            approval_policy: self.approval_policy.as_str(),
        }
    }

    pub fn turn_fields(&self) -> RuntimeTurnProfileFields<'_> {
        RuntimeTurnProfileFields {
            model: &self.model,
            reasoning_effort: &self.reasoning_effort,
            sandbox_policy_type: match self.sandbox {
                RuntimeSandbox::DangerFullAccess => "dangerFullAccess",
            },
            approval_policy: self.approval_policy.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskWorkerProfiles {
    pub developer: RuntimeProfile,
    pub reviewer: RuntimeProfile,
}

impl TaskWorkerProfiles {
    pub fn defaults() -> Self {
        Self {
            developer: RuntimeProfile::codex_exec_default(),
            reviewer: RuntimeProfile::codex_exec_default(),
        }
    }

    pub fn from_session_profiles(
        profiles: &SessionInvocationProfiles,
    ) -> Result<Self, RuntimeError> {
        let developer = match &profiles.developer {
            DeveloperInvocationProfile::Codex { profile } => {
                RuntimeProfile::from_codex("Codex developer", profile)?
            }
            DeveloperInvocationProfile::Claude { .. } => {
                return Err(RuntimeError::unsupported(
                    "Claude developer is unsupported in the Codex exec worker runtime lane",
                ));
            }
        };
        let reviewer = match &profiles.reviewer {
            ReviewerInvocationProfile::Codex { profile } => {
                RuntimeProfile::from_codex("Codex reviewer", profile)?
            }
            ReviewerInvocationProfile::Claude { .. } => {
                return Err(RuntimeError::unsupported(
                    "Claude reviewer is unsupported in the Codex exec worker runtime lane",
                ));
            }
        };
        Ok(Self {
            developer,
            reviewer,
        })
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        self.developer.validate("Codex developer")?;
        self.reviewer.validate("Codex reviewer")
    }

    pub fn canonical_hash(&self) -> String {
        canonical_hash(&("hcom-exec-worker-profiles-v1", self))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeContractIdentity {
    pub adapter: String,
    pub cli_version: String,
    pub executable_sha256: String,
    pub schema_canonical_sha256: String,
    pub selected_methods: Vec<String>,
    pub selected_fields: Vec<String>,
}

impl RuntimeContractIdentity {
    pub fn codex_exec_0_146() -> Self {
        Self {
            adapter: CODEX_TASK_WORKER_ADAPTER.into(),
            cli_version: CODEX_TASK_WORKER_VERSION.into(),
            executable_sha256: CODEX_EXECUTABLE_SHA256.into(),
            schema_canonical_sha256: CODEX_SCHEMA_CANONICAL_SHA256.into(),
            selected_methods: [
                "initialize",
                "initialized",
                "thread/start",
                "turn/start",
                "turn/interrupt",
                "turn/completed",
                "item/completed",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            selected_fields: [
                "initialize.params.clientInfo.name",
                "initialize.params.clientInfo.title",
                "initialize.params.clientInfo.version",
                "initialize.params.capabilities.optOutNotificationMethods",
                "thread/start.params.cwd",
                "thread/start.params.model",
                "thread/start.params.approvalPolicy",
                "thread/start.params.sandbox",
                "thread/start.params.ephemeral",
                "thread/start.params.developerInstructions",
                "thread/start.params.config.mcp_servers",
                "thread/start.result.thread.id",
                "turn/start.params.threadId",
                "turn/start.params.input",
                "turn/start.params.cwd",
                "turn/start.params.model",
                "turn/start.params.effort",
                "turn/start.params.approvalPolicy",
                "turn/start.params.sandboxPolicy.type",
                "turn/start.params.outputSchema",
                "turn/start.result.turn.id",
                "item/completed.params.threadId",
                "item/completed.params.turnId",
                "item/completed.params.item.id",
                "item/completed.params.item.type",
                "item/completed.params.item.text",
                "item/completed.params.item.phase",
                "turn/completed.params.threadId",
                "turn/completed.params.turn.id",
                "turn/completed.params.turn.status",
                "turn/completed.params.turn.itemsView",
                "turn/completed.params.turn.items",
                "turn/interrupt.params.threadId",
                "turn/interrupt.params.turnId",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_single_line("runtime adapter", &self.adapter, 128, false)?;
        validate_single_line("runtime CLI version", &self.cli_version, 64, false)?;
        validate_sha256("runtime executable SHA-256", &self.executable_sha256)?;
        validate_sha256(
            "runtime schema canonical SHA-256",
            &self.schema_canonical_sha256,
        )?;
        if self.selected_methods.is_empty() || self.selected_methods.len() > 32 {
            return Err(RuntimeError::invalid_contract(
                "runtime selected method inventory must contain 1-32 entries",
            ));
        }
        let mut unique = BTreeSet::new();
        for method in &self.selected_methods {
            validate_single_line("runtime method", method, 128, false)?;
            if !unique.insert(method) {
                return Err(RuntimeError::invalid_contract(
                    "runtime selected method inventory must be unique",
                ));
            }
        }
        if self.selected_fields.is_empty() || self.selected_fields.len() > 128 {
            return Err(RuntimeError::invalid_contract(
                "runtime selected field inventory must contain 1-128 entries",
            ));
        }
        unique.clear();
        for field in &self.selected_fields {
            validate_single_line("runtime field", field, 256, false)?;
            if !unique.insert(field) {
                return Err(RuntimeError::invalid_contract(
                    "runtime selected field inventory must be unique",
                ));
            }
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> String {
        canonical_hash(&("hcom-task-worker-runtime-contract-v1", self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeSessionKey(u64);

impl RuntimeSessionKey {
    pub(crate) fn from_counter(value: u64) -> Result<Self, RuntimeError> {
        if value == 0 {
            return Err(RuntimeError::invalid_identity(
                "runtime session key counter must be positive",
            ));
        }
        Ok(Self(value))
    }

    pub fn counter(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeTurnKey(u64);

impl RuntimeTurnKey {
    pub(crate) fn from_counter(value: u64) -> Result<Self, RuntimeError> {
        if value == 0 {
            return Err(RuntimeError::invalid_identity(
                "runtime turn key counter must be positive",
            ));
        }
        Ok(Self(value))
    }

    pub fn counter(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTurnPurpose {
    InitialDevelopment,
    DeveloperCorrection,
    DeveloperCompletionRecovery,
    InitialReview,
    ReviewerRereview,
}

impl RuntimeTurnPurpose {
    pub fn role(self) -> WorkerRole {
        match self {
            Self::InitialDevelopment
            | Self::DeveloperCorrection
            | Self::DeveloperCompletionRecovery => WorkerRole::Developer,
            Self::InitialReview | Self::ReviewerRereview => WorkerRole::Reviewer,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoleSessionSpec {
    pub role: WorkerRole,
    pub task_key: String,
    /// The Architect's project directory: the worker's native working root.
    /// It need not be a Git repository.
    pub cwd: PathBuf,
    /// The task's repository, exposed as an extra writable scope when it is
    /// not the project directory itself.
    pub task_repository: PathBuf,
    pub profile: RuntimeProfile,
    pub developer_instructions: String,
}

impl RoleSessionSpec {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_task_key(&self.task_key)?;
        validate_absolute_path("role session cwd", &self.cwd)?;
        validate_absolute_path("role session task repository", &self.task_repository)?;
        self.profile.validate("role runtime profile")?;
        validate_single_line_or_multiline(
            "role developer instructions",
            &self.developer_instructions,
            MAX_RUNTIME_INSTRUCTIONS_BYTES,
            false,
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeContract {
    DeveloperV1,
    ReviewerV1,
}

impl OutcomeContract {
    pub fn role(self) -> WorkerRole {
        match self {
            Self::DeveloperV1 => WorkerRole::Developer,
            Self::ReviewerV1 => WorkerRole::Reviewer,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::DeveloperV1 => "hcom-developer-outcome-v1",
            Self::ReviewerV1 => "hcom-reviewer-outcome-v1",
        }
    }


    pub fn parse(self, bytes: &[u8]) -> Result<RuntimeOutcome, RuntimeError> {
        if bytes.is_empty() || bytes.len() > MAX_RUNTIME_OUTCOME_BYTES {
            return Err(RuntimeError::invalid_outcome(format!(
                "{} output must contain 1-{} bytes",
                self.name(),
                MAX_RUNTIME_OUTCOME_BYTES
            )));
        }
        match self {
            Self::DeveloperV1 => {
                let outcome: DeveloperOutcomeV1 = serde_json::from_slice(bytes)
                    .map_err(|_| RuntimeError::invalid_outcome("invalid DeveloperV1 JSON"))?;
                outcome.validate()?;
                Ok(RuntimeOutcome::Developer(outcome))
            }
            Self::ReviewerV1 => {
                let outcome: ReviewerOutcomeV1 = serde_json::from_slice(bytes)
                    .map_err(|_| RuntimeError::invalid_outcome("invalid ReviewerV1 JSON"))?;
                outcome.validate()?;
                Ok(RuntimeOutcome::Reviewer(outcome))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTurnSpec {
    pub role: WorkerRole,
    pub task_key: String,
    pub purpose: RuntimeTurnPurpose,
    pub cwd: PathBuf,
    pub task_repository: PathBuf,
    pub prompt: String,
    pub profile: RuntimeProfile,
    pub outcome_contract: OutcomeContract,
    pub timeout: Duration,
}

impl RuntimeTurnSpec {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.role != self.purpose.role() || self.role != self.outcome_contract.role() {
            return Err(RuntimeError::invalid_contract(
                "runtime turn role, purpose, and outcome contract must agree",
            ));
        }
        validate_task_key(&self.task_key)?;
        validate_absolute_path("runtime turn cwd", &self.cwd)?;
        validate_single_line_or_multiline(
            "runtime turn prompt",
            &self.prompt,
            MAX_RUNTIME_PROMPT_BYTES,
            false,
        )?;
        self.profile.validate("runtime turn profile")?;
        if self.timeout.is_zero() {
            return Err(RuntimeError::invalid_contract(
                "runtime turn timeout must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperOutcomeStatus {
    Ready,
    NeedsHuman,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeveloperOutcomeV1 {
    pub status: DeveloperOutcomeStatus,
    pub summary: String,
    pub questions: Vec<String>,
}

impl DeveloperOutcomeV1 {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_chars(
            "developer outcome summary",
            &self.summary,
            MAX_OUTCOME_SUMMARY_CHARS,
            false,
        )?;
        if self.questions.len() > MAX_OUTCOME_QUESTIONS {
            return Err(RuntimeError::invalid_outcome(
                "developer outcome has too many questions",
            ));
        }
        let mut unique = BTreeSet::new();
        for question in &self.questions {
            validate_chars(
                "developer outcome question",
                question,
                MAX_OUTCOME_QUESTION_CHARS,
                false,
            )?;
            if !unique.insert(question) {
                return Err(RuntimeError::invalid_outcome(
                    "developer outcome questions must be unique",
                ));
            }
        }
        match self.status {
            DeveloperOutcomeStatus::Ready if !self.questions.is_empty() => Err(
                RuntimeError::invalid_outcome("ready developer outcome must not contain questions"),
            ),
            DeveloperOutcomeStatus::NeedsHuman if self.questions.is_empty() => {
                Err(RuntimeError::invalid_outcome(
                    "needs_human developer outcome must contain a concrete question",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerVerdict {
    Lgtm,
    RequestChanges,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReviewFindingSeverity {
    Major,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ReviewFindingV1 {
    pub severity: ReviewFindingSeverity,
    pub path: Option<String>,
    pub line: Option<u32>,
    pub message: String,
}

impl ReviewFindingV1 {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if let Some(path) = &self.path {
            validate_chars(
                "review finding path",
                path,
                MAX_REVIEW_FINDING_PATH_CHARS,
                false,
            )?;
            validate_repository_relative_path(path)?;
        }
        if self.line == Some(0) {
            return Err(RuntimeError::invalid_outcome(
                "review finding line must be positive",
            ));
        }
        validate_chars(
            "review finding message",
            &self.message,
            MAX_REVIEW_FINDING_MESSAGE_CHARS,
            false,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReviewerOutcomeV1 {
    pub verdict: ReviewerVerdict,
    pub summary: String,
    pub findings: Vec<ReviewFindingV1>,
}

impl ReviewerOutcomeV1 {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_chars(
            "reviewer outcome summary",
            &self.summary,
            MAX_OUTCOME_SUMMARY_CHARS,
            false,
        )?;
        if self.findings.len() > MAX_REVIEW_FINDINGS {
            return Err(RuntimeError::invalid_outcome(
                "reviewer outcome has too many findings",
            ));
        }
        let mut unique = BTreeSet::new();
        for finding in &self.findings {
            finding.validate()?;
            if !unique.insert(finding) {
                return Err(RuntimeError::invalid_outcome(
                    "reviewer outcome findings must be unique",
                ));
            }
        }
        match self.verdict {
            ReviewerVerdict::Lgtm if !self.findings.is_empty() => Err(
                RuntimeError::invalid_outcome("lgtm reviewer outcome must not contain findings"),
            ),
            ReviewerVerdict::RequestChanges if self.findings.is_empty() => {
                Err(RuntimeError::invalid_outcome(
                    "request_changes reviewer outcome must contain a Major finding",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "role", content = "outcome", rename_all = "snake_case")]
pub enum RuntimeOutcome {
    Developer(DeveloperOutcomeV1),
    Reviewer(ReviewerOutcomeV1),
}

impl RuntimeOutcome {
    pub fn role(&self) -> WorkerRole {
        match self {
            Self::Developer(_) => WorkerRole::Developer,
            Self::Reviewer(_) => WorkerRole::Reviewer,
        }
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Developer(outcome) => outcome.validate(),
            Self::Reviewer(outcome) => outcome.validate(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFailureClass {
    Protocol,
    Process,
    Timeout,
    Canceled,
    Contract,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SanitizedRuntimeFailure {
    pub class: RuntimeFailureClass,
    pub detail: String,
    pub retryable: bool,
}

impl SanitizedRuntimeFailure {
    pub fn new(
        class: RuntimeFailureClass,
        detail: impl Into<String>,
        retryable: bool,
    ) -> Result<Self, RuntimeError> {
        let detail = detail.into();
        validate_single_line(
            "runtime failure detail",
            &detail,
            MAX_RUNTIME_DIAGNOSTIC_BYTES,
            false,
        )?;
        Ok(Self {
            class,
            detail,
            retryable,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTelemetry {
    pub protocol_bytes: u64,
    pub stderr_bytes: u64,
    pub notification_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeTurnPoll {
    Pending {
        telemetry: RuntimeTelemetry,
    },
    Completed {
        outcome: RuntimeOutcome,
        telemetry: RuntimeTelemetry,
    },
    Failed {
        failure: SanitizedRuntimeFailure,
        telemetry: RuntimeTelemetry,
    },
}

impl RuntimeTurnPoll {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Pending { .. })
    }
}

pub trait TaskWorkerRuntime: Send {
    fn contract(&self) -> &RuntimeContractIdentity;

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError>;

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError>;

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError>;

    fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError>;

    fn shutdown(&mut self) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeErrorCode {
    Unsupported,
    InvalidProfile,
    InvalidContract,
    InvalidIdentity,
    InvalidOutcome,
    InvalidTransition,
    Internal,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{code:?}: {detail}")]
pub struct RuntimeError {
    pub code: RuntimeErrorCode,
    pub detail: String,
}

impl RuntimeError {
    pub fn unsupported(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::Unsupported, detail)
    }

    pub fn invalid_profile(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::InvalidProfile, detail)
    }

    pub fn invalid_contract(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::InvalidContract, detail)
    }

    pub fn invalid_identity(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::InvalidIdentity, detail)
    }

    pub fn invalid_outcome(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::InvalidOutcome, detail)
    }

    pub fn invalid_transition(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::InvalidTransition, detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::bounded(RuntimeErrorCode::Internal, detail)
    }

    fn bounded(code: RuntimeErrorCode, detail: impl Into<String>) -> Self {
        let mut detail = detail.into().replace(['\r', '\n'], " ");
        if detail.len() > MAX_RUNTIME_DIAGNOSTIC_BYTES {
            let mut boundary = MAX_RUNTIME_DIAGNOSTIC_BYTES;
            while !detail.is_char_boundary(boundary) {
                boundary -= 1;
            }
            detail.truncate(boundary);
        }
        Self { code, detail }
    }
}

fn validate_model(label: &str, value: &str) -> Result<(), RuntimeError> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('-')
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':' | b'@')
        })
    {
        return Err(RuntimeError::invalid_profile(format!(
            "{label} model must be a safe 1-128 byte ASCII model name"
        )));
    }
    Ok(())
}

fn validate_task_key(value: &str) -> Result<(), RuntimeError> {
    validate_single_line("runtime task key", value, MAX_TASK_KEY_BYTES, false)?;
    if !matches!(
        Path::new(value).components().collect::<Vec<_>>().as_slice(),
        [Component::Normal(_)]
    ) {
        return Err(RuntimeError::invalid_contract(
            "runtime task key must be one normal path component",
        ));
    }
    Ok(())
}

fn validate_absolute_path(label: &str, value: &Path) -> Result<(), RuntimeError> {
    let text = value
        .to_str()
        .ok_or_else(|| RuntimeError::invalid_contract(format!("{label} must be UTF-8")))?;
    if text.len() > MAX_PATH_BYTES || !value.is_absolute() {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} must be an absolute path no longer than {MAX_PATH_BYTES} bytes"
        )));
    }
    if value
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} must be lexically normalized"
        )));
    }
    Ok(())
}

fn validate_repository_relative_path(value: &str) -> Result<(), RuntimeError> {
    let path = Path::new(value);
    if path.is_absolute()
        || value.ends_with('/')
        || value.contains('\\')
        || value.contains('\0')
        || path.components().next().is_none()
        || path.components().any(|component| {
            matches!(
                component,
                Component::RootDir
                    | Component::Prefix(_)
                    | Component::ParentDir
                    | Component::CurDir
            )
        })
    {
        return Err(RuntimeError::invalid_outcome(
            "review finding path must be a normalized repository-relative path",
        ));
    }
    Ok(())
}

fn validate_single_line(
    label: &str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), RuntimeError> {
    if (!allow_empty && value.is_empty())
        || value.len() > max_bytes
        || value.contains(['\r', '\n', '\0'])
    {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} must be {}single-line UTF-8 no longer than {max_bytes} bytes",
            if allow_empty { "" } else { "non-empty " }
        )));
    }
    Ok(())
}

fn validate_single_line_or_multiline(
    label: &str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), RuntimeError> {
    if (!allow_empty && value.is_empty()) || value.len() > max_bytes || value.contains('\0') {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} must be {}UTF-8 no longer than {max_bytes} bytes",
            if allow_empty { "" } else { "non-empty " }
        )));
    }
    Ok(())
}

fn validate_chars(
    label: &str,
    value: &str,
    max_chars: usize,
    allow_empty: bool,
) -> Result<(), RuntimeError> {
    if (!allow_empty && value.is_empty())
        || value.chars().count() > max_chars
        || value.contains('\0')
    {
        return Err(RuntimeError::invalid_outcome(format!(
            "{label} must be {}UTF-8 no longer than {max_chars} characters",
            if allow_empty { "" } else { "non-empty " }
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<(), RuntimeError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn canonical_hash(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("closed runtime contracts are serializable");
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::profile::{
        ClaudeInvocationProfile, DeveloperInvocationProfile, ReviewerInvocationProfile,
    };

    #[test]
    fn exact_defaults_match_the_approved_cli_semantics_for_both_roles() {
        let profiles = TaskWorkerProfiles::defaults();
        profiles.validate().unwrap();
        assert_eq!(profiles.developer, profiles.reviewer);
        for profile in [&profiles.developer, &profiles.reviewer] {
            assert_eq!(profile.provider, RuntimeProvider::CodexExec);
            assert_eq!(profile.model, "gpt-5.6-sol");
            assert_eq!(profile.reasoning_effort, "xhigh");
            assert_eq!(profile.sandbox, RuntimeSandbox::DangerFullAccess);
            assert_eq!(profile.approval_policy, RuntimeApprovalPolicy::Never);
            assert_eq!(
                profile.cli_equivalent_arguments(),
                [
                    "--sandbox",
                    "danger-full-access",
                    "--ask-for-approval",
                    "never",
                    "--model",
                    "gpt-5.6-sol",
                    "--config",
                    "model_reasoning_effort=\"xhigh\"",
                ]
            );
            assert_eq!(
                profile.thread_fields(),
                RuntimeThreadProfileFields {
                    model: "gpt-5.6-sol",
                    sandbox: "danger-full-access",
                    approval_policy: "never",
                }
            );
            assert_eq!(
                profile.turn_fields(),
                RuntimeTurnProfileFields {
                    model: "gpt-5.6-sol",
                    reasoning_effort: "xhigh",
                    sandbox_policy_type: "dangerFullAccess",
                    approval_policy: "never",
                }
            );
        }
    }

    #[test]
    fn explicit_codex_overrides_are_preserved_and_invalid_combinations_are_rejected() {
        let mut profiles = SessionInvocationProfiles {
            developer: DeveloperInvocationProfile::Codex {
                profile: CodexInvocationProfile {
                    model: "developer-override".into(),
                    reasoning_effort: "max".into(),
                    sandbox: CodexSandbox::DangerFullAccess,
                    approval_policy: CodexApprovalPolicy::Never,
                },
            },
            reviewer: ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile {
                    model: "reviewer-override".into(),
                    reasoning_effort: "high".into(),
                    sandbox: CodexSandbox::DangerFullAccess,
                    approval_policy: CodexApprovalPolicy::Never,
                },
            },
            ..SessionInvocationProfiles::default()
        };
        let resolved = TaskWorkerProfiles::from_session_profiles(&profiles).unwrap();
        assert_eq!(resolved.developer.model, "developer-override");
        assert_eq!(resolved.developer.reasoning_effort, "max");
        assert_eq!(resolved.reviewer.model, "reviewer-override");
        assert_eq!(resolved.reviewer.reasoning_effort, "high");

        profiles.developer = DeveloperInvocationProfile::Claude {
            profile: ClaudeInvocationProfile::developer_default(),
        };
        let error = TaskWorkerProfiles::from_session_profiles(&profiles).unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::Unsupported);
        assert_eq!(
            error.detail,
            "Claude developer is unsupported in the Codex exec worker runtime lane"
        );

        for invalid in [
            CodexInvocationProfile {
                model: "unsafe model".into(),
                ..CodexInvocationProfile::reviewer_default()
            },
            CodexInvocationProfile {
                reasoning_effort: "unknown".into(),
                ..CodexInvocationProfile::reviewer_default()
            },
            CodexInvocationProfile {
                sandbox: CodexSandbox::WorkspaceWrite,
                ..CodexInvocationProfile::reviewer_default()
            },
            CodexInvocationProfile {
                approval_policy: CodexApprovalPolicy::OnRequest,
                ..CodexInvocationProfile::reviewer_default()
            },
        ] {
            let error = RuntimeProfile::from_codex("Codex reviewer", &invalid).unwrap_err();
            assert_eq!(error.code, RuntimeErrorCode::InvalidProfile);
        }
    }

    #[test]
    fn provider_parse_never_silently_falls_back() {
        assert_eq!(
            RuntimeProvider::parse("codex-exec").unwrap(),
            RuntimeProvider::CodexExec
        );
        for unsupported in ["codex", "claude", "future"] {
            assert_eq!(
                RuntimeProvider::parse(unsupported).unwrap_err().code,
                RuntimeErrorCode::Unsupported
            );
        }
    }

    #[test]
    fn contract_identity_and_outcome_schema_hashes_are_stable() {
        let identity = RuntimeContractIdentity::codex_exec_0_146();
        identity.validate().unwrap();
        assert_eq!(
            identity.schema_canonical_sha256,
            CODEX_SCHEMA_CANONICAL_SHA256
        );
        assert_eq!(
            identity.selected_fields,
            [
                "initialize.params.clientInfo.name",
                "initialize.params.clientInfo.title",
                "initialize.params.clientInfo.version",
                "initialize.params.capabilities.optOutNotificationMethods",
                "thread/start.params.cwd",
                "thread/start.params.model",
                "thread/start.params.approvalPolicy",
                "thread/start.params.sandbox",
                "thread/start.params.ephemeral",
                "thread/start.params.developerInstructions",
                "thread/start.params.config.mcp_servers",
                "thread/start.result.thread.id",
                "turn/start.params.threadId",
                "turn/start.params.input",
                "turn/start.params.cwd",
                "turn/start.params.model",
                "turn/start.params.effort",
                "turn/start.params.approvalPolicy",
                "turn/start.params.sandboxPolicy.type",
                "turn/start.params.outputSchema",
                "turn/start.result.turn.id",
                "item/completed.params.threadId",
                "item/completed.params.turnId",
                "item/completed.params.item.id",
                "item/completed.params.item.type",
                "item/completed.params.item.text",
                "item/completed.params.item.phase",
                "turn/completed.params.threadId",
                "turn/completed.params.turn.id",
                "turn/completed.params.turn.status",
                "turn/completed.params.turn.itemsView",
                "turn/completed.params.turn.items",
                "turn/interrupt.params.threadId",
                "turn/interrupt.params.turnId",
            ]
        );
        assert_eq!(identity.canonical_hash().len(), 64);
        assert!(serde_json::from_str::<OutcomeContract>(r#""developer_v2""#).is_err());
    }

    #[test]
    fn outcome_contracts_are_closed_and_enforce_cross_field_rules() {
        let ready = br#"{"status":"ready","summary":"done","questions":[]}"#;
        assert!(matches!(
            OutcomeContract::DeveloperV1.parse(ready).unwrap(),
            RuntimeOutcome::Developer(_)
        ));
        for invalid in [
            br#"{"status":"ready","summary":"done","questions":["why?"]}"#.as_slice(),
            br#"{"status":"needs_human","summary":"blocked","questions":[]}"#.as_slice(),
            br#"{"status":"ready","summary":"done","questions":[],"extra":true}"#.as_slice(),
            br#"{"status":"unknown","summary":"done","questions":[]}"#.as_slice(),
        ] {
            assert!(OutcomeContract::DeveloperV1.parse(invalid).is_err());
        }

        let lgtm = br#"{"verdict":"lgtm","summary":"sound","findings":[]}"#;
        assert!(matches!(
            OutcomeContract::ReviewerV1.parse(lgtm).unwrap(),
            RuntimeOutcome::Reviewer(_)
        ));
        for invalid in [
            br#"{"verdict":"lgtm","summary":"bad","findings":[{"severity":"major","message":"x"}]}"#.as_slice(),
            br#"{"verdict":"request_changes","summary":"bad","findings":[]}"#.as_slice(),
            br#"{"verdict":"request_changes","summary":"bad","findings":[{"severity":"major","path":"../escape","message":"x"}]}"#.as_slice(),
        ] {
            assert!(OutcomeContract::ReviewerV1.parse(invalid).is_err());
        }
    }

    #[test]
    fn outcome_bounds_cover_below_equal_and_above_every_collection_limit() {
        for length in [
            MAX_OUTCOME_SUMMARY_CHARS - 1,
            MAX_OUTCOME_SUMMARY_CHARS,
            MAX_OUTCOME_SUMMARY_CHARS + 1,
        ] {
            let outcome = DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
                summary: "s".repeat(length),
                questions: Vec::new(),
            };
            assert_eq!(
                outcome.validate().is_ok(),
                length <= MAX_OUTCOME_SUMMARY_CHARS
            );
        }
        for length in [
            MAX_OUTCOME_QUESTION_CHARS - 1,
            MAX_OUTCOME_QUESTION_CHARS,
            MAX_OUTCOME_QUESTION_CHARS + 1,
        ] {
            let outcome = DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::NeedsHuman,
                summary: "needs a decision".into(),
                questions: vec!["q".repeat(length)],
            };
            assert_eq!(
                outcome.validate().is_ok(),
                length <= MAX_OUTCOME_QUESTION_CHARS
            );
        }
        for count in [
            MAX_OUTCOME_QUESTIONS - 1,
            MAX_OUTCOME_QUESTIONS,
            MAX_OUTCOME_QUESTIONS + 1,
        ] {
            let outcome = DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::NeedsHuman,
                summary: "needs decisions".into(),
                questions: (0..count)
                    .map(|index| format!("question-{index}"))
                    .collect(),
            };
            assert_eq!(outcome.validate().is_ok(), count <= MAX_OUTCOME_QUESTIONS);
        }

        for count in [
            MAX_REVIEW_FINDINGS - 1,
            MAX_REVIEW_FINDINGS,
            MAX_REVIEW_FINDINGS + 1,
        ] {
            let outcome = ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                summary: "changes required".into(),
                findings: (0..count)
                    .map(|index| ReviewFindingV1 {
                        severity: ReviewFindingSeverity::Major,
                        path: Some(format!("src/file-{index}.rs")),
                        line: Some(1),
                        message: format!("finding-{index}"),
                    })
                    .collect(),
            };
            assert_eq!(outcome.validate().is_ok(), count <= MAX_REVIEW_FINDINGS);
        }
    }

    #[test]
    fn encoded_outcome_bound_and_missing_fields_fail_closed() {
        assert!(
            OutcomeContract::DeveloperV1
                .parse(&vec![b' '; MAX_RUNTIME_OUTCOME_BYTES + 1])
                .is_err()
        );
        for missing in [
            br#"{"summary":"x","questions":[]}"#.as_slice(),
            br#"{"status":"ready","questions":[]}"#.as_slice(),
            br#"{"status":"ready","summary":"x"}"#.as_slice(),
        ] {
            assert!(OutcomeContract::DeveloperV1.parse(missing).is_err());
        }
        for missing in [
            br#"{"summary":"x","findings":[]}"#.as_slice(),
            br#"{"verdict":"lgtm","findings":[]}"#.as_slice(),
            br#"{"verdict":"lgtm","summary":"x"}"#.as_slice(),
        ] {
            assert!(OutcomeContract::ReviewerV1.parse(missing).is_err());
        }
    }

    #[test]
    fn encoded_outcome_accepts_exactly_its_bound_and_rejects_one_byte_more() {
        let mut outcome = DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Blocked,
            summary: String::new(),
            questions: (0..MAX_OUTCOME_QUESTIONS)
                .map(|index| format!("q{index}"))
                .collect(),
        };
        // Character limits bind before the byte bound, so a maximal outcome
        // is accepted; the byte bound is what stops an oversized encoding.
        outcome.summary = "\u{1f980}".repeat(MAX_OUTCOME_SUMMARY_CHARS);
        for (index, question) in outcome.questions.iter_mut().enumerate() {
            // Questions must stay unique.
            *question = format!("{index}{}", "x".repeat(MAX_OUTCOME_QUESTION_CHARS - 1));
        }
        let maximal = serde_json::to_vec(&outcome).unwrap();
        assert!(maximal.len() <= MAX_RUNTIME_OUTCOME_BYTES);
        assert!(
            OutcomeContract::DeveloperV1.parse(&maximal).is_ok(),
            "a maximal in-contract outcome must be accepted"
        );

        let oversized = vec![b'x'; MAX_RUNTIME_OUTCOME_BYTES + 1];
        assert!(OutcomeContract::DeveloperV1.parse(&oversized).is_err());
    }

    #[test]
    fn runtime_error_truncation_is_utf8_safe() {
        let error = RuntimeError::internal("🦀".repeat(MAX_RUNTIME_DIAGNOSTIC_BYTES));
        assert!(error.detail.len() <= MAX_RUNTIME_DIAGNOSTIC_BYTES);
        assert!(error.detail.is_char_boundary(error.detail.len()));
        assert!(error.detail.chars().all(|character| character == '🦀'));
    }

    #[test]
    fn runtime_keys_are_role_neutral_but_type_separated() {
        let session = RuntimeSessionKey::from_counter(7).unwrap();
        let turn = RuntimeTurnKey::from_counter(7).unwrap();
        assert_eq!(session.counter(), turn.counter());
        assert_ne!(
            std::any::TypeId::of::<RuntimeSessionKey>(),
            std::any::TypeId::of::<RuntimeTurnKey>()
        );
        assert!(RuntimeSessionKey::from_counter(0).is_err());
        assert!(RuntimeTurnKey::from_counter(0).is_err());
    }

    #[test]
    fn serde_rejects_unknown_profile_and_outcome_fields() {
        assert!(serde_json::from_str::<RuntimeProfile>(
            r#"{"provider":"codex-exec","model":"gpt-5.6-sol","reasoning_effort":"xhigh","sandbox":"danger-full-access","approval_policy":"never","extra":true}"#
        )
        .is_err());
        assert!(
            serde_json::from_str::<ReviewerOutcomeV1>(
                r#"{"verdict":"lgtm","summary":"ok","findings":[],"extra":true}"#
            )
            .is_err()
        );
    }
}
