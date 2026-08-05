//! Provider-neutral contracts for one task-local worker runtime.
//!
//! The supervisor owns logical sessions, turns, scheduling, and terminal
//! decisions. A runtime owns only the provider process/transport needed to
//! implement those logical operations.

pub use crate::control_api::ReviewerVerdict;
use crate::control_api::WorkerRole;
use crate::worker::profile::{
    ClaudeInvocationProfile, CodexApprovalPolicy, CodexInvocationProfile, CodexSandbox,
    DeveloperInvocationProfile, ReviewerId, ReviewerInvocationProfile, SessionInvocationProfiles,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

pub const CODEX_TASK_WORKER_ADAPTER: &str = "codex-exec";
pub const CLAUDE_TASK_WORKER_ADAPTER: &str = "claude-exec";
pub const ROLE_ROUTER_TASK_WORKER_ADAPTER: &str = "role-router";

pub const MAX_RUNTIME_KEY_BYTES: usize = 128;
pub const MAX_RUNTIME_PROMPT_BYTES: usize = 256 * 1024;
pub const MAX_RUNTIME_INSTRUCTIONS_BYTES: usize = 64 * 1024;
pub const MAX_RUNTIME_OUTCOME_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_RUNTIME_DIAGNOSTIC_BYTES: usize = 1024;

const DEFAULT_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_REASONING_EFFORT: &str = "xhigh";
const MAX_TASK_KEY_BYTES: usize = 128;
const MAX_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeProvider {
    CodexExec,
    ClaudeExec,
}

impl RuntimeProvider {
    pub fn parse(value: &str) -> Result<Self, RuntimeError> {
        match value {
            "codex-exec" => Ok(Self::CodexExec),
            "claude-exec" => Ok(Self::ClaudeExec),
            "codex" | "claude" => Err(RuntimeError::unsupported(format!(
                "{value} is a profile adapter name, not a task runtime provider"
            ))),
            _ => Err(RuntimeError::unsupported(format!(
                "unknown worker runtime provider {value:?}; expected codex-exec or claude-exec"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CodexExec => "codex-exec",
            Self::ClaudeExec => "claude-exec",
        }
    }

    pub fn contract_identity(self) -> RuntimeContractIdentity {
        match self {
            Self::CodexExec => RuntimeContractIdentity::codex_exec(),
            Self::ClaudeExec => RuntimeContractIdentity::claude_exec(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeClaudePermissions {
    pub dangerously_skip_permissions: bool,
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
    /// Claude's native permission flag has no Codex equivalent. Omitting this
    /// field keeps the established Codex profile serialization and hash exact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_permissions: Option<RuntimeClaudePermissions>,
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
            claude_permissions: None,
        }
    }

    pub fn claude_exec_default() -> Self {
        Self::from_claude(
            "default Claude runtime",
            &ClaudeInvocationProfile::reviewer_default(),
        )
        .expect("built-in Claude runtime profile must remain valid")
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
            claude_permissions: None,
        })
    }

    pub fn from_claude(
        label: &str,
        profile: &ClaudeInvocationProfile,
    ) -> Result<Self, RuntimeError> {
        profile
            .validate(label)
            .map_err(|error| RuntimeError::invalid_profile(error.to_string()))?;
        Ok(Self {
            provider: RuntimeProvider::ClaudeExec,
            model: profile.model.clone(),
            reasoning_effort: profile.effort.clone(),
            // These fields preserve the closed RuntimeProfile shape for the
            // existing Codex child. Claude transport reads only its explicit
            // native permission field when CLAUDE-03 supplies that child.
            sandbox: RuntimeSandbox::DangerFullAccess,
            approval_policy: RuntimeApprovalPolicy::Never,
            claude_permissions: Some(RuntimeClaudePermissions {
                dangerously_skip_permissions: profile.dangerously_skip_permissions,
            }),
        })
    }

    pub fn validate(&self, label: &str) -> Result<(), RuntimeError> {
        validate_model(label, &self.model)?;
        match self.provider {
            RuntimeProvider::CodexExec => {
                if self.claude_permissions.is_some() {
                    return Err(RuntimeError::invalid_profile(format!(
                        "{label} Codex profile cannot carry Claude permissions"
                    )));
                }
                if !matches!(
                    self.reasoning_effort.as_str(),
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                ) {
                    return Err(RuntimeError::invalid_profile(format!(
                        "{label} reasoning_effort must be one of none, minimal, low, medium, high, xhigh, or max"
                    )));
                }
            }
            RuntimeProvider::ClaudeExec => {
                if self.claude_permissions.is_none() {
                    return Err(RuntimeError::invalid_profile(format!(
                        "{label} Claude profile must bind its native permission mode"
                    )));
                }
                if !matches!(
                    self.reasoning_effort.as_str(),
                    "low" | "medium" | "high" | "xhigh" | "max"
                ) {
                    return Err(RuntimeError::invalid_profile(format!(
                        "{label} effort must be one of low, medium, high, xhigh, or max"
                    )));
                }
            }
        }
        Ok(())
    }

    /// The native CLI options whose semantics the exec invocation must
    /// preserve. This is diagnostic/test evidence, never a process argv.
    pub fn cli_equivalent_arguments(&self) -> Vec<String> {
        match self.provider {
            RuntimeProvider::CodexExec => vec![
                "--sandbox".into(),
                self.sandbox.as_str().into(),
                "--ask-for-approval".into(),
                self.approval_policy.as_str().into(),
                "--model".into(),
                self.model.clone(),
                "--config".into(),
                format!("model_reasoning_effort=\"{}\"", self.reasoning_effort),
            ],
            RuntimeProvider::ClaudeExec => {
                let mut arguments = vec![
                    "--model".into(),
                    self.model.clone(),
                    "--effort".into(),
                    self.reasoning_effort.clone(),
                ];
                if self
                    .claude_permissions
                    .as_ref()
                    .is_some_and(|permissions| permissions.dangerously_skip_permissions)
                {
                    arguments.push("--dangerously-skip-permissions".into());
                }
                arguments
            }
        }
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
pub struct ReviewerRuntimeProfile {
    pub reviewer_id: ReviewerId,
    pub profile: RuntimeProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskWorkerProfiles {
    pub developer: RuntimeProfile,
    pub reviewers: Vec<ReviewerRuntimeProfile>,
}

impl TaskWorkerProfiles {
    pub fn defaults() -> Self {
        Self {
            developer: RuntimeProfile::codex_exec_default(),
            reviewers: vec![ReviewerRuntimeProfile {
                reviewer_id: ReviewerId::Reviewer1,
                profile: RuntimeProfile::claude_exec_default(),
            }],
        }
    }

    pub fn from_session_profiles(
        profiles: &SessionInvocationProfiles,
    ) -> Result<Self, RuntimeError> {
        let developer = match &profiles.developer {
            DeveloperInvocationProfile::Codex { profile } => {
                RuntimeProfile::from_codex("Codex developer", profile)?
            }
            DeveloperInvocationProfile::Claude { profile } => {
                RuntimeProfile::from_claude("Claude developer", profile)?
            }
        };
        profiles
            .validate()
            .map_err(|error| RuntimeError::invalid_profile(error.to_string()))?;
        let reviewers = profiles
            .reviewers
            .iter()
            .map(|binding| {
                let profile = match &binding.profile {
                    ReviewerInvocationProfile::Codex { profile } => {
                        RuntimeProfile::from_codex("Codex reviewer", profile)?
                    }
                    ReviewerInvocationProfile::Claude { profile } => {
                        RuntimeProfile::from_claude("Claude reviewer", profile)?
                    }
                };
                Ok(ReviewerRuntimeProfile {
                    reviewer_id: binding.reviewer_id,
                    profile,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        Ok(Self {
            developer,
            reviewers,
        })
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        self.developer.validate("developer runtime profile")?;
        let [reviewer] = self.reviewers.as_slice() else {
            return Err(RuntimeError::invalid_profile(
                "runtime reviewer collection must contain exactly one Reviewer1 entry",
            ));
        };
        if reviewer.reviewer_id != ReviewerId::Reviewer1 {
            return Err(RuntimeError::invalid_profile(
                "runtime reviewer collection must contain exactly one Reviewer1 entry",
            ));
        }
        reviewer.profile.validate("Reviewer1 runtime profile")
    }

    pub fn canonical_hash(&self) -> String {
        canonical_hash(&("hcom-exec-worker-profiles-v2", self))
    }

    pub fn reviewer1(&self) -> &RuntimeProfile {
        &self
            .reviewers
            .iter()
            .find(|binding| binding.reviewer_id == ReviewerId::Reviewer1)
            .expect("validated task worker profiles contain Reviewer1")
            .profile
    }

    pub fn profile(&self, role: WorkerRole) -> &RuntimeProfile {
        match role {
            WorkerRole::Developer => &self.developer,
            WorkerRole::Reviewer => self.reviewer1(),
        }
    }

    pub fn provider(&self, role: WorkerRole) -> RuntimeProvider {
        self.profile(role).provider
    }

    pub fn contract_identity(&self) -> RuntimeContractIdentity {
        RuntimeContractIdentity::for_role_providers(
            self.developer.provider,
            self.reviewer1().provider,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeContractIdentity {
    pub adapter: String,
    pub contract_sha256: String,
    pub selected_methods: Vec<String>,
    pub selected_fields: Vec<String>,
}

impl RuntimeContractIdentity {
    pub fn codex_exec() -> Self {
        Self::new(
            CODEX_TASK_WORKER_ADAPTER,
            vec!["exec".into(), "exec resume".into()],
            vec![
                "stdout.thread.started.thread_id".into(),
                "output-last-message".into(),
            ],
        )
    }

    pub fn claude_exec() -> Self {
        Self::new(
            CLAUDE_TASK_WORKER_ADAPTER,
            vec!["-p".into(), "--resume".into()],
            vec![
                "stream-json.system.init.session_id".into(),
                "stream-json.result.session_id".into(),
                "stream-json.result.result".into(),
            ],
        )
    }

    pub fn for_role_providers(developer: RuntimeProvider, reviewer: RuntimeProvider) -> Self {
        let developer_contract = developer.contract_identity();
        let reviewer_contract = reviewer.contract_identity();
        if developer == reviewer {
            return developer_contract;
        }
        Self::new(
            ROLE_ROUTER_TASK_WORKER_ADAPTER,
            vec![
                format!("developer={}", developer.as_str()),
                format!("reviewer={}", reviewer.as_str()),
            ],
            vec![
                format!(
                    "developer.contract_sha256={}",
                    developer_contract.contract_sha256
                ),
                format!(
                    "reviewer.contract_sha256={}",
                    reviewer_contract.contract_sha256
                ),
            ],
        )
    }

    pub fn new(
        adapter: impl Into<String>,
        selected_methods: Vec<String>,
        selected_fields: Vec<String>,
    ) -> Self {
        let adapter = adapter.into();
        let contract_sha256 = canonical_hash(&(
            "hcom-native-runtime-contract-v1",
            &adapter,
            &selected_methods,
            &selected_fields,
        ));
        Self {
            adapter,
            contract_sha256,
            selected_methods,
            selected_fields,
        }
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_single_line("runtime adapter", &self.adapter, 128, false)?;
        validate_sha256("runtime contract SHA-256", &self.contract_sha256)?;
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
    DeveloperClarificationResume,
    InitialReview,
    ReviewerRereview,
}

impl RuntimeTurnPurpose {
    pub fn role(self) -> WorkerRole {
        match self {
            Self::InitialDevelopment
            | Self::DeveloperCorrection
            | Self::DeveloperClarificationResume => WorkerRole::Developer,
            Self::InitialReview | Self::ReviewerRereview => WorkerRole::Reviewer,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InitialDevelopment => "initial_development",
            Self::DeveloperCorrection => "developer_correction",
            Self::DeveloperClarificationResume => "developer_clarification_resume",
            Self::InitialReview => "initial_review",
            Self::ReviewerRereview => "reviewer_rereview",
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
    ClarificationRequired,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeveloperOutcomeV1 {
    pub status: DeveloperOutcomeStatus,
}

impl DeveloperOutcomeV1 {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReviewerOutcomeV1 {
    pub verdict: ReviewerVerdict,
    /// Earlier durable final messages that belong to the same review round.
    /// This is empty for a normal review and contains the original reviewer
    /// final after the single verdict-clarification turn.
    pub preceding_final_message_paths: Vec<PathBuf>,
}

impl ReviewerOutcomeV1 {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.preceding_final_message_paths.len() > 1 {
            return Err(RuntimeError::invalid_outcome(
                "reviewer outcome may carry at most one preceding final message path",
            ));
        }
        for path in &self.preceding_final_message_paths {
            validate_absolute_path("preceding reviewer final message path", path)?;
        }
        Ok(())
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
        final_message_path: PathBuf,
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

    pub fn validate(&self) -> Result<(), RuntimeError> {
        if let Self::Completed {
            outcome,
            final_message_path,
            ..
        } = self
        {
            outcome.validate()?;
            validate_absolute_path("runtime final message path", final_message_path)?;
        }
        Ok(())
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
        ClaudeInvocationProfile, DeveloperInvocationProfile, ReviewerInvocationBinding,
        ReviewerInvocationProfile,
    };

    #[test]
    fn exact_defaults_match_the_approved_mixed_worker_pair() {
        let profiles = TaskWorkerProfiles::defaults();
        profiles.validate().unwrap();
        let developer = &profiles.developer;
        assert_eq!(developer.provider, RuntimeProvider::CodexExec);
        assert_eq!(developer.model, "gpt-5.6-sol");
        assert_eq!(developer.reasoning_effort, "xhigh");
        assert_eq!(developer.sandbox, RuntimeSandbox::DangerFullAccess);
        assert_eq!(developer.approval_policy, RuntimeApprovalPolicy::Never);
        assert_eq!(developer.claude_permissions, None);
        assert_eq!(
            developer.cli_equivalent_arguments(),
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

        let reviewer = profiles.reviewer1();
        assert_eq!(reviewer.provider, RuntimeProvider::ClaudeExec);
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.reasoning_effort, "xhigh");
        assert_eq!(reviewer.sandbox, RuntimeSandbox::DangerFullAccess);
        assert_eq!(reviewer.approval_policy, RuntimeApprovalPolicy::Never);
        assert_eq!(
            reviewer.claude_permissions,
            Some(RuntimeClaudePermissions {
                dangerously_skip_permissions: true,
            })
        );
        assert_eq!(
            reviewer.cli_equivalent_arguments(),
            [
                "--model",
                "opus",
                "--effort",
                "xhigh",
                "--dangerously-skip-permissions",
            ]
        );
        assert_eq!(profiles.canonical_hash().len(), 64);
        assert_eq!(
            profiles.contract_identity().adapter,
            ROLE_ROUTER_TASK_WORKER_ADAPTER
        );
        assert_eq!(profiles.contract_identity().canonical_hash().len(), 64);
    }

    #[test]
    fn reviewer_runtime_collection_is_ordered_identity_bound_and_exactly_reviewer1() {
        let profiles = TaskWorkerProfiles::defaults();
        profiles.validate().unwrap();
        assert_eq!(profiles.reviewers.len(), 1);
        assert_eq!(profiles.reviewers[0].reviewer_id, ReviewerId::Reviewer1);
        assert_eq!(profiles.profile(WorkerRole::Developer), &profiles.developer);
        assert_eq!(profiles.profile(WorkerRole::Reviewer), profiles.reviewer1());

        let mut wrong_identity = profiles.clone();
        wrong_identity.reviewers[0].reviewer_id = ReviewerId::Reviewer2;
        assert!(wrong_identity.validate().is_err());
        assert_ne!(
            profiles.canonical_hash(),
            wrong_identity.canonical_hash(),
            "runtime reviewer lane identity must be hash-bound"
        );

        let mut changed_profile = profiles.clone();
        changed_profile.reviewers[0].profile.reasoning_effort = "medium".into();
        assert_ne!(
            profiles.canonical_hash(),
            changed_profile.canonical_hash(),
            "the complete reviewer runtime profile must be hash-bound"
        );

        let reviewer2 = ReviewerRuntimeProfile {
            reviewer_id: ReviewerId::Reviewer2,
            profile: profiles.reviewer1().clone(),
        };
        let mut reviewer1_then_reviewer2 = profiles.clone();
        reviewer1_then_reviewer2.reviewers.push(reviewer2.clone());
        let mut reviewer2_then_reviewer1 = profiles.clone();
        reviewer2_then_reviewer1.reviewers.insert(0, reviewer2);
        assert!(reviewer1_then_reviewer2.validate().is_err());
        assert!(reviewer2_then_reviewer1.validate().is_err());
        assert_ne!(
            reviewer1_then_reviewer2.canonical_hash(),
            reviewer2_then_reviewer1.canonical_hash(),
            "runtime reviewer order must be hash-bound"
        );
    }

    #[test]
    fn explicit_role_profiles_preserve_each_selected_provider() {
        let mut profiles = SessionInvocationProfiles {
            developer: DeveloperInvocationProfile::Codex {
                profile: CodexInvocationProfile {
                    model: "developer-override".into(),
                    reasoning_effort: "max".into(),
                    sandbox: CodexSandbox::DangerFullAccess,
                    approval_policy: CodexApprovalPolicy::Never,
                },
            },
            reviewers: vec![ReviewerInvocationBinding {
                reviewer_id: ReviewerId::Reviewer1,
                profile: ReviewerInvocationProfile::Codex {
                    profile: CodexInvocationProfile {
                        model: "reviewer-override".into(),
                        reasoning_effort: "high".into(),
                        sandbox: CodexSandbox::DangerFullAccess,
                        approval_policy: CodexApprovalPolicy::Never,
                    },
                },
            }],
            ..SessionInvocationProfiles::default()
        };
        let resolved = TaskWorkerProfiles::from_session_profiles(&profiles).unwrap();
        assert_eq!(resolved.developer.model, "developer-override");
        assert_eq!(resolved.developer.reasoning_effort, "max");
        assert_eq!(resolved.reviewer1().model, "reviewer-override");
        assert_eq!(resolved.reviewer1().reasoning_effort, "high");

        profiles.developer = DeveloperInvocationProfile::Claude {
            profile: ClaudeInvocationProfile::developer_default(),
        };
        let resolved = TaskWorkerProfiles::from_session_profiles(&profiles).unwrap();
        assert_eq!(resolved.developer.provider, RuntimeProvider::ClaudeExec);
        assert_eq!(resolved.developer.model, "opus");
        assert_eq!(resolved.developer.reasoning_effort, "xhigh");
        assert_eq!(
            resolved.developer.claude_permissions,
            Some(RuntimeClaudePermissions {
                dangerously_skip_permissions: true,
            })
        );
        assert_eq!(resolved.reviewer1().provider, RuntimeProvider::CodexExec);

        profiles.developer = DeveloperInvocationProfile::Codex {
            profile: CodexInvocationProfile::developer_default(),
        };

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
        assert_eq!(
            RuntimeProvider::parse("claude-exec").unwrap(),
            RuntimeProvider::ClaudeExec
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
        let identity = RuntimeContractIdentity::codex_exec();
        identity.validate().unwrap();
        assert_eq!(
            identity.contract_sha256,
            canonical_hash(&(
                "hcom-native-runtime-contract-v1",
                CODEX_TASK_WORKER_ADAPTER,
                &identity.selected_methods,
                &identity.selected_fields,
            ))
        );
        assert_eq!(identity.selected_methods, ["exec", "exec resume"]);
        assert_eq!(identity.canonical_hash().len(), 64);
        let claude = RuntimeContractIdentity::claude_exec();
        claude.validate().unwrap();
        assert_eq!(claude.adapter, CLAUDE_TASK_WORKER_ADAPTER);
        assert_eq!(
            RuntimeContractIdentity::for_role_providers(
                RuntimeProvider::CodexExec,
                RuntimeProvider::CodexExec,
            ),
            identity
        );
        assert_eq!(
            RuntimeContractIdentity::for_role_providers(
                RuntimeProvider::ClaudeExec,
                RuntimeProvider::ClaudeExec,
            ),
            claude
        );
        assert_eq!(
            RuntimeContractIdentity::for_role_providers(
                RuntimeProvider::CodexExec,
                RuntimeProvider::ClaudeExec,
            )
            .adapter,
            ROLE_ROUTER_TASK_WORKER_ADAPTER
        );
        assert!(serde_json::from_str::<OutcomeContract>(r#""developer_v2""#).is_err());
    }

    #[test]
    fn outcome_contracts_are_closed_and_carry_no_peer_body() {
        let ready = br#"{"status":"ready"}"#;
        assert!(matches!(
            OutcomeContract::DeveloperV1.parse(ready).unwrap(),
            RuntimeOutcome::Developer(_)
        ));
        for invalid in [
            br#"{"status":"ready","summary":"done"}"#.as_slice(),
            br#"{"status":"needs_human"}"#.as_slice(),
            br#"{"status":"ready","extra":true}"#.as_slice(),
            br#"{"status":"unknown"}"#.as_slice(),
        ] {
            assert!(OutcomeContract::DeveloperV1.parse(invalid).is_err());
        }

        let lgtm = br#"{"verdict":"lgtm","preceding_final_message_paths":[]}"#;
        assert!(matches!(
            OutcomeContract::ReviewerV1.parse(lgtm).unwrap(),
            RuntimeOutcome::Reviewer(_)
        ));
        for invalid in [
            br#"{"verdict":"lgtm","preceding_final_message_paths":[],"summary":"bad"}"#.as_slice(),
            br#"{"verdict":"request_changes","preceding_final_message_paths":["relative.md"]}"#
                .as_slice(),
            br#"{"verdict":"request_changes","preceding_final_message_paths":["/one","/two"]}"#
                .as_slice(),
        ] {
            assert!(OutcomeContract::ReviewerV1.parse(invalid).is_err());
        }
    }

    #[test]
    fn completed_poll_requires_a_normalized_absolute_final_path() {
        let outcome = RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Ready,
        });
        for (path, valid) in [
            ("/durable/native-final.partial", true),
            ("relative/native-final.partial", false),
            ("/durable/../escape", false),
        ] {
            let poll = RuntimeTurnPoll::Completed {
                outcome: outcome.clone(),
                final_message_path: PathBuf::from(path),
                telemetry: RuntimeTelemetry::default(),
            };
            assert_eq!(poll.validate().is_ok(), valid);
        }
    }

    #[test]
    fn encoded_outcome_bound_and_missing_fields_fail_closed() {
        assert!(
            OutcomeContract::DeveloperV1
                .parse(&vec![b' '; MAX_RUNTIME_OUTCOME_BYTES + 1])
                .is_err()
        );
        assert!(OutcomeContract::DeveloperV1.parse(br#"{}"#).is_err());
        for missing in [
            br#"{"preceding_final_message_paths":[]}"#.as_slice(),
            br#"{"verdict":"lgtm"}"#.as_slice(),
        ] {
            assert!(OutcomeContract::ReviewerV1.parse(missing).is_err());
        }
    }

    #[test]
    fn encoded_outcome_rejects_more_than_its_bound() {
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
                r#"{"verdict":"lgtm","preceding_final_message_paths":[],"extra":true}"#
            )
            .is_err()
        );
    }
}
