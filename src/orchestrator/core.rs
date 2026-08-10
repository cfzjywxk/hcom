//! Pure deterministic domain reducer for one foreground Architect session.
//!
//! `SupervisorCore` owns scheduling decisions and accepts only normalized
//! events. It performs no filesystem, Git, process, clock, network, provider,
//! or terminal I/O. A later `SupervisorDriver` executes the ordered effects and
//! feeds observations back as new events.

use crate::control_api::{
    ActiveWorkerSnapshot, ArchitectActionReason, ClarificationPage, ClarificationRecord,
    DeliveryBinding, GitHubCheckSnapshot, GitHubDeliveryOutcome, GitHubDeliveryPhase,
    GitHubDeliveryStatusSnapshot, GitHubFinalizationSnapshot, GitHubInspectionBinding,
    GitHubReviewSnapshot, GitHubRunBinding, GitHubTaskProgressSnapshot,
    MAX_CLARIFICATION_PAGE_RECORDS, MAX_CLARIFICATION_RECORDS_PER_RUN,
    MAX_CLARIFICATION_RECORDS_PER_TASK, MAX_PROGRESS_EVENTS_PER_RUN,
    PendingArchitectActionSnapshot, ReviewerBindingSnapshot, ReviewerResultSnapshot,
    SessionProgressEvent, SessionState, SessionStatusSnapshot, TaskCompletionOutcome, TaskDraft,
    TaskState, TaskStatusSnapshot, WorkerRole,
};
use crate::worker::profile::ReviewerId;
use crate::worker::runtime::{
    DeveloperOutcomeStatus, ReviewerOutcomeV1, ReviewerVerdict, RuntimeFailureClass,
    RuntimeOutcome, RuntimeProfile, RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPurpose,
    SanitizedRuntimeFailure, WorkerLane,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

const MAX_CORE_DIAGNOSTIC_BYTES: usize = 1024;
const MAX_COMPLETION_TOKEN_BYTES: usize = 128;
const MAX_REPOSITORY_PATH_BYTES: usize = 4096;
const MAX_TASKS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SupervisorEventKind {
    PlanBound,
    ExecutionAuthorized,
    RepositoryPrepared,
    CandidatePublished,
    ReviewerReviewPublished,
    ReviewCheckPublished,
    MergeGateObserved,
    PullRequestMerged,
    GitHubRunFinalized,
    GitHubOperationFailed,
    TaskRuntimeOpened,
    RoleSessionOpened,
    TurnStarted,
    TurnCompleted,
    ClarificationSubmitted,
    ClarificationHumanRequired,
    TurnFailed,
    DriverFailed,
    Timeout,
    CancelRequested,
    ParentStopping,
    StatusRequested,
}

impl SupervisorEventKind {
    pub const ALL: [Self; 22] = [
        Self::PlanBound,
        Self::ExecutionAuthorized,
        Self::RepositoryPrepared,
        Self::CandidatePublished,
        Self::ReviewerReviewPublished,
        Self::ReviewCheckPublished,
        Self::MergeGateObserved,
        Self::PullRequestMerged,
        Self::GitHubRunFinalized,
        Self::GitHubOperationFailed,
        Self::TaskRuntimeOpened,
        Self::RoleSessionOpened,
        Self::TurnStarted,
        Self::TurnCompleted,
        Self::ClarificationSubmitted,
        Self::ClarificationHumanRequired,
        Self::TurnFailed,
        Self::DriverFailed,
        Self::Timeout,
        Self::CancelRequested,
        Self::ParentStopping,
        Self::StatusRequested,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHubReviewCheckConclusion {
    Success,
    ActionRequired,
    Cancelled,
}

impl GitHubReviewCheckConclusion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ActionRequired => "action_required",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubTaskOutcomeEvidence {
    pub task_ordinal: usize,
    pub task_key: String,
    pub task_title: String,
    pub task_base_sha: String,
    pub task_final_head_sha: Option<String>,
    pub outcome: Option<TaskCompletionOutcome>,
    pub reviews: Vec<GitHubReviewSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareGitHubRunRequest {
    pub operation_id: String,
    pub run_id: String,
    pub plan_hash: String,
    pub run_binding: GitHubRunBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialRepositoryPreparation {
    pub base_sha: String,
    pub branch: String,
    pub worktree_path: String,
    pub local_base_ref_created: bool,
    pub local_branch_created: bool,
    pub local_worktree_created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishDeveloperCandidateRequest {
    pub operation_id: String,
    pub run_id: String,
    pub plan_hash: String,
    pub task_ordinal: usize,
    pub task_count: usize,
    pub task_key: String,
    pub task_title: String,
    pub generation: u32,
    pub task_base_sha: String,
    pub previous_head_sha: String,
    pub developer_final_path: String,
    pub correction: bool,
    pub existing_pr_number: Option<u64>,
    pub expected_check_run_id: Option<u64>,
    pub task_outcomes: Vec<GitHubTaskOutcomeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialCandidatePublication {
    pub task_ordinal: usize,
    pub generation: u32,
    pub previous_head_sha: String,
    pub head_sha: String,
    pub pr_number: Option<u64>,
    pub pr_node_id: Option<String>,
    pub pr_url: Option<String>,
    pub pr_actor_bot_user_id: Option<u64>,
    pub check_run_id: Option<u64>,
    pub check_url: Option<String>,
    pub check_actor_app_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialReviewerReview {
    pub task_ordinal: usize,
    pub reviewer_id: ReviewerId,
    pub generation: u32,
    pub head_sha: String,
    pub verdict: ReviewerVerdict,
    pub review_id: u64,
    pub review_url: String,
    pub actor_bot_user_id: u64,
    pub final_artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialReviewCheck {
    pub task_ordinal: usize,
    pub generation: u32,
    pub head_sha: String,
    pub check_run_id: u64,
    pub check_url: String,
    pub state: String,
    pub actor_app_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialMerge {
    pub pr_number: u64,
    pub final_head_sha: String,
    pub merge_sha: String,
    pub merge_url: String,
    pub actor_bot_user_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPartialFinalization {
    pub pr_number: u64,
    pub final_head_sha: String,
    pub merge_sha: String,
    pub finalization: GitHubFinalizationSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitHubPartialOperation {
    RepositoryPreparation(GitHubPartialRepositoryPreparation),
    Candidate(GitHubPartialCandidatePublication),
    ReviewerReview(GitHubPartialReviewerReview),
    ReviewCheck(GitHubPartialReviewCheck),
    Merge(GitHubPartialMerge),
    Finalization(GitHubPartialFinalization),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReviewerReviewRequest {
    pub operation_id: String,
    pub run_id: String,
    pub task_ordinal: usize,
    pub task_count: usize,
    pub task_key: String,
    pub task_title: String,
    pub generation: u32,
    pub task_base_sha: String,
    pub head_sha: String,
    pub pr_number: u64,
    pub reviewer_id: ReviewerId,
    pub verdict: ReviewerVerdict,
    pub reviewer_final_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReviewCheckRequest {
    pub operation_id: String,
    pub run_id: String,
    pub task_ordinal: usize,
    pub task_count: usize,
    pub task_key: String,
    pub task_title: String,
    pub generation: u32,
    pub task_base_sha: String,
    pub head_sha: String,
    pub check_run_id: u64,
    pub check_url: String,
    pub ruleset_attestation_sha256: String,
    pub conclusion: GitHubReviewCheckConclusion,
    pub task_outcomes: Vec<GitHubTaskOutcomeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitForMergeGateRequest {
    pub operation_id: String,
    pub run_id: String,
    pub pr_number: u64,
    pub final_head_sha: String,
    pub check_run_id: u64,
    pub ruleset_attestation_sha256: String,
    pub tasks: Vec<GitHubTaskOutcomeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePullRequestRequest {
    pub operation_id: String,
    pub run_id: String,
    pub pr_number: u64,
    pub final_head_sha: String,
    pub base_sha: String,
    pub ruleset_attestation_sha256: String,
    pub check_run_id: u64,
    pub tasks: Vec<GitHubTaskOutcomeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeGitHubRunRequest {
    pub operation_id: String,
    pub run_id: String,
    pub pr_number: u64,
    pub final_head_sha: String,
    pub merge_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishGitHubTerminalRequest {
    pub run_id: String,
    pub pr_number: u64,
    pub head_sha: String,
    pub check_run_id: Option<u64>,
    pub outcome: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEvent {
    PlanBound {
        expected_version: u64,
        plan_version: u64,
        plan_hash: String,
        tasks: Vec<TaskDraft>,
        github_run_binding: Option<GitHubRunBinding>,
    },
    ExecutionAuthorized {
        expected_version: u64,
        plan_version: Option<u64>,
        plan_hash: Option<String>,
    },
    RepositoryPrepared {
        expected_version: u64,
        operation_id: String,
        base_sha: String,
        branch: String,
        worktree_path: String,
    },
    CandidatePublished {
        expected_version: u64,
        operation_id: String,
        task_ordinal: usize,
        generation: u32,
        previous_head_sha: String,
        head_sha: String,
        pr_number: u64,
        pr_node_id: String,
        pr_url: String,
        pr_actor_bot_user_id: u64,
        check_run_id: u64,
        check_url: String,
        check_actor_app_id: u64,
    },
    ReviewerReviewPublished {
        expected_version: u64,
        operation_id: String,
        task_ordinal: usize,
        reviewer_id: ReviewerId,
        generation: u32,
        head_sha: String,
        verdict: ReviewerVerdict,
        review_id: u64,
        review_url: String,
        actor_bot_user_id: u64,
        final_artifact_sha256: String,
    },
    ReviewCheckPublished {
        expected_version: u64,
        operation_id: String,
        task_ordinal: usize,
        generation: u32,
        head_sha: String,
        check_run_id: u64,
        check_url: String,
        state: String,
        actor_app_id: u64,
    },
    MergeGateObserved {
        expected_version: u64,
        operation_id: String,
        pr_number: u64,
        final_head_sha: String,
        base_sha: String,
        ruleset_attestation_sha256: String,
        check_run_id: u64,
    },
    PullRequestMerged {
        expected_version: u64,
        operation_id: String,
        pr_number: u64,
        final_head_sha: String,
        merge_sha: String,
        merge_url: String,
        actor_bot_user_id: u64,
        merge_evidence_durable: bool,
    },
    GitHubRunFinalized {
        expected_version: u64,
        operation_id: String,
        pr_number: u64,
        final_head_sha: String,
        merge_sha: String,
        finalization: GitHubFinalizationSnapshot,
    },
    GitHubOperationFailed {
        expected_version: u64,
        operation_id: String,
        detail: String,
        partial_operation: Option<GitHubPartialOperation>,
    },
    TaskRuntimeOpened {
        expected_version: u64,
        task_ordinal: usize,
    },
    RoleSessionOpened {
        expected_version: u64,
        task_ordinal: usize,
        lane: WorkerLane,
        session: RuntimeSessionKey,
    },
    TurnStarted {
        expected_version: u64,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
    },
    TurnCompleted {
        expected_version: u64,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
        outcome: RuntimeOutcome,
        final_message_path: PathBuf,
    },
    ClarificationSubmitted {
        expected_version: u64,
        task_ordinal: usize,
        task_key: String,
        action_sequence: u32,
        developer_request_path: String,
        clarification_document_path: String,
        human_decision_confirmed: bool,
    },
    ClarificationHumanRequired {
        expected_version: u64,
        task_ordinal: usize,
        task_key: String,
        action_sequence: u32,
        developer_request_path: String,
    },
    TurnFailed {
        expected_version: u64,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
        failure: SanitizedRuntimeFailure,
    },
    DriverFailed {
        expected_version: u64,
        task_ordinal: usize,
        failure: DriverFailure,
    },
    Timeout {
        expected_version: u64,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
    },
    CancelRequested {
        expected_version: u64,
        reason: String,
    },
    ParentStopping {
        expected_version: u64,
    },
    StatusRequested,
}

impl SupervisorEvent {
    pub fn kind(&self) -> SupervisorEventKind {
        match self {
            Self::PlanBound { .. } => SupervisorEventKind::PlanBound,
            Self::ExecutionAuthorized { .. } => SupervisorEventKind::ExecutionAuthorized,
            Self::RepositoryPrepared { .. } => SupervisorEventKind::RepositoryPrepared,
            Self::CandidatePublished { .. } => SupervisorEventKind::CandidatePublished,
            Self::ReviewerReviewPublished { .. } => SupervisorEventKind::ReviewerReviewPublished,
            Self::ReviewCheckPublished { .. } => SupervisorEventKind::ReviewCheckPublished,
            Self::MergeGateObserved { .. } => SupervisorEventKind::MergeGateObserved,
            Self::PullRequestMerged { .. } => SupervisorEventKind::PullRequestMerged,
            Self::GitHubRunFinalized { .. } => SupervisorEventKind::GitHubRunFinalized,
            Self::GitHubOperationFailed { .. } => SupervisorEventKind::GitHubOperationFailed,
            Self::TaskRuntimeOpened { .. } => SupervisorEventKind::TaskRuntimeOpened,
            Self::RoleSessionOpened { .. } => SupervisorEventKind::RoleSessionOpened,
            Self::TurnStarted { .. } => SupervisorEventKind::TurnStarted,
            Self::TurnCompleted { .. } => SupervisorEventKind::TurnCompleted,
            Self::ClarificationSubmitted { .. } => SupervisorEventKind::ClarificationSubmitted,
            Self::ClarificationHumanRequired { .. } => {
                SupervisorEventKind::ClarificationHumanRequired
            }
            Self::TurnFailed { .. } => SupervisorEventKind::TurnFailed,
            Self::DriverFailed { .. } => SupervisorEventKind::DriverFailed,
            Self::Timeout { .. } => SupervisorEventKind::Timeout,
            Self::CancelRequested { .. } => SupervisorEventKind::CancelRequested,
            Self::ParentStopping { .. } => SupervisorEventKind::ParentStopping,
            Self::StatusRequested => SupervisorEventKind::StatusRequested,
        }
    }

    fn expected_version(&self) -> Option<u64> {
        match self {
            Self::PlanBound {
                expected_version, ..
            }
            | Self::ExecutionAuthorized {
                expected_version, ..
            }
            | Self::RepositoryPrepared {
                expected_version, ..
            }
            | Self::CandidatePublished {
                expected_version, ..
            }
            | Self::ReviewerReviewPublished {
                expected_version, ..
            }
            | Self::ReviewCheckPublished {
                expected_version, ..
            }
            | Self::MergeGateObserved {
                expected_version, ..
            }
            | Self::PullRequestMerged {
                expected_version, ..
            }
            | Self::GitHubRunFinalized {
                expected_version, ..
            }
            | Self::GitHubOperationFailed {
                expected_version, ..
            }
            | Self::TaskRuntimeOpened {
                expected_version, ..
            }
            | Self::RoleSessionOpened {
                expected_version, ..
            }
            | Self::TurnStarted {
                expected_version, ..
            }
            | Self::TurnCompleted {
                expected_version, ..
            }
            | Self::ClarificationSubmitted {
                expected_version, ..
            }
            | Self::ClarificationHumanRequired {
                expected_version, ..
            }
            | Self::TurnFailed {
                expected_version, ..
            }
            | Self::DriverFailed {
                expected_version, ..
            }
            | Self::Timeout {
                expected_version, ..
            }
            | Self::CancelRequested {
                expected_version, ..
            }
            | Self::ParentStopping { expected_version } => Some(*expected_version),
            Self::StatusRequested => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DriverFailureClass {
    Repository,
    Runtime,
    Environment,
    Contract,
    Cleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DriverFailure {
    pub class: DriverFailureClass,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEffect {
    PrepareGitHubRunRepository(PrepareGitHubRunRequest),
    PublishDeveloperCandidate(PublishDeveloperCandidateRequest),
    PublishReviewerReview(PublishReviewerReviewRequest),
    OpenOrUpdateReviewCheck(PublishReviewCheckRequest),
    WaitForMergeGate(WaitForMergeGateRequest),
    MergePullRequest(MergePullRequestRequest),
    FinalizeGitHubRun(FinalizeGitHubRunRequest),
    PublishGitHubTerminal(PublishGitHubTerminalRequest),
    OpenTaskRuntime {
        task_ordinal: usize,
    },
    OpenRoleSession {
        task_ordinal: usize,
        lane: WorkerLane,
    },
    StartTurn {
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
    },
    InterruptTurn {
        task_ordinal: usize,
        lane: WorkerLane,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
    },
    CloseTaskRuntime {
        task_ordinal: usize,
    },
    PrepareClarificationArtifact {
        task_ordinal: usize,
        task_key: String,
        sequence: u32,
        path: PathBuf,
    },
    FinishSession {
        state: SessionState,
        detail: String,
    },
    PublishStatus,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SupervisorEffectKind {
    PrepareGitHubRunRepository,
    PublishDeveloperCandidate,
    PublishReviewerReview,
    OpenOrUpdateReviewCheck,
    WaitForMergeGate,
    MergePullRequest,
    FinalizeGitHubRun,
    PublishGitHubTerminal,
    OpenTaskRuntime,
    OpenRoleSession,
    StartTurn,
    InterruptTurn,
    CloseTaskRuntime,
    PrepareClarificationArtifact,
    FinishSession,
    PublishStatus,
}

#[cfg(test)]
impl SupervisorEffectKind {
    const ALL: [Self; 16] = [
        Self::PrepareGitHubRunRepository,
        Self::PublishDeveloperCandidate,
        Self::PublishReviewerReview,
        Self::OpenOrUpdateReviewCheck,
        Self::WaitForMergeGate,
        Self::MergePullRequest,
        Self::FinalizeGitHubRun,
        Self::PublishGitHubTerminal,
        Self::OpenTaskRuntime,
        Self::OpenRoleSession,
        Self::StartTurn,
        Self::InterruptTurn,
        Self::CloseTaskRuntime,
        Self::PrepareClarificationArtifact,
        Self::FinishSession,
        Self::PublishStatus,
    ];
}

#[cfg(test)]
impl SupervisorEffect {
    fn kind(&self) -> SupervisorEffectKind {
        match self {
            Self::PrepareGitHubRunRepository(_) => SupervisorEffectKind::PrepareGitHubRunRepository,
            Self::PublishDeveloperCandidate(_) => SupervisorEffectKind::PublishDeveloperCandidate,
            Self::PublishReviewerReview(_) => SupervisorEffectKind::PublishReviewerReview,
            Self::OpenOrUpdateReviewCheck(_) => SupervisorEffectKind::OpenOrUpdateReviewCheck,
            Self::WaitForMergeGate(_) => SupervisorEffectKind::WaitForMergeGate,
            Self::MergePullRequest(_) => SupervisorEffectKind::MergePullRequest,
            Self::FinalizeGitHubRun(_) => SupervisorEffectKind::FinalizeGitHubRun,
            Self::PublishGitHubTerminal(_) => SupervisorEffectKind::PublishGitHubTerminal,
            Self::OpenTaskRuntime { .. } => SupervisorEffectKind::OpenTaskRuntime,
            Self::OpenRoleSession { .. } => SupervisorEffectKind::OpenRoleSession,
            Self::StartTurn { .. } => SupervisorEffectKind::StartTurn,
            Self::InterruptTurn { .. } => SupervisorEffectKind::InterruptTurn,
            Self::CloseTaskRuntime { .. } => SupervisorEffectKind::CloseTaskRuntime,
            Self::PrepareClarificationArtifact { .. } => {
                SupervisorEffectKind::PrepareClarificationArtifact
            }
            Self::FinishSession { .. } => SupervisorEffectKind::FinishSession,
            Self::PublishStatus => SupervisorEffectKind::PublishStatus,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorErrorCode {
    VersionMismatch,
    InvalidPlan,
    InvalidEvent,
    InvalidIdentity,
    InvalidTransition,
    InvalidRepository,
    DuplicateCompletion,
    Terminal,
    Overflow,
    InvariantViolation,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{code:?}: {detail}")]
pub struct SupervisorError {
    pub code: SupervisorErrorCode,
    pub detail: String,
}

impl SupervisorError {
    fn new(code: SupervisorErrorCode, detail: impl Into<String>) -> Self {
        let mut detail = detail.into().replace(['\r', '\n'], " ");
        truncate_utf8(&mut detail, MAX_CORE_DIAGNOSTIC_BYTES);
        Self { code, detail }
    }

    fn version(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::VersionMismatch, detail)
    }

    fn invalid_plan(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvalidPlan, detail)
    }

    fn invalid_event(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvalidEvent, detail)
    }

    fn invalid_identity(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvalidIdentity, detail)
    }

    fn invalid_transition(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvalidTransition, detail)
    }

    fn invalid_repository(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvalidRepository, detail)
    }

    fn duplicate(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::DuplicateCompletion, detail)
    }

    fn terminal(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::Terminal, detail)
    }

    fn overflow(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::Overflow, detail)
    }

    fn invariant(detail: impl Into<String>) -> Self {
        Self::new(SupervisorErrorCode::InvariantViolation, detail)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTask {
    pub spec: TaskDraft,
    pub state: TaskState,
    pub review_round: u32,
    pub review_generation: u32,
    pub clarification_rounds_used: u32,
    pub developer_session: Option<RuntimeSessionKey>,
    pub reviewer_sessions: BTreeMap<ReviewerId, RuntimeSessionKey>,
    pub outcome_detail: Option<String>,
    latest_developer_final_path: Option<String>,
    latest_reviewer_final_paths: Vec<String>,
    reviewer_results: BTreeMap<ReviewerId, CoreReviewerResult>,
    historical_reviewer_final_paths: BTreeMap<ReviewerId, Vec<String>>,
    review_requested_generation: Option<u32>,
    clarification_records: Vec<ClarificationRecord>,
    github_base_sha: Option<String>,
    github_final_head_sha: Option<String>,
    github_reviews: BTreeMap<ReviewerId, GitHubReviewSnapshot>,
    github_check: Option<GitHubCheckSnapshot>,
    pending_native_reviews: BTreeMap<ReviewerId, CorePendingNativeReview>,
}

impl CoreTask {
    fn new(spec: TaskDraft, reviewer_ids: &[ReviewerId]) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            review_round: 0,
            review_generation: 0,
            clarification_rounds_used: 0,
            developer_session: None,
            reviewer_sessions: BTreeMap::new(),
            outcome_detail: None,
            latest_developer_final_path: None,
            latest_reviewer_final_paths: Vec::new(),
            reviewer_results: BTreeMap::new(),
            historical_reviewer_final_paths: reviewer_ids
                .iter()
                .copied()
                .map(|reviewer_id| (reviewer_id, Vec::new()))
                .collect(),
            review_requested_generation: None,
            clarification_records: Vec::new(),
            github_base_sha: None,
            github_final_head_sha: None,
            github_reviews: BTreeMap::new(),
            github_check: None,
            pending_native_reviews: BTreeMap::new(),
        }
    }

    pub fn latest_developer_final_path(&self) -> Option<&str> {
        self.latest_developer_final_path.as_deref()
    }

    pub fn latest_reviewer_final_paths(&self) -> &[String] {
        &self.latest_reviewer_final_paths
    }

    pub fn reviewer_final_paths(&self, reviewer_id: ReviewerId) -> &[String] {
        self.reviewer_results
            .get(&reviewer_id)
            .map(|result| result.final_message_paths.as_slice())
            .unwrap_or_default()
    }

    pub fn clarification_records(&self) -> &[ClarificationRecord] {
        &self.clarification_records
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoreReviewerResult {
    generation: u32,
    verdict: ReviewerVerdict,
    final_message_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CorePendingNativeReview {
    generation: u32,
    verdict: ReviewerVerdict,
    final_message_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostCheckTransition {
    Correction,
    Lgtm,
    ReviewExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingGitHubOperation {
    Prepare(PrepareGitHubRunRequest),
    Candidate(PublishDeveloperCandidateRequest),
    Review(PublishReviewerReviewRequest),
    Check {
        request: PublishReviewCheckRequest,
        transition: PostCheckTransition,
    },
    MergeGate(WaitForMergeGateRequest),
    Merge(MergePullRequestRequest),
    Finalize(FinalizeGitHubRunRequest),
}

impl PendingGitHubOperation {
    fn operation_id(&self) -> &str {
        match self {
            Self::Prepare(request) => &request.operation_id,
            Self::Candidate(request) => &request.operation_id,
            Self::Review(request) => &request.operation_id,
            Self::Check { request, .. } => &request.operation_id,
            Self::MergeGate(request) => &request.operation_id,
            Self::Merge(request) => &request.operation_id,
            Self::Finalize(request) => &request.operation_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoreGitHubRunState {
    phase: GitHubDeliveryPhase,
    local_base_ref_present: bool,
    local_branch_present: bool,
    worktree_path: Option<String>,
    published_head_sha: Option<String>,
    pr_number: Option<u64>,
    pr_node_id: Option<String>,
    pr_url: Option<String>,
    current_check: Option<GitHubCheckSnapshot>,
    outcome: Option<GitHubDeliveryOutcome>,
    final_base_sha: Option<String>,
    final_ruleset_attestation_sha256: Option<String>,
    merge_sha: Option<String>,
    merge_url: Option<String>,
    finalization: Option<GitHubFinalizationSnapshot>,
    pending_operations: BTreeMap<String, PendingGitHubOperation>,
}

impl CoreGitHubRunState {
    fn new() -> Self {
        Self {
            phase: GitHubDeliveryPhase::PreparingRepository,
            local_base_ref_present: false,
            local_branch_present: false,
            worktree_path: None,
            published_head_sha: None,
            pr_number: None,
            pr_node_id: None,
            pr_url: None,
            current_check: None,
            outcome: None,
            final_base_sha: None,
            final_ruleset_attestation_sha256: None,
            merge_sha: None,
            merge_url: None,
            finalization: None,
            pending_operations: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedSessionOpen {
    task_ordinal: usize,
    lane: WorkerLane,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedTurnStart {
    task_ordinal: usize,
    lane: WorkerLane,
    review_generation: Option<u32>,
    purpose: RuntimeTurnPurpose,
    session: RuntimeSessionKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoreActiveTurn {
    task_ordinal: usize,
    lane: WorkerLane,
    review_generation: Option<u32>,
    purpose: RuntimeTurnPurpose,
    session: RuntimeSessionKey,
    turn: RuntimeTurnKey,
    completion_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorCore {
    run_id: String,
    project_root: PathBuf,
    profile_hash: String,
    delivery_binding: DeliveryBinding,
    github_run_binding: Option<GitHubRunBinding>,
    github_state: Option<CoreGitHubRunState>,
    reviewer_bindings: Vec<ReviewerBindingSnapshot>,
    session_state: SessionState,
    version: u64,
    next_plan_version: u64,
    plan_version: Option<u64>,
    plan_hash: Option<String>,
    tasks: Vec<CoreTask>,
    current_task: Option<usize>,
    terminal_detail: Option<String>,
    pending_architect_action: Option<PendingArchitectActionSnapshot>,
    pending_runtime_open: Option<usize>,
    runtime_open: Option<usize>,
    pending_session_opens: BTreeMap<WorkerLane, ExpectedSessionOpen>,
    pending_turn_starts: BTreeMap<WorkerLane, ExpectedTurnStart>,
    active_turns: BTreeMap<WorkerLane, CoreActiveTurn>,
    used_sessions: BTreeSet<RuntimeSessionKey>,
    used_turns: BTreeSet<RuntimeTurnKey>,
    accepted_completion_tokens: BTreeSet<String>,
    progress_events: Vec<SessionProgressEvent>,
}

impl SupervisorCore {
    pub fn new(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
    ) -> Result<Self, SupervisorError> {
        Self::new_with_reviewer_bindings(
            run_id,
            project_root,
            profile_hash,
            default_reviewer_bindings(),
        )
    }

    pub fn new_with_reviewer_bindings(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
        reviewer_bindings: Vec<ReviewerBindingSnapshot>,
    ) -> Result<Self, SupervisorError> {
        Self::new_at_version(run_id, project_root, profile_hash, reviewer_bindings, 0)
    }

    pub fn new_with_delivery_binding(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
        reviewer_bindings: Vec<ReviewerBindingSnapshot>,
        delivery_binding: DeliveryBinding,
    ) -> Result<Self, SupervisorError> {
        Self::new_at_version_with_delivery(
            run_id,
            project_root,
            profile_hash,
            reviewer_bindings,
            delivery_binding,
            0,
        )
    }

    fn new_at_version(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
        reviewer_bindings: Vec<ReviewerBindingSnapshot>,
        version: u64,
    ) -> Result<Self, SupervisorError> {
        Self::new_at_version_with_delivery(
            run_id,
            project_root,
            profile_hash,
            reviewer_bindings,
            DeliveryBinding::LocalCandidate,
            version,
        )
    }

    fn new_at_version_with_delivery(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
        reviewer_bindings: Vec<ReviewerBindingSnapshot>,
        delivery_binding: DeliveryBinding,
        version: u64,
    ) -> Result<Self, SupervisorError> {
        validate_identifier("run id", &run_id)?;
        let project_text = project_root
            .to_str()
            .ok_or_else(|| SupervisorError::invalid_event("project root must be UTF-8"))?;
        validate_absolute_path("project root", project_text)?;
        validate_sha256("profile hash", &profile_hash)?;
        let core = Self {
            run_id,
            project_root,
            profile_hash,
            delivery_binding,
            github_run_binding: None,
            github_state: None,
            reviewer_bindings,
            session_state: SessionState::AwaitingPlan,
            version,
            next_plan_version: 1,
            plan_version: None,
            plan_hash: None,
            tasks: Vec::new(),
            current_task: None,
            terminal_detail: None,
            pending_architect_action: None,
            pending_runtime_open: None,
            runtime_open: None,
            pending_session_opens: BTreeMap::new(),
            pending_turn_starts: BTreeMap::new(),
            active_turns: BTreeMap::new(),
            used_sessions: BTreeSet::new(),
            used_turns: BTreeSet::new(),
            accepted_completion_tokens: BTreeSet::new(),
            progress_events: Vec::new(),
        };
        core.assert_invariants()?;
        Ok(core)
    }

    /// Create the next immutable run for the same foreground Architect.
    ///
    /// The completed core is left untouched. The new core keeps the project
    /// and frozen worker profile binding, resets all run-local task/session
    /// state, and advances the session version so delayed mutations from an
    /// earlier run cannot match the new one.
    pub fn next_run(&self, run_id: String) -> Result<Self, SupervisorError> {
        self.assert_invariants()?;
        if !self.session_state.is_terminal() {
            return Err(SupervisorError::invalid_transition(
                "a new run requires a terminal current run",
            ));
        }
        let version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("session version overflow"))?;
        Self::new_at_version_with_delivery(
            run_id,
            self.project_root.clone(),
            self.profile_hash.clone(),
            self.reviewer_bindings.clone(),
            self.delivery_binding.clone(),
            version,
        )
    }

    /// Compatibility constructor retained from P0 for source-level seam tests.
    pub fn skeleton(run_id: String, project_root: PathBuf) -> Self {
        Self::new(run_id, project_root, "0".repeat(64))
            .expect("P0 skeleton arguments must form a valid core")
    }

    pub fn session_state(&self) -> SessionState {
        self.session_state
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn project_root(&self) -> &PathBuf {
        &self.project_root
    }

    pub fn profile_hash(&self) -> &str {
        &self.profile_hash
    }

    pub fn tasks(&self) -> &[CoreTask] {
        &self.tasks
    }

    pub fn current_task(&self) -> Option<usize> {
        self.current_task
    }

    pub fn plan_version(&self) -> Option<u64> {
        self.plan_version
    }

    pub fn plan_hash(&self) -> Option<&str> {
        self.plan_hash.as_deref()
    }

    pub fn github_run_binding(&self) -> Option<&GitHubRunBinding> {
        self.github_run_binding.as_ref()
    }

    pub fn expected_plan_hash(&self, plan_version: u64, tasks: &[TaskDraft]) -> String {
        self.expected_plan_hash_for_inspection(plan_version, tasks, None)
    }

    pub fn expected_plan_hash_for_inspection(
        &self,
        plan_version: u64,
        tasks: &[TaskDraft],
        github_inspection: Option<&GitHubInspectionBinding>,
    ) -> String {
        canonical_hash(&(
            "hcom-provider-routed-session-plan-v3",
            &self.run_id,
            plan_version,
            &self.project_root,
            &self.profile_hash,
            &self.reviewer_bindings,
            &self.delivery_binding,
            github_inspection,
            "hcom/<run-id>-<first-12-plan-hash-chars>",
            tasks,
        ))
    }

    pub fn snapshot(&self) -> SessionStatusSnapshot {
        SessionStatusSnapshot {
            run_id: self.run_id.clone(),
            state: self.session_state,
            version: self.version,
            project_root: self.project_root.to_string_lossy().into_owned(),
            delivery_binding: self.delivery_binding.clone(),
            github: self
                .delivery_binding
                .is_github()
                .then(|| GitHubDeliveryStatusSnapshot {
                    run_binding: self.github_run_binding.clone(),
                    worktree_path: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.worktree_path.clone()),
                    pr_number: self.github_state.as_ref().and_then(|state| state.pr_number),
                    pr_url: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.pr_url.clone()),
                    published_head_sha: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.published_head_sha.clone()),
                    current_check: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.current_check.clone()),
                    phase: self.github_state.as_ref().map(|state| state.phase),
                    outcome: self.github_state.as_ref().and_then(|state| state.outcome),
                    final_base_sha: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.final_base_sha.clone()),
                    final_ruleset_attestation_sha256: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.final_ruleset_attestation_sha256.clone()),
                    merge_already_confirmed: self
                        .github_state
                        .as_ref()
                        .is_some_and(|state| state.merge_sha.is_some()),
                    merge_sha: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.merge_sha.clone()),
                    merge_url: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.merge_url.clone()),
                    finalization: self
                        .github_state
                        .as_ref()
                        .and_then(|state| state.finalization.clone()),
                    preserved_branch: self.github_state.as_ref().and_then(|state| {
                        let branch_remains = state.finalization.as_ref().map_or(
                            state.local_branch_present,
                            |finalization| {
                                !finalization.local_ref_removed
                                    || !matches!(
                                        finalization.remote_ref_outcome.as_deref(),
                                        Some("deleted" | "already_absent")
                                    )
                            },
                        );
                        (matches!(
                            state.phase,
                            GitHubDeliveryPhase::PreservedUnmerged
                                | GitHubDeliveryPhase::PreparingRepository
                                | GitHubDeliveryPhase::TasksRunning
                                | GitHubDeliveryPhase::AwaitingMerge
                                | GitHubDeliveryPhase::Finalizing
                        ) && branch_remains)
                            .then(|| {
                                self.github_run_binding
                                    .as_ref()
                                    .map(|run| run.generated_run_branch.clone())
                            })
                            .flatten()
                    }),
                    preserved_worktree: self.github_state.as_ref().and_then(|state| {
                        (state.phase != GitHubDeliveryPhase::Delivered
                            && state
                                .finalization
                                .as_ref()
                                .is_none_or(|finalization| !finalization.local_worktree_removed))
                        .then(|| state.worktree_path.clone())
                        .flatten()
                    }),
                    latest_inspection: None,
                }),
            plan_version: self.plan_version,
            plan_hash: self.plan_hash.clone(),
            current_task_ordinal: self
                .current_task
                .and_then(|index| u32::try_from(index).ok()),
            active_workers: self
                .active_turns
                .values()
                .map(|active| ActiveWorkerSnapshot {
                    task_ordinal: u32::try_from(active.task_ordinal).unwrap_or(u32::MAX),
                    task_key: self.tasks[active.task_ordinal].spec.task_key.clone(),
                    worker_lane: active.lane,
                    reviewer_id: active.lane.reviewer_id(),
                    purpose: active.purpose.as_str().into(),
                })
                .collect(),
            reviewer_bindings: self.reviewer_bindings.clone(),
            pending_architect_action: self.pending_architect_action.clone(),
            terminal_detail: self.terminal_detail.clone(),
            tasks: self
                .tasks
                .iter()
                .enumerate()
                .map(|(index, task)| TaskStatusSnapshot {
                    task_key: task.spec.task_key.clone(),
                    ordinal: u32::try_from(index).unwrap_or(u32::MAX),
                    state: task.state,
                    repository_root: task.spec.repository_root.clone(),
                    task_document_path: task.spec.task_document_path.clone(),
                    design_document_paths: task.spec.design_document_paths.clone(),
                    task_selector: task.spec.task_selector.clone(),
                    branch: self
                        .github_run_binding
                        .as_ref()
                        .map(|binding| binding.generated_run_branch.clone()),
                    review_round: task.review_round,
                    review_generation: task.review_generation,
                    max_review_rounds: task.spec.max_review_rounds,
                    clarification_rounds_used: task.clarification_rounds_used,
                    max_clarification_rounds: task.spec.max_clarification_rounds,
                    clarification_record_count: u32::try_from(task.clarification_records.len())
                        .unwrap_or(u32::MAX),
                    base_revision: task.github_base_sha.clone().or_else(|| {
                        (index == 0)
                            .then(|| {
                                self.github_run_binding
                                    .as_ref()
                                    .map(|binding| binding.expected_base_sha.clone())
                            })
                            .flatten()
                    }),
                    head_revision: task.github_final_head_sha.clone().or_else(|| {
                        (self.current_task == Some(index))
                            .then(|| {
                                self.github_state
                                    .as_ref()
                                    .and_then(|state| state.published_head_sha.clone())
                            })
                            .flatten()
                    }),
                    github_reviews: task.github_reviews.values().cloned().collect(),
                    github_check: task.github_check.clone(),
                    developer_session_bound: task.developer_session.is_some(),
                    reviewers: self
                        .reviewer_ids()
                        .into_iter()
                        .map(|reviewer_id| {
                            let result = task.reviewer_results.get(&reviewer_id);
                            ReviewerResultSnapshot {
                                reviewer_id,
                                session_bound: task.reviewer_sessions.contains_key(&reviewer_id),
                                current_generation: result.map(|result| result.generation),
                                current_verdict: result.map(|result| result.verdict),
                                current_final_message_paths: result
                                    .map(|result| result.final_message_paths.clone())
                                    .unwrap_or_default(),
                            }
                        })
                        .collect(),
                    outcome_detail: task.outcome_detail.clone(),
                    latest_developer_final_path: task.latest_developer_final_path.clone(),
                })
                .collect(),
        }
    }

    pub fn clarification_page(
        &self,
        run_id: &str,
        task_ordinal: u32,
        task_key: &str,
        after_sequence: u32,
        limit: u8,
    ) -> Result<ClarificationPage, SupervisorError> {
        if run_id != self.run_id {
            return Err(SupervisorError::invalid_identity(
                "run id does not match the current run",
            ));
        }
        if !(1..=MAX_CLARIFICATION_PAGE_RECORDS).contains(&limit) {
            return Err(SupervisorError::invalid_event(
                "clarification page limit is out of range",
            ));
        }
        let task_index = usize::try_from(task_ordinal)
            .map_err(|_| SupervisorError::invalid_identity("task ordinal is out of range"))?;
        let task = self
            .tasks
            .get(task_index)
            .ok_or_else(|| SupervisorError::invalid_identity("task ordinal is out of range"))?;
        if task.spec.task_key != task_key {
            return Err(SupervisorError::invalid_identity(
                "task key does not match the requested task ordinal",
            ));
        }
        let total_records = u32::try_from(task.clarification_records.len())
            .map_err(|_| SupervisorError::overflow("clarification record count overflow"))?;
        if after_sequence > total_records {
            return Err(SupervisorError::invalid_event(
                "clarification page cursor is ahead of the task record count",
            ));
        }
        let records = task
            .clarification_records
            .iter()
            .skip(
                usize::try_from(after_sequence)
                    .map_err(|_| SupervisorError::overflow("clarification cursor overflow"))?,
            )
            .take(usize::from(limit))
            .cloned()
            .collect::<Vec<_>>();
        let next_after_sequence = records
            .last()
            .map(|record| record.sequence)
            .filter(|sequence| *sequence < total_records);
        Ok(ClarificationPage {
            run_id: self.run_id.clone(),
            session_version: self.version,
            task_ordinal,
            task_key: task_key.to_owned(),
            total_records,
            after_sequence,
            records,
            next_after_sequence,
        })
    }

    pub fn progress_event_after(
        &self,
        run_id: &str,
        after_sequence: u32,
    ) -> Result<Option<SessionProgressEvent>, SupervisorError> {
        if run_id != self.run_id {
            return Err(SupervisorError::invalid_identity(
                "progress cursor run id does not match the current run",
            ));
        }
        let event_count = u32::try_from(self.progress_events.len())
            .map_err(|_| SupervisorError::overflow("progress event count overflow"))?;
        if after_sequence > event_count {
            return Err(SupervisorError::invalid_identity(
                "progress cursor is ahead of the current run",
            ));
        }
        Ok(self
            .progress_events
            .get(
                usize::try_from(after_sequence)
                    .map_err(|_| SupervisorError::overflow("progress cursor overflow"))?,
            )
            .cloned())
    }

    /// Apply one event transactionally and return the exact ordered effects.
    ///
    /// Rejected events leave the original state byte-for-byte unchanged.
    /// `StatusRequested` is the only read-only event: it produces no effect
    /// and does not advance the session version.
    pub fn reduce(
        &mut self,
        event: SupervisorEvent,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.assert_invariants()?;
        if event.kind() == SupervisorEventKind::StatusRequested {
            return Ok(Vec::new());
        }
        if self.session_state.is_terminal() {
            return Err(SupervisorError::terminal(
                "terminal session accepts only status requests",
            ));
        }
        let expected_version = event
            .expected_version()
            .expect("mutating events carry an expected version");
        if expected_version != self.version {
            return Err(SupervisorError::version(
                "event expected_session_version does not match current version",
            ));
        }
        let next_version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("session version overflow"))?;

        let mut next = self.clone();
        let mut effects = next.apply(event)?;
        next.version = next_version;
        effects.push(SupervisorEffect::PublishStatus);
        next.assert_invariants()?;
        *self = next;
        Ok(effects)
    }

    fn apply(&mut self, event: SupervisorEvent) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        match event {
            SupervisorEvent::PlanBound {
                plan_version,
                plan_hash,
                tasks,
                github_run_binding,
                ..
            } => self.bind_plan(plan_version, plan_hash, tasks, github_run_binding),
            SupervisorEvent::ExecutionAuthorized {
                plan_version,
                plan_hash,
                ..
            } => self.authorize(plan_version, plan_hash),
            SupervisorEvent::RepositoryPrepared {
                operation_id,
                base_sha,
                branch,
                worktree_path,
                ..
            } => self.repository_prepared(&operation_id, &base_sha, &branch, &worktree_path),
            SupervisorEvent::CandidatePublished {
                operation_id,
                task_ordinal,
                generation,
                previous_head_sha,
                head_sha,
                pr_number,
                pr_node_id,
                pr_url,
                pr_actor_bot_user_id,
                check_run_id,
                check_url,
                check_actor_app_id,
                ..
            } => self.candidate_published(
                &operation_id,
                task_ordinal,
                generation,
                &previous_head_sha,
                &head_sha,
                pr_number,
                &pr_node_id,
                &pr_url,
                pr_actor_bot_user_id,
                check_run_id,
                &check_url,
                check_actor_app_id,
            ),
            SupervisorEvent::ReviewerReviewPublished {
                operation_id,
                task_ordinal,
                reviewer_id,
                generation,
                head_sha,
                verdict,
                review_id,
                review_url,
                actor_bot_user_id,
                final_artifact_sha256,
                ..
            } => self.reviewer_review_published(
                &operation_id,
                task_ordinal,
                reviewer_id,
                generation,
                &head_sha,
                verdict,
                review_id,
                &review_url,
                actor_bot_user_id,
                &final_artifact_sha256,
            ),
            SupervisorEvent::ReviewCheckPublished {
                operation_id,
                task_ordinal,
                generation,
                head_sha,
                check_run_id,
                check_url,
                state,
                actor_app_id,
                ..
            } => self.review_check_published(
                &operation_id,
                task_ordinal,
                generation,
                &head_sha,
                check_run_id,
                &check_url,
                &state,
                actor_app_id,
            ),
            SupervisorEvent::MergeGateObserved {
                operation_id,
                pr_number,
                final_head_sha,
                base_sha,
                ruleset_attestation_sha256,
                check_run_id,
                ..
            } => self.merge_gate_observed(
                &operation_id,
                pr_number,
                &final_head_sha,
                &base_sha,
                &ruleset_attestation_sha256,
                check_run_id,
            ),
            SupervisorEvent::PullRequestMerged {
                operation_id,
                pr_number,
                final_head_sha,
                merge_sha,
                merge_url,
                actor_bot_user_id,
                merge_evidence_durable,
                ..
            } => self.pull_request_merged(
                &operation_id,
                pr_number,
                &final_head_sha,
                &merge_sha,
                &merge_url,
                actor_bot_user_id,
                merge_evidence_durable,
            ),
            SupervisorEvent::GitHubRunFinalized {
                operation_id,
                pr_number,
                final_head_sha,
                merge_sha,
                finalization,
                ..
            } => self.github_run_finalized(
                &operation_id,
                pr_number,
                &final_head_sha,
                &merge_sha,
                finalization,
            ),
            SupervisorEvent::GitHubOperationFailed {
                operation_id,
                detail,
                partial_operation,
                ..
            } => self.github_operation_failed(&operation_id, &detail, partial_operation),
            SupervisorEvent::TaskRuntimeOpened { task_ordinal, .. } => {
                self.task_runtime_opened(task_ordinal)
            }
            SupervisorEvent::RoleSessionOpened {
                task_ordinal,
                lane,
                session,
                ..
            } => self.role_session_opened(task_ordinal, lane, session),
            SupervisorEvent::TurnStarted {
                task_ordinal,
                lane,
                review_generation,
                purpose,
                session,
                turn,
                completion_token,
                ..
            } => self.turn_started(
                task_ordinal,
                lane,
                review_generation,
                purpose,
                session,
                turn,
                completion_token,
            ),
            SupervisorEvent::TurnCompleted {
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                completion_token,
                outcome,
                final_message_path,
                ..
            } => self.turn_completed(
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                &completion_token,
                outcome,
                final_message_path,
            ),
            SupervisorEvent::ClarificationSubmitted {
                task_ordinal,
                task_key,
                action_sequence,
                developer_request_path,
                clarification_document_path,
                human_decision_confirmed,
                ..
            } => self.clarification_submitted(
                task_ordinal,
                &task_key,
                action_sequence,
                &developer_request_path,
                &clarification_document_path,
                human_decision_confirmed,
            ),
            SupervisorEvent::ClarificationHumanRequired {
                task_ordinal,
                task_key,
                action_sequence,
                developer_request_path,
                ..
            } => self.clarification_human_required(
                task_ordinal,
                &task_key,
                action_sequence,
                &developer_request_path,
            ),
            SupervisorEvent::TurnFailed {
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                completion_token,
                failure,
                ..
            } => self.turn_failed(
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                &completion_token,
                failure,
            ),
            SupervisorEvent::DriverFailed {
                task_ordinal,
                failure,
                ..
            } => self.driver_failed(task_ordinal, failure),
            SupervisorEvent::Timeout {
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                completion_token,
                ..
            } => self.timeout(
                task_ordinal,
                lane,
                review_generation,
                session,
                turn,
                &completion_token,
            ),
            SupervisorEvent::CancelRequested { reason, .. } => self.cancel(&reason),
            SupervisorEvent::ParentStopping { .. } => self.parent_stopping(),
            SupervisorEvent::StatusRequested => unreachable!("handled before mutation"),
        }
    }

    fn bind_plan(
        &mut self,
        plan_version: u64,
        plan_hash: String,
        tasks: Vec<TaskDraft>,
        github_run_binding: Option<GitHubRunBinding>,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if !matches!(
            self.session_state,
            SessionState::AwaitingPlan | SessionState::AwaitingApproval
        ) {
            return Err(SupervisorError::invalid_transition(
                "plan can only be bound before execution",
            ));
        }
        if plan_version == 0 || plan_version != self.next_plan_version {
            return Err(SupervisorError::invalid_plan(
                "plan_version must equal the next positive plan version",
            ));
        }
        validate_sha256("plan hash", &plan_hash)
            .map_err(|_| SupervisorError::invalid_plan("plan hash is malformed"))?;
        if tasks.is_empty() || tasks.len() > MAX_TASKS {
            return Err(SupervisorError::invalid_plan(
                "ordered plan must contain between 1 and 64 tasks",
            ));
        }
        let mut keys = BTreeSet::new();
        for task in &tasks {
            task.validate_for_reviewer_count(self.reviewer_bindings.len())
                .map_err(|error| SupervisorError::invalid_plan(error.to_string()))?;
            if !keys.insert(task.task_key.as_str()) {
                return Err(SupervisorError::invalid_plan(
                    "ordered plan task keys must be unique",
                ));
            }
        }

        match (&self.delivery_binding, &github_run_binding) {
            (DeliveryBinding::LocalCandidate, None) => {}
            (DeliveryBinding::LocalCandidate, Some(_)) => {
                return Err(SupervisorError::invalid_plan(
                    "local candidate plan unexpectedly contains a GitHub run binding",
                ));
            }
            (DeliveryBinding::GitHubPullRequest { binding }, Some(run)) => {
                if run.inspected_repository_id != binding.repository_id
                    || run.expected_base_ref != format!("refs/heads/{}", binding.base_branch)
                    || run.expected_base_sha.len() != 40
                    || !run
                        .expected_base_sha
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || validate_sha256(
                        "GitHub ruleset attestation",
                        &run.ruleset_attestation_sha256,
                    )
                    .is_err()
                    || validate_identifier("GitHub inspection ID", &run.inspection_id).is_err()
                    || binding
                        .reviewer_apps
                        .iter()
                        .map(|reviewer| reviewer.reviewer_id)
                        .ne(self.reviewer_ids())
                    || tasks
                        .iter()
                        .any(|task| task.repository_root != binding.local_repository_root)
                {
                    return Err(SupervisorError::invalid_plan(
                        "GitHub run binding differs from the frozen delivery binding",
                    ));
                }
            }
            (DeliveryBinding::GitHubPullRequest { .. }, None) => {
                return Err(SupervisorError::invalid_plan(
                    "GitHub Pull Request plan omitted its inspected run binding",
                ));
            }
        }
        let inspection = github_run_binding
            .as_ref()
            .map(GitHubRunBinding::inspection);
        let expected_hash =
            self.expected_plan_hash_for_inspection(plan_version, &tasks, inspection.as_ref());
        if plan_hash != expected_hash {
            return Err(SupervisorError::invalid_plan(
                "plan hash does not match the exact ordered plan binding",
            ));
        }
        if let Some(run) = &github_run_binding {
            let expected_branch = format!("hcom/{}-{}", self.run_id, &plan_hash[..12]);
            if run.generated_run_branch != expected_branch {
                return Err(SupervisorError::invalid_plan(
                    "GitHub run branch is not derived from the exact run and plan hash",
                ));
            }
        }
        let next_plan_version = plan_version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("plan version overflow"))?;

        let reviewer_ids = self.reviewer_ids();
        self.tasks = tasks
            .into_iter()
            .map(|task| CoreTask::new(task, &reviewer_ids))
            .collect();
        self.plan_version = Some(plan_version);
        self.plan_hash = Some(plan_hash);
        self.github_run_binding = github_run_binding;
        self.github_state = None;
        self.next_plan_version = next_plan_version;
        self.current_task = None;
        self.terminal_detail = None;
        self.session_state = SessionState::AwaitingApproval;
        self.clear_runtime_state();
        Ok(Vec::new())
    }

    fn authorize(
        &mut self,
        plan_version: Option<u64>,
        plan_hash: Option<String>,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.session_state != SessionState::AwaitingApproval {
            return Err(SupervisorError::invalid_transition(
                "execution authorization requires an awaiting-approval plan",
            ));
        }
        let supplied_version = plan_version.ok_or_else(|| {
            SupervisorError::invalid_plan("execution authorization omitted plan_version")
        })?;
        let supplied_hash = plan_hash.ok_or_else(|| {
            SupervisorError::invalid_plan("execution authorization omitted plan_hash")
        })?;
        validate_sha256("authorized plan hash", &supplied_hash)
            .map_err(|_| SupervisorError::invalid_plan("authorized plan hash is malformed"))?;
        if Some(supplied_version) != self.plan_version
            || Some(&supplied_hash) != self.plan_hash.as_ref()
        {
            return Err(SupervisorError::invalid_plan(
                "execution authorization references a stale plan version or hash",
            ));
        }
        if self.tasks.is_empty() {
            return Err(SupervisorError::invariant(
                "approved plan unexpectedly contains no tasks",
            ));
        }

        self.session_state = SessionState::Running;
        self.current_task = Some(0);
        if self.delivery_binding.is_github() {
            let run_binding = self.github_run_binding.clone().ok_or_else(|| {
                SupervisorError::invariant("GitHub authorization lost its run binding")
            })?;
            let plan_hash = self
                .plan_hash
                .clone()
                .ok_or_else(|| SupervisorError::invariant("authorized plan lost its hash"))?;
            let request = PrepareGitHubRunRequest {
                operation_id: self.github_operation_id("prepare", None, None, None)?,
                run_id: self.run_id.clone(),
                plan_hash,
                run_binding,
            };
            let mut state = CoreGitHubRunState::new();
            state.pending_operations.insert(
                request.operation_id.clone(),
                PendingGitHubOperation::Prepare(request.clone()),
            );
            self.github_state = Some(state);
            Ok(vec![SupervisorEffect::PrepareGitHubRunRepository(request)])
        } else {
            self.schedule_runtime_open(0)
        }
    }

    fn github_operation_id(
        &self,
        kind: &str,
        task_ordinal: Option<usize>,
        generation: Option<u32>,
        lane: Option<ReviewerId>,
    ) -> Result<String, SupervisorError> {
        validate_identifier("GitHub operation kind", kind)?;
        let task = task_ordinal
            .map(|value| value.to_string())
            .unwrap_or_else(|| "run".into());
        let generation = generation
            .map(|value| value.to_string())
            .unwrap_or_else(|| "0".into());
        let lane = lane.map(ReviewerId::as_str).unwrap_or("supervisor");
        let operation_id = format!("github.{kind}.{task}.{generation}.{lane}");
        validate_identifier("GitHub operation ID", &operation_id)?;
        Ok(operation_id)
    }

    fn github_state(&self) -> Result<&CoreGitHubRunState, SupervisorError> {
        self.github_state
            .as_ref()
            .ok_or_else(|| SupervisorError::invariant("GitHub run state is missing"))
    }

    fn github_state_mut(&mut self) -> Result<&mut CoreGitHubRunState, SupervisorError> {
        self.github_state
            .as_mut()
            .ok_or_else(|| SupervisorError::invariant("GitHub run state is missing"))
    }

    fn take_github_operation(
        &mut self,
        operation_id: &str,
    ) -> Result<PendingGitHubOperation, SupervisorError> {
        validate_identifier("GitHub operation ID", operation_id)?;
        self.github_state_mut()?
            .pending_operations
            .remove(operation_id)
            .ok_or_else(|| {
                SupervisorError::invalid_identity(
                    "GitHub observation does not match a pending exact operation",
                )
            })
    }

    fn repository_prepared(
        &mut self,
        operation_id: &str,
        base_sha: &str,
        branch: &str,
        worktree_path: &str,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(0)?;
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Prepare(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "repository preparation observation matched another operation kind",
            ));
        };
        if request.operation_id != operation_id
            || request.run_binding.expected_base_sha != base_sha
            || request.run_binding.generated_run_branch != branch
        {
            return Err(SupervisorError::invalid_identity(
                "prepared repository differs from the exact approved run binding",
            ));
        }
        validate_git_sha("prepared GitHub base SHA", base_sha)?;
        let expected_worktree = self
            .project_root
            .join("hcom-tasks")
            .join(&self.run_id)
            .join("repository");
        if expected_worktree.to_str() != Some(worktree_path) {
            return Err(SupervisorError::invalid_repository(
                "prepared GitHub worktree path differs from the run-owned path",
            ));
        }
        validate_absolute_path("prepared GitHub worktree path", worktree_path)?;
        let state = self.github_state_mut()?;
        if state.phase != GitHubDeliveryPhase::PreparingRepository
            || state.worktree_path.is_some()
            || state.local_base_ref_present
            || state.local_branch_present
            || state.published_head_sha.is_some()
        {
            return Err(SupervisorError::invalid_transition(
                "repository preparation arrived outside the preparing phase",
            ));
        }
        state.local_base_ref_present = true;
        state.local_branch_present = true;
        state.worktree_path = Some(worktree_path.into());
        state.phase = GitHubDeliveryPhase::TasksRunning;
        self.schedule_runtime_open(0)
    }

    fn task_outcome_evidence(
        &self,
        current_override: Option<(usize, TaskCompletionOutcome)>,
    ) -> Vec<GitHubTaskOutcomeEvidence> {
        self.tasks
            .iter()
            .enumerate()
            .map(|(task_ordinal, task)| GitHubTaskOutcomeEvidence {
                task_ordinal,
                task_key: task.spec.task_key.clone(),
                task_title: task.spec.title.clone(),
                task_base_sha: task.github_base_sha.clone().unwrap_or_default(),
                task_final_head_sha: task.github_final_head_sha.clone(),
                outcome: current_override
                    .filter(|(ordinal, _)| *ordinal == task_ordinal)
                    .map(|(_, outcome)| outcome)
                    .or(match task.state {
                        TaskState::Lgtm => Some(TaskCompletionOutcome::Lgtm),
                        TaskState::ReviewExhausted => Some(TaskCompletionOutcome::ReviewExhausted),
                        _ => None,
                    }),
                reviews: task.github_reviews.values().cloned().collect(),
            })
            .collect()
    }

    fn publish_developer_candidate(
        &mut self,
        task_ordinal: usize,
        developer_final_path: String,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.begin_review_generation(task_ordinal, developer_final_path.clone())?;
        let (previous_head_sha, pr_number, check_run_id) = {
            let state = self.github_state()?;
            if state.phase != GitHubDeliveryPhase::TasksRunning
                || !state.pending_operations.is_empty()
            {
                return Err(SupervisorError::invalid_transition(
                    "Developer candidate publication requires an idle task-running GitHub phase",
                ));
            }
            (
                state.published_head_sha.clone().unwrap_or_else(|| {
                    self.github_run_binding
                        .as_ref()
                        .expect("GitHub run state has a run binding")
                        .expected_base_sha
                        .clone()
                }),
                state.pr_number,
                state.current_check.as_ref().map(|check| check.check_run_id),
            )
        };
        let (task_base_sha, correction, generation, task_key, task_title) = {
            let task = &mut self.tasks[task_ordinal];
            let task_base_sha = task
                .github_base_sha
                .get_or_insert_with(|| previous_head_sha.clone())
                .clone();
            task.state = TaskState::PublishingCandidate;
            (
                task_base_sha,
                task.review_round > 0,
                task.review_generation,
                task.spec.task_key.clone(),
                task.spec.title.clone(),
            )
        };
        let request = PublishDeveloperCandidateRequest {
            operation_id: self.github_operation_id(
                "candidate",
                Some(task_ordinal),
                Some(generation),
                None,
            )?,
            run_id: self.run_id.clone(),
            plan_hash: self
                .plan_hash
                .clone()
                .ok_or_else(|| SupervisorError::invariant("GitHub plan hash is missing"))?,
            task_ordinal,
            task_count: self.tasks.len(),
            task_key,
            task_title,
            generation,
            task_base_sha,
            previous_head_sha,
            developer_final_path,
            correction,
            existing_pr_number: pr_number,
            expected_check_run_id: check_run_id,
            task_outcomes: self.task_outcome_evidence(None),
        };
        self.github_state_mut()?.pending_operations.insert(
            request.operation_id.clone(),
            PendingGitHubOperation::Candidate(request.clone()),
        );
        Ok(vec![SupervisorEffect::PublishDeveloperCandidate(request)])
    }

    #[allow(clippy::too_many_arguments)]
    fn candidate_published(
        &mut self,
        operation_id: &str,
        task_ordinal: usize,
        generation: u32,
        previous_head_sha: &str,
        head_sha: &str,
        pr_number: u64,
        pr_node_id: &str,
        pr_url: &str,
        pr_actor_bot_user_id: u64,
        check_run_id: u64,
        check_url: &str,
        check_actor_app_id: u64,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Candidate(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "candidate publication observation matched another operation kind",
            ));
        };
        let binding = self
            .delivery_binding
            .github()
            .ok_or_else(|| SupervisorError::invariant("GitHub delivery binding is missing"))?;
        if request.operation_id != operation_id
            || request.task_ordinal != task_ordinal
            || request.generation != generation
            || request.previous_head_sha != previous_head_sha
            || self.tasks[task_ordinal].state != TaskState::PublishingCandidate
            || self.tasks[task_ordinal].review_generation != generation
            || pr_number == 0
            || check_run_id == 0
            || pr_actor_bot_user_id != binding.developer_app.bot_user_id
            || check_actor_app_id != binding.architect_app.app_id
        {
            return Err(SupervisorError::invalid_identity(
                "candidate publication differs from the exact task/head/actor operation",
            ));
        }
        validate_git_sha("published GitHub previous head", previous_head_sha)?;
        validate_git_sha("published GitHub candidate head", head_sha)?;
        if head_sha == previous_head_sha {
            return Err(SupervisorError::invalid_event(
                "published GitHub candidate did not advance the head",
            ));
        }
        validate_identifier("GitHub Pull Request node ID", pr_node_id)?;
        validate_github_url("GitHub Pull Request URL", pr_url)?;
        validate_github_url("GitHub Check URL", check_url)?;
        {
            let state = self.github_state()?;
            if state.phase != GitHubDeliveryPhase::TasksRunning
                || state
                    .published_head_sha
                    .as_deref()
                    .unwrap_or(previous_head_sha)
                    != previous_head_sha
                || state.pr_number.is_some_and(|number| number != pr_number)
                || state
                    .pr_node_id
                    .as_deref()
                    .is_some_and(|node| node != pr_node_id)
                || state.pr_url.as_deref().is_some_and(|url| url != pr_url)
            {
                return Err(SupervisorError::invalid_identity(
                    "candidate publication conflicts with retained GitHub run evidence",
                ));
            }
        }
        let check = GitHubCheckSnapshot {
            check_run_id,
            check_url: check_url.into(),
            state: "in_progress".into(),
            head_sha: head_sha.into(),
        };
        let state = self.github_state_mut()?;
        state.published_head_sha = Some(head_sha.into());
        state.pr_number = Some(pr_number);
        state.pr_node_id = Some(pr_node_id.into());
        state.pr_url = Some(pr_url.into());
        state.current_check = Some(check.clone());
        let task = &mut self.tasks[task_ordinal];
        task.state = TaskState::Reviewing;
        task.github_check = Some(check);
        self.push_candidate_published(task_ordinal)?;
        self.start_reviewers(task_ordinal)
    }

    /// Open the task's worker runtime. There is no Git observation before it:
    /// the supervisor only sequences processes.
    fn schedule_runtime_open(
        &mut self,
        task_ordinal: usize,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_no_pending_operation()?;
        if self.tasks[task_ordinal].state != TaskState::Pending {
            return Err(SupervisorError::invalid_transition(
                "task runtime open requires a pending task",
            ));
        }
        self.pending_runtime_open = Some(task_ordinal);
        Ok(vec![SupervisorEffect::OpenTaskRuntime { task_ordinal }])
    }

    fn task_runtime_opened(
        &mut self,
        task_ordinal: usize,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        if self.pending_runtime_open != Some(task_ordinal) || self.runtime_open.is_some() {
            return Err(SupervisorError::invalid_transition(
                "task runtime opened without the exact pending open effect",
            ));
        }
        if self.tasks[task_ordinal].state != TaskState::Pending {
            return Err(SupervisorError::invalid_transition(
                "task runtime can only open for a pending task",
            ));
        }
        self.pending_runtime_open = None;
        self.runtime_open = Some(task_ordinal);
        self.tasks[task_ordinal].state = TaskState::Developing;
        self.schedule_session_open(task_ordinal, WorkerLane::Developer)
    }

    fn role_session_opened(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        session: RuntimeSessionKey,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let expected = self.pending_session_opens.get(&lane).ok_or_else(|| {
            SupervisorError::invalid_transition("role session opened without a pending effect")
        })?;
        if expected.task_ordinal != task_ordinal || expected.lane != lane {
            return Err(SupervisorError::invalid_identity(
                "role session open does not match the expected task and lane",
            ));
        }
        if self.runtime_open != Some(task_ordinal) {
            return Err(SupervisorError::invalid_transition(
                "role session requires the exact task runtime",
            ));
        }
        if self.used_sessions.contains(&session) {
            return Err(SupervisorError::invalid_identity(
                "logical runtime session key was reused",
            ));
        }
        match lane {
            WorkerLane::Developer => {
                let slot = &mut self.tasks[task_ordinal].developer_session;
                if slot.replace(session).is_some() {
                    return Err(SupervisorError::invalid_transition(
                        "Developer already owns a logical runtime session",
                    ));
                }
            }
            WorkerLane::Reviewer(reviewer_id) => {
                if self.tasks[task_ordinal]
                    .reviewer_sessions
                    .insert(reviewer_id, session)
                    .is_some()
                {
                    return Err(SupervisorError::invalid_transition(
                        "Reviewer lane already owns a logical runtime session",
                    ));
                }
            }
        }
        self.used_sessions.insert(session);
        self.pending_session_opens.remove(&lane);
        let purpose = match lane {
            WorkerLane::Developer => RuntimeTurnPurpose::InitialDevelopment,
            WorkerLane::Reviewer(_) => RuntimeTurnPurpose::InitialReview,
        };
        self.schedule_turn(task_ordinal, lane, purpose, session)
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_started(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        validate_identifier_with_bound(
            "completion token",
            &completion_token,
            MAX_COMPLETION_TOKEN_BYTES,
        )?;
        let expected = self.pending_turn_starts.get(&lane).ok_or_else(|| {
            SupervisorError::invalid_transition("turn started without a pending start effect")
        })?;
        if expected.task_ordinal != task_ordinal
            || expected.lane != lane
            || expected.review_generation != review_generation
            || expected.purpose != purpose
            || expected.session != session
        {
            return Err(SupervisorError::invalid_identity(
                "turn start does not match its exact task, lane, generation, purpose, and session",
            ));
        }
        if purpose.role() != lane.role() || self.session_for(task_ordinal, lane) != Some(session) {
            return Err(SupervisorError::invalid_identity(
                "turn purpose or logical session does not match the lane",
            ));
        }
        if self.used_turns.contains(&turn) {
            return Err(SupervisorError::invalid_identity(
                "logical runtime turn key was reused",
            ));
        }
        if self.accepted_completion_tokens.contains(&completion_token) {
            return Err(SupervisorError::duplicate(
                "completion token was already accepted",
            ));
        }
        if self.active_turns.contains_key(&lane)
            || (lane == WorkerLane::Developer && !self.active_turns.is_empty())
            || (lane.role() == WorkerRole::Reviewer
                && self.active_turns.contains_key(&WorkerLane::Developer))
        {
            return Err(SupervisorError::invalid_transition(
                "worker lane conflicts with an active turn",
            ));
        }
        self.pending_turn_starts.remove(&lane);
        self.used_turns.insert(turn);
        self.active_turns.insert(
            lane,
            CoreActiveTurn {
                task_ordinal,
                lane,
                review_generation,
                purpose,
                session,
                turn,
                completion_token,
            },
        );
        if lane.role() == WorkerRole::Reviewer
            && self.reviewer_ids().into_iter().all(|reviewer_id| {
                self.active_turns
                    .contains_key(&WorkerLane::Reviewer(reviewer_id))
            })
            && self.tasks[task_ordinal].review_requested_generation
                != Some(self.tasks[task_ordinal].review_generation)
        {
            self.tasks[task_ordinal].review_requested_generation =
                Some(self.tasks[task_ordinal].review_generation);
            self.push_review_requested(task_ordinal)?;
        }
        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_completed(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
        outcome: RuntimeOutcome,
        final_message_path: PathBuf,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let active = self.take_matching_active(
            task_ordinal,
            lane,
            review_generation,
            session,
            turn,
            completion_token,
        )?;
        outcome
            .validate()
            .map_err(|_| SupervisorError::invalid_event("typed runtime outcome is invalid"))?;
        if outcome.role() != lane.role() {
            return Err(SupervisorError::invalid_event(
                "typed runtime outcome role does not match the active turn",
            ));
        }
        let final_message_path = final_message_path
            .to_str()
            .ok_or_else(|| {
                SupervisorError::invalid_event("runtime final message path must be UTF-8")
            })?
            .to_owned();
        validate_absolute_path("runtime final message path", &final_message_path)?;
        self.accepted_completion_tokens
            .insert(active.completion_token);

        match outcome {
            RuntimeOutcome::Developer(developer) => match developer.status {
                // The developer's exit routes straight to review. The
                // supervisor inspects nothing about the work itself.
                DeveloperOutcomeStatus::Ready => {
                    if lane != WorkerLane::Developer {
                        return Err(SupervisorError::invalid_identity(
                            "Developer outcome arrived on a Reviewer lane",
                        ));
                    }
                    if self.tasks[task_ordinal].state != TaskState::Developing {
                        return Err(SupervisorError::invalid_transition(
                            "developer completion requires a developing task",
                        ));
                    }
                    if self.delivery_binding.is_github() {
                        self.publish_developer_candidate(task_ordinal, final_message_path)
                    } else {
                        self.begin_review_generation(task_ordinal, final_message_path)?;
                        self.start_reviewers(task_ordinal)
                    }
                }
                DeveloperOutcomeStatus::ClarificationRequired => self.await_architect_action(
                    task_ordinal,
                    ArchitectActionReason::Clarification,
                    final_message_path,
                ),
                DeveloperOutcomeStatus::Blocked => self.await_architect_action(
                    task_ordinal,
                    ArchitectActionReason::Blocker,
                    final_message_path,
                ),
            },
            RuntimeOutcome::Reviewer(reviewer) => {
                let reviewer_id = lane.reviewer_id().ok_or_else(|| {
                    SupervisorError::invalid_identity(
                        "Reviewer outcome arrived on the Developer lane",
                    )
                })?;
                let generation = review_generation.ok_or_else(|| {
                    SupervisorError::invalid_identity(
                        "Reviewer completion omitted its review generation",
                    )
                })?;
                if self.delivery_binding.is_github() {
                    self.publish_native_reviewer_result(
                        task_ordinal,
                        reviewer_id,
                        generation,
                        reviewer,
                        final_message_path,
                    )
                } else {
                    let effects = self.handle_reviewer_verdict(
                        task_ordinal,
                        reviewer_id,
                        generation,
                        reviewer,
                        final_message_path,
                    )?;
                    self.push_review_responded(task_ordinal, reviewer_id)?;
                    if matches!(
                        self.tasks[task_ordinal].state,
                        TaskState::Lgtm | TaskState::ReviewExhausted
                    ) {
                        self.push_task_completed(task_ordinal)?;
                    }
                    Ok(effects)
                }
            }
        }
    }

    fn await_architect_action(
        &mut self,
        task_ordinal: usize,
        reason: ArchitectActionReason,
        developer_request_path: String,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.tasks[task_ordinal].state != TaskState::Developing {
            return Err(SupervisorError::invalid_transition(
                "Developer action request requires a developing task",
            ));
        }
        if self.pending_architect_action.is_some() {
            return Err(SupervisorError::invariant(
                "a second Architect action cannot be pending",
            ));
        }
        let task_record_count = self.tasks[task_ordinal].clarification_records.len();
        let run_record_count = self
            .tasks
            .iter()
            .try_fold(0usize, |total, task| {
                total.checked_add(task.clarification_records.len())
            })
            .ok_or_else(|| SupervisorError::overflow("clarification record count overflow"))?;
        if task_record_count >= MAX_CLARIFICATION_RECORDS_PER_TASK
            || run_record_count >= MAX_CLARIFICATION_RECORDS_PER_RUN
        {
            self.tasks[task_ordinal].latest_developer_final_path = Some(developer_request_path);
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "clarification record capacity exhausted; cancel and approve a new run",
                Vec::new(),
            );
        }
        let task = &self.tasks[task_ordinal];
        let sequence = u32::try_from(task.clarification_records.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| SupervisorError::overflow("clarification sequence overflow"))?;
        let human_decision_required =
            task.clarification_rounds_used >= u32::from(task.spec.max_clarification_rounds);
        let clarification_output_path = self
            .project_root
            .join("hcom-tasks")
            .join(&self.run_id)
            .join(&task.spec.task_key)
            .join("clarification")
            .join(format!("turn-{sequence}.md"));
        let clarification_output_path = clarification_output_path
            .to_str()
            .ok_or_else(|| {
                SupervisorError::invalid_event("clarification output path must be UTF-8")
            })?
            .to_owned();
        validate_absolute_path("clarification output path", &clarification_output_path)?;
        let published_version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("session version overflow"))?;
        let pending = PendingArchitectActionSnapshot {
            task_ordinal: u32::try_from(task_ordinal)
                .map_err(|_| SupervisorError::overflow("task ordinal overflow"))?,
            task_key: task.spec.task_key.clone(),
            sequence,
            reason,
            developer_request_path: developer_request_path.clone(),
            clarification_output_path: clarification_output_path.clone(),
            clarification_rounds_used: task.clarification_rounds_used,
            max_clarification_rounds: task.spec.max_clarification_rounds,
            human_decision_required,
            published_version,
        };
        let task = &mut self.tasks[task_ordinal];
        task.latest_developer_final_path = Some(developer_request_path);
        task.state = TaskState::AwaitingArchitectAction;
        task.outcome_detail = Some(match reason {
            ArchitectActionReason::Clarification => {
                "Developer requested requirement clarification".into()
            }
            ArchitectActionReason::Blocker => {
                "Developer reported an evidenced external blocker".into()
            }
        });
        self.pending_architect_action = Some(pending);
        Ok(vec![SupervisorEffect::PrepareClarificationArtifact {
            task_ordinal,
            task_key: task.spec.task_key.clone(),
            sequence,
            path: PathBuf::from(clarification_output_path),
        }])
    }

    #[allow(clippy::too_many_arguments)]
    fn clarification_submitted(
        &mut self,
        task_ordinal: usize,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
        clarification_document_path: &str,
        human_decision_confirmed: bool,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_pending_architect_action(
            task_ordinal,
            task_key,
            action_sequence,
            developer_request_path,
        )?;
        let pending = self
            .pending_architect_action
            .as_ref()
            .expect("pending Architect action was just validated");
        if pending.clarification_output_path != clarification_document_path {
            return Err(SupervisorError::invalid_identity(
                "clarification document path does not match the pending action",
            ));
        }
        validate_absolute_path("clarification document path", clarification_document_path)?;
        if pending.human_decision_required != human_decision_confirmed {
            return Err(SupervisorError::invalid_transition(
                "human_decision_confirmed does not match the pending action mode",
            ));
        }

        let reason = pending.reason;
        let task = &mut self.tasks[task_ordinal];
        if !human_decision_confirmed {
            if task.clarification_rounds_used >= u32::from(task.spec.max_clarification_rounds) {
                return Err(SupervisorError::invalid_transition(
                    "Architect autonomous clarification budget is exhausted",
                ));
            }
            task.clarification_rounds_used = task
                .clarification_rounds_used
                .checked_add(1)
                .ok_or_else(|| SupervisorError::overflow("clarification round overflow"))?;
        }
        task.clarification_records.push(ClarificationRecord {
            sequence: action_sequence,
            reason,
            developer_request_path: developer_request_path.to_owned(),
            architect_clarification_path: clarification_document_path.to_owned(),
            human_decision_confirmed,
        });
        task.state = TaskState::Developing;
        task.outcome_detail = Some(if human_decision_confirmed {
            "human-confirmed clarification submitted; resuming Developer".into()
        } else {
            "Architect clarification submitted; resuming Developer".into()
        });
        self.pending_architect_action = None;
        let session = task
            .developer_session
            .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
        self.schedule_turn(
            task_ordinal,
            WorkerLane::Developer,
            RuntimeTurnPurpose::DeveloperClarificationResume,
            session,
        )
    }

    fn clarification_human_required(
        &mut self,
        task_ordinal: usize,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_pending_architect_action(
            task_ordinal,
            task_key,
            action_sequence,
            developer_request_path,
        )?;
        let pending = self
            .pending_architect_action
            .as_mut()
            .expect("pending Architect action was just validated");
        if pending.human_decision_required {
            return Err(SupervisorError::invalid_transition(
                "pending Architect action already requires a human decision",
            ));
        }
        pending.published_version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("session version overflow"))?;
        pending.human_decision_required = true;
        self.tasks[task_ordinal].outcome_detail =
            Some("Architect requested a human decision before Developer resume".into());
        Ok(Vec::new())
    }

    fn require_pending_architect_action(
        &self,
        task_ordinal: usize,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
    ) -> Result<(), SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let pending = self
            .pending_architect_action
            .as_ref()
            .ok_or_else(|| SupervisorError::invalid_transition("no Architect action is pending"))?;
        if self.tasks[task_ordinal].state != TaskState::AwaitingArchitectAction
            || usize::try_from(pending.task_ordinal).ok() != Some(task_ordinal)
            || pending.task_key != task_key
            || pending.sequence != action_sequence
            || pending.developer_request_path != developer_request_path
        {
            return Err(SupervisorError::invalid_identity(
                "Architect action identity does not match the pending task request",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_failed(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
        failure: SanitizedRuntimeFailure,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        validate_single_line(
            "runtime failure detail",
            &failure.detail,
            MAX_CORE_DIAGNOSTIC_BYTES,
        )?;
        let active = self.take_matching_active(
            task_ordinal,
            lane,
            review_generation,
            session,
            turn,
            completion_token,
        )?;
        self.accepted_completion_tokens
            .insert(active.completion_token);

        // The runtime already sanitized `failure.detail`; keeping it is what
        // makes a needs_human report actionable. Replacing it with a fixed
        // string would destroy the only evidence of what actually failed.
        let (session_state, task_state, label) = match failure.class {
            RuntimeFailureClass::Canceled => (
                SessionState::Failed,
                TaskState::Failed,
                "worker runtime canceled without a supervisor cancel request",
            ),
            RuntimeFailureClass::Protocol => (
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "worker runtime protocol failed",
            ),
            RuntimeFailureClass::Process => (
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "worker runtime process failed",
            ),
            RuntimeFailureClass::Timeout => (
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "worker runtime reported a timeout",
            ),
            RuntimeFailureClass::Contract => (
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "worker runtime contract failed",
            ),
        };
        let mut detail = if failure.detail.is_empty() {
            label.to_string()
        } else {
            format!("{label}: {}", failure.detail)
        };
        truncate_utf8(&mut detail, MAX_CORE_DIAGNOSTIC_BYTES);
        let peer_interrupts = self.interrupt_active_effects();
        self.terminalize_current(session_state, task_state, &detail, peer_interrupts)
    }

    fn driver_failed(
        &mut self,
        task_ordinal: usize,
        failure: DriverFailure,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        validate_single_line(
            "supervisor driver failure detail",
            &failure.detail,
            MAX_CORE_DIAGNOSTIC_BYTES,
        )?;
        let detail = match failure.class {
            DriverFailureClass::Repository => "repository observation failed",
            DriverFailureClass::Runtime => "task worker runtime operation failed",
            DriverFailureClass::Environment => "task-private environment setup failed",
            DriverFailureClass::Contract => "session runtime contract failed",
            DriverFailureClass::Cleanup => "task worker runtime cleanup failed",
        };
        self.terminalize_current(
            SessionState::NeedsHuman,
            TaskState::NeedsHuman,
            detail,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn timeout(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let active = self.take_matching_active(
            task_ordinal,
            lane,
            review_generation,
            session,
            turn,
            completion_token,
        )?;
        self.accepted_completion_tokens
            .insert(active.completion_token);
        let interrupt = SupervisorEffect::InterruptTurn {
            task_ordinal,
            lane,
            session,
            turn,
        };
        let mut interrupts = vec![interrupt];
        interrupts.extend(self.interrupt_active_effects());
        self.terminalize_current(
            SessionState::NeedsHuman,
            TaskState::NeedsHuman,
            "worker turn timed out",
            interrupts,
        )
    }

    fn github_terminal_request(
        &self,
        outcome: &str,
        detail: &str,
    ) -> Option<PublishGitHubTerminalRequest> {
        let state = self.github_state.as_ref()?;
        Some(PublishGitHubTerminalRequest {
            run_id: self.run_id.clone(),
            pr_number: state.pr_number?,
            head_sha: state.published_head_sha.clone()?,
            check_run_id: state.current_check.as_ref().map(|check| check.check_run_id),
            outcome: outcome.into(),
            detail: detail.into(),
        })
    }

    fn cancel(&mut self, reason: &str) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        validate_single_line("cancel reason", reason, 4096)?;
        let mut effects = self.interrupt_active_effects();
        if let Some(terminal) = self.github_terminal_request(
            "cancelled",
            "canceled by explicit Architect-session request",
        ) {
            effects.push(SupervisorEffect::PublishGitHubTerminal(terminal));
        }
        self.terminalize_current(
            SessionState::Canceled,
            TaskState::Canceled,
            "canceled by explicit Architect-session request",
            effects,
        )
    }

    fn parent_stopping(&mut self) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let mut effects = self.interrupt_active_effects();
        if let Some(terminal) =
            self.github_terminal_request("cancelled", "foreground Architect parent stopped")
        {
            effects.push(SupervisorEffect::PublishGitHubTerminal(terminal));
        }
        self.terminalize_current(
            SessionState::Canceled,
            TaskState::Canceled,
            "foreground Architect parent stopped",
            effects,
        )
    }

    fn begin_review_generation(
        &mut self,
        task_ordinal: usize,
        developer_final_path: String,
    ) -> Result<(), SupervisorError> {
        let reviewer_ids = self.reviewer_ids();
        let task = &mut self.tasks[task_ordinal];
        if task.review_round >= u32::from(task.spec.max_review_rounds) {
            return Err(SupervisorError::invariant(
                "Developer READY cannot allocate a review generation beyond the task maximum",
            ));
        }
        task.review_generation = task
            .review_round
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("review generation overflow"))?;
        for reviewer_id in reviewer_ids {
            if let Some(result) = task.reviewer_results.remove(&reviewer_id) {
                task.historical_reviewer_final_paths
                    .get_mut(&reviewer_id)
                    .expect("CoreTask initializes every active Reviewer history lane")
                    .extend(result.final_message_paths);
            }
        }
        task.latest_reviewer_final_paths.clear();
        task.github_reviews.clear();
        task.pending_native_reviews.clear();
        task.review_requested_generation = None;
        task.latest_developer_final_path = Some(developer_final_path);
        task.state = TaskState::Reviewing;
        task.outcome_detail = Some("Developer completed; routing to active Reviewers".into());
        Ok(())
    }

    /// Atomically schedule every active Reviewer lane for one generation.
    fn start_reviewers(
        &mut self,
        task_ordinal: usize,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let rereview = self.tasks[task_ordinal].review_round > 0;
        let reviewer_ids = self.reviewer_ids();
        let mut effects = Vec::with_capacity(reviewer_ids.len());
        for reviewer_id in reviewer_ids {
            let lane = WorkerLane::Reviewer(reviewer_id);
            if let Some(session) = self.tasks[task_ordinal]
                .reviewer_sessions
                .get(&reviewer_id)
                .copied()
            {
                effects.extend(self.schedule_turn(
                    task_ordinal,
                    lane,
                    if rereview {
                        RuntimeTurnPurpose::ReviewerRereview
                    } else {
                        RuntimeTurnPurpose::InitialReview
                    },
                    session,
                )?);
            } else {
                effects.extend(self.schedule_session_open(task_ordinal, lane)?);
            }
        }
        Ok(effects)
    }

    fn publish_native_reviewer_result(
        &mut self,
        task_ordinal: usize,
        reviewer_id: ReviewerId,
        generation: u32,
        outcome: ReviewerOutcomeV1,
        final_message_path: String,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let reviewer_ids = self.reviewer_ids();
        let task = &self.tasks[task_ordinal];
        if !reviewer_ids.contains(&reviewer_id)
            || !matches!(
                task.state,
                TaskState::Reviewing | TaskState::PublishingReview
            )
            || generation != task.review_generation
            || generation != task.review_round.saturating_add(1)
            || task.reviewer_results.contains_key(&reviewer_id)
            || task.pending_native_reviews.contains_key(&reviewer_id)
        {
            return Err(SupervisorError::invalid_identity(
                "native Reviewer result is stale, duplicate, or outside the current GitHub generation",
            ));
        }
        let mut paths = Vec::with_capacity(
            outcome
                .preceding_final_message_paths
                .len()
                .saturating_add(1),
        );
        for path in &outcome.preceding_final_message_paths {
            let path = path.to_str().ok_or_else(|| {
                SupervisorError::invalid_event(
                    "preceding reviewer final message path must be UTF-8",
                )
            })?;
            validate_absolute_path("preceding reviewer final message path", path)?;
            paths.push(path.to_owned());
        }
        paths.push(final_message_path.clone());
        let (task_key, task_title, task_base_sha) = {
            let task = &mut self.tasks[task_ordinal];
            task.pending_native_reviews.insert(
                reviewer_id,
                CorePendingNativeReview {
                    generation,
                    verdict: outcome.verdict,
                    final_message_paths: paths,
                },
            );
            task.state = TaskState::PublishingReview;
            (
                task.spec.task_key.clone(),
                task.spec.title.clone(),
                task.github_base_sha.clone().ok_or_else(|| {
                    SupervisorError::invariant("GitHub Reviewer task base is missing")
                })?,
            )
        };
        let (head_sha, pr_number) = {
            let state = self.github_state()?;
            (
                state.published_head_sha.clone().ok_or_else(|| {
                    SupervisorError::invariant("GitHub Reviewer published head is missing")
                })?,
                state.pr_number.ok_or_else(|| {
                    SupervisorError::invariant("GitHub Reviewer Pull Request is missing")
                })?,
            )
        };
        let request = PublishReviewerReviewRequest {
            operation_id: self.github_operation_id(
                "review",
                Some(task_ordinal),
                Some(generation),
                Some(reviewer_id),
            )?,
            run_id: self.run_id.clone(),
            task_ordinal,
            task_count: self.tasks.len(),
            task_key,
            task_title,
            generation,
            task_base_sha,
            head_sha,
            pr_number,
            reviewer_id,
            verdict: outcome.verdict,
            reviewer_final_path: final_message_path,
        };
        self.github_state_mut()?.pending_operations.insert(
            request.operation_id.clone(),
            PendingGitHubOperation::Review(request.clone()),
        );
        Ok(vec![SupervisorEffect::PublishReviewerReview(request)])
    }

    #[allow(clippy::too_many_arguments)]
    fn reviewer_review_published(
        &mut self,
        operation_id: &str,
        task_ordinal: usize,
        reviewer_id: ReviewerId,
        generation: u32,
        head_sha: &str,
        verdict: ReviewerVerdict,
        review_id: u64,
        review_url: &str,
        actor_bot_user_id: u64,
        final_artifact_sha256: &str,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Review(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "Reviewer publication observation matched another operation kind",
            ));
        };
        let expected_actor = self
            .delivery_binding
            .github()
            .and_then(|binding| {
                binding
                    .reviewer_apps
                    .iter()
                    .find(|binding| binding.reviewer_id == reviewer_id)
            })
            .map(|binding| binding.app.bot_user_id)
            .ok_or_else(|| SupervisorError::invariant("Reviewer App binding is missing"))?;
        if request.operation_id != operation_id
            || request.task_ordinal != task_ordinal
            || request.reviewer_id != reviewer_id
            || request.generation != generation
            || request.head_sha != head_sha
            || request.verdict != verdict
            || review_id == 0
            || actor_bot_user_id != expected_actor
        {
            return Err(SupervisorError::invalid_identity(
                "published Reviewer review differs from the exact lane/generation/head/actor",
            ));
        }
        validate_git_sha("published Reviewer head", head_sha)?;
        validate_github_url("GitHub review URL", review_url)?;
        validate_sha256("Reviewer final artifact SHA-256", final_artifact_sha256)?;
        let pending_native = self.tasks[task_ordinal]
            .pending_native_reviews
            .remove(&reviewer_id)
            .ok_or_else(|| {
                SupervisorError::invariant("published Reviewer review lost its native final")
            })?;
        if pending_native.generation != generation || pending_native.verdict != verdict {
            return Err(SupervisorError::invalid_identity(
                "published Reviewer review differs from its retained native result",
            ));
        }
        self.tasks[task_ordinal].reviewer_results.insert(
            reviewer_id,
            CoreReviewerResult {
                generation,
                verdict,
                final_message_paths: pending_native.final_message_paths,
            },
        );
        self.tasks[task_ordinal].github_reviews.insert(
            reviewer_id,
            GitHubReviewSnapshot {
                reviewer_id,
                generation,
                head_sha: head_sha.into(),
                verdict,
                review_id,
                review_url: review_url.into(),
                final_artifact_sha256: final_artifact_sha256.into(),
            },
        );
        self.push_review_responded(task_ordinal, reviewer_id)?;

        let reviewer_ids = self.reviewer_ids();
        if self.tasks[task_ordinal].reviewer_results.len() < reviewer_ids.len() {
            self.tasks[task_ordinal].state = TaskState::PublishingReview;
            return Ok(Vec::new());
        }
        if reviewer_ids.iter().any(|id| {
            self.tasks[task_ordinal]
                .reviewer_results
                .get(id)
                .is_none_or(|result| result.generation != generation)
                || self.tasks[task_ordinal]
                    .github_reviews
                    .get(id)
                    .is_none_or(|review| {
                        review.generation != generation || review.head_sha != head_sha
                    })
        }) {
            return Err(SupervisorError::invariant(
                "joined published Reviewer results do not share one exact generation and head",
            ));
        }
        {
            let task = &mut self.tasks[task_ordinal];
            task.review_round = generation;
            task.latest_reviewer_final_paths = reviewer_ids
                .iter()
                .flat_map(|id| {
                    task.reviewer_results
                        .get(id)
                        .expect("joined Reviewer result exists")
                        .final_message_paths
                        .clone()
                })
                .collect();
        }
        let all_lgtm = reviewer_ids.iter().all(|id| {
            self.tasks[task_ordinal].reviewer_results[id].verdict == ReviewerVerdict::Lgtm
        });
        let maximum = u32::from(self.tasks[task_ordinal].spec.max_review_rounds);
        let transition = if all_lgtm {
            PostCheckTransition::Lgtm
        } else if generation >= maximum {
            PostCheckTransition::ReviewExhausted
        } else {
            PostCheckTransition::Correction
        };
        self.schedule_review_check(task_ordinal, transition)
    }

    fn schedule_review_check(
        &mut self,
        task_ordinal: usize,
        transition: PostCheckTransition,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let task = &self.tasks[task_ordinal];
        let intended_outcome = match transition {
            PostCheckTransition::Lgtm => Some(TaskCompletionOutcome::Lgtm),
            PostCheckTransition::ReviewExhausted => Some(TaskCompletionOutcome::ReviewExhausted),
            PostCheckTransition::Correction => None,
        };
        let earlier_exhausted = self
            .tasks
            .iter()
            .take(task_ordinal)
            .any(|task| task.state == TaskState::ReviewExhausted);
        let conclusion = match transition {
            PostCheckTransition::Lgtm if !earlier_exhausted => GitHubReviewCheckConclusion::Success,
            _ => GitHubReviewCheckConclusion::ActionRequired,
        };
        let current_check = task.github_check.clone().ok_or_else(|| {
            SupervisorError::invariant("joined GitHub review has no in-progress Check")
        })?;
        let state = self.github_state()?;
        let request = PublishReviewCheckRequest {
            operation_id: self.github_operation_id(
                "check",
                Some(task_ordinal),
                Some(task.review_generation),
                None,
            )?,
            run_id: self.run_id.clone(),
            task_ordinal,
            task_count: self.tasks.len(),
            task_key: task.spec.task_key.clone(),
            task_title: task.spec.title.clone(),
            generation: task.review_generation,
            task_base_sha: task
                .github_base_sha
                .clone()
                .ok_or_else(|| SupervisorError::invariant("GitHub Check task base is missing"))?,
            head_sha: state.published_head_sha.clone().ok_or_else(|| {
                SupervisorError::invariant("GitHub Check published head is missing")
            })?,
            check_run_id: current_check.check_run_id,
            check_url: current_check.check_url,
            ruleset_attestation_sha256: self
                .github_run_binding
                .as_ref()
                .ok_or_else(|| SupervisorError::invariant("GitHub Check run binding is missing"))?
                .ruleset_attestation_sha256
                .clone(),
            conclusion,
            task_outcomes: self
                .task_outcome_evidence(intended_outcome.map(|outcome| (task_ordinal, outcome))),
        };
        self.github_state_mut()?.pending_operations.insert(
            request.operation_id.clone(),
            PendingGitHubOperation::Check {
                request: request.clone(),
                transition,
            },
        );
        self.tasks[task_ordinal].state = TaskState::PublishingReview;
        Ok(vec![SupervisorEffect::OpenOrUpdateReviewCheck(request)])
    }

    #[allow(clippy::too_many_arguments)]
    fn review_check_published(
        &mut self,
        operation_id: &str,
        task_ordinal: usize,
        generation: u32,
        head_sha: &str,
        check_run_id: u64,
        check_url: &str,
        state_value: &str,
        actor_app_id: u64,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Check {
            request,
            transition,
        } = pending
        else {
            return Err(SupervisorError::invalid_identity(
                "Check publication observation matched another operation kind",
            ));
        };
        let expected_architect = self
            .delivery_binding
            .github()
            .map(|binding| binding.architect_app.app_id)
            .ok_or_else(|| SupervisorError::invariant("Architect App binding is missing"))?;
        if request.operation_id != operation_id
            || request.task_ordinal != task_ordinal
            || request.generation != generation
            || request.head_sha != head_sha
            || request.check_run_id != check_run_id
            || request.check_url != check_url
            || request.conclusion.as_str() != state_value
            || actor_app_id != expected_architect
        {
            return Err(SupervisorError::invalid_identity(
                "published Check differs from the exact generation/head/conclusion/actor",
            ));
        }
        validate_github_url("GitHub Check URL", check_url)?;
        let check = GitHubCheckSnapshot {
            check_run_id,
            check_url: check_url.into(),
            state: state_value.into(),
            head_sha: head_sha.into(),
        };
        self.github_state_mut()?.current_check = Some(check.clone());
        self.tasks[task_ordinal].github_check = Some(check);
        match transition {
            PostCheckTransition::Correction => {
                let session = self.tasks[task_ordinal]
                    .developer_session
                    .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
                self.tasks[task_ordinal].state = TaskState::Developing;
                self.tasks[task_ordinal].outcome_detail =
                    Some("published same-head review requested changes".into());
                self.schedule_turn(
                    task_ordinal,
                    WorkerLane::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    session,
                )
            }
            PostCheckTransition::Lgtm => {
                let task = &mut self.tasks[task_ordinal];
                task.state = TaskState::Lgtm;
                task.github_final_head_sha = Some(head_sha.into());
                task.outcome_detail =
                    Some("all active same-generation GitHub reviews were published as LGTM".into());
                self.push_task_completed(task_ordinal)?;
                self.complete_current_task(task_ordinal)
            }
            PostCheckTransition::ReviewExhausted => {
                let task = &mut self.tasks[task_ordinal];
                task.state = TaskState::ReviewExhausted;
                task.github_final_head_sha = Some(head_sha.into());
                task.outcome_detail = Some(
                    "maximum published review generations exhausted; advancing by policy".into(),
                );
                self.push_task_completed(task_ordinal)?;
                self.complete_current_task(task_ordinal)
            }
        }
    }

    fn handle_reviewer_verdict(
        &mut self,
        task_ordinal: usize,
        reviewer_id: ReviewerId,
        generation: u32,
        outcome: ReviewerOutcomeV1,
        final_message_path: String,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let reviewer_ids = self.reviewer_ids();
        if !reviewer_ids.contains(&reviewer_id) {
            return Err(SupervisorError::invalid_identity(
                "Reviewer completion references an inactive lane",
            ));
        }
        let task = &self.tasks[task_ordinal];
        if task.state != TaskState::Reviewing {
            return Err(SupervisorError::invalid_transition(
                "a reviewer verdict requires a reviewing task",
            ));
        }
        if generation != task.review_generation
            || generation
                != task
                    .review_round
                    .checked_add(1)
                    .ok_or_else(|| SupervisorError::overflow("review generation overflow"))?
        {
            return Err(SupervisorError::invalid_identity(
                "Reviewer completion generation is stale or future",
            ));
        }
        if task.reviewer_results.contains_key(&reviewer_id) {
            return Err(SupervisorError::duplicate(
                "Reviewer already completed the current generation",
            ));
        }
        let mut paths = Vec::with_capacity(
            outcome
                .preceding_final_message_paths
                .len()
                .saturating_add(1),
        );
        for path in &outcome.preceding_final_message_paths {
            let path = path.to_str().ok_or_else(|| {
                SupervisorError::invalid_event(
                    "preceding reviewer final message path must be UTF-8",
                )
            })?;
            validate_absolute_path("preceding reviewer final message path", path)?;
            paths.push(path.to_owned());
        }
        paths.push(final_message_path);
        self.tasks[task_ordinal].reviewer_results.insert(
            reviewer_id,
            CoreReviewerResult {
                generation,
                verdict: outcome.verdict,
                final_message_paths: paths,
            },
        );
        if self.tasks[task_ordinal].reviewer_results.len() < reviewer_ids.len() {
            return Ok(Vec::new());
        }
        if reviewer_ids.iter().any(|id| {
            self.tasks[task_ordinal]
                .reviewer_results
                .get(id)
                .is_none_or(|result| result.generation != generation)
        }) {
            return Err(SupervisorError::invariant(
                "joined Reviewer results do not share the current generation",
            ));
        }
        {
            let task = &mut self.tasks[task_ordinal];
            task.review_round = generation;
            task.latest_reviewer_final_paths = reviewer_ids
                .iter()
                .flat_map(|id| {
                    task.reviewer_results
                        .get(id)
                        .expect("joined Reviewer result exists")
                        .final_message_paths
                        .clone()
                })
                .collect();
        }
        let all_lgtm = reviewer_ids.iter().all(|id| {
            self.tasks[task_ordinal].reviewer_results[id].verdict == ReviewerVerdict::Lgtm
        });
        if all_lgtm {
            let task = &mut self.tasks[task_ordinal];
            task.state = TaskState::Lgtm;
            task.outcome_detail = Some("all active same-generation Reviewers returned LGTM".into());
            self.complete_current_task(task_ordinal)
        } else if generation >= u32::from(self.tasks[task_ordinal].spec.max_review_rounds) {
            let task = &mut self.tasks[task_ordinal];
            task.state = TaskState::ReviewExhausted;
            task.outcome_detail = Some(
                "maximum synchronized review generations exhausted; advancing by policy".into(),
            );
            self.complete_current_task(task_ordinal)
        } else {
            let session = self.tasks[task_ordinal]
                .developer_session
                .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
            let task = &mut self.tasks[task_ordinal];
            task.state = TaskState::Developing;
            task.outcome_detail = Some("at least one Reviewer requested changes".into());
            self.schedule_turn(
                task_ordinal,
                WorkerLane::Developer,
                RuntimeTurnPurpose::DeveloperCorrection,
                session,
            )
        }
    }

    fn complete_current_task(
        &mut self,
        task_ordinal: usize,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.runtime_open != Some(task_ordinal) {
            return Err(SupervisorError::invariant(
                "completed task lost its task-local runtime",
            ));
        }
        self.runtime_open = None;
        self.pending_session_opens.clear();
        self.pending_turn_starts.clear();
        self.active_turns.clear();

        let mut effects = vec![SupervisorEffect::CloseTaskRuntime { task_ordinal }];
        if task_ordinal + 1 < self.tasks.len() {
            let next = task_ordinal + 1;
            self.current_task = Some(next);
            effects.extend(self.schedule_runtime_open(next)?);
        } else if self.delivery_binding.is_github() {
            if self
                .tasks
                .iter()
                .any(|task| task.state == TaskState::ReviewExhausted)
            {
                let state = self.github_state_mut()?;
                state.phase = GitHubDeliveryPhase::PreservedUnmerged;
                state.outcome = Some(GitHubDeliveryOutcome::UnmergedReviewExhausted);
                self.session_state = SessionState::Completed;
                self.terminal_detail = Some(
                    "ordered tasks completed with review exhaustion; GitHub Pull Request preserved unmerged"
                        .into(),
                );
                effects.push(SupervisorEffect::FinishSession {
                    state: SessionState::Completed,
                    detail: self
                        .terminal_detail
                        .clone()
                        .expect("completed session has terminal detail"),
                });
            } else {
                let state = self.github_state()?;
                let final_head_sha = state.published_head_sha.clone().ok_or_else(|| {
                    SupervisorError::invariant("all-LGTM GitHub run has no published final head")
                })?;
                let pr_number = state.pr_number.ok_or_else(|| {
                    SupervisorError::invariant("all-LGTM GitHub run has no Pull Request")
                })?;
                let check = state.current_check.clone().ok_or_else(|| {
                    SupervisorError::invariant("all-LGTM GitHub run has no final Check")
                })?;
                if check.state != GitHubReviewCheckConclusion::Success.as_str()
                    || check.head_sha != final_head_sha
                {
                    return Err(SupervisorError::invariant(
                        "all-LGTM GitHub run lacks a successful exact-final-head Check",
                    ));
                }
                let request = WaitForMergeGateRequest {
                    operation_id: self.github_operation_id("merge_gate", None, None, None)?,
                    run_id: self.run_id.clone(),
                    pr_number,
                    final_head_sha: final_head_sha.clone(),
                    check_run_id: check.check_run_id,
                    ruleset_attestation_sha256: self
                        .github_run_binding
                        .as_ref()
                        .ok_or_else(|| SupervisorError::invariant("GitHub run binding is missing"))?
                        .ruleset_attestation_sha256
                        .clone(),
                    tasks: self.task_outcome_evidence(None),
                };
                let state = self.github_state_mut()?;
                state.phase = GitHubDeliveryPhase::AwaitingMerge;
                state.pending_operations.insert(
                    request.operation_id.clone(),
                    PendingGitHubOperation::MergeGate(request.clone()),
                );
                self.push_merge_waiting(&request, &check)?;
                effects.push(SupervisorEffect::WaitForMergeGate(request));
            }
        } else {
            self.session_state = SessionState::Completed;
            self.terminal_detail =
                Some("all ordered tasks reached a terminal review outcome".into());
            effects.push(SupervisorEffect::FinishSession {
                state: SessionState::Completed,
                detail: self
                    .terminal_detail
                    .clone()
                    .expect("completed session has terminal detail"),
            });
        }
        Ok(effects)
    }

    fn merge_gate_observed(
        &mut self,
        operation_id: &str,
        pr_number: u64,
        final_head_sha: &str,
        base_sha: &str,
        ruleset_attestation_sha256: &str,
        check_run_id: u64,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::MergeGate(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "merge-gate observation matched another operation kind",
            ));
        };
        let run = self
            .github_run_binding
            .as_ref()
            .ok_or_else(|| SupervisorError::invariant("GitHub run binding is missing"))?;
        if request.operation_id != operation_id
            || request.pr_number != pr_number
            || request.final_head_sha != final_head_sha
            || request.check_run_id != check_run_id
            || run.expected_base_sha != base_sha
            || run.ruleset_attestation_sha256 != ruleset_attestation_sha256
            || self.github_state()?.phase != GitHubDeliveryPhase::AwaitingMerge
            || self.tasks.iter().any(|task| task.state != TaskState::Lgtm)
        {
            return Err(SupervisorError::invalid_identity(
                "merge-gate observation differs from the exact all-task LGTM binding",
            ));
        }
        validate_git_sha("merge-gate final head", final_head_sha)?;
        validate_sha256("merge-gate ruleset attestation", ruleset_attestation_sha256)?;
        let merge = MergePullRequestRequest {
            operation_id: self.github_operation_id("merge", None, None, None)?,
            run_id: self.run_id.clone(),
            pr_number,
            final_head_sha: final_head_sha.into(),
            base_sha: base_sha.into(),
            ruleset_attestation_sha256: ruleset_attestation_sha256.into(),
            check_run_id,
            tasks: request.tasks,
        };
        let state = self.github_state_mut()?;
        state.final_base_sha = Some(base_sha.into());
        state.final_ruleset_attestation_sha256 = Some(ruleset_attestation_sha256.into());
        state.pending_operations.insert(
            merge.operation_id.clone(),
            PendingGitHubOperation::Merge(merge.clone()),
        );
        Ok(vec![SupervisorEffect::MergePullRequest(merge)])
    }

    #[allow(clippy::too_many_arguments)]
    fn pull_request_merged(
        &mut self,
        operation_id: &str,
        pr_number: u64,
        final_head_sha: &str,
        merge_sha: &str,
        merge_url: &str,
        actor_bot_user_id: u64,
        merge_evidence_durable: bool,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Merge(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "merge observation matched another operation kind",
            ));
        };
        let expected_architect = self
            .delivery_binding
            .github()
            .map(|binding| binding.architect_app.bot_user_id)
            .ok_or_else(|| SupervisorError::invariant("Architect App binding is missing"))?;
        if request.operation_id != operation_id
            || request.pr_number != pr_number
            || request.final_head_sha != final_head_sha
            || actor_bot_user_id != expected_architect
            || !merge_evidence_durable
            || self.github_state()?.phase != GitHubDeliveryPhase::AwaitingMerge
        {
            return Err(SupervisorError::invalid_identity(
                "confirmed merge differs from the exact final head, actor, or durable evidence",
            ));
        }
        validate_git_sha("GitHub final head", final_head_sha)?;
        validate_git_sha("GitHub merge SHA", merge_sha)?;
        validate_github_url("GitHub merge URL", merge_url)?;
        let finalize = FinalizeGitHubRunRequest {
            operation_id: self.github_operation_id("finalize", None, None, None)?,
            run_id: self.run_id.clone(),
            pr_number,
            final_head_sha: final_head_sha.into(),
            merge_sha: merge_sha.into(),
        };
        let state = self.github_state_mut()?;
        state.phase = GitHubDeliveryPhase::Finalizing;
        state.merge_sha = Some(merge_sha.into());
        state.merge_url = Some(merge_url.into());
        state.pending_operations.insert(
            finalize.operation_id.clone(),
            PendingGitHubOperation::Finalize(finalize.clone()),
        );
        self.push_run_finalizing(&finalize)?;
        Ok(vec![SupervisorEffect::FinalizeGitHubRun(finalize)])
    }

    fn github_run_finalized(
        &mut self,
        operation_id: &str,
        pr_number: u64,
        final_head_sha: &str,
        merge_sha: &str,
        finalization: GitHubFinalizationSnapshot,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let pending = self.take_github_operation(operation_id)?;
        let PendingGitHubOperation::Finalize(request) = pending else {
            return Err(SupervisorError::invalid_identity(
                "finalization observation matched another operation kind",
            ));
        };
        if request.operation_id != operation_id
            || request.pr_number != pr_number
            || request.final_head_sha != final_head_sha
            || request.merge_sha != merge_sha
            || !finalization.local_worktree_removed
            || !finalization.local_ref_removed
            || self.github_state()?.phase != GitHubDeliveryPhase::Finalizing
            || self.runtime_open.is_some()
            || !self.active_turns.is_empty()
        {
            return Err(SupervisorError::invalid_identity(
                "GitHub finalization did not prove required exact-run cleanup",
            ));
        }
        let state = self.github_state_mut()?;
        state.phase = GitHubDeliveryPhase::Delivered;
        state.outcome = Some(GitHubDeliveryOutcome::Delivered);
        state.finalization = Some(finalization);
        self.session_state = SessionState::Completed;
        self.terminal_detail =
            Some("GitHub Pull Request exact-head merge and required finalization completed".into());
        Ok(vec![SupervisorEffect::FinishSession {
            state: SessionState::Completed,
            detail: self
                .terminal_detail
                .clone()
                .expect("delivered session has terminal detail"),
        }])
    }

    fn github_operation_failed(
        &mut self,
        operation_id: &str,
        detail: &str,
        partial_operation: Option<GitHubPartialOperation>,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        validate_single_line(
            "GitHub operation failure",
            detail,
            MAX_CORE_DIAGNOSTIC_BYTES,
        )?;
        let pending = self.take_github_operation(operation_id)?;
        if pending.operation_id() != operation_id {
            return Err(SupervisorError::invalid_identity(
                "GitHub failure identity differs from the pending operation",
            ));
        }
        if let Some(partial) = partial_operation {
            self.retain_partial_github_operation(&pending, partial)?;
        }
        let mut terminal_detail = format!("GitHub operation {operation_id} failed: {detail}");
        truncate_utf8(&mut terminal_detail, MAX_CORE_DIAGNOSTIC_BYTES);
        let mut effects = self.interrupt_active_effects();
        if let Some(terminal) = self.github_terminal_request("needs_human", &terminal_detail) {
            effects.push(SupervisorEffect::PublishGitHubTerminal(terminal));
        }
        self.terminalize_current(
            SessionState::NeedsHuman,
            TaskState::NeedsHuman,
            &terminal_detail,
            effects,
        )
    }

    fn retain_partial_github_operation(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialOperation,
    ) -> Result<(), SupervisorError> {
        match partial {
            GitHubPartialOperation::RepositoryPreparation(partial) => {
                self.retain_partial_repository_preparation(pending, partial)
            }
            GitHubPartialOperation::Candidate(partial) => {
                self.retain_partial_candidate_publication(pending, partial)
            }
            GitHubPartialOperation::ReviewerReview(partial) => {
                self.retain_partial_reviewer_review(pending, partial)
            }
            GitHubPartialOperation::ReviewCheck(partial) => {
                self.retain_partial_review_check(pending, partial)
            }
            GitHubPartialOperation::Merge(partial) => self.retain_partial_merge(pending, partial),
            GitHubPartialOperation::Finalization(partial) => {
                self.retain_partial_finalization(pending, partial)
            }
        }
    }

    fn retain_partial_repository_preparation(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialRepositoryPreparation,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Prepare(request) = pending else {
            return Err(SupervisorError::invalid_event(
                "partial repository preparation accompanied another GitHub operation kind",
            ));
        };
        validate_git_sha("partial GitHub preparation base SHA", &partial.base_sha)?;
        validate_absolute_path(
            "partial GitHub preparation worktree path",
            &partial.worktree_path,
        )?;
        let expected_worktree = self
            .project_root
            .join("hcom-tasks")
            .join(&self.run_id)
            .join("repository");
        if request.run_binding.expected_base_sha != partial.base_sha
            || request.run_binding.generated_run_branch != partial.branch
            || expected_worktree.to_str() != Some(partial.worktree_path.as_str())
            || (partial.local_branch_created && !partial.local_base_ref_created)
            || (partial.local_worktree_created && !partial.local_branch_created)
            || (!partial.local_base_ref_created
                && !partial.local_branch_created
                && !partial.local_worktree_created)
            || self.github_state()?.phase != GitHubDeliveryPhase::PreparingRepository
        {
            return Err(SupervisorError::invalid_identity(
                "partial repository preparation differs from the exact approved run binding",
            ));
        }
        let state = self.github_state_mut()?;
        state.local_base_ref_present = partial.local_base_ref_created;
        state.local_branch_present = partial.local_branch_created;
        state.worktree_path = partial
            .local_worktree_created
            .then_some(partial.worktree_path);
        Ok(())
    }

    fn retain_partial_candidate_publication(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialCandidatePublication,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Candidate(request) = pending else {
            return Err(SupervisorError::invalid_event(
                "partial candidate evidence accompanied another GitHub operation kind",
            ));
        };
        if request.task_ordinal != partial.task_ordinal
            || request.generation != partial.generation
            || request.previous_head_sha != partial.previous_head_sha
            || self.tasks[partial.task_ordinal].state != TaskState::PublishingCandidate
        {
            return Err(SupervisorError::invalid_identity(
                "partial candidate evidence differs from the pending publication",
            ));
        }
        validate_git_sha(
            "partial GitHub candidate previous head",
            &partial.previous_head_sha,
        )?;
        validate_git_sha("partial GitHub candidate head", &partial.head_sha)?;
        if partial.head_sha == partial.previous_head_sha {
            return Err(SupervisorError::invalid_event(
                "partial GitHub candidate did not advance the published head",
            ));
        }

        let binding = self
            .delivery_binding
            .github()
            .ok_or_else(|| SupervisorError::invariant("GitHub delivery binding is missing"))?;
        let pr = match (
            partial.pr_number,
            partial.pr_node_id.as_deref(),
            partial.pr_url.as_deref(),
            partial.pr_actor_bot_user_id,
        ) {
            (None, None, None, None) => None,
            (Some(number), Some(node_id), Some(url), Some(actor))
                if number > 0 && actor == binding.developer_app.bot_user_id =>
            {
                validate_identifier("partial GitHub Pull Request node ID", node_id)?;
                validate_github_url("partial GitHub Pull Request URL", url)?;
                Some((number, node_id.to_owned(), url.to_owned()))
            }
            _ => {
                return Err(SupervisorError::invalid_identity(
                    "partial GitHub Pull Request coordinates are incomplete or actor-mismatched",
                ));
            }
        };
        let check = match (
            partial.check_run_id,
            partial.check_url.as_deref(),
            partial.check_actor_app_id,
        ) {
            (None, None, None) => None,
            (Some(id), Some(url), Some(actor))
                if id > 0 && actor == binding.architect_app.app_id && pr.is_some() =>
            {
                validate_github_url("partial GitHub Check URL", url)?;
                Some(GitHubCheckSnapshot {
                    check_run_id: id,
                    check_url: url.into(),
                    state: "in_progress".into(),
                    head_sha: partial.head_sha.clone(),
                })
            }
            _ => {
                return Err(SupervisorError::invalid_identity(
                    "partial GitHub Check coordinates are incomplete or actor-mismatched",
                ));
            }
        };
        let state = self.github_state_mut()?;
        if state
            .published_head_sha
            .as_deref()
            .is_some_and(|head| head != partial.previous_head_sha)
        {
            return Err(SupervisorError::invalid_identity(
                "partial GitHub candidate conflicts with the retained published head",
            ));
        }
        state.published_head_sha = Some(partial.head_sha);
        if let Some((number, node_id, url)) = pr {
            if state.pr_number.is_some_and(|retained| retained != number)
                || state
                    .pr_node_id
                    .as_deref()
                    .is_some_and(|retained| retained != node_id)
                || state
                    .pr_url
                    .as_deref()
                    .is_some_and(|retained| retained != url)
            {
                return Err(SupervisorError::invalid_identity(
                    "partial GitHub Pull Request conflicts with retained run evidence",
                ));
            }
            state.pr_number = Some(number);
            state.pr_node_id = Some(node_id);
            state.pr_url = Some(url);
        }
        state.current_check = check.clone();
        self.tasks[partial.task_ordinal].github_check = check;
        Ok(())
    }

    fn retain_partial_reviewer_review(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialReviewerReview,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Review(request) = pending else {
            return Err(SupervisorError::invalid_event(
                "partial Reviewer evidence accompanied another GitHub operation kind",
            ));
        };
        self.require_running_task(partial.task_ordinal)?;
        let expected_actor = self
            .delivery_binding
            .github()
            .and_then(|binding| {
                binding
                    .reviewer_apps
                    .iter()
                    .find(|binding| binding.reviewer_id == partial.reviewer_id)
            })
            .map(|binding| binding.app.bot_user_id)
            .ok_or_else(|| SupervisorError::invariant("Reviewer App binding is missing"))?;
        if request.task_ordinal != partial.task_ordinal
            || request.reviewer_id != partial.reviewer_id
            || request.generation != partial.generation
            || request.head_sha != partial.head_sha
            || request.verdict != partial.verdict
            || partial.review_id == 0
            || partial.actor_bot_user_id != expected_actor
        {
            return Err(SupervisorError::invalid_identity(
                "partial Reviewer review differs from the pending lane/generation/head/actor",
            ));
        }
        validate_git_sha("partial Reviewer head", &partial.head_sha)?;
        validate_github_url("partial GitHub review URL", &partial.review_url)?;
        validate_sha256(
            "partial Reviewer final artifact SHA-256",
            &partial.final_artifact_sha256,
        )?;
        let pending_native = self.tasks[partial.task_ordinal]
            .pending_native_reviews
            .remove(&partial.reviewer_id)
            .ok_or_else(|| {
                SupervisorError::invariant("partial Reviewer review lost its native final")
            })?;
        if pending_native.generation != partial.generation
            || pending_native.verdict != partial.verdict
        {
            return Err(SupervisorError::invalid_identity(
                "partial Reviewer review differs from its retained native result",
            ));
        }
        self.tasks[partial.task_ordinal].reviewer_results.insert(
            partial.reviewer_id,
            CoreReviewerResult {
                generation: partial.generation,
                verdict: partial.verdict,
                final_message_paths: pending_native.final_message_paths,
            },
        );
        self.tasks[partial.task_ordinal].github_reviews.insert(
            partial.reviewer_id,
            GitHubReviewSnapshot {
                reviewer_id: partial.reviewer_id,
                generation: partial.generation,
                head_sha: partial.head_sha,
                verdict: partial.verdict,
                review_id: partial.review_id,
                review_url: partial.review_url,
                final_artifact_sha256: partial.final_artifact_sha256,
            },
        );
        self.push_review_responded(partial.task_ordinal, partial.reviewer_id)
    }

    fn retain_partial_review_check(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialReviewCheck,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Check { request, .. } = pending else {
            return Err(SupervisorError::invalid_event(
                "partial Check evidence accompanied another GitHub operation kind",
            ));
        };
        self.require_running_task(partial.task_ordinal)?;
        let expected_architect = self
            .delivery_binding
            .github()
            .map(|binding| binding.architect_app.app_id)
            .ok_or_else(|| SupervisorError::invariant("Architect App binding is missing"))?;
        if request.task_ordinal != partial.task_ordinal
            || request.generation != partial.generation
            || request.head_sha != partial.head_sha
            || request.check_run_id != partial.check_run_id
            || request.check_url != partial.check_url
            || request.conclusion.as_str() != partial.state
            || partial.actor_app_id != expected_architect
        {
            return Err(SupervisorError::invalid_identity(
                "partial Check differs from the pending generation/head/conclusion/actor",
            ));
        }
        validate_github_url("partial GitHub Check URL", &partial.check_url)?;
        let check = GitHubCheckSnapshot {
            check_run_id: partial.check_run_id,
            check_url: partial.check_url,
            state: partial.state,
            head_sha: partial.head_sha,
        };
        self.github_state_mut()?.current_check = Some(check.clone());
        self.tasks[partial.task_ordinal].github_check = Some(check);
        Ok(())
    }

    fn retain_partial_merge(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialMerge,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Merge(request) = pending else {
            return Err(SupervisorError::invalid_event(
                "partial merge evidence accompanied another GitHub operation kind",
            ));
        };
        let expected_architect = self
            .delivery_binding
            .github()
            .map(|binding| binding.architect_app.bot_user_id)
            .ok_or_else(|| SupervisorError::invariant("Architect App binding is missing"))?;
        if request.pr_number != partial.pr_number
            || request.final_head_sha != partial.final_head_sha
            || partial.actor_bot_user_id != expected_architect
            || self.github_state()?.phase != GitHubDeliveryPhase::AwaitingMerge
        {
            return Err(SupervisorError::invalid_identity(
                "partial merge differs from the pending exact final head or actor",
            ));
        }
        validate_git_sha("partial GitHub final head", &partial.final_head_sha)?;
        validate_git_sha("partial GitHub merge SHA", &partial.merge_sha)?;
        validate_github_url("partial GitHub merge URL", &partial.merge_url)?;
        let state = self.github_state_mut()?;
        state.phase = GitHubDeliveryPhase::Finalizing;
        state.merge_sha = Some(partial.merge_sha);
        state.merge_url = Some(partial.merge_url);
        Ok(())
    }

    fn retain_partial_finalization(
        &mut self,
        pending: &PendingGitHubOperation,
        partial: GitHubPartialFinalization,
    ) -> Result<(), SupervisorError> {
        let PendingGitHubOperation::Finalize(request) = pending else {
            return Err(SupervisorError::invalid_event(
                "partial finalization evidence accompanied another GitHub operation kind",
            ));
        };
        if request.pr_number != partial.pr_number
            || request.final_head_sha != partial.final_head_sha
            || request.merge_sha != partial.merge_sha
            || (partial.finalization.local_ref_removed
                && !partial.finalization.local_worktree_removed)
            || partial
                .finalization
                .remote_ref_outcome
                .as_deref()
                .is_some_and(|outcome| {
                    !matches!(
                        outcome,
                        "deleted"
                            | "already_absent"
                            | "preserved_by_policy"
                            | "preserved_after_delete_failure"
                            | "preserved_after_mutation"
                    )
                })
            || (!partial.finalization.local_worktree_removed
                && !partial.finalization.local_ref_removed
                && partial.finalization.remote_ref_outcome.as_deref()
                    != Some("preserved_after_mutation"))
            || self.github_state()?.phase != GitHubDeliveryPhase::Finalizing
            || self.github_state()?.merge_sha.as_deref() != Some(partial.merge_sha.as_str())
        {
            return Err(SupervisorError::invalid_identity(
                "partial finalization differs from the confirmed merge binding",
            ));
        }
        self.github_state_mut()?.finalization = Some(partial.finalization);
        Ok(())
    }

    fn next_progress_sequence(&self) -> Result<u32, SupervisorError> {
        if self.progress_events.len() >= MAX_PROGRESS_EVENTS_PER_RUN {
            return Err(SupervisorError::invariant(
                "run progress event capacity was exceeded",
            ));
        }
        u32::try_from(self.progress_events.len())
            .ok()
            .and_then(|sequence| sequence.checked_add(1))
            .ok_or_else(|| SupervisorError::overflow("progress event sequence overflow"))
    }

    fn progress_task_counts(&self) -> Result<(u32, u32), SupervisorError> {
        let completed = self
            .tasks
            .iter()
            .filter(|task| matches!(task.state, TaskState::Lgtm | TaskState::ReviewExhausted))
            .count();
        let completed = u32::try_from(completed)
            .map_err(|_| SupervisorError::overflow("completed task count overflow"))?;
        let total = u32::try_from(self.tasks.len())
            .map_err(|_| SupervisorError::overflow("total task count overflow"))?;
        Ok((completed, total))
    }

    fn progress_task_ordinal(&self, task_ordinal: usize) -> Result<u32, SupervisorError> {
        u32::try_from(task_ordinal)
            .map_err(|_| SupervisorError::overflow("progress task ordinal overflow"))
    }

    fn github_task_progress(
        &self,
        task_ordinal: usize,
    ) -> Result<GitHubTaskProgressSnapshot, SupervisorError> {
        let task = self
            .tasks
            .get(task_ordinal)
            .ok_or_else(|| SupervisorError::invariant("GitHub progress task is missing"))?;
        let state = self.github_state()?;
        let check = task.github_check.as_ref().or(state.current_check.as_ref());
        Ok(GitHubTaskProgressSnapshot {
            pr_number: state
                .pr_number
                .ok_or_else(|| SupervisorError::invariant("GitHub progress PR is missing"))?,
            pr_url: state
                .pr_url
                .clone()
                .ok_or_else(|| SupervisorError::invariant("GitHub progress PR URL is missing"))?,
            task_base_sha: task.github_base_sha.clone().ok_or_else(|| {
                SupervisorError::invariant("GitHub progress task base is missing")
            })?,
            published_head_sha: state.published_head_sha.clone().ok_or_else(|| {
                SupervisorError::invariant("GitHub progress published head is missing")
            })?,
            check_run_id: check.map(|check| check.check_run_id),
            check_url: check.map(|check| check.check_url.clone()),
        })
    }

    fn push_candidate_published(&mut self, task_ordinal: usize) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let task_ordinal_value = self.progress_task_ordinal(task_ordinal)?;
        let task = self
            .tasks
            .get(task_ordinal)
            .ok_or_else(|| SupervisorError::invariant("candidate progress task is missing"))?;
        let developer_final_path = task.latest_developer_final_path.clone().ok_or_else(|| {
            SupervisorError::invariant("candidate progress lacks a Developer final path")
        })?;
        let github = self.github_task_progress(task_ordinal)?;
        self.progress_events
            .push(SessionProgressEvent::CandidatePublished {
                sequence,
                task_ordinal: task_ordinal_value,
                task_key: task.spec.task_key.clone(),
                completed_tasks,
                total_tasks,
                review_generation: task.review_generation,
                developer_final_path,
                github,
            });
        Ok(())
    }

    fn push_merge_waiting(
        &mut self,
        request: &WaitForMergeGateRequest,
        check: &GitHubCheckSnapshot,
    ) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let pr_url =
            self.github_state()?.pr_url.clone().ok_or_else(|| {
                SupervisorError::invariant("merge-wait progress PR URL is missing")
            })?;
        self.progress_events
            .push(SessionProgressEvent::MergeWaiting {
                sequence,
                completed_tasks,
                total_tasks,
                pr_number: request.pr_number,
                pr_url,
                final_head_sha: request.final_head_sha.clone(),
                check_run_id: check.check_run_id,
                check_url: check.check_url.clone(),
            });
        Ok(())
    }

    fn push_run_finalizing(
        &mut self,
        request: &FinalizeGitHubRunRequest,
    ) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let pr_url =
            self.github_state()?.pr_url.clone().ok_or_else(|| {
                SupervisorError::invariant("finalizing progress PR URL is missing")
            })?;
        self.progress_events
            .push(SessionProgressEvent::RunFinalizing {
                sequence,
                completed_tasks,
                total_tasks,
                pr_number: request.pr_number,
                pr_url,
                final_head_sha: request.final_head_sha.clone(),
                merge_sha: request.merge_sha.clone(),
            });
        Ok(())
    }

    fn push_review_requested(&mut self, task_ordinal: usize) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let task_ordinal_value = self.progress_task_ordinal(task_ordinal)?;
        let task = self
            .tasks
            .get(task_ordinal)
            .ok_or_else(|| SupervisorError::invariant("review-request progress task is missing"))?;
        let developer_final_path = task.latest_developer_final_path.clone().ok_or_else(|| {
            SupervisorError::invariant("review-request progress lacks a Developer final path")
        })?;
        let clarification_record_count =
            u32::try_from(task.clarification_records.len()).map_err(|_| {
                SupervisorError::overflow("review-request clarification count overflow")
            })?;
        self.progress_events
            .push(SessionProgressEvent::ReviewRequested {
                sequence,
                task_ordinal: task_ordinal_value,
                task_key: task.spec.task_key.clone(),
                completed_tasks,
                total_tasks,
                review_round: task.review_round,
                review_generation: task.review_generation,
                max_review_rounds: task.spec.max_review_rounds,
                developer_final_path,
                task_document_path: task.spec.task_document_path.clone(),
                design_document_paths: task.spec.design_document_paths.clone(),
                task_selector: task.spec.task_selector.clone(),
                clarification_record_count,
                reviewer_bindings: self.reviewer_bindings.clone(),
                github: self
                    .delivery_binding
                    .is_github()
                    .then(|| self.github_task_progress(task_ordinal))
                    .transpose()?,
            });
        Ok(())
    }

    fn push_review_responded(
        &mut self,
        task_ordinal: usize,
        reviewer_id: ReviewerId,
    ) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let task_ordinal_value = self.progress_task_ordinal(task_ordinal)?;
        let task = self.tasks.get(task_ordinal).ok_or_else(|| {
            SupervisorError::invariant("review-response progress task is missing")
        })?;
        let developer_final_path = task.latest_developer_final_path.clone().ok_or_else(|| {
            SupervisorError::invariant("review-response progress lacks a Developer final path")
        })?;
        let result = task.reviewer_results.get(&reviewer_id).ok_or_else(|| {
            SupervisorError::invariant("review-response progress lacks a Reviewer verdict")
        })?;
        if result.final_message_paths.is_empty() {
            return Err(SupervisorError::invariant(
                "review-response progress lacks a Reviewer final path",
            ));
        }
        self.progress_events
            .push(SessionProgressEvent::ReviewResponded {
                sequence,
                task_ordinal: task_ordinal_value,
                task_key: task.spec.task_key.clone(),
                completed_tasks,
                total_tasks,
                review_round: task.review_round,
                review_generation: task.review_generation,
                max_review_rounds: task.spec.max_review_rounds,
                reviewer_id,
                reviewer_verdict: result.verdict,
                developer_final_path,
                reviewer_final_message_paths: result.final_message_paths.clone(),
                responses_received: u8::try_from(task.reviewer_results.len())
                    .map_err(|_| SupervisorError::overflow("Reviewer response count overflow"))?,
                responses_expected: u8::try_from(self.reviewer_bindings.len())
                    .map_err(|_| SupervisorError::overflow("Reviewer binding count overflow"))?,
                github: self
                    .delivery_binding
                    .is_github()
                    .then(|| self.github_task_progress(task_ordinal))
                    .transpose()?,
            });
        Ok(())
    }

    fn push_task_completed(&mut self, task_ordinal: usize) -> Result<(), SupervisorError> {
        let sequence = self.next_progress_sequence()?;
        let (completed_tasks, total_tasks) = self.progress_task_counts()?;
        let task_ordinal_value = self.progress_task_ordinal(task_ordinal)?;
        let task = self
            .tasks
            .get(task_ordinal)
            .ok_or_else(|| SupervisorError::invariant("completed progress task is missing"))?;
        let outcome = match task.state {
            TaskState::Lgtm => TaskCompletionOutcome::Lgtm,
            TaskState::ReviewExhausted => TaskCompletionOutcome::ReviewExhausted,
            _ => {
                return Err(SupervisorError::invariant(
                    "task-completed progress requires a terminal review outcome",
                ));
            }
        };
        let developer_final_path = task.latest_developer_final_path.clone().ok_or_else(|| {
            SupervisorError::invariant("completed progress lacks a Developer final path")
        })?;
        if task.reviewer_results.len() != self.reviewer_bindings.len() {
            return Err(SupervisorError::invariant(
                "completed progress lacks the active Reviewer results",
            ));
        }
        self.progress_events
            .push(SessionProgressEvent::TaskCompleted {
                sequence,
                task_ordinal: task_ordinal_value,
                task_key: task.spec.task_key.clone(),
                completed_tasks,
                total_tasks,
                review_round: task.review_round,
                review_generation: task.review_generation,
                max_review_rounds: task.spec.max_review_rounds,
                outcome,
                developer_final_path,
                reviewers: reviewer_result_snapshots(task, &self.reviewer_ids()),
                github: self
                    .delivery_binding
                    .is_github()
                    .then(|| self.github_task_progress(task_ordinal))
                    .transpose()?,
            });
        Ok(())
    }

    fn schedule_session_open(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_lane_available(lane)?;
        self.pending_session_opens
            .insert(lane, ExpectedSessionOpen { task_ordinal, lane });
        Ok(vec![SupervisorEffect::OpenRoleSession {
            task_ordinal,
            lane,
        }])
    }

    fn schedule_turn(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_lane_available(lane)?;
        if purpose.role() != lane.role() {
            return Err(SupervisorError::invalid_event(
                "turn purpose does not match its lane role",
            ));
        }
        let review_generation = lane
            .reviewer_id()
            .map(|_| self.tasks[task_ordinal].review_generation);
        self.pending_turn_starts.insert(
            lane,
            ExpectedTurnStart {
                task_ordinal,
                lane,
                review_generation,
                purpose,
                session,
            },
        );
        Ok(vec![SupervisorEffect::StartTurn {
            task_ordinal,
            lane,
            review_generation,
            purpose,
            session,
        }])
    }

    fn require_no_pending_operation(&self) -> Result<(), SupervisorError> {
        if self.pending_runtime_open.is_some()
            || !self.pending_session_opens.is_empty()
            || !self.pending_turn_starts.is_empty()
            || !self.active_turns.is_empty()
        {
            return Err(SupervisorError::invariant(
                "cannot schedule two supervisor operations at once",
            ));
        }
        Ok(())
    }

    fn require_lane_available(&self, lane: WorkerLane) -> Result<(), SupervisorError> {
        if self.pending_runtime_open.is_some() {
            return Err(SupervisorError::invariant(
                "worker lane cannot schedule while task runtime open is pending",
            ));
        }
        if self.pending_session_opens.contains_key(&lane)
            || self.pending_turn_starts.contains_key(&lane)
            || self.active_turns.contains_key(&lane)
        {
            return Err(SupervisorError::invariant(
                "worker lane already owns a pending or active operation",
            ));
        }
        if lane == WorkerLane::Developer
            && (!self.pending_session_opens.is_empty()
                || !self.pending_turn_starts.is_empty()
                || !self.active_turns.is_empty())
        {
            return Err(SupervisorError::invariant(
                "Developer cannot schedule with any Reviewer operation",
            ));
        }
        if lane.role() == WorkerRole::Reviewer
            && (self
                .pending_session_opens
                .contains_key(&WorkerLane::Developer)
                || self
                    .pending_turn_starts
                    .contains_key(&WorkerLane::Developer)
                || self.active_turns.contains_key(&WorkerLane::Developer))
        {
            return Err(SupervisorError::invariant(
                "Reviewer cannot schedule with a Developer operation",
            ));
        }
        Ok(())
    }

    fn require_running_task(&self, task_ordinal: usize) -> Result<(), SupervisorError> {
        if self.session_state != SessionState::Running {
            return Err(SupervisorError::invalid_transition(
                "worker lifecycle event requires a running session",
            ));
        }
        if self.current_task != Some(task_ordinal) || self.tasks.get(task_ordinal).is_none() {
            return Err(SupervisorError::invalid_identity(
                "worker lifecycle event references a non-current task",
            ));
        }
        Ok(())
    }

    fn session_for(&self, task_ordinal: usize, lane: WorkerLane) -> Option<RuntimeSessionKey> {
        let task = self.tasks.get(task_ordinal)?;
        match lane {
            WorkerLane::Developer => task.developer_session,
            WorkerLane::Reviewer(reviewer_id) => task.reviewer_sessions.get(&reviewer_id).copied(),
        }
    }

    fn take_matching_active(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Result<CoreActiveTurn, SupervisorError> {
        if self.accepted_completion_tokens.contains(completion_token) {
            return Err(SupervisorError::duplicate(
                "completion token was already accepted",
            ));
        }
        let active = self.active_turns.get(&lane).ok_or_else(|| {
            SupervisorError::invalid_transition("turn completion arrived with no active turn")
        })?;
        if active.task_ordinal != task_ordinal
            || active.lane != lane
            || active.review_generation != review_generation
            || active.session != session
            || active.turn != turn
            || active.completion_token != completion_token
        {
            return Err(SupervisorError::invalid_identity(
                "turn completion identity does not match the active turn",
            ));
        }
        Ok(self
            .active_turns
            .remove(&lane)
            .expect("active turn was just validated"))
    }

    fn interrupt_active_effects(&mut self) -> Vec<SupervisorEffect> {
        std::mem::take(&mut self.active_turns)
            .into_values()
            .map(|active| {
                self.accepted_completion_tokens
                    .insert(active.completion_token);
                SupervisorEffect::InterruptTurn {
                    task_ordinal: active.task_ordinal,
                    lane: active.lane,
                    session: active.session,
                    turn: active.turn,
                }
            })
            .collect()
    }

    fn terminalize_current(
        &mut self,
        session_state: SessionState,
        task_state: TaskState,
        detail: &str,
        mut effects: Vec<SupervisorEffect>,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if !session_state.is_terminal() {
            return Err(SupervisorError::invariant(
                "terminal transition received a non-terminal session state",
            ));
        }
        validate_single_line("terminal detail", detail, MAX_CORE_DIAGNOSTIC_BYTES)?;
        if let Some(task_ordinal) = self.current_task
            && let Some(task) = self.tasks.get_mut(task_ordinal)
            && !matches!(task.state, TaskState::Lgtm | TaskState::ReviewExhausted)
        {
            task.state = task_state;
            task.outcome_detail = Some(detail.into());
        }
        let runtime_task = self
            .runtime_open
            .take()
            .or(self.pending_runtime_open.take());
        if let Some(task_ordinal) = runtime_task {
            effects.push(SupervisorEffect::CloseTaskRuntime { task_ordinal });
        }
        self.pending_session_opens.clear();
        self.pending_turn_starts.clear();
        self.active_turns.clear();
        self.pending_architect_action = None;
        if let Some(github) = self.github_state.as_mut() {
            github.pending_operations.clear();
        }
        self.session_state = session_state;
        self.terminal_detail = Some(detail.into());
        effects.push(SupervisorEffect::FinishSession {
            state: session_state,
            detail: detail.into(),
        });
        Ok(effects)
    }

    fn clear_runtime_state(&mut self) {
        self.pending_runtime_open = None;
        self.runtime_open = None;
        self.pending_session_opens.clear();
        self.pending_turn_starts.clear();
        self.active_turns.clear();
        self.pending_architect_action = None;
        self.used_sessions.clear();
        self.used_turns.clear();
        self.accepted_completion_tokens.clear();
    }

    fn assert_invariants(&self) -> Result<(), SupervisorError> {
        if self.tasks.len() > MAX_TASKS {
            return Err(SupervisorError::invariant(
                "core contains more than 64 tasks",
            ));
        }
        if !matches!(
            self.reviewer_bindings.as_slice(),
            [ReviewerBindingSnapshot {
                reviewer_id: ReviewerId::Reviewer1,
                ..
            }] | [
                ReviewerBindingSnapshot {
                    reviewer_id: ReviewerId::Reviewer1,
                    ..
                },
                ReviewerBindingSnapshot {
                    reviewer_id: ReviewerId::Reviewer2,
                    ..
                }
            ]
        ) {
            return Err(SupervisorError::invariant(
                "session Reviewer bindings are not a canonical ordered topology",
            ));
        }
        for binding in &self.reviewer_bindings {
            if !matches!(binding.provider.as_str(), "codex-exec" | "claude-exec")
                || validate_single_line("Reviewer model", &binding.model, 128).is_err()
                || validate_single_line("Reviewer reasoning effort", &binding.reasoning_effort, 32)
                    .is_err()
                || validate_sha256("Reviewer contract hash", &binding.contract_sha256).is_err()
            {
                return Err(SupervisorError::invariant(
                    "session Reviewer binding metadata is invalid",
                ));
            }
        }
        if self.progress_events.len() > MAX_PROGRESS_EVENTS_PER_RUN {
            return Err(SupervisorError::invariant(
                "run progress event capacity was exceeded",
            ));
        }
        let total_tasks = u32::try_from(self.tasks.len())
            .map_err(|_| SupervisorError::invariant("total task count overflow"))?;
        for (index, event) in self.progress_events.iter().enumerate() {
            let expected_sequence = u32::try_from(index)
                .ok()
                .and_then(|sequence| sequence.checked_add(1))
                .ok_or_else(|| SupervisorError::invariant("progress event sequence overflow"))?;
            if event.sequence() != expected_sequence {
                return Err(SupervisorError::invariant(
                    "run progress events are not ordered and contiguous",
                ));
            }
            match (event.task_ordinal(), event.task_key()) {
                (Some(task_ordinal), Some(task_key)) => {
                    let task_ordinal = usize::try_from(task_ordinal).map_err(|_| {
                        SupervisorError::invariant("progress task ordinal overflow")
                    })?;
                    if self
                        .tasks
                        .get(task_ordinal)
                        .is_none_or(|task| task.spec.task_key != task_key)
                    {
                        return Err(SupervisorError::invariant(
                            "progress event task identity is invalid",
                        ));
                    }
                }
                (None, None) => {}
                _ => {
                    return Err(SupervisorError::invariant(
                        "progress event task identity is incomplete",
                    ));
                }
            }
            if event.total_tasks() != total_tasks || event.completed_tasks() > total_tasks {
                return Err(SupervisorError::invariant(
                    "progress event task counts are invalid",
                ));
            }
        }
        let has_plan =
            self.plan_version.is_some() || self.plan_hash.is_some() || !self.tasks.is_empty();
        if !has_plan {
            if !matches!(
                self.session_state,
                SessionState::AwaitingPlan | SessionState::Canceled
            ) || self.plan_version.is_some()
                || self.plan_hash.is_some()
                || !self.tasks.is_empty()
                || self.current_task.is_some()
            {
                return Err(SupervisorError::invariant(
                    "planless core is not an empty awaiting-plan or canceled session",
                ));
            }
        } else if self.session_state == SessionState::AwaitingPlan {
            if self.plan_version.is_some()
                || self.plan_hash.is_some()
                || !self.tasks.is_empty()
                || self.current_task.is_some()
            {
                return Err(SupervisorError::invariant(
                    "awaiting-plan session contains a bound plan",
                ));
            }
        } else if self.plan_version.is_none() || self.plan_hash.is_none() || self.tasks.is_empty() {
            return Err(SupervisorError::invariant(
                "post-plan session lost its plan binding",
            ));
        }
        if self.session_state == SessionState::AwaitingApproval && self.current_task.is_some() {
            return Err(SupervisorError::invariant(
                "awaiting-approval session has a current task",
            ));
        }
        if self.session_state == SessionState::Running {
            let current = self
                .current_task
                .ok_or_else(|| SupervisorError::invariant("running session has no current task"))?;
            if current >= self.tasks.len() {
                return Err(SupervisorError::invariant(
                    "current task ordinal is out of range",
                ));
            }
            for task in self.tasks.iter().take(current) {
                if !matches!(task.state, TaskState::Lgtm | TaskState::ReviewExhausted) {
                    return Err(SupervisorError::invariant(
                        "an earlier task is not terminal before the current task",
                    ));
                }
            }
            for task in self.tasks.iter().skip(current + 1) {
                if task.state != TaskState::Pending {
                    return Err(SupervisorError::invariant(
                        "a future task advanced before the current task",
                    ));
                }
            }
        }
        if self.session_state.is_terminal()
            && (self.pending_runtime_open.is_some()
                || self.runtime_open.is_some()
                || !self.pending_session_opens.is_empty()
                || !self.pending_turn_starts.is_empty()
                || !self.active_turns.is_empty()
                || self.pending_architect_action.is_some())
        {
            return Err(SupervisorError::invariant(
                "terminal session retains a live runtime operation",
            ));
        }
        if self.pending_runtime_open.is_some() && self.runtime_open.is_some() {
            return Err(SupervisorError::invariant(
                "runtime cannot be pending-open and open simultaneously",
            ));
        }
        let scheduled_operations = self.pending_session_opens.len()
            + self.pending_turn_starts.len()
            + self.active_turns.len();
        if scheduled_operations > self.reviewer_bindings.len().max(1)
            || (self.pending_runtime_open.is_some() && scheduled_operations != 0)
        {
            return Err(SupervisorError::invariant(
                "worker operation count exceeds the active topology ceiling",
            ));
        }
        if let Some(task_ordinal) = self.pending_runtime_open
            && (self.current_task != Some(task_ordinal)
                || self
                    .tasks
                    .get(task_ordinal)
                    .is_none_or(|task| task.state != TaskState::Pending))
        {
            return Err(SupervisorError::invariant(
                "pending runtime open is not bound to the current pending task",
            ));
        }
        if let Some(task_ordinal) = self.runtime_open
            && (self.current_task != Some(task_ordinal)
                || self.tasks.get(task_ordinal).is_none_or(|task| {
                    !matches!(
                        task.state,
                        TaskState::Developing
                            | TaskState::AwaitingArchitectAction
                            | TaskState::PublishingCandidate
                            | TaskState::Reviewing
                            | TaskState::PublishingReview
                    )
                }))
        {
            return Err(SupervisorError::invariant(
                "open runtime is not bound to the current active task",
            ));
        }
        for (lane, expected) in &self.pending_session_opens {
            let state_matches = |state: TaskState| match lane.role() {
                WorkerRole::Developer => state == TaskState::Developing,
                WorkerRole::Reviewer => {
                    matches!(state, TaskState::Reviewing | TaskState::PublishingReview)
                }
            };
            if lane
                .reviewer_id()
                .is_some_and(|reviewer_id| !self.reviewer_ids().contains(&reviewer_id))
                || *lane != expected.lane
                || self.runtime_open != Some(expected.task_ordinal)
                || self
                    .tasks
                    .get(expected.task_ordinal)
                    .is_none_or(|task| !state_matches(task.state))
                || self.session_for(expected.task_ordinal, *lane).is_some()
            {
                return Err(SupervisorError::invariant(
                    "pending role-session open is not bound to an unbound current role",
                ));
            }
        }
        for (lane, expected) in &self.pending_turn_starts {
            let state_matches = |state: TaskState| match lane.role() {
                WorkerRole::Developer => state == TaskState::Developing,
                WorkerRole::Reviewer => {
                    matches!(state, TaskState::Reviewing | TaskState::PublishingReview)
                }
            };
            if lane
                .reviewer_id()
                .is_some_and(|reviewer_id| !self.reviewer_ids().contains(&reviewer_id))
                || *lane != expected.lane
                || self.runtime_open != Some(expected.task_ordinal)
                || expected.purpose.role() != lane.role()
                || self.session_for(expected.task_ordinal, *lane) != Some(expected.session)
                || self
                    .tasks
                    .get(expected.task_ordinal)
                    .is_none_or(|task| !state_matches(task.state))
                || expected.review_generation
                    != lane
                        .reviewer_id()
                        .map(|_| self.tasks[expected.task_ordinal].review_generation)
            {
                return Err(SupervisorError::invariant(
                    "pending turn is not bound to the exact current role session",
                ));
            }
        }
        let mut run_clarification_records = 0usize;
        for task in &self.tasks {
            let max_review_rounds = u32::from(task.spec.max_review_rounds);
            if task.review_round > max_review_rounds
                || task.review_generation > max_review_rounds
                || task.review_round > task.review_generation
                || task.review_generation > task.review_round.saturating_add(1)
            {
                return Err(SupervisorError::invariant(
                    "task review round/generation counters are inconsistent",
                ));
            }
            if task.clarification_rounds_used > u32::from(task.spec.max_clarification_rounds) {
                return Err(SupervisorError::invariant(
                    "task clarification round exceeds its maximum",
                ));
            }
            if task.clarification_records.len() > MAX_CLARIFICATION_RECORDS_PER_TASK {
                return Err(SupervisorError::invariant(
                    "task clarification record capacity was exceeded",
                ));
            }
            run_clarification_records = run_clarification_records
                .checked_add(task.clarification_records.len())
                .ok_or_else(|| {
                    SupervisorError::invariant("run clarification record count overflow")
                })?;
            let autonomous_records = task
                .clarification_records
                .iter()
                .filter(|record| !record.human_decision_confirmed)
                .count();
            if usize::try_from(task.clarification_rounds_used).ok() != Some(autonomous_records) {
                return Err(SupervisorError::invariant(
                    "task clarification round count differs from its records",
                ));
            }
            for (index, record) in task.clarification_records.iter().enumerate() {
                let expected_sequence = u32::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| SupervisorError::invariant("clarification sequence overflow"))?;
                if record.sequence != expected_sequence {
                    return Err(SupervisorError::invariant(
                        "task clarification records are not ordered and contiguous",
                    ));
                }
                validate_absolute_path(
                    "clarification developer request path",
                    &record.developer_request_path,
                )?;
                validate_absolute_path(
                    "Architect clarification path",
                    &record.architect_clarification_path,
                )?;
            }
            match task.state {
                TaskState::Pending if task.review_round != 0 || task.review_generation != 0 => {
                    return Err(SupervisorError::invariant(
                        "pending task has allocated review counters",
                    ));
                }
                TaskState::Developing | TaskState::AwaitingArchitectAction
                    if task.review_round == 0 && task.review_generation != 0 =>
                {
                    return Err(SupervisorError::invariant(
                        "initial development has an allocated review generation",
                    ));
                }
                TaskState::Developing | TaskState::AwaitingArchitectAction
                    if task.review_round > 0
                        && (task.review_generation != task.review_round
                            || task.review_round >= max_review_rounds) =>
                {
                    return Err(SupervisorError::invariant(
                        "correction state does not preserve the completed generation",
                    ));
                }
                TaskState::Reviewing
                    if task.developer_session.is_none()
                        || task.review_generation == 0
                        || task.review_generation != task.review_round + 1 =>
                {
                    return Err(SupervisorError::invariant(
                        "reviewing task lacks an exact in-flight generation",
                    ));
                }
                TaskState::PublishingCandidate
                    if task.developer_session.is_none()
                        || task.review_generation == 0
                        || task.review_generation != task.review_round + 1 =>
                {
                    return Err(SupervisorError::invariant(
                        "publishing-candidate task lacks an exact in-flight generation",
                    ));
                }
                TaskState::PublishingReview
                    if task.developer_session.is_none()
                        || task.review_generation == 0
                        || (task.review_generation != task.review_round + 1
                            && task.review_generation != task.review_round) =>
                {
                    return Err(SupervisorError::invariant(
                        "publishing-review task lacks a current or just-joined generation",
                    ));
                }
                TaskState::Lgtm
                    if task.review_round == 0 || task.review_generation != task.review_round =>
                {
                    return Err(SupervisorError::invariant(
                        "LGTM task lacks a completed synchronized generation",
                    ));
                }
                TaskState::ReviewExhausted
                    if task.review_round != max_review_rounds
                        || task.review_generation != task.review_round =>
                {
                    return Err(SupervisorError::invariant(
                        "review-exhausted task did not reach its exact maximum",
                    ));
                }
                _ => {}
            }
            if !task.reviewer_sessions.is_empty() && task.developer_session.is_none() {
                return Err(SupervisorError::invariant(
                    "Reviewer session exists without the task Developer session",
                ));
            }
            let reviewer_ids = self.reviewer_ids();
            if task
                .reviewer_sessions
                .keys()
                .any(|reviewer_id| !reviewer_ids.contains(reviewer_id))
                || task.reviewer_results.len() > reviewer_ids.len()
                || task.reviewer_results.iter().any(|(reviewer_id, result)| {
                    !reviewer_ids.contains(reviewer_id)
                        || result.generation != task.review_generation
                        || result.final_message_paths.is_empty()
                        || result.final_message_paths.len() > 2
                })
            {
                return Err(SupervisorError::invariant(
                    "current Reviewer result set is invalid",
                ));
            }
            if task.historical_reviewer_final_paths.len() != reviewer_ids.len()
                || reviewer_ids.into_iter().any(|reviewer_id| {
                    task.historical_reviewer_final_paths
                        .get(&reviewer_id)
                        .is_none_or(|paths| {
                            paths.len() > usize::from(task.spec.max_review_rounds) * 2
                                || paths.iter().any(|path| {
                                    validate_absolute_path(
                                        "historical Reviewer final message path",
                                        path,
                                    )
                                    .is_err()
                                })
                        })
                })
            {
                return Err(SupervisorError::invariant(
                    "historical Reviewer evidence index is invalid",
                ));
            }
            if task
                .pending_native_reviews
                .iter()
                .any(|(reviewer_id, result)| {
                    !self.reviewer_ids().contains(reviewer_id)
                        || result.generation != task.review_generation
                        || result.final_message_paths.is_empty()
                        || result.final_message_paths.len() > 2
                })
                || task.github_reviews.iter().any(|(reviewer_id, review)| {
                    *reviewer_id != review.reviewer_id
                        || !self.reviewer_ids().contains(reviewer_id)
                        || review.generation != task.review_generation
                        || validate_git_sha("GitHub review head", &review.head_sha).is_err()
                        || review.review_id == 0
                        || validate_github_url("GitHub review URL", &review.review_url).is_err()
                        || validate_sha256(
                            "GitHub review artifact SHA-256",
                            &review.final_artifact_sha256,
                        )
                        .is_err()
                })
            {
                return Err(SupervisorError::invariant(
                    "GitHub Reviewer publication evidence is invalid",
                ));
            }
            if self.delivery_binding.is_github() {
                if task
                    .github_base_sha
                    .as_deref()
                    .is_some_and(|sha| validate_git_sha("GitHub task base", sha).is_err())
                    || task
                        .github_final_head_sha
                        .as_deref()
                        .is_some_and(|sha| validate_git_sha("GitHub task final head", sha).is_err())
                    || task.github_check.as_ref().is_some_and(|check| {
                        check.check_run_id == 0
                            || validate_github_url("GitHub Check URL", &check.check_url).is_err()
                            || validate_git_sha("GitHub Check head", &check.head_sha).is_err()
                            || !matches!(
                                check.state.as_str(),
                                "in_progress" | "success" | "action_required" | "cancelled"
                            )
                    })
                {
                    return Err(SupervisorError::invariant(
                        "GitHub task range or Check evidence is invalid",
                    ));
                }
            } else if task.github_base_sha.is_some()
                || task.github_final_head_sha.is_some()
                || !task.github_reviews.is_empty()
                || task.github_check.is_some()
                || !task.pending_native_reviews.is_empty()
            {
                return Err(SupervisorError::invariant(
                    "local candidate task contains GitHub publication state",
                ));
            }
            if let Some(session) = task.developer_session
                && !self.used_sessions.contains(&session)
            {
                return Err(SupervisorError::invariant(
                    "Developer session is absent from the global identity set",
                ));
            }
            for session in task.reviewer_sessions.values() {
                if !self.used_sessions.contains(session) {
                    return Err(SupervisorError::invariant(
                        "Reviewer session is absent from the global identity set",
                    ));
                }
            }
        }
        if run_clarification_records > MAX_CLARIFICATION_RECORDS_PER_RUN {
            return Err(SupervisorError::invariant(
                "run clarification record capacity was exceeded",
            ));
        }
        match (&self.delivery_binding, &self.github_state) {
            (DeliveryBinding::LocalCandidate, None) => {}
            (DeliveryBinding::LocalCandidate, Some(_)) => {
                return Err(SupervisorError::invariant(
                    "local candidate session contains GitHub run state",
                ));
            }
            (DeliveryBinding::GitHubPullRequest { .. }, None)
                if matches!(
                    self.session_state,
                    SessionState::AwaitingPlan | SessionState::AwaitingApproval
                ) || (self.session_state == SessionState::Canceled
                    && self.current_task.is_none()) => {}
            (DeliveryBinding::GitHubPullRequest { .. }, None) => {
                return Err(SupervisorError::invariant(
                    "started GitHub session lost its run state",
                ));
            }
            (DeliveryBinding::GitHubPullRequest { .. }, Some(state)) => {
                for (operation_id, operation) in &state.pending_operations {
                    if operation_id != operation.operation_id()
                        || validate_identifier("GitHub operation ID", operation_id).is_err()
                    {
                        return Err(SupervisorError::invariant(
                            "pending GitHub operation identity is invalid",
                        ));
                    }
                }
                if state.worktree_path.as_deref().is_some_and(|path| {
                    validate_absolute_path("GitHub worktree path", path).is_err()
                }) || state
                    .published_head_sha
                    .as_deref()
                    .is_some_and(|sha| validate_git_sha("GitHub published head", sha).is_err())
                    || state.pr_number == Some(0)
                    || state.pr_url.as_deref().is_some_and(|url| {
                        validate_github_url("GitHub Pull Request URL", url).is_err()
                    })
                    || state
                        .merge_sha
                        .as_deref()
                        .is_some_and(|sha| validate_git_sha("GitHub merge SHA", sha).is_err())
                    || state
                        .merge_url
                        .as_deref()
                        .is_some_and(|url| validate_github_url("GitHub merge URL", url).is_err())
                {
                    return Err(SupervisorError::invariant("GitHub run evidence is invalid"));
                }
                if self.session_state.is_terminal() && !state.pending_operations.is_empty() {
                    return Err(SupervisorError::invariant(
                        "terminal GitHub session retains a pending external operation",
                    ));
                }
                if (state.local_branch_present && !state.local_base_ref_present)
                    || (state.worktree_path.is_some() && !state.local_branch_present)
                {
                    return Err(SupervisorError::invariant(
                        "GitHub repository preparation state is not monotonic",
                    ));
                }
                match state.phase {
                    GitHubDeliveryPhase::PreparingRepository
                        if self.session_state == SessionState::Running
                            && (state.local_base_ref_present
                                || state.local_branch_present
                                || state.worktree_path.is_some()
                                || self.runtime_open.is_some()
                                || self.pending_runtime_open.is_some()) =>
                    {
                        return Err(SupervisorError::invariant(
                            "preparing GitHub phase already owns a worktree or worker runtime",
                        ));
                    }
                    GitHubDeliveryPhase::AwaitingMerge | GitHubDeliveryPhase::Finalizing
                        if self.runtime_open.is_some()
                            || self.pending_runtime_open.is_some()
                            || !self.active_turns.is_empty()
                            || self.tasks.iter().any(|task| task.state != TaskState::Lgtm) =>
                    {
                        return Err(SupervisorError::invariant(
                            "GitHub merge phase retains a worker or non-LGTM task",
                        ));
                    }
                    GitHubDeliveryPhase::PreservedUnmerged
                        if self.session_state != SessionState::Completed
                            || state.outcome
                                != Some(GitHubDeliveryOutcome::UnmergedReviewExhausted)
                            || !self
                                .tasks
                                .iter()
                                .any(|task| task.state == TaskState::ReviewExhausted) =>
                    {
                        return Err(SupervisorError::invariant(
                            "preserved GitHub run lacks completed review-exhausted evidence",
                        ));
                    }
                    GitHubDeliveryPhase::Delivered
                        if self.session_state != SessionState::Completed
                            || state.outcome != Some(GitHubDeliveryOutcome::Delivered)
                            || state.merge_sha.is_none()
                            || state.finalization.as_ref().is_none_or(|finalization| {
                                !finalization.local_worktree_removed
                                    || !finalization.local_ref_removed
                            }) =>
                    {
                        return Err(SupervisorError::invariant(
                            "delivered GitHub run lacks confirmed merge/finalization evidence",
                        ));
                    }
                    _ => {}
                }
            }
        }
        match &self.pending_architect_action {
            Some(pending) => {
                let task_ordinal = usize::try_from(pending.task_ordinal).map_err(|_| {
                    SupervisorError::invariant("pending Architect task ordinal overflow")
                })?;
                let task = self.tasks.get(task_ordinal).ok_or_else(|| {
                    SupervisorError::invariant("pending Architect action task is missing")
                })?;
                if self.session_state != SessionState::Running
                    || self.current_task != Some(task_ordinal)
                    || self.runtime_open != Some(task_ordinal)
                    || task.state != TaskState::AwaitingArchitectAction
                    || task.spec.task_key != pending.task_key
                    || pending.sequence
                        != u32::try_from(task.clarification_records.len())
                            .ok()
                            .and_then(|value| value.checked_add(1))
                            .ok_or_else(|| {
                                SupervisorError::invariant(
                                    "pending clarification sequence overflow",
                                )
                            })?
                    || pending.clarification_rounds_used != task.clarification_rounds_used
                    || pending.max_clarification_rounds != task.spec.max_clarification_rounds
                    || pending.published_version != self.version
                    || (!pending.human_decision_required
                        && task.clarification_rounds_used
                            >= u32::from(task.spec.max_clarification_rounds))
                    || !self.pending_session_opens.is_empty()
                    || !self.pending_turn_starts.is_empty()
                    || !self.active_turns.is_empty()
                {
                    return Err(SupervisorError::invariant(
                        "pending Architect action is not latched to an idle current task",
                    ));
                }
                validate_absolute_path(
                    "pending Developer request path",
                    &pending.developer_request_path,
                )?;
                validate_absolute_path(
                    "pending clarification output path",
                    &pending.clarification_output_path,
                )?;
            }
            None => {
                if self
                    .tasks
                    .iter()
                    .any(|task| task.state == TaskState::AwaitingArchitectAction)
                {
                    return Err(SupervisorError::invariant(
                        "awaiting-Architect task has no pending action",
                    ));
                }
            }
        }
        let session_count = self
            .tasks
            .iter()
            .map(|task| {
                usize::from(task.developer_session.is_some()) + task.reviewer_sessions.len()
            })
            .sum::<usize>();
        if session_count != self.used_sessions.len() {
            return Err(SupervisorError::invariant(
                "logical runtime session key was reused across roles or tasks",
            ));
        }
        for (lane, active) in &self.active_turns {
            if lane
                .reviewer_id()
                .is_some_and(|reviewer_id| !self.reviewer_ids().contains(&reviewer_id))
                || self.current_task != Some(active.task_ordinal)
                || self.runtime_open != Some(active.task_ordinal)
                || *lane != active.lane
                || self.session_for(active.task_ordinal, *lane) != Some(active.session)
                || active.purpose.role() != lane.role()
                || active.review_generation
                    != lane
                        .reviewer_id()
                        .map(|_| self.tasks[active.task_ordinal].review_generation)
            {
                return Err(SupervisorError::invariant(
                    "active turn is not bound to the exact current role session",
                ));
            }
            let state_matches = match lane.role() {
                WorkerRole::Developer => {
                    self.tasks[active.task_ordinal].state == TaskState::Developing
                }
                WorkerRole::Reviewer => matches!(
                    self.tasks[active.task_ordinal].state,
                    TaskState::Reviewing | TaskState::PublishingReview
                ),
            };
            if !state_matches {
                return Err(SupervisorError::invariant(
                    "active turn lane does not match the task state",
                ));
            }
            if self
                .accepted_completion_tokens
                .contains(&active.completion_token)
            {
                return Err(SupervisorError::invariant(
                    "active turn completion token was already accepted",
                ));
            }
        }
        Ok(())
    }

    fn reviewer_ids(&self) -> Vec<ReviewerId> {
        self.reviewer_bindings
            .iter()
            .map(|binding| binding.reviewer_id)
            .collect()
    }
}

fn validate_identifier(label: &str, value: &str) -> Result<(), SupervisorError> {
    validate_identifier_with_bound(label, value, 128)
}

#[cfg(test)]
fn reviewer_ids() -> [ReviewerId; 2] {
    [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
}

fn default_reviewer_bindings() -> Vec<ReviewerBindingSnapshot> {
    [
        (ReviewerId::Reviewer1, RuntimeProfile::codex_exec_default()),
        (ReviewerId::Reviewer2, RuntimeProfile::claude_exec_default()),
    ]
    .into_iter()
    .map(|(reviewer_id, profile)| ReviewerBindingSnapshot {
        reviewer_id,
        provider: profile.provider.as_str().into(),
        model: profile.model,
        reasoning_effort: profile.reasoning_effort,
        contract_sha256: profile.provider.contract_identity().contract_sha256,
    })
    .collect()
}

fn reviewer_result_snapshots(
    task: &CoreTask,
    reviewer_ids: &[ReviewerId],
) -> Vec<ReviewerResultSnapshot> {
    reviewer_ids
        .iter()
        .copied()
        .map(|reviewer_id| {
            let result = task.reviewer_results.get(&reviewer_id);
            ReviewerResultSnapshot {
                reviewer_id,
                session_bound: task.reviewer_sessions.contains_key(&reviewer_id),
                current_generation: result.map(|result| result.generation),
                current_verdict: result.map(|result| result.verdict),
                current_final_message_paths: result
                    .map(|result| result.final_message_paths.clone())
                    .unwrap_or_default(),
            }
        })
        .collect()
}

fn validate_identifier_with_bound(
    label: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), SupervisorError> {
    if value.is_empty()
        || value.len() > max_bytes
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        return Err(SupervisorError::invalid_event(format!(
            "{label} is not a bounded opaque identifier"
        )));
    }
    Ok(())
}

fn validate_single_line(label: &str, value: &str, max_bytes: usize) -> Result<(), SupervisorError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        return Err(SupervisorError::invalid_event(format!(
            "{label} is not bounded single-line text"
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<(), SupervisorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SupervisorError::invalid_event(format!(
            "{label} must be a lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_git_sha(label: &str, value: &str) -> Result<(), SupervisorError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SupervisorError::invalid_event(format!(
            "{label} must be a lowercase 40-hex Git object ID"
        )));
    }
    Ok(())
}

fn validate_github_url(label: &str, value: &str) -> Result<(), SupervisorError> {
    validate_single_line(label, value, 2048)?;
    if !value.starts_with("https://github.com/") {
        return Err(SupervisorError::invalid_event(format!(
            "{label} is not a bounded github.com URL"
        )));
    }
    Ok(())
}

fn validate_absolute_path(label: &str, value: &str) -> Result<(), SupervisorError> {
    if value.is_empty() || value.len() > MAX_REPOSITORY_PATH_BYTES {
        return Err(SupervisorError::invalid_repository(format!(
            "{label} is empty or exceeds its bound"
        )));
    }
    let path = Path::new(value);
    if !path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(SupervisorError::invalid_repository(format!(
            "{label} must be absolute and lexically normalized"
        )));
    }
    Ok(())
}

fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn canonical_hash(value: &impl Serialize) -> String {
    let encoded = serde_json::to_vec(value).expect("closed core contracts are serializable");
    let digest = Sha256::digest(encoded);
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
    use crate::control_api::protocol::{
        GitHubAppBinding, GitHubPermissionLevel, GitHubPullRequestBinding,
        GitHubReviewerAppBinding, MAX_REVIEW_ROUNDS, MIN_DUAL_REVIEW_ROUNDS,
        MIN_SINGLE_REVIEW_ROUNDS,
    };
    use crate::worker::runtime::DeveloperOutcomeV1;

    const PROFILE_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn task(key: &str, root: &str, max_review_rounds: u8) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: format!("Task {key}"),
            repository_root: root.into(),
            task_document_path: format!("/project/tasks/{key}.md"),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: key.into(),
            max_review_rounds: max_review_rounds.max(MIN_DUAL_REVIEW_ROUNDS),
            max_clarification_rounds: 2,
        }
    }

    fn ready() -> RuntimeOutcome {
        RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Ready,
        })
    }

    fn clarification_required() -> RuntimeOutcome {
        RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::ClarificationRequired,
        })
    }

    fn clarification_record(sequence: u32) -> ClarificationRecord {
        ClarificationRecord {
            sequence,
            reason: ArchitectActionReason::Clarification,
            developer_request_path: format!("/artifacts/developer/request-{sequence}.md"),
            architect_clarification_path: format!("/project/clarification/turn-{sequence}.md"),
            human_decision_confirmed: true,
        }
    }

    fn runtime_failure(
        class: RuntimeFailureClass,
        retryable: bool,
        detail: impl Into<String>,
    ) -> SanitizedRuntimeFailure {
        SanitizedRuntimeFailure {
            class,
            detail: detail.into(),
            retryable,
        }
    }

    fn lgtm() -> RuntimeOutcome {
        RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
            verdict: ReviewerVerdict::Lgtm,
            preceding_final_message_paths: Vec::new(),
        })
    }

    fn request_changes() -> RuntimeOutcome {
        RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
            verdict: ReviewerVerdict::RequestChanges,
            preceding_final_message_paths: Vec::new(),
        })
    }

    fn final_message_path(role: WorkerRole, turn: RuntimeTurnKey) -> PathBuf {
        let role = match role {
            WorkerRole::Developer => "developer",
            WorkerRole::Reviewer => "reviewer",
        };
        PathBuf::from(format!(
            "/artifacts/{role}/turn-{}/native-final.partial",
            turn.counter()
        ))
    }

    fn reviewer_paths(task: &TaskStatusSnapshot) -> Vec<String> {
        task.reviewers
            .iter()
            .flat_map(|reviewer| reviewer.current_final_message_paths.clone())
            .collect()
    }

    fn joined_reviewer_verdict(task: &TaskStatusSnapshot) -> Option<ReviewerVerdict> {
        if task
            .reviewers
            .iter()
            .any(|reviewer| reviewer.current_verdict.is_none())
        {
            return None;
        }
        Some(
            if task
                .reviewers
                .iter()
                .all(|reviewer| reviewer.current_verdict == Some(ReviewerVerdict::Lgtm))
            {
                ReviewerVerdict::Lgtm
            } else {
                ReviewerVerdict::RequestChanges
            },
        )
    }

    fn new_core() -> SupervisorCore {
        SupervisorCore::new(
            "run-1".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
        )
        .unwrap()
    }

    fn bind(core: &mut SupervisorCore, tasks: Vec<TaskDraft>) -> Vec<SupervisorEffect> {
        let plan_version = core.next_plan_version;
        let plan_hash = core.expected_plan_hash(plan_version, &tasks);
        core.reduce(SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
            github_run_binding: None,
        })
        .unwrap()
    }

    fn authorize(core: &mut SupervisorCore) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::ExecutionAuthorized {
            expected_version: core.version(),
            plan_version: core.plan_version(),
            plan_hash: core.plan_hash().map(str::to_owned),
        })
        .unwrap()
    }

    fn open_runtime(core: &mut SupervisorCore, task_ordinal: usize) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::TaskRuntimeOpened {
            expected_version: core.version(),
            task_ordinal,
        })
        .unwrap()
    }

    fn open_session(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
    ) -> Vec<SupervisorEffect> {
        open_lane_session(
            core,
            task_ordinal,
            WorkerLane::released_for_role(role),
            session,
        )
    }

    fn open_lane_session(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        lane: WorkerLane,
        session: RuntimeSessionKey,
    ) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::RoleSessionOpened {
            expected_version: core.version(),
            task_ordinal,
            lane,
            session,
        })
        .unwrap()
    }

    fn start_turn(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Vec<SupervisorEffect> {
        start_lane_turn(
            core,
            task_ordinal,
            WorkerLane::released_for_role(role),
            purpose,
            session,
            turn,
            completion_token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_lane_turn(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        lane: WorkerLane,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::TurnStarted {
            expected_version: core.version(),
            task_ordinal,
            lane,
            review_generation: lane
                .reviewer_id()
                .map(|_| core.tasks[task_ordinal].review_generation),
            purpose,
            session,
            turn,
            completion_token: completion_token.into(),
        })
        .unwrap()
    }

    fn complete_turn(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
        outcome: RuntimeOutcome,
    ) -> Vec<SupervisorEffect> {
        complete_lane_turn(
            core,
            task_ordinal,
            WorkerLane::released_for_role(role),
            session,
            turn,
            completion_token,
            outcome,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_lane_turn(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        lane: WorkerLane,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
        outcome: RuntimeOutcome,
    ) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::TurnCompleted {
            expected_version: core.version(),
            task_ordinal,
            lane,
            review_generation: lane
                .reviewer_id()
                .map(|_| core.tasks[task_ordinal].review_generation),
            session,
            turn,
            completion_token: completion_token.into(),
            outcome,
            final_message_path: final_message_path(lane.role(), turn),
        })
        .unwrap()
    }

    fn fail_turn_event(
        core: &SupervisorCore,
        active: ActiveIdentity,
        class: RuntimeFailureClass,
        retryable: bool,
        detail: impl Into<String>,
    ) -> SupervisorEvent {
        SupervisorEvent::TurnFailed {
            expected_version: core.version(),
            task_ordinal: active.task,
            lane: active.lane,
            review_generation: active.review_generation,
            session: active.session,
            turn: active.turn,
            completion_token: active.token.into(),
            failure: runtime_failure(class, retryable, detail),
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct ActiveIdentity {
        task: usize,
        role: WorkerRole,
        lane: WorkerLane,
        review_generation: Option<u32>,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        token: &'static str,
    }

    fn start_first_developer(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        session_counter: u64,
        turn_counter: u64,
        token: &'static str,
    ) -> ActiveIdentity {
        assert_eq!(
            open_runtime(core, task_ordinal),
            vec![
                SupervisorEffect::OpenRoleSession {
                    task_ordinal,
                    lane: WorkerLane::Developer,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let session = RuntimeSessionKey::from_counter(session_counter).unwrap();
        assert_eq!(
            open_session(core, task_ordinal, WorkerRole::Developer, session),
            vec![
                SupervisorEffect::StartTurn {
                    task_ordinal,
                    lane: WorkerLane::Developer,
                    review_generation: None,
                    purpose: RuntimeTurnPurpose::InitialDevelopment,
                    session,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let turn = RuntimeTurnKey::from_counter(turn_counter).unwrap();
        assert_eq!(
            start_turn(
                core,
                task_ordinal,
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                session,
                turn,
                token,
            ),
            vec![SupervisorEffect::PublishStatus]
        );
        ActiveIdentity {
            task: task_ordinal,
            role: WorkerRole::Developer,
            lane: WorkerLane::Developer,
            review_generation: None,
            session,
            turn,
            token,
        }
    }

    fn complete_developer_ready(core: &mut SupervisorCore, active: ActiveIdentity) {
        let effects = complete_turn(
            core,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        assert!(
            matches!(
                effects.first(),
                Some(SupervisorEffect::OpenRoleSession {
                    lane: WorkerLane::Reviewer(_),
                    ..
                }) | Some(SupervisorEffect::StartTurn {
                    lane: WorkerLane::Reviewer(_),
                    ..
                })
            ),
            "{effects:?}"
        );
    }

    fn start_reviewer(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        session_counter: u64,
        turn_counter: u64,
        token: &'static str,
        first: bool,
    ) -> ActiveIdentity {
        // The OpenRoleSession / StartTurn effect was already emitted by the
        // developer's completion; this helper only drives what follows.
        let reviewer1 = ReviewerId::Reviewer1;
        let reviewer2 = ReviewerId::Reviewer2;
        let dual_review = core.reviewer_bindings.len() == 2;
        let session = if first {
            let session = RuntimeSessionKey::from_counter(session_counter).unwrap();
            assert_eq!(
                open_lane_session(core, task_ordinal, WorkerLane::Reviewer(reviewer1), session,),
                vec![
                    SupervisorEffect::StartTurn {
                        task_ordinal,
                        lane: WorkerLane::Reviewer(reviewer1),
                        review_generation: Some(core.tasks[task_ordinal].review_generation),
                        purpose: RuntimeTurnPurpose::InitialReview,
                        session,
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            if dual_review {
                let peer_session =
                    RuntimeSessionKey::from_counter(session_counter + 10_000).unwrap();
                open_lane_session(
                    core,
                    task_ordinal,
                    WorkerLane::Reviewer(reviewer2),
                    peer_session,
                );
            }
            session
        } else {
            core.tasks[task_ordinal].reviewer_sessions[&reviewer1]
        };
        let purpose = if first {
            RuntimeTurnPurpose::InitialReview
        } else {
            RuntimeTurnPurpose::ReviewerRereview
        };
        let turn = RuntimeTurnKey::from_counter(turn_counter).unwrap();
        assert_eq!(
            start_lane_turn(
                core,
                task_ordinal,
                WorkerLane::Reviewer(reviewer1),
                purpose,
                session,
                turn,
                token,
            ),
            vec![SupervisorEffect::PublishStatus]
        );
        if dual_review {
            let peer_session = core.tasks[task_ordinal].reviewer_sessions[&reviewer2];
            let peer_turn = RuntimeTurnKey::from_counter(turn_counter + 10_000).unwrap();
            let peer_token: &'static str = Box::leak(format!("{token}-reviewer2").into_boxed_str());
            start_lane_turn(
                core,
                task_ordinal,
                WorkerLane::Reviewer(reviewer2),
                purpose,
                peer_session,
                peer_turn,
                peer_token,
            );
        }
        ActiveIdentity {
            task: task_ordinal,
            role: WorkerRole::Reviewer,
            lane: WorkerLane::Reviewer(reviewer1),
            review_generation: Some(core.tasks[task_ordinal].review_generation),
            session,
            turn,
            token,
        }
    }

    /// A reviewer verdict lands directly from its completed turn: the lane
    /// takes no Git observation around review.
    fn complete_review(
        core: &mut SupervisorCore,
        active: ActiveIdentity,
        outcome: RuntimeOutcome,
    ) -> Vec<SupervisorEffect> {
        let first = complete_lane_turn(
            core,
            active.task,
            active.lane,
            active.session,
            active.turn,
            active.token,
            outcome.clone(),
        );
        if core.reviewer_bindings.len() == 1 {
            return first;
        }
        assert_eq!(first, vec![SupervisorEffect::PublishStatus]);
        let peer_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
        let peer = core
            .active_turns
            .get(&peer_lane)
            .cloned()
            .expect("Reviewer2 remains active until the join");
        complete_lane_turn(
            core,
            peer.task_ordinal,
            peer_lane,
            peer.session,
            peer.turn,
            &peer.completion_token,
            outcome,
        )
    }

    fn correct_and_start_rereview(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        developer_turn_counter: u64,
        reviewer_turn_counter: u64,
        developer_token: &'static str,
        reviewer_token: &'static str,
    ) -> ActiveIdentity {
        let developer_session = core.tasks[task_ordinal].developer_session.unwrap();
        let developer_turn = RuntimeTurnKey::from_counter(developer_turn_counter).unwrap();
        start_turn(
            core,
            task_ordinal,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
            developer_session,
            developer_turn,
            developer_token,
        );
        complete_turn(
            core,
            task_ordinal,
            WorkerRole::Developer,
            developer_session,
            developer_turn,
            developer_token,
            ready(),
        );
        let reviewer_session_counter =
            core.tasks[task_ordinal].reviewer_sessions[&ReviewerId::Reviewer1].counter();
        start_reviewer(
            core,
            task_ordinal,
            reviewer_session_counter,
            reviewer_turn_counter,
            reviewer_token,
            false,
        )
    }

    fn bound_core() -> SupervisorCore {
        let mut core = new_core();
        bind(&mut core, vec![task("one", "/repo", 3)]);
        core
    }

    fn authorized_core() -> SupervisorCore {
        let mut core = bound_core();
        authorize(&mut core);
        core
    }

    /// Authorization already schedules the runtime open; there is no Git
    /// observation between them any more.
    fn pending_runtime_core() -> SupervisorCore {
        authorized_core()
    }

    fn pending_session_core() -> SupervisorCore {
        let mut core = pending_runtime_core();
        open_runtime(&mut core, 0);
        core
    }

    fn pending_turn_core() -> SupervisorCore {
        let mut core = pending_session_core();
        open_session(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeSessionKey::from_counter(1).unwrap(),
        );
        core
    }

    fn active_core() -> (SupervisorCore, ActiveIdentity) {
        let mut core = authorized_core();
        let active = start_first_developer(&mut core, 0, 1, 1, "active");
        (core, active)
    }

    fn awaiting_architect_core() -> SupervisorCore {
        let (mut core, active) = active_core();
        complete_turn(
            &mut core,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            clarification_required(),
        );
        core
    }

    fn completed_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "review", true);
        complete_review(&mut core, reviewer, lgtm());
        core
    }

    fn needs_human_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
        core.reduce(SupervisorEvent::TurnFailed {
            expected_version: core.version(),
            task_ordinal: developer.task,
            lane: developer.lane,
            review_generation: developer.review_generation,
            session: developer.session,
            turn: developer.turn,
            completion_token: developer.token.into(),
            failure: runtime_failure(
                RuntimeFailureClass::Contract,
                false,
                "developer needs a human decision",
            ),
        })
        .unwrap();
        core
    }

    fn failed_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
        core.reduce(SupervisorEvent::TurnFailed {
            expected_version: core.version(),
            task_ordinal: 0,
            lane: WorkerLane::Developer,
            review_generation: None,
            session: developer.session,
            turn: developer.turn,
            completion_token: developer.token.into(),
            failure: runtime_failure(
                RuntimeFailureClass::Canceled,
                false,
                "unexpected provider cancellation",
            ),
        })
        .unwrap();
        core
    }

    fn canceled_core() -> SupervisorCore {
        let mut core = new_core();
        core.reduce(SupervisorEvent::CancelRequested {
            expected_version: 0,
            reason: "stop".into(),
        })
        .unwrap();
        core
    }

    fn active_reviewer_core(max_review_rounds: u8) -> (SupervisorCore, ActiveIdentity) {
        let mut core = new_core();
        bind(&mut core, vec![task("one", "/repo", max_review_rounds)]);
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "developer");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "reviewer", true);
        (core, reviewer)
    }

    fn active_single_reviewer_core(max_review_rounds: u8) -> (SupervisorCore, ActiveIdentity) {
        let mut bindings = default_reviewer_bindings();
        bindings.truncate(1);
        let mut core = SupervisorCore::new_with_reviewer_bindings(
            "run-single".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
            bindings,
        )
        .unwrap();
        let mut spec = task("one", "/repo", MIN_DUAL_REVIEW_ROUNDS);
        spec.max_review_rounds = max_review_rounds;
        bind(&mut core, vec![spec]);
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "single-developer");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "single-reviewer", true);
        (core, reviewer)
    }

    /// The developer's `Ready` completion already scheduled the reviewer's
    /// session open, so this is the reviewing task with that open still pending.
    fn reviewing_pending_session_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
        complete_developer_ready(&mut core, developer);
        core
    }

    fn reviewing_pending_turn_core() -> SupervisorCore {
        let mut core = reviewing_pending_session_core();
        open_session(
            &mut core,
            0,
            WorkerRole::Reviewer,
            RuntimeSessionKey::from_counter(2).unwrap(),
        );
        core
    }

    fn review_exhausted_core() -> SupervisorCore {
        let (mut core, reviewer) = active_reviewer_core(MIN_DUAL_REVIEW_ROUNDS);
        exhaust_review(&mut core, reviewer);
        core
    }

    fn exhaust_review(
        core: &mut SupervisorCore,
        mut reviewer: ActiveIdentity,
    ) -> Vec<SupervisorEffect> {
        let maximum = core.tasks[reviewer.task].spec.max_review_rounds;
        for generation in 1..=maximum {
            let effects = complete_review(core, reviewer, request_changes());
            if generation == maximum {
                return effects;
            }
            let developer_turn = u64::from(generation) * 2 + 1;
            let developer_token: &'static str =
                Box::leak(format!("exhaust-developer-{}", generation + 1).into_boxed_str());
            let reviewer_token: &'static str =
                Box::leak(format!("exhaust-reviewer-{}", generation + 1).into_boxed_str());
            reviewer = correct_and_start_rereview(
                core,
                0,
                developer_turn,
                developer_turn + 1,
                developer_token,
                reviewer_token,
            );
        }
        unreachable!("bounded review loop returns at the configured maximum")
    }

    fn active_canceled_core() -> SupervisorCore {
        let (mut core, _active) = active_core();
        core.reduce(SupervisorEvent::CancelRequested {
            expected_version: core.version(),
            reason: "stop active task".into(),
        })
        .unwrap();
        core
    }

    fn plan_event(core: &SupervisorCore, key: &str) -> SupervisorEvent {
        plan_event_for(core, vec![task(key, "/replacement", 2)])
    }

    fn plan_event_for(core: &SupervisorCore, tasks: Vec<TaskDraft>) -> SupervisorEvent {
        let plan_version = core.next_plan_version;
        let plan_hash = core.expected_plan_hash(plan_version, &tasks);
        SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
            github_run_binding: None,
        }
    }

    fn plan_result(tasks: Vec<TaskDraft>) -> Result<SupervisorCore, SupervisorError> {
        let mut core = new_core();
        let event = plan_event_for(&core, tasks);
        core.reduce(event)?;
        Ok(core)
    }

    fn generic_event(core: &SupervisorCore, kind: SupervisorEventKind) -> SupervisorEvent {
        match kind {
            SupervisorEventKind::PlanBound => plan_event(core, "replacement"),
            SupervisorEventKind::ExecutionAuthorized => SupervisorEvent::ExecutionAuthorized {
                expected_version: core.version(),
                plan_version: core.plan_version().or(Some(1)),
                plan_hash: core.plan_hash().map(str::to_owned).or(Some("a".repeat(64))),
            },
            SupervisorEventKind::RepositoryPrepared => SupervisorEvent::RepositoryPrepared {
                expected_version: core.version(),
                operation_id: "github.prepare.run.0.supervisor".into(),
                base_sha: "a".repeat(40),
                branch: "hcom/run-test-aaaaaaaaaaaa".into(),
                worktree_path: "/project/hcom-tasks/run-1/repository".into(),
            },
            SupervisorEventKind::CandidatePublished => SupervisorEvent::CandidatePublished {
                expected_version: core.version(),
                operation_id: "github.candidate.0.1.supervisor".into(),
                task_ordinal: 0,
                generation: 1,
                previous_head_sha: "a".repeat(40),
                head_sha: "b".repeat(40),
                pr_number: 1,
                pr_node_id: "PR_node".into(),
                pr_url: "https://github.com/owner/repo/pull/1".into(),
                pr_actor_bot_user_id: 2,
                check_run_id: 1,
                check_url: "https://github.com/owner/repo/runs/1".into(),
                check_actor_app_id: 1,
            },
            SupervisorEventKind::ReviewerReviewPublished => {
                SupervisorEvent::ReviewerReviewPublished {
                    expected_version: core.version(),
                    operation_id: "github.review.0.1.reviewer1".into(),
                    task_ordinal: 0,
                    reviewer_id: ReviewerId::Reviewer1,
                    generation: 1,
                    head_sha: "b".repeat(40),
                    verdict: ReviewerVerdict::Lgtm,
                    review_id: 1,
                    review_url: "https://github.com/owner/repo/pull/1#review-1".into(),
                    actor_bot_user_id: 3,
                    final_artifact_sha256: "c".repeat(64),
                }
            }
            SupervisorEventKind::ReviewCheckPublished => SupervisorEvent::ReviewCheckPublished {
                expected_version: core.version(),
                operation_id: "github.check.0.1.supervisor".into(),
                task_ordinal: 0,
                generation: 1,
                head_sha: "b".repeat(40),
                check_run_id: 1,
                check_url: "https://github.com/owner/repo/runs/1".into(),
                state: "success".into(),
                actor_app_id: 1,
            },
            SupervisorEventKind::MergeGateObserved => SupervisorEvent::MergeGateObserved {
                expected_version: core.version(),
                operation_id: "github.merge_gate.run.0.supervisor".into(),
                pr_number: 1,
                final_head_sha: "b".repeat(40),
                base_sha: "a".repeat(40),
                ruleset_attestation_sha256: "d".repeat(64),
                check_run_id: 1,
            },
            SupervisorEventKind::PullRequestMerged => SupervisorEvent::PullRequestMerged {
                expected_version: core.version(),
                operation_id: "github.merge.run.0.supervisor".into(),
                pr_number: 1,
                final_head_sha: "b".repeat(40),
                merge_sha: "e".repeat(40),
                merge_url: "https://github.com/owner/repo/commit/merge".into(),
                actor_bot_user_id: 1,
                merge_evidence_durable: true,
            },
            SupervisorEventKind::GitHubRunFinalized => SupervisorEvent::GitHubRunFinalized {
                expected_version: core.version(),
                operation_id: "github.finalize.run.0.supervisor".into(),
                pr_number: 1,
                final_head_sha: "b".repeat(40),
                merge_sha: "e".repeat(40),
                finalization: GitHubFinalizationSnapshot {
                    local_worktree_removed: true,
                    local_ref_removed: true,
                    remote_ref_outcome: Some("deleted".into()),
                },
            },
            SupervisorEventKind::GitHubOperationFailed => SupervisorEvent::GitHubOperationFailed {
                expected_version: core.version(),
                operation_id: "github.prepare.run.0.supervisor".into(),
                detail: "bounded failure".into(),
                partial_operation: None,
            },
            SupervisorEventKind::TaskRuntimeOpened => SupervisorEvent::TaskRuntimeOpened {
                expected_version: core.version(),
                task_ordinal: 0,
            },
            SupervisorEventKind::RoleSessionOpened => SupervisorEvent::RoleSessionOpened {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
            },
            SupervisorEventKind::TurnStarted => SupervisorEvent::TurnStarted {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                purpose: RuntimeTurnPurpose::InitialDevelopment,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "generic".into(),
            },
            SupervisorEventKind::TurnCompleted => SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "active".into(),
                outcome: ready(),
                final_message_path: PathBuf::from("/artifacts/developer/final.md"),
            },
            SupervisorEventKind::ClarificationSubmitted => {
                let pending = core.pending_architect_action.clone().unwrap_or(
                    PendingArchitectActionSnapshot {
                        task_ordinal: 0,
                        task_key: "one".into(),
                        sequence: 1,
                        reason: ArchitectActionReason::Clarification,
                        developer_request_path: "/artifacts/developer/final.md".into(),
                        clarification_output_path:
                            "/project/hcom-tasks/run-1/one/clarification/turn-1.md".into(),
                        clarification_rounds_used: 0,
                        max_clarification_rounds: 2,
                        human_decision_required: false,
                        published_version: core.version(),
                    },
                );
                SupervisorEvent::ClarificationSubmitted {
                    expected_version: core.version(),
                    task_ordinal: pending.task_ordinal as usize,
                    task_key: pending.task_key,
                    action_sequence: pending.sequence,
                    developer_request_path: pending.developer_request_path,
                    clarification_document_path: pending.clarification_output_path,
                    human_decision_confirmed: pending.human_decision_required,
                }
            }
            SupervisorEventKind::ClarificationHumanRequired => {
                let pending = core.pending_architect_action.clone().unwrap_or(
                    PendingArchitectActionSnapshot {
                        task_ordinal: 0,
                        task_key: "one".into(),
                        sequence: 1,
                        reason: ArchitectActionReason::Clarification,
                        developer_request_path: "/artifacts/developer/final.md".into(),
                        clarification_output_path:
                            "/project/hcom-tasks/run-1/one/clarification/turn-1.md".into(),
                        clarification_rounds_used: 0,
                        max_clarification_rounds: 2,
                        human_decision_required: false,
                        published_version: core.version(),
                    },
                );
                SupervisorEvent::ClarificationHumanRequired {
                    expected_version: core.version(),
                    task_ordinal: pending.task_ordinal as usize,
                    task_key: pending.task_key,
                    action_sequence: pending.sequence,
                    developer_request_path: pending.developer_request_path,
                }
            }
            SupervisorEventKind::TurnFailed => SupervisorEvent::TurnFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "active".into(),
                failure: runtime_failure(RuntimeFailureClass::Process, false, "provider exited"),
            },
            SupervisorEventKind::DriverFailed => SupervisorEvent::DriverFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                failure: DriverFailure {
                    class: DriverFailureClass::Runtime,
                    detail: "bounded driver failure".into(),
                },
            },
            SupervisorEventKind::Timeout => SupervisorEvent::Timeout {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "active".into(),
            },
            SupervisorEventKind::CancelRequested => SupervisorEvent::CancelRequested {
                expected_version: core.version(),
                reason: "matrix cancel".into(),
            },
            SupervisorEventKind::ParentStopping => SupervisorEvent::ParentStopping {
                expected_version: core.version(),
            },
            SupervisorEventKind::StatusRequested => SupervisorEvent::StatusRequested,
        }
    }

    fn running_fixture(kind: SupervisorEventKind) -> (SupervisorCore, SupervisorEvent) {
        match kind {
            SupervisorEventKind::TaskRuntimeOpened => {
                let core = pending_runtime_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::RoleSessionOpened => {
                let core = pending_session_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::TurnStarted => {
                let core = pending_turn_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::TurnCompleted
            | SupervisorEventKind::TurnFailed
            | SupervisorEventKind::Timeout => {
                let (core, _) = active_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::ClarificationSubmitted
            | SupervisorEventKind::ClarificationHumanRequired => {
                let core = awaiting_architect_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            _ => {
                let core = authorized_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
        }
    }

    fn task_state_fixture(
        state: TaskState,
        kind: SupervisorEventKind,
    ) -> (SupervisorCore, SupervisorEvent, bool) {
        let relevant = [
            SupervisorEventKind::TaskRuntimeOpened,
            SupervisorEventKind::RoleSessionOpened,
            SupervisorEventKind::TurnStarted,
            SupervisorEventKind::TurnCompleted,
            SupervisorEventKind::TurnFailed,
            SupervisorEventKind::Timeout,
        ];
        assert!(relevant.contains(&kind));

        match state {
            TaskState::Pending => {
                let core = if kind == SupervisorEventKind::TaskRuntimeOpened {
                    pending_runtime_core()
                } else {
                    authorized_core()
                };
                let event = generic_event(&core, kind);
                let accepted = kind == SupervisorEventKind::TaskRuntimeOpened;
                (core, event, accepted)
            }
            TaskState::Developing => {
                let (core, event) = match kind {
                    SupervisorEventKind::RoleSessionOpened => {
                        let core = pending_session_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::TurnStarted => {
                        let core = pending_turn_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::TurnCompleted
                    | SupervisorEventKind::TurnFailed
                    | SupervisorEventKind::Timeout => {
                        let (core, _) = active_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::TaskRuntimeOpened => {
                        let (core, _) = active_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    _ => unreachable!(),
                };
                (core, event, kind != SupervisorEventKind::TaskRuntimeOpened)
            }
            TaskState::AwaitingArchitectAction => {
                let core = awaiting_architect_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::PublishingCandidate | TaskState::PublishingReview => {
                let core = active_core().0;
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::Reviewing => {
                let (core, event) = match kind {
                    SupervisorEventKind::RoleSessionOpened => {
                        let core = reviewing_pending_session_core();
                        let event = SupervisorEvent::RoleSessionOpened {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                            session: RuntimeSessionKey::from_counter(2).unwrap(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TurnStarted => {
                        let core = reviewing_pending_turn_core();
                        let event = SupervisorEvent::TurnStarted {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                            review_generation: Some(core.tasks[0].review_generation),
                            purpose: RuntimeTurnPurpose::InitialReview,
                            session: RuntimeSessionKey::from_counter(2).unwrap(),
                            turn: RuntimeTurnKey::from_counter(2).unwrap(),
                            completion_token: "reviewer".into(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TurnCompleted
                    | SupervisorEventKind::TurnFailed
                    | SupervisorEventKind::Timeout => {
                        let (core, _) = active_reviewer_core(2);
                        let event = match kind {
                            SupervisorEventKind::TurnCompleted => SupervisorEvent::TurnCompleted {
                                expected_version: core.version(),
                                task_ordinal: 0,
                                lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                                review_generation: Some(core.tasks[0].review_generation),
                                session: RuntimeSessionKey::from_counter(2).unwrap(),
                                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                                completion_token: "reviewer".into(),
                                outcome: lgtm(),
                                final_message_path: PathBuf::from("/artifacts/reviewer/final.md"),
                            },
                            SupervisorEventKind::TurnFailed => SupervisorEvent::TurnFailed {
                                expected_version: core.version(),
                                task_ordinal: 0,
                                lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                                review_generation: Some(core.tasks[0].review_generation),
                                session: RuntimeSessionKey::from_counter(2).unwrap(),
                                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                                completion_token: "reviewer".into(),
                                failure: runtime_failure(
                                    RuntimeFailureClass::Process,
                                    false,
                                    "exit",
                                ),
                            },
                            SupervisorEventKind::Timeout => SupervisorEvent::Timeout {
                                expected_version: core.version(),
                                task_ordinal: 0,
                                lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                                review_generation: Some(core.tasks[0].review_generation),
                                session: RuntimeSessionKey::from_counter(2).unwrap(),
                                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                                completion_token: "reviewer".into(),
                            },
                            _ => unreachable!(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TaskRuntimeOpened => {
                        let (core, _) = active_reviewer_core(2);
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    _ => unreachable!(),
                };
                // A reviewing task accepts no second runtime open.
                let accepted = kind != SupervisorEventKind::TaskRuntimeOpened;
                (core, event, accepted)
            }
            TaskState::Lgtm => {
                let core = completed_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::ReviewExhausted => {
                let core = review_exhausted_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::NeedsHuman => {
                let core = needs_human_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::Failed => {
                let core = failed_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
            TaskState::Canceled => {
                let core = active_canceled_core();
                let event = generic_event(&core, kind);
                (core, event, false)
            }
        }
    }

    #[test]
    fn p0_core_skeleton_is_pure_and_empty() {
        let core = SupervisorCore::skeleton("run-1".into(), PathBuf::from("/project"));
        assert_eq!(core.session_state(), SessionState::AwaitingPlan);
        assert_eq!(core.version(), 0);
        assert_eq!(core.run_id(), "run-1");
        assert_eq!(core.project_root(), &PathBuf::from("/project"));
        assert!(core.tasks().is_empty());
        assert_eq!(core.current_task(), None);
    }

    #[test]
    fn terminal_core_creates_a_fresh_run_without_mutating_terminal_evidence() {
        let terminal = completed_core();
        let before = terminal.clone();
        let next = terminal.next_run("run-next".into()).unwrap();

        assert_eq!(terminal, before);
        assert_eq!(next.run_id(), "run-next");
        assert_eq!(next.project_root(), terminal.project_root());
        assert_eq!(next.profile_hash(), terminal.profile_hash());
        assert_eq!(next.version(), terminal.version() + 1);
        assert_eq!(next.session_state(), SessionState::AwaitingPlan);
        assert!(next.tasks().is_empty());
        assert_eq!(next.plan_version(), None);
        assert_eq!(next.plan_hash(), None);
        assert_eq!(next.current_task(), None);
        assert!(next.snapshot().terminal_detail.is_none());

        let nonterminal = new_core();
        let error = nonterminal.next_run("run-too-early".into()).unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidTransition);
    }

    #[test]
    fn plan_hash_is_bound_to_the_exact_run() {
        let tasks = vec![task("one", "/repo", 2)];
        let first =
            SupervisorCore::new("run-one".into(), PathBuf::from("/project"), "0".repeat(64))
                .unwrap();
        let second =
            SupervisorCore::new("run-two".into(), PathBuf::from("/project"), "0".repeat(64))
                .unwrap();
        assert_ne!(
            first.expected_plan_hash(1, &tasks),
            second.expected_plan_hash(1, &tasks)
        );
    }

    #[test]
    fn plan_hash_is_bound_to_the_exact_session_profile() {
        let tasks = vec![task("one", "/repo", 2)];
        let first =
            SupervisorCore::new("run-one".into(), PathBuf::from("/project"), "0".repeat(64))
                .unwrap();
        let second =
            SupervisorCore::new("run-one".into(), PathBuf::from("/project"), "1".repeat(64))
                .unwrap();
        assert_ne!(
            first.expected_plan_hash(1, &tasks),
            second.expected_plan_hash(1, &tasks)
        );
    }

    #[test]
    fn plan_hash_is_bound_to_single_or_dual_reviewer_topology() {
        let dual = new_core();
        let mut bindings = default_reviewer_bindings();
        bindings.truncate(1);
        let single = SupervisorCore::new_with_reviewer_bindings(
            "run-1".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
            bindings,
        )
        .unwrap();
        let tasks = vec![task("one", "/repo", MIN_DUAL_REVIEW_ROUNDS)];
        assert_ne!(
            dual.expected_plan_hash(1, &tasks),
            single.expected_plan_hash(1, &tasks)
        );
    }

    fn github_delivery_binding() -> DeliveryBinding {
        let app =
            |id: u64, slug: &str, permissions: &[(&str, GitHubPermissionLevel)]| GitHubAppBinding {
                app_id: id,
                installation_id: id + 10,
                slug: slug.into(),
                bot_user_id: id + 20,
                effective_permissions: permissions
                    .iter()
                    .map(|(name, level)| ((*name).into(), *level))
                    .collect(),
            };
        let reviewer_permissions = [("pull_requests", GitHubPermissionLevel::Write)];
        DeliveryBinding::GitHubPullRequest {
            binding: Box::new(GitHubPullRequestBinding {
                owner: "owner".into(),
                repository: "repo".into(),
                repository_id: 99,
                visibility: "private".into(),
                local_repository_root: "/repo".into(),
                base_branch: "master".into(),
                merge_method: "squash".into(),
                merge_wait_seconds: 21_600,
                delete_remote_branch_after_merge: true,
                architect_app: app(
                    1,
                    "hcom-arch",
                    &[
                        ("administration", GitHubPermissionLevel::Read),
                        ("checks", GitHubPermissionLevel::Write),
                        ("contents", GitHubPermissionLevel::Write),
                        ("pull_requests", GitHubPermissionLevel::Write),
                    ],
                ),
                developer_app: app(
                    2,
                    "hcom-dev",
                    &[
                        ("contents", GitHubPermissionLevel::Write),
                        ("pull_requests", GitHubPermissionLevel::Write),
                    ],
                ),
                reviewer_apps: vec![
                    GitHubReviewerAppBinding {
                        reviewer_id: ReviewerId::Reviewer1,
                        app: app(3, "hcom-reviewer1", &reviewer_permissions),
                    },
                    GitHubReviewerAppBinding {
                        reviewer_id: ReviewerId::Reviewer2,
                        app: app(4, "hcom-reviewer2", &reviewer_permissions),
                    },
                ],
                review_check_name: "hcom/review".into(),
            }),
        }
    }

    fn github_core(tasks: Vec<TaskDraft>, single_review: bool) -> SupervisorCore {
        let mut reviewer_bindings = default_reviewer_bindings();
        let mut delivery = github_delivery_binding();
        if single_review {
            reviewer_bindings.truncate(1);
            let DeliveryBinding::GitHubPullRequest { binding } = &mut delivery else {
                unreachable!()
            };
            binding.reviewer_apps.truncate(1);
        }
        let mut core = SupervisorCore::new_with_delivery_binding(
            "run-github".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
            reviewer_bindings,
            delivery,
        )
        .unwrap();
        let inspection = GitHubInspectionBinding {
            inspected_repository_id: 99,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: "d".repeat(64),
            inspection_id: "inspect-github".into(),
        };
        let plan_hash = core.expected_plan_hash_for_inspection(1, &tasks, Some(&inspection));
        let run_binding = GitHubRunBinding {
            inspected_repository_id: inspection.inspected_repository_id,
            expected_base_ref: inspection.expected_base_ref,
            expected_base_sha: inspection.expected_base_sha,
            ruleset_attestation_sha256: inspection.ruleset_attestation_sha256,
            inspection_id: inspection.inspection_id,
            generated_run_branch: format!("hcom/run-github-{}", &plan_hash[..12]),
        };
        core.reduce(SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version: 1,
            plan_hash,
            tasks,
            github_run_binding: Some(run_binding),
        })
        .unwrap();
        core
    }

    fn prepare_github(core: &mut SupervisorCore) -> Vec<SupervisorEffect> {
        let effects = authorize(core);
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PrepareGitHubRunRepository(request) => Some(request.clone()),
                _ => None,
            })
            .expect("GitHub authorization prepares the repository");
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, SupervisorEffect::OpenTaskRuntime { .. })),
            "no worker may open before repository preparation"
        );
        let mut observed = effects;
        observed.extend(
            core.reduce(SupervisorEvent::RepositoryPrepared {
                expected_version: core.version(),
                operation_id: request.operation_id,
                base_sha: request.run_binding.expected_base_sha,
                branch: request.run_binding.generated_run_branch,
                worktree_path: "/project/hcom-tasks/run-github/repository".into(),
            })
            .unwrap(),
        );
        observed
    }

    fn publish_github_candidate(
        core: &mut SupervisorCore,
        active: ActiveIdentity,
        head_sha: &str,
    ) -> Vec<SupervisorEffect> {
        let mut effects = complete_lane_turn(
            core,
            active.task,
            active.lane,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PublishDeveloperCandidate(request) => Some(request.clone()),
                _ => None,
            })
            .expect("Developer READY publishes before review");
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                SupervisorEffect::OpenRoleSession {
                    lane: WorkerLane::Reviewer(_),
                    ..
                } | SupervisorEffect::StartTurn {
                    lane: WorkerLane::Reviewer(_),
                    ..
                }
            )),
            "Reviewer cannot start before exact candidate publication"
        );
        let pr_number = request.existing_pr_number.unwrap_or(7);
        effects.extend(
            core.reduce(SupervisorEvent::CandidatePublished {
                expected_version: core.version(),
                operation_id: request.operation_id,
                task_ordinal: request.task_ordinal,
                generation: request.generation,
                previous_head_sha: request.previous_head_sha,
                head_sha: head_sha.into(),
                pr_number,
                pr_node_id: "PR_kwDOhcom".into(),
                pr_url: "https://github.com/owner/repo/pull/7".into(),
                pr_actor_bot_user_id: 22,
                check_run_id: 1000
                    + u64::try_from(request.task_ordinal).unwrap() * 100
                    + u64::from(request.generation),
                check_url: format!(
                    "https://github.com/owner/repo/runs/{}",
                    1000 + u64::try_from(request.task_ordinal).unwrap() * 100
                        + u64::from(request.generation)
                ),
                check_actor_app_id: 1,
            })
            .unwrap(),
        );
        effects
    }

    fn start_github_reviewers(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        session_seed: u64,
        turn_seed: u64,
        first: bool,
    ) {
        let generation = core.tasks[task_ordinal].review_generation;
        for (offset, reviewer_id) in core.reviewer_ids().into_iter().enumerate() {
            let lane = WorkerLane::Reviewer(reviewer_id);
            let session = if first {
                let session =
                    RuntimeSessionKey::from_counter(session_seed + u64::try_from(offset).unwrap())
                        .unwrap();
                open_lane_session(core, task_ordinal, lane, session);
                session
            } else {
                core.tasks[task_ordinal].reviewer_sessions[&reviewer_id]
            };
            start_lane_turn(
                core,
                task_ordinal,
                lane,
                if first {
                    RuntimeTurnPurpose::InitialReview
                } else {
                    RuntimeTurnPurpose::ReviewerRereview
                },
                session,
                RuntimeTurnKey::from_counter(turn_seed + u64::try_from(offset).unwrap()).unwrap(),
                &format!("github-review-{task_ordinal}-{generation}-{offset}"),
            );
        }
    }

    fn publish_github_reviews(
        core: &mut SupervisorCore,
        outcomes: &[(ReviewerId, ReviewerVerdict)],
    ) -> Vec<SupervisorEffect> {
        let task_ordinal = core.current_task.unwrap();
        let generation = core.tasks[task_ordinal].review_generation;
        let mut requests = Vec::new();
        let mut observed = Vec::new();
        for (reviewer_id, verdict) in outcomes {
            let lane = WorkerLane::Reviewer(*reviewer_id);
            let active = core
                .active_turns
                .get(&lane)
                .cloned()
                .expect("active same-generation Reviewer exists");
            let outcome = match verdict {
                ReviewerVerdict::Lgtm => lgtm(),
                ReviewerVerdict::RequestChanges => request_changes(),
            };
            let effects = complete_lane_turn(
                core,
                task_ordinal,
                lane,
                active.session,
                active.turn,
                &active.completion_token,
                outcome,
            );
            requests.push(
                effects
                    .iter()
                    .find_map(|effect| match effect {
                        SupervisorEffect::PublishReviewerReview(request) => Some(request.clone()),
                        _ => None,
                    })
                    .expect("native Reviewer final requires publication"),
            );
            observed.extend(effects);
        }
        assert_eq!(
            core.tasks[task_ordinal].review_round,
            generation - 1,
            "native results do not cross the published-review barrier"
        );
        for (index, request) in requests.into_iter().rev().enumerate() {
            let actor_bot_user_id = match request.reviewer_id {
                ReviewerId::Reviewer1 => 23,
                ReviewerId::Reviewer2 => 24,
            };
            observed.extend(
                core.reduce(SupervisorEvent::ReviewerReviewPublished {
                    expected_version: core.version(),
                    operation_id: request.operation_id,
                    task_ordinal: request.task_ordinal,
                    reviewer_id: request.reviewer_id,
                    generation: request.generation,
                    head_sha: request.head_sha,
                    verdict: request.verdict,
                    review_id: 2000 + u64::from(generation) * 10 + u64::try_from(index).unwrap(),
                    review_url: format!(
                        "https://github.com/owner/repo/pull/7#pullrequestreview-{}",
                        2000 + u64::from(generation) * 10 + u64::try_from(index).unwrap()
                    ),
                    actor_bot_user_id,
                    final_artifact_sha256: format!("{:064x}", 3000 + index),
                })
                .unwrap(),
            );
        }
        observed
    }

    fn publish_github_check(
        core: &mut SupervisorCore,
        effects: &[SupervisorEffect],
    ) -> Vec<SupervisorEffect> {
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::OpenOrUpdateReviewCheck(request) => Some(request.clone()),
                _ => None,
            })
            .expect("joined published reviews conclude the exact-head Check");
        core.reduce(SupervisorEvent::ReviewCheckPublished {
            expected_version: core.version(),
            operation_id: request.operation_id,
            task_ordinal: request.task_ordinal,
            generation: request.generation,
            head_sha: request.head_sha,
            check_run_id: request.check_run_id,
            check_url: request.check_url,
            state: request.conclusion.as_str().into(),
            actor_app_id: 1,
        })
        .unwrap()
    }

    #[test]
    fn github_plan_hash_binds_delivery_inspection_and_cycle_free_branch() {
        let tasks = vec![task("one", "/repo", MIN_DUAL_REVIEW_ROUNDS)];
        let delivery = github_delivery_binding();
        let mut github = SupervisorCore::new_with_delivery_binding(
            "run-one".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
            default_reviewer_bindings(),
            delivery,
        )
        .unwrap();
        let inspection = GitHubInspectionBinding {
            inspected_repository_id: 99,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: "b".repeat(64),
            inspection_id: "inspect-one".into(),
        };
        let hash = github.expected_plan_hash_for_inspection(1, &tasks, Some(&inspection));
        assert_ne!(hash, new_core().expected_plan_hash(1, &tasks));
        let mut actor_delivery = github_delivery_binding();
        let DeliveryBinding::GitHubPullRequest { binding } = &mut actor_delivery else {
            unreachable!()
        };
        binding.architect_app.bot_user_id += 1;
        let actor_core = SupervisorCore::new_with_delivery_binding(
            "run-one".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
            default_reviewer_bindings(),
            actor_delivery,
        )
        .unwrap();
        assert_ne!(
            hash,
            actor_core.expected_plan_hash_for_inspection(1, &tasks, Some(&inspection))
        );
        let mut moved_base = inspection.clone();
        moved_base.expected_base_sha = "c".repeat(40);
        assert_ne!(
            hash,
            github.expected_plan_hash_for_inspection(1, &tasks, Some(&moved_base))
        );
        let mut moved_rules = inspection.clone();
        moved_rules.ruleset_attestation_sha256 = "d".repeat(64);
        assert_ne!(
            hash,
            github.expected_plan_hash_for_inspection(1, &tasks, Some(&moved_rules))
        );
        let mut newer_inspection = inspection.clone();
        newer_inspection.inspection_id = "inspect-two".into();
        assert_ne!(
            hash,
            github.expected_plan_hash_for_inspection(1, &tasks, Some(&newer_inspection))
        );
        let expected_branch = format!("hcom/run-one-{}", &hash[..12]);
        let run_binding = GitHubRunBinding {
            inspected_repository_id: inspection.inspected_repository_id,
            expected_base_ref: inspection.expected_base_ref.clone(),
            expected_base_sha: inspection.expected_base_sha.clone(),
            ruleset_attestation_sha256: inspection.ruleset_attestation_sha256.clone(),
            inspection_id: inspection.inspection_id.clone(),
            generated_run_branch: expected_branch.clone(),
        };
        github
            .reduce(SupervisorEvent::PlanBound {
                expected_version: 0,
                plan_version: 1,
                plan_hash: hash.clone(),
                tasks,
                github_run_binding: Some(run_binding.clone()),
            })
            .unwrap();
        let snapshot = github.snapshot();
        assert_eq!(snapshot.plan_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(snapshot.github.unwrap().run_binding, Some(run_binding));
        assert_eq!(
            snapshot.tasks[0].branch.as_deref(),
            Some(expected_branch.as_str())
        );
        assert_eq!(
            snapshot.tasks[0].base_revision.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn github_dual_review_correction_multi_task_chain_merges_only_after_finalization() {
        let mut core = github_core(
            vec![
                task("one", "/repo", MIN_DUAL_REVIEW_ROUNDS),
                task("two", "/repo", MIN_DUAL_REVIEW_ROUNDS),
            ],
            false,
        );
        let mut observed = BTreeSet::new();
        let mut record = |effects: &[SupervisorEffect]| {
            observed.extend(effects.iter().map(SupervisorEffect::kind));
        };

        let effects = prepare_github(&mut core);
        record(&effects);
        let developer = start_first_developer(&mut core, 0, 1, 1, "github-dev-one");
        let effects = publish_github_candidate(&mut core, developer, &"b".repeat(40));
        record(&effects);
        assert_eq!(core.snapshot().tasks[0].base_revision, Some("a".repeat(40)));
        start_github_reviewers(&mut core, 0, 10, 20, true);

        let effects = publish_github_reviews(
            &mut core,
            &[
                (ReviewerId::Reviewer1, ReviewerVerdict::RequestChanges),
                (ReviewerId::Reviewer2, ReviewerVerdict::Lgtm),
            ],
        );
        record(&effects);
        let check = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::OpenOrUpdateReviewCheck(request) => Some(request),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            check.conclusion,
            GitHubReviewCheckConclusion::ActionRequired
        );
        assert_eq!(check.ruleset_attestation_sha256, "d".repeat(64));
        let effects = publish_github_check(&mut core, &effects);
        record(&effects);
        let developer_session = core.tasks[0].developer_session.unwrap();
        let correction = ActiveIdentity {
            task: 0,
            role: WorkerRole::Developer,
            lane: WorkerLane::Developer,
            review_generation: None,
            session: developer_session,
            turn: RuntimeTurnKey::from_counter(30).unwrap(),
            token: "github-dev-correction",
        };
        start_lane_turn(
            &mut core,
            0,
            correction.lane,
            RuntimeTurnPurpose::DeveloperCorrection,
            correction.session,
            correction.turn,
            correction.token,
        );
        let effects = publish_github_candidate(&mut core, correction, &"c".repeat(40));
        record(&effects);
        start_github_reviewers(&mut core, 0, 10, 40, false);
        let effects = publish_github_reviews(
            &mut core,
            &[
                (ReviewerId::Reviewer1, ReviewerVerdict::Lgtm),
                (ReviewerId::Reviewer2, ReviewerVerdict::Lgtm),
            ],
        );
        record(&effects);
        let effects = publish_github_check(&mut core, &effects);
        record(&effects);
        assert_eq!(core.tasks[0].github_final_head_sha, Some("c".repeat(40)));

        let developer = start_first_developer(&mut core, 1, 50, 50, "github-dev-two");
        let effects = publish_github_candidate(&mut core, developer, &"e".repeat(40));
        record(&effects);
        assert_eq!(core.snapshot().tasks[1].base_revision, Some("c".repeat(40)));
        assert_eq!(core.snapshot().github.unwrap().pr_number, Some(7));
        start_github_reviewers(&mut core, 1, 60, 60, true);
        let effects = publish_github_reviews(
            &mut core,
            &[
                (ReviewerId::Reviewer1, ReviewerVerdict::Lgtm),
                (ReviewerId::Reviewer2, ReviewerVerdict::Lgtm),
            ],
        );
        record(&effects);
        let effects = publish_github_check(&mut core, &effects);
        record(&effects);
        assert_eq!(core.session_state(), SessionState::Running);
        assert_eq!(
            core.snapshot().github.unwrap().phase,
            Some(GitHubDeliveryPhase::AwaitingMerge)
        );
        let gate = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::WaitForMergeGate(request) => Some(request.clone()),
                _ => None,
            })
            .expect("all-task LGTM waits on the exact final head");
        assert_eq!(gate.final_head_sha, "e".repeat(40));
        assert!(gate.tasks.iter().all(|task| {
            task.outcome == Some(TaskCompletionOutcome::Lgtm) && task.task_final_head_sha.is_some()
        }));

        let effects = core
            .reduce(SupervisorEvent::MergeGateObserved {
                expected_version: core.version(),
                operation_id: gate.operation_id,
                pr_number: gate.pr_number,
                final_head_sha: gate.final_head_sha,
                base_sha: "a".repeat(40),
                ruleset_attestation_sha256: "d".repeat(64),
                check_run_id: gate.check_run_id,
            })
            .unwrap();
        record(&effects);
        let merge = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::MergePullRequest(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        let effects = core
            .reduce(SupervisorEvent::PullRequestMerged {
                expected_version: core.version(),
                operation_id: merge.operation_id,
                pr_number: merge.pr_number,
                final_head_sha: merge.final_head_sha,
                merge_sha: "f".repeat(40),
                merge_url: "https://github.com/owner/repo/commit/final".into(),
                actor_bot_user_id: 21,
                merge_evidence_durable: true,
            })
            .unwrap();
        record(&effects);
        assert_eq!(core.session_state(), SessionState::Running);
        assert_eq!(
            core.snapshot().github.unwrap().phase,
            Some(GitHubDeliveryPhase::Finalizing)
        );
        let finalize = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::FinalizeGitHubRun(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        let mut finalization_failed = core.clone();
        let failure_effects = finalization_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: finalization_failed.version(),
                operation_id: finalize.operation_id.clone(),
                detail: "required local cleanup could not be confirmed".into(),
                partial_operation: None,
            })
            .unwrap();
        assert!(failure_effects.iter().all(|effect| {
            !matches!(
                effect,
                SupervisorEffect::WaitForMergeGate(_)
                    | SupervisorEffect::MergePullRequest(_)
                    | SupervisorEffect::FinalizeGitHubRun(_)
            )
        }));
        let failed_snapshot = finalization_failed.snapshot();
        assert_eq!(failed_snapshot.state, SessionState::NeedsHuman);
        let failed_github = failed_snapshot.github.unwrap();
        assert!(failed_github.merge_already_confirmed);
        assert_eq!(failed_github.merge_sha, Some("f".repeat(40)));
        assert_eq!(failed_github.phase, Some(GitHubDeliveryPhase::Finalizing));
        assert!(failed_github.finalization.is_none());
        assert!(failed_github.preserved_branch.is_some());
        assert!(failed_github.preserved_worktree.is_some());

        let fresh = finalization_failed
            .next_run("run-fresh-after-merge".into())
            .unwrap();
        let fresh_snapshot = fresh.snapshot();
        assert_eq!(fresh_snapshot.state, SessionState::AwaitingPlan);
        assert_eq!(fresh_snapshot.run_id, "run-fresh-after-merge");
        let fresh_github = fresh_snapshot.github.unwrap();
        assert!(!fresh_github.merge_already_confirmed);
        assert!(fresh_github.run_binding.is_none());
        assert!(fresh_github.pr_number.is_none());
        assert!(fresh_github.worktree_path.is_none());
        assert!(fresh_github.merge_sha.is_none());

        let effects = core
            .reduce(SupervisorEvent::GitHubRunFinalized {
                expected_version: core.version(),
                operation_id: finalize.operation_id,
                pr_number: finalize.pr_number,
                final_head_sha: finalize.final_head_sha,
                merge_sha: finalize.merge_sha,
                finalization: GitHubFinalizationSnapshot {
                    local_worktree_removed: true,
                    local_ref_removed: true,
                    remote_ref_outcome: Some("deleted".into()),
                },
            })
            .unwrap();
        record(&effects);
        assert_eq!(core.session_state(), SessionState::Completed);
        let github = core.snapshot().github.unwrap();
        assert_eq!(github.phase, Some(GitHubDeliveryPhase::Delivered));
        assert_eq!(github.outcome, Some(GitHubDeliveryOutcome::Delivered));

        let mut failed = github_core(vec![task("failure", "/repo", 2)], false);
        prepare_github(&mut failed);
        let developer = start_first_developer(&mut failed, 0, 1, 1, "failed-dev");
        publish_github_candidate(&mut failed, developer, &"b".repeat(40));
        start_github_reviewers(&mut failed, 0, 10, 20, true);
        let reviewer = failed
            .active_turns
            .get(&WorkerLane::Reviewer(ReviewerId::Reviewer1))
            .cloned()
            .unwrap();
        let failure_effects = complete_lane_turn(
            &mut failed,
            0,
            WorkerLane::Reviewer(ReviewerId::Reviewer1),
            reviewer.session,
            reviewer.turn,
            &reviewer.completion_token,
            lgtm(),
        );
        let operation_id = failure_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PublishReviewerReview(request) => {
                    Some(request.operation_id.clone())
                }
                _ => None,
            })
            .unwrap();
        let failure_effects = failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: failed.version(),
                operation_id,
                detail: "remote observation could not be proved".into(),
                partial_operation: None,
            })
            .unwrap();
        record(&failure_effects);
        assert_eq!(failed.session_state(), SessionState::NeedsHuman);

        for required in [
            SupervisorEffectKind::PrepareGitHubRunRepository,
            SupervisorEffectKind::PublishDeveloperCandidate,
            SupervisorEffectKind::PublishReviewerReview,
            SupervisorEffectKind::OpenOrUpdateReviewCheck,
            SupervisorEffectKind::WaitForMergeGate,
            SupervisorEffectKind::MergePullRequest,
            SupervisorEffectKind::FinalizeGitHubRun,
            SupervisorEffectKind::PublishGitHubTerminal,
        ] {
            assert!(observed.contains(&required), "missing {required:?}");
        }
    }

    #[test]
    fn failed_candidate_publication_retains_confirmed_remote_coordinates() {
        let mut core = github_core(vec![task("partial", "/repo", 2)], false);
        prepare_github(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "partial-developer");
        let effects = complete_lane_turn(
            &mut core,
            developer.task,
            developer.lane,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PublishDeveloperCandidate(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        core.reduce(SupervisorEvent::GitHubOperationFailed {
            expected_version: core.version(),
            operation_id: request.operation_id,
            detail: "Check creation remained unavailable".into(),
            partial_operation: Some(GitHubPartialOperation::Candidate(
                GitHubPartialCandidatePublication {
                    task_ordinal: request.task_ordinal,
                    generation: request.generation,
                    previous_head_sha: request.previous_head_sha,
                    head_sha: "b".repeat(40),
                    pr_number: Some(7),
                    pr_node_id: Some("PR_kwDOhcom".into()),
                    pr_url: Some("https://github.com/owner/repo/pull/7".into()),
                    pr_actor_bot_user_id: Some(22),
                    check_run_id: None,
                    check_url: None,
                    check_actor_app_id: None,
                },
            )),
        })
        .unwrap();
        let snapshot = core.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        let github = snapshot.github.unwrap();
        assert_eq!(github.published_head_sha, Some("b".repeat(40)));
        assert_eq!(github.pr_number, Some(7));
        assert_eq!(
            github.pr_url.as_deref(),
            Some("https://github.com/owner/repo/pull/7")
        );
        assert!(github.current_check.is_none());
    }

    #[test]
    fn failed_repository_preparation_reports_only_confirmed_managed_artifacts() {
        let mut core = github_core(vec![task("prepare-partial", "/repo", 2)], false);
        let effects = authorize(&mut core);
        let request = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PrepareGitHubRunRepository(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();

        let mut before_effect = core.clone();
        before_effect
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: before_effect.version(),
                operation_id: request.operation_id.clone(),
                detail: "preparation failed before creating managed artifacts".into(),
                partial_operation: None,
            })
            .unwrap();
        let github = before_effect.snapshot().github.unwrap();
        assert!(github.preserved_branch.is_none());
        assert!(github.preserved_worktree.is_none());

        let mut after_base_fetch = core.clone();
        after_base_fetch
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: after_base_fetch.version(),
                operation_id: request.operation_id,
                detail: "preparation validation failed after fetching the base".into(),
                partial_operation: Some(GitHubPartialOperation::RepositoryPreparation(
                    GitHubPartialRepositoryPreparation {
                        base_sha: request.run_binding.expected_base_sha,
                        branch: request.run_binding.generated_run_branch,
                        worktree_path: "/project/hcom-tasks/run-github/repository".into(),
                        local_base_ref_created: true,
                        local_branch_created: false,
                        local_worktree_created: false,
                    },
                )),
            })
            .unwrap();
        let github = after_base_fetch.snapshot().github.unwrap();
        assert!(github.preserved_branch.is_none());
        assert!(github.preserved_worktree.is_none());
    }

    #[test]
    fn failed_evidence_writes_retain_confirmed_review_check_merge_and_cleanup() {
        let mut core = github_core(vec![task("partial", "/repo", 2)], true);
        prepare_github(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "partial-developer");
        publish_github_candidate(&mut core, developer, &"b".repeat(40));
        start_github_reviewers(&mut core, 0, 10, 20, true);

        let mut review_failed = core.clone();
        let reviewer = review_failed
            .active_turns
            .get(&WorkerLane::Reviewer(ReviewerId::Reviewer1))
            .cloned()
            .unwrap();
        let review_effects = complete_lane_turn(
            &mut review_failed,
            0,
            WorkerLane::Reviewer(ReviewerId::Reviewer1),
            reviewer.session,
            reviewer.turn,
            &reviewer.completion_token,
            lgtm(),
        );
        let review_request = review_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PublishReviewerReview(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        review_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: review_failed.version(),
                operation_id: review_request.operation_id,
                detail: "review evidence write failed after confirmation".into(),
                partial_operation: Some(GitHubPartialOperation::ReviewerReview(
                    GitHubPartialReviewerReview {
                        task_ordinal: review_request.task_ordinal,
                        reviewer_id: review_request.reviewer_id,
                        generation: review_request.generation,
                        head_sha: review_request.head_sha,
                        verdict: review_request.verdict,
                        review_id: 2001,
                        review_url: "https://github.com/owner/repo/pull/7#pullrequestreview-2001"
                            .into(),
                        actor_bot_user_id: 23,
                        final_artifact_sha256: "e".repeat(64),
                    },
                )),
            })
            .unwrap();
        let review_snapshot = review_failed.snapshot();
        assert_eq!(review_snapshot.state, SessionState::NeedsHuman);
        assert_eq!(review_snapshot.tasks[0].github_reviews.len(), 1);
        assert_eq!(
            review_snapshot.tasks[0].reviewers[0].current_verdict,
            Some(ReviewerVerdict::Lgtm)
        );

        let review_effects =
            publish_github_reviews(&mut core, &[(ReviewerId::Reviewer1, ReviewerVerdict::Lgtm)]);
        let check_request = review_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::OpenOrUpdateReviewCheck(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        let mut check_failed = core.clone();
        check_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: check_failed.version(),
                operation_id: check_request.operation_id.clone(),
                detail: "Check evidence write failed after confirmation".into(),
                partial_operation: Some(GitHubPartialOperation::ReviewCheck(
                    GitHubPartialReviewCheck {
                        task_ordinal: check_request.task_ordinal,
                        generation: check_request.generation,
                        head_sha: check_request.head_sha.clone(),
                        check_run_id: check_request.check_run_id,
                        check_url: check_request.check_url.clone(),
                        state: check_request.conclusion.as_str().into(),
                        actor_app_id: 1,
                    },
                )),
            })
            .unwrap();
        let check_snapshot = check_failed.snapshot();
        assert_eq!(check_snapshot.state, SessionState::NeedsHuman);
        assert_eq!(
            check_snapshot
                .github
                .as_ref()
                .and_then(|github| github.current_check.as_ref())
                .map(|check| check.state.as_str()),
            Some("success")
        );
        assert_eq!(
            check_snapshot.tasks[0]
                .github_check
                .as_ref()
                .map(|check| check.state.as_str()),
            Some("success")
        );

        let merge_wait_effects = publish_github_check(&mut core, &review_effects);
        let gate = merge_wait_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::WaitForMergeGate(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        let merge_effects = core
            .reduce(SupervisorEvent::MergeGateObserved {
                expected_version: core.version(),
                operation_id: gate.operation_id,
                pr_number: gate.pr_number,
                final_head_sha: gate.final_head_sha,
                base_sha: "a".repeat(40),
                ruleset_attestation_sha256: "d".repeat(64),
                check_run_id: gate.check_run_id,
            })
            .unwrap();
        let merge_request = merge_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::MergePullRequest(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();

        let mut merge_failed = core.clone();
        let failure_effects = merge_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: merge_failed.version(),
                operation_id: merge_request.operation_id.clone(),
                detail: "merge evidence write failed after confirmation".into(),
                partial_operation: Some(GitHubPartialOperation::Merge(GitHubPartialMerge {
                    pr_number: merge_request.pr_number,
                    final_head_sha: merge_request.final_head_sha.clone(),
                    merge_sha: "f".repeat(40),
                    merge_url: "https://github.com/owner/repo/commit/final".into(),
                    actor_bot_user_id: 21,
                })),
            })
            .unwrap();
        assert!(failure_effects.iter().all(|effect| !matches!(
            effect,
            SupervisorEffect::WaitForMergeGate(_)
                | SupervisorEffect::MergePullRequest(_)
                | SupervisorEffect::FinalizeGitHubRun(_)
        )));
        let merge_snapshot = merge_failed.snapshot();
        assert_eq!(merge_snapshot.state, SessionState::NeedsHuman);
        let merge_github = merge_snapshot.github.unwrap();
        assert!(merge_github.merge_already_confirmed);
        assert_eq!(merge_github.merge_sha, Some("f".repeat(40)));
        assert_eq!(
            merge_github.merge_url.as_deref(),
            Some("https://github.com/owner/repo/commit/final")
        );
        assert_eq!(merge_github.phase, Some(GitHubDeliveryPhase::Finalizing));

        let finalize_effects = core
            .reduce(SupervisorEvent::PullRequestMerged {
                expected_version: core.version(),
                operation_id: merge_request.operation_id,
                pr_number: merge_request.pr_number,
                final_head_sha: merge_request.final_head_sha,
                merge_sha: "f".repeat(40),
                merge_url: "https://github.com/owner/repo/commit/final".into(),
                actor_bot_user_id: 21,
                merge_evidence_durable: true,
            })
            .unwrap();
        let finalize_request = finalize_effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::FinalizeGitHubRun(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();

        let mut remote_mutation_failed = core.clone();
        remote_mutation_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: remote_mutation_failed.version(),
                operation_id: finalize_request.operation_id.clone(),
                detail: "remote branch changed before cleanup".into(),
                partial_operation: Some(GitHubPartialOperation::Finalization(
                    GitHubPartialFinalization {
                        pr_number: finalize_request.pr_number,
                        final_head_sha: finalize_request.final_head_sha.clone(),
                        merge_sha: finalize_request.merge_sha.clone(),
                        finalization: GitHubFinalizationSnapshot {
                            local_worktree_removed: false,
                            local_ref_removed: false,
                            remote_ref_outcome: Some("preserved_after_mutation".into()),
                        },
                    },
                )),
            })
            .unwrap();
        let remote_mutation_snapshot = remote_mutation_failed.snapshot();
        assert_eq!(remote_mutation_snapshot.state, SessionState::NeedsHuman);
        let remote_mutation_github = remote_mutation_snapshot.github.unwrap();
        assert_eq!(
            remote_mutation_github.finalization,
            Some(GitHubFinalizationSnapshot {
                local_worktree_removed: false,
                local_ref_removed: false,
                remote_ref_outcome: Some("preserved_after_mutation".into()),
            })
        );
        assert!(remote_mutation_github.preserved_branch.is_some());
        assert!(remote_mutation_github.preserved_worktree.is_some());

        let mut progressive_cleanup_failed = core.clone();
        progressive_cleanup_failed
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: progressive_cleanup_failed.version(),
                operation_id: finalize_request.operation_id.clone(),
                detail: "local ref changed after worktree cleanup".into(),
                partial_operation: Some(GitHubPartialOperation::Finalization(
                    GitHubPartialFinalization {
                        pr_number: finalize_request.pr_number,
                        final_head_sha: finalize_request.final_head_sha.clone(),
                        merge_sha: finalize_request.merge_sha.clone(),
                        finalization: GitHubFinalizationSnapshot {
                            local_worktree_removed: true,
                            local_ref_removed: false,
                            remote_ref_outcome: None,
                        },
                    },
                )),
            })
            .unwrap();
        let progressive_snapshot = progressive_cleanup_failed.snapshot();
        assert_eq!(progressive_snapshot.state, SessionState::NeedsHuman);
        let progressive_github = progressive_snapshot.github.unwrap();
        assert_eq!(
            progressive_github.finalization,
            Some(GitHubFinalizationSnapshot {
                local_worktree_removed: true,
                local_ref_removed: false,
                remote_ref_outcome: None,
            })
        );
        assert!(progressive_github.preserved_branch.is_some());
        assert!(progressive_github.preserved_worktree.is_none());

        let failure_effects = core
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: core.version(),
                operation_id: finalize_request.operation_id,
                detail: "finalization evidence write failed after cleanup".into(),
                partial_operation: Some(GitHubPartialOperation::Finalization(
                    GitHubPartialFinalization {
                        pr_number: finalize_request.pr_number,
                        final_head_sha: finalize_request.final_head_sha,
                        merge_sha: finalize_request.merge_sha,
                        finalization: GitHubFinalizationSnapshot {
                            local_worktree_removed: true,
                            local_ref_removed: true,
                            remote_ref_outcome: Some("deleted".into()),
                        },
                    },
                )),
            })
            .unwrap();
        assert!(failure_effects.iter().all(|effect| !matches!(
            effect,
            SupervisorEffect::WaitForMergeGate(_)
                | SupervisorEffect::MergePullRequest(_)
                | SupervisorEffect::FinalizeGitHubRun(_)
        )));
        let finalization_snapshot = core.snapshot();
        assert_eq!(finalization_snapshot.state, SessionState::NeedsHuman);
        let finalization_github = finalization_snapshot.github.unwrap();
        assert!(finalization_github.merge_already_confirmed);
        assert_eq!(
            finalization_github.finalization,
            Some(GitHubFinalizationSnapshot {
                local_worktree_removed: true,
                local_ref_removed: true,
                remote_ref_outcome: Some("deleted".into()),
            })
        );
        assert!(finalization_github.preserved_branch.is_none());
        assert!(finalization_github.preserved_worktree.is_none());
    }

    #[test]
    fn github_review_exhaustion_advances_and_preserves_one_pr_with_aggregate_block() {
        let mut one = task("one", "/repo", MIN_SINGLE_REVIEW_ROUNDS);
        let mut two = task("two", "/repo", MIN_SINGLE_REVIEW_ROUNDS);
        one.max_review_rounds = MIN_SINGLE_REVIEW_ROUNDS;
        two.max_review_rounds = MIN_SINGLE_REVIEW_ROUNDS;
        let mut core = github_core(vec![one, two], true);
        prepare_github(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "exhaust-dev-one");
        publish_github_candidate(&mut core, developer, &"b".repeat(40));
        start_github_reviewers(&mut core, 0, 10, 20, true);
        let maximum = core.tasks[0].spec.max_review_rounds;
        let correction_heads = ['c', 'e', 'f', '1'];
        let mut effects = Vec::new();
        for generation in 1..=maximum {
            effects = publish_github_reviews(
                &mut core,
                &[(ReviewerId::Reviewer1, ReviewerVerdict::RequestChanges)],
            );
            effects = publish_github_check(&mut core, &effects);
            if generation == maximum {
                break;
            }
            let session = core.tasks[0].developer_session.unwrap();
            let token: &'static str =
                Box::leak(format!("exhaust-correction-{generation}").into_boxed_str());
            let correction = ActiveIdentity {
                task: 0,
                role: WorkerRole::Developer,
                lane: WorkerLane::Developer,
                review_generation: None,
                session,
                turn: RuntimeTurnKey::from_counter(30 + u64::from(generation)).unwrap(),
                token,
            };
            start_lane_turn(
                &mut core,
                0,
                correction.lane,
                RuntimeTurnPurpose::DeveloperCorrection,
                session,
                correction.turn,
                correction.token,
            );
            publish_github_candidate(
                &mut core,
                correction,
                &correction_heads[usize::from(generation - 1)]
                    .to_string()
                    .repeat(40),
            );
            start_github_reviewers(&mut core, 0, 10, 40 + u64::from(generation), false);
        }
        assert_eq!(core.tasks[0].state, TaskState::ReviewExhausted);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SupervisorEffect::OpenTaskRuntime { task_ordinal: 1 }
        )));

        let developer = start_first_developer(&mut core, 1, 50, 50, "exhaust-dev-two");
        publish_github_candidate(&mut core, developer, &"2".repeat(40));
        start_github_reviewers(&mut core, 1, 60, 60, true);
        let effects =
            publish_github_reviews(&mut core, &[(ReviewerId::Reviewer1, ReviewerVerdict::Lgtm)]);
        let check = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::OpenOrUpdateReviewCheck(request) => Some(request),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            check.conclusion,
            GitHubReviewCheckConclusion::ActionRequired
        );
        assert_eq!(
            check.task_outcomes[0].outcome,
            Some(TaskCompletionOutcome::ReviewExhausted)
        );
        let effects = publish_github_check(&mut core, &effects);
        assert_eq!(core.session_state(), SessionState::Completed);
        let github = core.snapshot().github.unwrap();
        assert_eq!(github.pr_number, Some(7));
        assert_eq!(github.phase, Some(GitHubDeliveryPhase::PreservedUnmerged));
        assert_eq!(
            github.outcome,
            Some(GitHubDeliveryOutcome::UnmergedReviewExhausted)
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            SupervisorEffect::WaitForMergeGate(_)
                | SupervisorEffect::MergePullRequest(_)
                | SupervisorEffect::FinalizeGitHubRun(_)
        )));
    }

    #[test]
    fn github_single_review_has_no_peer_and_stale_publication_is_transactional() {
        let mut core = github_core(vec![task("one", "/repo", 2)], true);
        prepare_github(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "single-github-dev");
        let effects = complete_lane_turn(
            &mut core,
            developer.task,
            developer.lane,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        let candidate = effects
            .iter()
            .find_map(|effect| match effect {
                SupervisorEffect::PublishDeveloperCandidate(request) => Some(request.clone()),
                _ => None,
            })
            .unwrap();
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::CandidatePublished {
                expected_version: core.version(),
                operation_id: candidate.operation_id.clone(),
                task_ordinal: 0,
                generation: 1,
                previous_head_sha: "f".repeat(40),
                head_sha: "b".repeat(40),
                pr_number: 7,
                pr_node_id: "PR_kwDOhcom".into(),
                pr_url: "https://github.com/owner/repo/pull/7".into(),
                pr_actor_bot_user_id: 22,
                check_run_id: 1001,
                check_url: "https://github.com/owner/repo/runs/1001".into(),
                check_actor_app_id: 1,
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);
        let effects = core
            .reduce(SupervisorEvent::CandidatePublished {
                expected_version: core.version(),
                operation_id: candidate.operation_id,
                task_ordinal: candidate.task_ordinal,
                generation: candidate.generation,
                previous_head_sha: candidate.previous_head_sha,
                head_sha: "b".repeat(40),
                pr_number: 7,
                pr_node_id: "PR_kwDOhcom".into(),
                pr_url: "https://github.com/owner/repo/pull/7".into(),
                pr_actor_bot_user_id: 22,
                check_run_id: 1001,
                check_url: "https://github.com/owner/repo/runs/1001".into(),
                check_actor_app_id: 1,
            })
            .unwrap();
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(
                    effect,
                    SupervisorEffect::OpenRoleSession {
                        lane: WorkerLane::Reviewer(_),
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(
            !core.snapshot().tasks[0]
                .reviewers
                .iter()
                .any(|reviewer| reviewer.reviewer_id == ReviewerId::Reviewer2)
        );
    }

    #[test]
    fn provider_transport_types_do_not_leak_into_the_core_source() {
        let source = include_str!("core.rs");
        let implementation = source
            .split("#[cfg(test)]")
            .next()
            .expect("core source contains its implementation");
        for forbidden in [
            "serde_json::Value",
            "JSON-RPC",
            "threadId",
            "turnId",
            "std::process::Child",
            "std::process::Stdio",
            "ClaudeInvocationProfile",
            "CodexInvocationProfile",
            "std::fs",
            "std::thread",
            "SystemTime",
        ] {
            assert!(
                !implementation.contains(forbidden),
                "provider/process/I/O type leaked into SupervisorCore: {forbidden}"
            );
        }
    }

    #[test]
    fn every_session_state_by_event_kind_has_an_explicit_accept_or_reject_row() {
        let session_states = [
            SessionState::AwaitingPlan,
            SessionState::AwaitingApproval,
            SessionState::Running,
            SessionState::Completed,
            SessionState::NeedsHuman,
            SessionState::Failed,
            SessionState::Canceled,
        ];
        let mut rows = 0;
        let mut accepted = 0;
        let mut rejected = 0;

        for state in session_states {
            for kind in SupervisorEventKind::ALL {
                rows += 1;
                let (mut core, event, should_accept) = match state {
                    SessionState::AwaitingPlan => {
                        let core = new_core();
                        let event = generic_event(&core, kind);
                        let should_accept = matches!(
                            kind,
                            SupervisorEventKind::PlanBound
                                | SupervisorEventKind::CancelRequested
                                | SupervisorEventKind::ParentStopping
                                | SupervisorEventKind::StatusRequested
                        );
                        (core, event, should_accept)
                    }
                    SessionState::AwaitingApproval => {
                        let core = bound_core();
                        let event = generic_event(&core, kind);
                        let should_accept = matches!(
                            kind,
                            SupervisorEventKind::PlanBound
                                | SupervisorEventKind::ExecutionAuthorized
                                | SupervisorEventKind::CancelRequested
                                | SupervisorEventKind::ParentStopping
                                | SupervisorEventKind::StatusRequested
                        );
                        (core, event, should_accept)
                    }
                    SessionState::Running => {
                        let (core, event) = running_fixture(kind);
                        let should_accept = !matches!(
                            kind,
                            SupervisorEventKind::PlanBound
                                | SupervisorEventKind::ExecutionAuthorized
                                | SupervisorEventKind::RepositoryPrepared
                                | SupervisorEventKind::CandidatePublished
                                | SupervisorEventKind::ReviewerReviewPublished
                                | SupervisorEventKind::ReviewCheckPublished
                                | SupervisorEventKind::MergeGateObserved
                                | SupervisorEventKind::PullRequestMerged
                                | SupervisorEventKind::GitHubRunFinalized
                                | SupervisorEventKind::GitHubOperationFailed
                        );
                        (core, event, should_accept)
                    }
                    SessionState::Completed => {
                        let core = completed_core();
                        let event = generic_event(&core, kind);
                        (core, event, kind == SupervisorEventKind::StatusRequested)
                    }
                    SessionState::NeedsHuman => {
                        let core = needs_human_core();
                        let event = generic_event(&core, kind);
                        (core, event, kind == SupervisorEventKind::StatusRequested)
                    }
                    SessionState::Failed => {
                        let core = failed_core();
                        let event = generic_event(&core, kind);
                        (core, event, kind == SupervisorEventKind::StatusRequested)
                    }
                    SessionState::Canceled => {
                        let core = canceled_core();
                        let event = generic_event(&core, kind);
                        (core, event, kind == SupervisorEventKind::StatusRequested)
                    }
                };
                assert_eq!(core.session_state(), state);
                let before = core.clone();
                let result = core.reduce(event);
                if should_accept {
                    accepted += 1;
                    assert!(
                        result.is_ok(),
                        "expected {state:?} × {kind:?} to be accepted: {result:?}"
                    );
                } else {
                    rejected += 1;
                    assert!(
                        result.is_err(),
                        "expected {state:?} × {kind:?} to be rejected"
                    );
                    assert_eq!(
                        core, before,
                        "rejected {state:?} × {kind:?} mutated the core"
                    );
                }
            }
        }

        assert_eq!(rows, 7 * SupervisorEventKind::ALL.len());
        assert_eq!(accepted, 25);
        assert_eq!(rejected, 129);
    }

    #[test]
    fn every_local_effect_kind_has_a_real_core_production_path() {
        let mut observed = BTreeSet::new();
        let mut record = |effects: Vec<SupervisorEffect>| {
            observed.extend(effects.iter().map(SupervisorEffect::kind));
        };

        let mut core = bound_core();
        record(authorize(&mut core));
        record(open_runtime(&mut core, 0));
        let session = RuntimeSessionKey::from_counter(1).unwrap();
        record(open_session(&mut core, 0, WorkerRole::Developer, session));
        let turn = RuntimeTurnKey::from_counter(1).unwrap();
        record(start_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
            session,
            turn,
            "effect-inventory",
        ));
        record(
            core.reduce(SupervisorEvent::Timeout {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session,
                turn,
                completion_token: "effect-inventory".into(),
            })
            .unwrap(),
        );

        let (mut clarification, active) = active_core();
        record(complete_turn(
            &mut clarification,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            clarification_required(),
        ));

        let github_effects = BTreeSet::from([
            SupervisorEffectKind::PrepareGitHubRunRepository,
            SupervisorEffectKind::PublishDeveloperCandidate,
            SupervisorEffectKind::PublishReviewerReview,
            SupervisorEffectKind::OpenOrUpdateReviewCheck,
            SupervisorEffectKind::WaitForMergeGate,
            SupervisorEffectKind::MergePullRequest,
            SupervisorEffectKind::FinalizeGitHubRun,
            SupervisorEffectKind::PublishGitHubTerminal,
        ]);
        assert_eq!(
            observed,
            SupervisorEffectKind::ALL
                .into_iter()
                .filter(|kind| !github_effects.contains(kind))
                .collect(),
            "every local effect variant must have an exercised production path"
        );
    }

    #[test]
    fn task_transition_inventory_covers_every_state_and_rejects_terminal_lifecycle() {
        let mut observed_states = BTreeSet::new();
        let mut edges = BTreeSet::new();
        let name = |state: TaskState| match state {
            TaskState::Pending => "pending",
            TaskState::Developing => "developing",
            TaskState::AwaitingArchitectAction => "awaiting_architect_action",
            TaskState::PublishingCandidate => "publishing_candidate",
            TaskState::Reviewing => "reviewing",
            TaskState::PublishingReview => "publishing_review",
            TaskState::Lgtm => "lgtm",
            TaskState::ReviewExhausted => "review_exhausted",
            TaskState::NeedsHuman => "needs_human",
            TaskState::Failed => "failed",
            TaskState::Canceled => "canceled",
        };

        let mut pending = pending_runtime_core();
        observed_states.insert(name(pending.tasks[0].state));
        let before = pending.tasks[0].state;
        open_runtime(&mut pending, 0);
        let after = pending.tasks[0].state;
        edges.insert((name(before), "runtime_opened", name(after)));

        let (mut developing, active) = active_core();
        observed_states.insert(name(developing.tasks[0].state));
        let before = developing.tasks[0].state;
        complete_turn(
            &mut developing,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        edges.insert((
            name(before),
            "developer_ready",
            name(developing.tasks[0].state),
        ));
        observed_states.insert(name(developing.tasks[0].state));

        let (mut clarification, active) = active_core();
        let before = clarification.tasks[0].state;
        complete_turn(
            &mut clarification,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            clarification_required(),
        );
        edges.insert((
            name(before),
            "developer_clarification",
            name(clarification.tasks[0].state),
        ));
        observed_states.insert(name(clarification.tasks[0].state));
        let pending = clarification
            .pending_architect_action
            .clone()
            .expect("clarification is pending");
        let before = clarification.tasks[0].state;
        clarification
            .reduce(SupervisorEvent::ClarificationSubmitted {
                expected_version: clarification.version(),
                task_ordinal: 0,
                task_key: pending.task_key,
                action_sequence: pending.sequence,
                developer_request_path: pending.developer_request_path,
                clarification_document_path: pending.clarification_output_path,
                human_decision_confirmed: false,
            })
            .unwrap();
        edges.insert((
            name(before),
            "clarification_submitted",
            name(clarification.tasks[0].state),
        ));

        let (mut blocked_core, active) = active_core();
        let before = blocked_core.tasks[0].state;
        blocked_core
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: blocked_core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                failure: runtime_failure(RuntimeFailureClass::Contract, false, "blocked"),
            })
            .unwrap();
        edges.insert((
            name(before),
            "runtime_contract_failure",
            name(blocked_core.tasks[0].state),
        ));
        observed_states.insert(name(blocked_core.tasks[0].state));

        let (mut changes, reviewer) = active_reviewer_core(3);
        let before = changes.tasks[0].state;
        complete_review(&mut changes, reviewer, request_changes());
        edges.insert((
            name(before),
            "request_changes",
            name(changes.tasks[0].state),
        ));

        let (mut approved, reviewer) = active_reviewer_core(3);
        let before = approved.tasks[0].state;
        complete_review(&mut approved, reviewer, lgtm());
        edges.insert((name(before), "lgtm", name(approved.tasks[0].state)));
        observed_states.insert(name(approved.tasks[0].state));

        let (mut exhausted, reviewer) = active_reviewer_core(MIN_DUAL_REVIEW_ROUNDS);
        let before = exhausted.tasks[0].state;
        exhaust_review(&mut exhausted, reviewer);
        edges.insert((
            name(before),
            "max_round_request_changes",
            name(exhausted.tasks[0].state),
        ));
        observed_states.insert(name(exhausted.tasks[0].state));

        let failed = failed_core();
        observed_states.insert(name(failed.tasks[0].state));
        let canceled = active_canceled_core();
        observed_states.insert(name(canceled.tasks[0].state));

        assert_eq!(
            observed_states,
            BTreeSet::from([
                "pending",
                "developing",
                "awaiting_architect_action",
                "reviewing",
                "lgtm",
                "review_exhausted",
                "needs_human",
                "failed",
                "canceled",
            ])
        );
        assert_eq!(
            edges,
            BTreeSet::from([
                ("pending", "runtime_opened", "developing"),
                // Task-agnostic lane: the developer's exit routes straight to
                // review; the supervisor inspects nothing about the work.
                ("developing", "developer_ready", "reviewing"),
                (
                    "developing",
                    "developer_clarification",
                    "awaiting_architect_action",
                ),
                (
                    "awaiting_architect_action",
                    "clarification_submitted",
                    "developing",
                ),
                ("developing", "runtime_contract_failure", "needs_human"),
                ("reviewing", "request_changes", "developing"),
                ("reviewing", "lgtm", "lgtm"),
                ("reviewing", "max_round_request_changes", "review_exhausted",),
            ])
        );

        for terminal in [
            completed_core(),
            review_exhausted_core(),
            needs_human_core(),
            failed_core(),
            active_canceled_core(),
        ] {
            let terminal_task_state = terminal.tasks[0].state;
            for kind in [
                SupervisorEventKind::TaskRuntimeOpened,
                SupervisorEventKind::RoleSessionOpened,
                SupervisorEventKind::TurnStarted,
                SupervisorEventKind::TurnCompleted,
                SupervisorEventKind::TurnFailed,
                SupervisorEventKind::Timeout,
            ] {
                let mut core = terminal.clone();
                let before = core.clone();
                let error = core.reduce(generic_event(&core, kind)).unwrap_err();
                assert_eq!(
                    error.code,
                    SupervisorErrorCode::Terminal,
                    "{terminal_task_state:?} accepted {kind:?}"
                );
                assert_eq!(core, before);
            }
        }
    }

    #[test]
    fn architect_can_escalate_early_and_only_a_confirmed_human_answer_resumes() {
        let mut core = awaiting_architect_core();
        let pending = core
            .pending_architect_action
            .clone()
            .expect("action must be pending");
        let before = core.clone();
        let mut wrong_identity =
            generic_event(&core, SupervisorEventKind::ClarificationHumanRequired);
        let SupervisorEvent::ClarificationHumanRequired { task_key, .. } = &mut wrong_identity
        else {
            unreachable!()
        };
        *task_key = "wrong-task".into();
        assert!(core.reduce(wrong_identity).is_err());
        assert_eq!(core, before);

        core.reduce(SupervisorEvent::ClarificationHumanRequired {
            expected_version: core.version(),
            task_ordinal: 0,
            task_key: pending.task_key.clone(),
            action_sequence: pending.sequence,
            developer_request_path: pending.developer_request_path.clone(),
        })
        .unwrap();
        assert_eq!(
            core.pending_architect_action
                .as_ref()
                .unwrap()
                .published_version,
            core.version()
        );
        assert!(
            core.pending_architect_action
                .as_ref()
                .unwrap()
                .human_decision_required
        );
        let before = core.clone();
        assert!(
            core.reduce(SupervisorEvent::ClarificationSubmitted {
                expected_version: core.version(),
                task_ordinal: 0,
                task_key: pending.task_key.clone(),
                action_sequence: pending.sequence,
                developer_request_path: pending.developer_request_path.clone(),
                clarification_document_path: pending.clarification_output_path.clone(),
                human_decision_confirmed: false,
            })
            .is_err()
        );
        assert_eq!(core, before);

        core.reduce(SupervisorEvent::ClarificationSubmitted {
            expected_version: core.version(),
            task_ordinal: 0,
            task_key: pending.task_key,
            action_sequence: pending.sequence,
            developer_request_path: pending.developer_request_path,
            clarification_document_path: pending.clarification_output_path,
            human_decision_confirmed: true,
        })
        .unwrap();
        assert!(core.pending_architect_action.is_none());
        assert_eq!(core.tasks[0].state, TaskState::Developing);
        assert_eq!(core.tasks[0].clarification_rounds_used, 0);
        assert!(core.tasks[0].clarification_records[0].human_decision_confirmed);
    }

    #[test]
    fn status_snapshot_is_bounded_and_clarification_records_are_exactly_paginated() {
        let mut core = bound_core();
        core.tasks[0].clarification_records =
            (1..=10).map(clarification_record).collect::<Vec<_>>();
        core.assert_invariants().unwrap();

        let snapshot = core.snapshot();
        assert_eq!(snapshot.tasks[0].clarification_record_count, 10);

        let first = core.clarification_page("run-1", 0, "one", 0, 3).unwrap();
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(first.total_records, 10);
        assert_eq!(first.next_after_sequence, Some(3));

        let final_page = core.clarification_page("run-1", 0, "one", 3, 8).unwrap();
        assert_eq!(
            final_page
                .records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            [4, 5, 6, 7, 8, 9, 10]
        );
        assert_eq!(final_page.next_after_sequence, None);
        assert!(
            core.clarification_page("run-wrong", 0, "one", 0, 1)
                .is_err()
        );
        assert!(core.clarification_page("run-1", 0, "wrong", 0, 1).is_err());
        assert!(core.clarification_page("run-1", 0, "one", 11, 1).is_err());
        assert!(core.clarification_page("run-1", 0, "one", 0, 0).is_err());
    }

    #[test]
    fn clarification_capacity_exhaustion_terminalizes_instead_of_latching_more_state() {
        let (mut per_task, active) = active_core();
        per_task.tasks[0].clarification_records =
            (1..=u32::try_from(MAX_CLARIFICATION_RECORDS_PER_TASK).unwrap())
                .map(clarification_record)
                .collect();
        per_task.assert_invariants().unwrap();
        let effects = complete_turn(
            &mut per_task,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            clarification_required(),
        );
        assert_eq!(per_task.session_state(), SessionState::NeedsHuman);
        assert_eq!(per_task.tasks[0].state, TaskState::NeedsHuman);
        assert!(per_task.pending_architect_action.is_none());
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SupervisorEffect::FinishSession {
                state: SessionState::NeedsHuman,
                ..
            }
        )));

        let mut run = new_core();
        bind(
            &mut run,
            (0..MAX_TASKS)
                .map(|index| {
                    let mut task = task(&format!("task-{index}"), "/repo", 1);
                    task.max_clarification_rounds = 20;
                    task
                })
                .collect(),
        );
        authorize(&mut run);
        let active = start_first_developer(&mut run, 0, 1, 1, "run-capacity");
        for task in &mut run.tasks {
            task.clarification_records = (1..=20).map(clarification_record).collect();
        }
        assert_eq!(
            run.tasks
                .iter()
                .map(|task| task.clarification_records.len())
                .sum::<usize>(),
            MAX_CLARIFICATION_RECORDS_PER_RUN
        );
        run.assert_invariants().unwrap();
        complete_turn(
            &mut run,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
            clarification_required(),
        );
        assert_eq!(run.session_state(), SessionState::NeedsHuman);
        assert!(run.pending_architect_action.is_none());
    }

    #[test]
    fn every_task_state_by_relevant_lifecycle_event_has_an_explicit_matrix_row() {
        let task_states = [
            TaskState::Pending,
            TaskState::Developing,
            TaskState::AwaitingArchitectAction,
            TaskState::Reviewing,
            TaskState::Lgtm,
            TaskState::ReviewExhausted,
            TaskState::NeedsHuman,
            TaskState::Failed,
            TaskState::Canceled,
        ];
        let event_kinds = [
            SupervisorEventKind::TaskRuntimeOpened,
            SupervisorEventKind::RoleSessionOpened,
            SupervisorEventKind::TurnStarted,
            SupervisorEventKind::TurnCompleted,
            SupervisorEventKind::TurnFailed,
            SupervisorEventKind::Timeout,
        ];
        let mut rows = 0;
        let mut accepted = 0;
        let mut rejected = 0;
        for state in task_states {
            for kind in event_kinds {
                rows += 1;
                let (mut core, event, should_accept) = task_state_fixture(state, kind);
                assert_eq!(core.tasks[0].state, state);
                let before = core.clone();
                let result = core.reduce(event);
                if should_accept {
                    accepted += 1;
                    assert!(
                        result.is_ok(),
                        "expected {state:?} × {kind:?} to be accepted: {result:?}"
                    );
                } else {
                    rejected += 1;
                    assert!(
                        result.is_err(),
                        "expected {state:?} × {kind:?} to be rejected"
                    );
                    assert_eq!(core, before);
                }
            }
        }
        assert_eq!(rows, 9 * 6);
        assert_eq!(accepted, 11);
        assert_eq!(rejected, 43);
    }

    #[test]
    fn one_task_first_review_lgtm_has_exact_effects_and_status() {
        let mut core = new_core();
        assert_eq!(
            bind(&mut core, vec![task("one", "/repo", 3)]),
            vec![SupervisorEffect::PublishStatus]
        );
        assert_eq!(core.session_state(), SessionState::AwaitingApproval);
        assert_eq!(
            authorize(&mut core),
            vec![
                SupervisorEffect::OpenTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::PublishStatus,
            ]
        );
        let developer = start_first_developer(&mut core, 0, 1, 1, "dev-1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "review-1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, lgtm()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::Completed,
                    detail: "all ordered tasks reached a terminal review outcome".into(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );

        let snapshot = core.snapshot();
        assert_eq!(snapshot.run_id, "run-1");
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.version, 12);
        assert_eq!(snapshot.project_root, "/project");
        assert_eq!(snapshot.plan_version, Some(1));
        assert_eq!(snapshot.plan_hash, core.plan_hash);
        assert_eq!(snapshot.current_task_ordinal, Some(0));
        assert!(snapshot.active_workers.is_empty());
        assert_eq!(snapshot.reviewer_bindings, core.reviewer_bindings);
        assert!(snapshot.pending_architect_action.is_none());
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("all ordered tasks reached a terminal review outcome")
        );
        let task = &snapshot.tasks[0];
        assert_eq!(task.state, TaskState::Lgtm);
        assert_eq!(task.review_round, 1);
        assert_eq!(task.review_generation, 1);
        assert!(task.developer_session_bound);
        assert_eq!(
            task.latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-1/native-final.partial")
        );
        assert_eq!(
            reviewer_paths(task),
            vec![
                "/artifacts/reviewer/turn-2/native-final.partial",
                "/artifacts/reviewer/turn-10002/native-final.partial",
            ]
        );
        assert!(task.reviewers.iter().all(|reviewer| reviewer.session_bound
            && reviewer.current_generation == Some(1)
            && reviewer.current_verdict == Some(ReviewerVerdict::Lgtm)));
        let before = core.clone();
        assert_eq!(
            core.reduce(SupervisorEvent::StatusRequested).unwrap(),
            Vec::<SupervisorEffect>::new()
        );
        assert_eq!(core, before);
    }

    #[test]
    fn single_review_has_no_reviewer2_state_and_completes_on_reviewer1() {
        let (mut core, reviewer) =
            active_single_reviewer_core(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS);
        assert_eq!(core.reviewer_bindings.len(), 1);
        assert_eq!(core.reviewer_bindings[0].reviewer_id, ReviewerId::Reviewer1);
        assert!(
            !core.tasks[0]
                .reviewer_sessions
                .contains_key(&ReviewerId::Reviewer2)
        );
        assert!(
            !core
                .active_turns
                .contains_key(&WorkerLane::Reviewer(ReviewerId::Reviewer2))
        );

        assert_eq!(
            complete_review(&mut core, reviewer, lgtm()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::Completed,
                    detail: "all ordered tasks reached a terminal review outcome".into(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let snapshot = core.snapshot();
        assert_eq!(snapshot.tasks[0].reviewers.len(), 1);
        assert_eq!(
            snapshot.tasks[0].reviewers[0].reviewer_id,
            ReviewerId::Reviewer1
        );
        assert!(matches!(
            core.progress_event_after("run-single", 1).unwrap(),
            Some(SessionProgressEvent::ReviewResponded {
                reviewer_id: ReviewerId::Reviewer1,
                responses_received: 1,
                responses_expected: 1,
                ..
            })
        ));
    }

    #[test]
    fn single_review_exhausts_at_five_generations_without_reviewer2() {
        let (mut core, reviewer) =
            active_single_reviewer_core(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS);
        exhaust_review(&mut core, reviewer);
        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(core.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(
            core.tasks[0].review_round,
            u32::from(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS)
        );
        assert_eq!(core.tasks[0].reviewer_results.len(), 1);
        assert!(
            !core.tasks[0]
                .historical_reviewer_final_paths
                .contains_key(&ReviewerId::Reviewer2)
        );
    }

    #[test]
    fn single_review_failure_cancel_and_parent_stop_never_target_reviewer2() {
        let assert_single_reviewer_effects = |effects: &[SupervisorEffect]| {
            assert!(effects.iter().all(|effect| !matches!(
                effect,
                SupervisorEffect::OpenRoleSession {
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer2),
                    ..
                } | SupervisorEffect::StartTurn {
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer2),
                    ..
                } | SupervisorEffect::InterruptTurn {
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer2),
                    ..
                }
            )));
        };

        let (mut failed, reviewer) =
            active_single_reviewer_core(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS);
        let effects = failed
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: failed.version(),
                task_ordinal: reviewer.task,
                lane: reviewer.lane,
                review_generation: Some(1),
                session: reviewer.session,
                turn: reviewer.turn,
                completion_token: reviewer.token.into(),
                failure: runtime_failure(RuntimeFailureClass::Process, false, "Reviewer1 failed"),
            })
            .unwrap();
        assert_single_reviewer_effects(&effects);
        assert_eq!(failed.session_state(), SessionState::NeedsHuman);
        assert_eq!(failed.snapshot().reviewer_bindings.len(), 1);
        assert_eq!(failed.snapshot().tasks[0].reviewers.len(), 1);

        let (mut canceled, _) =
            active_single_reviewer_core(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS);
        let effects = canceled
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: canceled.version(),
                reason: "cancel single review".into(),
            })
            .unwrap();
        assert_single_reviewer_effects(&effects);
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SupervisorEffect::InterruptTurn { .. }))
                .count(),
            1
        );
        assert_eq!(canceled.session_state(), SessionState::Canceled);

        let (mut parent_stopping, _) =
            active_single_reviewer_core(crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS);
        let effects = parent_stopping
            .reduce(SupervisorEvent::ParentStopping {
                expected_version: parent_stopping.version(),
            })
            .unwrap();
        assert_single_reviewer_effects(&effects);
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SupervisorEffect::InterruptTurn { .. }))
                .count(),
            1
        );
        assert_eq!(parent_stopping.session_state(), SessionState::Canceled);
    }

    #[test]
    fn progress_events_preserve_review_paths_rounds_and_terminal_order() {
        let mut core = authorized_core();
        let developer = start_first_developer(&mut core, 0, 1, 1, "developer-1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "reviewer-1", true);
        let requested = core.progress_event_after("run-1", 0).unwrap().unwrap();
        assert!(matches!(
            requested,
            SessionProgressEvent::ReviewRequested {
                sequence: 1,
                review_round: 0,
                review_generation: 1,
                reviewer_bindings,
                ..
            } if reviewer_bindings.len() == 2
        ));
        complete_review(&mut core, reviewer, request_changes());
        for (sequence, reviewer_id, review_round, responses_received) in [
            (2, ReviewerId::Reviewer1, 0, 1),
            (3, ReviewerId::Reviewer2, 1, 2),
        ] {
            assert!(matches!(
                core.progress_event_after("run-1", sequence - 1)
                    .unwrap()
                    .unwrap(),
                SessionProgressEvent::ReviewResponded {
                    sequence: actual_sequence,
                    review_round: actual_review_round,
                    review_generation: 1,
                    reviewer_id: actual_reviewer_id,
                    reviewer_verdict: ReviewerVerdict::RequestChanges,
                    responses_received: actual_responses_received,
                    responses_expected: 2,
                    ..
                } if actual_sequence == sequence
                    && actual_reviewer_id == reviewer_id
                    && actual_review_round == review_round
                    && actual_responses_received == responses_received
            ));
        }

        let reviewer = correct_and_start_rereview(&mut core, 0, 3, 4, "developer-2", "reviewer-2");
        assert!(matches!(
            core.progress_event_after("run-1", 3).unwrap().unwrap(),
            SessionProgressEvent::ReviewRequested {
                sequence: 4,
                review_round: 1,
                review_generation: 2,
                ..
            }
        ));

        complete_review(&mut core, reviewer, lgtm());
        assert_eq!(core.session_state(), SessionState::Completed);
        for (sequence, reviewer_id, review_round, responses_received) in [
            (5, ReviewerId::Reviewer1, 1, 1),
            (6, ReviewerId::Reviewer2, 2, 2),
        ] {
            assert!(matches!(
                core.progress_event_after("run-1", sequence - 1)
                    .unwrap()
                    .unwrap(),
                SessionProgressEvent::ReviewResponded {
                    sequence: actual_sequence,
                    review_round: actual_review_round,
                    review_generation: 2,
                    reviewer_id: actual_reviewer_id,
                    reviewer_verdict: ReviewerVerdict::Lgtm,
                    responses_received: actual_responses_received,
                    responses_expected: 2,
                    ..
                } if actual_sequence == sequence
                    && actual_reviewer_id == reviewer_id
                    && actual_review_round == review_round
                    && actual_responses_received == responses_received
            ));
        }
        assert!(matches!(
            core.progress_event_after("run-1", 6).unwrap().unwrap(),
            SessionProgressEvent::TaskCompleted {
                sequence: 7,
                review_round: 2,
                review_generation: 2,
                outcome: TaskCompletionOutcome::Lgtm,
                reviewers,
                ..
            } if reviewers.len() == 2
                && reviewers.iter().all(|reviewer| {
                    reviewer.current_generation == Some(2)
                        && reviewer.current_verdict == Some(ReviewerVerdict::Lgtm)
                })
        ));
        assert!(core.progress_event_after("run-1", 7).unwrap().is_none());
        assert!(core.progress_event_after("run-2", 0).is_err());
        assert!(core.progress_event_after("run-1", 8).is_err());
    }

    #[test]
    fn reviewer_arrival_order_does_not_change_join_order_or_cross_the_barrier() {
        let (mut core, reviewer1) = active_reviewer_core(3);
        let reviewer2_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
        let reviewer2 = core.active_turns[&reviewer2_lane].clone();
        assert_eq!(
            complete_lane_turn(
                &mut core,
                reviewer2.task_ordinal,
                reviewer2_lane,
                reviewer2.session,
                reviewer2.turn,
                &reviewer2.completion_token,
                lgtm(),
            ),
            vec![SupervisorEffect::PublishStatus]
        );
        assert_eq!(core.tasks[0].state, TaskState::Reviewing);
        assert_eq!(core.tasks[0].review_round, 0);
        assert_eq!(core.tasks[0].review_generation, 1);
        assert!(!core.active_turns.contains_key(&WorkerLane::Developer));
        assert!(
            !core
                .pending_turn_starts
                .contains_key(&WorkerLane::Developer)
        );

        let effects = complete_lane_turn(
            &mut core,
            reviewer1.task,
            reviewer1.lane,
            reviewer1.session,
            reviewer1.turn,
            reviewer1.token,
            request_changes(),
        );
        assert!(matches!(
            effects.as_slice(),
            [
                SupervisorEffect::StartTurn {
                    lane: WorkerLane::Developer,
                    purpose: RuntimeTurnPurpose::DeveloperCorrection,
                    ..
                },
                SupervisorEffect::PublishStatus,
            ]
        ));
        assert_eq!(core.tasks[0].state, TaskState::Developing);
        assert_eq!(core.tasks[0].review_round, 1);
        assert_eq!(
            core.tasks[0].latest_reviewer_final_paths(),
            [
                "/artifacts/reviewer/turn-2/native-final.partial",
                "/artifacts/reviewer/turn-10002/native-final.partial",
            ],
            "Developer correction paths remain Reviewer1 then Reviewer2 regardless of arrival"
        );
    }

    #[test]
    fn both_completion_orders_cover_all_four_verdict_pairs_and_join_once() {
        for reviewer1_verdict in [ReviewerVerdict::Lgtm, ReviewerVerdict::RequestChanges] {
            for reviewer2_verdict in [ReviewerVerdict::Lgtm, ReviewerVerdict::RequestChanges] {
                for reviewer2_first in [false, true] {
                    let (mut core, _) = active_reviewer_core(3);
                    let reviewer1_lane = WorkerLane::Reviewer(ReviewerId::Reviewer1);
                    let reviewer2_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
                    let reviewer1 = core.active_turns[&reviewer1_lane].clone();
                    let reviewer2 = core.active_turns[&reviewer2_lane].clone();
                    let outcome = |verdict| match verdict {
                        ReviewerVerdict::Lgtm => lgtm(),
                        ReviewerVerdict::RequestChanges => request_changes(),
                    };
                    let (first, first_verdict, second, second_verdict) = if reviewer2_first {
                        (reviewer2, reviewer2_verdict, reviewer1, reviewer1_verdict)
                    } else {
                        (reviewer1, reviewer1_verdict, reviewer2, reviewer2_verdict)
                    };

                    assert_eq!(
                        complete_lane_turn(
                            &mut core,
                            first.task_ordinal,
                            first.lane,
                            first.session,
                            first.turn,
                            &first.completion_token,
                            outcome(first_verdict),
                        ),
                        vec![SupervisorEffect::PublishStatus]
                    );
                    assert_eq!(core.tasks[0].state, TaskState::Reviewing);
                    assert_eq!(core.tasks[0].review_round, 0);
                    assert_eq!(core.tasks[0].review_generation, 1);
                    assert!(!core.active_turns.contains_key(&WorkerLane::Developer));
                    assert!(
                        !core
                            .pending_turn_starts
                            .contains_key(&WorkerLane::Developer)
                    );

                    let effects = complete_lane_turn(
                        &mut core,
                        second.task_ordinal,
                        second.lane,
                        second.session,
                        second.turn,
                        &second.completion_token,
                        outcome(second_verdict),
                    );
                    assert_eq!(core.tasks[0].review_round, 1);
                    assert_eq!(core.tasks[0].review_generation, 1);
                    assert_eq!(
                        core.tasks[0].reviewer_results[&ReviewerId::Reviewer1].verdict,
                        reviewer1_verdict
                    );
                    assert_eq!(
                        core.tasks[0].reviewer_results[&ReviewerId::Reviewer2].verdict,
                        reviewer2_verdict
                    );

                    if reviewer1_verdict == ReviewerVerdict::Lgtm
                        && reviewer2_verdict == ReviewerVerdict::Lgtm
                    {
                        assert_eq!(core.tasks[0].state, TaskState::Lgtm);
                        assert_eq!(core.session_state(), SessionState::Completed);
                    } else {
                        assert_eq!(core.tasks[0].state, TaskState::Developing);
                        assert_eq!(
                            effects
                                .iter()
                                .filter(|effect| {
                                    matches!(
                                        effect,
                                        SupervisorEffect::StartTurn {
                                            lane: WorkerLane::Developer,
                                            purpose: RuntimeTurnPurpose::DeveloperCorrection,
                                            ..
                                        }
                                    )
                                })
                                .count(),
                            1,
                            "one joined generation must start one Developer correction"
                        );
                    }
                    core.assert_invariants().unwrap();
                }
            }
        }
    }

    #[test]
    fn reviewer_open_and_start_acknowledgements_can_interleave_by_lane() {
        let mut core = authorized_core();
        let developer = start_first_developer(&mut core, 0, 1, 1, "developer");
        complete_developer_ready(&mut core, developer);
        assert!(core.progress_events.is_empty());

        let reviewer1_lane = WorkerLane::Reviewer(ReviewerId::Reviewer1);
        let reviewer2_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
        let reviewer2_session = RuntimeSessionKey::from_counter(3).unwrap();
        let reviewer1_session = RuntimeSessionKey::from_counter(2).unwrap();
        assert!(matches!(
            open_lane_session(&mut core, 0, reviewer2_lane, reviewer2_session).as_slice(),
            [
                SupervisorEffect::StartTurn {
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer2),
                    ..
                },
                SupervisorEffect::PublishStatus,
            ]
        ));
        assert!(matches!(
            open_lane_session(&mut core, 0, reviewer1_lane, reviewer1_session).as_slice(),
            [
                SupervisorEffect::StartTurn {
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                    ..
                },
                SupervisorEffect::PublishStatus,
            ]
        ));
        start_lane_turn(
            &mut core,
            0,
            reviewer1_lane,
            RuntimeTurnPurpose::InitialReview,
            reviewer1_session,
            RuntimeTurnKey::from_counter(2).unwrap(),
            "reviewer1",
        );
        assert!(core.progress_events.is_empty());
        start_lane_turn(
            &mut core,
            0,
            reviewer2_lane,
            RuntimeTurnPurpose::InitialReview,
            reviewer2_session,
            RuntimeTurnKey::from_counter(3).unwrap(),
            "reviewer2",
        );
        assert_eq!(core.active_turns.len(), 2);
        assert!(matches!(
            core.progress_events.as_slice(),
            [SessionProgressEvent::ReviewRequested {
                sequence: 1,
                review_generation: 1,
                ..
            }]
        ));
    }

    #[test]
    fn peer_failure_preserves_first_response_without_aggregating_it() {
        let (mut core, reviewer1) = active_reviewer_core(3);
        complete_lane_turn(
            &mut core,
            reviewer1.task,
            reviewer1.lane,
            reviewer1.session,
            reviewer1.turn,
            reviewer1.token,
            lgtm(),
        );
        let reviewer2_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
        let reviewer2 = core.active_turns[&reviewer2_lane].clone();
        core.reduce(SupervisorEvent::TurnFailed {
            expected_version: core.version(),
            task_ordinal: reviewer2.task_ordinal,
            lane: reviewer2_lane,
            review_generation: reviewer2.review_generation,
            session: reviewer2.session,
            turn: reviewer2.turn,
            completion_token: reviewer2.completion_token,
            failure: runtime_failure(
                RuntimeFailureClass::Process,
                false,
                "Reviewer2 exited before a durable response",
            ),
        })
        .unwrap();

        let snapshot = core.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].review_round, 0);
        assert_eq!(snapshot.tasks[0].review_generation, 1);
        assert_eq!(
            snapshot.tasks[0].reviewers[0].current_verdict,
            Some(ReviewerVerdict::Lgtm)
        );
        assert_eq!(snapshot.tasks[0].reviewers[1].current_verdict, None);
        assert!(
            core.progress_events
                .iter()
                .all(|event| !matches!(event, SessionProgressEvent::TaskCompleted { .. }))
        );
    }

    #[test]
    fn full_dual_review_progress_capacity_retains_exactly_5186_events() {
        let mut core = new_core();
        let tasks = (0..MAX_TASKS)
            .map(|ordinal| task(&format!("task-{ordinal}"), "/repo", MAX_REVIEW_ROUNDS))
            .collect();
        bind(&mut core, tasks);

        let mut sequence = 0u32;
        for task_ordinal in 0..MAX_TASKS {
            let task_key = core.tasks[task_ordinal].spec.task_key.clone();
            for review_generation in 1..=u32::from(MAX_REVIEW_ROUNDS) {
                sequence += 1;
                core.progress_events
                    .push(SessionProgressEvent::CandidatePublished {
                        sequence,
                        task_ordinal: u32::try_from(task_ordinal).unwrap(),
                        task_key: task_key.clone(),
                        completed_tasks: u32::try_from(task_ordinal).unwrap(),
                        total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                        review_generation,
                        developer_final_path: "/artifacts/developer/final.md".into(),
                        github: GitHubTaskProgressSnapshot {
                            pr_number: 1,
                            pr_url: "https://github.com/owner/repo/pull/1".into(),
                            task_base_sha: "a".repeat(40),
                            published_head_sha: "b".repeat(40),
                            check_run_id: Some(1),
                            check_url: Some("https://github.com/owner/repo/runs/1".into()),
                        },
                    });
                sequence += 1;
                core.progress_events
                    .push(SessionProgressEvent::ReviewRequested {
                        sequence,
                        task_ordinal: u32::try_from(task_ordinal).unwrap(),
                        task_key: task_key.clone(),
                        completed_tasks: u32::try_from(task_ordinal).unwrap(),
                        total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                        review_round: review_generation - 1,
                        review_generation,
                        max_review_rounds: MAX_REVIEW_ROUNDS,
                        developer_final_path: "/artifacts/developer/final.md".into(),
                        task_document_path: core.tasks[task_ordinal]
                            .spec
                            .task_document_path
                            .clone(),
                        design_document_paths: core.tasks[task_ordinal]
                            .spec
                            .design_document_paths
                            .clone(),
                        task_selector: task_key.clone(),
                        clarification_record_count: 0,
                        reviewer_bindings: core.reviewer_bindings.clone(),
                        github: None,
                    });
                for (response_index, reviewer_id) in reviewer_ids().into_iter().enumerate() {
                    sequence += 1;
                    core.progress_events
                        .push(SessionProgressEvent::ReviewResponded {
                            sequence,
                            task_ordinal: u32::try_from(task_ordinal).unwrap(),
                            task_key: task_key.clone(),
                            completed_tasks: u32::try_from(task_ordinal).unwrap(),
                            total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                            review_round: if response_index == 0 {
                                review_generation - 1
                            } else {
                                review_generation
                            },
                            review_generation,
                            max_review_rounds: MAX_REVIEW_ROUNDS,
                            reviewer_id,
                            reviewer_verdict: ReviewerVerdict::RequestChanges,
                            developer_final_path: "/artifacts/developer/final.md".into(),
                            reviewer_final_message_paths: vec![
                                "/artifacts/reviewer/final.md".into(),
                            ],
                            responses_received: u8::try_from(response_index + 1).unwrap(),
                            responses_expected: 2,
                            github: None,
                        });
                }
            }
            sequence += 1;
            core.progress_events
                .push(SessionProgressEvent::TaskCompleted {
                    sequence,
                    task_ordinal: u32::try_from(task_ordinal).unwrap(),
                    task_key,
                    completed_tasks: u32::try_from(task_ordinal + 1).unwrap(),
                    total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                    review_round: u32::from(MAX_REVIEW_ROUNDS),
                    review_generation: u32::from(MAX_REVIEW_ROUNDS),
                    max_review_rounds: MAX_REVIEW_ROUNDS,
                    outcome: TaskCompletionOutcome::ReviewExhausted,
                    developer_final_path: "/artifacts/developer/final.md".into(),
                    reviewers: reviewer_ids()
                        .into_iter()
                        .map(|reviewer_id| ReviewerResultSnapshot {
                            reviewer_id,
                            session_bound: true,
                            current_generation: Some(u32::from(MAX_REVIEW_ROUNDS)),
                            current_verdict: Some(ReviewerVerdict::RequestChanges),
                            current_final_message_paths: vec![
                                "/artifacts/reviewer/final.md".into(),
                            ],
                        })
                        .collect(),
                    github: None,
                });
        }

        sequence += 1;
        core.progress_events
            .push(SessionProgressEvent::MergeWaiting {
                sequence,
                completed_tasks: 0,
                total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                pr_number: 1,
                pr_url: "https://github.com/owner/repo/pull/1".into(),
                final_head_sha: "b".repeat(40),
                check_run_id: 1,
                check_url: "https://github.com/owner/repo/runs/1".into(),
            });
        sequence += 1;
        core.progress_events
            .push(SessionProgressEvent::RunFinalizing {
                sequence,
                completed_tasks: 0,
                total_tasks: u32::try_from(MAX_TASKS).unwrap(),
                pr_number: 1,
                pr_url: "https://github.com/owner/repo/pull/1".into(),
                final_head_sha: "b".repeat(40),
                merge_sha: "c".repeat(40),
            });

        assert_eq!(MAX_PROGRESS_EVENTS_PER_RUN, 5186);
        assert_eq!(
            usize::try_from(sequence).unwrap(),
            MAX_PROGRESS_EVENTS_PER_RUN
        );
        core.assert_invariants().unwrap();
        assert_eq!(
            core.progress_event_after("run-1", sequence - 1)
                .unwrap()
                .unwrap()
                .sequence(),
            sequence
        );
        assert!(
            core.progress_event_after("run-1", sequence)
                .unwrap()
                .is_none()
        );

        let mut overflow = core.clone();
        let mut impossible = overflow.progress_events.last().unwrap().clone();
        let SessionProgressEvent::RunFinalizing {
            sequence: impossible_sequence,
            ..
        } = &mut impossible
        else {
            unreachable!()
        };
        *impossible_sequence += 1;
        overflow.progress_events.push(impossible);
        assert_eq!(
            overflow.assert_invariants().unwrap_err().code,
            SupervisorErrorCode::InvariantViolation
        );
    }

    #[test]
    fn clarified_reviewer_round_keeps_original_then_clarification_paths() {
        let (mut core, reviewer) = active_reviewer_core(2);
        complete_turn(
            &mut core,
            reviewer.task,
            reviewer.role,
            reviewer.session,
            reviewer.turn,
            reviewer.token,
            RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                preceding_final_message_paths: vec![PathBuf::from(
                    "/artifacts/reviewer/original/native-final.partial",
                )],
            }),
        );
        assert_eq!(core.tasks[0].review_round, 0);
        assert_eq!(
            core.progress_events
                .iter()
                .filter(|event| matches!(event, SessionProgressEvent::ReviewResponded { .. }))
                .count(),
            1,
            "verdict clarification must remain one logical Reviewer response"
        );
        let peer_lane = WorkerLane::Reviewer(ReviewerId::Reviewer2);
        let peer = core.active_turns[&peer_lane].clone();
        complete_lane_turn(
            &mut core,
            peer.task_ordinal,
            peer_lane,
            peer.session,
            peer.turn,
            &peer.completion_token,
            request_changes(),
        );
        let task = &core.snapshot().tasks[0];
        assert_eq!(task.state, TaskState::Developing);
        assert_eq!(
            reviewer_paths(task),
            vec![
                "/artifacts/reviewer/original/native-final.partial",
                "/artifacts/reviewer/turn-2/native-final.partial",
                "/artifacts/reviewer/turn-10002/native-final.partial",
            ]
        );
        assert_eq!(
            joined_reviewer_verdict(task),
            Some(ReviewerVerdict::RequestChanges)
        );
        assert_eq!(task.review_round, 1);
        assert_eq!(
            task.reviewers[0].current_final_message_paths.len(),
            2,
            "one logical Reviewer response may contain at most original plus clarification"
        );
        assert_eq!(
            core.progress_events
                .iter()
                .filter(|event| matches!(event, SessionProgressEvent::ReviewResponded { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn failed_and_canceled_terminals_keep_successfully_published_peer_paths() {
        let (mut before_review, developer) = active_core();
        before_review
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: before_review.version(),
                task_ordinal: developer.task,
                lane: developer.lane,
                review_generation: developer.review_generation,
                session: developer.session,
                turn: developer.turn,
                completion_token: developer.token.into(),
                failure: runtime_failure(
                    RuntimeFailureClass::Process,
                    false,
                    "initial development failed",
                ),
            })
            .unwrap();
        let before_review_task = &before_review.snapshot().tasks[0];
        assert!(before_review_task.latest_developer_final_path.is_none());
        assert!(reviewer_paths(before_review_task).is_empty());
        assert_eq!(joined_reviewer_verdict(before_review_task), None);

        let (mut during_review, reviewer) = active_reviewer_core(2);
        during_review
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: during_review.version(),
                task_ordinal: reviewer.task,
                lane: reviewer.lane,
                review_generation: reviewer.review_generation,
                session: reviewer.session,
                turn: reviewer.turn,
                completion_token: reviewer.token.into(),
                failure: runtime_failure(
                    RuntimeFailureClass::Process,
                    false,
                    "review process failed before publishing a final",
                ),
            })
            .unwrap();
        let during_review_task = &during_review.snapshot().tasks[0];
        assert_eq!(
            during_review_task.latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-1/native-final.partial")
        );
        assert!(reviewer_paths(during_review_task).is_empty());
        assert_eq!(joined_reviewer_verdict(during_review_task), None);

        let (mut after_review, reviewer) = active_reviewer_core(2);
        complete_review(&mut after_review, reviewer, request_changes());
        let before_terminal = after_review.snapshot().tasks[0].clone();
        assert_eq!(
            before_terminal.latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-1/native-final.partial")
        );
        assert_eq!(
            reviewer_paths(&before_terminal),
            [
                "/artifacts/reviewer/turn-2/native-final.partial",
                "/artifacts/reviewer/turn-10002/native-final.partial",
            ]
        );
        assert_eq!(
            joined_reviewer_verdict(&before_terminal),
            Some(ReviewerVerdict::RequestChanges)
        );

        let mut canceled = after_review.clone();
        canceled
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: canceled.version(),
                reason: "stop after a published review".into(),
            })
            .unwrap();
        let canceled_task = &canceled.snapshot().tasks[0];
        assert_eq!(
            canceled_task.latest_developer_final_path,
            before_terminal.latest_developer_final_path
        );
        assert_eq!(
            reviewer_paths(canceled_task),
            reviewer_paths(&before_terminal)
        );
        assert_eq!(
            joined_reviewer_verdict(canceled_task),
            joined_reviewer_verdict(&before_terminal)
        );

        let developer_session = after_review.tasks[0].developer_session.unwrap();
        let developer_turn = RuntimeTurnKey::from_counter(3).unwrap();
        start_turn(
            &mut after_review,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
            developer_session,
            developer_turn,
            "failed-correction",
        );
        after_review
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: after_review.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: developer_session,
                turn: developer_turn,
                completion_token: "failed-correction".into(),
                failure: runtime_failure(
                    RuntimeFailureClass::Process,
                    false,
                    "correction process failed",
                ),
            })
            .unwrap();
        let failed_task = &after_review.snapshot().tasks[0];
        assert_eq!(
            failed_task.latest_developer_final_path,
            before_terminal.latest_developer_final_path
        );
        assert_eq!(
            reviewer_paths(failed_task),
            reviewer_paths(&before_terminal)
        );
        assert_eq!(
            joined_reviewer_verdict(failed_task),
            joined_reviewer_verdict(&before_terminal)
        );
    }

    #[test]
    fn two_task_journey_closes_each_runtime_before_the_next_task_opens() {
        let mut core = new_core();
        bind(
            &mut core,
            vec![
                task("one", "/repo", MIN_DUAL_REVIEW_ROUNDS),
                task("two", "/repo", MIN_DUAL_REVIEW_ROUNDS),
            ],
        );
        authorize(&mut core);

        let developer = start_first_developer(&mut core, 0, 1, 1, "d1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "r1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, lgtm()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::OpenTaskRuntime { task_ordinal: 1 },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.current_task(), Some(1));
        assert!(matches!(
            core.progress_event_after("run-1", 3).unwrap(),
            Some(SessionProgressEvent::TaskCompleted {
                sequence: 4,
                task_ordinal: 0,
                completed_tasks: 1,
                total_tasks: 2,
                ..
            })
        ));

        let developer = start_first_developer(&mut core, 1, 3, 3, "d2");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 1, 4, 4, "r2", true);
        assert!(matches!(
            core.progress_event_after("run-1", 4).unwrap(),
            Some(SessionProgressEvent::ReviewRequested {
                sequence: 5,
                task_ordinal: 1,
                completed_tasks: 1,
                total_tasks: 2,
                ..
            })
        ));
        complete_review(&mut core, reviewer, lgtm());
        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(
            core.tasks.iter().map(|task| task.state).collect::<Vec<_>>(),
            [TaskState::Lgtm, TaskState::Lgtm]
        );
        assert_eq!(
            core.tasks[0].developer_session.unwrap().counter(),
            1,
            "first task keeps its logical session evidence"
        );
        assert_eq!(core.tasks[1].developer_session.unwrap().counter(), 3);
        let snapshot = core.snapshot();
        assert_eq!(
            reviewer_paths(&snapshot.tasks[0]),
            [
                "/artifacts/reviewer/turn-2/native-final.partial",
                "/artifacts/reviewer/turn-10002/native-final.partial",
            ]
        );
        assert_eq!(
            reviewer_paths(&snapshot.tasks[1]),
            [
                "/artifacts/reviewer/turn-4/native-final.partial",
                "/artifacts/reviewer/turn-10004/native-final.partial",
            ]
        );
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| joined_reviewer_verdict(task) == Some(ReviewerVerdict::Lgtm))
        );
    }

    #[test]
    fn request_changes_reuses_exact_role_sessions_then_lgtm() {
        let mut core = new_core();
        bind(&mut core, vec![task("one", "/repo", 3)]);
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "d1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "r1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, request_changes()),
            vec![
                SupervisorEffect::StartTurn {
                    task_ordinal: 0,
                    lane: WorkerLane::Developer,
                    review_generation: None,
                    purpose: RuntimeTurnPurpose::DeveloperCorrection,
                    session: RuntimeSessionKey::from_counter(1).unwrap(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let correction_turn = RuntimeTurnKey::from_counter(3).unwrap();
        start_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
            RuntimeSessionKey::from_counter(1).unwrap(),
            correction_turn,
            "d2",
        );
        complete_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeSessionKey::from_counter(1).unwrap(),
            correction_turn,
            "d2",
            ready(),
        );
        assert_eq!(core.tasks[0].review_round, 1);
        assert_eq!(core.tasks[0].review_generation, 2);
        assert!(
            core.tasks[0].reviewer_results.is_empty(),
            "a Developer amendment must invalidate both prior verdicts before rereview"
        );
        assert!(core.tasks[0].latest_reviewer_final_paths.is_empty());
        assert!(reviewer_ids().into_iter().all(|reviewer_id| {
            core.tasks[0].historical_reviewer_final_paths[&reviewer_id].len() == 1
        }));
        let rereviewer = start_reviewer(&mut core, 0, 2, 4, "r2", false);
        complete_review(&mut core, rereviewer, lgtm());

        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(core.tasks[0].review_round, 2);
        assert_eq!(
            core.tasks[0].developer_session.unwrap().counter(),
            1,
            "Developer correction must use the first logical session"
        );
        assert_eq!(
            core.tasks[0].reviewer_sessions[&ReviewerId::Reviewer1].counter(),
            2,
            "Reviewer re-review must use the first logical session"
        );
        assert_eq!(
            core.tasks[0].reviewer_sessions[&ReviewerId::Reviewer2].counter(),
            10_002,
            "Reviewer2 re-review must use its own first logical session"
        );
        let task = &core.snapshot().tasks[0];
        assert_eq!(
            task.latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-3/native-final.partial")
        );
        assert_eq!(
            reviewer_paths(task),
            [
                "/artifacts/reviewer/turn-4/native-final.partial",
                "/artifacts/reviewer/turn-10004/native-final.partial",
            ],
            "the Architect handoff carries only the final review round"
        );
        assert_eq!(joined_reviewer_verdict(task), Some(ReviewerVerdict::Lgtm));
        assert!(reviewer_ids().into_iter().all(|reviewer_id| {
            core.tasks[0].historical_reviewer_final_paths[&reviewer_id].len() == 1
        }));
        assert_eq!(
            task.reviewers
                .iter()
                .flat_map(|reviewer| &reviewer.current_final_message_paths)
                .count(),
            2,
            "status exposes only the current generation, not historical Reviewer paths"
        );
    }

    #[test]
    fn multiple_request_changes_can_reach_max_minus_one_then_lgtm() {
        let (mut core, mut reviewer) = active_reviewer_core(3);
        assert!(matches!(
            complete_review(&mut core, reviewer, request_changes()).first(),
            Some(SupervisorEffect::StartTurn {
                purpose: RuntimeTurnPurpose::DeveloperCorrection,
                ..
            })
        ));
        reviewer = correct_and_start_rereview(&mut core, 0, 3, 4, "developer-2", "reviewer-2");
        complete_review(&mut core, reviewer, request_changes());
        reviewer = correct_and_start_rereview(&mut core, 0, 5, 6, "developer-3", "reviewer-3");
        complete_review(&mut core, reviewer, lgtm());

        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(core.tasks[0].state, TaskState::Lgtm);
        assert_eq!(core.tasks[0].review_round, 3);
    }

    #[test]
    fn request_changes_at_exact_max_exhausts_then_opens_the_next_task() {
        let mut core = new_core();
        bind(
            &mut core,
            vec![task("one", "/repo", 2), task("two", "/repo", 2)],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "d1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "r1", true);
        assert_eq!(
            exhaust_review(&mut core, reviewer),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::OpenTaskRuntime { task_ordinal: 1 },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(
            core.tasks[0].review_round,
            u32::from(MIN_DUAL_REVIEW_ROUNDS)
        );
        let exhausted = &core.snapshot().tasks[0];
        assert_eq!(
            exhausted.latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-13/native-final.partial")
        );
        assert_eq!(
            reviewer_paths(exhausted),
            [
                "/artifacts/reviewer/turn-14/native-final.partial",
                "/artifacts/reviewer/turn-10014/native-final.partial",
            ],
            "review exhaustion carries only the last REQUEST_CHANGES round"
        );
        assert_eq!(
            joined_reviewer_verdict(exhausted),
            Some(ReviewerVerdict::RequestChanges)
        );
        assert!(matches!(
            core.progress_event_after("run-1", 21).unwrap(),
            Some(SessionProgressEvent::TaskCompleted {
                sequence: 22,
                task_ordinal: 0,
                completed_tasks: 1,
                total_tasks: 2,
                outcome: TaskCompletionOutcome::ReviewExhausted,
                reviewers,
                ..
            }) if reviewers.iter().all(|reviewer| {
                reviewer.current_verdict == Some(ReviewerVerdict::RequestChanges)
            })
        ));
        assert_eq!(core.current_task(), Some(1));
        start_first_developer(&mut core, 1, 3, 15, "next-developer");
        assert_eq!(core.tasks[1].state, TaskState::Developing);
    }

    #[test]
    fn runtime_failure_retryability_matrix_is_role_exact_and_transactional() {
        for (role, retryable) in [
            (WorkerRole::Developer, false),
            (WorkerRole::Developer, true),
            (WorkerRole::Reviewer, false),
            (WorkerRole::Reviewer, true),
        ] {
            let (mut core, active) = match role {
                WorkerRole::Developer => active_core(),
                WorkerRole::Reviewer => active_reviewer_core(2),
            };

            let mut rejected = fail_turn_event(
                &core,
                active,
                RuntimeFailureClass::Contract,
                retryable,
                "typed final outcome was missing or invalid",
            );
            if let SupervisorEvent::TurnFailed {
                completion_token, ..
            } = &mut rejected
            {
                *completion_token = "wrong-token".into();
            }
            let before = core.clone();
            let error = core.reduce(rejected).unwrap_err();
            assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
            assert_eq!(
                core, before,
                "{role:?} retryable={retryable} rejection mutated the core"
            );

            let effects = core
                .reduce(fail_turn_event(
                    &core,
                    active,
                    RuntimeFailureClass::Contract,
                    retryable,
                    "typed final outcome was missing or invalid",
                ))
                .unwrap();
            // Task-agnostic lane: the retryable flag no longer triggers any
            // recovery path — every contract failure is terminal for every
            // role.
            let expected_detail =
                "worker runtime contract failed: typed final outcome was missing or invalid";
            let mut expected = Vec::new();
            if role == WorkerRole::Reviewer {
                let peer = before
                    .active_turns
                    .get(&WorkerLane::Reviewer(ReviewerId::Reviewer2))
                    .expect("Reviewer failure has a live peer");
                expected.push(SupervisorEffect::InterruptTurn {
                    task_ordinal: peer.task_ordinal,
                    lane: peer.lane,
                    session: peer.session,
                    turn: peer.turn,
                });
            }
            expected.extend([
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::NeedsHuman,
                    detail: expected_detail.into(),
                },
                SupervisorEffect::PublishStatus,
            ]);
            assert_eq!(effects, expected);
            let snapshot = core.snapshot();
            assert_eq!(snapshot.state, SessionState::NeedsHuman);
            assert_eq!(snapshot.terminal_detail.as_deref(), Some(expected_detail));
            assert_eq!(
                snapshot.tasks[0].outcome_detail.as_deref(),
                Some(expected_detail)
            );
        }
    }

    #[test]
    fn completion_identity_ordering_and_at_most_once_are_transactional() {
        let (core, active) = active_core();
        let wrong_events = [
            (
                SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: 1,
                    lane: active.lane,
                    review_generation: active.review_generation,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: ready(),
                    final_message_path: PathBuf::from("/artifacts/developer/wrong-task.md"),
                },
                SupervisorErrorCode::InvalidIdentity,
            ),
            (
                SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: active.task,
                    lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                    review_generation: active.review_generation,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: lgtm(),
                    final_message_path: PathBuf::from("/artifacts/reviewer/wrong-role.md"),
                },
                SupervisorErrorCode::InvalidTransition,
            ),
            (
                SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: active.task,
                    lane: active.lane,
                    review_generation: active.review_generation,
                    session: RuntimeSessionKey::from_counter(99).unwrap(),
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: ready(),
                    final_message_path: PathBuf::from("/artifacts/developer/wrong-session.md"),
                },
                SupervisorErrorCode::InvalidIdentity,
            ),
            (
                SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: active.task,
                    lane: active.lane,
                    review_generation: active.review_generation,
                    session: active.session,
                    turn: RuntimeTurnKey::from_counter(99).unwrap(),
                    completion_token: active.token.into(),
                    outcome: ready(),
                    final_message_path: PathBuf::from("/artifacts/developer/wrong-turn.md"),
                },
                SupervisorErrorCode::InvalidIdentity,
            ),
            (
                SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: active.task,
                    lane: active.lane,
                    review_generation: active.review_generation,
                    session: active.session,
                    turn: active.turn,
                    completion_token: "wrong-token".into(),
                    outcome: ready(),
                    final_message_path: PathBuf::from("/artifacts/developer/wrong-token.md"),
                },
                SupervisorErrorCode::InvalidIdentity,
            ),
        ];
        for (event, expected_code) in wrong_events {
            let mut candidate = core.clone();
            let before = candidate.clone();
            assert_eq!(candidate.reduce(event).unwrap_err().code, expected_code);
            assert_eq!(candidate, before);
        }

        let mut before_start = pending_turn_core();
        let before = before_start.clone();
        let error = before_start
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: before_start.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "not-started".into(),
                outcome: ready(),
                final_message_path: PathBuf::from("/artifacts/developer/not-started.md"),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidTransition);
        assert_eq!(before_start, before);

        let mut accepted = core;
        let event = SupervisorEvent::TurnCompleted {
            expected_version: accepted.version(),
            task_ordinal: active.task,
            lane: active.lane,
            review_generation: active.review_generation,
            session: active.session,
            turn: active.turn,
            completion_token: active.token.into(),
            outcome: ready(),
            final_message_path: PathBuf::from("/artifacts/developer/accepted.md"),
        };
        accepted.reduce(event.clone()).unwrap();
        let before = accepted.clone();
        let mut duplicate = event;
        let SupervisorEvent::TurnCompleted {
            expected_version, ..
        } = &mut duplicate
        else {
            unreachable!()
        };
        *expected_version = accepted.version();
        let error = accepted.reduce(duplicate).unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::DuplicateCompletion);
        assert_eq!(accepted, before);

        let (mut wrong_role, active) = active_core();
        let before = wrong_role.clone();
        let error = wrong_role
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: wrong_role.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: lgtm(),
                final_message_path: PathBuf::from("/artifacts/reviewer/wrong-outcome.md"),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidEvent);
        assert_eq!(wrong_role, before);
    }

    #[test]
    fn second_active_turn_and_cross_task_session_reuse_are_rejected() {
        let (mut active_core, _active) = active_core();
        let before = active_core.clone();
        let error = active_core
            .reduce(SupervisorEvent::TurnStarted {
                expected_version: active_core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                purpose: RuntimeTurnPurpose::InitialDevelopment,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                completion_token: "second".into(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidTransition);
        assert_eq!(active_core, before);

        let mut core = new_core();
        bind(
            &mut core,
            vec![task("one", "/repo", 1), task("two", "/repo", 1)],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "d1");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "r1", true);
        complete_review(&mut core, reviewer, lgtm());
        open_runtime(&mut core, 1);
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::RoleSessionOpened {
                expected_version: core.version(),
                task_ordinal: 1,
                lane: WorkerLane::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);
    }

    #[test]
    fn cancel_completion_and_parent_failure_races_have_one_deterministic_winner() {
        let (mut cancel_first, active) = active_core();
        assert_eq!(
            cancel_first
                .reduce(SupervisorEvent::CancelRequested {
                    expected_version: cancel_first.version(),
                    reason: "stop now".into(),
                })
                .unwrap(),
            vec![
                SupervisorEffect::InterruptTurn {
                    task_ordinal: 0,
                    lane: WorkerLane::Developer,
                    session: active.session,
                    turn: active.turn,
                },
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::Canceled,
                    detail: "canceled by explicit Architect-session request".into(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let error = cancel_first
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: cancel_first.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: ready(),
                final_message_path: PathBuf::from("/artifacts/developer/after-cancel.md"),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::Terminal);

        let (mut completion_first, active) = active_core();
        complete_turn(
            &mut completion_first,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        completion_first
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: completion_first.version(),
                reason: "stop after completion".into(),
            })
            .unwrap();
        assert_eq!(completion_first.session_state(), SessionState::Canceled);

        let (mut parent_first, active) = active_core();
        parent_first
            .reduce(SupervisorEvent::ParentStopping {
                expected_version: parent_first.version(),
            })
            .unwrap();
        assert_eq!(
            parent_first
                .reduce(SupervisorEvent::TurnFailed {
                    expected_version: parent_first.version(),
                    task_ordinal: 0,
                    lane: WorkerLane::Developer,
                    review_generation: None,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    failure: runtime_failure(RuntimeFailureClass::Process, false, "late exit",),
                })
                .unwrap_err()
                .code,
            SupervisorErrorCode::Terminal
        );

        let (mut failure_first, active) = active_core();
        failure_first
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: failure_first.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                failure: runtime_failure(RuntimeFailureClass::Process, false, "exit"),
            })
            .unwrap();
        assert_eq!(failure_first.session_state(), SessionState::NeedsHuman);
        assert_eq!(
            failure_first
                .reduce(SupervisorEvent::ParentStopping {
                    expected_version: failure_first.version(),
                })
                .unwrap_err()
                .code,
            SupervisorErrorCode::Terminal
        );
    }

    #[test]
    fn dual_reviewer_completion_cancel_timeout_and_parent_stop_races_close_both_lanes() {
        let (mut completion_first, reviewer1) = active_reviewer_core(3);
        complete_lane_turn(
            &mut completion_first,
            reviewer1.task,
            reviewer1.lane,
            reviewer1.session,
            reviewer1.turn,
            reviewer1.token,
            lgtm(),
        );
        completion_first
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: completion_first.version(),
                reason: "cancel after first Reviewer response".into(),
            })
            .unwrap();
        let snapshot = completion_first.snapshot();
        assert_eq!(snapshot.state, SessionState::Canceled);
        assert_eq!(snapshot.tasks[0].review_round, 0);
        assert_eq!(snapshot.tasks[0].review_generation, 1);
        assert_eq!(
            snapshot.tasks[0].reviewers[0].current_verdict,
            Some(ReviewerVerdict::Lgtm)
        );
        assert_eq!(snapshot.tasks[0].reviewers[1].current_verdict, None);

        let (mut cancel_first, reviewer1) = active_reviewer_core(3);
        let reviewer2 =
            cancel_first.active_turns[&WorkerLane::Reviewer(ReviewerId::Reviewer2)].clone();
        let effects = cancel_first
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: cancel_first.version(),
                reason: "cancel both Reviewers".into(),
            })
            .unwrap();
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SupervisorEffect::InterruptTurn { .. }))
                .count(),
            2
        );
        for (lane, active) in [
            (
                WorkerLane::Reviewer(ReviewerId::Reviewer1),
                (
                    reviewer1.session,
                    reviewer1.turn,
                    reviewer1.token.to_owned(),
                ),
            ),
            (
                WorkerLane::Reviewer(ReviewerId::Reviewer2),
                (
                    reviewer2.session,
                    reviewer2.turn,
                    reviewer2.completion_token,
                ),
            ),
        ] {
            assert!(
                cancel_first
                    .reduce(SupervisorEvent::TurnCompleted {
                        expected_version: cancel_first.version(),
                        task_ordinal: 0,
                        lane,
                        review_generation: Some(1),
                        session: active.0,
                        turn: active.1,
                        completion_token: active.2,
                        outcome: lgtm(),
                        final_message_path: PathBuf::from("/artifacts/reviewer/late.md"),
                    })
                    .is_err()
            );
        }

        let (mut timed_out, _) = active_reviewer_core(3);
        let reviewer2 =
            timed_out.active_turns[&WorkerLane::Reviewer(ReviewerId::Reviewer2)].clone();
        let effects = timed_out
            .reduce(SupervisorEvent::Timeout {
                expected_version: timed_out.version(),
                task_ordinal: reviewer2.task_ordinal,
                lane: reviewer2.lane,
                review_generation: reviewer2.review_generation,
                session: reviewer2.session,
                turn: reviewer2.turn,
                completion_token: reviewer2.completion_token,
            })
            .unwrap();
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SupervisorEffect::InterruptTurn { .. }))
                .count(),
            2
        );
        assert_eq!(timed_out.session_state(), SessionState::NeedsHuman);
        assert_eq!(timed_out.tasks[0].review_round, 0);
        assert_eq!(timed_out.tasks[0].review_generation, 1);

        let (mut parent_stopping, _) = active_reviewer_core(3);
        let effects = parent_stopping
            .reduce(SupervisorEvent::ParentStopping {
                expected_version: parent_stopping.version(),
            })
            .unwrap();
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, SupervisorEffect::InterruptTurn { .. }))
                .count(),
            2
        );
        assert_eq!(parent_stopping.session_state(), SessionState::Canceled);
        assert_eq!(parent_stopping.tasks[0].review_round, 0);
        assert_eq!(parent_stopping.tasks[0].review_generation, 1);
    }

    #[test]
    fn plan_task_count_review_round_and_status_ordinal_bounds_are_exact() {
        assert_eq!(
            plan_result(Vec::new()).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );
        assert_eq!(
            plan_result(vec![task("one", "/repo", 1)])
                .unwrap()
                .tasks()
                .len(),
            1
        );

        let tasks_64 = (0..64)
            .map(|index| task(&format!("task-{index:02}"), "/repo", 20))
            .collect::<Vec<_>>();
        let core = plan_result(tasks_64).unwrap();
        let snapshot = core.snapshot();
        assert_eq!(snapshot.tasks.len(), 64);
        assert_eq!(snapshot.tasks.first().unwrap().ordinal, 0);
        assert_eq!(snapshot.tasks.last().unwrap().ordinal, 63);
        assert_eq!(snapshot.tasks.first().unwrap().max_review_rounds, 20);

        let tasks_65 = (0..65)
            .map(|index| task(&format!("task-{index:02}"), "/repo", 1))
            .collect::<Vec<_>>();
        assert_eq!(
            plan_result(tasks_65).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );

        for (rounds, accepted) in [
            (0, false),
            (1, false),
            (3, false),
            (6, false),
            (7, true),
            (20, true),
            (21, false),
        ] {
            let mut candidate = task("rounds", "/repo", MIN_DUAL_REVIEW_ROUNDS);
            candidate.max_review_rounds = rounds;
            assert_eq!(
                plan_result(vec![candidate]).is_ok(),
                accepted,
                "unexpected max_review_rounds boundary result for {rounds}"
            );
        }
        assert!(new_core().snapshot().tasks.is_empty());
    }

    #[test]
    fn every_file_backed_task_field_bound_has_below_equal_and_above_cases() {
        fn accepted(task: TaskDraft) -> bool {
            plan_result(vec![task]).is_ok()
        }

        for (length, expected) in [
            (0, false),
            (1, true),
            (127, true),
            (128, true),
            (129, false),
        ] {
            let mut candidate = task("key", "/repo", 1);
            candidate.task_key = "k".repeat(length);
            assert_eq!(accepted(candidate), expected, "task_key length {length}");
        }
        for (length, expected) in [
            (0, false),
            (1, true),
            (511, true),
            (512, true),
            (513, false),
        ] {
            let mut candidate = task("title", "/repo", 1);
            candidate.title = "t".repeat(length);
            assert_eq!(accepted(candidate), expected, "title length {length}");
        }
        for (length, expected) in [(2, true), (4095, true), (4096, true), (4097, false)] {
            let mut candidate = task("root", "/repo", 1);
            candidate.repository_root = format!("/{}", "r".repeat(length - 1));
            assert_eq!(
                accepted(candidate),
                expected,
                "repository root length {length}"
            );
        }
        for (length, expected) in [
            (0, false),
            (1, true),
            (4095, true),
            (4096, true),
            (4097, false),
        ] {
            let mut candidate = task("task-document", "/repo", 1);
            candidate.task_document_path = if length == 0 {
                String::new()
            } else {
                format!("/{}", "t".repeat(length - 1))
            };
            assert_eq!(
                accepted(candidate),
                expected,
                "task document path length {length}"
            );
        }
        for (length, expected) in [
            (0, false),
            (1, true),
            (4095, true),
            (4096, true),
            (4097, false),
        ] {
            let mut candidate = task("selector", "/repo", 1);
            candidate.task_selector = "s".repeat(length);
            assert_eq!(accepted(candidate), expected, "selector length {length}");
        }
        for (count, expected) in [(255, true), (256, true), (257, false)] {
            let mut candidate = task("design-list-count", "/repo", 1);
            candidate.design_document_paths = (0..count)
                .map(|index| format!("/project/design-{index}.md"))
                .collect();
            assert_eq!(accepted(candidate), expected, "design path count {count}");
        }

        let mut relative_task_path = task("relative-task-path", "/repo", 1);
        relative_task_path.task_document_path = "current_todo.md".into();
        assert!(!accepted(relative_task_path));
        let mut relative_design_path = task("relative-design-path", "/repo", 1);
        relative_design_path.design_document_paths = vec!["design.md".into()];
        assert!(!accepted(relative_design_path));
        let mut duplicate_path = task("duplicate-path", "/repo", 1);
        duplicate_path.design_document_paths =
            vec!["/project/design.md".into(), "/project/design.md".into()];
        assert!(!accepted(duplicate_path));
        let duplicate_tasks = vec![
            task("duplicate-task", "/repo", 1),
            task("duplicate-task", "/repo", 1),
        ];
        assert_eq!(
            plan_result(duplicate_tasks).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );
    }

    #[test]
    fn session_and_plan_versions_hashes_and_overflow_fail_without_mutation() {
        let mut core = new_core();
        let first_event = plan_event(&core, "first");
        core.reduce(first_event).unwrap();
        assert_eq!(core.version(), 1);

        for expected_version in [0, 2, u64::MAX] {
            let mut candidate = core.clone();
            let before = candidate.clone();
            let mut event = plan_event(&candidate, "replacement");
            let SupervisorEvent::PlanBound {
                expected_version: supplied,
                ..
            } = &mut event
            else {
                unreachable!()
            };
            *supplied = expected_version;
            assert_eq!(
                candidate.reduce(event).unwrap_err().code,
                SupervisorErrorCode::VersionMismatch
            );
            assert_eq!(candidate, before);
        }
        let mut exact = core.clone();
        exact.reduce(plan_event(&exact, "replacement")).unwrap();
        assert_eq!(exact.version(), 2);
        assert_eq!(exact.plan_version(), Some(2));

        for mutation in [
            (None, core.plan_hash.clone()),
            (core.plan_version, None),
            (Some(2), core.plan_hash.clone()),
            (core.plan_version, Some("b".repeat(64))),
            (core.plan_version, Some("short".into())),
        ] {
            let mut candidate = core.clone();
            let before = candidate.clone();
            assert!(
                candidate
                    .reduce(SupervisorEvent::ExecutionAuthorized {
                        expected_version: candidate.version(),
                        plan_version: mutation.0,
                        plan_hash: mutation.1,
                    })
                    .is_err()
            );
            assert_eq!(candidate, before);
        }

        let mut version_overflow = new_core();
        version_overflow.version = u64::MAX;
        let before = version_overflow.clone();
        assert_eq!(
            version_overflow
                .reduce(SupervisorEvent::CancelRequested {
                    expected_version: u64::MAX,
                    reason: "overflow".into(),
                })
                .unwrap_err()
                .code,
            SupervisorErrorCode::Overflow
        );
        assert_eq!(version_overflow, before);

        let mut plan_overflow = new_core();
        plan_overflow.next_plan_version = u64::MAX;
        let event = plan_event(&plan_overflow, "overflow");
        let before = plan_overflow.clone();
        assert_eq!(
            plan_overflow.reduce(event).unwrap_err().code,
            SupervisorErrorCode::Overflow
        );
        assert_eq!(plan_overflow, before);
    }

    #[test]
    fn invalid_final_paths_are_rejected_without_consuming_the_turn() {
        let (reviewer_core, active) = active_reviewer_core(2);
        let invalid_reviewer = [
            ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::Lgtm,
                preceding_final_message_paths: vec![PathBuf::from("relative.md")],
            },
            ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                preceding_final_message_paths: vec![PathBuf::from("/one"), PathBuf::from("/two")],
            },
        ];
        for outcome in invalid_reviewer {
            let mut core = reviewer_core.clone();
            let before = core.clone();
            let error = core
                .reduce(SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: 0,
                    lane: active.lane,
                    review_generation: active.review_generation,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: RuntimeOutcome::Reviewer(outcome),
                    final_message_path: PathBuf::from("/review/current.md"),
                })
                .unwrap_err();
            assert_eq!(error.code, SupervisorErrorCode::InvalidEvent);
            assert_eq!(core, before);
        }
    }

    #[test]
    fn timeout_and_runtime_failures_have_exact_safe_outputs() {
        let mut terminal_details = BTreeSet::new();

        let (mut timed_out, active) = active_core();
        let effects = timed_out
            .reduce(SupervisorEvent::Timeout {
                expected_version: timed_out.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
            })
            .unwrap();
        assert_eq!(
            effects,
            vec![
                SupervisorEffect::InterruptTurn {
                    task_ordinal: 0,
                    lane: WorkerLane::Developer,
                    session: active.session,
                    turn: active.turn,
                },
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::NeedsHuman,
                    detail: "worker turn timed out".into(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        terminal_details.insert(timed_out.snapshot().terminal_detail.unwrap());

        for (class, expected_state, expected) in [
            (
                RuntimeFailureClass::Protocol,
                SessionState::NeedsHuman,
                "worker runtime protocol failed",
            ),
            (
                RuntimeFailureClass::Process,
                SessionState::NeedsHuman,
                "worker runtime process failed",
            ),
            (
                RuntimeFailureClass::Timeout,
                SessionState::NeedsHuman,
                "worker runtime reported a timeout",
            ),
            (
                RuntimeFailureClass::Contract,
                SessionState::NeedsHuman,
                "worker runtime contract failed",
            ),
            (
                RuntimeFailureClass::Canceled,
                SessionState::Failed,
                "worker runtime canceled without a supervisor cancel request",
            ),
        ] {
            let (mut core, active) = active_core();
            core.reduce(SupervisorEvent::TurnFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                failure: runtime_failure(class, false, "codex exec exited with status 7"),
            })
            .unwrap();
            let snapshot = core.snapshot();
            assert_eq!(snapshot.state, expected_state);
            // The class label classifies; the runtime's already-sanitized
            // detail is the evidence and must survive into the report.
            let detail = snapshot.terminal_detail.clone().unwrap();
            assert!(detail.starts_with(expected), "{detail}");
            assert!(
                detail.contains("codex exec exited with status 7"),
                "{detail}"
            );
            terminal_details.insert(detail);
        }
        assert_eq!(terminal_details.len(), 6);
    }

    #[test]
    fn every_driver_failure_class_has_a_distinct_bounded_secret_free_terminal() {
        for (class, expected) in [
            (
                DriverFailureClass::Repository,
                "repository observation failed",
            ),
            (
                DriverFailureClass::Runtime,
                "task worker runtime operation failed",
            ),
            (
                DriverFailureClass::Environment,
                "task-private environment setup failed",
            ),
            (
                DriverFailureClass::Contract,
                "session runtime contract failed",
            ),
            (
                DriverFailureClass::Cleanup,
                "task worker runtime cleanup failed",
            ),
        ] {
            let mut core = authorized_core();
            core.reduce(SupervisorEvent::DriverFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                failure: DriverFailure {
                    class,
                    detail: "RAW_SECRET_DRIVER_DETAIL".into(),
                },
            })
            .unwrap();
            let snapshot = core.snapshot();
            assert_eq!(snapshot.state, SessionState::NeedsHuman);
            assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
            assert_eq!(snapshot.terminal_detail.as_deref(), Some(expected));
            assert!(
                !serde_json::to_string(&snapshot)
                    .unwrap()
                    .contains("RAW_SECRET_DRIVER_DETAIL")
            );
        }

        for length in [
            MAX_CORE_DIAGNOSTIC_BYTES - 1,
            MAX_CORE_DIAGNOSTIC_BYTES,
            MAX_CORE_DIAGNOSTIC_BYTES + 1,
        ] {
            let mut core = authorized_core();
            let before = core.clone();
            let result = core.reduce(SupervisorEvent::DriverFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                failure: DriverFailure {
                    class: DriverFailureClass::Runtime,
                    detail: "x".repeat(length),
                },
            });
            if length <= MAX_CORE_DIAGNOSTIC_BYTES {
                result.unwrap();
                assert_eq!(core.session_state(), SessionState::NeedsHuman);
            } else {
                assert_eq!(result.unwrap_err().code, SupervisorErrorCode::InvalidEvent);
                assert_eq!(core, before);
            }
        }
    }

    #[test]
    fn snapshots_carry_developer_path_without_peer_body() {
        let (mut core, active) = active_core();
        complete_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        let snapshot = core.snapshot();
        assert_eq!(
            snapshot.tasks[0].latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/turn-1/native-final.partial")
        );
        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(!encoded.contains("summary"));
        assert!(!encoded.contains("findings"));
    }

    #[test]
    fn plan_replacement_and_hashing_are_deterministic_and_order_sensitive() {
        let mut first = new_core();
        let tasks = vec![task("one", "/one", 2), task("two", "/two", 2)];
        let hash = first.expected_plan_hash(1, &tasks);
        assert_eq!(hash, first.expected_plan_hash(1, &tasks));

        let mut reversed_tasks = tasks.clone();
        reversed_tasks.reverse();
        assert_ne!(hash, first.expected_plan_hash(1, &reversed_tasks));

        let event = plan_event_for(&first, tasks.clone());
        let mut second = first.clone();
        let effects_first = first.reduce(event.clone()).unwrap();
        let effects_second = second.reduce(event).unwrap();
        assert_eq!(effects_first, effects_second);
        assert_eq!(first, second);

        let old_version = first.plan_version();
        let old_hash = first.plan_hash.clone();
        let replacement = plan_event(&first, "replacement");
        first.reduce(replacement).unwrap();
        assert_eq!(first.plan_version(), Some(2));
        assert_eq!(first.tasks.len(), 1);
        assert_eq!(first.tasks[0].spec.task_key, "replacement");
        assert_ne!(first.plan_hash, old_hash);
        let before = first.clone();
        assert_eq!(
            first
                .reduce(SupervisorEvent::ExecutionAuthorized {
                    expected_version: first.version(),
                    plan_version: old_version,
                    plan_hash: old_hash,
                })
                .unwrap_err()
                .code,
            SupervisorErrorCode::InvalidPlan
        );
        assert_eq!(first, before);
    }

    #[test]
    fn future_task_events_are_rejected_before_their_turn() {
        let mut core = new_core();
        bind(
            &mut core,
            vec![task("one", "/repo", 2), task("two", "/repo", 2)],
        );
        authorize(&mut core);
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::TaskRuntimeOpened {
                expected_version: core.version(),
                task_ordinal: 1,
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);

        let developer = start_first_developer(&mut core, 0, 1, 1, "developer");
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: 1,
                lane: WorkerLane::Developer,
                review_generation: None,
                session: developer.session,
                turn: developer.turn,
                completion_token: developer.token.into(),
                outcome: ready(),
                final_message_path: PathBuf::from("/artifacts/developer/wrong-task.md"),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);
    }

    #[test]
    fn accepted_completion_token_cannot_be_reused_for_a_later_role_turn() {
        let (mut core, developer) = active_core();
        complete_developer_ready(&mut core, developer);
        open_session(
            &mut core,
            0,
            WorkerRole::Reviewer,
            RuntimeSessionKey::from_counter(2).unwrap(),
        );
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::TurnStarted {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                review_generation: Some(core.tasks[0].review_generation),
                purpose: RuntimeTurnPurpose::InitialReview,
                session: RuntimeSessionKey::from_counter(2).unwrap(),
                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                completion_token: developer.token.into(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::DuplicateCompletion);
        assert_eq!(core, before);
    }

    #[test]
    fn core_owned_identifier_and_diagnostic_bounds_are_exact() {
        for (length, accepted) in [
            (0, false),
            (1, true),
            (127, true),
            (128, true),
            (129, false),
        ] {
            assert_eq!(
                SupervisorCore::new(
                    "r".repeat(length),
                    PathBuf::from("/project"),
                    PROFILE_HASH.into(),
                )
                .is_ok(),
                accepted,
                "run id length {length}"
            );
        }
        assert!(
            SupervisorCore::new("run".into(), PathBuf::from("relative"), PROFILE_HASH.into())
                .is_err()
        );
        assert!(
            SupervisorCore::new("run".into(), PathBuf::from("/project"), "short".into()).is_err()
        );

        for (length, accepted) in [
            (0, false),
            (1, true),
            (127, true),
            (128, true),
            (129, false),
        ] {
            let mut core = pending_turn_core();
            let event = SupervisorEvent::TurnStarted {
                expected_version: core.version(),
                task_ordinal: 0,
                lane: WorkerLane::Developer,
                review_generation: None,
                purpose: RuntimeTurnPurpose::InitialDevelopment,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "t".repeat(length),
            };
            assert_eq!(
                core.reduce(event).is_ok(),
                accepted,
                "completion token length {length}"
            );
        }
        for (length, accepted) in [
            (0, false),
            (1, true),
            (4095, true),
            (4096, true),
            (4097, false),
        ] {
            let mut core = new_core();
            assert_eq!(
                core.reduce(SupervisorEvent::CancelRequested {
                    expected_version: 0,
                    reason: "r".repeat(length),
                })
                .is_ok(),
                accepted,
                "cancel reason length {length}"
            );
        }
        for role in [WorkerRole::Developer, WorkerRole::Reviewer] {
            for retryable in [false, true] {
                for (length, accepted) in [
                    (0, false),
                    (1, true),
                    (1023, true),
                    (1024, true),
                    (1025, false),
                ] {
                    let (mut core, active) = match role {
                        WorkerRole::Developer => active_core(),
                        WorkerRole::Reviewer => active_reviewer_core(2),
                    };
                    let before = core.clone();
                    let event = fail_turn_event(
                        &core,
                        active,
                        RuntimeFailureClass::Contract,
                        retryable,
                        "d".repeat(length),
                    );
                    assert_eq!(
                        core.reduce(event).is_ok(),
                        accepted,
                        "{role:?} retryable={retryable} runtime failure detail length {length}"
                    );
                    if !accepted {
                        assert_eq!(
                            core, before,
                            "{role:?} retryable={retryable} invalid bound mutated the core"
                        );
                    }
                }
            }
        }

        let error = SupervisorError::new(
            SupervisorErrorCode::InvalidEvent,
            "界".repeat(MAX_CORE_DIAGNOSTIC_BYTES),
        );
        assert!(error.detail.len() <= MAX_CORE_DIAGNOSTIC_BYTES);
        assert!(std::str::from_utf8(error.detail.as_bytes()).is_ok());
    }

    #[test]
    fn invariant_audit_rejects_corrupted_operation_task_identity_and_terminal_state() {
        let mut cases = Vec::new();

        let mut missing_current = authorized_core();
        missing_current.current_task = None;
        cases.push(("running without current task", missing_current));

        let (mut active_without_runtime, _active) = active_core();
        active_without_runtime.runtime_open = None;
        cases.push(("active turn without runtime", active_without_runtime));

        let (mut excessive_round, _active) = active_reviewer_core(1);
        excessive_round.tasks[0].review_round = 2;
        cases.push(("review round above maximum", excessive_round));

        let mut future_advanced = new_core();
        bind(
            &mut future_advanced,
            vec![task("one", "/repo", 1), task("two", "/repo", 1)],
        );
        authorize(&mut future_advanced);
        future_advanced.tasks[1].state = TaskState::Developing;
        cases.push(("future task advanced", future_advanced));

        let mut terminal_live = completed_core();
        terminal_live.runtime_open = Some(0);
        cases.push(("terminal session with live runtime", terminal_live));

        let mut duplicate_session = bound_core();
        let session = RuntimeSessionKey::from_counter(1).unwrap();
        duplicate_session.tasks[0].developer_session = Some(session);
        duplicate_session.tasks[0]
            .reviewer_sessions
            .insert(ReviewerId::Reviewer1, session);
        duplicate_session.used_sessions.insert(session);
        cases.push(("same logical session for two roles", duplicate_session));

        for (label, core) in cases {
            let error = core.assert_invariants().unwrap_err();
            assert_eq!(
                error.code,
                SupervisorErrorCode::InvariantViolation,
                "{label}"
            );
        }
    }
}
