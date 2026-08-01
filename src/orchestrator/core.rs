//! Pure deterministic domain reducer for one foreground Architect session.
//!
//! `SupervisorCore` owns scheduling decisions and accepts only normalized
//! events. It performs no filesystem, Git, process, clock, network, provider,
//! or terminal I/O. A later `SupervisorDriver` executes the ordered effects and
//! feeds observations back as new events.

use crate::control_api::{
    SessionState, SessionStatusSnapshot, TaskDraft, TaskState, TaskStatusSnapshot, WorkerRole,
};
use crate::worker::runtime::{
    DeveloperOutcomeStatus, DeveloperOutcomeV1, ReviewerOutcomeV1, ReviewerVerdict,
    RuntimeFailureClass, RuntimeOutcome, RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPurpose,
    SanitizedRuntimeFailure,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

const MAX_CORE_DIAGNOSTIC_BYTES: usize = 1024;
const MAX_COMPLETION_TOKEN_BYTES: usize = 128;
const MAX_REPOSITORY_PATHS: usize = 256;
const MAX_REPOSITORY_PATH_BYTES: usize = 4096;
const MAX_TASKS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RepositoryObservation {
    pub repository_root: String,
    pub identity_hash: String,
    pub branch: String,
    pub head: String,
    pub tracked_diff_hash: String,
    pub index_diff_hash: String,
    pub untracked_status_hash: String,
    pub clean: bool,
    /// Normalized paths changed from the checkpoint revision, including any
    /// current worktree/index changes. The driver supplies a sorted unique
    /// inventory; the core applies the task allowlist.
    pub changed_paths: Vec<String>,
    /// The driver-computed ancestry result against the checkpoint expected by
    /// the effect that requested this observation.
    pub head_descends_from_expected: bool,
}

impl RepositoryObservation {
    fn validate(&self) -> Result<(), SupervisorError> {
        validate_absolute_path("repository observation root", &self.repository_root)?;
        validate_sha256("repository identity hash", &self.identity_hash).map_err(|_| {
            SupervisorError::invalid_repository(
                "repository identity hash must be a lowercase SHA-256 digest",
            )
        })?;
        validate_single_line("repository branch", &self.branch, 512).map_err(|_| {
            SupervisorError::invalid_repository("repository branch is not bounded single-line text")
        })?;
        validate_revision("repository HEAD", &self.head)?;
        for (label, hash) in [
            ("tracked diff hash", &self.tracked_diff_hash),
            ("index diff hash", &self.index_diff_hash),
            ("untracked status hash", &self.untracked_status_hash),
        ] {
            validate_sha256(label, hash).map_err(|_| {
                SupervisorError::invalid_repository(format!(
                    "{label} must be a lowercase SHA-256 digest"
                ))
            })?;
        }
        if self.changed_paths.len() > MAX_REPOSITORY_PATHS {
            return Err(SupervisorError::invalid_repository(
                "repository changed-path inventory exceeds 256 entries",
            ));
        }
        let mut previous: Option<&str> = None;
        for path in &self.changed_paths {
            validate_relative_path(path)?;
            if let Some(previous) = previous
                && previous >= path.as_str()
            {
                return Err(SupervisorError::invalid_repository(
                    "repository changed paths must be sorted and unique",
                ));
            }
            previous = Some(path);
        }
        Ok(())
    }

    fn same_git_state(&self, other: &Self) -> bool {
        self.repository_root == other.repository_root
            && self.identity_hash == other.identity_hash
            && self.branch == other.branch
            && self.head == other.head
            && self.tracked_diff_hash == other.tracked_diff_hash
            && self.index_diff_hash == other.index_diff_hash
            && self.untracked_status_hash == other.untracked_status_hash
            && self.clean == other.clean
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskRepositoryBinding {
    pub task_key: String,
    pub observation: RepositoryObservation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryCheckpoint {
    TaskStart,
    DeveloperCompletion,
    DeveloperRecoveryPreflight,
    ReviewerPreflight,
    ReviewerPostflight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SupervisorEventKind {
    PlanBound,
    ExecutionAuthorized,
    TaskRuntimeOpened,
    RoleSessionOpened,
    TurnStarted,
    TurnCompleted,
    TurnFailed,
    DriverFailed,
    RepositoryObserved,
    Timeout,
    CancelRequested,
    ParentStopping,
    StatusRequested,
}

impl SupervisorEventKind {
    pub const ALL: [Self; 13] = [
        Self::PlanBound,
        Self::ExecutionAuthorized,
        Self::TaskRuntimeOpened,
        Self::RoleSessionOpened,
        Self::TurnStarted,
        Self::TurnCompleted,
        Self::TurnFailed,
        Self::DriverFailed,
        Self::RepositoryObserved,
        Self::Timeout,
        Self::CancelRequested,
        Self::ParentStopping,
        Self::StatusRequested,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEvent {
    PlanBound {
        expected_version: u64,
        plan_version: u64,
        plan_hash: String,
        tasks: Vec<TaskDraft>,
        repositories: Vec<TaskRepositoryBinding>,
    },
    ExecutionAuthorized {
        expected_version: u64,
        plan_version: Option<u64>,
        plan_hash: Option<String>,
    },
    TaskRuntimeOpened {
        expected_version: u64,
        task_ordinal: usize,
    },
    RoleSessionOpened {
        expected_version: u64,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
    },
    TurnStarted {
        expected_version: u64,
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
    },
    TurnCompleted {
        expected_version: u64,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
        outcome: RuntimeOutcome,
    },
    TurnFailed {
        expected_version: u64,
        task_ordinal: usize,
        role: WorkerRole,
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
    RepositoryObserved {
        expected_version: u64,
        task_ordinal: usize,
        checkpoint: RepositoryCheckpoint,
        observation: RepositoryObservation,
    },
    Timeout {
        expected_version: u64,
        task_ordinal: usize,
        role: WorkerRole,
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
            Self::TaskRuntimeOpened { .. } => SupervisorEventKind::TaskRuntimeOpened,
            Self::RoleSessionOpened { .. } => SupervisorEventKind::RoleSessionOpened,
            Self::TurnStarted { .. } => SupervisorEventKind::TurnStarted,
            Self::TurnCompleted { .. } => SupervisorEventKind::TurnCompleted,
            Self::TurnFailed { .. } => SupervisorEventKind::TurnFailed,
            Self::DriverFailed { .. } => SupervisorEventKind::DriverFailed,
            Self::RepositoryObserved { .. } => SupervisorEventKind::RepositoryObserved,
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
            | Self::TurnFailed {
                expected_version, ..
            }
            | Self::DriverFailed {
                expected_version, ..
            }
            | Self::RepositoryObserved {
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
    OpenTaskRuntime {
        task_ordinal: usize,
    },
    OpenRoleSession {
        task_ordinal: usize,
        role: WorkerRole,
    },
    StartTurn {
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
    },
    ObserveRepository {
        task_ordinal: usize,
        checkpoint: RepositoryCheckpoint,
    },
    InterruptTurn {
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
    },
    CloseTaskRuntime {
        task_ordinal: usize,
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
    OpenTaskRuntime,
    OpenRoleSession,
    StartTurn,
    ObserveRepository,
    InterruptTurn,
    CloseTaskRuntime,
    FinishSession,
    PublishStatus,
}

#[cfg(test)]
impl SupervisorEffectKind {
    const ALL: [Self; 8] = [
        Self::OpenTaskRuntime,
        Self::OpenRoleSession,
        Self::StartTurn,
        Self::ObserveRepository,
        Self::InterruptTurn,
        Self::CloseTaskRuntime,
        Self::FinishSession,
        Self::PublishStatus,
    ];
}

#[cfg(test)]
impl SupervisorEffect {
    fn kind(&self) -> SupervisorEffectKind {
        match self {
            Self::OpenTaskRuntime { .. } => SupervisorEffectKind::OpenTaskRuntime,
            Self::OpenRoleSession { .. } => SupervisorEffectKind::OpenRoleSession,
            Self::StartTurn { .. } => SupervisorEffectKind::StartTurn,
            Self::ObserveRepository { .. } => SupervisorEffectKind::ObserveRepository,
            Self::InterruptTurn { .. } => SupervisorEffectKind::InterruptTurn,
            Self::CloseTaskRuntime { .. } => SupervisorEffectKind::CloseTaskRuntime,
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
    pub branch: Option<String>,
    pub base_revision: Option<String>,
    pub head_revision: Option<String>,
    pub developer_session: Option<RuntimeSessionKey>,
    pub reviewer_session: Option<RuntimeSessionKey>,
    pub recovery_used: bool,
    pub outcome_detail: Option<String>,
    expected_repository: RepositoryObservation,
    reviewer_pre_repository: Option<RepositoryObservation>,
    last_developer_outcome: Option<DeveloperOutcomeV1>,
    last_reviewer_outcome: Option<ReviewerOutcomeV1>,
    recovery_checkpoint: Option<RepositoryObservation>,
}

impl CoreTask {
    fn new(spec: TaskDraft, binding: RepositoryObservation) -> Self {
        Self {
            branch: Some(binding.branch.clone()),
            base_revision: Some(binding.head.clone()),
            expected_repository: binding,
            spec,
            state: TaskState::Pending,
            review_round: 0,
            head_revision: None,
            developer_session: None,
            reviewer_session: None,
            recovery_used: false,
            outcome_detail: None,
            reviewer_pre_repository: None,
            last_developer_outcome: None,
            last_reviewer_outcome: None,
            recovery_checkpoint: None,
        }
    }

    pub fn expected_repository(&self) -> &RepositoryObservation {
        &self.expected_repository
    }

    pub fn last_developer_outcome(&self) -> Option<&DeveloperOutcomeV1> {
        self.last_developer_outcome.as_ref()
    }

    pub fn last_reviewer_outcome(&self) -> Option<&ReviewerOutcomeV1> {
        self.last_reviewer_outcome.as_ref()
    }

    pub fn recovery_checkpoint(&self) -> Option<&RepositoryObservation> {
        self.recovery_checkpoint.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedSessionOpen {
    task_ordinal: usize,
    role: WorkerRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedTurnStart {
    task_ordinal: usize,
    role: WorkerRole,
    purpose: RuntimeTurnPurpose,
    session: RuntimeSessionKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoreActiveTurn {
    task_ordinal: usize,
    role: WorkerRole,
    purpose: RuntimeTurnPurpose,
    session: RuntimeSessionKey,
    turn: RuntimeTurnKey,
    completion_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRepository {
    task_ordinal: usize,
    checkpoint: RepositoryCheckpoint,
    outcome: Option<RuntimeOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorCore {
    run_id: String,
    project_root: PathBuf,
    profile_hash: String,
    session_state: SessionState,
    version: u64,
    next_plan_version: u64,
    plan_version: Option<u64>,
    plan_hash: Option<String>,
    tasks: Vec<CoreTask>,
    current_task: Option<usize>,
    terminal_detail: Option<String>,
    pending_runtime_open: Option<usize>,
    runtime_open: Option<usize>,
    pending_session_open: Option<ExpectedSessionOpen>,
    pending_turn_start: Option<ExpectedTurnStart>,
    active_turn: Option<CoreActiveTurn>,
    pending_repository: Option<PendingRepository>,
    used_sessions: BTreeSet<RuntimeSessionKey>,
    used_turns: BTreeSet<RuntimeTurnKey>,
    accepted_completion_tokens: BTreeSet<String>,
}

impl SupervisorCore {
    pub fn new(
        run_id: String,
        project_root: PathBuf,
        profile_hash: String,
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
            session_state: SessionState::AwaitingPlan,
            version: 0,
            next_plan_version: 1,
            plan_version: None,
            plan_hash: None,
            tasks: Vec::new(),
            current_task: None,
            terminal_detail: None,
            pending_runtime_open: None,
            runtime_open: None,
            pending_session_open: None,
            pending_turn_start: None,
            active_turn: None,
            pending_repository: None,
            used_sessions: BTreeSet::new(),
            used_turns: BTreeSet::new(),
            accepted_completion_tokens: BTreeSet::new(),
        };
        core.assert_invariants()?;
        Ok(core)
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

    pub fn expected_plan_hash(
        &self,
        plan_version: u64,
        tasks: &[TaskDraft],
        repositories: &[TaskRepositoryBinding],
    ) -> String {
        canonical_hash(&(
            "hcom-codex-app-server-session-plan-v1",
            plan_version,
            &self.project_root,
            &self.profile_hash,
            tasks,
            repositories,
        ))
    }

    pub fn snapshot(&self) -> SessionStatusSnapshot {
        SessionStatusSnapshot {
            run_id: self.run_id.clone(),
            state: self.session_state,
            version: self.version,
            project_root: self.project_root.to_string_lossy().into_owned(),
            plan_version: self.plan_version,
            plan_hash: self.plan_hash.clone(),
            current_task_ordinal: self
                .current_task
                .and_then(|index| u32::try_from(index).ok()),
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
                    branch: task.branch.clone(),
                    review_round: task.review_round,
                    max_review_rounds: task.spec.max_review_rounds,
                    base_revision: task.base_revision.clone(),
                    head_revision: task.head_revision.clone(),
                    developer_session_bound: task.developer_session.is_some(),
                    reviewer_session_bound: task.reviewer_session.is_some(),
                    outcome_detail: task.outcome_detail.clone(),
                })
                .collect(),
        }
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
                repositories,
                ..
            } => self.bind_plan(plan_version, plan_hash, tasks, repositories),
            SupervisorEvent::ExecutionAuthorized {
                plan_version,
                plan_hash,
                ..
            } => self.authorize(plan_version, plan_hash),
            SupervisorEvent::TaskRuntimeOpened { task_ordinal, .. } => {
                self.task_runtime_opened(task_ordinal)
            }
            SupervisorEvent::RoleSessionOpened {
                task_ordinal,
                role,
                session,
                ..
            } => self.role_session_opened(task_ordinal, role, session),
            SupervisorEvent::TurnStarted {
                task_ordinal,
                role,
                purpose,
                session,
                turn,
                completion_token,
                ..
            } => self.turn_started(task_ordinal, role, purpose, session, turn, completion_token),
            SupervisorEvent::TurnCompleted {
                task_ordinal,
                role,
                session,
                turn,
                completion_token,
                outcome,
                ..
            } => self.turn_completed(
                task_ordinal,
                role,
                session,
                turn,
                &completion_token,
                outcome,
            ),
            SupervisorEvent::TurnFailed {
                task_ordinal,
                role,
                session,
                turn,
                completion_token,
                failure,
                ..
            } => self.turn_failed(
                task_ordinal,
                role,
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
            SupervisorEvent::RepositoryObserved {
                task_ordinal,
                checkpoint,
                observation,
                ..
            } => self.repository_observed(task_ordinal, checkpoint, observation),
            SupervisorEvent::Timeout {
                task_ordinal,
                role,
                session,
                turn,
                completion_token,
                ..
            } => self.timeout(task_ordinal, role, session, turn, &completion_token),
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
        repositories: Vec<TaskRepositoryBinding>,
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
        if repositories.len() != tasks.len() {
            return Err(SupervisorError::invalid_plan(
                "plan repository bindings must match the ordered task count",
            ));
        }

        let mut keys = BTreeSet::new();
        let mut roots: BTreeMap<&str, &RepositoryObservation> = BTreeMap::new();
        for (task, binding) in tasks.iter().zip(&repositories) {
            task.validate()
                .map_err(|error| SupervisorError::invalid_plan(error.to_string()))?;
            if !keys.insert(task.task_key.as_str()) {
                return Err(SupervisorError::invalid_plan(
                    "ordered plan task keys must be unique",
                ));
            }
            if binding.task_key != task.task_key
                || binding.observation.repository_root != task.repository_root
            {
                return Err(SupervisorError::invalid_plan(
                    "task repository binding identity or order does not match the plan",
                ));
            }
            binding.observation.validate()?;
            if !binding.observation.clean || !binding.observation.changed_paths.is_empty() {
                return Err(SupervisorError::invalid_plan(
                    "task repository must be clean when the plan is bound",
                ));
            }
            if let Some(previous) = roots.insert(
                binding.observation.repository_root.as_str(),
                &binding.observation,
            ) && !previous.same_git_state(&binding.observation)
            {
                return Err(SupervisorError::invalid_plan(
                    "same-repository task bindings must describe one exact Git state",
                ));
            }
        }

        let expected_hash = self.expected_plan_hash(plan_version, &tasks, &repositories);
        if plan_hash != expected_hash {
            return Err(SupervisorError::invalid_plan(
                "plan hash does not match the exact ordered plan binding",
            ));
        }
        let next_plan_version = plan_version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("plan version overflow"))?;

        self.tasks = tasks
            .into_iter()
            .zip(repositories)
            .map(|(task, binding)| CoreTask::new(task, binding.observation))
            .collect();
        self.plan_version = Some(plan_version);
        self.plan_hash = Some(plan_hash);
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
        self.schedule_repository(0, RepositoryCheckpoint::TaskStart, None)
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
        self.schedule_session_open(task_ordinal, WorkerRole::Developer)
    }

    fn role_session_opened(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let expected = self.pending_session_open.as_ref().ok_or_else(|| {
            SupervisorError::invalid_transition("role session opened without a pending effect")
        })?;
        if expected.task_ordinal != task_ordinal || expected.role != role {
            return Err(SupervisorError::invalid_identity(
                "role session open does not match the expected task and role",
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
        let slot = match role {
            WorkerRole::Developer => &mut self.tasks[task_ordinal].developer_session,
            WorkerRole::Reviewer => &mut self.tasks[task_ordinal].reviewer_session,
        };
        if slot.is_some() {
            return Err(SupervisorError::invalid_transition(
                "role already owns a logical runtime session",
            ));
        }
        *slot = Some(session);
        self.used_sessions.insert(session);
        self.pending_session_open = None;
        let purpose = match role {
            WorkerRole::Developer => RuntimeTurnPurpose::InitialDevelopment,
            WorkerRole::Reviewer if self.tasks[task_ordinal].review_round == 0 => {
                RuntimeTurnPurpose::InitialReview
            }
            WorkerRole::Reviewer => RuntimeTurnPurpose::ReviewerRereview,
        };
        self.schedule_turn(task_ordinal, role, purpose, session)
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_started(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
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
        let expected = self.pending_turn_start.as_ref().ok_or_else(|| {
            SupervisorError::invalid_transition("turn started without a pending start effect")
        })?;
        if expected.task_ordinal != task_ordinal
            || expected.role != role
            || expected.purpose != purpose
            || expected.session != session
        {
            return Err(SupervisorError::invalid_identity(
                "turn start does not match its exact task, role, purpose, and session",
            ));
        }
        if purpose.role() != role || self.session_for(task_ordinal, role) != Some(session) {
            return Err(SupervisorError::invalid_identity(
                "turn purpose or logical session does not match the role",
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
        if self.active_turn.is_some() {
            return Err(SupervisorError::invalid_transition(
                "a second active turn is forbidden",
            ));
        }
        self.pending_turn_start = None;
        self.used_turns.insert(turn);
        self.active_turn = Some(CoreActiveTurn {
            task_ordinal,
            role,
            purpose,
            session,
            turn,
            completion_token,
        });
        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_completed(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
        outcome: RuntimeOutcome,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let active =
            self.take_matching_active(task_ordinal, role, session, turn, completion_token)?;
        outcome
            .validate()
            .map_err(|_| SupervisorError::invalid_event("typed runtime outcome is invalid"))?;
        if outcome.role() != role {
            return Err(SupervisorError::invalid_event(
                "typed runtime outcome role does not match the active turn",
            ));
        }
        self.accepted_completion_tokens
            .insert(active.completion_token);

        match outcome {
            RuntimeOutcome::Developer(developer) => match developer.status {
                DeveloperOutcomeStatus::Ready => self.schedule_repository(
                    task_ordinal,
                    RepositoryCheckpoint::DeveloperCompletion,
                    Some(RuntimeOutcome::Developer(developer)),
                ),
                DeveloperOutcomeStatus::NeedsHuman => {
                    self.tasks[task_ordinal].last_developer_outcome = Some(developer);
                    self.terminalize_current(
                        SessionState::NeedsHuman,
                        TaskState::NeedsHuman,
                        "developer requested human input",
                        Vec::new(),
                    )
                }
                DeveloperOutcomeStatus::Blocked => {
                    self.tasks[task_ordinal].last_developer_outcome = Some(developer);
                    self.terminalize_current(
                        SessionState::NeedsHuman,
                        TaskState::NeedsHuman,
                        "developer reported an unrecoverable block",
                        Vec::new(),
                    )
                }
            },
            RuntimeOutcome::Reviewer(reviewer) => self.schedule_repository(
                task_ordinal,
                RepositoryCheckpoint::ReviewerPostflight,
                Some(RuntimeOutcome::Reviewer(reviewer)),
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn turn_failed(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
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
        let active =
            self.take_matching_active(task_ordinal, role, session, turn, completion_token)?;
        self.accepted_completion_tokens
            .insert(active.completion_token);

        if role == WorkerRole::Developer
            && failure.class == RuntimeFailureClass::Contract
            && failure.retryable
        {
            if self.tasks[task_ordinal].recovery_used {
                return self.terminalize_current(
                    SessionState::NeedsHuman,
                    TaskState::NeedsHuman,
                    "developer completion result remained invalid after one recovery",
                    Vec::new(),
                );
            }
            return self.schedule_repository(
                task_ordinal,
                RepositoryCheckpoint::DeveloperRecoveryPreflight,
                None,
            );
        }

        let (session_state, task_state, terminal_detail) = match failure.class {
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
        self.terminalize_current(session_state, task_state, terminal_detail, Vec::new())
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

    fn repository_observed(
        &mut self,
        task_ordinal: usize,
        checkpoint: RepositoryCheckpoint,
        observation: RepositoryObservation,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        observation.validate()?;
        let pending = self.pending_repository.as_ref().ok_or_else(|| {
            SupervisorError::invalid_transition(
                "repository observation arrived without an outstanding checkpoint",
            )
        })?;
        if pending.task_ordinal != task_ordinal || pending.checkpoint != checkpoint {
            return Err(SupervisorError::invalid_identity(
                "repository observation does not match the exact pending checkpoint",
            ));
        }
        let pending = self
            .pending_repository
            .take()
            .expect("pending repository was just validated");
        match checkpoint {
            RepositoryCheckpoint::TaskStart => self.handle_task_start(task_ordinal, observation),
            RepositoryCheckpoint::DeveloperCompletion => {
                let Some(RuntimeOutcome::Developer(outcome)) = pending.outcome else {
                    return Err(SupervisorError::invariant(
                        "developer checkpoint lost its typed outcome",
                    ));
                };
                self.handle_developer_repository(task_ordinal, observation, outcome)
            }
            RepositoryCheckpoint::DeveloperRecoveryPreflight => {
                if pending.outcome.is_some() {
                    return Err(SupervisorError::invariant(
                        "developer recovery preflight unexpectedly carried an outcome",
                    ));
                }
                self.handle_developer_recovery_preflight(task_ordinal, observation)
            }
            RepositoryCheckpoint::ReviewerPreflight => {
                if pending.outcome.is_some() {
                    return Err(SupervisorError::invariant(
                        "reviewer preflight unexpectedly carried an outcome",
                    ));
                }
                self.handle_reviewer_preflight(task_ordinal, observation)
            }
            RepositoryCheckpoint::ReviewerPostflight => {
                let Some(RuntimeOutcome::Reviewer(outcome)) = pending.outcome else {
                    return Err(SupervisorError::invariant(
                        "reviewer postflight lost its typed outcome",
                    ));
                };
                self.handle_reviewer_postflight(task_ordinal, observation, outcome)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn timeout(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_running_task(task_ordinal)?;
        let active =
            self.take_matching_active(task_ordinal, role, session, turn, completion_token)?;
        self.accepted_completion_tokens
            .insert(active.completion_token);
        let interrupt = SupervisorEffect::InterruptTurn {
            task_ordinal,
            role,
            session,
            turn,
        };
        self.terminalize_current(
            SessionState::NeedsHuman,
            TaskState::NeedsHuman,
            "worker turn timed out",
            vec![interrupt],
        )
    }

    fn cancel(&mut self, reason: &str) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        validate_single_line("cancel reason", reason, 4096)?;
        let effects = self.interrupt_active_effect();
        self.terminalize_current(
            SessionState::Canceled,
            TaskState::Canceled,
            "canceled by explicit Architect-session request",
            effects,
        )
    }

    fn parent_stopping(&mut self) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let effects = self.interrupt_active_effect();
        self.terminalize_current(
            SessionState::Canceled,
            TaskState::Canceled,
            "foreground Architect parent stopped",
            effects,
        )
    }

    fn handle_task_start(
        &mut self,
        task_ordinal: usize,
        observation: RepositoryObservation,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let task = &mut self.tasks[task_ordinal];
        if task.state != TaskState::Pending {
            return Err(SupervisorError::invalid_transition(
                "task-start observation requires a pending task",
            ));
        }
        if !observation.clean
            || !observation.head_descends_from_expected
            || !observation.same_git_state(&task.expected_repository)
        {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "task repository drifted before task start",
                Vec::new(),
            );
        }
        task.branch = Some(observation.branch.clone());
        task.base_revision = Some(observation.head.clone());
        task.expected_repository = observation;
        self.pending_runtime_open = Some(task_ordinal);
        Ok(vec![SupervisorEffect::OpenTaskRuntime { task_ordinal }])
    }

    fn handle_developer_repository(
        &mut self,
        task_ordinal: usize,
        observation: RepositoryObservation,
        outcome: DeveloperOutcomeV1,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.tasks[task_ordinal].state != TaskState::Developing {
            return Err(SupervisorError::invalid_transition(
                "developer completion checkpoint requires a developing task",
            ));
        }
        let expected = self.tasks[task_ordinal].expected_repository.clone();
        if observation.repository_root != expected.repository_root
            || observation.identity_hash != expected.identity_hash
            || observation.branch != expected.branch
            || !observation.head_descends_from_expected
        {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "developer repository identity, branch, or history drifted",
                Vec::new(),
            );
        }
        if !self.changed_paths_allowed(task_ordinal, &observation.changed_paths) {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "developer changed paths outside the task allowlist",
                Vec::new(),
            );
        }

        let valid_completion = observation.clean
            && observation.head != expected.head
            && !observation.changed_paths.is_empty();
        if valid_completion {
            let task = &mut self.tasks[task_ordinal];
            task.head_revision = Some(observation.head.clone());
            task.expected_repository = observation;
            task.last_developer_outcome = Some(outcome);
            task.recovery_checkpoint = None;
            task.state = TaskState::Reviewing;
            task.outcome_detail = Some("developer produced a clean committed revision".into());
            return self.schedule_repository(
                task_ordinal,
                RepositoryCheckpoint::ReviewerPreflight,
                None,
            );
        }

        if self.tasks[task_ordinal].recovery_used {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "developer completion remained invalid after one recovery",
                Vec::new(),
            );
        }
        let session = self.tasks[task_ordinal]
            .developer_session
            .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
        let task = &mut self.tasks[task_ordinal];
        task.recovery_used = true;
        task.recovery_checkpoint = Some(observation);
        task.last_developer_outcome = Some(outcome);
        task.outcome_detail = Some("developer completion recovery in progress".into());
        self.schedule_turn(
            task_ordinal,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCompletionRecovery,
            session,
        )
    }

    fn handle_developer_recovery_preflight(
        &mut self,
        task_ordinal: usize,
        observation: RepositoryObservation,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.tasks[task_ordinal].state != TaskState::Developing {
            return Err(SupervisorError::invalid_transition(
                "developer recovery checkpoint requires a developing task",
            ));
        }
        let expected = self.tasks[task_ordinal].expected_repository.clone();
        if observation.repository_root != expected.repository_root
            || observation.identity_hash != expected.identity_hash
            || observation.branch != expected.branch
            || !observation.head_descends_from_expected
        {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "developer repository identity, branch, or history drifted",
                Vec::new(),
            );
        }
        if !self.changed_paths_allowed(task_ordinal, &observation.changed_paths) {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "developer changed paths outside the task allowlist",
                Vec::new(),
            );
        }
        if self.tasks[task_ordinal].recovery_used {
            return Err(SupervisorError::invariant(
                "developer recovery preflight arrived after the recovery budget was consumed",
            ));
        }
        let session = self.tasks[task_ordinal]
            .developer_session
            .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
        let task = &mut self.tasks[task_ordinal];
        task.recovery_used = true;
        task.recovery_checkpoint = Some(observation);
        task.outcome_detail = Some("developer completion recovery in progress".into());
        self.schedule_turn(
            task_ordinal,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCompletionRecovery,
            session,
        )
    }

    fn handle_reviewer_preflight(
        &mut self,
        task_ordinal: usize,
        observation: RepositoryObservation,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let task = &mut self.tasks[task_ordinal];
        if task.state != TaskState::Reviewing {
            return Err(SupervisorError::invalid_transition(
                "reviewer preflight requires a reviewing task",
            ));
        }
        if !observation.clean
            || !observation.head_descends_from_expected
            || !observation.same_git_state(&task.expected_repository)
        {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "repository drifted before Reviewer started",
                Vec::new(),
            );
        }
        task.reviewer_pre_repository = Some(observation);
        if let Some(session) = task.reviewer_session {
            let purpose = if task.review_round == 0 {
                RuntimeTurnPurpose::InitialReview
            } else {
                RuntimeTurnPurpose::ReviewerRereview
            };
            self.schedule_turn(task_ordinal, WorkerRole::Reviewer, purpose, session)
        } else {
            self.schedule_session_open(task_ordinal, WorkerRole::Reviewer)
        }
    }

    fn handle_reviewer_postflight(
        &mut self,
        task_ordinal: usize,
        observation: RepositoryObservation,
        outcome: ReviewerOutcomeV1,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.tasks[task_ordinal].state != TaskState::Reviewing {
            return Err(SupervisorError::invalid_transition(
                "reviewer postflight requires a reviewing task",
            ));
        }
        let before = self.tasks[task_ordinal]
            .reviewer_pre_repository
            .clone()
            .ok_or_else(|| SupervisorError::invariant("reviewer preflight state disappeared"))?;
        if !observation.clean || !observation.same_git_state(&before) {
            return self.terminalize_current(
                SessionState::NeedsHuman,
                TaskState::NeedsHuman,
                "Reviewer changed repository state; verdict rejected",
                Vec::new(),
            );
        }
        let next_round = self.tasks[task_ordinal]
            .review_round
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("review round overflow"))?;
        if next_round > u32::from(self.tasks[task_ordinal].spec.max_review_rounds) {
            return Err(SupervisorError::invariant(
                "review round exceeded the task maximum",
            ));
        }
        {
            let task = &mut self.tasks[task_ordinal];
            task.review_round = next_round;
            task.last_reviewer_outcome = Some(outcome.clone());
            task.reviewer_pre_repository = None;
        }

        match outcome.verdict {
            ReviewerVerdict::Lgtm => {
                let task = &mut self.tasks[task_ordinal];
                task.state = TaskState::Lgtm;
                task.outcome_detail = Some("Reviewer approved the exact clean revision".into());
                self.complete_current_task(task_ordinal)
            }
            ReviewerVerdict::RequestChanges
                if next_round >= u32::from(self.tasks[task_ordinal].spec.max_review_rounds) =>
            {
                let task = &mut self.tasks[task_ordinal];
                task.state = TaskState::ReviewExhausted;
                task.outcome_detail =
                    Some("maximum review rounds exhausted; advancing by policy".into());
                self.complete_current_task(task_ordinal)
            }
            ReviewerVerdict::RequestChanges => {
                let session = self.tasks[task_ordinal]
                    .developer_session
                    .ok_or_else(|| SupervisorError::invariant("developer session disappeared"))?;
                let task = &mut self.tasks[task_ordinal];
                task.state = TaskState::Developing;
                task.outcome_detail = Some("Reviewer requested changes".into());
                self.schedule_turn(
                    task_ordinal,
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    session,
                )
            }
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
        let repository = self.tasks[task_ordinal].expected_repository.clone();
        let repository_root = repository.repository_root.clone();
        for future in self.tasks.iter_mut().skip(task_ordinal + 1) {
            if future.spec.repository_root == repository_root {
                future.expected_repository = repository.clone();
                future.branch = Some(repository.branch.clone());
                future.base_revision = Some(repository.head.clone());
            }
        }
        self.runtime_open = None;
        self.pending_session_open = None;
        self.pending_turn_start = None;
        self.active_turn = None;
        self.pending_repository = None;

        let mut effects = vec![SupervisorEffect::CloseTaskRuntime { task_ordinal }];
        if task_ordinal + 1 < self.tasks.len() {
            let next = task_ordinal + 1;
            self.current_task = Some(next);
            effects.extend(self.schedule_repository(
                next,
                RepositoryCheckpoint::TaskStart,
                None,
            )?);
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

    fn schedule_repository(
        &mut self,
        task_ordinal: usize,
        checkpoint: RepositoryCheckpoint,
        outcome: Option<RuntimeOutcome>,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_no_pending_operation()?;
        self.pending_repository = Some(PendingRepository {
            task_ordinal,
            checkpoint,
            outcome,
        });
        Ok(vec![SupervisorEffect::ObserveRepository {
            task_ordinal,
            checkpoint,
        }])
    }

    fn schedule_session_open(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_no_pending_operation()?;
        self.pending_session_open = Some(ExpectedSessionOpen { task_ordinal, role });
        Ok(vec![SupervisorEffect::OpenRoleSession {
            task_ordinal,
            role,
        }])
    }

    fn schedule_turn(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        self.require_no_pending_operation()?;
        if purpose.role() != role {
            return Err(SupervisorError::invalid_event(
                "turn purpose does not match its role",
            ));
        }
        self.pending_turn_start = Some(ExpectedTurnStart {
            task_ordinal,
            role,
            purpose,
            session,
        });
        Ok(vec![SupervisorEffect::StartTurn {
            task_ordinal,
            role,
            purpose,
            session,
        }])
    }

    fn require_no_pending_operation(&self) -> Result<(), SupervisorError> {
        if self.pending_runtime_open.is_some()
            || self.pending_session_open.is_some()
            || self.pending_turn_start.is_some()
            || self.active_turn.is_some()
            || self.pending_repository.is_some()
        {
            return Err(SupervisorError::invariant(
                "cannot schedule two supervisor operations at once",
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

    fn session_for(&self, task_ordinal: usize, role: WorkerRole) -> Option<RuntimeSessionKey> {
        let task = self.tasks.get(task_ordinal)?;
        match role {
            WorkerRole::Developer => task.developer_session,
            WorkerRole::Reviewer => task.reviewer_session,
        }
    }

    fn take_matching_active(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: &str,
    ) -> Result<CoreActiveTurn, SupervisorError> {
        if self.accepted_completion_tokens.contains(completion_token) {
            return Err(SupervisorError::duplicate(
                "completion token was already accepted",
            ));
        }
        let active = self.active_turn.as_ref().ok_or_else(|| {
            SupervisorError::invalid_transition("turn completion arrived with no active turn")
        })?;
        if active.task_ordinal != task_ordinal
            || active.role != role
            || active.session != session
            || active.turn != turn
            || active.completion_token != completion_token
        {
            return Err(SupervisorError::invalid_identity(
                "turn completion identity does not match the active turn",
            ));
        }
        Ok(self
            .active_turn
            .take()
            .expect("active turn was just validated"))
    }

    fn changed_paths_allowed(&self, task_ordinal: usize, changed_paths: &[String]) -> bool {
        changed_paths.iter().all(|changed| {
            self.tasks[task_ordinal]
                .spec
                .allowed_paths
                .iter()
                .any(|allowed| {
                    changed == allowed
                        || changed
                            .strip_prefix(allowed)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                })
        })
    }

    fn interrupt_active_effect(&mut self) -> Vec<SupervisorEffect> {
        let Some(active) = self.active_turn.take() else {
            return Vec::new();
        };
        self.accepted_completion_tokens
            .insert(active.completion_token);
        vec![SupervisorEffect::InterruptTurn {
            task_ordinal: active.task_ordinal,
            role: active.role,
            session: active.session,
            turn: active.turn,
        }]
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
        self.pending_session_open = None;
        self.pending_turn_start = None;
        self.active_turn = None;
        self.pending_repository = None;
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
        self.pending_session_open = None;
        self.pending_turn_start = None;
        self.active_turn = None;
        self.pending_repository = None;
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
                || self.pending_session_open.is_some()
                || self.pending_turn_start.is_some()
                || self.active_turn.is_some()
                || self.pending_repository.is_some())
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
        let scheduled_operations = [
            self.pending_runtime_open.is_some(),
            self.pending_session_open.is_some(),
            self.pending_turn_start.is_some(),
            self.active_turn.is_some(),
            self.pending_repository.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if scheduled_operations > 1 {
            return Err(SupervisorError::invariant(
                "more than one supervisor operation is active",
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
                    !matches!(task.state, TaskState::Developing | TaskState::Reviewing)
                }))
        {
            return Err(SupervisorError::invariant(
                "open runtime is not bound to the current active task",
            ));
        }
        if let Some(expected) = &self.pending_session_open {
            let expected_state = match expected.role {
                WorkerRole::Developer => TaskState::Developing,
                WorkerRole::Reviewer => TaskState::Reviewing,
            };
            if self.runtime_open != Some(expected.task_ordinal)
                || self
                    .tasks
                    .get(expected.task_ordinal)
                    .is_none_or(|task| task.state != expected_state)
                || self
                    .session_for(expected.task_ordinal, expected.role)
                    .is_some()
            {
                return Err(SupervisorError::invariant(
                    "pending role-session open is not bound to an unbound current role",
                ));
            }
        }
        if let Some(expected) = &self.pending_turn_start {
            let expected_state = match expected.role {
                WorkerRole::Developer => TaskState::Developing,
                WorkerRole::Reviewer => TaskState::Reviewing,
            };
            if self.runtime_open != Some(expected.task_ordinal)
                || expected.purpose.role() != expected.role
                || self.session_for(expected.task_ordinal, expected.role) != Some(expected.session)
                || self
                    .tasks
                    .get(expected.task_ordinal)
                    .is_none_or(|task| task.state != expected_state)
            {
                return Err(SupervisorError::invariant(
                    "pending turn is not bound to the exact current role session",
                ));
            }
        }
        for task in &self.tasks {
            if task.review_round > u32::from(task.spec.max_review_rounds) {
                return Err(SupervisorError::invariant(
                    "task review round exceeds its maximum",
                ));
            }
            if task.expected_repository.repository_root != task.spec.repository_root
                || task.branch.as_deref() != Some(task.expected_repository.branch.as_str())
            {
                return Err(SupervisorError::invariant(
                    "task repository binding disagrees with its normalized evidence",
                ));
            }
            if task.state == TaskState::Reviewing
                && (task.head_revision.is_none()
                    || !task.expected_repository.clean
                    || task.developer_session.is_none()
                    || task.review_round >= u32::from(task.spec.max_review_rounds))
            {
                return Err(SupervisorError::invariant(
                    "reviewing task lacks a clean committed Developer handoff",
                ));
            }
            if task.reviewer_session.is_some() && task.developer_session.is_none() {
                return Err(SupervisorError::invariant(
                    "Reviewer session exists without the task Developer session",
                ));
            }
            if matches!(task.state, TaskState::Lgtm | TaskState::ReviewExhausted)
                && task.review_round == 0
            {
                return Err(SupervisorError::invariant(
                    "terminal review outcome has no accepted review round",
                ));
            }
            if task.state == TaskState::ReviewExhausted
                && task.review_round != u32::from(task.spec.max_review_rounds)
            {
                return Err(SupervisorError::invariant(
                    "review-exhausted task did not reach its exact maximum",
                ));
            }
            if let Some(session) = task.developer_session
                && !self.used_sessions.contains(&session)
            {
                return Err(SupervisorError::invariant(
                    "Developer session is absent from the global identity set",
                ));
            }
            if let Some(session) = task.reviewer_session
                && !self.used_sessions.contains(&session)
            {
                return Err(SupervisorError::invariant(
                    "Reviewer session is absent from the global identity set",
                ));
            }
        }
        let session_count = self
            .tasks
            .iter()
            .map(|task| {
                usize::from(task.developer_session.is_some())
                    + usize::from(task.reviewer_session.is_some())
            })
            .sum::<usize>();
        if session_count != self.used_sessions.len() {
            return Err(SupervisorError::invariant(
                "logical runtime session key was reused across roles or tasks",
            ));
        }
        if let Some(active) = &self.active_turn {
            if self.current_task != Some(active.task_ordinal)
                || self.runtime_open != Some(active.task_ordinal)
                || self.session_for(active.task_ordinal, active.role) != Some(active.session)
                || active.purpose.role() != active.role
            {
                return Err(SupervisorError::invariant(
                    "active turn is not bound to the exact current role session",
                ));
            }
            let expected_state = match active.role {
                WorkerRole::Developer => TaskState::Developing,
                WorkerRole::Reviewer => TaskState::Reviewing,
            };
            if self.tasks[active.task_ordinal].state != expected_state {
                return Err(SupervisorError::invariant(
                    "active turn role does not match the task state",
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
        if let Some(pending) = &self.pending_repository {
            if self.current_task != Some(pending.task_ordinal) {
                return Err(SupervisorError::invariant(
                    "repository checkpoint does not belong to the current task",
                ));
            }
            let task = &self.tasks[pending.task_ordinal];
            let valid = match (&pending.checkpoint, &pending.outcome) {
                (RepositoryCheckpoint::TaskStart, None) => {
                    task.state == TaskState::Pending && self.runtime_open.is_none()
                }
                (RepositoryCheckpoint::DeveloperCompletion, Some(RuntimeOutcome::Developer(_))) => {
                    task.state == TaskState::Developing
                        && self.runtime_open == Some(pending.task_ordinal)
                }
                (RepositoryCheckpoint::DeveloperRecoveryPreflight, None) => {
                    task.state == TaskState::Developing
                        && !task.recovery_used
                        && self.runtime_open == Some(pending.task_ordinal)
                }
                (RepositoryCheckpoint::ReviewerPreflight, None) => {
                    task.state == TaskState::Reviewing
                        && self.runtime_open == Some(pending.task_ordinal)
                }
                (RepositoryCheckpoint::ReviewerPostflight, Some(RuntimeOutcome::Reviewer(_))) => {
                    task.state == TaskState::Reviewing
                        && self.runtime_open == Some(pending.task_ordinal)
                }
                _ => false,
            };
            if !valid {
                return Err(SupervisorError::invariant(
                    "repository checkpoint type does not match task state and outcome",
                ));
            }
        }
        Ok(())
    }
}

fn validate_identifier(label: &str, value: &str) -> Result<(), SupervisorError> {
    validate_identifier_with_bound(label, value, 128)
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

fn validate_revision(label: &str, value: &str) -> Result<(), SupervisorError> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SupervisorError::invalid_repository(format!(
            "{label} must be a full lowercase Git object ID"
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

fn validate_relative_path(value: &str) -> Result<(), SupervisorError> {
    if value.is_empty() || value.len() > MAX_REPOSITORY_PATH_BYTES || value.contains('\\') {
        return Err(SupervisorError::invalid_repository(
            "repository changed path is empty, unbounded, or non-portable",
        ));
    }
    let path = Path::new(value);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(SupervisorError::invalid_repository(
            "repository changed path must be normalized and relative",
        ));
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

    const PROFILE_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CLEAN_HASH: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn revision(character: char) -> String {
        character.to_string().repeat(40)
    }

    fn task(key: &str, root: &str, max_review_rounds: u8) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: format!("Task {key}"),
            objective: format!("Implement {key}"),
            repository_root: root.into(),
            acceptance_criteria: vec!["the requested behavior is complete".into()],
            required_checks: vec!["cargo test".into()],
            allowed_paths: vec!["src".into(), "tests".into()],
            forbidden_actions: vec!["do not push".into()],
            max_review_rounds,
        }
    }

    fn observation(root: &str, head: char) -> RepositoryObservation {
        RepositoryObservation {
            repository_root: root.into(),
            identity_hash: "1".repeat(64),
            branch: "master".into(),
            head: revision(head),
            tracked_diff_hash: CLEAN_HASH.into(),
            index_diff_hash: CLEAN_HASH.into(),
            untracked_status_hash: CLEAN_HASH.into(),
            clean: true,
            changed_paths: Vec::new(),
            head_descends_from_expected: true,
        }
    }

    fn developer_observation(
        prior: &RepositoryObservation,
        head: char,
        clean: bool,
        paths: &[&str],
    ) -> RepositoryObservation {
        let mut observation = prior.clone();
        observation.head = revision(head);
        observation.clean = clean;
        observation.changed_paths = paths.iter().map(|path| (*path).into()).collect();
        observation.tracked_diff_hash = head.to_string().repeat(64);
        observation.index_diff_hash = if clean {
            CLEAN_HASH.into()
        } else {
            "c".repeat(64)
        };
        observation
    }

    fn ready() -> RuntimeOutcome {
        RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Ready,
            summary: "implementation complete".into(),
            questions: Vec::new(),
        })
    }

    fn needs_human() -> RuntimeOutcome {
        RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::NeedsHuman,
            summary: "a decision is required".into(),
            questions: vec!["Which behavior should be selected?".into()],
        })
    }

    fn blocked() -> RuntimeOutcome {
        RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Blocked,
            summary: "the required tool is unavailable".into(),
            questions: Vec::new(),
        })
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
            summary: "sound".into(),
            findings: Vec::new(),
        })
    }

    fn request_changes() -> RuntimeOutcome {
        RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
            verdict: ReviewerVerdict::RequestChanges,
            summary: "changes required".into(),
            findings: vec![crate::worker::runtime::ReviewFindingV1 {
                severity: crate::worker::runtime::ReviewFindingSeverity::Major,
                path: Some("src/lib.rs".into()),
                line: Some(1),
                message: "fix the boundary".into(),
            }],
        })
    }

    fn new_core() -> SupervisorCore {
        SupervisorCore::new(
            "run-1".into(),
            PathBuf::from("/project"),
            PROFILE_HASH.into(),
        )
        .unwrap()
    }

    fn bind(
        core: &mut SupervisorCore,
        tasks: Vec<TaskDraft>,
        observations: Vec<RepositoryObservation>,
    ) -> Vec<SupervisorEffect> {
        let plan_version = core.next_plan_version;
        let repositories = tasks
            .iter()
            .zip(observations)
            .map(|(task, observation)| TaskRepositoryBinding {
                task_key: task.task_key.clone(),
                observation,
            })
            .collect::<Vec<_>>();
        let plan_hash = core.expected_plan_hash(plan_version, &tasks, &repositories);
        core.reduce(SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
            repositories,
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

    fn observe(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        checkpoint: RepositoryCheckpoint,
        observation: RepositoryObservation,
    ) -> Vec<SupervisorEffect> {
        core.reduce(SupervisorEvent::RepositoryObserved {
            expected_version: core.version(),
            task_ordinal,
            checkpoint,
            observation,
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
        core.reduce(SupervisorEvent::RoleSessionOpened {
            expected_version: core.version(),
            task_ordinal,
            role,
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
        core.reduce(SupervisorEvent::TurnStarted {
            expected_version: core.version(),
            task_ordinal,
            role,
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
        core.reduce(SupervisorEvent::TurnCompleted {
            expected_version: core.version(),
            task_ordinal,
            role,
            session,
            turn,
            completion_token: completion_token.into(),
            outcome,
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
            role: active.role,
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
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        token: &'static str,
    }

    fn start_first_developer(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        start: RepositoryObservation,
        session_counter: u64,
        turn_counter: u64,
        token: &'static str,
    ) -> ActiveIdentity {
        assert_eq!(
            observe(core, task_ordinal, RepositoryCheckpoint::TaskStart, start),
            vec![
                SupervisorEffect::OpenTaskRuntime { task_ordinal },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(
            open_runtime(core, task_ordinal),
            vec![
                SupervisorEffect::OpenRoleSession {
                    task_ordinal,
                    role: WorkerRole::Developer,
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
                    role: WorkerRole::Developer,
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
            session,
            turn,
            token,
        }
    }

    fn complete_developer_ready(
        core: &mut SupervisorCore,
        active: ActiveIdentity,
        repository: RepositoryObservation,
    ) {
        assert_eq!(
            complete_turn(
                core,
                active.task,
                active.role,
                active.session,
                active.turn,
                active.token,
                ready(),
            ),
            vec![
                SupervisorEffect::ObserveRepository {
                    task_ordinal: active.task,
                    checkpoint: RepositoryCheckpoint::DeveloperCompletion,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(
            observe(
                core,
                active.task,
                RepositoryCheckpoint::DeveloperCompletion,
                repository
            ),
            vec![
                SupervisorEffect::ObserveRepository {
                    task_ordinal: active.task,
                    checkpoint: RepositoryCheckpoint::ReviewerPreflight,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
    }

    fn start_reviewer(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        repository: RepositoryObservation,
        session_counter: u64,
        turn_counter: u64,
        token: &'static str,
        first: bool,
    ) -> ActiveIdentity {
        let session = if first {
            assert_eq!(
                observe(
                    core,
                    task_ordinal,
                    RepositoryCheckpoint::ReviewerPreflight,
                    repository
                ),
                vec![
                    SupervisorEffect::OpenRoleSession {
                        task_ordinal,
                        role: WorkerRole::Reviewer,
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            let session = RuntimeSessionKey::from_counter(session_counter).unwrap();
            assert_eq!(
                open_session(core, task_ordinal, WorkerRole::Reviewer, session),
                vec![
                    SupervisorEffect::StartTurn {
                        task_ordinal,
                        role: WorkerRole::Reviewer,
                        purpose: RuntimeTurnPurpose::InitialReview,
                        session,
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            session
        } else {
            let session = core.tasks[task_ordinal].reviewer_session.unwrap();
            assert_eq!(
                observe(
                    core,
                    task_ordinal,
                    RepositoryCheckpoint::ReviewerPreflight,
                    repository
                ),
                vec![
                    SupervisorEffect::StartTurn {
                        task_ordinal,
                        role: WorkerRole::Reviewer,
                        purpose: RuntimeTurnPurpose::ReviewerRereview,
                        session,
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            session
        };
        let purpose = if first {
            RuntimeTurnPurpose::InitialReview
        } else {
            RuntimeTurnPurpose::ReviewerRereview
        };
        let turn = RuntimeTurnKey::from_counter(turn_counter).unwrap();
        assert_eq!(
            start_turn(
                core,
                task_ordinal,
                WorkerRole::Reviewer,
                purpose,
                session,
                turn,
                token,
            ),
            vec![SupervisorEffect::PublishStatus]
        );
        ActiveIdentity {
            task: task_ordinal,
            role: WorkerRole::Reviewer,
            session,
            turn,
            token,
        }
    }

    fn complete_review(
        core: &mut SupervisorCore,
        active: ActiveIdentity,
        outcome: RuntimeOutcome,
        repository: RepositoryObservation,
    ) -> Vec<SupervisorEffect> {
        assert_eq!(
            complete_turn(
                core,
                active.task,
                active.role,
                active.session,
                active.turn,
                active.token,
                outcome,
            ),
            vec![
                SupervisorEffect::ObserveRepository {
                    task_ordinal: active.task,
                    checkpoint: RepositoryCheckpoint::ReviewerPostflight,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        observe(
            core,
            active.task,
            RepositoryCheckpoint::ReviewerPostflight,
            repository,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn correct_and_start_rereview(
        core: &mut SupervisorCore,
        task_ordinal: usize,
        prior: &RepositoryObservation,
        next_head: char,
        developer_turn_counter: u64,
        reviewer_turn_counter: u64,
        developer_token: &'static str,
        reviewer_token: &'static str,
    ) -> (RepositoryObservation, ActiveIdentity) {
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
        let committed = developer_observation(prior, next_head, true, &["src/lib.rs"]);
        observe(
            core,
            task_ordinal,
            RepositoryCheckpoint::DeveloperCompletion,
            committed.clone(),
        );
        let reviewer_session_counter = core.tasks[task_ordinal].reviewer_session.unwrap().counter();
        let reviewer = start_reviewer(
            core,
            task_ordinal,
            committed.clone(),
            reviewer_session_counter,
            reviewer_turn_counter,
            reviewer_token,
            false,
        );
        (committed, reviewer)
    }

    fn bound_core() -> (SupervisorCore, RepositoryObservation) {
        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(&mut core, vec![task("one", "/repo", 3)], vec![base.clone()]);
        (core, base)
    }

    fn authorized_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, base) = bound_core();
        authorize(&mut core);
        (core, base)
    }

    fn pending_runtime_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, base) = authorized_core();
        observe(&mut core, 0, RepositoryCheckpoint::TaskStart, base.clone());
        (core, base)
    }

    fn pending_session_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, base) = pending_runtime_core();
        open_runtime(&mut core, 0);
        (core, base)
    }

    fn pending_turn_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, base) = pending_session_core();
        open_session(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeSessionKey::from_counter(1).unwrap(),
        );
        (core, base)
    }

    fn active_core() -> (SupervisorCore, RepositoryObservation, ActiveIdentity) {
        let (mut core, base) = authorized_core();
        let active = start_first_developer(&mut core, 0, base.clone(), 1, 1, "active");
        (core, base, active)
    }

    fn completed_core() -> SupervisorCore {
        let (mut core, base, developer) = active_core();
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "review", true);
        complete_review(&mut core, reviewer, lgtm(), committed);
        core
    }

    fn needs_human_core() -> SupervisorCore {
        let (mut core, _base, developer) = active_core();
        complete_turn(
            &mut core,
            developer.task,
            developer.role,
            developer.session,
            developer.turn,
            developer.token,
            needs_human(),
        );
        core
    }

    fn failed_core() -> SupervisorCore {
        let (mut core, _base, developer) = active_core();
        core.reduce(SupervisorEvent::TurnFailed {
            expected_version: core.version(),
            task_ordinal: 0,
            role: WorkerRole::Developer,
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

    fn active_reviewer_core(
        max_review_rounds: u8,
    ) -> (SupervisorCore, RepositoryObservation, ActiveIdentity) {
        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(
            &mut core,
            vec![task("one", "/repo", max_review_rounds)],
            vec![base.clone()],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "developer");
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "reviewer", true);
        (core, committed, reviewer)
    }

    fn pending_developer_repository_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, base, developer) = active_core();
        complete_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        (
            core,
            developer_observation(&base, '2', true, &["src/lib.rs"]),
        )
    }

    fn reviewing_preflight_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, committed) = pending_developer_repository_core();
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            committed.clone(),
        );
        (core, committed)
    }

    fn reviewing_pending_session_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, committed) = reviewing_preflight_core();
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::ReviewerPreflight,
            committed.clone(),
        );
        (core, committed)
    }

    fn reviewing_pending_turn_core() -> (SupervisorCore, RepositoryObservation) {
        let (mut core, committed) = reviewing_pending_session_core();
        open_session(
            &mut core,
            0,
            WorkerRole::Reviewer,
            RuntimeSessionKey::from_counter(2).unwrap(),
        );
        (core, committed)
    }

    fn review_exhausted_core() -> SupervisorCore {
        let (mut core, committed, reviewer) = active_reviewer_core(1);
        complete_review(&mut core, reviewer, request_changes(), committed);
        core
    }

    fn active_canceled_core() -> SupervisorCore {
        let (mut core, _base, _active) = active_core();
        core.reduce(SupervisorEvent::CancelRequested {
            expected_version: core.version(),
            reason: "stop active task".into(),
        })
        .unwrap();
        core
    }

    fn plan_event(core: &SupervisorCore, key: &str) -> SupervisorEvent {
        let plan_version = core.next_plan_version;
        let task = task(key, "/replacement", 2);
        let binding = TaskRepositoryBinding {
            task_key: key.into(),
            observation: observation("/replacement", '5'),
        };
        let tasks = vec![task];
        let repositories = vec![binding];
        let plan_hash = core.expected_plan_hash(plan_version, &tasks, &repositories);
        SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
            repositories,
        }
    }

    fn plan_event_for(
        core: &SupervisorCore,
        tasks: Vec<TaskDraft>,
        repositories: Vec<TaskRepositoryBinding>,
    ) -> SupervisorEvent {
        let plan_version = core.next_plan_version;
        let plan_hash = core.expected_plan_hash(plan_version, &tasks, &repositories);
        SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
            repositories,
        }
    }

    fn bindings_for(tasks: &[TaskDraft]) -> Vec<TaskRepositoryBinding> {
        tasks
            .iter()
            .map(|task| TaskRepositoryBinding {
                task_key: task.task_key.clone(),
                observation: observation(&task.repository_root, '1'),
            })
            .collect()
    }

    fn plan_result(tasks: Vec<TaskDraft>) -> Result<SupervisorCore, SupervisorError> {
        let mut core = new_core();
        let repositories = bindings_for(&tasks);
        let event = plan_event_for(&core, tasks, repositories);
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
            SupervisorEventKind::TaskRuntimeOpened => SupervisorEvent::TaskRuntimeOpened {
                expected_version: core.version(),
                task_ordinal: 0,
            },
            SupervisorEventKind::RoleSessionOpened => SupervisorEvent::RoleSessionOpened {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
            },
            SupervisorEventKind::TurnStarted => SupervisorEvent::TurnStarted {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                purpose: RuntimeTurnPurpose::InitialDevelopment,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "generic".into(),
            },
            SupervisorEventKind::TurnCompleted => SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "active".into(),
                outcome: ready(),
            },
            SupervisorEventKind::TurnFailed => SupervisorEvent::TurnFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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
            SupervisorEventKind::RepositoryObserved => SupervisorEvent::RepositoryObserved {
                expected_version: core.version(),
                task_ordinal: 0,
                checkpoint: RepositoryCheckpoint::TaskStart,
                observation: observation("/repo", '1'),
            },
            SupervisorEventKind::Timeout => SupervisorEvent::Timeout {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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
                let (core, _) = pending_runtime_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::RoleSessionOpened => {
                let (core, _) = pending_session_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::TurnStarted => {
                let (core, _) = pending_turn_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::TurnCompleted
            | SupervisorEventKind::TurnFailed
            | SupervisorEventKind::Timeout => {
                let (core, _, _) = active_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            SupervisorEventKind::RepositoryObserved => {
                let (core, _) = authorized_core();
                let event = generic_event(&core, kind);
                (core, event)
            }
            _ => {
                let (core, _) = authorized_core();
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
            SupervisorEventKind::RepositoryObserved,
            SupervisorEventKind::Timeout,
        ];
        assert!(relevant.contains(&kind));

        match state {
            TaskState::Pending => {
                let (core, _) = if kind == SupervisorEventKind::TaskRuntimeOpened {
                    pending_runtime_core()
                } else {
                    authorized_core()
                };
                let event = generic_event(&core, kind);
                let accepted = matches!(
                    kind,
                    SupervisorEventKind::TaskRuntimeOpened
                        | SupervisorEventKind::RepositoryObserved
                );
                (core, event, accepted)
            }
            TaskState::Developing => {
                let (core, event) = match kind {
                    SupervisorEventKind::RoleSessionOpened => {
                        let (core, _) = pending_session_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::TurnStarted => {
                        let (core, _) = pending_turn_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::TurnCompleted
                    | SupervisorEventKind::TurnFailed
                    | SupervisorEventKind::Timeout => {
                        let (core, _, _) = active_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    SupervisorEventKind::RepositoryObserved => {
                        let (core, committed) = pending_developer_repository_core();
                        let event = SupervisorEvent::RepositoryObserved {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            checkpoint: RepositoryCheckpoint::DeveloperCompletion,
                            observation: committed,
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TaskRuntimeOpened => {
                        let (core, _, _) = active_core();
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    _ => unreachable!(),
                };
                (core, event, kind != SupervisorEventKind::TaskRuntimeOpened)
            }
            TaskState::Reviewing => {
                let (core, event) = match kind {
                    SupervisorEventKind::RoleSessionOpened => {
                        let (core, _) = reviewing_pending_session_core();
                        let event = SupervisorEvent::RoleSessionOpened {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            role: WorkerRole::Reviewer,
                            session: RuntimeSessionKey::from_counter(2).unwrap(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TurnStarted => {
                        let (core, _) = reviewing_pending_turn_core();
                        let event = SupervisorEvent::TurnStarted {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            role: WorkerRole::Reviewer,
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
                        let (core, _, _) = active_reviewer_core(2);
                        let event = match kind {
                            SupervisorEventKind::TurnCompleted => SupervisorEvent::TurnCompleted {
                                expected_version: core.version(),
                                task_ordinal: 0,
                                role: WorkerRole::Reviewer,
                                session: RuntimeSessionKey::from_counter(2).unwrap(),
                                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                                completion_token: "reviewer".into(),
                                outcome: lgtm(),
                            },
                            SupervisorEventKind::TurnFailed => SupervisorEvent::TurnFailed {
                                expected_version: core.version(),
                                task_ordinal: 0,
                                role: WorkerRole::Reviewer,
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
                                role: WorkerRole::Reviewer,
                                session: RuntimeSessionKey::from_counter(2).unwrap(),
                                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                                completion_token: "reviewer".into(),
                            },
                            _ => unreachable!(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::RepositoryObserved => {
                        let (core, committed) = reviewing_preflight_core();
                        let event = SupervisorEvent::RepositoryObserved {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            checkpoint: RepositoryCheckpoint::ReviewerPreflight,
                            observation: committed,
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TaskRuntimeOpened => {
                        let (core, _, _) = active_reviewer_core(2);
                        let event = generic_event(&core, kind);
                        (core, event)
                    }
                    _ => unreachable!(),
                };
                (core, event, kind != SupervisorEventKind::TaskRuntimeOpened)
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
                        let (core, _) = bound_core();
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
        assert_eq!(accepted, 24);
        assert_eq!(rejected, 67);
    }

    #[test]
    fn every_effect_kind_has_a_real_core_production_path() {
        let mut observed = BTreeSet::new();
        let mut record = |effects: Vec<SupervisorEffect>| {
            observed.extend(effects.iter().map(SupervisorEffect::kind));
        };

        let (mut core, base) = bound_core();
        record(authorize(&mut core));
        record(observe(&mut core, 0, RepositoryCheckpoint::TaskStart, base));
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
                role: WorkerRole::Developer,
                session,
                turn,
                completion_token: "effect-inventory".into(),
            })
            .unwrap(),
        );

        assert_eq!(
            observed,
            BTreeSet::from(SupervisorEffectKind::ALL),
            "every effect variant must have an exercised production path"
        );
    }

    #[test]
    fn task_transition_inventory_covers_every_state_and_rejects_terminal_lifecycle() {
        let mut observed_states = BTreeSet::new();
        let mut edges = BTreeSet::new();
        let name = |state: TaskState| match state {
            TaskState::Pending => "pending",
            TaskState::Developing => "developing",
            TaskState::Reviewing => "reviewing",
            TaskState::Lgtm => "lgtm",
            TaskState::ReviewExhausted => "review_exhausted",
            TaskState::NeedsHuman => "needs_human",
            TaskState::Failed => "failed",
            TaskState::Canceled => "canceled",
        };

        let (mut pending, _) = pending_runtime_core();
        observed_states.insert(name(pending.tasks[0].state));
        let before = pending.tasks[0].state;
        open_runtime(&mut pending, 0);
        let after = pending.tasks[0].state;
        edges.insert((name(before), "runtime_opened", name(after)));

        let (mut developing, base, active) = active_core();
        observed_states.insert(name(developing.tasks[0].state));
        complete_turn(
            &mut developing,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        let dirty = developer_observation(&base, '1', false, &["src/lib.rs"]);
        let before = developing.tasks[0].state;
        observe(
            &mut developing,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            dirty,
        );
        edges.insert((
            name(before),
            "safe_completion_recovery",
            name(developing.tasks[0].state),
        ));

        let (mut developing, base, active) = active_core();
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_turn(
            &mut developing,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        let before = developing.tasks[0].state;
        observe(
            &mut developing,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            committed,
        );
        edges.insert((
            name(before),
            "clean_commit",
            name(developing.tasks[0].state),
        ));
        observed_states.insert(name(developing.tasks[0].state));

        let (mut blocked_core, _base, active) = active_core();
        let before = blocked_core.tasks[0].state;
        complete_turn(
            &mut blocked_core,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            blocked(),
        );
        edges.insert((
            name(before),
            "developer_blocked",
            name(blocked_core.tasks[0].state),
        ));
        observed_states.insert(name(blocked_core.tasks[0].state));

        let (mut changes, committed, reviewer) = active_reviewer_core(3);
        let before = changes.tasks[0].state;
        complete_review(&mut changes, reviewer, request_changes(), committed);
        edges.insert((
            name(before),
            "request_changes",
            name(changes.tasks[0].state),
        ));

        let (mut approved, committed, reviewer) = active_reviewer_core(3);
        let before = approved.tasks[0].state;
        complete_review(&mut approved, reviewer, lgtm(), committed);
        edges.insert((name(before), "lgtm", name(approved.tasks[0].state)));
        observed_states.insert(name(approved.tasks[0].state));

        let (mut exhausted, committed, reviewer) = active_reviewer_core(1);
        let before = exhausted.tasks[0].state;
        complete_review(&mut exhausted, reviewer, request_changes(), committed);
        edges.insert((
            name(before),
            "max_round_request_changes",
            name(exhausted.tasks[0].state),
        ));
        observed_states.insert(name(exhausted.tasks[0].state));

        let (mut mutation, committed, reviewer) = active_reviewer_core(2);
        complete_turn(
            &mut mutation,
            0,
            WorkerRole::Reviewer,
            reviewer.session,
            reviewer.turn,
            reviewer.token,
            lgtm(),
        );
        let mut changed = committed;
        changed.untracked_status_hash = "d".repeat(64);
        changed.clean = false;
        changed.changed_paths = vec!["src/reviewer-edit.rs".into()];
        let before = mutation.tasks[0].state;
        observe(
            &mut mutation,
            0,
            RepositoryCheckpoint::ReviewerPostflight,
            changed,
        );
        edges.insert((
            name(before),
            "reviewer_mutation",
            name(mutation.tasks[0].state),
        ));

        let failed = failed_core();
        observed_states.insert(name(failed.tasks[0].state));
        let canceled = active_canceled_core();
        observed_states.insert(name(canceled.tasks[0].state));

        assert_eq!(
            observed_states,
            BTreeSet::from([
                "pending",
                "developing",
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
                ("developing", "safe_completion_recovery", "developing"),
                ("developing", "clean_commit", "reviewing"),
                ("developing", "developer_blocked", "needs_human"),
                ("reviewing", "request_changes", "developing"),
                ("reviewing", "lgtm", "lgtm"),
                ("reviewing", "max_round_request_changes", "review_exhausted",),
                ("reviewing", "reviewer_mutation", "needs_human"),
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
                SupervisorEventKind::RepositoryObserved,
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
    fn every_task_state_by_relevant_lifecycle_event_has_an_explicit_matrix_row() {
        let task_states = [
            TaskState::Pending,
            TaskState::Developing,
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
            SupervisorEventKind::RepositoryObserved,
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
        assert_eq!(rows, 8 * 7);
        assert_eq!(accepted, 14);
        assert_eq!(rejected, 42);
    }

    #[test]
    fn one_task_first_review_lgtm_has_exact_effects_and_status() {
        let mut core = new_core();
        let base = observation("/repo", '1');
        assert_eq!(
            bind(&mut core, vec![task("one", "/repo", 3)], vec![base.clone()]),
            vec![SupervisorEffect::PublishStatus]
        );
        assert_eq!(core.session_state(), SessionState::AwaitingApproval);
        assert_eq!(
            authorize(&mut core),
            vec![
                SupervisorEffect::ObserveRepository {
                    task_ordinal: 0,
                    checkpoint: RepositoryCheckpoint::TaskStart,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "dev-1");
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "review-1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, lgtm(), committed.clone()),
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
        assert_eq!(
            snapshot,
            SessionStatusSnapshot {
                run_id: "run-1".into(),
                state: SessionState::Completed,
                version: 13,
                project_root: "/project".into(),
                plan_version: Some(1),
                plan_hash: core.plan_hash.clone(),
                current_task_ordinal: Some(0),
                terminal_detail: Some("all ordered tasks reached a terminal review outcome".into()),
                tasks: vec![TaskStatusSnapshot {
                    task_key: "one".into(),
                    ordinal: 0,
                    state: TaskState::Lgtm,
                    repository_root: "/repo".into(),
                    branch: Some("master".into()),
                    review_round: 1,
                    max_review_rounds: 3,
                    base_revision: Some(revision('1')),
                    head_revision: Some(revision('2')),
                    developer_session_bound: true,
                    reviewer_session_bound: true,
                    outcome_detail: Some("Reviewer approved the exact clean revision".into()),
                }],
            }
        );
        let before = core.clone();
        assert_eq!(
            core.reduce(SupervisorEvent::StatusRequested).unwrap(),
            Vec::<SupervisorEffect>::new()
        );
        assert_eq!(core, before);
    }

    #[test]
    fn two_task_journey_closes_before_next_and_rebinds_same_repository_head() {
        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(
            &mut core,
            vec![task("one", "/repo", 2), task("two", "/repo", 2)],
            vec![base.clone(), base.clone()],
        );
        authorize(&mut core);

        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "d1");
        let head_one = developer_observation(&base, '2', true, &["src/one.rs"]);
        complete_developer_ready(&mut core, developer, head_one.clone());
        let reviewer = start_reviewer(&mut core, 0, head_one.clone(), 2, 2, "r1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, lgtm(), head_one.clone()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::ObserveRepository {
                    task_ordinal: 1,
                    checkpoint: RepositoryCheckpoint::TaskStart,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.current_task(), Some(1));
        assert_eq!(core.tasks[1].base_revision, Some(revision('2')));

        let developer = start_first_developer(&mut core, 1, head_one.clone(), 3, 3, "d2");
        let head_two = developer_observation(&head_one, '3', true, &["tests/two.rs"]);
        complete_developer_ready(&mut core, developer, head_two.clone());
        let reviewer = start_reviewer(&mut core, 1, head_two.clone(), 4, 4, "r2", true);
        complete_review(&mut core, reviewer, lgtm(), head_two);
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
    }

    #[test]
    fn request_changes_reuses_exact_role_sessions_then_lgtm() {
        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(&mut core, vec![task("one", "/repo", 3)], vec![base.clone()]);
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "d1");
        let head_one = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, head_one.clone());
        let reviewer = start_reviewer(&mut core, 0, head_one.clone(), 2, 2, "r1", true);
        assert_eq!(
            complete_review(&mut core, reviewer, request_changes(), head_one.clone()),
            vec![
                SupervisorEffect::StartTurn {
                    task_ordinal: 0,
                    role: WorkerRole::Developer,
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
        let head_two = developer_observation(&head_one, '3', true, &["src/lib.rs"]);
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            head_two.clone(),
        );
        let rereviewer = start_reviewer(&mut core, 0, head_two.clone(), 2, 4, "r2", false);
        complete_review(&mut core, rereviewer, lgtm(), head_two);

        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(core.tasks[0].review_round, 2);
        assert_eq!(
            core.tasks[0].developer_session.unwrap().counter(),
            1,
            "Developer correction must use the first logical session"
        );
        assert_eq!(
            core.tasks[0].reviewer_session.unwrap().counter(),
            2,
            "Reviewer re-review must use the first logical session"
        );
    }

    #[test]
    fn multiple_request_changes_can_reach_max_minus_one_then_lgtm() {
        let (mut core, mut committed, mut reviewer) = active_reviewer_core(3);
        assert!(matches!(
            complete_review(&mut core, reviewer, request_changes(), committed.clone()).first(),
            Some(SupervisorEffect::StartTurn {
                purpose: RuntimeTurnPurpose::DeveloperCorrection,
                ..
            })
        ));
        (committed, reviewer) = correct_and_start_rereview(
            &mut core,
            0,
            &committed,
            '3',
            3,
            4,
            "developer-2",
            "reviewer-2",
        );
        complete_review(&mut core, reviewer, request_changes(), committed.clone());
        (committed, reviewer) = correct_and_start_rereview(
            &mut core,
            0,
            &committed,
            '4',
            5,
            6,
            "developer-3",
            "reviewer-3",
        );
        complete_review(&mut core, reviewer, lgtm(), committed);

        assert_eq!(core.session_state(), SessionState::Completed);
        assert_eq!(core.tasks[0].state, TaskState::Lgtm);
        assert_eq!(core.tasks[0].review_round, 3);
        assert_eq!(core.tasks[0].head_revision, Some(revision('4')));
    }

    #[test]
    fn request_changes_at_exact_max_exhausts_then_next_task_uses_terminal_head() {
        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(
            &mut core,
            vec![task("one", "/repo", 2), task("two", "/repo", 2)],
            vec![base.clone(), base.clone()],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "d1");
        let mut committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let mut reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "r1", true);
        complete_review(&mut core, reviewer, request_changes(), committed.clone());
        (committed, reviewer) =
            correct_and_start_rereview(&mut core, 0, &committed, '3', 3, 4, "d2", "r2");
        assert_eq!(
            complete_review(&mut core, reviewer, request_changes(), committed.clone()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::ObserveRepository {
                    task_ordinal: 1,
                    checkpoint: RepositoryCheckpoint::TaskStart,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(core.tasks[0].review_round, 2);
        assert_eq!(core.current_task(), Some(1));
        assert_eq!(core.tasks[1].base_revision, Some(revision('3')));
        start_first_developer(&mut core, 1, committed, 3, 5, "next-developer");
        assert_eq!(core.tasks[1].state, TaskState::Developing);
    }

    #[test]
    fn different_repository_task_keeps_its_bound_base_until_ordered_start() {
        let mut core = new_core();
        let first_base = observation("/repo-one", '1');
        let second_base = observation("/repo-two", '7');
        bind(
            &mut core,
            vec![task("one", "/repo-one", 2), task("two", "/repo-two", 2)],
            vec![first_base.clone(), second_base.clone()],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, first_base.clone(), 1, 1, "d1");
        let committed = developer_observation(&first_base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "r1", true);
        complete_review(&mut core, reviewer, lgtm(), committed);

        assert_eq!(core.current_task(), Some(1));
        assert_eq!(core.tasks[1].base_revision, Some(revision('7')));
        start_first_developer(&mut core, 1, second_base, 3, 3, "d2");
        assert_eq!(core.tasks[1].state, TaskState::Developing);
    }

    #[test]
    fn one_safe_developer_completion_recovery_succeeds_and_second_failure_stops() {
        let base = observation("/repo", '1');

        let mut success = new_core();
        bind(
            &mut success,
            vec![task("success", "/repo", 2)],
            vec![base.clone()],
        );
        authorize(&mut success);
        let developer = start_first_developer(&mut success, 0, base.clone(), 1, 1, "d1");
        complete_turn(
            &mut success,
            0,
            WorkerRole::Developer,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        let dirty = developer_observation(&base, '1', false, &["src/lib.rs"]);
        assert_eq!(
            observe(
                &mut success,
                0,
                RepositoryCheckpoint::DeveloperCompletion,
                dirty
            ),
            vec![
                SupervisorEffect::StartTurn {
                    task_ordinal: 0,
                    role: WorkerRole::Developer,
                    purpose: RuntimeTurnPurpose::DeveloperCompletionRecovery,
                    session: developer.session,
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        let recovery_turn = RuntimeTurnKey::from_counter(2).unwrap();
        start_turn(
            &mut success,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCompletionRecovery,
            developer.session,
            recovery_turn,
            "d-recovery",
        );
        complete_turn(
            &mut success,
            0,
            WorkerRole::Developer,
            developer.session,
            recovery_turn,
            "d-recovery",
            ready(),
        );
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        observe(
            &mut success,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            committed,
        );
        assert_eq!(success.tasks[0].state, TaskState::Reviewing);
        assert!(success.tasks[0].recovery_used);

        let mut failure = new_core();
        bind(
            &mut failure,
            vec![task("failure", "/repo", 2)],
            vec![base.clone()],
        );
        authorize(&mut failure);
        let developer = start_first_developer(&mut failure, 0, base.clone(), 1, 1, "d1");
        complete_turn(
            &mut failure,
            0,
            WorkerRole::Developer,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        observe(
            &mut failure,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            developer_observation(&base, '1', false, &["src/lib.rs"]),
        );
        let recovery_turn = RuntimeTurnKey::from_counter(2).unwrap();
        start_turn(
            &mut failure,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCompletionRecovery,
            developer.session,
            recovery_turn,
            "d-recovery",
        );
        complete_turn(
            &mut failure,
            0,
            WorkerRole::Developer,
            developer.session,
            recovery_turn,
            "d-recovery",
            ready(),
        );
        let effects = observe(
            &mut failure,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            developer_observation(&base, '1', false, &["src/lib.rs"]),
        );
        assert_eq!(
            effects,
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::FinishSession {
                    state: SessionState::NeedsHuman,
                    detail: "developer completion remained invalid after one recovery".into(),
                },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(failure.session_state(), SessionState::NeedsHuman);
    }

    #[test]
    fn runtime_failure_retryability_matrix_is_role_exact_and_transactional() {
        for (role, retryable) in [
            (WorkerRole::Developer, false),
            (WorkerRole::Developer, true),
            (WorkerRole::Reviewer, false),
            (WorkerRole::Reviewer, true),
        ] {
            let (mut core, repository, active) = match role {
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
            if role == WorkerRole::Developer && retryable {
                assert_eq!(
                    effects,
                    vec![
                        SupervisorEffect::ObserveRepository {
                            task_ordinal: 0,
                            checkpoint: RepositoryCheckpoint::DeveloperRecoveryPreflight,
                        },
                        SupervisorEffect::PublishStatus,
                    ]
                );
                assert_eq!(core.session_state(), SessionState::Running);
                assert!(!core.tasks[0].recovery_used);

                let safe_dirty = developer_observation(&repository, '1', false, &["src/lib.rs"]);
                assert_eq!(
                    observe(
                        &mut core,
                        0,
                        RepositoryCheckpoint::DeveloperRecoveryPreflight,
                        safe_dirty,
                    ),
                    vec![
                        SupervisorEffect::StartTurn {
                            task_ordinal: 0,
                            role: WorkerRole::Developer,
                            purpose: RuntimeTurnPurpose::DeveloperCompletionRecovery,
                            session: active.session,
                        },
                        SupervisorEffect::PublishStatus,
                    ]
                );
                let snapshot = core.snapshot();
                assert_eq!(snapshot.state, SessionState::Running);
                assert_eq!(snapshot.current_task_ordinal, Some(0));
                assert_eq!(
                    snapshot.tasks[0].outcome_detail.as_deref(),
                    Some("developer completion recovery in progress")
                );
                assert!(core.tasks[0].recovery_used);
                assert_eq!(core.tasks[0].developer_session, Some(active.session));

                let recovery_turn = RuntimeTurnKey::from_counter(20).unwrap();
                start_turn(
                    &mut core,
                    0,
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCompletionRecovery,
                    active.session,
                    recovery_turn,
                    "retryable-recovery",
                );
                let recovery = ActiveIdentity {
                    task: 0,
                    role: WorkerRole::Developer,
                    session: active.session,
                    turn: recovery_turn,
                    token: "retryable-recovery",
                };
                assert_eq!(
                    core.reduce(fail_turn_event(
                        &core,
                        recovery,
                        RuntimeFailureClass::Contract,
                        true,
                        "typed final outcome was invalid again",
                    ))
                    .unwrap(),
                    vec![
                        SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                        SupervisorEffect::FinishSession {
                            state: SessionState::NeedsHuman,
                            detail:
                                "developer completion result remained invalid after one recovery"
                                    .into(),
                        },
                        SupervisorEffect::PublishStatus,
                    ]
                );
                assert_eq!(
                    core.snapshot().terminal_detail.as_deref(),
                    Some("developer completion result remained invalid after one recovery")
                );
            } else {
                assert_eq!(
                    effects,
                    vec![
                        SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                        SupervisorEffect::FinishSession {
                            state: SessionState::NeedsHuman,
                            detail: "worker runtime contract failed".into(),
                        },
                        SupervisorEffect::PublishStatus,
                    ]
                );
                let snapshot = core.snapshot();
                assert_eq!(snapshot.state, SessionState::NeedsHuman);
                assert_eq!(
                    snapshot.terminal_detail.as_deref(),
                    Some("worker runtime contract failed")
                );
                assert_eq!(
                    snapshot.tasks[0].outcome_detail.as_deref(),
                    Some("worker runtime contract failed")
                );
            }
        }
    }

    #[test]
    fn retryable_developer_failure_requires_safe_repository_evidence() {
        let base = observation("/repo", '1');
        let mut identity_drift = developer_observation(&base, '1', false, &["src/lib.rs"]);
        identity_drift.identity_hash = "2".repeat(64);
        let mut branch_drift = developer_observation(&base, '1', false, &["src/lib.rs"]);
        branch_drift.branch = "other".into();
        let mut history_drift = developer_observation(&base, '2', true, &["src/lib.rs"]);
        history_drift.head_descends_from_expected = false;
        let outside_allowlist = developer_observation(&base, '1', false, &["outside/secret.txt"]);

        for (repository, expected) in [
            (
                identity_drift,
                "developer repository identity, branch, or history drifted",
            ),
            (
                branch_drift,
                "developer repository identity, branch, or history drifted",
            ),
            (
                history_drift,
                "developer repository identity, branch, or history drifted",
            ),
            (
                outside_allowlist,
                "developer changed paths outside the task allowlist",
            ),
        ] {
            let (mut core, _base, active) = active_core();
            assert_eq!(
                core.reduce(fail_turn_event(
                    &core,
                    active,
                    RuntimeFailureClass::Contract,
                    true,
                    "typed final outcome was missing",
                ))
                .unwrap(),
                vec![
                    SupervisorEffect::ObserveRepository {
                        task_ordinal: 0,
                        checkpoint: RepositoryCheckpoint::DeveloperRecoveryPreflight,
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            assert_eq!(
                observe(
                    &mut core,
                    0,
                    RepositoryCheckpoint::DeveloperRecoveryPreflight,
                    repository,
                ),
                vec![
                    SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                    SupervisorEffect::FinishSession {
                        state: SessionState::NeedsHuman,
                        detail: expected.into(),
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
            assert_eq!(core.session_state(), SessionState::NeedsHuman);
            assert!(!core.tasks[0].recovery_used);
            assert_eq!(core.snapshot().terminal_detail.as_deref(), Some(expected));
        }
    }

    #[test]
    fn completion_identity_ordering_and_at_most_once_are_transactional() {
        let (core, _base, active) = active_core();
        let wrong_events = [
            SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: 1,
                role: active.role,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: ready(),
            },
            SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: active.task,
                role: WorkerRole::Reviewer,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: lgtm(),
            },
            SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: active.task,
                role: active.role,
                session: RuntimeSessionKey::from_counter(99).unwrap(),
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: ready(),
            },
            SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: active.task,
                role: active.role,
                session: active.session,
                turn: RuntimeTurnKey::from_counter(99).unwrap(),
                completion_token: active.token.into(),
                outcome: ready(),
            },
            SupervisorEvent::TurnCompleted {
                expected_version: core.version(),
                task_ordinal: active.task,
                role: active.role,
                session: active.session,
                turn: active.turn,
                completion_token: "wrong-token".into(),
                outcome: ready(),
            },
        ];
        for event in wrong_events {
            let mut candidate = core.clone();
            let before = candidate.clone();
            assert_eq!(
                candidate.reduce(event).unwrap_err().code,
                SupervisorErrorCode::InvalidIdentity
            );
            assert_eq!(candidate, before);
        }

        let (mut before_start, _) = pending_turn_core();
        let before = before_start.clone();
        let error = before_start
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: before_start.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(1).unwrap(),
                completion_token: "not-started".into(),
                outcome: ready(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidTransition);
        assert_eq!(before_start, before);

        let mut accepted = core;
        let event = SupervisorEvent::TurnCompleted {
            expected_version: accepted.version(),
            task_ordinal: active.task,
            role: active.role,
            session: active.session,
            turn: active.turn,
            completion_token: active.token.into(),
            outcome: ready(),
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

        let (mut wrong_role, _base, active) = active_core();
        let before = wrong_role.clone();
        let error = wrong_role
            .reduce(SupervisorEvent::TurnCompleted {
                expected_version: wrong_role.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: lgtm(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidEvent);
        assert_eq!(wrong_role, before);
    }

    #[test]
    fn stale_checkpoint_second_active_turn_and_cross_task_session_reuse_are_rejected() {
        let (mut checkpoint, base) = authorized_core();
        let before = checkpoint.clone();
        let error = checkpoint
            .reduce(SupervisorEvent::RepositoryObserved {
                expected_version: checkpoint.version(),
                task_ordinal: 0,
                checkpoint: RepositoryCheckpoint::DeveloperCompletion,
                observation: base,
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(checkpoint, before);

        let (mut active_core, _base, _active) = active_core();
        let before = active_core.clone();
        let error = active_core
            .reduce(SupervisorEvent::TurnStarted {
                expected_version: active_core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                purpose: RuntimeTurnPurpose::InitialDevelopment,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
                turn: RuntimeTurnKey::from_counter(2).unwrap(),
                completion_token: "second".into(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidTransition);
        assert_eq!(active_core, before);

        let mut core = new_core();
        let base = observation("/repo", '1');
        bind(
            &mut core,
            vec![task("one", "/repo", 1), task("two", "/repo", 1)],
            vec![base.clone(), base.clone()],
        );
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "d1");
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        let reviewer = start_reviewer(&mut core, 0, committed.clone(), 2, 2, "r1", true);
        complete_review(&mut core, reviewer, lgtm(), committed.clone());
        observe(&mut core, 1, RepositoryCheckpoint::TaskStart, committed);
        open_runtime(&mut core, 1);
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::RoleSessionOpened {
                expected_version: core.version(),
                task_ordinal: 1,
                role: WorkerRole::Developer,
                session: RuntimeSessionKey::from_counter(1).unwrap(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);
    }

    #[test]
    fn cancel_completion_and_parent_failure_races_have_one_deterministic_winner() {
        let (mut cancel_first, _base, active) = active_core();
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
                    role: WorkerRole::Developer,
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
                role: WorkerRole::Developer,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                outcome: ready(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::Terminal);

        let (mut completion_first, _base, active) = active_core();
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

        let (mut parent_first, _base, active) = active_core();
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
                    role: WorkerRole::Developer,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    failure: runtime_failure(RuntimeFailureClass::Process, false, "late exit",),
                })
                .unwrap_err()
                .code,
            SupervisorErrorCode::Terminal
        );

        let (mut failure_first, _base, active) = active_core();
        failure_first
            .reduce(SupervisorEvent::TurnFailed {
                expected_version: failure_first.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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

        for (rounds, accepted) in [(0, false), (1, true), (20, true), (21, false)] {
            assert_eq!(
                plan_result(vec![task("rounds", "/repo", rounds)]).is_ok(),
                accepted,
                "unexpected max_review_rounds boundary result for {rounds}"
            );
        }
        assert!(new_core().snapshot().tasks.is_empty());
    }

    #[test]
    fn every_task_text_list_and_path_bound_has_below_equal_and_above_cases() {
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
        for (length, expected) in [
            (0, false),
            (1, true),
            (65_535, true),
            (65_536, true),
            (65_537, false),
        ] {
            let mut candidate = task("objective", "/repo", 1);
            candidate.objective = "o".repeat(length);
            assert_eq!(accepted(candidate), expected, "objective length {length}");
        }
        for (length, expected) in [(1, true), (65_535, true), (65_536, true), (65_537, false)] {
            let mut candidate = task("criterion", "/repo", 1);
            candidate.acceptance_criteria = vec!["a".repeat(length)];
            assert_eq!(
                accepted(candidate),
                expected,
                "acceptance criterion length {length}"
            );
        }
        for (length, expected) in [(1, true), (4095, true), (4096, true), (4097, false)] {
            let mut candidate = task("check", "/repo", 1);
            candidate.required_checks = vec!["c".repeat(length)];
            assert_eq!(
                accepted(candidate),
                expected,
                "required check length {length}"
            );
        }
        for (length, expected) in [(1, true), (4095, true), (4096, true), (4097, false)] {
            let mut candidate = task("path", "/repo", 1);
            candidate.allowed_paths = vec!["p".repeat(length)];
            assert_eq!(accepted(candidate), expected, "path length {length}");
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

        for (count, expected) in [(255, true), (256, true), (257, false)] {
            let mut candidate = task("list-count", "/repo", 1);
            candidate.acceptance_criteria = (0..count)
                .map(|index| format!("criterion-{index}"))
                .collect();
            assert_eq!(accepted(candidate), expected, "list item count {count}");
        }

        let mut duplicate_list = task("duplicate-list", "/repo", 1);
        duplicate_list.required_checks = vec!["cargo test".into(), "cargo test".into()];
        assert!(!accepted(duplicate_list));
        let mut duplicate_path = task("duplicate-path", "/repo", 1);
        duplicate_path.allowed_paths = vec!["src".into(), "src".into()];
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
    fn repository_observation_bounds_ordering_and_plan_cleanliness_fail_closed() {
        let base_task = task("one", "/repo", 1);
        let invalid_observations = {
            let mut values = Vec::new();
            let mut invalid = observation("/repo", '1');
            invalid.identity_hash = "short".into();
            values.push(invalid);
            let mut invalid = observation("/repo", '1');
            invalid.branch = "bad\nbranch".into();
            values.push(invalid);
            let mut invalid = observation("/repo", '1');
            invalid.head = "a".repeat(39);
            values.push(invalid);
            let mut invalid = observation("/repo", '1');
            invalid.changed_paths = vec!["src/z.rs".into(), "src/a.rs".into()];
            values.push(invalid);
            let mut invalid = observation("/repo", '1');
            invalid.changed_paths = vec!["../escape".into()];
            values.push(invalid);
            let mut invalid = observation("/repo", '1');
            invalid.changed_paths = (0..257)
                .map(|index| format!("src/file-{index:03}.rs"))
                .collect();
            values.push(invalid);
            values
        };
        for invalid in invalid_observations {
            let mut core = new_core();
            let tasks = vec![base_task.clone()];
            let bindings = vec![TaskRepositoryBinding {
                task_key: "one".into(),
                observation: invalid,
            }];
            let event = plan_event_for(&core, tasks, bindings);
            let before = core.clone();
            assert!(core.reduce(event).is_err());
            assert_eq!(core, before);
        }

        let mut dirty = observation("/repo", '1');
        dirty.clean = false;
        dirty.changed_paths = vec!["src/lib.rs".into()];
        let mut core = new_core();
        let event = plan_event_for(
            &core,
            vec![base_task],
            vec![TaskRepositoryBinding {
                task_key: "one".into(),
                observation: dirty,
            }],
        );
        assert_eq!(
            core.reduce(event).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );
    }

    #[test]
    fn typed_outcome_cross_field_failures_are_rejected_without_consuming_the_turn() {
        let (developer_core, _base, active) = active_core();
        let invalid_developer = [
            DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
                summary: "done".into(),
                questions: vec!["unexpected".into()],
            },
            DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::NeedsHuman,
                summary: "needs input".into(),
                questions: Vec::new(),
            },
            DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Blocked,
                summary: String::new(),
                questions: Vec::new(),
            },
            DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
                summary: "s".repeat(crate::worker::runtime::MAX_OUTCOME_SUMMARY_CHARS + 1),
                questions: Vec::new(),
            },
        ];
        for outcome in invalid_developer {
            let mut core = developer_core.clone();
            let before = core.clone();
            let error = core
                .reduce(SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: 0,
                    role: WorkerRole::Developer,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: RuntimeOutcome::Developer(outcome),
                })
                .unwrap_err();
            assert_eq!(error.code, SupervisorErrorCode::InvalidEvent);
            assert_eq!(core, before);
        }

        let (reviewer_core, _committed, active) = active_reviewer_core(2);
        let RuntimeOutcome::Reviewer(request_changes_outcome) = request_changes() else {
            unreachable!()
        };
        let invalid_reviewer = [
            ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::Lgtm,
                summary: "not sound".into(),
                findings: request_changes_outcome.findings,
            },
            ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                summary: "missing finding".into(),
                findings: Vec::new(),
            },
            ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                summary: "bad path".into(),
                findings: vec![crate::worker::runtime::ReviewFindingV1 {
                    severity: crate::worker::runtime::ReviewFindingSeverity::Major,
                    path: Some("../escape".into()),
                    line: Some(0),
                    message: "bad".into(),
                }],
            },
        ];
        for outcome in invalid_reviewer {
            let mut core = reviewer_core.clone();
            let before = core.clone();
            let error = core
                .reduce(SupervisorEvent::TurnCompleted {
                    expected_version: core.version(),
                    task_ordinal: 0,
                    role: WorkerRole::Reviewer,
                    session: active.session,
                    turn: active.turn,
                    completion_token: active.token.into(),
                    outcome: RuntimeOutcome::Reviewer(outcome),
                })
                .unwrap_err();
            assert_eq!(error.code, SupervisorErrorCode::InvalidEvent);
            assert_eq!(core, before);
        }
    }

    #[test]
    fn needs_human_blocked_timeout_and_runtime_failures_have_exact_safe_outputs() {
        let mut terminal_details = BTreeSet::new();

        for (outcome, expected) in [
            (needs_human(), "developer requested human input"),
            (blocked(), "developer reported an unrecoverable block"),
        ] {
            let (mut core, _base, active) = active_core();
            complete_turn(
                &mut core,
                0,
                WorkerRole::Developer,
                active.session,
                active.turn,
                active.token,
                outcome,
            );
            assert_eq!(core.session_state(), SessionState::NeedsHuman);
            assert_eq!(core.snapshot().terminal_detail.as_deref(), Some(expected));
            terminal_details.insert(expected.to_owned());
        }

        let (mut timed_out, _base, active) = active_core();
        let effects = timed_out
            .reduce(SupervisorEvent::Timeout {
                expected_version: timed_out.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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
                    role: WorkerRole::Developer,
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
            let (mut core, _base, active) = active_core();
            core.reduce(SupervisorEvent::TurnFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
                session: active.session,
                turn: active.turn,
                completion_token: active.token.into(),
                failure: runtime_failure(class, false, "RAW_SECRET_PROVIDER_DETAIL"),
            })
            .unwrap();
            let snapshot = core.snapshot();
            assert_eq!(snapshot.state, expected_state);
            assert_eq!(snapshot.terminal_detail.as_deref(), Some(expected));
            assert!(
                !serde_json::to_string(&snapshot)
                    .unwrap()
                    .contains("RAW_SECRET_PROVIDER_DETAIL")
            );
            terminal_details.insert(expected.into());
        }
        assert_eq!(terminal_details.len(), 8);
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
            let (mut core, _base) = authorized_core();
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
            let (mut core, _base) = authorized_core();
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
    fn git_failure_diagnostics_are_distinct_and_snapshots_exclude_raw_outcomes() {
        let base = observation("/repo", '1');

        let mut dirty_start = new_core();
        bind(
            &mut dirty_start,
            vec![task("dirty", "/repo", 1)],
            vec![base.clone()],
        );
        authorize(&mut dirty_start);
        let mut dirty = base.clone();
        dirty.clean = false;
        dirty.changed_paths = vec!["src/lib.rs".into()];
        dirty.index_diff_hash = "c".repeat(64);
        observe(&mut dirty_start, 0, RepositoryCheckpoint::TaskStart, dirty);

        let (mut out_of_scope, _base, active) = active_core();
        complete_turn(
            &mut out_of_scope,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        observe(
            &mut out_of_scope,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            developer_observation(&base, '2', true, &["Cargo.toml"]),
        );

        let (mut history_drift, _base, active) = active_core();
        complete_turn(
            &mut history_drift,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            ready(),
        );
        let mut non_ff = developer_observation(&base, '2', true, &["src/lib.rs"]);
        non_ff.head_descends_from_expected = false;
        observe(
            &mut history_drift,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            non_ff,
        );

        let (mut reviewer_mutation, committed, reviewer) = active_reviewer_core(1);
        complete_turn(
            &mut reviewer_mutation,
            0,
            WorkerRole::Reviewer,
            reviewer.session,
            reviewer.turn,
            reviewer.token,
            lgtm(),
        );
        let mut changed = committed;
        changed.tracked_diff_hash = "d".repeat(64);
        changed.clean = false;
        changed.changed_paths = vec!["src/lib.rs".into()];
        observe(
            &mut reviewer_mutation,
            0,
            RepositoryCheckpoint::ReviewerPostflight,
            changed,
        );

        let details = [
            dirty_start.snapshot().terminal_detail.unwrap(),
            out_of_scope.snapshot().terminal_detail.unwrap(),
            history_drift.snapshot().terminal_detail.unwrap(),
            reviewer_mutation.snapshot().terminal_detail.unwrap(),
        ];
        assert_eq!(details.iter().collect::<BTreeSet<_>>().len(), details.len());
        assert_eq!(
            details,
            [
                "task repository drifted before task start",
                "developer changed paths outside the task allowlist",
                "developer repository identity, branch, or history drifted",
                "Reviewer changed repository state; verdict rejected",
            ]
        );

        let (mut raw_outcome, _base, active) = active_core();
        complete_turn(
            &mut raw_outcome,
            0,
            WorkerRole::Developer,
            active.session,
            active.turn,
            active.token,
            RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::NeedsHuman,
                summary: "RAW_SECRET_SUMMARY".into(),
                questions: vec!["RAW_SECRET_QUESTION".into()],
            }),
        );
        let encoded = serde_json::to_string(&raw_outcome.snapshot()).unwrap();
        assert!(!encoded.contains("RAW_SECRET"));
        assert!(encoded.contains("developer requested human input"));
    }

    #[test]
    fn plan_replacement_and_hashing_are_deterministic_and_order_sensitive() {
        let mut first = new_core();
        let tasks = vec![task("one", "/one", 2), task("two", "/two", 2)];
        let bindings = bindings_for(&tasks);
        let hash = first.expected_plan_hash(1, &tasks, &bindings);
        assert_eq!(hash, first.expected_plan_hash(1, &tasks, &bindings));

        let mut reversed_tasks = tasks.clone();
        let mut reversed_bindings = bindings.clone();
        reversed_tasks.reverse();
        reversed_bindings.reverse();
        assert_ne!(
            hash,
            first.expected_plan_hash(1, &reversed_tasks, &reversed_bindings)
        );

        let event = plan_event_for(&first, tasks.clone(), bindings);
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
    fn plan_repository_binding_order_and_same_root_identity_are_exact() {
        let tasks = vec![task("one", "/repo", 1), task("two", "/repo", 1)];
        let mut mismatched_order = bindings_for(&tasks);
        mismatched_order.swap(0, 1);
        let mut core = new_core();
        let event = plan_event_for(&core, tasks.clone(), mismatched_order);
        assert_eq!(
            core.reduce(event).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );

        let mut inconsistent = bindings_for(&tasks);
        inconsistent[1].observation.head = revision('2');
        let mut core = new_core();
        let event = plan_event_for(&core, tasks.clone(), inconsistent);
        assert_eq!(
            core.reduce(event).unwrap_err().code,
            SupervisorErrorCode::InvalidPlan
        );

        assert!(plan_result(tasks).is_ok());
    }

    #[test]
    fn recovery_checkpoint_external_drift_and_future_task_events_stop_or_reject() {
        let base = observation("/repo", '1');
        let mut core = new_core();
        bind(
            &mut core,
            vec![task("one", "/repo", 2), task("two", "/repo", 2)],
            vec![base.clone(), base.clone()],
        );
        authorize(&mut core);
        let before = core.clone();
        let error = core
            .reduce(SupervisorEvent::RepositoryObserved {
                expected_version: core.version(),
                task_ordinal: 1,
                checkpoint: RepositoryCheckpoint::TaskStart,
                observation: base.clone(),
            })
            .unwrap_err();
        assert_eq!(error.code, SupervisorErrorCode::InvalidIdentity);
        assert_eq!(core, before);

        let developer = start_first_developer(&mut core, 0, base.clone(), 1, 1, "developer");
        complete_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            developer.session,
            developer.turn,
            developer.token,
            ready(),
        );
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            developer_observation(&base, '1', false, &["src/lib.rs"]),
        );
        let recovery_turn = RuntimeTurnKey::from_counter(2).unwrap();
        start_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCompletionRecovery,
            developer.session,
            recovery_turn,
            "recovery",
        );
        complete_turn(
            &mut core,
            0,
            WorkerRole::Developer,
            developer.session,
            recovery_turn,
            "recovery",
            ready(),
        );
        let mut drift = developer_observation(&base, '2', true, &["src/lib.rs"]);
        drift.branch = "other".into();
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::DeveloperCompletion,
            drift,
        );
        assert_eq!(core.session_state(), SessionState::NeedsHuman);
        assert_eq!(
            core.snapshot().terminal_detail.as_deref(),
            Some("developer repository identity, branch, or history drifted")
        );
    }

    #[test]
    fn accepted_completion_token_cannot_be_reused_for_a_later_role_turn() {
        let (mut core, base, developer) = active_core();
        let committed = developer_observation(&base, '2', true, &["src/lib.rs"]);
        complete_developer_ready(&mut core, developer, committed.clone());
        observe(
            &mut core,
            0,
            RepositoryCheckpoint::ReviewerPreflight,
            committed,
        );
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
                role: WorkerRole::Reviewer,
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
    fn core_owned_identifier_diagnostic_and_repository_bounds_are_exact() {
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
            let (mut core, _) = pending_turn_core();
            let event = SupervisorEvent::TurnStarted {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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
                    let (mut core, _repository, active) = match role {
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

        for (length, accepted) in [(511, true), (512, true), (513, false)] {
            let mut value = observation("/repo", '1');
            value.branch = "b".repeat(length);
            assert_eq!(value.validate().is_ok(), accepted, "branch length {length}");
        }
        for (length, accepted) in [(4095, true), (4096, true), (4097, false)] {
            let mut value = observation("/repo", '1');
            value.changed_paths = vec![format!("src/{}", "p".repeat(length - 4))];
            assert_eq!(
                value.validate().is_ok(),
                accepted,
                "repository changed path length {length}"
            );
        }
        for (count, accepted) in [(255, true), (256, true), (257, false)] {
            let mut value = observation("/repo", '1');
            value.changed_paths = (0..count)
                .map(|index| format!("src/{index:03}.rs"))
                .collect();
            assert_eq!(
                value.validate().is_ok(),
                accepted,
                "repository changed-path count {count}"
            );
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

        let (mut missing_current, _) = authorized_core();
        missing_current.current_task = None;
        cases.push(("running without current task", missing_current));

        let (mut active_without_runtime, _base, _active) = active_core();
        active_without_runtime.runtime_open = None;
        cases.push(("active turn without runtime", active_without_runtime));

        let (mut excessive_round, _committed, _active) = active_reviewer_core(1);
        excessive_round.tasks[0].review_round = 2;
        cases.push(("review round above maximum", excessive_round));

        let mut future_advanced = new_core();
        let base = observation("/repo", '1');
        bind(
            &mut future_advanced,
            vec![task("one", "/repo", 1), task("two", "/repo", 1)],
            vec![base.clone(), base],
        );
        authorize(&mut future_advanced);
        future_advanced.tasks[1].state = TaskState::Developing;
        cases.push(("future task advanced", future_advanced));

        let mut terminal_live = completed_core();
        terminal_live.runtime_open = Some(0);
        cases.push(("terminal session with live runtime", terminal_live));

        let (mut duplicate_session, _) = bound_core();
        let session = RuntimeSessionKey::from_counter(1).unwrap();
        duplicate_session.tasks[0].developer_session = Some(session);
        duplicate_session.tasks[0].reviewer_session = Some(session);
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
