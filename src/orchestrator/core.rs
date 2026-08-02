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
use std::collections::BTreeSet;
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
    TaskRuntimeOpened,
    RoleSessionOpened,
    TurnStarted,
    TurnCompleted,
    TurnFailed,
    DriverFailed,
    Timeout,
    CancelRequested,
    ParentStopping,
    StatusRequested,
}

impl SupervisorEventKind {
    pub const ALL: [Self; 12] = [
        Self::PlanBound,
        Self::ExecutionAuthorized,
        Self::TaskRuntimeOpened,
        Self::RoleSessionOpened,
        Self::TurnStarted,
        Self::TurnCompleted,
        Self::TurnFailed,
        Self::DriverFailed,
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
    InterruptTurn,
    CloseTaskRuntime,
    FinishSession,
    PublishStatus,
}

#[cfg(test)]
impl SupervisorEffectKind {
    const ALL: [Self; 7] = [
        Self::OpenTaskRuntime,
        Self::OpenRoleSession,
        Self::StartTurn,
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
    pub developer_session: Option<RuntimeSessionKey>,
    pub reviewer_session: Option<RuntimeSessionKey>,
    pub outcome_detail: Option<String>,
    latest_developer_final_path: Option<String>,
    latest_reviewer_final_paths: Vec<String>,
    latest_reviewer_verdict: Option<ReviewerVerdict>,
    last_developer_outcome: Option<DeveloperOutcomeV1>,
    last_reviewer_outcome: Option<ReviewerOutcomeV1>,
}

impl CoreTask {
    fn new(spec: TaskDraft) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            review_round: 0,
            developer_session: None,
            reviewer_session: None,
            outcome_detail: None,
            latest_developer_final_path: None,
            latest_reviewer_final_paths: Vec::new(),
            latest_reviewer_verdict: None,
            last_developer_outcome: None,
            last_reviewer_outcome: None,
        }
    }

    pub fn last_developer_outcome(&self) -> Option<&DeveloperOutcomeV1> {
        self.last_developer_outcome.as_ref()
    }

    pub fn last_reviewer_outcome(&self) -> Option<&ReviewerOutcomeV1> {
        self.last_reviewer_outcome.as_ref()
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

    pub fn expected_plan_hash(&self, plan_version: u64, tasks: &[TaskDraft]) -> String {
        canonical_hash(&(
            "hcom-codex-exec-session-plan-v2",
            plan_version,
            &self.project_root,
            &self.profile_hash,
            tasks,
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
                    task_document_path: task.spec.task_document_path.clone(),
                    design_document_paths: task.spec.design_document_paths.clone(),
                    task_selector: task.spec.task_selector.clone(),
                    branch: None,
                    review_round: task.review_round,
                    max_review_rounds: task.spec.max_review_rounds,
                    base_revision: None,
                    head_revision: None,
                    developer_session_bound: task.developer_session.is_some(),
                    reviewer_session_bound: task.reviewer_session.is_some(),
                    outcome_detail: task.outcome_detail.clone(),
                    latest_developer_final_path: task.latest_developer_final_path.clone(),
                    final_reviewer_message_paths: task.latest_reviewer_final_paths.clone(),
                    reviewer_verdict: task.latest_reviewer_verdict,
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
                ..
            } => self.bind_plan(plan_version, plan_hash, tasks),
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
            task.validate()
                .map_err(|error| SupervisorError::invalid_plan(error.to_string()))?;
            if !keys.insert(task.task_key.as_str()) {
                return Err(SupervisorError::invalid_plan(
                    "ordered plan task keys must be unique",
                ));
            }
        }

        let expected_hash = self.expected_plan_hash(plan_version, &tasks);
        if plan_hash != expected_hash {
            return Err(SupervisorError::invalid_plan(
                "plan hash does not match the exact ordered plan binding",
            ));
        }
        let next_plan_version = plan_version
            .checked_add(1)
            .ok_or_else(|| SupervisorError::overflow("plan version overflow"))?;

        self.tasks = tasks.into_iter().map(CoreTask::new).collect();
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
        self.schedule_runtime_open(0)
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
                // The developer's exit routes straight to review. The
                // supervisor inspects nothing about the work itself.
                DeveloperOutcomeStatus::Ready => {
                    if self.tasks[task_ordinal].state != TaskState::Developing {
                        return Err(SupervisorError::invalid_transition(
                            "developer completion requires a developing task",
                        ));
                    }
                    let task = &mut self.tasks[task_ordinal];
                    task.last_developer_outcome = Some(developer);
                    task.state = TaskState::Reviewing;
                    task.outcome_detail =
                        Some("developer turn completed; routing to review".into());
                    self.start_reviewer(task_ordinal)
                }
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
            RuntimeOutcome::Reviewer(reviewer) => {
                self.handle_reviewer_verdict(task_ordinal, reviewer)
            }
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
        self.terminalize_current(session_state, task_state, &detail, Vec::new())
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

    /// Start (or resume) the reviewer for a task already in review.
    ///
    /// Deliberately takes no Git observation: the diff base and head were
    /// captured at task start and developer completion, which is everything
    /// routing needs. Re-inspecting the repository here would reintroduce a
    /// quality gate — whether the tree drifted is the reviewer's and the
    /// human's call.
    fn start_reviewer(
        &mut self,
        task_ordinal: usize,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        let task = &mut self.tasks[task_ordinal];
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

    fn handle_reviewer_verdict(
        &mut self,
        task_ordinal: usize,
        outcome: ReviewerOutcomeV1,
    ) -> Result<Vec<SupervisorEffect>, SupervisorError> {
        if self.tasks[task_ordinal].state != TaskState::Reviewing {
            return Err(SupervisorError::invalid_transition(
                "a reviewer verdict requires a reviewing task",
            ));
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
        self.runtime_open = None;
        self.pending_session_open = None;
        self.pending_turn_start = None;
        self.active_turn = None;

        let mut effects = vec![SupervisorEffect::CloseTaskRuntime { task_ordinal }];
        if task_ordinal + 1 < self.tasks.len() {
            let next = task_ordinal + 1;
            self.current_task = Some(next);
            effects.extend(self.schedule_runtime_open(next)?);
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
                || self.active_turn.is_some())
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
            if task.state == TaskState::Reviewing
                && (task.developer_session.is_none()
                    || task.review_round >= u32::from(task.spec.max_review_rounds))
            {
                return Err(SupervisorError::invariant(
                    "reviewing task lacks a Developer handoff",
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

    const PROFILE_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn task(key: &str, root: &str, max_review_rounds: u8) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: format!("Task {key}"),
            repository_root: root.into(),
            task_document_path: format!("/project/tasks/{key}.md"),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: key.into(),
            max_review_rounds,
        }
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

    fn bind(core: &mut SupervisorCore, tasks: Vec<TaskDraft>) -> Vec<SupervisorEffect> {
        let plan_version = core.next_plan_version;
        let plan_hash = core.expected_plan_hash(plan_version, &tasks);
        core.reduce(SupervisorEvent::PlanBound {
            expected_version: core.version(),
            plan_version,
            plan_hash,
            tasks,
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
        session_counter: u64,
        turn_counter: u64,
        token: &'static str,
    ) -> ActiveIdentity {
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
                    role: WorkerRole::Reviewer,
                    ..
                }) | Some(SupervisorEffect::StartTurn {
                    role: WorkerRole::Reviewer,
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
        let session = if first {
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
            core.tasks[task_ordinal].reviewer_session.unwrap()
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

    /// A reviewer verdict lands directly from its completed turn: the lane
    /// takes no Git observation around review.
    fn complete_review(
        core: &mut SupervisorCore,
        active: ActiveIdentity,
        outcome: RuntimeOutcome,
    ) -> Vec<SupervisorEffect> {
        complete_turn(
            core,
            active.task,
            active.role,
            active.session,
            active.turn,
            active.token,
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
        let reviewer_session_counter = core.tasks[task_ordinal].reviewer_session.unwrap().counter();
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

    fn completed_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "review", true);
        complete_review(&mut core, reviewer, lgtm());
        core
    }

    fn needs_human_core() -> SupervisorCore {
        let (mut core, developer) = active_core();
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
        let (mut core, developer) = active_core();
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

    fn active_reviewer_core(max_review_rounds: u8) -> (SupervisorCore, ActiveIdentity) {
        let mut core = new_core();
        bind(&mut core, vec![task("one", "/repo", max_review_rounds)]);
        authorize(&mut core);
        let developer = start_first_developer(&mut core, 0, 1, 1, "developer");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 0, 2, 2, "reviewer", true);
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
        let (mut core, reviewer) = active_reviewer_core(1);
        complete_review(&mut core, reviewer, request_changes());
        core
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
            TaskState::Reviewing => {
                let (core, event) = match kind {
                    SupervisorEventKind::RoleSessionOpened => {
                        let core = reviewing_pending_session_core();
                        let event = SupervisorEvent::RoleSessionOpened {
                            expected_version: core.version(),
                            task_ordinal: 0,
                            role: WorkerRole::Reviewer,
                            session: RuntimeSessionKey::from_counter(2).unwrap(),
                        };
                        (core, event)
                    }
                    SupervisorEventKind::TurnStarted => {
                        let core = reviewing_pending_turn_core();
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
                        let (core, _) = active_reviewer_core(2);
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
        assert_eq!(accepted, 23);
        assert_eq!(rejected, 61);
    }

    #[test]
    fn every_effect_kind_has_a_real_core_production_path() {
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

        let (mut blocked_core, active) = active_core();
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

        let (mut exhausted, reviewer) = active_reviewer_core(1);
        let before = exhausted.tasks[0].state;
        complete_review(&mut exhausted, reviewer, request_changes());
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
                ("developing", "developer_blocked", "needs_human"),
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
        assert_eq!(rows, 8 * 6);
        assert_eq!(accepted, 11);
        assert_eq!(rejected, 37);
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
        assert_eq!(
            snapshot,
            SessionStatusSnapshot {
                run_id: "run-1".into(),
                state: SessionState::Completed,
                version: 9,
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
                    task_document_path: "/project/tasks/one.md".into(),
                    design_document_paths: vec!["/project/design.md".into()],
                    task_selector: "one".into(),
                    // hcom no longer observes Git, so the snapshot carries no
                    // branch or revision evidence at all.
                    branch: None,
                    review_round: 1,
                    max_review_rounds: 3,
                    base_revision: None,
                    head_revision: None,
                    developer_session_bound: true,
                    reviewer_session_bound: true,
                    outcome_detail: Some("Reviewer approved the exact clean revision".into()),
                    latest_developer_final_path: None,
                    final_reviewer_message_paths: Vec::new(),
                    reviewer_verdict: None,
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
    fn two_task_journey_closes_each_runtime_before_the_next_task_opens() {
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
            complete_review(&mut core, reviewer, lgtm()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::OpenTaskRuntime { task_ordinal: 1 },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.current_task(), Some(1));

        let developer = start_first_developer(&mut core, 1, 3, 3, "d2");
        complete_developer_ready(&mut core, developer);
        let reviewer = start_reviewer(&mut core, 1, 4, 4, "r2", true);
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
            core.tasks[0].reviewer_session.unwrap().counter(),
            2,
            "Reviewer re-review must use the first logical session"
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
        let mut reviewer = start_reviewer(&mut core, 0, 2, 2, "r1", true);
        complete_review(&mut core, reviewer, request_changes());
        reviewer = correct_and_start_rereview(&mut core, 0, 3, 4, "d2", "r2");
        assert_eq!(
            complete_review(&mut core, reviewer, request_changes()),
            vec![
                SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                SupervisorEffect::OpenTaskRuntime { task_ordinal: 1 },
                SupervisorEffect::PublishStatus,
            ]
        );
        assert_eq!(core.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(core.tasks[0].review_round, 2);
        assert_eq!(core.current_task(), Some(1));
        start_first_developer(&mut core, 1, 3, 5, "next-developer");
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
            assert_eq!(
                effects,
                vec![
                    SupervisorEffect::CloseTaskRuntime { task_ordinal: 0 },
                    SupervisorEffect::FinishSession {
                        state: SessionState::NeedsHuman,
                        detail: expected_detail.into(),
                    },
                    SupervisorEffect::PublishStatus,
                ]
            );
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

        let mut before_start = pending_turn_core();
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

        let (mut wrong_role, active) = active_core();
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
    fn second_active_turn_and_cross_task_session_reuse_are_rejected() {
        let (mut active_core, _active) = active_core();
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
                role: WorkerRole::Developer,
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

        let (mut failure_first, active) = active_core();
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
    fn typed_outcome_cross_field_failures_are_rejected_without_consuming_the_turn() {
        let (developer_core, active) = active_core();
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

        let (reviewer_core, active) = active_reviewer_core(2);
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
            let (mut core, active) = active_core();
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

        let (mut timed_out, active) = active_core();
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
            let (mut core, active) = active_core();
            core.reduce(SupervisorEvent::TurnFailed {
                expected_version: core.version(),
                task_ordinal: 0,
                role: WorkerRole::Developer,
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
    fn snapshots_exclude_raw_developer_outcomes() {
        let (mut raw_outcome, active) = active_core();
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
                role: WorkerRole::Developer,
                session: developer.session,
                turn: developer.turn,
                completion_token: developer.token.into(),
                outcome: ready(),
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
