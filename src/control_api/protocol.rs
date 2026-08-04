use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path};

pub const PROTOCOL_VERSION: u32 = 5;
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;

const MAX_ID_BYTES: usize = 128;
const MAX_PATH_BYTES: usize = 4096;
const MAX_TASKS: usize = 64;
const MAX_LIST_ITEMS: usize = 256;
const MAX_CLARIFICATION_ROUNDS: u8 = 20;
pub const MAX_CLARIFICATION_PAGE_RECORDS: u8 = 8;
pub const MAX_CLARIFICATION_RECORDS_PER_TASK: usize = 64;
pub const MAX_CLARIFICATION_RECORDS_PER_RUN: usize = MAX_TASKS * MAX_CLARIFICATION_ROUNDS as usize;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub protocol_version: u32,
    pub request_id: String,
    pub caller: CallerAuth,
    pub action: ControlAction,
}

impl ControlRequest {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolValidationError::new("unsupported protocol version"));
        }
        validate_id("request_id", &self.request_id)?;
        self.caller.validate()?;
        self.action.validate()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallerAuth {
    Human {
        process_birth: String,
    },
    Architect {
        binding_id: String,
        launch_nonce: String,
        capability: String,
    },
}

impl CallerAuth {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        match self {
            Self::Human { process_birth } => {
                validate_single_line("process_birth", process_birth, 256)
            }
            Self::Architect {
                binding_id,
                launch_nonce,
                capability,
            } => {
                validate_id("binding_id", binding_id)?;
                validate_secret("launch_nonce", launch_nonce)?;
                validate_secret("capability", capability)?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ActionName {
    SessionPlanReplace,
    SessionApproveAndStart,
    SessionClarificationSubmit,
    SessionClarificationRequireHuman,
    SessionClarificationsList,
    SessionWait,
    SessionStatus,
    SessionCancel,
}

impl ActionName {
    pub const ARCHITECT: [Self; 8] = [
        Self::SessionPlanReplace,
        Self::SessionApproveAndStart,
        Self::SessionClarificationSubmit,
        Self::SessionClarificationRequireHuman,
        Self::SessionClarificationsList,
        Self::SessionWait,
        Self::SessionStatus,
        Self::SessionCancel,
    ];
    pub const ALL: [Self; 8] = Self::ARCHITECT;

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionPlanReplace => "session_plan_replace",
            Self::SessionApproveAndStart => "session_approve_and_start",
            Self::SessionClarificationSubmit => "session_clarification_submit",
            Self::SessionClarificationRequireHuman => "session_clarification_require_human",
            Self::SessionClarificationsList => "session_clarifications_list",
            Self::SessionWait => "session_wait",
            Self::SessionStatus => "session_status",
            Self::SessionCancel => "session_cancel",
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    SessionPlanReplace {
        expected_session_version: u64,
        developer_adapter: String,
        reviewer_adapter: String,
        tasks: Vec<TaskDraft>,
    },
    SessionApproveAndStart {
        expected_session_version: u64,
        plan_version: u64,
        plan_hash: String,
        approval_confirmed: bool,
    },
    SessionClarificationSubmit {
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: String,
        action_sequence: u32,
        developer_request_path: String,
        clarification_document_path: String,
        human_decision_confirmed: bool,
    },
    SessionClarificationRequireHuman {
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: String,
        action_sequence: u32,
        developer_request_path: String,
    },
    SessionClarificationsList {
        task_ordinal: u32,
        task_key: String,
        after_sequence: u32,
        limit: u8,
    },
    SessionWait {
        after_session_version: u64,
    },
    SessionStatus,
    SessionCancel {
        expected_session_version: u64,
        reason: String,
    },
}

impl ControlAction {
    pub fn name(&self) -> ActionName {
        match self {
            Self::SessionPlanReplace { .. } => ActionName::SessionPlanReplace,
            Self::SessionApproveAndStart { .. } => ActionName::SessionApproveAndStart,
            Self::SessionClarificationSubmit { .. } => ActionName::SessionClarificationSubmit,
            Self::SessionClarificationRequireHuman { .. } => {
                ActionName::SessionClarificationRequireHuman
            }
            Self::SessionClarificationsList { .. } => ActionName::SessionClarificationsList,
            Self::SessionWait { .. } => ActionName::SessionWait,
            Self::SessionStatus => ActionName::SessionStatus,
            Self::SessionCancel { .. } => ActionName::SessionCancel,
        }
    }

    pub(crate) fn validate_for_tool(&self) -> Result<(), ProtocolValidationError> {
        self.validate()
    }

    fn validate(&self) -> Result<(), ProtocolValidationError> {
        match self {
            Self::SessionPlanReplace {
                developer_adapter,
                reviewer_adapter,
                tasks,
                ..
            } => {
                validate_single_line("developer_adapter", developer_adapter, 64)?;
                validate_single_line("reviewer_adapter", reviewer_adapter, 64)?;
                validate_tasks(tasks)
            }
            Self::SessionApproveAndStart {
                plan_version,
                plan_hash,
                approval_confirmed,
                ..
            } => {
                if *plan_version == 0 {
                    return Err(ProtocolValidationError::new(
                        "plan_version must be positive",
                    ));
                }
                validate_hash("plan_hash", plan_hash)?;
                if !approval_confirmed {
                    return Err(ProtocolValidationError::new(
                        "approval_confirmed must be true",
                    ));
                }
                Ok(())
            }
            Self::SessionClarificationSubmit {
                task_key,
                action_sequence,
                developer_request_path,
                clarification_document_path,
                ..
            } => {
                validate_id("task_key", task_key)?;
                if *action_sequence == 0 {
                    return Err(ProtocolValidationError::new(
                        "action_sequence must be positive",
                    ));
                }
                validate_document_path("developer request path", developer_request_path)?;
                validate_document_path("clarification document path", clarification_document_path)
            }
            Self::SessionClarificationRequireHuman {
                task_key,
                action_sequence,
                developer_request_path,
                ..
            } => {
                validate_id("task_key", task_key)?;
                if *action_sequence == 0 {
                    return Err(ProtocolValidationError::new(
                        "action_sequence must be positive",
                    ));
                }
                validate_document_path("developer request path", developer_request_path)
            }
            Self::SessionClarificationsList {
                task_key, limit, ..
            } => {
                validate_id("task_key", task_key)?;
                if !(1..=MAX_CLARIFICATION_PAGE_RECORDS).contains(limit) {
                    return Err(ProtocolValidationError::new(
                        "clarification page limit is out of range",
                    ));
                }
                Ok(())
            }
            Self::SessionWait { .. } | Self::SessionStatus => Ok(()),
            Self::SessionCancel { reason, .. } => {
                validate_free_text("cancel reason", reason, 4096, false)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRole {
    Developer,
    Reviewer,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeSessionMode {
    Preassigned,
    Discovered,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapabilitySnapshot {
    pub contract_hash: String,
    pub features: Vec<String>,
}

impl CapabilitySnapshot {
    pub(crate) fn validate(&self) -> Result<(), ProtocolValidationError> {
        validate_hash("contract_hash", &self.contract_hash)?;
        validate_list_len("capability features", &self.features)?;
        let mut unique = BTreeSet::new();
        for feature in &self.features {
            validate_single_line("capability feature", feature, 128)?;
            if !unique.insert(feature) {
                return Err(ProtocolValidationError::new(
                    "capability features must be unique",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskDraft {
    pub task_key: String,
    pub title: String,
    pub repository_root: String,
    pub task_document_path: String,
    pub design_document_paths: Vec<String>,
    pub task_selector: String,
    pub max_review_rounds: u8,
    pub max_clarification_rounds: u8,
}

impl TaskDraft {
    pub(crate) fn validate(&self) -> Result<(), ProtocolValidationError> {
        validate_id("task_key", &self.task_key)?;
        if !matches!(
            Path::new(&self.task_key)
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [Component::Normal(_)]
        ) {
            return Err(ProtocolValidationError::new(
                "task_key must also be one normal path component",
            ));
        }
        validate_free_text("task title", &self.title, 512, false)?;
        validate_repository_root(&self.repository_root)?;
        validate_document_path("task document path", &self.task_document_path)?;
        validate_list_len("design document path", &self.design_document_paths)?;
        let mut design_document_paths = BTreeSet::new();
        for path in &self.design_document_paths {
            validate_document_path("design document path", path)?;
            if !design_document_paths.insert(path) {
                return Err(ProtocolValidationError::new(
                    "design document path entries must be unique",
                ));
            }
        }
        validate_single_line("task selector", &self.task_selector, 4096)?;
        if !(1..=20).contains(&self.max_review_rounds) {
            return Err(ProtocolValidationError::new(
                "max_review_rounds must be between 1 and 20",
            ));
        }
        if !(1..=MAX_CLARIFICATION_ROUNDS).contains(&self.max_clarification_rounds) {
            return Err(ProtocolValidationError::new(
                "max_clarification_rounds must be between 1 and 20",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    AwaitingPlan,
    AwaitingApproval,
    Running,
    Completed,
    NeedsHuman,
    Failed,
    Canceled,
}

impl SessionState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::NeedsHuman | Self::Failed | Self::Canceled
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Developing,
    AwaitingArchitectAction,
    Reviewing,
    Lgtm,
    ReviewExhausted,
    NeedsHuman,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchitectActionReason {
    Clarification,
    Blocker,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClarificationRecord {
    pub sequence: u32,
    pub reason: ArchitectActionReason,
    pub developer_request_path: String,
    pub architect_clarification_path: String,
    pub human_decision_confirmed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PendingArchitectActionSnapshot {
    pub task_ordinal: u32,
    pub task_key: String,
    pub sequence: u32,
    pub reason: ArchitectActionReason,
    pub developer_request_path: String,
    pub clarification_output_path: String,
    pub clarification_rounds_used: u32,
    pub max_clarification_rounds: u8,
    pub human_decision_required: bool,
    pub published_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActiveWorkerSnapshot {
    pub task_ordinal: u32,
    pub task_key: String,
    pub role: WorkerRole,
    pub purpose: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerVerdict {
    Lgtm,
    RequestChanges,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControlResponse {
    pub protocol_version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<ControlResult>,
    pub error: Option<ControlErrorBody>,
}

impl ControlResponse {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolValidationError::new("unsupported protocol version"));
        }
        validate_id("request_id", &self.request_id)?;
        match (self.ok, &self.result, &self.error) {
            (true, Some(_), None) => Ok(()),
            (false, None, Some(error)) => {
                validate_free_text("control error message", &error.message, 4096, false)
            }
            _ => Err(ProtocolValidationError::new(
                "invalid control response envelope",
            )),
        }
    }

    pub(crate) fn error(
        request_id: impl Into<String>,
        code: ControlErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            ok: false,
            result: None,
            error: Some(ControlErrorBody {
                code,
                message: message.into(),
            }),
        }
    }

    pub(crate) fn success(request_id: impl Into<String>, result: ControlResult) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            ok: true,
            result: Some(result),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlResult {
    Session {
        session: SessionStatusSnapshot,
    },
    Plan {
        session: SessionStatusSnapshot,
        plan_version: u64,
        plan_hash: String,
    },
    Clarifications {
        page: ClarificationPage,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClarificationPage {
    pub run_id: String,
    pub session_version: u64,
    pub task_ordinal: u32,
    pub task_key: String,
    pub total_records: u32,
    pub after_sequence: u32,
    pub records: Vec<ClarificationRecord>,
    pub next_after_sequence: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionStatusSnapshot {
    pub run_id: String,
    pub state: SessionState,
    pub version: u64,
    pub project_root: String,
    pub plan_version: Option<u64>,
    pub plan_hash: Option<String>,
    pub current_task_ordinal: Option<u32>,
    pub active_worker: Option<ActiveWorkerSnapshot>,
    pub pending_architect_action: Option<PendingArchitectActionSnapshot>,
    pub terminal_detail: Option<String>,
    pub tasks: Vec<TaskStatusSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskStatusSnapshot {
    pub task_key: String,
    pub ordinal: u32,
    pub state: TaskState,
    pub repository_root: String,
    pub task_document_path: String,
    pub design_document_paths: Vec<String>,
    pub task_selector: String,
    pub branch: Option<String>,
    pub review_round: u32,
    pub max_review_rounds: u8,
    pub clarification_rounds_used: u32,
    pub max_clarification_rounds: u8,
    pub clarification_record_count: u32,
    pub base_revision: Option<String>,
    pub head_revision: Option<String>,
    pub developer_session_bound: bool,
    pub reviewer_session_bound: bool,
    pub outcome_detail: Option<String>,
    pub latest_developer_final_path: Option<String>,
    pub final_reviewer_message_paths: Vec<String>,
    pub reviewer_verdict: Option<ReviewerVerdict>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControlErrorBody {
    pub code: ControlErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    InvalidRequest,
    Unauthorized,
    Conflict,
    RequestInProgress,
    NeedsHuman,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolValidationError {
    message: String,
}

impl ProtocolValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ProtocolValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProtocolValidationError {}

pub(crate) fn canonical_action_set(
    actions: impl IntoIterator<Item = ActionName>,
) -> Result<(String, BTreeSet<ActionName>), ProtocolValidationError> {
    let set: BTreeSet<ActionName> = actions.into_iter().collect();
    if set.is_empty() {
        return Err(ProtocolValidationError::new(
            "architect action set must not be empty",
        ));
    }
    let json = serde_json::to_string(&set)
        .map_err(|_| ProtocolValidationError::new("failed to encode action set"))?;
    Ok((json, set))
}

pub(crate) fn parse_canonical_action_set(
    json: &str,
) -> Result<BTreeSet<ActionName>, ProtocolValidationError> {
    let actions: Vec<ActionName> = serde_json::from_str(json)
        .map_err(|_| ProtocolValidationError::new("stored action set is malformed"))?;
    let set: BTreeSet<_> = actions.iter().copied().collect();
    if actions.is_empty() || actions.len() != set.len() {
        return Err(ProtocolValidationError::new(
            "stored action set is not canonical",
        ));
    }
    let canonical = serde_json::to_string(&set)
        .map_err(|_| ProtocolValidationError::new("failed to encode action set"))?;
    if canonical != json {
        return Err(ProtocolValidationError::new(
            "stored action set is not canonical",
        ));
    }
    Ok(set)
}

fn validate_tasks(tasks: &[TaskDraft]) -> Result<(), ProtocolValidationError> {
    if tasks.is_empty() || tasks.len() > MAX_TASKS {
        return Err(ProtocolValidationError::new(
            "ordered task plan must contain between 1 and 64 tasks",
        ));
    }
    let mut keys = BTreeSet::new();
    for task in tasks {
        task.validate()?;
        if !keys.insert(&task.task_key) {
            return Err(ProtocolValidationError::new(
                "task_key values must be unique",
            ));
        }
    }
    Ok(())
}

fn validate_id(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        return Err(ProtocolValidationError::new(format!(
            "{label} is not a bounded opaque identifier"
        )));
    }
    Ok(())
}

fn validate_secret(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    if !(16..=512).contains(&value.len())
        || value
            .chars()
            .any(|character| character.is_control() || is_c1(character))
    {
        return Err(ProtocolValidationError::new(format!(
            "{label} is not a bounded secret"
        )));
    }
    Ok(())
}

fn validate_single_line(
    label: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ProtocolValidationError> {
    validate_free_text(label, value, max_bytes, false)
}

fn validate_free_text(
    label: &str,
    value: &str,
    max_bytes: usize,
    allow_newlines: bool,
) -> Result<(), ProtocolValidationError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(|character| {
            character == '\u{1b}'
                || character == '\r'
                || is_c1(character)
                || (character.is_control() && character != '\n' && character != '\t')
                || (!allow_newlines && matches!(character, '\n' | '\t'))
        })
    {
        return Err(ProtocolValidationError::new(format!(
            "{label} contains invalid or unbounded text"
        )));
    }
    Ok(())
}

fn is_c1(character: char) -> bool {
    ('\u{80}'..='\u{9f}').contains(&character)
}

fn validate_document_path(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    validate_free_text(label, value, MAX_PATH_BYTES, false)?;
    if !Path::new(value).is_absolute() {
        return Err(ProtocolValidationError::new(
            "task and design document paths must be absolute",
        ));
    }
    Ok(())
}

fn validate_repository_root(value: &str) -> Result<(), ProtocolValidationError> {
    validate_free_text("task repository root", value, MAX_PATH_BYTES, false)?;
    let path = Path::new(value);
    if !path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(ProtocolValidationError::new(
            "task repository root must be absolute and lexically normalized",
        ));
    }
    Ok(())
}

fn validate_hash(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ProtocolValidationError::new(format!(
            "{label} must be a lowercase sha256 digest"
        )));
    }
    Ok(())
}

fn validate_list_len<T>(label: &str, values: &[T]) -> Result<(), ProtocolValidationError> {
    if values.len() > MAX_LIST_ITEMS {
        return Err(ProtocolValidationError::new(format!(
            "{label} exceeds its bounded item count"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(key: &str) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: format!("Task {key}"),
            repository_root: "/source/example".into(),
            task_document_path: format!("/project/tasks/{key}.md"),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: key.into(),
            max_review_rounds: 2,
            max_clarification_rounds: 2,
        }
    }

    #[test]
    fn ordered_plan_is_strict_and_bounded() {
        let valid = ControlAction::SessionPlanReplace {
            expected_session_version: 0,
            developer_adapter: "codex-developer".into(),
            reviewer_adapter: "codex-reviewer".into(),
            tasks: vec![task("one"), task("two")],
        };
        assert!(valid.validate().is_ok());

        let mut duplicate = valid.clone();
        let ControlAction::SessionPlanReplace { tasks, .. } = &mut duplicate else {
            unreachable!()
        };
        tasks[1].task_key = "one".into();
        assert!(duplicate.validate().is_err());

        let mut traversal = valid;
        let ControlAction::SessionPlanReplace { tasks, .. } = &mut traversal else {
            unreachable!()
        };
        tasks[0].task_key = "..".into();
        assert!(traversal.validate().is_err());

        let mut relative_repository = task("relative-repository");
        relative_repository.repository_root = "src/repository".into();
        assert!(relative_repository.validate().is_err());
        relative_repository.repository_root = "/source/../repository".into();
        assert!(relative_repository.validate().is_err());
    }

    #[test]
    fn task_contract_accepts_only_bounded_absolute_document_paths_and_selector() {
        let mut candidate = task("file-backed");
        assert!(candidate.validate().is_ok());

        candidate.task_document_path = "tasks/current.md".into();
        assert!(candidate.validate().is_err());
        candidate.task_document_path = "/project/tasks/current.md".into();
        candidate.design_document_paths = vec!["design.md".into()];
        assert!(candidate.validate().is_err());
        candidate.design_document_paths =
            vec!["/project/design.md".into(), "/project/design.md".into()];
        assert!(candidate.validate().is_err());
        candidate.design_document_paths = Vec::new();
        candidate.task_selector = String::new();
        assert!(candidate.validate().is_err());
        candidate.task_selector = "FBTC-01\nhidden".into();
        assert!(candidate.validate().is_err());
        candidate.task_selector = "FBTC-01".into();
        candidate.max_clarification_rounds = 0;
        assert!(candidate.validate().is_err());
        candidate.max_clarification_rounds = 21;
        assert!(candidate.validate().is_err());
    }

    #[test]
    fn document_paths_are_shape_checked_without_filesystem_preflight() {
        let mut candidate = task("missing-files-are-worker-concerns");
        candidate.task_document_path = "/definitely/not/present/../current_todo.md".into();
        candidate.design_document_paths =
            vec!["/also/not/present/file_backed_task_contract.md".into()];
        assert!(candidate.validate().is_ok());
    }

    #[test]
    fn start_requires_exact_positive_plan_and_explicit_confirmation() {
        let action = ControlAction::SessionApproveAndStart {
            expected_session_version: 1,
            plan_version: 1,
            plan_hash: "a".repeat(64),
            approval_confirmed: true,
        };
        assert!(action.validate().is_ok());
        let mut encoded = serde_json::to_value(&action).unwrap();
        encoded["approval_confirmed"] = false.into();
        let action: ControlAction = serde_json::from_value(encoded).unwrap();
        assert!(action.validate().is_err());
    }

    #[test]
    fn clarification_actions_require_exact_bounded_identity_fields() {
        let submit = ControlAction::SessionClarificationSubmit {
            expected_session_version: 8,
            task_ordinal: 0,
            task_key: "task-one".into(),
            action_sequence: 1,
            developer_request_path: "/artifacts/developer/request.md".into(),
            clarification_document_path: "/project/hcom-tasks/run/task-one/clarification/turn-1.md"
                .into(),
            human_decision_confirmed: false,
        };
        assert!(submit.validate().is_ok());
        let mut zero_sequence = submit.clone();
        let ControlAction::SessionClarificationSubmit {
            action_sequence, ..
        } = &mut zero_sequence
        else {
            unreachable!()
        };
        *action_sequence = 0;
        assert!(zero_sequence.validate().is_err());

        let require_human = ControlAction::SessionClarificationRequireHuman {
            expected_session_version: 8,
            task_ordinal: 0,
            task_key: "task-one".into(),
            action_sequence: 1,
            developer_request_path: "/artifacts/developer/request.md".into(),
        };
        assert!(require_human.validate().is_ok());

        let page = ControlAction::SessionClarificationsList {
            task_ordinal: 0,
            task_key: "task-one".into(),
            after_sequence: 0,
            limit: MAX_CLARIFICATION_PAGE_RECORDS,
        };
        assert!(page.validate().is_ok());
        let mut oversized_page = page;
        let ControlAction::SessionClarificationsList { limit, .. } = &mut oversized_page else {
            unreachable!()
        };
        *limit = MAX_CLARIFICATION_PAGE_RECORDS + 1;
        assert!(oversized_page.validate().is_err());
    }

    #[test]
    fn previous_protocol_version_fails_closed() {
        assert_eq!(PROTOCOL_VERSION, 5);
        let request = ControlRequest {
            protocol_version: 4,
            request_id: "v4-request".into(),
            caller: CallerAuth::Human {
                process_birth: "123:456".into(),
            },
            action: ControlAction::SessionStatus,
        };
        assert!(request.validate().is_err());

        let response = ControlResponse {
            protocol_version: 4,
            request_id: "v4-response".into(),
            ok: false,
            result: None,
            error: Some(ControlErrorBody {
                code: ControlErrorCode::InvalidRequest,
                message: "old binary".into(),
            }),
        };
        assert!(response.validate().is_err());
    }

    #[test]
    fn public_action_inventory_contains_only_session_tools() {
        let names: Vec<_> = ActionName::ALL
            .into_iter()
            .map(ActionName::as_str)
            .collect();
        assert_eq!(
            names,
            [
                "session_plan_replace",
                "session_approve_and_start",
                "session_clarification_submit",
                "session_clarification_require_human",
                "session_clarifications_list",
                "session_wait",
                "session_status",
                "session_cancel",
            ]
        );
        assert!(names.iter().all(|name| !name.contains("project")));
    }
}
