use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path};

pub const PROTOCOL_VERSION: u32 = 2;
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;

const MAX_ID_BYTES: usize = 128;
const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_PATH_BYTES: usize = 4096;
const MAX_TASKS: usize = 64;
const MAX_LIST_ITEMS: usize = 256;

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
        native_session_id: Option<String>,
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
                native_session_id,
            } => {
                validate_id("binding_id", binding_id)?;
                validate_secret("launch_nonce", launch_nonce)?;
                validate_secret("capability", capability)?;
                if let Some(native_session_id) = native_session_id {
                    validate_single_line("native_session_id", native_session_id, 256)?;
                }
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
    SessionStatus,
    SessionCancel,
}

impl ActionName {
    pub const ARCHITECT: [Self; 4] = [
        Self::SessionPlanReplace,
        Self::SessionApproveAndStart,
        Self::SessionStatus,
        Self::SessionCancel,
    ];
    pub const ALL: [Self; 4] = Self::ARCHITECT;

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionPlanReplace => "session_plan_replace",
            Self::SessionApproveAndStart => "session_approve_and_start",
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
            Self::SessionStatus => Ok(()),
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
    pub objective: String,
    pub repository_root: String,
    pub acceptance_criteria: Vec<String>,
    pub required_checks: Vec<String>,
    pub allowed_paths: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub max_review_rounds: u8,
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
        // Objectives are the one plan field that must preserve human-authored
        // multi-line instructions (for example, exact file contents). The
        // remaining task fields stay single-line list entries.
        validate_free_text("task objective", &self.objective, MAX_TEXT_BYTES, true)?;
        for (label, values, max_bytes) in [
            (
                "acceptance criterion",
                &self.acceptance_criteria,
                MAX_TEXT_BYTES,
            ),
            ("required check", &self.required_checks, 4096),
            ("forbidden action", &self.forbidden_actions, 4096),
        ] {
            validate_list_len(label, values)?;
            let mut unique = BTreeSet::new();
            for value in values {
                validate_free_text(label, value, max_bytes, false)?;
                if !unique.insert(value) {
                    return Err(ProtocolValidationError::new(format!(
                        "{label} entries must be unique"
                    )));
                }
            }
        }
        validate_list_len("allowed path", &self.allowed_paths)?;
        let mut allowed_paths = BTreeSet::new();
        for path in &self.allowed_paths {
            validate_task_path(path)?;
            if !allowed_paths.insert(path) {
                return Err(ProtocolValidationError::new(
                    "allowed path entries must be unique",
                ));
            }
        }
        if !(1..=20).contains(&self.max_review_rounds) {
            return Err(ProtocolValidationError::new(
                "max_review_rounds must be between 1 and 20",
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
    Reviewing,
    Lgtm,
    ReviewExhausted,
    NeedsHuman,
    Failed,
    Canceled,
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
    pub branch: Option<String>,
    pub review_round: u32,
    pub max_review_rounds: u8,
    pub base_revision: Option<String>,
    pub head_revision: Option<String>,
    pub developer_session_bound: bool,
    pub reviewer_session_bound: bool,
    pub outcome_detail: Option<String>,
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

fn validate_task_path(value: &str) -> Result<(), ProtocolValidationError> {
    validate_free_text("task path", value, MAX_PATH_BYTES, false)?;
    let path = Path::new(value);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(ProtocolValidationError::new(
            "task path must be workspace-relative and normalized",
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
            objective: format!("Complete {key}"),
            repository_root: "/source/example".into(),
            acceptance_criteria: vec!["the bounded behavior works".into()],
            required_checks: vec!["cargo test".into()],
            allowed_paths: vec!["src".into()],
            forbidden_actions: vec!["do not push".into()],
            max_review_rounds: 2,
        }
    }

    #[test]
    fn ordered_plan_is_strict_and_bounded() {
        let valid = ControlAction::SessionPlanReplace {
            expected_session_version: 0,
            developer_adapter: "codex-developer-0.145.0".into(),
            reviewer_adapter: "codex-reviewer-0.145.0".into(),
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
    fn task_objective_preserves_bounded_multiline_instructions_only() {
        let mut multiline = task("multiline");
        multiline.objective =
            "Create task.txt with exactly two lines:\nphase9-task\nstatus: complete".into();
        assert!(multiline.validate().is_ok());

        multiline.objective.push('\r');
        assert!(multiline.validate().is_err());
        multiline.objective =
            "Create task.txt with exactly two lines:\nphase9-task\nstatus: complete".into();
        multiline.title = "Task\nmultiline".into();
        assert!(multiline.validate().is_err());
        multiline.title = "Task multiline".into();
        multiline.acceptance_criteria = vec!["first line\nsecond line".into()];
        assert!(multiline.validate().is_err());
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
                "session_status",
                "session_cancel",
            ]
        );
        assert!(names.iter().all(|name| !name.contains("project")));
    }
}
