use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;

const MAX_ID_BYTES: usize = 128;
const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_PATH_BYTES: usize = 4096;
const MAX_TASKS: usize = 256;
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
    ProjectCreate,
    ProjectGet,
    ProjectPlanReplace,
    ProjectApprove,
    ProjectRun,
    ProjectWait,
    ProjectStatus,
    ProjectLogs,
    ProjectPause,
    ProjectResume,
    ProjectCancel,
    ProjectAnswer,
    ProjectAbandonForReplan,
}

impl ActionName {
    pub const ALL: [Self; 13] = [
        Self::ProjectCreate,
        Self::ProjectGet,
        Self::ProjectPlanReplace,
        Self::ProjectApprove,
        Self::ProjectRun,
        Self::ProjectWait,
        Self::ProjectStatus,
        Self::ProjectLogs,
        Self::ProjectPause,
        Self::ProjectResume,
        Self::ProjectCancel,
        Self::ProjectAnswer,
        Self::ProjectAbandonForReplan,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCreate => "project_create",
            Self::ProjectGet => "project_get",
            Self::ProjectPlanReplace => "project_plan_replace",
            Self::ProjectApprove => "project_approve",
            Self::ProjectRun => "project_run",
            Self::ProjectWait => "project_wait",
            Self::ProjectStatus => "project_status",
            Self::ProjectLogs => "project_logs",
            Self::ProjectPause => "project_pause",
            Self::ProjectResume => "project_resume",
            Self::ProjectCancel => "project_cancel",
            Self::ProjectAnswer => "project_answer",
            Self::ProjectAbandonForReplan => "project_abandon_for_replan",
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    ProjectCreate {
        repo_root: String,
        target_ref: String,
    },
    ProjectGet {
        project_id: String,
    },
    ProjectPlanReplace {
        project_id: String,
        expected_project_version: u64,
        base_checkpoint_sha: String,
        developer_profile: Box<WorkerProfileDraft>,
        reviewer_profile: Box<WorkerProfileDraft>,
        tasks: Vec<TaskDraft>,
        automatic_through_ordinal: Option<u32>,
    },
    ProjectApprove {
        project_id: String,
        expected_project_version: u64,
        plan_version: u64,
        plan_hash: String,
    },
    ProjectRun {
        project_id: String,
        expected_project_version: u64,
        plan_version: u64,
        plan_hash: String,
    },
    ProjectWait {
        project_id: String,
        after_project_version: u64,
        max_wait_ms: u32,
    },
    ProjectStatus {
        project_id: String,
    },
    ProjectLogs {
        project_id: String,
        task_id: Option<String>,
        role: Option<WorkerRole>,
        turn_sequence: Option<u32>,
        after_activity_sequence: Option<u64>,
        limit: u32,
        follow: bool,
    },
    ProjectPause {
        project_id: String,
        expected_project_version: u64,
        reason: String,
    },
    ProjectResume {
        project_id: String,
        expected_project_version: u64,
    },
    ProjectCancel {
        project_id: String,
        expected_project_version: u64,
        reason: String,
    },
    ProjectAnswer {
        project_id: String,
        task_id: String,
        expected_project_version: u64,
        expected_task_version: u64,
        answer: String,
    },
    ProjectAbandonForReplan {
        project_id: String,
        task_id: String,
        expected_project_version: u64,
        expected_task_version: u64,
        archive_manifest_hash: String,
    },
}

impl ControlAction {
    pub fn name(&self) -> ActionName {
        match self {
            Self::ProjectCreate { .. } => ActionName::ProjectCreate,
            Self::ProjectGet { .. } => ActionName::ProjectGet,
            Self::ProjectPlanReplace { .. } => ActionName::ProjectPlanReplace,
            Self::ProjectApprove { .. } => ActionName::ProjectApprove,
            Self::ProjectRun { .. } => ActionName::ProjectRun,
            Self::ProjectWait { .. } => ActionName::ProjectWait,
            Self::ProjectStatus { .. } => ActionName::ProjectStatus,
            Self::ProjectLogs { .. } => ActionName::ProjectLogs,
            Self::ProjectPause { .. } => ActionName::ProjectPause,
            Self::ProjectResume { .. } => ActionName::ProjectResume,
            Self::ProjectCancel { .. } => ActionName::ProjectCancel,
            Self::ProjectAnswer { .. } => ActionName::ProjectAnswer,
            Self::ProjectAbandonForReplan { .. } => ActionName::ProjectAbandonForReplan,
        }
    }

    pub fn project_id(&self) -> Option<&str> {
        match self {
            Self::ProjectCreate { .. } => None,
            Self::ProjectGet { project_id }
            | Self::ProjectPlanReplace { project_id, .. }
            | Self::ProjectApprove { project_id, .. }
            | Self::ProjectRun { project_id, .. }
            | Self::ProjectWait { project_id, .. }
            | Self::ProjectStatus { project_id }
            | Self::ProjectLogs { project_id, .. }
            | Self::ProjectPause { project_id, .. }
            | Self::ProjectResume { project_id, .. }
            | Self::ProjectCancel { project_id, .. }
            | Self::ProjectAnswer { project_id, .. }
            | Self::ProjectAbandonForReplan { project_id, .. } => Some(project_id),
        }
    }

    fn validate(&self) -> Result<(), ProtocolValidationError> {
        match self {
            Self::ProjectCreate {
                repo_root,
                target_ref,
            } => {
                validate_absolute_path("repo_root", repo_root)?;
                validate_git_ref(target_ref)
            }
            Self::ProjectGet { project_id } | Self::ProjectStatus { project_id } => {
                validate_id("project_id", project_id)
            }
            Self::ProjectPlanReplace {
                project_id,
                base_checkpoint_sha,
                developer_profile,
                reviewer_profile,
                tasks,
                automatic_through_ordinal,
                ..
            } => {
                validate_id("project_id", project_id)?;
                validate_git_sha("base_checkpoint_sha", base_checkpoint_sha)?;
                developer_profile.validate(WorkerRole::Developer)?;
                reviewer_profile.validate(WorkerRole::Reviewer)?;
                validate_tasks(tasks)?;
                if let Some(ordinal) = automatic_through_ordinal
                    && usize::try_from(*ordinal).unwrap_or(usize::MAX) >= tasks.len()
                {
                    return Err(ProtocolValidationError::new(
                        "automatic_through_ordinal is outside the task plan",
                    ));
                }
                Ok(())
            }
            Self::ProjectApprove {
                project_id,
                plan_version,
                plan_hash,
                ..
            }
            | Self::ProjectRun {
                project_id,
                plan_version,
                plan_hash,
                ..
            } => {
                validate_id("project_id", project_id)?;
                if *plan_version == 0 {
                    return Err(ProtocolValidationError::new(
                        "plan_version must be positive",
                    ));
                }
                validate_hash("plan_hash", plan_hash)
            }
            Self::ProjectWait {
                project_id,
                max_wait_ms,
                ..
            } => {
                validate_id("project_id", project_id)?;
                if !(1..=300_000).contains(max_wait_ms) {
                    return Err(ProtocolValidationError::new(
                        "max_wait_ms must be between 1 and 300000",
                    ));
                }
                Ok(())
            }
            Self::ProjectLogs {
                project_id,
                task_id,
                turn_sequence,
                limit,
                ..
            } => {
                validate_id("project_id", project_id)?;
                if let Some(task_id) = task_id {
                    validate_id("task_id", task_id)?;
                }
                if turn_sequence == &Some(0) {
                    return Err(ProtocolValidationError::new(
                        "turn_sequence must be positive",
                    ));
                }
                if !(1..=1000).contains(limit) {
                    return Err(ProtocolValidationError::new(
                        "log limit must be between 1 and 1000",
                    ));
                }
                Ok(())
            }
            Self::ProjectPause {
                project_id, reason, ..
            }
            | Self::ProjectCancel {
                project_id, reason, ..
            } => {
                validate_id("project_id", project_id)?;
                validate_free_text("reason", reason, 4096, false)
            }
            Self::ProjectResume { project_id, .. } => validate_id("project_id", project_id),
            Self::ProjectAnswer {
                project_id,
                task_id,
                answer,
                ..
            } => {
                validate_id("project_id", project_id)?;
                validate_id("task_id", task_id)?;
                validate_free_text("answer", answer, MAX_TEXT_BYTES, false)
            }
            Self::ProjectAbandonForReplan {
                project_id,
                task_id,
                archive_manifest_hash,
                ..
            } => {
                validate_id("project_id", project_id)?;
                validate_id("task_id", task_id)?;
                validate_hash("archive_manifest_hash", archive_manifest_hash)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
pub struct WorkerProfileDraft {
    pub adapter: String,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub cli_path: String,
    pub cli_version: String,
    pub adapter_contract_version: u32,
    pub native_session_mode: NativeSessionMode,
    pub capability: CapabilitySnapshot,
}

impl WorkerProfileDraft {
    fn validate(&self, _expected_role: WorkerRole) -> Result<(), ProtocolValidationError> {
        validate_single_line("adapter", &self.adapter, 64)?;
        validate_single_line("model", &self.model, 256)?;
        validate_single_line("reasoning", &self.reasoning, 64)?;
        validate_single_line("policy", &self.policy, 2048)?;
        validate_absolute_path("cli_path", &self.cli_path)?;
        validate_single_line("cli_version", &self.cli_version, 128)?;
        if self.adapter_contract_version == 0 {
            return Err(ProtocolValidationError::new(
                "adapter_contract_version must be positive",
            ));
        }
        self.capability.validate()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapabilitySnapshot {
    pub contract_hash: String,
    pub features: Vec<String>,
}

impl CapabilitySnapshot {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskDraft {
    pub task_key: String,
    pub title: String,
    pub objective: String,
    pub dependencies: Vec<String>,
    pub acceptance_criteria: Vec<String>,
    pub required_checks: Vec<String>,
    pub allowed_paths: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub max_review_rounds: u8,
    pub context_refs: Vec<ContextRef>,
}

impl TaskDraft {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        validate_id("task_key", &self.task_key)?;
        validate_free_text("task title", &self.title, 512, false)?;
        validate_free_text("task objective", &self.objective, MAX_TEXT_BYTES, false)?;
        for (label, values, max_bytes) in [
            ("dependency", &self.dependencies, MAX_ID_BYTES),
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
                if label == "dependency" {
                    validate_id(label, value)?;
                } else {
                    validate_free_text(label, value, max_bytes, false)?;
                }
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
        if self.context_refs.len() > MAX_LIST_ITEMS {
            return Err(ProtocolValidationError::new("too many context references"));
        }
        for context in &self.context_refs {
            context.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextRef {
    pub kind: ContextKind,
    pub task_id: String,
    pub digest: String,
}

impl ContextRef {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        validate_id("context task_id", &self.task_id)?;
        validate_hash("context digest", &self.digest)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    TaskResult,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlResult {
    ProtocolAccepted { action: ActionName, spawned: bool },
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
    NeedsRecovery,
    NotImplemented,
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
    let (canonical, set) = canonical_action_set(actions)?;
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
            "task plan must contain between 1 and 256 tasks",
        ));
    }
    let mut task_indexes = BTreeMap::new();
    for (index, task) in tasks.iter().enumerate() {
        task.validate()?;
        if task_indexes.insert(task.task_key.as_str(), index).is_some() {
            return Err(ProtocolValidationError::new("task_key must be unique"));
        }
    }
    for task in tasks {
        for dependency in &task.dependencies {
            if !task_indexes.contains_key(dependency.as_str()) {
                return Err(ProtocolValidationError::new(
                    "task dependency does not exist in this plan",
                ));
            }
        }
        for context in &task.context_refs {
            if !task_indexes.contains_key(context.task_id.as_str()) {
                return Err(ProtocolValidationError::new(
                    "task context does not exist in this plan",
                ));
            }
        }
    }
    let mut colors = vec![0u8; tasks.len()];
    for index in 0..tasks.len() {
        visit_task(index, tasks, &task_indexes, &mut colors)?;
    }
    Ok(())
}

fn visit_task(
    index: usize,
    tasks: &[TaskDraft],
    task_indexes: &BTreeMap<&str, usize>,
    colors: &mut [u8],
) -> Result<(), ProtocolValidationError> {
    match colors[index] {
        1 => {
            return Err(ProtocolValidationError::new(
                "task dependency graph is cyclic",
            ));
        }
        2 => return Ok(()),
        _ => {}
    }
    colors[index] = 1;
    for dependency in &tasks[index].dependencies {
        visit_task(
            task_indexes[dependency.as_str()],
            tasks,
            task_indexes,
            colors,
        )?;
    }
    colors[index] = 2;
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
            "{label} has an invalid secret shape"
        )));
    }
    Ok(())
}

fn validate_single_line(
    label: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ProtocolValidationError> {
    validate_free_text(label, value, max_bytes, true)
}

fn validate_free_text(
    label: &str,
    value: &str,
    max_bytes: usize,
    single_line: bool,
) -> Result<(), ProtocolValidationError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(ProtocolValidationError::new(format!(
            "{label} must contain between 1 and {max_bytes} bytes"
        )));
    }
    if value.chars().any(|character| {
        character == '\u{1b}'
            || character == '\r'
            || is_c1(character)
            || (character.is_control() && character != '\n' && character != '\t')
            || (single_line && matches!(character, '\n' | '\t'))
    }) {
        return Err(ProtocolValidationError::new(format!(
            "{label} contains forbidden control bytes"
        )));
    }
    Ok(())
}

fn is_c1(character: char) -> bool {
    ('\u{80}'..='\u{9f}').contains(&character)
}

fn validate_absolute_path(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    validate_single_line(label, value, MAX_PATH_BYTES)?;
    if !Path::new(value).is_absolute() {
        return Err(ProtocolValidationError::new(format!(
            "{label} must be absolute"
        )));
    }
    Ok(())
}

fn validate_task_path(value: &str) -> Result<(), ProtocolValidationError> {
    validate_single_line("allowed path", value, MAX_PATH_BYTES)?;
    let path = Path::new(value);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return Err(ProtocolValidationError::new(
            "allowed path must be a safe workspace-relative path",
        ));
    }
    Ok(())
}

fn validate_git_ref(value: &str) -> Result<(), ProtocolValidationError> {
    validate_single_line("target_ref", value, 1024)?;
    if !value.starts_with("refs/")
        || value.contains("..")
        || value.contains("@{")
        || value.contains("//")
        || value.ends_with('/')
        || value.ends_with('.')
        || value.ends_with(".lock")
        || value.bytes().any(|byte| {
            byte.is_ascii_whitespace() || byte < 0x20 || byte == 0x7f || b"~^:?*[\\".contains(&byte)
        })
    {
        return Err(ProtocolValidationError::new(
            "target_ref must be a full safe refs/... name",
        ));
    }
    Ok(())
}

fn validate_git_sha(label: &str, value: &str) -> Result<(), ProtocolValidationError> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ProtocolValidationError::new(format!(
            "{label} must be a full lowercase Git object ID"
        )));
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
            "too many {label} entries"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }

    fn profile() -> WorkerProfileDraft {
        WorkerProfileDraft {
            adapter: "fake".into(),
            model: "fake-model".into(),
            reasoning: "high".into(),
            policy: "sandboxed".into(),
            cli_path: "/bin/false".into(),
            cli_version: "1".into(),
            adapter_contract_version: 1,
            native_session_mode: NativeSessionMode::Discovered,
            capability: CapabilitySnapshot {
                contract_hash: digest('a'),
                features: vec!["structured-result".into()],
            },
        }
    }

    fn task(key: &str, dependencies: &[&str]) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: key.into(),
            objective: format!("implement {key}"),
            dependencies: dependencies.iter().map(|value| (*value).into()).collect(),
            acceptance_criteria: vec!["observable result".into()],
            required_checks: vec!["cargo test".into()],
            allowed_paths: vec!["src/".into()],
            forbidden_actions: vec!["push".into()],
            max_review_rounds: 5,
            context_refs: vec![],
        }
    }

    #[test]
    fn every_required_action_has_a_stable_wire_name() {
        let names: Vec<&str> = ActionName::ALL
            .into_iter()
            .map(ActionName::as_str)
            .collect();
        assert_eq!(
            names,
            vec![
                "project_create",
                "project_get",
                "project_plan_replace",
                "project_approve",
                "project_run",
                "project_wait",
                "project_status",
                "project_logs",
                "project_pause",
                "project_resume",
                "project_cancel",
                "project_answer",
                "project_abandon_for_replan",
            ]
        );
        let encoded = serde_json::to_string(&ActionName::ALL).unwrap();
        for name in names {
            assert!(encoded.contains(name));
        }
    }

    #[test]
    fn every_required_action_shape_validates_and_round_trips() {
        let project_id = "project-1".to_string();
        let task_id = "task-1".to_string();
        let plan_hash = digest('b');
        let actions = vec![
            ControlAction::ProjectCreate {
                repo_root: "/repo".into(),
                target_ref: "refs/heads/master".into(),
            },
            ControlAction::ProjectGet {
                project_id: project_id.clone(),
            },
            ControlAction::ProjectPlanReplace {
                project_id: project_id.clone(),
                expected_project_version: 0,
                base_checkpoint_sha: std::iter::repeat_n('1', 40).collect(),
                developer_profile: Box::new(profile()),
                reviewer_profile: Box::new(profile()),
                tasks: vec![task("task-1", &[])],
                automatic_through_ordinal: Some(0),
            },
            ControlAction::ProjectApprove {
                project_id: project_id.clone(),
                expected_project_version: 1,
                plan_version: 1,
                plan_hash: plan_hash.clone(),
            },
            ControlAction::ProjectRun {
                project_id: project_id.clone(),
                expected_project_version: 2,
                plan_version: 1,
                plan_hash,
            },
            ControlAction::ProjectWait {
                project_id: project_id.clone(),
                after_project_version: 2,
                max_wait_ms: 30_000,
            },
            ControlAction::ProjectStatus {
                project_id: project_id.clone(),
            },
            ControlAction::ProjectLogs {
                project_id: project_id.clone(),
                task_id: Some(task_id.clone()),
                role: Some(WorkerRole::Developer),
                turn_sequence: Some(1),
                after_activity_sequence: Some(0),
                limit: 100,
                follow: false,
            },
            ControlAction::ProjectPause {
                project_id: project_id.clone(),
                expected_project_version: 3,
                reason: "human pause".into(),
            },
            ControlAction::ProjectResume {
                project_id: project_id.clone(),
                expected_project_version: 4,
            },
            ControlAction::ProjectCancel {
                project_id: project_id.clone(),
                expected_project_version: 5,
                reason: "human cancel".into(),
            },
            ControlAction::ProjectAnswer {
                project_id: project_id.clone(),
                task_id: task_id.clone(),
                expected_project_version: 6,
                expected_task_version: 2,
                answer: "bounded answer".into(),
            },
            ControlAction::ProjectAbandonForReplan {
                project_id,
                task_id,
                expected_project_version: 7,
                expected_task_version: 3,
                archive_manifest_hash: digest('c'),
            },
        ];
        let mut names = BTreeSet::new();
        for (index, action) in actions.into_iter().enumerate() {
            names.insert(action.name());
            let request = ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: format!("req-{index}"),
                caller: CallerAuth::Human {
                    process_birth: "linux:boot:1".into(),
                },
                action,
            };
            request.validate().unwrap();
            let encoded = serde_json::to_vec(&request).unwrap();
            assert!(encoded.len() <= MAX_REQUEST_BYTES);
            assert!(serde_json::from_slice::<ControlRequest>(&encoded).unwrap() == request);
        }
        assert_eq!(names, ActionName::ALL.into_iter().collect());
    }

    #[test]
    fn plan_shape_rejects_cycles_and_controls() {
        let mut cyclic = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".into(),
            caller: CallerAuth::Human {
                process_birth: "linux:boot:1".into(),
            },
            action: ControlAction::ProjectPlanReplace {
                project_id: "project-1".into(),
                expected_project_version: 0,
                base_checkpoint_sha: std::iter::repeat_n('1', 40).collect(),
                developer_profile: Box::new(profile()),
                reviewer_profile: Box::new(profile()),
                tasks: vec![task("a", &["b"]), task("b", &["a"])],
                automatic_through_ordinal: None,
            },
        };
        assert!(
            cyclic
                .validate()
                .unwrap_err()
                .to_string()
                .contains("cyclic")
        );
        if let ControlAction::ProjectPlanReplace { tasks, .. } = &mut cyclic.action {
            tasks[0].dependencies.clear();
            tasks[1].dependencies.clear();
            tasks[0].objective = "bad\u{1b}]0;title".into();
        }
        assert!(
            cyclic
                .validate()
                .unwrap_err()
                .to_string()
                .contains("control")
        );
        if let ControlAction::ProjectPlanReplace { tasks, .. } = &mut cyclic.action {
            tasks[0].objective = "valid objective".into();
            tasks[0].allowed_paths = vec!["../escape".into()];
        }
        assert!(
            cyclic
                .validate()
                .unwrap_err()
                .to_string()
                .contains("workspace-relative")
        );
    }

    #[test]
    fn canonical_action_set_rejects_duplicates_or_order_drift() {
        let (json, set) =
            canonical_action_set([ActionName::ProjectGet, ActionName::ProjectStatus]).unwrap();
        assert_eq!(parse_canonical_action_set(&json).unwrap(), set);
        assert!(parse_canonical_action_set(r#"["project_status","project_get"]"#).is_err());
        assert!(parse_canonical_action_set(r#"["project_get","project_get"]"#).is_err());
    }

    #[test]
    fn decoded_response_controls_and_invalid_envelopes_are_rejected() {
        let mut response = ControlResponse::error(
            "req-1",
            ControlErrorCode::InvalidRequest,
            "bad\u{1b}]0;title",
        );
        assert!(response.validate().is_err());
        response.error.as_mut().unwrap().message = "bounded error".into();
        response.ok = true;
        assert!(response.validate().is_err());
    }
}
