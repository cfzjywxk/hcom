//! Foreground, in-memory task supervisor for one `hcom architect` invocation.

use crate::artifact::{
    ArtifactAttempt, ArtifactRoot, ArtifactScope, ManifestMetadata, TurnManifest,
};
use crate::control_api::{
    NativeSessionMode, SessionState, SessionStatusSnapshot, TaskDraft, TaskState,
    TaskStatusSnapshot, WorkerRole,
};
use crate::worker::codex::{
    CodexDeveloperAdapter, CodexDeveloperConfig, GIT_EXECUTABLE, GIT_VERSION,
};
use crate::worker::contract::{
    NativeObservation, NativeResult, TurnControl, WorkerAdapter, WorkerAdapterRegistry,
    WorkerProfile,
};
use crate::worker::environment::{
    EnvironmentPolicy, ExecutionEnvironmentLease, ParentEnvironment, WorkerEnvironmentIdentity,
};
use crate::worker::process::{
    HeartbeatControl, ProcessCompletion, ProcessRunner, WorkerExit, WorkerTermination,
};
use crate::worker::profile::{
    CLAUDE_DEVELOPER_ADAPTER, CLAUDE_REVIEWER_ADAPTER, CODEX_DEVELOPER_ADAPTER,
    CODEX_REVIEWER_ADAPTER, SessionInvocationProfiles,
};
use crate::worker::result::{
    CheckResult, CheckStatus, DeveloperDecision, DeveloperResult, ReviewDecision, ReviewerResult,
};
use crate::worker::reviewer::{
    ClaudeDeveloperAdapter, ClaudeDeveloperConfig, ClaudeReviewerAdapter, ClaudeReviewerConfig,
    CodexReviewerAdapter, CodexReviewerConfig, claude_auth_redaction_values,
    validate_claude_auth_readiness,
};
use crate::worker::{ExecutableIdentity, prepare_create_turn, prepare_resume_turn};
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, TryRecvError},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const MAX_WORKER_ATTEMPTS: u32 = 3;
const MAX_GIT_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_STATUS_OUTCOME_BYTES: usize = 1024;
const GIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionStartup {
    pub(crate) run_id: String,
    pub(crate) project_root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct SessionRuntimeSources {
    parent_environment: ParentEnvironment,
    codex_auth_source: Option<PathBuf>,
    claude_auth_source: Option<PathBuf>,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
    host_runtime_dir: PathBuf,
    profiles: Option<SessionInvocationProfiles>,
}

struct WorkerEnvironmentPaths {
    home: PathBuf,
    native_config: PathBuf,
    temp: PathBuf,
    runtime: PathBuf,
    xdg_config: PathBuf,
    xdg_state: PathBuf,
    xdg_cache: PathBuf,
    xdg_data: PathBuf,
}

impl SessionRuntimeSources {
    pub(crate) fn capture(
        parent_environment: impl Into<ParentEnvironment>,
        host_runtime_dir: PathBuf,
        profiles: SessionInvocationProfiles,
    ) -> Result<Self> {
        profiles.validate()?;
        let parent_environment = parent_environment.into();
        let home = parent_environment
            .unicode("HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("session worker environment requires non-empty parent HOME"))?;
        let host_runtime_dir =
            canonical_private_directory(&host_runtime_dir, "host XDG runtime directory")?;
        let codex_home = parent_environment
            .unicode("CODEX_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        let claude_home = parent_environment
            .unicode("CLAUDE_CONFIG_DIR")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"));
        let uses_codex =
            profiles.developer.codex().is_some() || profiles.reviewer.codex().is_some();
        let uses_claude =
            profiles.developer.claude().is_some() || profiles.reviewer.claude().is_some();
        let codex_auth_source = if uses_codex {
            Some(canonical_private_file(
                &codex_home.join("auth.json"),
                "Codex auth source",
            )?)
        } else {
            None
        };
        let claude_auth_source = if uses_claude {
            let claude_auth_path = claude_home.join(".credentials.json");
            match fs::symlink_metadata(&claude_auth_path) {
                Ok(_) => Some(canonical_private_file(
                    &claude_auth_path,
                    "Claude auth source",
                )?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error).context("failed to inspect Claude auth source"),
            }
        } else {
            None
        };
        let cargo_bin_source = parent_environment
            .unicode("CARGO_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cargo"))
            .join("bin");
        let rustup_home_source = parent_environment
            .unicode("RUSTUP_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".rustup"));
        let cargo_bin_source =
            canonical_readable_directory(&cargo_bin_source, "Rust cargo-bin source")?;
        let rustup_home_source =
            canonical_readable_directory(&rustup_home_source, "Rust rustup source")?;
        Ok(Self {
            parent_environment,
            codex_auth_source,
            claude_auth_source,
            cargo_bin_source,
            rustup_home_source,
            host_runtime_dir,
            profiles: Some(profiles),
        })
    }

    #[cfg(test)]
    pub(crate) fn fake(path: &Path) -> Self {
        Self {
            parent_environment: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]).into(),
            codex_auth_source: None,
            claude_auth_source: None,
            cargo_bin_source: path.to_owned(),
            rustup_home_source: path.to_owned(),
            host_runtime_dir: path.to_owned(),
            profiles: None,
        }
    }

    fn environment_for(
        &self,
        adapter: &str,
        epoch: &str,
        run_id: &str,
        task_id: &str,
        paths: &WorkerEnvironmentPaths,
    ) -> Result<ExecutionEnvironmentLease> {
        let policy = match adapter {
            CODEX_DEVELOPER_ADAPTER => CodexDeveloperAdapter::environment_policy()?,
            CLAUDE_DEVELOPER_ADAPTER => ClaudeDeveloperAdapter::environment_policy()?,
            CODEX_REVIEWER_ADAPTER => CodexReviewerAdapter::environment_policy()?,
            CLAUDE_REVIEWER_ADAPTER => ClaudeReviewerAdapter::environment_policy()?,
            _ => EnvironmentPolicy::baseline(),
        };
        let cargo_home = self
            .cargo_bin_source
            .parent()
            .ok_or_else(|| anyhow!("Rust cargo-bin source has no parent"))?;
        let overrides = [
            ("CARGO_HOME", path_value("worker Cargo home", cargo_home)?),
            ("HOME", path_value("worker private HOME", &paths.home)?),
            (
                "TMPDIR",
                path_value("worker temporary directory", &paths.temp)?,
            ),
            (
                "XDG_RUNTIME_DIR",
                path_value("worker runtime directory", &paths.runtime)?,
            ),
            (
                "CODEX_HOME",
                path_value("worker native config", &paths.native_config)?,
            ),
            (
                "CLAUDE_CONFIG_DIR",
                path_value("worker native config", &paths.native_config)?,
            ),
            (
                "XDG_CONFIG_HOME",
                path_value("worker XDG config", &paths.xdg_config)?,
            ),
            (
                "XDG_STATE_HOME",
                path_value("worker XDG state", &paths.xdg_state)?,
            ),
            (
                "XDG_CACHE_HOME",
                path_value("worker XDG cache", &paths.xdg_cache)?,
            ),
            (
                "XDG_DATA_HOME",
                path_value("worker XDG data", &paths.xdg_data)?,
            ),
            (
                "PYTHONPYCACHEPREFIX",
                path_value(
                    "worker Python bytecode cache",
                    &paths.temp.join("python-pycache"),
                )?,
            ),
            (
                "RUSTUP_HOME",
                path_value("worker Rustup home", &self.rustup_home_source)?,
            ),
            ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS", "1".to_owned()),
            ("CLAUDE_CODE_DISABLE_FAST_MODE", "1".to_owned()),
            ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1".to_owned()),
            ("CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION", "false".to_owned()),
        ];
        let overrides = overrides
            .into_iter()
            .filter(|(name, _)| policy.override_names.iter().any(|allowed| allowed == name))
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        let lease = ExecutionEnvironmentLease::capture_complete(
            format!("lease-{}", Uuid::new_v4()),
            epoch,
            &policy,
            &self.parent_environment,
            overrides,
        )
        .with_context(|| format!("failed to capture worker environment for {run_id}/{task_id}"))?;
        if matches!(adapter, CLAUDE_DEVELOPER_ADAPTER | CLAUDE_REVIEWER_ADAPTER) {
            let source = self
                .claude_auth_source
                .as_deref()
                .ok_or_else(|| anyhow!("Claude auth source is unavailable"))?;
            return lease.with_secret_redaction_values(claude_auth_redaction_values(source)?);
        }
        Ok(lease)
    }

    fn require_adapter_ready(&self, adapter: &str) -> Result<()> {
        if matches!(adapter, CLAUDE_DEVELOPER_ADAPTER | CLAUDE_REVIEWER_ADAPTER) {
            let source = self
                .claude_auth_source
                .as_deref()
                .ok_or_else(|| anyhow!("Claude auth source is unavailable"))?;
            validate_claude_auth_readiness(source, SystemTime::now())?;
        }
        Ok(())
    }

    fn profile_hash(&self) -> Option<String> {
        self.profiles
            .as_ref()
            .map(SessionInvocationProfiles::canonical_hash)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SessionMetrics {
    pub(crate) current_workers: u64,
    pub(crate) max_live_workers: u64,
    pub(crate) developer_spawns: u64,
    pub(crate) reviewer_spawns: u64,
    pub(crate) worker_retries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpawnAudit {
    pub(crate) task_key: String,
    pub(crate) role: WorkerRole,
    pub(crate) logical_session_id: String,
    pub(crate) native_session_id: Option<String>,
    pub(crate) turn_sequence: u32,
    pub(crate) attempt: u32,
    pub(crate) resume: bool,
    pub(crate) workspace_cwd: PathBuf,
    pub(crate) prompt_in_argv: bool,
}

#[derive(Clone)]
struct DraftPlan {
    version: u64,
    hash: String,
    developer_adapter: String,
    reviewer_adapter: String,
}

struct WorkerSession {
    logical_session_id: String,
    native_session_id: Option<String>,
    turn_sequence: u32,
    adapter: Option<Arc<dyn WorkerAdapter>>,
    profile: Option<WorkerProfile>,
}

impl WorkerSession {
    fn fresh() -> Self {
        Self {
            logical_session_id: Uuid::new_v4().to_string(),
            native_session_id: None,
            turn_sequence: 0,
            adapter: None,
            profile: None,
        }
    }
}

struct TaskRuntime {
    spec: TaskDraft,
    state: TaskState,
    base_revision: Option<String>,
    head_revision: Option<String>,
    review_round: u32,
    developer: WorkerSession,
    reviewer: WorkerSession,
    last_review: Option<ReviewerResult>,
    outcome_detail: Option<String>,
}

impl TaskRuntime {
    fn new(spec: TaskDraft) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            base_revision: None,
            head_revision: None,
            review_round: 0,
            developer: WorkerSession::fresh(),
            reviewer: WorkerSession::fresh(),
            last_review: None,
            outcome_detail: None,
        }
    }

    fn session(&self, role: WorkerRole) -> &WorkerSession {
        match role {
            WorkerRole::Developer => &self.developer,
            WorkerRole::Reviewer => &self.reviewer,
        }
    }

    fn session_mut(&mut self, role: WorkerRole) -> &mut WorkerSession {
        match role {
            WorkerRole::Developer => &mut self.developer,
            WorkerRole::Reviewer => &mut self.reviewer,
        }
    }
}

#[derive(Clone)]
struct RetryTurn {
    task_index: usize,
    role: WorkerRole,
    turn_sequence: u32,
    next_attempt: u32,
    review_round: u32,
    must_resume: bool,
}

struct ActiveTurn {
    token: String,
    task_index: usize,
    role: WorkerRole,
    turn_sequence: u32,
    attempt: u32,
    review_round: u32,
    adapter: Arc<dyn WorkerAdapter>,
    profile: WorkerProfile,
    control: TurnControl,
    environment: ExecutionEnvironmentLease,
    cancel: Arc<AtomicBool>,
    completion: Receiver<Result<ProcessCompletion>>,
    waiter: Option<JoinHandle<()>>,
    reviewer_drifted: bool,
    created_at: i64,
}

#[derive(Default)]
struct CompletionGate {
    active: Option<String>,
    accepted: BTreeSet<String>,
}

impl CompletionGate {
    fn begin(&mut self, token: &str) -> Result<()> {
        if self.active.is_some() || self.accepted.contains(token) {
            bail!("completion gate already owns an active or accepted turn");
        }
        self.active = Some(token.to_owned());
        Ok(())
    }

    fn accept(&mut self, token: &str) -> bool {
        if self.active.as_deref() != Some(token) || self.accepted.contains(token) {
            return false;
        }
        self.active = None;
        self.accepted.insert(token.to_owned());
        true
    }

    fn abandon(&mut self, token: &str) {
        if self.active.as_deref() == Some(token) {
            self.active = None;
        }
    }
}

pub(crate) struct SessionSupervisor {
    startup: SessionStartup,
    epoch: String,
    state: SessionState,
    version: u64,
    next_plan_version: u64,
    draft: Option<DraftPlan>,
    tasks: Vec<TaskRuntime>,
    current_task: Option<usize>,
    terminal_detail: Option<String>,
    repositories: BTreeMap<PathBuf, ManagedRepository>,
    lock_root: PathBuf,
    run_root: PathBuf,
    artifact_root: ArtifactRoot,
    artifact_root_path: PathBuf,
    sources: SessionRuntimeSources,
    adapters: WorkerAdapterRegistry,
    runner: ProcessRunner,
    active: Option<ActiveTurn>,
    retry: Option<RetryTurn>,
    used_native_sessions: BTreeMap<String, (usize, WorkerRole)>,
    completion_gate: CompletionGate,
    metrics: SessionMetrics,
    spawn_audit: Vec<SpawnAudit>,
}

impl SessionSupervisor {
    pub(crate) fn open(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        lock_root: PathBuf,
        sources: SessionRuntimeSources,
    ) -> Result<Self> {
        Self::open_with(
            run_id,
            project_root,
            run_root,
            lock_root,
            sources,
            WorkerAdapterRegistry::default(),
            ProcessRunner::default(),
        )
    }

    fn open_with(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        lock_root: PathBuf,
        sources: SessionRuntimeSources,
        adapters: WorkerAdapterRegistry,
        runner: ProcessRunner,
    ) -> Result<Self> {
        validate_id("run id", &run_id)?;
        let project_root = canonical_project_directory(&project_root)?;
        let run_root = canonical_private_directory(&run_root, "session runtime root")?;
        let lock_root = canonical_private_directory(&lock_root, "repository lock root")?;
        let artifact_root_path = run_root.join("artifacts");
        ensure_private_directory(&artifact_root_path)?;
        let artifact_root = ArtifactRoot::open(&artifact_root_path)?;
        let startup = SessionStartup {
            run_id,
            project_root,
        };
        Ok(Self {
            startup,
            epoch: format!("supervisor-{}", Uuid::new_v4()),
            state: SessionState::AwaitingPlan,
            version: 0,
            next_plan_version: 1,
            draft: None,
            tasks: Vec::new(),
            current_task: None,
            terminal_detail: None,
            repositories: BTreeMap::new(),
            lock_root,
            run_root,
            artifact_root,
            artifact_root_path,
            sources,
            adapters,
            runner,
            active: None,
            retry: None,
            used_native_sessions: BTreeMap::new(),
            completion_gate: CompletionGate::default(),
            metrics: SessionMetrics::default(),
            spawn_audit: Vec::new(),
        })
    }

    pub(crate) fn startup(&self) -> &SessionStartup {
        &self.startup
    }

    pub(crate) fn replace_plan(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapter: &str,
        tasks: Vec<TaskDraft>,
    ) -> Result<(u64, String)> {
        self.require_version(expected_session_version)?;
        if !matches!(
            self.state,
            SessionState::AwaitingPlan | SessionState::AwaitingApproval
        ) {
            bail!("task plan cannot change after this run starts");
        }
        if let Some(profiles) = &self.sources.profiles
            && (developer_adapter != profiles.developer_adapter_name()
                || reviewer_adapter != profiles.reviewer_adapter_name())
        {
            bail!("task plan adapters differ from the session-frozen profiles");
        }
        if !matches!(
            developer_adapter,
            CODEX_DEVELOPER_ADAPTER | CLAUDE_DEVELOPER_ADAPTER
        ) && self.adapters.resolve(developer_adapter).is_err()
        {
            bail!("unknown or disabled developer adapter");
        }
        if !matches!(
            reviewer_adapter,
            CODEX_REVIEWER_ADAPTER | CLAUDE_REVIEWER_ADAPTER
        ) && self.adapters.resolve(reviewer_adapter).is_err()
        {
            bail!("unknown or disabled reviewer adapter");
        }
        if tasks.is_empty() || tasks.len() > 64 {
            bail!("ordered plan must contain between 1 and 64 tasks");
        }
        let mut task_keys = BTreeSet::new();
        for task in &tasks {
            task.validate()
                .map_err(|error| anyhow!("invalid task plan: {error}"))?;
            if !task_keys.insert(&task.task_key) {
                bail!("ordered plan task keys must be unique");
            }
        }
        // A replacement draft may choose different repositories. Release any
        // prior draft-only leases before acquiring the exact repositories
        // discovered by the Architect from the project documentation.
        self.repositories.clear();
        let repositories = match open_managed_repositories(&tasks, &self.lock_root) {
            Ok(repositories) => repositories,
            Err(error) => {
                self.tasks.clear();
                self.draft = None;
                self.state = SessionState::AwaitingPlan;
                return Err(error);
            }
        };
        let plan_version = self.next_plan_version;
        self.next_plan_version = self
            .next_plan_version
            .checked_add(1)
            .ok_or_else(|| anyhow!("plan version overflow"))?;
        let canonical = serde_json::to_vec(&(
            "hcom-session-plan-v3",
            plan_version,
            &self.startup.project_root,
            repository_plan_snapshot(&repositories),
            self.sources.profile_hash(),
            developer_adapter,
            reviewer_adapter,
            &tasks,
        ))?;
        let plan_hash = sha256_hex(&canonical);
        self.repositories = repositories;
        self.tasks = tasks.iter().cloned().map(TaskRuntime::new).collect();
        for task in &mut self.tasks {
            let repository = self
                .repositories
                .get(Path::new(&task.spec.repository_root))
                .ok_or_else(|| anyhow!("draft task repository disappeared"))?;
            task.base_revision = Some(repository.current_head.clone());
        }
        self.draft = Some(DraftPlan {
            version: plan_version,
            hash: plan_hash.clone(),
            developer_adapter: developer_adapter.to_owned(),
            reviewer_adapter: reviewer_adapter.to_owned(),
        });
        self.current_task = None;
        self.state = SessionState::AwaitingApproval;
        self.terminal_detail = None;
        self.bump_version()?;
        Ok((plan_version, plan_hash))
    }

    pub(crate) fn approve_and_start(
        &mut self,
        expected_session_version: u64,
        plan_version: u64,
        plan_hash: &str,
        approval_confirmed: bool,
    ) -> Result<()> {
        self.require_version(expected_session_version)?;
        if self.state != SessionState::AwaitingApproval || !approval_confirmed {
            bail!("run start requires explicit human execution authorization");
        }
        let draft = self
            .draft
            .as_ref()
            .ok_or_else(|| anyhow!("approved plan disappeared"))?;
        if draft.version != plan_version || draft.hash != plan_hash {
            bail!("approved plan version or hash is stale");
        }
        let developer_adapter = draft.developer_adapter.clone();
        let reviewer_adapter = draft.reviewer_adapter.clone();
        if self
            .sources
            .require_adapter_ready(&developer_adapter)
            .and_then(|_| self.sources.require_adapter_ready(&reviewer_adapter))
            .is_err()
        {
            self.needs_human(
                "selected worker authentication is unavailable, expired, or too close to expiry",
            );
            bail!("approved worker adapter failed its authentication readiness gate");
        }
        for repository in self.repositories.values() {
            repository.require_current_exact()?;
        }
        let first = self
            .tasks
            .first_mut()
            .ok_or_else(|| anyhow!("approved plan contains no tasks"))?;
        first.base_revision = Some(
            self.repositories
                .get(Path::new(&first.spec.repository_root))
                .ok_or_else(|| anyhow!("first task repository disappeared"))?
                .current_head
                .clone(),
        );
        first.state = TaskState::Developing;
        self.current_task = Some(0);
        self.state = SessionState::Running;
        self.bump_version()
    }

    pub(crate) fn cancel(&mut self, expected_session_version: u64, reason: &str) -> Result<()> {
        self.require_version(expected_session_version)?;
        validate_text("cancel reason", reason, 4096)?;
        if self.state.is_terminal() {
            bail!("session is already terminal");
        }
        if let Some(active) = &self.active {
            active.cancel.store(true, Ordering::Release);
        }
        if let Some(index) = self.current_task
            && let Some(task) = self.tasks.get_mut(index)
        {
            task.state = TaskState::Canceled;
        }
        self.retry = None;
        self.state = SessionState::Canceled;
        self.terminal_detail = Some("canceled by explicit architect-session request".into());
        self.bump_version()?;
        self.shutdown()
    }

    pub(crate) fn snapshot(&self) -> SessionStatusSnapshot {
        SessionStatusSnapshot {
            run_id: self.startup.run_id.clone(),
            state: self.state,
            version: self.version,
            project_root: self.startup.project_root.to_string_lossy().into_owned(),
            plan_version: self.draft.as_ref().map(|plan| plan.version),
            plan_hash: self.draft.as_ref().map(|plan| plan.hash.clone()),
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
                    branch: self
                        .repositories
                        .get(Path::new(&task.spec.repository_root))
                        .map(|repository| repository.branch.clone()),
                    review_round: task.review_round,
                    max_review_rounds: task.spec.max_review_rounds,
                    base_revision: task.base_revision.clone(),
                    head_revision: task.head_revision.clone(),
                    developer_session_bound: task.developer.native_session_id.is_some(),
                    reviewer_session_bound: task.reviewer.native_session_id.is_some(),
                    outcome_detail: task.outcome_detail.clone(),
                })
                .collect(),
        }
    }

    pub(crate) fn poll_once(&mut self) -> Result<()> {
        if self.state != SessionState::Running {
            return Ok(());
        }
        let result = if self.active.is_some() {
            self.poll_active_turn()
        } else {
            self.spawn_next_turn()
        };
        if let Err(error) = result {
            if self.state == SessionState::Running {
                self.needs_human(&terminal_worker_error(&error));
            }
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn shutdown(&mut self) -> Result<()> {
        self.retry = None;
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };
        active.cancel.store(true, Ordering::Release);
        self.completion_gate.abandon(&active.token);
        if let Some(waiter) = active.waiter.take() {
            waiter
                .join()
                .map_err(|_| anyhow!("worker waiter panicked during parent shutdown"))?;
        }
        self.finish_worker_metric();
        Ok(())
    }

    #[cfg(test)]
    fn metrics(&self) -> &SessionMetrics {
        &self.metrics
    }

    #[cfg(test)]
    fn spawn_audit(&self) -> &[SpawnAudit] {
        &self.spawn_audit
    }

    fn spawn_next_turn(&mut self) -> Result<()> {
        let task_index = self
            .current_task
            .ok_or_else(|| anyhow!("running session has no current task"))?;
        let role = match self.tasks[task_index].state {
            TaskState::Developing => WorkerRole::Developer,
            TaskState::Reviewing => WorkerRole::Reviewer,
            _ => bail!("current task has no spawnable state"),
        };
        let retry = self.retry.take();
        if retry
            .as_ref()
            .is_some_and(|retry| retry.task_index != task_index || retry.role != role)
        {
            bail!("retry token no longer matches the current task state");
        }
        let adapter_name = {
            let draft = self
                .draft
                .as_ref()
                .ok_or_else(|| anyhow!("task adapter requested without a plan"))?;
            match role {
                WorkerRole::Developer => draft.developer_adapter.clone(),
                WorkerRole::Reviewer => draft.reviewer_adapter.clone(),
            }
        };
        if let Err(error) = self.sources.require_adapter_ready(&adapter_name) {
            self.needs_human(
                "selected worker authentication expired before its next isolated turn",
            );
            return Err(error).context("worker authentication readiness gate failed");
        }
        self.ensure_task_adapter(task_index, role)?;
        let task = &self.tasks[task_index];
        let session = task.session(role);
        let adapter = session
            .adapter
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("task adapter disappeared"))?;
        let profile = session
            .profile
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("task profile disappeared"))?;
        let base_revision = task
            .base_revision
            .clone()
            .ok_or_else(|| anyhow!("task base revision is unavailable"))?;
        let expected_head = task
            .head_revision
            .clone()
            .unwrap_or_else(|| base_revision.clone());
        self.repositories
            .get(Path::new(&task.spec.repository_root))
            .ok_or_else(|| anyhow!("task repository disappeared"))?
            .require_exact(&expected_head)?;

        let (turn_sequence, attempt, review_round, must_resume) = match retry {
            Some(retry) => (
                retry.turn_sequence,
                retry.next_attempt,
                retry.review_round,
                retry.must_resume,
            ),
            None => {
                let sequence = session
                    .turn_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("worker turn sequence overflow"))?;
                let round = if role == WorkerRole::Reviewer {
                    task.review_round
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("review round overflow"))?
                } else {
                    task.review_round
                };
                (sequence, 1, round, sequence > 1)
            }
        };
        let native_session_id = session.native_session_id.clone();
        if must_resume && native_session_id.is_none() {
            bail!("resume retry lost its exact native session");
        }
        let scope = ArtifactScope {
            run_id: self.startup.run_id.clone(),
            task_id: task.spec.task_key.clone(),
            role,
            logical_session_id: session.logical_session_id.clone(),
            turn_sequence,
            attempt,
        };
        let control = TurnControl {
            run_id: self.startup.run_id.clone(),
            task_id: task.spec.task_key.clone(),
            role,
            logical_session_id: session.logical_session_id.clone(),
            native_session_id: native_session_id.clone(),
            turn_sequence,
            attempt,
            task_version: self.version.saturating_add(1).max(1),
            review_round,
            base_revision,
            head_revision: (role == WorkerRole::Reviewer).then_some(expected_head),
            artifact_dir: scope.relative_path(),
        };
        let prompt = self.build_turn_prompt(task_index, role, review_round)?;
        let prepared = if must_resume {
            prepare_resume_turn(
                adapter.as_ref(),
                &profile,
                &control,
                native_session_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("resume turn lost native session"))?,
                prompt.clone(),
            )?
        } else {
            prepare_create_turn(adapter.as_ref(), &profile, &control, prompt.clone())?
        };
        let environment_paths =
            self.worker_environment_paths(task_index, role, &profile.adapter)?;
        let environment = self.sources.environment_for(
            &profile.adapter,
            &self.epoch,
            &self.startup.run_id,
            &task.spec.task_key,
            &environment_paths,
        )?;
        let attempt_artifact =
            ArtifactAttempt::create(&self.artifact_root, scope, &environment, &prompt)?;
        let materialized = environment.materialize(
            &self.epoch,
            &WorkerEnvironmentIdentity {
                role,
                run_id: self.startup.run_id.clone(),
                task_id: task.spec.task_key.clone(),
            },
        )?;
        let argv = prepared.command().materialized_control_argv();
        self.spawn_audit.push(SpawnAudit {
            task_key: task.spec.task_key.clone(),
            role,
            logical_session_id: session.logical_session_id.clone(),
            native_session_id: native_session_id.clone(),
            turn_sequence,
            attempt,
            resume: must_resume,
            workspace_cwd: prepared.command().workspace_cwd.clone(),
            prompt_in_argv: argv
                .iter()
                .any(|argument| contains_bytes(argument.as_bytes(), &prompt)),
        });
        let worker = match self
            .runner
            .spawn(role, prepared, &materialized, attempt_artifact)
        {
            Ok(worker) => worker,
            Err(error) => {
                self.schedule_spawn_retry(
                    task_index,
                    role,
                    turn_sequence,
                    attempt,
                    review_round,
                    must_resume,
                );
                let _ = error;
                return Ok(());
            }
        };
        if role == WorkerRole::Reviewer && review_round > self.tasks[task_index].review_round {
            self.tasks[task_index].review_round = review_round;
        }
        let token = format!("turn-{}", Uuid::new_v4());
        self.completion_gate.begin(&token)?;
        let cancel = Arc::new(AtomicBool::new(false));
        let waiter_cancel = Arc::clone(&cancel);
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let waiter = std::thread::Builder::new()
            .name(format!(
                "hcom-session-worker-{}-{}",
                role_name(role),
                turn_sequence
            ))
            .spawn(move || {
                let completion =
                    worker.wait_with_cancel(waiter_cancel, |_| Ok(HeartbeatControl::Continue));
                let _ = completion_tx.send(completion);
            })
            .context("failed to start session worker waiter")?;
        self.start_worker_metric(role);
        self.active = Some(ActiveTurn {
            token,
            task_index,
            role,
            turn_sequence,
            attempt,
            review_round,
            adapter,
            profile,
            control,
            environment,
            cancel,
            completion: completion_rx,
            waiter: Some(waiter),
            reviewer_drifted: false,
            created_at: now_epoch_seconds()?,
        });
        Ok(())
    }

    fn poll_active_turn(&mut self) -> Result<()> {
        let mut active = self
            .active
            .take()
            .ok_or_else(|| anyhow!("active session worker disappeared"))?;
        if active.role == WorkerRole::Reviewer && !active.reviewer_drifted {
            let expected = active
                .control
                .head_revision
                .as_deref()
                .ok_or_else(|| anyhow!("reviewer turn lost exact HEAD"))?;
            let repository_root = Path::new(&self.tasks[active.task_index].spec.repository_root);
            if self
                .repositories
                .get(repository_root)
                .is_none_or(|repository| repository.require_exact(expected).is_err())
            {
                active.reviewer_drifted = true;
                active.cancel.store(true, Ordering::Release);
            }
        }
        if self.state == SessionState::Canceled {
            active.cancel.store(true, Ordering::Release);
        }
        let completion = match active.completion.try_recv() {
            Ok(completion) => completion,
            Err(TryRecvError::Empty) => {
                self.active = Some(active);
                return Ok(());
            }
            Err(TryRecvError::Disconnected) => {
                if let Some(waiter) = active.waiter.take() {
                    let _ = waiter.join();
                }
                self.finish_worker_metric();
                self.completion_gate.abandon(&active.token);
                self.needs_human("worker completion channel disconnected");
                bail!("worker waiter disconnected")
            }
        };
        if let Some(waiter) = active.waiter.take() {
            waiter
                .join()
                .map_err(|_| anyhow!("session worker waiter panicked"))?;
        }
        self.finish_worker_metric();
        if active.reviewer_drifted {
            self.completion_gate.abandon(&active.token);
            self.needs_human("task repository drifted during reviewer turn");
            bail!("reviewer HEAD or worktree drifted")
        }
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                self.completion_gate.abandon(&active.token);
                self.schedule_runtime_retry(&active, None, None)?;
                let _ = error;
                return Ok(());
            }
        };
        if completion.exit.termination != WorkerTermination::Exited
            || completion.exit.code != Some(0)
            || completion.exit.signal.is_some()
        {
            let observed = observe_native_session(active.adapter.as_ref(), &completion);
            let failure_detail =
                terminal_process_failure(&completion.exit, completion.artifacts.stderr());
            self.completion_gate.abandon(&active.token);
            self.schedule_runtime_retry(
                &active,
                observed.as_deref(),
                Some(failure_detail.as_str()),
            )?;
            return Ok(());
        }
        let native = active
            .adapter
            .extract_result(&active.control, &completion.artifacts)
            .context("worker result failed its exact adapter contract")?;
        if native.role() != active.role {
            self.completion_gate.abandon(&active.token);
            self.needs_human("worker returned the wrong role");
            bail!("worker result role mismatch")
        }
        self.bind_native_session(active.task_index, active.role, native.native_session_id())?;
        let result_json = match &native {
            NativeResult::Developer { result, .. } => result.canonical_json()?,
            NativeResult::Reviewer { result, .. } => result.canonical_json()?,
        };
        let result_hash = sha256_hex(&result_json);
        let result_receipt = completion
            .artifact_attempt
            .write_result_json(&result_json)?;
        if result_receipt.sha256 != result_hash {
            self.completion_gate.abandon(&active.token);
            self.needs_human("validated result artifact hash mismatched");
            bail!("validated result artifact hash mismatch")
        }
        let completed_at = now_epoch_seconds()?;
        let head_revision = match &native {
            NativeResult::Developer { result, .. } => result.head_revision.clone(),
            NativeResult::Reviewer { .. } => active.control.head_revision.clone(),
        };
        let review_workspace_digest = if active.role == WorkerRole::Reviewer {
            Some(sha256_hex(&serde_json::to_vec(&(
                "hcom-session-review-workspace-v1",
                &self.tasks[active.task_index].spec.repository_root,
                active
                    .control
                    .head_revision
                    .as_deref()
                    .ok_or_else(|| anyhow!("reviewer turn lost its exact HEAD"))?,
            ))?))
        } else {
            None
        };
        let _manifest: TurnManifest =
            completion
                .artifact_attempt
                .finalize_manifest(ManifestMetadata {
                    native_session_id: native.native_session_id().to_owned(),
                    task_version: active.control.task_version,
                    review_round: active.control.review_round,
                    base_revision: active.control.base_revision.clone(),
                    head_revision,
                    review_workspace_digest,
                    supervisor_epoch: self.epoch.clone(),
                    environment_hash: active.environment.descriptor().environment_hash.clone(),
                    adapter_contract_hash: active.profile.capability.contract_hash.clone(),
                    result_hash,
                    created_at: active.created_at,
                    completed_at,
                })?;
        if !self.completion_gate.accept(&active.token) {
            bail!("duplicate or late worker completion was rejected");
        }
        self.apply_native_result(&active, native)
    }

    fn apply_native_result(&mut self, active: &ActiveTurn, native: NativeResult) -> Result<()> {
        match native {
            NativeResult::Developer { result, .. } => {
                if result.decision != DeveloperDecision::Completed {
                    self.tasks[active.task_index].outcome_detail =
                        Some(developer_outcome_detail(&result));
                    self.needs_human("developer needs human input or reported a blocker");
                    return Ok(());
                }
                require_checks_passed(
                    "developer",
                    &self.tasks[active.task_index].spec.required_checks,
                    &result.checks,
                )?;
                self.tasks[active.task_index].outcome_detail =
                    Some(developer_outcome_detail(&result));
                let base = self.tasks[active.task_index]
                    .base_revision
                    .as_deref()
                    .ok_or_else(|| anyhow!("developer task base disappeared"))?;
                let turn_start_head = self.tasks[active.task_index]
                    .head_revision
                    .as_deref()
                    .unwrap_or(base);
                require_changed_paths_in_scope(
                    &result.changed_paths,
                    &self.tasks[active.task_index].spec.allowed_paths,
                )?;
                let repository_root =
                    PathBuf::from(&self.tasks[active.task_index].spec.repository_root);
                let repository = self
                    .repositories
                    .get_mut(&repository_root)
                    .ok_or_else(|| anyhow!("developer task repository disappeared"))?;
                let head = repository.repository.validate_developer_completion(
                    &repository.branch,
                    base,
                    turn_start_head,
                    &result,
                )?;
                repository.current_head.clone_from(&head);
                let task = &mut self.tasks[active.task_index];
                task.developer.turn_sequence = active.turn_sequence;
                task.head_revision = Some(head);
                task.state = TaskState::Reviewing;
                self.retry = None;
                self.bump_version()
            }
            NativeResult::Reviewer { result, .. } => {
                if result.decision == ReviewDecision::Lgtm {
                    require_checks_passed(
                        "reviewer",
                        &self.tasks[active.task_index].spec.required_checks,
                        &result.checks,
                    )?;
                }
                self.tasks[active.task_index].outcome_detail =
                    Some(reviewer_outcome_detail(&result));
                let expected_head = self.tasks[active.task_index]
                    .head_revision
                    .as_deref()
                    .ok_or_else(|| anyhow!("reviewer task head disappeared"))?;
                self.repositories
                    .get(Path::new(
                        &self.tasks[active.task_index].spec.repository_root,
                    ))
                    .ok_or_else(|| anyhow!("reviewer task repository disappeared"))?
                    .require_exact(expected_head)?;
                let task = &mut self.tasks[active.task_index];
                task.reviewer.turn_sequence = active.turn_sequence;
                task.last_review = Some(result.clone());
                self.retry = None;
                match result.decision {
                    ReviewDecision::Lgtm => {
                        task.state = TaskState::Lgtm;
                        self.advance_after_reviewed_task(active.task_index)
                    }
                    ReviewDecision::RequestChanges
                        if task.review_round >= u32::from(task.spec.max_review_rounds) =>
                    {
                        task.state = TaskState::ReviewExhausted;
                        self.advance_after_reviewed_task(active.task_index)
                    }
                    ReviewDecision::RequestChanges => {
                        task.state = TaskState::Developing;
                        self.bump_version()
                    }
                }
            }
        }
    }

    fn advance_after_reviewed_task(&mut self, completed_index: usize) -> Result<()> {
        let reviewed_head = self.tasks[completed_index]
            .head_revision
            .clone()
            .ok_or_else(|| anyhow!("reviewed task has no exact head"))?;
        let completed_repository = self
            .repositories
            .get(Path::new(&self.tasks[completed_index].spec.repository_root))
            .ok_or_else(|| anyhow!("reviewed task repository disappeared"))?;
        if completed_repository.current_head != reviewed_head {
            bail!("reviewed task HEAD differs from its repository state");
        }
        let next = completed_index + 1;
        if next < self.tasks.len() {
            let next_base = self
                .repositories
                .get(Path::new(&self.tasks[next].spec.repository_root))
                .ok_or_else(|| anyhow!("next task repository disappeared"))?
                .current_head
                .clone();
            let task = &mut self.tasks[next];
            task.base_revision = Some(next_base);
            task.state = TaskState::Developing;
            self.current_task = Some(next);
        } else {
            self.current_task = None;
            self.state = SessionState::Completed;
            self.terminal_detail =
                Some("all explicitly approved tasks reached a terminal outcome".into());
        }
        self.bump_version()
    }

    fn ensure_task_adapter(&mut self, task_index: usize, role: WorkerRole) -> Result<()> {
        if self.tasks[task_index].session(role).adapter.is_some() {
            return Ok(());
        }
        let draft = self
            .draft
            .as_ref()
            .ok_or_else(|| anyhow!("task adapter requested without a plan"))?;
        let name = match role {
            WorkerRole::Developer => draft.developer_adapter.as_str(),
            WorkerRole::Reviewer => draft.reviewer_adapter.as_str(),
        };
        let adapter = match self.adapters.resolve(name) {
            Ok(adapter) => adapter,
            Err(_) => self.create_production_adapter(task_index, role, name)?,
        };
        let descriptor = adapter.descriptor();
        descriptor.validate()?;
        if !descriptor.capabilities.roles.contains(&role) {
            bail!("selected adapter does not support its assigned role");
        }
        let profile = WorkerProfile {
            role,
            adapter: descriptor.name.clone(),
            model: descriptor.model.clone(),
            reasoning: descriptor.reasoning.clone(),
            policy: descriptor.policy.clone(),
            executable: adapter.executable_contract().clone(),
            cli_version: descriptor.cli_version.clone(),
            adapter_contract_version: descriptor.contract_version,
            native_session_mode: descriptor.capabilities.native_session_mode,
            capability: crate::control_api::CapabilitySnapshot {
                contract_hash: descriptor.capability_contract_hash.clone(),
                features: descriptor.capabilities.features.clone(),
            },
        };
        profile.validate_for(adapter.as_ref())?;
        let task = &mut self.tasks[task_index];
        let session = task.session_mut(role);
        if profile.native_session_mode == NativeSessionMode::Preassigned {
            let native = Uuid::new_v4().to_string();
            if self
                .used_native_sessions
                .insert(native.clone(), (task_index, role))
                .is_some()
            {
                bail!("generated native session id collided");
            }
            session.native_session_id = Some(native);
        }
        session.profile = Some(profile);
        session.adapter = Some(adapter);
        Ok(())
    }

    fn create_production_adapter(
        &self,
        task_index: usize,
        role: WorkerRole,
        name: &str,
    ) -> Result<Arc<dyn WorkerAdapter>> {
        let profiles = self.sources.profiles.clone().unwrap_or_default();
        let task = &self.tasks[task_index];
        let environment_paths = self.worker_environment_paths(task_index, role, name)?;
        let role_root = environment_paths
            .native_config
            .parent()
            .ok_or_else(|| anyhow!("worker native config has no private HOME"))?
            .parent()
            .ok_or_else(|| anyhow!("worker private HOME has no role root"))?
            .to_owned();
        let home = role_root.join("home");
        let native = environment_paths.native_config.clone();
        let temp = environment_paths.temp.clone();
        let private_run = environment_paths.runtime.clone();
        let repository_root = PathBuf::from(&task.spec.repository_root);
        let adapter: Arc<dyn WorkerAdapter> =
            match (role, name) {
                (WorkerRole::Developer, CODEX_DEVELOPER_ADAPTER) => {
                    prepare_auth_mount_target(&native.join("auth.json"))?;
                    Arc::new(CodexDeveloperAdapter::discover(CodexDeveloperConfig {
                        run_id: self.startup.run_id.clone(),
                        launch_cwd: self.startup.project_root.clone(),
                        workspace_cwd: repository_root.clone(),
                        artifact_root: self.artifact_root_path.clone(),
                        isolated_home: home,
                        codex_home: native,
                        temp_dir: temp,
                        runtime_dir: private_run,
                        host_runtime_dir: self.sources.host_runtime_dir.clone(),
                        auth_source: self
                            .sources
                            .codex_auth_source
                            .clone()
                            .ok_or_else(|| anyhow!("Codex auth source is unavailable"))?,
                        cargo_bin_source: self.sources.cargo_bin_source.clone(),
                        rustup_home_source: self.sources.rustup_home_source.clone(),
                        invocation: profiles.developer.codex().cloned().ok_or_else(|| {
                            anyhow!("session-frozen developer profile is not Codex")
                        })?,
                    })?)
                }
                (WorkerRole::Developer, CLAUDE_DEVELOPER_ADAPTER) => {
                    prepare_auth_mount_target(&native.join(".credentials.json"))?;
                    let xdg_config = home.join(".config");
                    let xdg_state = home.join(".state");
                    let xdg_cache = home.join(".cache");
                    let xdg_data = home.join(".data");
                    for directory in [&xdg_config, &xdg_state, &xdg_cache, &xdg_data] {
                        ensure_private_directory(directory)?;
                    }
                    Arc::new(ClaudeDeveloperAdapter::discover(ClaudeDeveloperConfig {
                        run_id: self.startup.run_id.clone(),
                        launch_cwd: self.startup.project_root.clone(),
                        workspace_cwd: repository_root.clone(),
                        artifact_root: self.artifact_root_path.clone(),
                        isolated_home: home,
                        claude_config_dir: native,
                        xdg_config_home: xdg_config,
                        xdg_state_home: xdg_state,
                        xdg_cache_home: xdg_cache,
                        xdg_data_home: xdg_data,
                        temp_dir: temp,
                        runtime_dir: private_run,
                        host_runtime_dir: self.sources.host_runtime_dir.clone(),
                        auth_source: self
                            .sources
                            .claude_auth_source
                            .clone()
                            .ok_or_else(|| anyhow!("Claude auth source is unavailable"))?,
                        cargo_bin_source: self.sources.cargo_bin_source.clone(),
                        rustup_home_source: self.sources.rustup_home_source.clone(),
                        invocation: profiles.developer.claude().cloned().ok_or_else(|| {
                            anyhow!("session-frozen developer profile is not Claude")
                        })?,
                    })?)
                }
                (WorkerRole::Reviewer, CODEX_REVIEWER_ADAPTER) => {
                    prepare_auth_mount_target(&native.join("auth.json"))?;
                    Arc::new(CodexReviewerAdapter::discover(CodexReviewerConfig {
                        run_id: self.startup.run_id.clone(),
                        launch_cwd: self.startup.project_root.clone(),
                        workspace_cwd: repository_root.clone(),
                        artifact_root: self.artifact_root_path.clone(),
                        isolated_home: home,
                        codex_home: native,
                        temp_dir: temp,
                        runtime_dir: private_run,
                        host_runtime_dir: self.sources.host_runtime_dir.clone(),
                        auth_source: self
                            .sources
                            .codex_auth_source
                            .clone()
                            .ok_or_else(|| anyhow!("Codex auth source is unavailable"))?,
                        cargo_bin_source: self.sources.cargo_bin_source.clone(),
                        rustup_home_source: self.sources.rustup_home_source.clone(),
                        invocation: profiles.reviewer.codex().cloned().ok_or_else(|| {
                            anyhow!("session-frozen reviewer profile is not Codex")
                        })?,
                    })?)
                }
                (WorkerRole::Reviewer, CLAUDE_REVIEWER_ADAPTER) => {
                    prepare_auth_mount_target(&native.join(".credentials.json"))?;
                    let xdg_config = home.join(".config");
                    let xdg_state = home.join(".state");
                    let xdg_cache = home.join(".cache");
                    let xdg_data = home.join(".data");
                    for directory in [&xdg_config, &xdg_state, &xdg_cache, &xdg_data] {
                        ensure_private_directory(directory)?;
                    }
                    Arc::new(ClaudeReviewerAdapter::discover(ClaudeReviewerConfig {
                        run_id: self.startup.run_id.clone(),
                        launch_cwd: self.startup.project_root.clone(),
                        workspace_cwd: repository_root,
                        artifact_root: self.artifact_root_path.clone(),
                        isolated_home: home,
                        claude_config_dir: native,
                        xdg_config_home: xdg_config,
                        xdg_state_home: xdg_state,
                        xdg_cache_home: xdg_cache,
                        xdg_data_home: xdg_data,
                        temp_dir: temp,
                        runtime_dir: private_run,
                        host_runtime_dir: self.sources.host_runtime_dir.clone(),
                        auth_source: self
                            .sources
                            .claude_auth_source
                            .clone()
                            .ok_or_else(|| anyhow!("Claude auth source is unavailable"))?,
                        cargo_bin_source: self.sources.cargo_bin_source.clone(),
                        rustup_home_source: self.sources.rustup_home_source.clone(),
                        invocation: profiles.reviewer.claude().cloned().ok_or_else(|| {
                            anyhow!("session-frozen reviewer profile is not Claude")
                        })?,
                    })?)
                }
                _ => bail!("unknown or disabled exact session worker adapter"),
            };
        Ok(adapter)
    }

    fn worker_environment_paths(
        &self,
        task_index: usize,
        role: WorkerRole,
        adapter: &str,
    ) -> Result<WorkerEnvironmentPaths> {
        let task = self
            .tasks
            .get(task_index)
            .ok_or_else(|| anyhow!("worker task index is out of range"))?;
        let workers_root = self.run_root.join("workers");
        let task_root = workers_root.join(format!("{}-{}", task_index, task.spec.task_key));
        let role_root = task_root.join(role_name(role));
        let home = role_root.join("home");
        let native_config = match adapter {
            CLAUDE_DEVELOPER_ADAPTER | CLAUDE_REVIEWER_ADAPTER => home.join(".claude"),
            _ => home.join(".codex"),
        };
        let paths = WorkerEnvironmentPaths {
            home: home.clone(),
            native_config,
            temp: role_root.join("tmp"),
            runtime: role_root.join("run"),
            xdg_config: home.join(".config"),
            xdg_state: home.join(".state"),
            xdg_cache: home.join(".cache"),
            xdg_data: home.join(".data"),
        };
        for directory in [
            &workers_root,
            &task_root,
            &role_root,
            &home,
            &paths.native_config,
            &paths.temp,
            &paths.runtime,
            &paths.xdg_config,
            &paths.xdg_state,
            &paths.xdg_cache,
            &paths.xdg_data,
        ] {
            ensure_private_directory(directory)?;
        }
        Ok(paths)
    }

    fn build_turn_prompt(
        &self,
        task_index: usize,
        role: WorkerRole,
        review_round: u32,
    ) -> Result<Vec<u8>> {
        #[derive(Serialize)]
        struct Prompt<'a> {
            contract: &'static str,
            role: &'static str,
            turn_phase: &'static str,
            task_ordinal: usize,
            project_root: &'a Path,
            repository_root: &'a str,
            task: &'a TaskDraft,
            base_revision: &'a str,
            head_revision: Option<&'a str>,
            review_round: u32,
            max_review_rounds: u8,
            prior_review: Option<&'a ReviewerResult>,
            requirements: &'static [&'static str],
        }
        let task = &self.tasks[task_index];
        let turn_phase = match (role, task.last_review.as_ref()) {
            (WorkerRole::Developer, None) => "initial_development",
            (WorkerRole::Developer, Some(_)) => "fix_after_request_changes",
            (WorkerRole::Reviewer, _) => "independent_review",
        };
        let prompt = Prompt {
            contract: "hcom-session-worker-turn-v1",
            role: role_name(role),
            turn_phase,
            task_ordinal: task_index,
            project_root: &self.startup.project_root,
            repository_root: &task.spec.repository_root,
            task: &task.spec,
            base_revision: task
                .base_revision
                .as_deref()
                .ok_or_else(|| anyhow!("task prompt lost base revision"))?,
            head_revision: task.head_revision.as_deref(),
            review_round,
            max_review_rounds: task.spec.max_review_rounds,
            prior_review: (role == WorkerRole::Developer)
                .then_some(task.last_review.as_ref())
                .flatten(),
            requirements: match role {
                WorkerRole::Developer if task.last_review.is_none() => &[
                    "This is exactly one initial developer turn; stop after completing and committing only the current developer stage.",
                    "The native CLI starts in project_root for context. Apply code changes only in repository_root; use absolute paths or git -C repository_root as needed.",
                    "There has been no reviewer turn. Never claim, simulate, anticipate, or perform reviewer work.",
                    "Do not start, invoke, delegate to, or wait for any sub-agent or reviewer.",
                    "Do not implement any later change that is conditional on a future review or future finding.",
                    "Work only on this explicitly approved task and allowed paths.",
                    "Run every required check and report each command under its exact approved string.",
                    "For a completed result, commits must list every commit in chronological base_revision..HEAD order, and changed_paths must list the complete union over that same full task range.",
                    "Return a clean committed HEAD that fast-forwards the exact task base.",
                    "Do not push, install, reset, rebase, merge, or expand scope.",
                ],
                WorkerRole::Developer => &[
                    "This is exactly one resumed developer turn after the prior reviewer request_changes.",
                    "The native CLI remains in project_root for context. Apply fixes only in repository_root; use absolute paths or git -C repository_root as needed.",
                    "Address only the supplied prior_review findings within the approved task and allowed paths.",
                    "Never claim, simulate, anticipate, or perform reviewer work.",
                    "Do not start, invoke, delegate to, or wait for any sub-agent or reviewer.",
                    "Run every required check and report each command under its exact approved string.",
                    "For a completed result, commits must list every commit in chronological base_revision..HEAD order, and changed_paths must list the complete union over that same full task range.",
                    "The full-range result must include commits and paths from earlier turns; never report only the current resumed turn delta.",
                    "Commit the bounded fix and return a clean HEAD that fast-forwards both the task base and prior reviewed HEAD.",
                    "Do not push, install, reset, rebase, merge, or expand scope.",
                ],
                WorkerRole::Reviewer => &[
                    "This is exactly one independent reviewer turn; review only the exact bound HEAD, task, history, and acceptance criteria.",
                    "The native CLI starts in project_root for context. Inspect the exact repository_root without changing it.",
                    "Keep the canonical repository source and Git state unchanged. Run checks directly against repository_root; do not copy the source checkout elsewhere.",
                    "Generated bytecode, caches, and temporary check output may use the session-private HOME, TMPDIR, and configured language cache paths.",
                    "Session cache variables are already configured. Execute every required check byte-for-byte as approved; do not add, replace, or prefix environment assignments or wrappers.",
                    "Do not start, invoke, delegate to, or wait for any sub-agent or developer.",
                    "Run every required check and report each command under its exact approved string.",
                    "Return request_changes only with at least one major finding.",
                    "Return lgtm only when no major finding remains.",
                ],
            },
        };
        let bytes = serde_json::to_vec(&prompt)?;
        if bytes.len() > crate::worker::contract::MAX_PROMPT_BYTES {
            bail!("session worker prompt exceeds its bound");
        }
        Ok(bytes)
    }

    fn bind_native_session(
        &mut self,
        task_index: usize,
        role: WorkerRole,
        native_session_id: &str,
    ) -> Result<()> {
        crate::worker::contract::validate_native_session_id(native_session_id)?;
        let session = self.tasks[task_index].session_mut(role);
        match session.native_session_id.as_deref() {
            Some(expected) if expected == native_session_id => Ok(()),
            Some(_) => bail!("worker result changed the exact native session"),
            None => {
                if self.used_native_sessions.contains_key(native_session_id) {
                    bail!("native session id was reused across task or role");
                }
                session.native_session_id = Some(native_session_id.to_owned());
                self.used_native_sessions
                    .insert(native_session_id.to_owned(), (task_index, role));
                Ok(())
            }
        }
    }

    fn schedule_spawn_retry(
        &mut self,
        task_index: usize,
        role: WorkerRole,
        turn_sequence: u32,
        attempt: u32,
        review_round: u32,
        must_resume: bool,
    ) {
        if attempt < MAX_WORKER_ATTEMPTS {
            self.retry = Some(RetryTurn {
                task_index,
                role,
                turn_sequence,
                next_attempt: attempt + 1,
                review_round,
                must_resume,
            });
            self.metrics.worker_retries = self.metrics.worker_retries.saturating_add(1);
        } else {
            self.needs_human("worker spawn retries were exhausted");
        }
    }

    fn schedule_runtime_retry(
        &mut self,
        active: &ActiveTurn,
        observed_native_session: Option<&str>,
        failure_detail: Option<&str>,
    ) -> Result<()> {
        if let Some(native) = observed_native_session {
            self.bind_native_session(active.task_index, active.role, native)?;
        }
        let has_exact_native = self.tasks[active.task_index]
            .session(active.role)
            .native_session_id
            .is_some();
        if active.attempt < MAX_WORKER_ATTEMPTS && has_exact_native {
            self.retry = Some(RetryTurn {
                task_index: active.task_index,
                role: active.role,
                turn_sequence: active.turn_sequence,
                next_attempt: active.attempt + 1,
                review_round: active.review_round,
                must_resume: true,
            });
            self.metrics.worker_retries = self.metrics.worker_retries.saturating_add(1);
            Ok(())
        } else {
            let stage = if has_exact_native {
                "worker crash retries were exhausted"
            } else {
                "worker crashed before an exact native session could be proven"
            };
            let detail = match failure_detail {
                Some(failure) => truncate_status_detail(format!("{stage}: {failure}")),
                None => stage.to_owned(),
            };
            self.needs_human(&detail);
            Ok(())
        }
    }

    fn require_version(&self, expected: u64) -> Result<()> {
        if self.version != expected {
            bail!("session version is stale");
        }
        Ok(())
    }

    fn bump_version(&mut self) -> Result<()> {
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| anyhow!("session version overflow"))?;
        Ok(())
    }

    fn needs_human(&mut self, detail: &str) {
        if let Some(index) = self.current_task
            && let Some(task) = self.tasks.get_mut(index)
            && !matches!(
                task.state,
                TaskState::Lgtm | TaskState::ReviewExhausted | TaskState::Canceled
            )
        {
            task.state = TaskState::NeedsHuman;
        }
        self.retry = None;
        self.state = SessionState::NeedsHuman;
        self.terminal_detail = Some(detail.to_owned());
        let _ = self.bump_version();
    }

    fn start_worker_metric(&mut self, role: WorkerRole) {
        self.metrics.current_workers = self.metrics.current_workers.saturating_add(1);
        self.metrics.max_live_workers = self
            .metrics
            .max_live_workers
            .max(self.metrics.current_workers);
        match role {
            WorkerRole::Developer => {
                self.metrics.developer_spawns = self.metrics.developer_spawns.saturating_add(1)
            }
            WorkerRole::Reviewer => {
                self.metrics.reviewer_spawns = self.metrics.reviewer_spawns.saturating_add(1)
            }
        }
    }

    fn finish_worker_metric(&mut self) {
        self.metrics.current_workers = self.metrics.current_workers.saturating_sub(1);
    }
}

impl Drop for SessionSupervisor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn observe_native_session(
    adapter: &dyn WorkerAdapter,
    completion: &ProcessCompletion,
) -> Option<String> {
    let mut records = Vec::new();
    records.push(completion.artifacts.stdout());
    records.extend(
        completion
            .artifacts
            .stdout()
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|record| !record.iter().all(u8::is_ascii_whitespace)),
    );
    let mut observed = BTreeSet::new();
    for record in records {
        if let Ok(observations) = adapter.observe_native_record(record) {
            for observation in observations {
                if let NativeObservation::SessionStarted { native_session_id } = observation {
                    observed.insert(native_session_id);
                }
            }
        }
    }
    (observed.len() == 1).then(|| observed.into_iter().next().expect("one observed session"))
}

struct CanonicalRepository {
    root: PathBuf,
    git: ExecutableIdentity,
    root_identity: DirectoryIdentity,
    git_dir: DirectoryIdentity,
    common_dir: DirectoryIdentity,
    object_dir: DirectoryIdentity,
}

struct ManagedRepository {
    repository: CanonicalRepository,
    _lock: RepositoryLock,
    branch: String,
    start_head: String,
    current_head: String,
}

impl ManagedRepository {
    fn open(root: &Path, lock_root: &Path) -> Result<Self> {
        let repository = CanonicalRepository::open(root)?;
        let lock = RepositoryLock::acquire(&repository, lock_root)?;
        let branch = repository.branch()?;
        let start_head = repository.head()?;
        repository.require_exact(&branch, &start_head)?;
        Ok(Self {
            repository,
            _lock: lock,
            branch,
            start_head: start_head.clone(),
            current_head: start_head,
        })
    }

    fn require_exact(&self, head: &str) -> Result<()> {
        self.repository.require_exact(&self.branch, head)
    }

    fn require_current_exact(&self) -> Result<()> {
        self.require_exact(&self.current_head)
    }
}

fn open_managed_repositories(
    tasks: &[TaskDraft],
    lock_root: &Path,
) -> Result<BTreeMap<PathBuf, ManagedRepository>> {
    let roots: BTreeSet<PathBuf> = tasks
        .iter()
        .map(|task| PathBuf::from(&task.repository_root))
        .collect();
    let mut repositories = BTreeMap::new();
    for root in roots {
        let repository = ManagedRepository::open(&root, lock_root)
            .with_context(|| format!("failed to bind task repository {}", root.display()))?;
        if repository.repository.root != root {
            bail!(
                "task repository_root must name the exact canonical Git top level: {}",
                root.display()
            );
        }
        repositories.insert(root, repository);
    }
    Ok(repositories)
}

fn repository_plan_snapshot(
    repositories: &BTreeMap<PathBuf, ManagedRepository>,
) -> Vec<(String, String, String)> {
    repositories
        .iter()
        .map(|(root, repository)| {
            (
                root.to_string_lossy().into_owned(),
                repository.branch.clone(),
                repository.start_head.clone(),
            )
        })
        .collect()
}

impl CanonicalRepository {
    fn open(root: &Path) -> Result<Self> {
        if !root.is_absolute() {
            bail!("session repository must be absolute");
        }
        let root = fs::canonicalize(root).context("failed to canonicalize session repository")?;
        let root_identity =
            DirectoryIdentity::capture(&root, false).context("unsafe canonical checkout root")?;
        let git = capture_exact_git()?;
        let runner = GitRunner {
            git: &git,
            root: &root,
        };
        let top = canonical_git_path(&runner.one_line(&["rev-parse", "--show-toplevel"])?)?;
        if top != root {
            bail!("task repository_root must name the exact canonical Git top level");
        }
        let git_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
        ])?)?;
        let common_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        ])?)?;
        let object_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "objects",
        ])?)?;
        if !git_dir.starts_with(&root)
            || !common_dir.starts_with(&root)
            || !object_dir.starts_with(&root)
        {
            bail!("canonical checkout Git administration must remain inside the checkout");
        }
        let repository = Self {
            root,
            git,
            root_identity,
            git_dir: DirectoryIdentity::capture(&git_dir, false)
                .context("unsafe canonical checkout Git directory")?,
            common_dir: DirectoryIdentity::capture(&common_dir, false)
                .context("unsafe canonical checkout common Git directory")?,
            object_dir: DirectoryIdentity::capture(&object_dir, false)
                .context("unsafe canonical checkout object directory")?,
        };
        repository.reject_indirections()?;
        repository.require_clean()?;
        Ok(repository)
    }

    fn revalidate_identity(&self) -> Result<()> {
        self.git.revalidate()?;
        if DirectoryIdentity::capture(&self.root, false)? != self.root_identity
            || DirectoryIdentity::capture(self.git_dir.path(), false)? != self.git_dir
            || DirectoryIdentity::capture(self.common_dir.path(), false)? != self.common_dir
            || DirectoryIdentity::capture(self.object_dir.path(), false)? != self.object_dir
        {
            bail!("canonical repository identity drifted");
        }
        self.reject_indirections()
    }

    fn branch(&self) -> Result<String> {
        GitRunner {
            git: &self.git,
            root: &self.root,
        }
        .one_line(&["symbolic-ref", "--quiet", "--short", "HEAD"])
        .context("canonical checkout must have an attached branch")
    }

    fn head(&self) -> Result<String> {
        let head = GitRunner {
            git: &self.git,
            root: &self.root,
        }
        .one_line(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        validate_git_oid("canonical checkout HEAD", &head)?;
        Ok(head)
    }

    fn require_clean(&self) -> Result<()> {
        let runner = GitRunner {
            git: &self.git,
            root: &self.root,
        };
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
        {
            bail!("canonical checkout must be completely clean");
        }
        if !runner
            .success(&["for-each-ref", "--format=%(refname)", "refs/replace/"])?
            .is_empty()
        {
            bail!("canonical checkout contains replacement refs");
        }
        Ok(())
    }

    fn require_exact(&self, branch: &str, head: &str) -> Result<()> {
        self.revalidate_identity()?;
        self.require_clean()?;
        if self.branch()? != branch || self.head()? != head {
            bail!("canonical checkout branch or HEAD drifted");
        }
        self.revalidate_identity()?;
        self.require_clean()
    }

    fn validate_developer_completion(
        &self,
        branch: &str,
        base: &str,
        turn_start_head: &str,
        result: &DeveloperResult,
    ) -> Result<String> {
        self.revalidate_identity()?;
        self.require_clean()?;
        if self.branch()? != branch {
            bail!("developer changed the canonical checkout branch");
        }
        let head = self.head()?;
        if head == base || result.head_revision.as_deref() != Some(head.as_str()) {
            bail!("developer result must name a new exact committed HEAD");
        }
        let runner = GitRunner {
            git: &self.git,
            root: &self.root,
        };
        let ancestor = runner.run(&["merge-base", "--is-ancestor", base, &head])?;
        if ancestor.status.code() != Some(0) || !ancestor.stderr.is_empty() {
            bail!("developer HEAD is not a fast-forward from the task base");
        }
        let turn_ancestor = runner.run(&["merge-base", "--is-ancestor", turn_start_head, &head])?;
        if turn_ancestor.status.code() != Some(0) || !turn_ancestor.stderr.is_empty() {
            bail!("developer HEAD rewrote the exact same-task turn-start HEAD");
        }
        let range = format!("{base}..{head}");
        let commits = parse_git_commits(&runner.success(&[
            "log",
            "-z",
            "--reverse",
            "--topo-order",
            "--max-count=257",
            "--no-show-signature",
            "--format=%H%x00%s",
            &range,
            "--",
        ])?)?;
        if commits != result.commits {
            bail!("developer commit report differs from the exact Git range");
        }
        let mut changed = parse_nul_paths(&runner.success(&[
            "diff",
            "--name-only",
            "-z",
            "--no-ext-diff",
            "--no-textconv",
            &range,
            "--",
        ])?)?;
        let mut reported = result.changed_paths.clone();
        changed.sort();
        reported.sort();
        if changed != reported {
            bail!("developer changed-path report differs from the exact Git range");
        }
        self.require_exact(branch, &head)?;
        Ok(head)
    }

    fn reject_indirections(&self) -> Result<()> {
        for path in [
            self.common_dir.path().join("info/grafts"),
            self.object_dir.path().join("info/alternates"),
            self.object_dir.path().join("info/http-alternates"),
        ] {
            match fs::symlink_metadata(&path) {
                Ok(_) => bail!("canonical checkout uses forbidden Git object indirection"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

struct RepositoryLock {
    _file: File,
}

impl RepositoryLock {
    fn acquire(repository: &CanonicalRepository, root: &Path) -> Result<Self> {
        ensure_private_directory(root)?;
        let metadata = fs::metadata(&repository.root)?;
        let key = sha256_hex(serde_json::to_vec(&(metadata.dev(), metadata.ino()))?.as_slice());
        let path = root.join(format!("{key}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        let file_metadata = file.metadata()?;
        // SAFETY: geteuid has no preconditions.
        if file_metadata.uid() != unsafe { libc::geteuid() }
            || file_metadata.permissions().mode() & 0o777 != 0o600
            || file_metadata.nlink() != 1
        {
            bail!("repository lock file has an unsafe identity");
        }
        // SAFETY: flock operates on this live file descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                bail!("another architect session already owns this canonical checkout");
            }
            return Err(error.into());
        }
        Ok(Self { _file: file })
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DirectoryIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
}

impl DirectoryIdentity {
    fn capture(path: &Path, private: bool) -> Result<Self> {
        let link = fs::symlink_metadata(path)?;
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!("directory identity requires a real directory");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("directory identity path must already be canonical");
        }
        let metadata = fs::metadata(path)?;
        let identity = Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
        };
        // SAFETY: geteuid has no preconditions.
        if identity.uid != unsafe { libc::geteuid() } {
            bail!("directory identity is not owned by the current user");
        }
        if private && identity.mode != 0o700 {
            bail!("private directory must be mode 0700");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

struct GitRunner<'a> {
    git: &'a ExecutableIdentity,
    root: &'a Path,
}

struct BoundedOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl GitRunner<'_> {
    fn run(&self, args: &[&str]) -> Result<BoundedOutput> {
        let mut command = Command::new(&self.git.canonical_path);
        command
            .arg("--no-replace-objects")
            .args(["-c", "core.fsmonitor=false"])
            .args(["-c", "core.untrackedCache=false"])
            .args(["-c", "core.hooksPath=/dev/null"])
            .args(args)
            .current_dir(self.root)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_PAGER", "/bin/cat")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C");
        let output = run_bounded_command(command, MAX_GIT_OUTPUT_BYTES, GIT_TIMEOUT)?;
        Ok(output)
    }

    fn success(&self, args: &[&str]) -> Result<Vec<u8>> {
        let output = self.run(args)?;
        if !output.status.success() || !output.stderr.is_empty() {
            bail!("bounded Git evidence command failed");
        }
        Ok(output.stdout)
    }

    fn one_line(&self, args: &[&str]) -> Result<String> {
        let bytes = self.success(args)?;
        let text = std::str::from_utf8(&bytes)?
            .strip_suffix('\n')
            .unwrap_or(std::str::from_utf8(&bytes)?);
        if text.is_empty() || text.contains('\n') || text.contains('\r') {
            bail!("Git evidence did not contain one bounded line");
        }
        Ok(text.to_owned())
    }
}

fn capture_exact_git() -> Result<ExecutableIdentity> {
    let git = ExecutableIdentity::capture(Path::new(GIT_EXECUTABLE))?;
    let output = run_bounded_command(
        {
            let mut command = Command::new(GIT_EXECUTABLE);
            command.arg("--version").env_clear();
            command
        },
        4096,
        Duration::from_secs(5),
    )?;
    if !output.status.success()
        || !output.stderr.is_empty()
        || std::str::from_utf8(&output.stdout)?.trim_end() != GIT_VERSION
    {
        bail!("Git executable does not match the exact enabled version");
    }
    git.revalidate()?;
    Ok(git)
}

fn run_bounded_command(
    mut command: Command,
    cap: usize,
    timeout: Duration,
) -> Result<BoundedOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("missing stderr"))?;
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout_overflow = Arc::clone(&overflow);
    let stderr_overflow = Arc::clone(&overflow);
    let stdout_thread = std::thread::spawn(move || read_bounded(stdout, cap, stdout_overflow));
    let stderr_thread = std::thread::spawn(move || read_bounded(stderr, cap, stderr_overflow));
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if overflow.load(Ordering::Acquire) || started.elapsed() >= timeout {
            terminate_child(&mut child);
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            bail!("bounded Git command exceeded its output or time limit");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("stderr reader panicked"))??;
    if overflow.load(Ordering::Acquire) {
        bail!("bounded Git output exceeded its cap");
    }
    Ok(BoundedOutput {
        status,
        stdout,
        stderr,
    })
}

fn read_bounded(mut reader: impl Read, cap: usize, overflow: Arc<AtomicBool>) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if output.len().saturating_add(count) > cap {
            overflow.store(true, Ordering::Release);
            break;
        }
        output.extend_from_slice(&buffer[..count]);
    }
    Ok(output)
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn parse_git_commits(bytes: &[u8]) -> Result<Vec<crate::worker::result::CommitSummary>> {
    let mut fields: Vec<_> = bytes.split(|byte| *byte == 0).collect();
    if fields.last().is_some_and(|field| field.is_empty()) {
        fields.pop();
    }
    if fields.len() % 2 != 0 || fields.len() / 2 > 256 {
        bail!("Git commit evidence has an invalid shape");
    }
    let mut commits = Vec::new();
    for pair in fields.chunks_exact(2) {
        let sha = std::str::from_utf8(pair[0])?.to_owned();
        let subject = std::str::from_utf8(pair[1])?.to_owned();
        validate_git_oid("Git commit", &sha)?;
        validate_text("Git commit subject", &subject, 512)?;
        commits.push(crate::worker::result::CommitSummary { sha, subject });
    }
    Ok(commits)
}

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for component in bytes.split(|byte| *byte == 0) {
        if component.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(component)?;
        crate::worker::validation::validate_relative_path("Git changed path", path)?;
        paths.push(path.to_owned());
        if paths.len() > 256 {
            bail!("Git changed paths exceed their bound");
        }
    }
    Ok(paths)
}

fn canonical_git_path(value: &str) -> Result<PathBuf> {
    if value.len() > 4096 {
        bail!("Git path exceeds its bound");
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() || fs::canonicalize(&path)? != path {
        bail!("Git path must be absolute and canonical");
    }
    Ok(path)
}

fn canonical_project_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("architect project directory must be absolute");
    }
    let canonical =
        fs::canonicalize(path).context("failed to resolve architect project directory")?;
    let metadata = fs::symlink_metadata(path)?;
    if canonical != path || metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("architect project directory must be an existing canonical directory");
    }
    Ok(canonical)
}

fn path_value(label: &str, path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{label} is not valid UTF-8"))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| {
                format!("failed to create private directory {}", path.display())
            })?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Err(error) => return Err(error.into()),
    }
    let _ = canonical_private_directory(path, "private session directory")?;
    Ok(())
}

fn canonical_private_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).with_context(|| format!("failed to resolve {label}"))?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("{label} must be canonical, current-user owned, and mode 0700");
    }
    Ok(canonical)
}

fn canonical_readable_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o002 != 0
    {
        bail!("{label} has an unsafe identity");
    }
    Ok(canonical)
}

fn canonical_private_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o600 != 0o600
        || metadata.nlink() != 1
    {
        bail!("{label} has an unsafe identity");
    }
    Ok(canonical)
}

fn prepare_auth_mount_target(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if !metadata.file_type().is_symlink()
                && metadata.is_file()
                && metadata.permissions().mode() & 0o777 == 0o600
                && metadata.nlink() == 1 =>
        {
            return Ok(());
        }
        Ok(_) => bail!("worker auth mount target has an unsafe identity"),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        Err(_) => {}
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    file.sync_all()?;
    Ok(())
}

fn validate_id(label: &str, value: &str) -> Result<()> {
    crate::worker::validation::validate_opaque_id(label, value)
}

fn validate_git_oid(label: &str, value: &str) -> Result<()> {
    crate::worker::validation::validate_git_oid(label, value)
}

fn validate_text(label: &str, value: &str, max: usize) -> Result<()> {
    crate::worker::validation::validate_text(label, value, max, false)
}

fn now_epoch_seconds() -> Result<i64> {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
        .context("system time exceeds i64")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn role_name(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => "developer",
        WorkerRole::Reviewer => "reviewer",
    }
}

fn require_changed_paths_in_scope(changed: &[String], allowed: &[String]) -> Result<()> {
    for changed_path in changed {
        let changed_path = Path::new(changed_path);
        if !allowed.iter().any(|allowed_path| {
            let allowed_path = Path::new(allowed_path);
            changed_path == allowed_path || changed_path.starts_with(allowed_path)
        }) {
            bail!("developer changed a path outside the explicitly approved task scope");
        }
    }
    Ok(())
}

fn require_checks_passed(role: &str, required: &[String], reported: &[CheckResult]) -> Result<()> {
    let statuses: BTreeMap<_, _> = reported
        .iter()
        .map(|check| (check.command.as_str(), check.status))
        .collect();
    if required
        .iter()
        .any(|command| statuses.get(command.as_str()).copied() != Some(CheckStatus::Passed))
    {
        bail!("{role} did not pass every explicitly approved required check");
    }
    Ok(())
}

fn developer_outcome_detail(result: &DeveloperResult) -> String {
    let decision = match result.decision {
        DeveloperDecision::Completed => "completed",
        DeveloperDecision::NeedsInput => "needs_input",
        DeveloperDecision::Blocked => "blocked",
    };
    let mut detail = format!("developer {decision}: {}", result.summary);
    if !result.questions.is_empty() {
        detail.push_str("; questions: ");
        detail.push_str(&result.questions.join(" | "));
    }
    if !result.risks.is_empty() {
        detail.push_str("; risks: ");
        detail.push_str(&result.risks.join(" | "));
    }
    truncate_status_detail(detail)
}

fn reviewer_outcome_detail(result: &ReviewerResult) -> String {
    let decision = match result.decision {
        ReviewDecision::Lgtm => "lgtm",
        ReviewDecision::RequestChanges => "request_changes",
    };
    let mut detail = format!("reviewer {decision}: {}", result.summary);
    if !result.findings.is_empty() {
        detail.push_str("; findings: ");
        detail.push_str(
            &result
                .findings
                .iter()
                .map(|finding| finding.title.as_str())
                .collect::<Vec<_>>()
                .join(" | "),
        );
    }
    truncate_status_detail(detail)
}

fn truncate_status_detail(mut detail: String) -> String {
    if detail.len() <= MAX_STATUS_OUTCOME_BYTES {
        return detail;
    }
    const MARKER: &str = "…";
    let mut boundary = MAX_STATUS_OUTCOME_BYTES - MARKER.len();
    while !detail.is_char_boundary(boundary) {
        boundary -= 1;
    }
    detail.truncate(boundary);
    detail.push_str(MARKER);
    detail
}

fn terminal_worker_error(error: &anyhow::Error) -> String {
    let chain = error
        .chain()
        .take(4)
        .map(ToString::to_string)
        .map(|detail| {
            detail
                .chars()
                .map(|character| {
                    if character.is_control() {
                        ' '
                    } else {
                        character
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(": ");
    truncate_status_detail(format!("session worker loop failed: {chain}"))
}

fn terminal_process_failure(exit: &WorkerExit, stderr: &[u8]) -> String {
    const CODEX_UNTRUSTED_PROJECT: &[u8] =
        b"Not inside a trusted directory and --skip-git-repo-check was not specified.\n";
    if stderr == CODEX_UNTRUSTED_PROJECT {
        return "Codex rejected the non-Git project cwd because its required \
--skip-git-repo-check argument was absent"
            .into();
    }
    truncate_status_detail(format!(
        "termination={:?}, exit_code={:?}, signal={:?}",
        exit.termination, exit.code, exit.signal
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::fake::FakeWorkerAdapter;
    use crate::worker::profile::{
        ClaudeInvocationProfile, CodexInvocationProfile, DeveloperInvocationProfile,
        ReviewerInvocationProfile,
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    const FAKE_WORKER: &str = r#"#!/usr/bin/python3
import json
import os
import subprocess
import sys
import time

if any(os.isatty(fd) for fd in (0, 1, 2)):
    raise SystemExit(90)

prompt_bytes = sys.stdin.buffer.read()
if not prompt_bytes:
    raise SystemExit(91)
prompt = json.loads(prompt_bytes)
role = os.environ["HCOM_WORKER_ROLE"]
task = os.environ["HCOM_TASK_ID"]
if os.environ.get("HCOM_RUN_ID") != "run-test":
    raise SystemExit(92)

arguments = sys.argv[1:]
if "--session-id" in arguments:
    session_id = arguments[arguments.index("--session-id") + 1]
    invocation = "create"
elif "--resume" in arguments:
    session_id = arguments[arguments.index("--resume") + 1]
    invocation = "resume"
else:
    raise SystemExit(93)

root = os.path.dirname(os.path.realpath(sys.argv[0]))
try:
    with open(os.path.join(root, "behavior"), encoding="utf-8") as source:
        behavior = source.read().strip()
except FileNotFoundError:
    behavior = "normal"
count_path = os.path.join(root, f"{task}-{role}.count")
try:
    with open(count_path, encoding="utf-8") as source:
        count = int(source.read()) + 1
except FileNotFoundError:
    count = 1
with open(count_path, "w", encoding="utf-8") as output:
    output.write(str(count))
with open(os.path.join(root, "audit.ndjson"), "a", encoding="utf-8") as output:
    output.write(json.dumps({
        "task": task,
        "role": role,
        "session_id": session_id,
        "invocation": invocation,
        "count": count,
        "stdin_is_tty": os.isatty(0),
    }, separators=(",", ":")) + "\n")

if behavior == "crash-once" and task == "one" and role == "developer" and count == 1:
    raise SystemExit(42)

if role == "developer":
    if behavior == "rewrite-on-resume" and task == "one" and count == 2:
        subprocess.run(
            ["/usr/bin/git", "reset", "--hard", prompt["base_revision"]],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    path = f"{task}.txt"
    with open(path, "a", encoding="utf-8") as output:
        output.write(f"{invocation}-{count}\n")
    subprocess.run(
        ["/usr/bin/git", "add", "--", path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-c", "user.name=Phase 8 Fake",
            "-c", "user.email=phase8-fake@example.invalid",
            "commit", "-m", f"{task} developer turn {count}",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    head = subprocess.check_output(
        ["/usr/bin/git", "rev-parse", "HEAD"], text=True
    ).strip()
    base = prompt["base_revision"]
    log = subprocess.check_output(
        [
            "/usr/bin/git", "log", "--reverse",
            "--format=%H%x1f%s", f"{base}..{head}", "--",
        ],
        text=True,
    )
    commits = []
    for line in log.splitlines():
        sha, subject = line.split("\x1f", 1)
        commits.append({"sha": sha, "subject": subject})
    changed = subprocess.check_output(
        ["/usr/bin/git", "diff", "--name-only", f"{base}..{head}", "--"],
        text=True,
    ).splitlines()
    result = {
        "decision": "completed",
        "summary": f"completed {task} turn {count}",
        "head_revision": head,
        "commits": commits,
        "checks": [{
            "command": "fake deterministic check",
            "status": "passed",
            "summary": "the deterministic fake worker completed its approved check",
        }],
        "questions": [],
        "risks": [],
        "changed_paths": sorted(changed),
    }
else:
    if behavior == "slow-review":
        time.sleep(1.0)
    request_changes = task == "one" and (behavior == "exhaust" or count == 1)
    if request_changes:
        result = {
            "decision": "request_changes",
            "summary": "one bounded major finding remains",
            "findings": [{
                "severity": "major",
                "title": "bounded fake finding",
                "body": "exercise exact same-task resume",
                "file": f"{task}.txt",
                "line": 1,
            }],
            "checks": [],
        }
    else:
        result = {
            "decision": "lgtm",
            "summary": "no major finding remains",
            "findings": [],
            "checks": [{
                "command": "fake deterministic check",
                "status": "passed",
                "summary": "the deterministic fake reviewer completed its approved check",
            }],
        }

sys.stdout.write(json.dumps({
    "session_id": session_id,
    "role": role,
    "result": result,
}, separators=(",", ":")))
"#;

    fn task(key: &str, max_review_rounds: u8) -> TaskDraft {
        TaskDraft {
            task_key: key.into(),
            title: format!("Task {key}"),
            objective: format!("Implement the bounded {key} task"),
            repository_root: "/test-fixture-repository".into(),
            acceptance_criteria: vec![format!("{key} is committed and reviewed")],
            required_checks: vec!["fake deterministic check".into()],
            allowed_paths: vec![format!("{key}.txt")],
            forbidden_actions: vec!["push, install, reset, rebase, or merge".into()],
            max_review_rounds,
        }
    }

    fn git(repo: &Path, args: &[&str]) -> Vec<u8> {
        let output = Command::new("/usr/bin/git")
            .args(args)
            .current_dir(repo)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    fn initialize_repository(root: &Path) -> PathBuf {
        let repository = root.join("repo");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        fs::write(repository.join("seed.txt"), "seed\n").unwrap();
        git(&repository, &["add", "--", "seed.txt"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=Phase 8 Fixture",
                "-c",
                "user.email=phase8-fixture@example.invalid",
                "commit",
                "-m",
                "Initial fixture",
            ],
        );
        fs::canonicalize(repository).unwrap()
    }

    fn write_fake_worker(root: &Path, behavior: &str) -> ExecutableIdentity {
        let worker = root.join("fake-worker");
        fs::write(&worker, FAKE_WORKER).unwrap();
        fs::set_permissions(&worker, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root.join("behavior"), behavior).unwrap();
        ExecutableIdentity::capture(fs::canonicalize(worker).unwrap()).unwrap()
    }

    fn private_directory(path: &Path) -> PathBuf {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::canonicalize(path).unwrap()
    }

    fn open_supervisor(
        root: &Path,
        repository: &Path,
        behavior: &str,
        run_name: &str,
        lock_root: &Path,
    ) -> SessionSupervisor {
        let executable = write_fake_worker(root, behavior);
        let adapter =
            Arc::new(FakeWorkerAdapter::preassigned(executable, repository.to_owned()).unwrap());
        let mut adapters = WorkerAdapterRegistry::default();
        adapters.register(adapter).unwrap();
        let run_root = private_directory(&root.join(run_name));
        let toolchain = private_directory(&root.join(format!("{run_name}-toolchain")));
        SessionSupervisor::open_with(
            "run-test".into(),
            repository.to_owned(),
            run_root,
            lock_root.to_owned(),
            SessionRuntimeSources::fake(&toolchain),
            adapters,
            ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(100)).unwrap(),
        )
        .unwrap()
    }

    fn approve(supervisor: &mut SessionSupervisor, mut tasks: Vec<TaskDraft>) {
        for task in &mut tasks {
            task.repository_root = supervisor
                .startup()
                .project_root
                .to_string_lossy()
                .into_owned();
        }
        let (plan_version, plan_hash) = supervisor
            .replace_plan(0, "fake-envelope", "fake-envelope", tasks)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
    }

    #[test]
    fn session_frozen_profiles_reject_plan_adapter_drift_before_start() {
        let temp = tempfile::tempdir().unwrap();
        let repository = initialize_repository(temp.path());
        let locks = private_directory(&temp.path().join("locks"));
        let mut supervisor = open_supervisor(
            temp.path(),
            &repository,
            "normal",
            "run-frozen-profile",
            &locks,
        );
        supervisor.sources.profiles = Some(SessionInvocationProfiles::default());
        assert!(
            supervisor
                .replace_plan(0, "fake-envelope", "fake-envelope", {
                    let mut task = task("one", 1);
                    task.repository_root = repository.to_string_lossy().into_owned();
                    vec![task]
                })
                .is_err()
        );
        assert_eq!(supervisor.version, 0);
        assert_eq!(supervisor.state, SessionState::AwaitingPlan);
    }

    #[test]
    fn non_git_project_context_can_bind_external_and_nested_task_repositories() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project = root.join("project-context");
        let external_parent = root.join("external-source");
        let nested_parent = project.join("src");
        for directory in [&project, &external_parent, &nested_parent] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::write(
            project.join("AGENTS.md"),
            "source repositories are external-source/repo and src/repo\n",
        )
        .unwrap();
        let external_repository = initialize_repository(&external_parent);
        let nested_repository = initialize_repository(&nested_parent);
        assert!(!project.join(".git").exists());

        let executable = write_fake_worker(&root, "normal");
        let adapter = Arc::new(
            FakeWorkerAdapter::preassigned(executable, external_repository.clone()).unwrap(),
        );
        let mut adapters = WorkerAdapterRegistry::default();
        adapters.register(adapter).unwrap();
        let run_root = private_directory(&root.join("run-project-context"));
        let lock_root = private_directory(&root.join("locks-project-context"));
        let toolchain = private_directory(&root.join("toolchain-project-context"));
        let mut supervisor = SessionSupervisor::open_with(
            "run-project-context".into(),
            fs::canonicalize(&project).unwrap(),
            run_root,
            lock_root,
            SessionRuntimeSources::fake(&toolchain),
            adapters,
            ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(100)).unwrap(),
        )
        .unwrap();

        let mut external_task = task("external", 1);
        external_task.repository_root = external_repository.to_string_lossy().into_owned();
        let mut nested_task = task("nested", 1);
        nested_task.repository_root = nested_repository.to_string_lossy().into_owned();
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                "fake-envelope",
                "fake-envelope",
                vec![external_task, nested_task],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();

        let snapshot = supervisor.snapshot();
        assert_eq!(
            snapshot.project_root,
            fs::canonicalize(project).unwrap().to_string_lossy()
        );
        assert_eq!(
            snapshot.tasks[0].repository_root,
            external_repository.to_string_lossy()
        );
        assert_eq!(
            snapshot.tasks[1].repository_root,
            nested_repository.to_string_lossy()
        );
        assert_eq!(snapshot.tasks[0].state, TaskState::Developing);
        assert_eq!(snapshot.tasks[1].state, TaskState::Pending);
    }

    #[test]
    fn session_frozen_profiles_accept_all_codex_claude_role_combinations() {
        let developers = [
            DeveloperInvocationProfile::Codex {
                profile: CodexInvocationProfile::developer_default(),
            },
            DeveloperInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::developer_default(),
            },
        ];
        let reviewers = [
            ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile::reviewer_default(),
            },
            ReviewerInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::reviewer_default(),
            },
        ];

        for developer in developers {
            for reviewer in reviewers.clone() {
                let temp = tempfile::tempdir().unwrap();
                let repository = initialize_repository(temp.path());
                let locks = private_directory(&temp.path().join("locks"));
                let mut supervisor = open_supervisor(
                    temp.path(),
                    &repository,
                    "normal",
                    "run-role-combination",
                    &locks,
                );
                let profiles = SessionInvocationProfiles {
                    developer: developer.clone(),
                    reviewer,
                    ..SessionInvocationProfiles::default()
                };
                let developer_adapter = profiles.developer_adapter_name();
                let reviewer_adapter = profiles.reviewer_adapter_name();
                supervisor.sources.profiles = Some(profiles);
                supervisor
                    .replace_plan(0, developer_adapter, reviewer_adapter, {
                        let mut task = task("one", 1);
                        task.repository_root = repository.to_string_lossy().into_owned();
                        vec![task]
                    })
                    .unwrap();
                assert_eq!(supervisor.state, SessionState::AwaitingApproval);
            }
        }
    }

    #[test]
    #[ignore = "requires pinned real Codex, current auth, and network access"]
    fn real_codex_fibonacci_two_task_session_reaches_reviewed_completion() {
        if !Path::new(crate::worker::codex::CODEX_DEVELOPER_EXECUTABLE).exists() {
            return;
        }
        let host_runtime = fs::canonicalize(
            std::env::var_os("XDG_RUNTIME_DIR")
                .map(PathBuf::from)
                .expect("real acceptance requires XDG_RUNTIME_DIR"),
        )
        .unwrap();
        let mut parent_values = BTreeMap::new();
        for name in [
            "ALL_PROXY",
            "CARGO_HOME",
            "CODEX_HOME",
            "HOME",
            "HTTPS_PROXY",
            "HTTP_PROXY",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "NO_PROXY",
            "PATH",
            "RUSTUP_HOME",
            "SSL_CERT_DIR",
            "SSL_CERT_FILE",
            "TERM",
            "TZ",
            "all_proxy",
            "http_proxy",
            "https_proxy",
            "no_proxy",
        ] {
            if let Ok(value) = std::env::var(name) {
                parent_values.insert(name.into(), value);
            }
        }
        let profiles = SessionInvocationProfiles::default();
        let sources =
            SessionRuntimeSources::capture(parent_values, host_runtime, profiles.clone()).unwrap();
        let temp = tempfile::Builder::new()
            .prefix("hcom-real-fibonacci.")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project = root.join("project");
        let repository_parent = project.join("demo");
        fs::create_dir_all(&repository_parent).unwrap();
        let repository = initialize_repository(&repository_parent);
        let run_root = private_directory(&root.join("run"));
        let lock_root = private_directory(&root.join("locks"));
        let mut supervisor = SessionSupervisor::open_with(
            "run-real-fibonacci".into(),
            fs::canonicalize(&project).unwrap(),
            run_root,
            lock_root,
            sources,
            WorkerAdapterRegistry::default(),
            ProcessRunner::default(),
        )
        .unwrap();
        let tasks = vec![
            TaskDraft {
                task_key: "implement-fibonacci".into(),
                title: "Implement the Fibonacci calculator".into(),
                objective: concat!(
                    "Create a dependency-free Python Fibonacci library and CLI. ",
                    "Define fibonacci(n: int) -> int with F(0)=0 and F(1)=1 using an ",
                    "iterative O(n) algorithm with constant auxiliary space. Reject ",
                    "booleans, non-integers, and negative integers. Support ",
                    "`python3 -m src.fib_cli N`: valid input prints only the result and ",
                    "invalid input writes a concise error to stderr and exits nonzero. ",
                    "Document library and CLI usage in README.md."
                )
                .into(),
                repository_root: repository.to_string_lossy().into_owned(),
                acceptance_criteria: vec![
                    "the library and CLI behavior match the approved objective".into(),
                    "the developer commits a clean task HEAD".into(),
                ],
                required_checks: vec![
                    "python3 -m py_compile src/__init__.py src/fibonacci.py src/fib_cli.py".into(),
                ],
                allowed_paths: vec![
                    "src/__init__.py".into(),
                    "src/fibonacci.py".into(),
                    "src/fib_cli.py".into(),
                    "README.md".into(),
                ],
                forbidden_actions: vec![
                    "push, install dependencies, reset, rebase, merge, or edit hcom config".into(),
                ],
                max_review_rounds: 2,
            },
            TaskDraft {
                task_key: "test-fibonacci".into(),
                title: "Add the Fibonacci test suite".into(),
                objective: concat!(
                    "Add standard-library unittest coverage for F(0), F(1), F(2), ",
                    "F(10), and a larger value; rejected negative, boolean, and ",
                    "non-integer library inputs; and CLI subprocess cases for valid, ",
                    "negative, non-integer, missing, and extra input. Keep tests ",
                    "deterministic, offline, and dependency-free. Add the exact test ",
                    "command to README.md."
                )
                .into(),
                repository_root: repository.to_string_lossy().into_owned(),
                acceptance_criteria: vec![
                    "the approved unittest and CLI cases pass".into(),
                    "the final repository is clean at a committed HEAD".into(),
                ],
                required_checks: vec![
                    "python3 -m unittest discover -s tests -v".into(),
                    "python3 -m src.fib_cli 10".into(),
                ],
                allowed_paths: vec![
                    "tests/__init__.py".into(),
                    "tests/test_fibonacci.py".into(),
                    "README.md".into(),
                ],
                forbidden_actions: vec![
                    "push, install dependencies, reset, rebase, merge, or edit hcom config".into(),
                ],
                max_review_rounds: 2,
            },
        ];
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                profiles.developer_adapter_name(),
                profiles.reviewer_adapter_name(),
                tasks,
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(20 * 60);
        let mut poll_error = None;
        while supervisor.snapshot().state == SessionState::Running {
            if let Err(error) = supervisor.poll_once() {
                poll_error = Some(format!("{error:#}"));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "real Fibonacci session timed out: {:?}",
                supervisor.snapshot()
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        let snapshot = supervisor.snapshot();
        if snapshot.state != SessionState::Completed {
            let preserved = temp.keep();
            panic!(
                "real Fibonacci session did not complete; preserved private evidence at {}: \
{snapshot:?}; poll_error={poll_error:?}",
                preserved.display(),
            );
        }
        assert_eq!(snapshot.state, SessionState::Completed, "{snapshot:?}");
        assert_eq!(snapshot.tasks.len(), 2);
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.state == TaskState::Lgtm)
        );
        assert_eq!(supervisor.metrics().max_live_workers, 1);
        assert_eq!(
            String::from_utf8(git(&repository, &["status", "--porcelain=v1"]))
                .unwrap()
                .trim(),
            ""
        );
        let tests = Command::new("/usr/bin/python3")
            .args(["-m", "unittest", "discover", "-s", "tests", "-v"])
            .current_dir(&repository)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .output()
            .unwrap();
        assert!(
            tests.status.success(),
            "final unittest failed: {}",
            String::from_utf8_lossy(&tests.stderr)
        );
        let cli = Command::new("/usr/bin/python3")
            .args(["-m", "src.fib_cli", "10"])
            .current_dir(&repository)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .output()
            .unwrap();
        assert!(cli.status.success());
        assert_eq!(cli.stdout, b"55\n");
    }

    fn poll_until(
        supervisor: &mut SessionSupervisor,
        predicate: impl Fn(&SessionSupervisor) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !predicate(supervisor) {
            let result = supervisor.poll_once();
            if let Err(error) = &result
                && supervisor.snapshot().state != SessionState::NeedsHuman
            {
                panic!("unexpected non-terminal poll error: {error:#}");
            }
            assert!(
                Instant::now() < deadline,
                "session supervisor timed out: {:?}; active={:?}; retry={:?}",
                supervisor.snapshot(),
                supervisor.active.as_ref().map(|active| (
                    active.task_index,
                    active.role,
                    active.turn_sequence,
                    active.attempt,
                )),
                supervisor.retry.as_ref().map(|retry| (
                    retry.task_index,
                    retry.role,
                    retry.turn_sequence,
                    retry.next_attempt,
                ))
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn two_task_e2e_uses_fresh_cross_task_sessions_and_exact_same_task_resume() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor = open_supervisor(&root, &repository, "normal", "run", &locks);
        approve(&mut supervisor, vec![task("one", 2), task("two", 2)]);
        poll_until(&mut supervisor, |supervisor| {
            supervisor.snapshot().state.is_terminal()
        });

        let snapshot = supervisor.snapshot();
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "unexpected session snapshot: {snapshot:?}"
        );
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert_eq!(snapshot.tasks[0].review_round, 2);
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert_eq!(snapshot.tasks[1].review_round, 1);
        assert_eq!(supervisor.metrics().max_live_workers, 1);
        assert_eq!(supervisor.metrics().current_workers, 0);
        assert!(
            supervisor
                .spawn_audit()
                .iter()
                .all(|spawn| !spawn.prompt_in_argv && spawn.workspace_cwd == repository)
        );
        assert!(
            supervisor
                .spawn_audit()
                .iter()
                .all(|spawn| matches!(spawn.task_key.as_str(), "one" | "two")),
            "a task outside the exact approved two-task plan was spawned"
        );

        for role in [WorkerRole::Developer, WorkerRole::Reviewer] {
            let one: Vec<_> = supervisor
                .spawn_audit()
                .iter()
                .filter(|spawn| spawn.task_key == "one" && spawn.role == role)
                .collect();
            let two: Vec<_> = supervisor
                .spawn_audit()
                .iter()
                .filter(|spawn| spawn.task_key == "two" && spawn.role == role)
                .collect();
            assert_eq!(one.len(), 2);
            assert_eq!(two.len(), 1);
            assert_eq!(one[0].logical_session_id, one[1].logical_session_id);
            assert_eq!(one[0].native_session_id, one[1].native_session_id);
            assert!(!one[0].resume);
            assert!(one[1].resume);
            assert_ne!(one[0].logical_session_id, two[0].logical_session_id);
            assert_ne!(one[0].native_session_id, two[0].native_session_id);
        }
        assert_eq!(
            String::from_utf8(git(&repository, &["status", "--porcelain=v1"])).unwrap(),
            ""
        );
        drop(supervisor);
        drop(temp);
    }

    #[test]
    fn worker_prompts_enforce_one_role_and_one_review_stage_per_turn() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor = open_supervisor(&root, &repository, "normal", "run", &locks);
        approve(&mut supervisor, vec![task("one", 3)]);

        let initial: serde_json::Value = serde_json::from_slice(
            &supervisor
                .build_turn_prompt(0, WorkerRole::Developer, 0)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(initial["role"], "developer");
        assert_eq!(initial["turn_phase"], "initial_development");
        assert!(initial["prior_review"].is_null());
        let initial_requirements = initial["requirements"].as_array().unwrap();
        assert!(
            initial_requirements
                .iter()
                .any(|requirement| requirement.as_str().unwrap().contains("no reviewer turn"))
        );
        assert!(initial_requirements.iter().any(|requirement| {
            requirement
                .as_str()
                .unwrap()
                .contains("sub-agent or reviewer")
        }));
        assert!(initial_requirements.iter().any(|requirement| {
            requirement
                .as_str()
                .unwrap()
                .contains("conditional on a future review")
        }));
        assert!(initial_requirements.iter().any(|requirement| {
            requirement
                .as_str()
                .unwrap()
                .contains("chronological base_revision..HEAD")
        }));

        supervisor.tasks[0].last_review = Some(ReviewerResult {
            decision: ReviewDecision::RequestChanges,
            summary: "fix the bounded finding".into(),
            findings: vec![crate::worker::result::ReviewFinding {
                severity: crate::worker::result::FindingSeverity::Major,
                title: "bounded finding".into(),
                body: "change only the approved file".into(),
                file: Some("one.txt".into()),
                line: Some(1),
            }],
            checks: vec![],
        });
        let resumed: serde_json::Value = serde_json::from_slice(
            &supervisor
                .build_turn_prompt(0, WorkerRole::Developer, 1)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(resumed["turn_phase"], "fix_after_request_changes");
        assert_eq!(
            resumed["prior_review"]["decision"],
            serde_json::json!("request_changes")
        );
        assert!(
            resumed["requirements"]
                .as_array()
                .unwrap()
                .iter()
                .any(|requirement| requirement
                    .as_str()
                    .unwrap()
                    .contains("supplied prior_review findings"))
        );
        assert!(
            resumed["requirements"]
                .as_array()
                .unwrap()
                .iter()
                .any(|requirement| requirement
                    .as_str()
                    .unwrap()
                    .contains("never report only the current resumed turn delta"))
        );

        let reviewer: serde_json::Value = serde_json::from_slice(
            &supervisor
                .build_turn_prompt(0, WorkerRole::Reviewer, 1)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(reviewer["role"], "reviewer");
        assert_eq!(reviewer["turn_phase"], "independent_review");
        assert!(reviewer["prior_review"].is_null());
        assert!(
            reviewer["requirements"]
                .as_array()
                .unwrap()
                .iter()
                .any(|requirement| requirement
                    .as_str()
                    .unwrap()
                    .contains("sub-agent or developer"))
        );
    }

    #[test]
    fn review_exhausted_is_not_lgtm_and_still_advances_to_the_next_task() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor = open_supervisor(&root, &repository, "exhaust", "run", &locks);
        approve(&mut supervisor, vec![task("one", 1), task("two", 1)]);
        poll_until(&mut supervisor, |supervisor| {
            supervisor.snapshot().state.is_terminal()
        });

        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_ne!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert!(
            snapshot.tasks[0]
                .outcome_detail
                .as_deref()
                .is_some_and(|detail| detail.contains("request_changes"))
        );
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert!(
            supervisor
                .spawn_audit()
                .iter()
                .any(|spawn| spawn.task_key == "two" && spawn.role == WorkerRole::Developer)
        );
        drop(supervisor);
        drop(temp);
    }

    #[test]
    fn crash_retry_resumes_the_exact_native_session_with_a_bounded_attempt() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor = open_supervisor(&root, &repository, "crash-once", "run", &locks);
        approve(&mut supervisor, vec![task("one", 2)]);
        poll_until(&mut supervisor, |supervisor| {
            supervisor.snapshot().state.is_terminal()
        });

        assert_eq!(supervisor.snapshot().state, SessionState::Completed);
        assert_eq!(supervisor.metrics().worker_retries, 1);
        let developer: Vec<_> = supervisor
            .spawn_audit()
            .iter()
            .filter(|spawn| spawn.role == WorkerRole::Developer)
            .collect();
        assert!(developer.len() >= 2);
        assert_eq!(developer[0].turn_sequence, 1);
        assert_eq!(developer[1].turn_sequence, 1);
        assert_eq!(developer[0].attempt, 1);
        assert_eq!(developer[1].attempt, 2);
        assert!(!developer[0].resume);
        assert!(developer[1].resume);
        assert_eq!(
            developer[0].logical_session_id,
            developer[1].logical_session_id
        );
        assert_eq!(
            developer[0].native_session_id,
            developer[1].native_session_id
        );
        drop(supervisor);
        drop(temp);
    }

    #[test]
    fn reviewer_head_drift_stops_the_run_and_rejects_the_old_verdict() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor = open_supervisor(&root, &repository, "slow-review", "run", &locks);
        approve(&mut supervisor, vec![task("one", 2)]);
        poll_until(&mut supervisor, |supervisor| {
            supervisor
                .active
                .as_ref()
                .is_some_and(|active| active.role == WorkerRole::Reviewer)
        });

        fs::write(repository.join("external.txt"), "external drift\n").unwrap();
        git(&repository, &["add", "--", "external.txt"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=External Fixture",
                "-c",
                "user.email=external-fixture@example.invalid",
                "commit",
                "-m",
                "External drift",
            ],
        );
        poll_until(&mut supervisor, |supervisor| {
            supervisor.snapshot().state == SessionState::NeedsHuman
        });
        assert_eq!(supervisor.snapshot().tasks[0].state, TaskState::NeedsHuman);
        assert_ne!(supervisor.snapshot().tasks[0].state, TaskState::Lgtm);
        drop(supervisor);
        drop(temp);
    }

    #[test]
    fn same_task_developer_resume_cannot_rewrite_the_previous_review_head() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let locks = private_directory(&root.join("locks"));
        let mut supervisor =
            open_supervisor(&root, &repository, "rewrite-on-resume", "run", &locks);
        approve(&mut supervisor, vec![task("one", 2)]);
        poll_until(&mut supervisor, |supervisor| {
            supervisor.snapshot().state == SessionState::NeedsHuman
        });

        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_ne!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert_eq!(
            snapshot.tasks[0].review_round,
            1,
            "unexpected early stop: {snapshot:?}; audit={:?}",
            supervisor.spawn_audit()
        );
        drop(supervisor);
        drop(temp);
    }

    #[test]
    fn dirty_start_and_second_same_repository_session_fail_closed() {
        let dirty_temp = tempfile::tempdir().unwrap();
        let dirty_root = fs::canonicalize(dirty_temp.path()).unwrap();
        let dirty_repository = initialize_repository(&dirty_root);
        fs::write(dirty_repository.join("dirty.txt"), "dirty\n").unwrap();
        let dirty_locks = private_directory(&dirty_root.join("locks"));
        let mut dirty = open_supervisor(
            &dirty_root,
            &dirty_repository,
            "normal",
            "run-dirty",
            &dirty_locks,
        );
        let mut dirty_task = task("dirty", 1);
        dirty_task.repository_root = dirty_repository.to_string_lossy().into_owned();
        assert!(
            dirty
                .replace_plan(0, "fake-envelope", "fake-envelope", vec![dirty_task])
                .is_err()
        );

        let lock_temp = tempfile::tempdir().unwrap();
        let lock_root = fs::canonicalize(lock_temp.path()).unwrap();
        let repository = initialize_repository(&lock_root);
        let locks = private_directory(&lock_root.join("locks"));
        let mut first = open_supervisor(&lock_root, &repository, "normal", "run-one", &locks);
        let mut first_task = task("first", 1);
        first_task.repository_root = repository.to_string_lossy().into_owned();
        first
            .replace_plan(0, "fake-envelope", "fake-envelope", vec![first_task])
            .unwrap();
        let mut second = open_supervisor(&lock_root, &repository, "normal", "run-two", &locks);
        let mut second_task = task("second", 1);
        second_task.repository_root = repository.to_string_lossy().into_owned();
        assert!(
            second
                .replace_plan(0, "fake-envelope", "fake-envelope", vec![second_task])
                .is_err()
        );
        let renamed_repository = lock_root.join("renamed-repository");
        fs::rename(&repository, &renamed_repository).unwrap();
        let renamed_repository = fs::canonicalize(renamed_repository).unwrap();
        let mut renamed = open_supervisor(
            &lock_root,
            &renamed_repository,
            "normal",
            "run-renamed",
            &lock_root.join("locks"),
        );
        let mut renamed_task = task("renamed", 1);
        renamed_task.repository_root = renamed_repository.to_string_lossy().into_owned();
        assert!(
            renamed
                .replace_plan(0, "fake-envelope", "fake-envelope", vec![renamed_task],)
                .is_err(),
            "renaming the same checkout inode must not bypass its live runtime lock"
        );
        drop(first);
    }

    #[test]
    fn duplicate_and_late_completion_tokens_apply_at_most_once() {
        let mut gate = CompletionGate::default();
        gate.begin("turn-one").unwrap();
        let mut advances = 0;
        if gate.accept("turn-one") {
            advances += 1;
        }
        if gate.accept("turn-one") {
            advances += 1;
        }
        assert_eq!(advances, 1);
        assert!(gate.begin("turn-one").is_err());

        gate.begin("turn-two").unwrap();
        assert!(!gate.accept("turn-one"));
        assert!(gate.accept("turn-two"));
    }

    #[test]
    fn changed_path_scope_is_an_exact_or_descendant_allowlist() {
        assert!(
            require_changed_paths_in_scope(&["src/orchestrator/mod.rs".into()], &["src".into()])
                .is_ok()
        );
        assert!(
            require_changed_paths_in_scope(&["tests/outside.rs".into()], &["src".into()]).is_err()
        );
    }

    #[test]
    fn explicitly_required_checks_cannot_be_omitted_or_left_incomplete() {
        let required: Vec<String> = vec!["cargo test --all-targets".into()];
        let passed = CheckResult {
            command: required[0].clone(),
            status: CheckStatus::Passed,
            summary: "passed".into(),
        };
        assert!(require_checks_passed("developer", &required, &[passed]).is_ok());
        assert!(require_checks_passed("developer", &required, &[]).is_err());
        let failed = CheckResult {
            command: required[0].clone(),
            status: CheckStatus::Failed,
            summary: "failed".into(),
        };
        assert!(require_checks_passed("reviewer", &required, &[failed]).is_err());
    }

    #[test]
    fn session_worker_source_preserves_complete_parent_environment() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let mut sources = SessionRuntimeSources::fake(&root);
        let mut parent_values = BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]);
        for (name, value) in [
            ("HTTPS_PROXY", "http://upper.invalid"),
            ("https_proxy", "http://lower.invalid"),
            ("HTTP_PROXY", "http://upper-http.invalid"),
            ("http_proxy", "http://lower-http.invalid"),
            ("ALL_PROXY", "socks5://upper.invalid"),
            ("all_proxy", "socks5://lower.invalid"),
            ("NO_PROXY", "UPPER.internal"),
            ("no_proxy", "lower.internal"),
            ("ARBITRARY_PARENT_VALUE", "arbitrary-value"),
            ("SERVICE_ACCESS_TOKEN", "secret-shaped-value"),
            ("EMPTY_PARENT_VALUE", ""),
        ] {
            parent_values.insert(name.into(), value.into());
        }
        sources.parent_environment = ParentEnvironment::from_unicode(parent_values);
        let paths = WorkerEnvironmentPaths {
            home: root.clone(),
            native_config: root.clone(),
            temp: root.clone(),
            runtime: root.clone(),
            xdg_config: root.clone(),
            xdg_state: root.clone(),
            xdg_cache: root.clone(),
            xdg_data: root.clone(),
        };
        let lease = sources
            .environment_for(
                "fake-envelope",
                "epoch-proxy",
                "run-proxy",
                "task-proxy",
                &paths,
            )
            .unwrap();
        let materialized = lease
            .materialize(
                "epoch-proxy",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-proxy".into(),
                    task_id: "task-proxy".into(),
                },
            )
            .unwrap();
        for (name, value) in [
            ("HTTPS_PROXY", "http://upper.invalid"),
            ("https_proxy", "http://lower.invalid"),
            ("HTTP_PROXY", "http://upper-http.invalid"),
            ("http_proxy", "http://lower-http.invalid"),
            ("ALL_PROXY", "socks5://upper.invalid"),
            ("all_proxy", "socks5://lower.invalid"),
            ("NO_PROXY", "UPPER.internal"),
            ("no_proxy", "lower.internal"),
            ("ARBITRARY_PARENT_VALUE", "arbitrary-value"),
            ("SERVICE_ACCESS_TOKEN", "secret-shaped-value"),
            ("EMPTY_PARENT_VALUE", ""),
        ] {
            assert_eq!(
                materialized.get(std::ffi::OsStr::new(name)),
                Some(std::ffi::OsStr::new(value))
            );
        }
    }

    #[test]
    fn parent_terminal_native_config_overrides_are_frozen_as_auth_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let home = private_directory(&root.join("home"));
        let codex_home = private_directory(&root.join("parent-codex"));
        let claude_home = private_directory(&root.join("parent-claude"));
        let cargo_home = private_directory(&root.join("parent-cargo"));
        private_directory(&cargo_home.join("bin"));
        let rustup_home = private_directory(&root.join("parent-rustup"));
        let runtime = private_directory(&root.join("parent-runtime"));
        let codex_auth = codex_home.join("auth.json");
        let claude_auth = claude_home.join(".credentials.json");
        fs::write(
            &codex_auth,
            b"{\"tokens\":{\"access_token\":\"codex-test\"}}",
        )
        .unwrap();
        fs::write(
            &claude_auth,
            b"{\"claudeAiOauth\":{\"accessToken\":\"claude-test\"}}",
        )
        .unwrap();
        for path in [&codex_auth, &claude_auth] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let text = |path: &Path| path.to_str().unwrap().to_owned();
        let parent_values = BTreeMap::from([
            ("HOME".into(), text(&home)),
            ("CODEX_HOME".into(), text(&codex_home)),
            ("CLAUDE_CONFIG_DIR".into(), text(&claude_home)),
            ("CARGO_HOME".into(), text(&cargo_home)),
            ("RUSTUP_HOME".into(), text(&rustup_home)),
            ("PATH".into(), "/usr/bin:/bin".into()),
        ]);
        let profiles = SessionInvocationProfiles {
            reviewer: ReviewerInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::reviewer_default(),
            },
            ..SessionInvocationProfiles::default()
        };
        let sources = SessionRuntimeSources::capture(
            parent_values.clone(),
            runtime.clone(),
            profiles.clone(),
        )
        .unwrap();
        assert_eq!(
            sources.codex_auth_source,
            Some(fs::canonicalize(&codex_auth).unwrap())
        );
        assert_eq!(
            sources.claude_auth_source,
            Some(fs::canonicalize(&claude_auth).unwrap())
        );
        assert_eq!(
            sources.profiles.as_ref().unwrap().canonical_hash(),
            profiles.canonical_hash()
        );

        fs::set_permissions(&claude_auth, fs::Permissions::from_mode(0o666)).unwrap();
        let codex_only = SessionInvocationProfiles {
            reviewer: ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile::reviewer_default(),
            },
            ..SessionInvocationProfiles::default()
        };
        let sources =
            SessionRuntimeSources::capture(parent_values, runtime.clone(), codex_only).unwrap();
        assert_eq!(sources.claude_auth_source, None);

        fs::set_permissions(&claude_auth, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&codex_auth, fs::Permissions::from_mode(0o666)).unwrap();
        let claude_only = SessionInvocationProfiles {
            developer: DeveloperInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::developer_default(),
            },
            reviewer: ReviewerInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::reviewer_default(),
            },
            ..SessionInvocationProfiles::default()
        };
        let parent_values = BTreeMap::from([
            ("HOME".into(), text(&home)),
            ("CODEX_HOME".into(), text(&codex_home)),
            ("CLAUDE_CONFIG_DIR".into(), text(&claude_home)),
            ("CARGO_HOME".into(), text(&cargo_home)),
            ("RUSTUP_HOME".into(), text(&rustup_home)),
            ("PATH".into(), "/usr/bin:/bin".into()),
        ]);
        let sources = SessionRuntimeSources::capture(parent_values, runtime, claude_only).unwrap();
        assert_eq!(sources.codex_auth_source, None);
        assert_eq!(
            sources.claude_auth_source,
            Some(fs::canonicalize(&claude_auth).unwrap())
        );
    }

    #[test]
    fn empty_optional_parent_paths_fall_back_without_filtering_unowned_entries() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let home = private_directory(&root.join("home"));
        let claude_home = private_directory(&home.join(".claude"));
        let cargo_home = private_directory(&home.join(".cargo"));
        private_directory(&cargo_home.join("bin"));
        let rustup_home = private_directory(&home.join(".rustup"));
        let runtime = private_directory(&root.join("runtime"));
        let auth = claude_home.join(".credentials.json");
        fs::write(
            &auth,
            b"{\"claudeAiOauth\":{\"accessToken\":\"claude-test\"}}",
        )
        .unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        let parent = BTreeMap::from([
            ("HOME".into(), home.to_string_lossy().into_owned()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("CODEX_HOME".into(), String::new()),
            ("CLAUDE_CONFIG_DIR".into(), String::new()),
            ("CARGO_HOME".into(), String::new()),
            ("RUSTUP_HOME".into(), String::new()),
        ]);
        let profiles = SessionInvocationProfiles {
            developer: DeveloperInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::developer_default(),
            },
            reviewer: ReviewerInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::reviewer_default(),
            },
            ..SessionInvocationProfiles::default()
        };
        let sources =
            SessionRuntimeSources::capture(parent, runtime, profiles).expect("empty means unset");
        assert_eq!(
            sources.claude_auth_source,
            Some(fs::canonicalize(&auth).unwrap())
        );
        let paths = WorkerEnvironmentPaths {
            home: root.join("worker-home"),
            native_config: root.join("worker-home/.claude"),
            temp: root.join("worker-tmp"),
            runtime: root.join("worker-runtime"),
            xdg_config: root.join("worker-xdg-config"),
            xdg_state: root.join("worker-xdg-state"),
            xdg_cache: root.join("worker-xdg-cache"),
            xdg_data: root.join("worker-xdg-data"),
        };
        let lease = sources
            .environment_for(
                CLAUDE_DEVELOPER_ADAPTER,
                "epoch-empty-parent-paths",
                "run-empty-parent-paths",
                "task-empty-parent-paths",
                &paths,
            )
            .unwrap();
        let materialized = lease
            .materialize(
                "epoch-empty-parent-paths",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-empty-parent-paths".into(),
                    task_id: "task-empty-parent-paths".into(),
                },
            )
            .unwrap();

        assert_eq!(
            materialized.get(std::ffi::OsStr::new("CODEX_HOME")),
            Some(std::ffi::OsStr::new(""))
        );
        assert_eq!(
            materialized.get(std::ffi::OsStr::new("CLAUDE_CONFIG_DIR")),
            Some(paths.native_config.as_os_str())
        );
        assert_eq!(
            materialized.get(std::ffi::OsStr::new("CARGO_HOME")),
            Some(cargo_home.as_os_str())
        );
        assert_eq!(
            materialized.get(std::ffi::OsStr::new("RUSTUP_HOME")),
            Some(rustup_home.as_os_str())
        );
    }

    #[test]
    fn expired_claude_auth_fails_before_any_worker_or_repository_change() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = initialize_repository(&root);
        let start_head = String::from_utf8(git(&repository, &["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_owned();
        let locks = private_directory(&root.join("locks"));
        let run_root = private_directory(&root.join("run"));
        let toolchain = private_directory(&root.join("toolchain"));
        let host_auth_dir = private_directory(&root.join("host-auth"));
        let host_auth = host_auth_dir.join(".credentials.json");
        let auth = br#"{"claudeAiOauth":{"accessToken":"host-access-token-v1","refreshToken":"host-refresh-token-v1","expiresAt":0,"refreshTokenExpiresAt":1}}"#;
        fs::write(&host_auth, auth).unwrap();
        fs::set_permissions(&host_auth, fs::Permissions::from_mode(0o600)).unwrap();
        let before = fs::metadata(&host_auth).unwrap();
        let mut sources = SessionRuntimeSources::fake(&toolchain);
        sources.claude_auth_source = Some(fs::canonicalize(&host_auth).unwrap());

        let mut supervisor = SessionSupervisor::open_with(
            "run-test".into(),
            repository.clone(),
            run_root,
            locks,
            sources,
            WorkerAdapterRegistry::default(),
            ProcessRunner::default(),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan(0, "codex-developer-0.145.0", "claude-reviewer-2.1.220", {
                let mut task = task("one", 2);
                task.repository_root = repository.to_string_lossy().into_owned();
                vec![task]
            })
            .unwrap();
        assert!(
            supervisor
                .approve_and_start(1, plan_version, &plan_hash, true)
                .is_err()
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.current_task_ordinal, None);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("selected worker authentication is unavailable, expired, or too close to expiry")
        );
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].state, TaskState::Pending);
        assert_eq!(supervisor.spawn_audit().len(), 0);
        assert_eq!(
            String::from_utf8(git(&repository, &["rev-parse", "HEAD"]))
                .unwrap()
                .trim()
                .to_owned(),
            start_head
        );
        assert_eq!(fs::read(&host_auth).unwrap(), auth);
        let after = fs::metadata(&host_auth).unwrap();
        assert_eq!(
            (
                before.dev(),
                before.ino(),
                before.nlink(),
                before.len(),
                before.mtime(),
                before.mtime_nsec(),
                before.ctime(),
                before.ctime_nsec(),
                before.permissions().mode() & 0o777,
            ),
            (
                after.dev(),
                after.ino(),
                after.nlink(),
                after.len(),
                after.mtime(),
                after.mtime_nsec(),
                after.ctime(),
                after.ctime_nsec(),
                after.permissions().mode() & 0o777,
            )
        );
    }

    #[test]
    fn worker_outcome_status_is_human_readable_and_strictly_bounded() {
        let result = DeveloperResult {
            decision: DeveloperDecision::NeedsInput,
            summary: "scope is ambiguous".into(),
            head_revision: None,
            commits: vec![],
            checks: vec![],
            questions: vec!["which approved behavior should be used?".repeat(256)],
            risks: vec![],
            changed_paths: vec![],
        };
        let detail = developer_outcome_detail(&result);
        assert!(detail.contains("needs_input"));
        assert!(detail.contains("questions:"));
        assert!(detail.len() <= MAX_STATUS_OUTCOME_BYTES);
        assert!(detail.ends_with('…'));
    }

    #[test]
    fn terminal_worker_error_preserves_bounded_sanitized_cause_chain() {
        let error = anyhow!("exact adapter cause\nwith newline")
            .context("worker result failed\tits contract")
            .context("poll failed");
        let detail = terminal_worker_error(&error);
        assert!(detail.starts_with("session worker loop failed: poll failed:"));
        assert!(detail.contains("worker result failed its contract"));
        assert!(detail.contains("exact adapter cause with newline"));
        assert!(!detail.chars().any(char::is_control));
        assert!(detail.len() <= MAX_STATUS_OUTCOME_BYTES);

        let long = anyhow!("x".repeat(MAX_STATUS_OUTCOME_BYTES * 2));
        let truncated = terminal_worker_error(&long);
        assert!(truncated.len() <= MAX_STATUS_OUTCOME_BYTES);
        assert!(truncated.ends_with('…'));
    }

    #[test]
    fn terminal_process_failure_maps_only_the_exact_safe_codex_diagnostic() {
        let exit = WorkerExit {
            code: Some(1),
            signal: None,
            termination: WorkerTermination::Exited,
            heartbeat_count: 0,
        };
        let exact =
            b"Not inside a trusted directory and --skip-git-repo-check was not specified.\n";
        assert!(terminal_process_failure(&exit, exact).contains("non-Git project cwd"));
        let arbitrary = terminal_process_failure(&exit, b"provider-secret");
        assert!(!arbitrary.contains("provider-secret"));
        assert!(arbitrary.contains("exit_code=Some(1)"));
    }
}
