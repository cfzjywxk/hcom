//! Effect driver for the provider-routed `hcom arch` task worker lane.
//!
//! The driver is the only owner of Git, filesystem, environment, and runtime
//! I/O. Scheduling decisions remain in [`SupervisorCore`].

use super::core::{
    DriverFailure, DriverFailureClass, SupervisorCore, SupervisorEffect, SupervisorEvent,
};
use super::github::{GitHubInspectionRequest, validate_inspection_result};
use super::workspace::{ProjectTasksWorkspace, TasksWorkspace};
use super::{SessionRuntimeSources, SessionStartup, ensure_private_directory, sha256_hex};
use crate::control_api::{
    DeliveryBinding, GitHubInspectionBinding, GitHubRunBinding, ReviewerAdapterBinding,
    ReviewerBindingSnapshot, SessionState, SessionStatusSnapshot, TaskDraft, TaskState, WorkerRole,
};
use crate::worker::claude_exec_runtime::{ClaudeExecRuntimeConfig, ClaudeExecTaskWorkerRuntime};
use crate::worker::environment::{
    EnvironmentPolicy, ExecutionEnvironmentLease, MaterializedWorkerEnvironment, ParentEnvironment,
};
use crate::worker::exec_runtime::{ExecRuntimeConfig, ExecTaskPaths, ExecTaskWorkerRuntime};
use crate::worker::guardian::{CleanupRegistryInterlock, GuardianCleanupRegistry};
use crate::worker::profile::{ArchitectAdapter, SessionInvocationProfiles};
use crate::worker::role_router::{LaneRuntimeSlot, LaneTaskWorkerRuntime, TaskRuntimeBundle};
use crate::worker::runtime::{
    OutcomeContract, RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeErrorCode,
    RuntimeFailureClass, RuntimeProfile, RuntimeProvider, RuntimeSessionKey, RuntimeTurnKey,
    RuntimeTurnPoll, RuntimeTurnPurpose, RuntimeTurnSpec, SanitizedRuntimeFailure,
    TaskWorkerProfiles, WorkerLane,
};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const TURN_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

trait RuntimeFactory: Send {
    fn contract(&self, profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity;

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn LaneTaskWorkerRuntime>, RuntimeError>;
}

struct ProductionRuntimeFactory;

impl ProductionRuntimeFactory {
    fn new() -> Self {
        Self
    }
}

fn session_binding_hash(
    session_profiles: &SessionInvocationProfiles,
    worker_profiles: &TaskWorkerProfiles,
    contract: &RuntimeContractIdentity,
    architect_additional_directories: &[PathBuf],
    delivery_binding: &DeliveryBinding,
) -> Result<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        "hcom-provider-routed-session-binding-v2",
        session_profiles.canonical_hash(),
        worker_profiles.canonical_hash(),
        contract.canonical_hash(),
        architect_additional_directories,
        delivery_binding,
    ))?))
}

fn derive_github_run_binding(
    run_id: &str,
    plan_hash: &str,
    inspection: GitHubInspectionBinding,
) -> GitHubRunBinding {
    // Twelve digest characters keep the branch readable while the complete
    // digest remains bound in plan.md/status and in every later operation.
    let generated_run_branch = format!("hcom/{run_id}-{}", &plan_hash[..12]);
    GitHubRunBinding {
        inspected_repository_id: inspection.inspected_repository_id,
        expected_base_ref: inspection.expected_base_ref,
        expected_base_sha: inspection.expected_base_sha,
        ruleset_attestation_sha256: inspection.ruleset_attestation_sha256,
        inspection_id: inspection.inspection_id,
        generated_run_branch,
    }
}

impl RuntimeFactory for ProductionRuntimeFactory {
    fn contract(&self, profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity {
        profiles.contract_identity()
    }

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn LaneTaskWorkerRuntime>, RuntimeError> {
        request
            .cleanup_registry
            .ensure_available()
            .map_err(|error| RuntimeError::internal(error.to_string()))?;
        if request.task_ordinal >= 64 {
            return Err(RuntimeError::invalid_contract(
                "task runtime ordinal exceeds the session bound",
            ));
        }
        crate::worker::validation::validate_opaque_id("task runtime key", &request.task_key)
            .map_err(|_| RuntimeError::invalid_contract("task runtime key was invalid"))?;
        let RuntimeOpenRequest {
            task_ordinal: _,
            task_key,
            repository_root,
            paths,
            environment,
            lease,
            artifact_root,
            run_id,
            cleanup_registry,
            profiles,
            guardian_executable,
        } = request;
        let uses_claude = profiles.lanes().any(|lane| {
            profiles
                .profile_for_lane(lane)
                .is_ok_and(|profile| profile.provider == RuntimeProvider::ClaudeExec)
        });

        let claude_environment = if uses_claude {
            let parent = ParentEnvironment::from_raw_entries(environment.iter().cloned())
                .map_err(|error| RuntimeError::invalid_contract(error.to_string()))?;
            let materialized = parent
                .materialize_claude()
                .map_err(|error| RuntimeError::invalid_contract(error.to_string()))?;
            Some(materialized_environment(&materialized))
        } else {
            None
        };
        let mut slots = Vec::new();
        for lane in profiles.lanes() {
            let profile = profiles.profile_for_lane(lane)?;
            let runtime: Box<dyn crate::worker::runtime::TaskWorkerRuntime> = match profile.provider
            {
                RuntimeProvider::CodexExec => {
                    Box::new(ExecTaskWorkerRuntime::open(ExecRuntimeConfig {
                        codex: PathBuf::from("codex"),
                        repository_root: repository_root.clone(),
                        paths: ExecTaskPaths {
                            runtime: paths.runtime_for(lane)?.to_path_buf(),
                        },
                        environment: environment.clone(),
                        lease: lease.clone(),
                        artifact_root_path: artifact_root.clone(),
                        run_id: run_id.clone(),
                        task_id: task_key.clone(),
                        reviewer_id: lane.reviewer_id(),
                    })?)
                }
                RuntimeProvider::ClaudeExec => Box::new(ClaudeExecTaskWorkerRuntime::open(
                    ClaudeExecRuntimeConfig {
                        claude: "claude".into(),
                        guardian_executable: guardian_executable.clone(),
                        environment: claude_environment
                            .as_ref()
                            .expect("a Claude binding materializes its role environment")
                            .clone(),
                        lease: lease.clone(),
                        artifact_root_path: artifact_root.clone(),
                        run_id: run_id.clone(),
                        task_id: task_key.clone(),
                        reviewer_id: lane.reviewer_id(),
                        cleanup_registry: cleanup_registry.clone(),
                    },
                )?),
            };
            slots.push(LaneRuntimeSlot::available(lane, runtime)?);
        }
        Ok(Box::new(TaskRuntimeBundle::new(&profiles, slots)?))
    }
}

struct RuntimeOpenRequest {
    task_ordinal: usize,
    task_key: String,
    repository_root: PathBuf,
    paths: TaskRuntimePaths,
    environment: Vec<(OsString, OsString)>,
    lease: ExecutionEnvironmentLease,
    artifact_root: PathBuf,
    run_id: String,
    cleanup_registry: GuardianCleanupRegistry,
    profiles: TaskWorkerProfiles,
    guardian_executable: PathBuf,
}

struct RuntimeOpenFailure {
    class: DriverFailureClass,
    error: anyhow::Error,
}

impl RuntimeOpenFailure {
    fn new(class: DriverFailureClass, error: impl Into<anyhow::Error>) -> Self {
        Self {
            class,
            error: error.into(),
        }
    }
}

struct TaskRuntimePaths {
    runtimes: BTreeMap<WorkerLane, PathBuf>,
}

impl TaskRuntimePaths {
    fn create(
        run_root: &Path,
        task_ordinal: usize,
        task_key: &str,
        _repository_root: &Path,
        lanes: impl IntoIterator<Item = WorkerLane>,
    ) -> Result<(TempDir, Self)> {
        let workers = run_root.join("exec-workers");
        ensure_private_directory(&workers)?;
        let root = tempfile::Builder::new()
            .prefix(&format!("task-{task_ordinal}-{task_key}."))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(&workers)
            .context("failed to create task-private exec worker root")?;
        let root_path = fs::canonicalize(root.path())?;
        let runtime_root = root_path.join("run");
        fs::create_dir(&runtime_root).with_context(|| {
            format!(
                "failed to create task-private exec worker root {}",
                runtime_root.display()
            )
        })?;
        fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))?;
        let mut runtimes = BTreeMap::new();
        for lane in lanes {
            let runtime = runtime_root.join(lane.as_str());
            fs::create_dir(&runtime).with_context(|| {
                format!(
                    "failed to create {} task-private worker directory {}",
                    lane.as_str(),
                    runtime.display()
                )
            })?;
            fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700))?;
            if runtimes.insert(lane, runtime).is_some() {
                bail!("task-private worker runtime lane was duplicated");
            }
        }
        let paths = Self { runtimes };
        Ok((root, paths))
    }

    fn runtime_for(&self, lane: WorkerLane) -> Result<&Path, RuntimeError> {
        self.runtimes
            .get(&lane)
            .map(PathBuf::as_path)
            .ok_or_else(|| {
                RuntimeError::invalid_contract(format!(
                    "{} task runtime path is missing",
                    lane.as_str()
                ))
            })
    }
}

struct OpenTaskRuntime {
    task_ordinal: usize,
    _root: TempDir,
    runtime: Box<dyn LaneTaskWorkerRuntime>,
    sessions: BTreeMap<RuntimeSessionKey, LocalSession>,
}

#[derive(Clone, Copy)]
struct LocalSession {
    lane: WorkerLane,
    key: RuntimeSessionKey,
}

#[derive(Clone)]
struct ActiveTurn {
    task_ordinal: usize,
    lane: WorkerLane,
    review_generation: Option<u32>,
    logical_session: RuntimeSessionKey,
    logical_turn: RuntimeTurnKey,
    local_turn: RuntimeTurnKey,
    completion_token: String,
}

enum PendingGitHubResult {
    MergeGate(Result<crate::orchestrator::github::MergeGateObservation>),
    Merge(Result<crate::orchestrator::github::PullRequestMergedObservation>),
}

#[derive(Clone, Copy)]
enum PendingGitHubKind {
    MergeGate,
    Merge,
}

impl PendingGitHubKind {
    fn failure(self, error: anyhow::Error) -> PendingGitHubResult {
        match self {
            Self::MergeGate => PendingGitHubResult::MergeGate(Err(error)),
            Self::Merge => PendingGitHubResult::Merge(Err(error)),
        }
    }
}

struct PendingGitHubEffect {
    operation_id: String,
    kind: PendingGitHubKind,
    cancelled: Arc<AtomicBool>,
    receiver: mpsc::Receiver<PendingGitHubResult>,
    join: std::thread::JoinHandle<()>,
}

pub(crate) struct TaskLaneSupervisor {
    startup: SessionStartup,
    epoch: String,
    core: SupervisorCore,
    run_root: PathBuf,
    sources: SessionRuntimeSources,
    profiles: TaskWorkerProfiles,
    developer_adapter: String,
    reviewer_adapters: Vec<ReviewerAdapterBinding>,
    factory: Box<dyn RuntimeFactory>,
    task_runtime: Option<OpenTaskRuntime>,
    active: BTreeMap<WorkerLane, ActiveTurn>,
    next_session: u64,
    next_turn: u64,
    project_tasks_workspace: Option<ProjectTasksWorkspace>,
    tasks_workspace: Option<TasksWorkspace>,
    cleanup_registry: GuardianCleanupRegistry,
    latest_github_inspection: Option<GitHubInspectionBinding>,
    seen_github_inspection_ids: BTreeSet<String>,
    pending_github_effect: Option<PendingGitHubEffect>,
}

impl TaskLaneSupervisor {
    pub(crate) fn open(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        sources: SessionRuntimeSources,
    ) -> Result<Self> {
        Self::open_with_factory(
            run_id,
            project_root,
            run_root,
            sources,
            Box::new(ProductionRuntimeFactory::new()),
        )
    }

    fn open_with_factory(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        sources: SessionRuntimeSources,
        factory: Box<dyn RuntimeFactory>,
    ) -> Result<Self> {
        crate::worker::validation::validate_opaque_id("run id", &run_id)?;
        let project_root = super::canonical_project_directory(&project_root)?;
        let run_root = super::canonical_private_directory(&run_root, "session runtime root")?;
        let session_profiles = sources
            .profiles
            .clone()
            .ok_or_else(|| anyhow!("the provider-routed worker lane requires loaded profiles"))?;
        let profiles = TaskWorkerProfiles::from_session_profiles(&session_profiles)
            .map_err(|error| anyhow!(error.detail))?;
        profiles.validate().map_err(|error| anyhow!(error.detail))?;
        let contract = factory.contract(&profiles);
        contract.validate().map_err(|error| anyhow!(error.detail))?;
        let binding_hash = session_binding_hash(
            &session_profiles,
            &profiles,
            &contract,
            &sources.architect_additional_directories,
            &sources.delivery_binding,
        )?;
        let startup = SessionStartup {
            run_id: run_id.clone(),
            project_root: project_root.clone(),
            session_binding_hash: binding_hash.clone(),
        };
        let reviewer_bindings = profiles
            .reviewers
            .iter()
            .map(|binding| ReviewerBindingSnapshot {
                reviewer_id: binding.reviewer_id,
                provider: binding.profile.provider.as_str().into(),
                model: binding.profile.model.clone(),
                reasoning_effort: binding.profile.reasoning_effort.clone(),
                contract_sha256: binding.profile.provider.contract_identity().contract_sha256,
            })
            .collect();
        let core = SupervisorCore::new_with_delivery_binding(
            run_id,
            project_root,
            binding_hash,
            reviewer_bindings,
            sources.delivery_binding.clone(),
        )
        .map_err(|error| anyhow!(error.to_string()))?;
        let developer_adapter = profiles.developer.provider.as_str().into();
        let reviewer_adapters = profiles
            .reviewers
            .iter()
            .map(|binding| ReviewerAdapterBinding {
                reviewer_id: binding.reviewer_id,
                adapter: binding.profile.provider.as_str().into(),
            })
            .collect();
        let latest_github_inspection = sources
            .github_runtime
            .as_ref()
            .map(|runtime| runtime.initial_inspection.clone());
        let seen_github_inspection_ids = latest_github_inspection
            .as_ref()
            .map(|inspection| BTreeSet::from([inspection.inspection_id.clone()]))
            .unwrap_or_default();
        Ok(Self {
            startup,
            epoch: format!("exec-supervisor-{}", Uuid::new_v4()),
            core,
            run_root,
            sources,
            profiles,
            developer_adapter,
            reviewer_adapters,
            factory,
            task_runtime: None,
            active: BTreeMap::new(),
            next_session: 1,
            next_turn: 1,
            project_tasks_workspace: None,
            tasks_workspace: None,
            cleanup_registry: GuardianCleanupRegistry::default(),
            latest_github_inspection,
            seen_github_inspection_ids,
            pending_github_effect: None,
        })
    }

    pub(crate) fn startup(&self) -> &SessionStartup {
        &self.startup
    }

    pub(crate) fn begin_next_run(
        &mut self,
        expected_session_version: u64,
        terminal_run_id: &str,
    ) -> Result<()> {
        let run_id = format!("run-{}", Uuid::new_v4().simple());
        self.begin_next_run_with_id(expected_session_version, terminal_run_id, run_id)
    }

    fn begin_next_run_with_id(
        &mut self,
        expected_session_version: u64,
        terminal_run_id: &str,
        run_id: String,
    ) -> Result<()> {
        if expected_session_version != self.core.version() {
            bail!("session version is stale");
        }
        if terminal_run_id != self.core.run_id() {
            bail!("terminal run identity is stale");
        }
        if !self.core.session_state().is_terminal() {
            bail!("a new run requires a terminal current run");
        }
        if self.task_runtime.is_some() || !self.active.is_empty() {
            bail!("terminal run retained a live task runtime");
        }
        if self.pending_github_effect.is_some() {
            bail!("terminal run retained an in-flight GitHub merge operation");
        }
        self.cleanup_registry.ensure_available()?;
        crate::worker::validation::validate_opaque_id("run id", &run_id)?;

        let next_core = self
            .core
            .next_run(run_id.clone())
            .map_err(|error| anyhow!(error.to_string()))?;
        if let Some(runtime) = self.sources.github_runtime.as_ref() {
            runtime
                .workflow
                .begin_fresh_run(terminal_run_id, &run_id)
                .context("failed to reset GitHub workflow for the fresh run")?;
        }
        self.startup.run_id = run_id;
        self.epoch = format!("exec-supervisor-{}", Uuid::new_v4());
        self.core = next_core;
        self.next_session = 1;
        self.next_turn = 1;
        self.tasks_workspace = None;
        self.latest_github_inspection = None;
        self.seen_github_inspection_ids.clear();
        Ok(())
    }

    pub(crate) fn inspect_github_delivery(
        &mut self,
        expected_session_version: u64,
        run_id: &str,
    ) -> Result<GitHubInspectionBinding> {
        if expected_session_version != self.core.version() {
            bail!("session version is stale");
        }
        if run_id != self.core.run_id() {
            bail!("GitHub inspection run identity is stale");
        }
        if !matches!(
            self.core.session_state(),
            SessionState::AwaitingPlan | SessionState::AwaitingApproval
        ) {
            bail!("GitHub delivery inspection is available only before run start");
        }
        let runtime =
            self.sources.github_runtime.as_ref().ok_or_else(|| {
                anyhow!("GitHub delivery inspection is unavailable in local mode")
            })?;
        // Once a refresh is requested, the previously retained observation can
        // no longer authorize start. This stays fail-closed even when the
        // provider fails or violates its contract by reusing an inspection ID.
        self.latest_github_inspection = None;
        let result = runtime.inspector.inspect(&GitHubInspectionRequest {
            run_id: run_id.into(),
            session_version: expected_session_version,
            delivery_binding: runtime.binding.clone(),
        })?;
        validate_inspection_result(&runtime.binding, &result)?;
        if !self
            .seen_github_inspection_ids
            .insert(result.inspection.inspection_id.clone())
        {
            bail!("GitHub delivery inspection did not return a new inspection ID");
        }
        self.latest_github_inspection = Some(result.inspection.clone());
        Ok(result.inspection)
    }

    #[cfg(test)]
    pub(crate) fn replace_plan(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapters: &[ReviewerAdapterBinding],
        tasks: Vec<TaskDraft>,
    ) -> Result<(u64, String)> {
        self.replace_plan_with_inspection(
            expected_session_version,
            developer_adapter,
            reviewer_adapters,
            None,
            tasks,
        )
    }

    pub(crate) fn replace_plan_with_inspection(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapters: &[ReviewerAdapterBinding],
        github_inspection_id: Option<&str>,
        tasks: Vec<TaskDraft>,
    ) -> Result<(u64, String)> {
        if developer_adapter != self.developer_adapter
            || reviewer_adapters != self.reviewer_adapters
        {
            bail!("task plan adapters differ from the provider-routed worker session binding");
        }
        if expected_session_version != self.core.version() {
            bail!("session version is stale");
        }
        if !matches!(
            self.core.session_state(),
            SessionState::AwaitingPlan | SessionState::AwaitingApproval
        ) {
            bail!("task plan cannot change after this run starts");
        }

        let github_inspection = match (&self.sources.delivery_binding, github_inspection_id) {
            (DeliveryBinding::LocalCandidate, None) => None,
            (DeliveryBinding::LocalCandidate, Some(_)) => {
                bail!("local candidate plans cannot bind a GitHub inspection")
            }
            (DeliveryBinding::GitHubPullRequest { .. }, None) => {
                bail!("GitHub Pull Request plans require the latest inspection ID")
            }
            (DeliveryBinding::GitHubPullRequest { .. }, Some(inspection_id)) => {
                let inspection = self.latest_github_inspection.clone().ok_or_else(|| {
                    anyhow!("GitHub Pull Request plan has no current read-only inspection")
                })?;
                if inspection.inspection_id != inspection_id {
                    bail!("GitHub Pull Request plan inspection ID is stale");
                }
                Some(inspection)
            }
        };

        // A local task's repository_root is just the source directory handed
        // to its workers. GitHub v1 deliberately narrows every task to the one
        // canonical repository frozen by preflight.
        for task in &tasks {
            let root = PathBuf::from(&task.repository_root);
            if !root.is_dir() {
                bail!(
                    "task {} names a source directory that does not exist: {}",
                    task.task_key,
                    root.display()
                );
            }
            if let Some(binding) = self.sources.delivery_binding.github()
                && task.repository_root != binding.local_repository_root
            {
                bail!(
                    "GitHub Pull Request task {} repository_root differs from the frozen local repository root",
                    task.task_key
                );
            }
            if self
                .sources
                .profiles
                .as_ref()
                .is_some_and(|profiles| profiles.architect.adapter() == ArchitectAdapter::Claude)
            {
                let canonical = fs::canonicalize(&root).with_context(|| {
                    format!(
                        "failed to canonicalize task {} repository_root",
                        task.task_key
                    )
                })?;
                if canonical != root {
                    bail!(
                        "task {} repository_root must be canonical for a Claude Architect session",
                        task.task_key
                    );
                }
                if !root.starts_with(&self.startup.project_root)
                    && !self
                        .sources
                        .architect_additional_directories
                        .contains(&root)
                {
                    bail!(
                        "task {} uses external repository_root {} that the Claude Architect did not predeclare with --add-dir",
                        task.task_key,
                        root.display()
                    );
                }
            }
        }
        let plan_version = self
            .core
            .plan_version()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("plan version overflow"))?;
        let plan_hash = self.core.expected_plan_hash_for_inspection(
            plan_version,
            &tasks,
            github_inspection.as_ref(),
        );
        let github_run_binding = github_inspection.map(|inspection| {
            derive_github_run_binding(&self.startup.run_id, &plan_hash, inspection)
        });
        let event = SupervisorEvent::PlanBound {
            expected_version: expected_session_version,
            plan_version,
            plan_hash: plan_hash.clone(),
            tasks,
            github_run_binding,
        };
        let effects = self
            .core
            .reduce(event)
            .map_err(|error| anyhow!(error.to_string()))?;
        self.execute_effects(effects)?;
        Ok((plan_version, plan_hash))
    }

    pub(crate) fn approve_and_start(
        &mut self,
        expected_session_version: u64,
        plan_version: u64,
        plan_hash: &str,
        approval_confirmed: bool,
    ) -> Result<()> {
        if !approval_confirmed {
            bail!("run start requires explicit human execution authorization");
        }
        if expected_session_version != self.core.version()
            || self.core.session_state() != SessionState::AwaitingApproval
            || self.core.plan_version() != Some(plan_version)
            || self.core.plan_hash() != Some(plan_hash)
        {
            bail!("run start binding is stale or does not match the displayed plan");
        }
        if let Some((delivery_binding, inspector)) = self
            .sources
            .github_runtime
            .as_ref()
            .map(|runtime| (runtime.binding.clone(), runtime.inspector.clone()))
        {
            let approved = self
                .core
                .github_run_binding()
                .cloned()
                .ok_or_else(|| anyhow!("GitHub Pull Request plan has no frozen run binding"))?;
            if self
                .latest_github_inspection
                .as_ref()
                .is_none_or(|latest| latest.inspection_id != approved.inspection_id)
            {
                bail!(
                    "GitHub Pull Request plan inspection was superseded; bind and display the newest inspection"
                );
            }
            let observed = inspector.inspect(&GitHubInspectionRequest {
                run_id: self.core.run_id().into(),
                session_version: expected_session_version,
                delivery_binding: delivery_binding.clone(),
            })?;
            if let Err(error) = validate_inspection_result(&delivery_binding, &observed) {
                // A validated identity or complete-permission-map drift makes
                // this approval permanently stale, even if GitHub later
                // returns to the previously approved values.
                self.latest_github_inspection = None;
                return Err(error);
            }
            if observed.inspection.inspected_repository_id != approved.inspected_repository_id
                || observed.inspection.expected_base_ref != approved.expected_base_ref
                || observed.inspection.expected_base_sha != approved.expected_base_sha
                || observed.inspection.ruleset_attestation_sha256
                    != approved.ruleset_attestation_sha256
            {
                // Do not let a drift-then-revert sequence resurrect the same
                // approval. A fresh inspection ID and redisplayed plan are
                // required before another start attempt.
                self.latest_github_inspection = None;
                bail!(
                    "GitHub base ref, base SHA, or hcom-critical rules changed after plan binding; inspect and redisplay the plan"
                );
            }
        }
        self.cleanup_registry.ensure_available()?;
        let effects = self
            .core
            .reduce(SupervisorEvent::ExecutionAuthorized {
                expected_version: expected_session_version,
                plan_version: Some(plan_version),
                plan_hash: Some(plan_hash.into()),
            })
            .map_err(|error| anyhow!(error.to_string()))?;
        if self.tasks_workspace.is_none() {
            // Authorization is already committed above; a mechanical staging
            // failure here must terminalize the run as needs_human instead of
            // returning early, which would leave a Running session with no
            // worker, no effects, and no way to re-authorize.
            match self.stage_tasks_workspace(plan_version, plan_hash) {
                Ok(workspace) => self.tasks_workspace = Some(workspace),
                Err(error) => {
                    let task_ordinal = effects
                        .iter()
                        .find_map(|effect| match effect {
                            SupervisorEffect::OpenTaskRuntime { task_ordinal } => {
                                Some(*task_ordinal)
                            }
                            _ => None,
                        })
                        .unwrap_or(0);
                    return self.fail_driver_effect(
                        task_ordinal,
                        DriverFailureClass::Environment,
                        error,
                    );
                }
            }
        }
        self.execute_effects(effects)
    }

    fn stage_tasks_workspace(
        &mut self,
        plan_version: u64,
        plan_hash: &str,
    ) -> Result<TasksWorkspace> {
        if self.project_tasks_workspace.is_none() {
            self.project_tasks_workspace = Some(
                ProjectTasksWorkspace::open(&self.startup.project_root)
                    .context("failed to open the hcom-tasks workspace")?,
            );
        }
        let workspace = self
            .project_tasks_workspace
            .as_ref()
            .expect("project tasks workspace was opened above")
            .claim_run(&self.startup.run_id)
            .context("failed to claim the hcom-tasks run workspace")?;
        workspace.write_run_file(
            "plan.md",
            self.render_plan(plan_version, plan_hash).as_bytes(),
        )?;
        let _ = workspace.append_decision(&format!(
            "execution authorized: plan version {plan_version} hash {plan_hash}"
        ));
        Ok(workspace)
    }

    fn render_plan(&self, plan_version: u64, plan_hash: &str) -> String {
        let mut plan = format!(
            "# hcom arch run {}\n\nplan version: {plan_version}\nplan hash: {plan_hash}\n\n",
            self.startup.run_id
        );
        if let DeliveryBinding::GitHubPullRequest { binding } = &self.sources.delivery_binding {
            let run_binding = self
                .core
                .github_run_binding()
                .expect("a bound GitHub plan has a run binding");
            plan.push_str("## delivery\n\n- mode: GitHub Pull Request\n");
            plan.push_str(&format!(
                "- delivery policy: {}\n- private repository: {}/{} (id {})\n- local repository root: {}\n- base branch: {}\n- inspected base ref: {}\n- inspected base SHA: {}\n- inspection ID: {}\n- generated run branch: {}\n- review Check: {}\n",
                binding.delivery_policy.as_str(),
                binding.owner,
                binding.repository,
                binding.repository_id,
                binding.local_repository_root,
                binding.base_branch,
                run_binding.expected_base_ref,
                run_binding.expected_base_sha,
                run_binding.inspection_id,
                run_binding.generated_run_branch,
                binding.review_check_name,
            ));
            if binding.delivery_policy.is_protected_auto_merge() {
                plan.push_str(&format!(
                    "- ruleset attestation SHA-256: {}\n- merge method: {}\n- merge wait seconds: {}\n- delete remote branch after merge: {}\n",
                    run_binding
                        .ruleset_attestation_sha256
                        .as_deref()
                        .expect("protected plan has ruleset attestation"),
                    binding.merge_method,
                    binding.merge_wait_seconds,
                    binding.delete_remote_branch_after_merge,
                ));
            } else {
                plan.push_str(
                    "- server-side enforcement: not attested; this private GitHub Free repository has no hcom-provable PR/direct-push protection\n- hcom enforcement boundary: hcom verifies its exact base/head, actors, append-only task chain, published reviews, and hcom/review Check, but cannot prevent an authorized external actor from direct-pushing or merging early\n- terminal disposition: all-LGTM means hcom review complete only, not GitHub rules/CI merge-ready; the open PR, generated remote/local branch, linked worktree, and evidence are preserved for human disposition\n- prohibited run operations: no merge, remote branch deletion, or merged-run finalization\n",
                );
            }
            let commit_identity = binding.developer_commit_identity();
            plan.push_str(&format!(
                "- Developer commit identity: {} <{}> (author, committer, and Signed-off-by)\n",
                commit_identity.name, commit_identity.email
            ));
            plan.push_str(&format!(
                "- Architect App: {} (app {}, installation {}, bot {}, effective permissions {})\n- Developer App: {} (app {}, installation {}, bot {}, effective permissions {})\n",
                binding.architect_app.slug,
                binding.architect_app.app_id,
                binding.architect_app.installation_id,
                binding.architect_app.bot_user_id,
                serde_json::to_string(&binding.architect_app.effective_permissions)
                    .expect("GitHub permission map is serializable"),
                binding.developer_app.slug,
                binding.developer_app.app_id,
                binding.developer_app.installation_id,
                binding.developer_app.bot_user_id,
                serde_json::to_string(&binding.developer_app.effective_permissions)
                    .expect("GitHub permission map is serializable"),
            ));
            for reviewer in &binding.reviewer_apps {
                plan.push_str(&format!(
                    "- {:?} App: {} (app {}, installation {}, bot {}, effective permissions {})\n",
                    reviewer.reviewer_id,
                    reviewer.app.slug,
                    reviewer.app.app_id,
                    reviewer.app.installation_id,
                    reviewer.app.bot_user_id,
                    serde_json::to_string(&reviewer.app.effective_permissions)
                        .expect("GitHub permission map is serializable"),
                ));
            }
            if binding.delivery_policy.is_protected_auto_merge() {
                plan.push_str(
                    "- topology: one Pull Request per approved run; all tasks append commits to the one generated branch; final delivery uses one ruleset-attested exact-head squash merge\n- publication: exact opaque Developer and Reviewer finals are published unredacted to the selected private repository; Architect owns hcom/review and final merge\n- credential boundary: keys and operation tokens remain supervisor-owned and never enter worker environments or prompts; mode-0600 files are not an adversarial boundary against another process running as the same user\n- hooks: native Developer commit hooks remain enabled; supervisor-authenticated fetch/push disables repository hooks\n- gates: every task must be LGTM for merge; review_exhausted completes unmerged; base/ruleset/permission/identity drift fails closed\n- cleanup: successful merge removes required generated local artifacts; unsuccessful or needs-human outcomes preserve the PR/branch/worktree with evidence\n\n",
                );
            } else {
                plan.push_str(
                    "- topology: one Pull Request per approved run; all tasks append commits to the one generated branch; hcom stops after the final exact-head review Check\n- publication: exact opaque Developer and Reviewer finals are published unredacted to the selected private repository; Architect owns hcom/review but has no run authority to merge\n- credential boundary: keys and operation tokens remain supervisor-owned and never enter worker environments or prompts; mode-0600 files are not an adversarial boundary against another process running as the same user\n- hooks: native Developer commit hooks remain enabled; supervisor-authenticated fetch/push disables repository hooks\n- gates: review_complete_unmerged follows all-task LGTM; review_exhausted remains unmerged; base/head/permission/identity drift fails closed\n- cleanup: manual completion and every unsuccessful or needs-human outcome preserve the PR/branch/worktree with evidence\n\n",
                );
            }
        }
        for (ordinal, task) in self.core.tasks().iter().enumerate() {
            let spec = &task.spec;
            plan.push_str(&format!(
                "## task {ordinal}: {}\n\n- title: {}\n- repository root: {}\n- task document path: {}\n- task selector: {}\n- max review rounds: {}\n- max clarification rounds: {}\n- design document paths:\n",
                spec.task_key,
                spec.title,
                spec.repository_root,
                spec.task_document_path,
                spec.task_selector,
                spec.max_review_rounds,
                spec.max_clarification_rounds,
            ));
            if spec.design_document_paths.is_empty() {
                plan.push_str("  - (none)\n");
            } else {
                for path in &spec.design_document_paths {
                    plan.push_str(&format!("  - {path}\n"));
                }
            }
            plan.push('\n');
        }
        plan
    }

    /// Best-effort mechanical narration; the decision log never gates a run.
    fn note(&self, line: &str) {
        if let Some(workspace) = &self.tasks_workspace {
            let _ = workspace.append_decision(line);
        }
    }

    pub(crate) fn cancel(&mut self, expected_session_version: u64, reason: &str) -> Result<()> {
        if expected_session_version != self.core.version() {
            bail!("session version is stale");
        }
        self.reconcile_pending_before_terminal()?;
        if self.core.session_state().is_terminal() {
            return Ok(());
        }
        let effects = self
            .core
            .reduce(SupervisorEvent::CancelRequested {
                expected_version: expected_session_version,
                reason: reason.into(),
            })
            .map_err(|error| anyhow!(error.to_string()))?;
        self.execute_effects(effects)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
        clarification_document_path: &str,
        human_decision_confirmed: bool,
    ) -> Result<()> {
        let task_ordinal =
            usize::try_from(task_ordinal).map_err(|_| anyhow!("task ordinal is out of range"))?;
        let pending = self
            .core
            .snapshot()
            .pending_architect_action
            .ok_or_else(|| anyhow!("no Architect action is pending"))?;
        if pending.task_ordinal as usize != task_ordinal
            || pending.task_key != task_key
            || pending.sequence != action_sequence
            || pending.developer_request_path != developer_request_path
            || pending.clarification_output_path != clarification_document_path
        {
            bail!("clarification submission does not match the pending Architect action");
        }
        let workspace = self
            .tasks_workspace
            .as_ref()
            .ok_or_else(|| anyhow!("hcom-tasks workspace is not open"))?;
        workspace.validate_clarification_document(Path::new(clarification_document_path))?;
        let effects = self
            .core
            .reduce(SupervisorEvent::ClarificationSubmitted {
                expected_version: expected_session_version,
                task_ordinal,
                task_key: task_key.into(),
                action_sequence,
                developer_request_path: developer_request_path.into(),
                clarification_document_path: clarification_document_path.into(),
                human_decision_confirmed,
            })
            .map_err(|error| anyhow!(error.to_string()))?;
        self.note(&format!(
            "task {task_ordinal}: clarification {action_sequence} submitted (human_confirmed={human_decision_confirmed})"
        ));
        self.execute_effects(effects)
    }

    pub(crate) fn require_human_for_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
    ) -> Result<()> {
        let task_ordinal =
            usize::try_from(task_ordinal).map_err(|_| anyhow!("task ordinal is out of range"))?;
        let effects = self
            .core
            .reduce(SupervisorEvent::ClarificationHumanRequired {
                expected_version: expected_session_version,
                task_ordinal,
                task_key: task_key.into(),
                action_sequence,
                developer_request_path: developer_request_path.into(),
            })
            .map_err(|error| anyhow!(error.to_string()))?;
        self.note(&format!(
            "task {task_ordinal}: clarification {action_sequence} escalated to human"
        ));
        self.execute_effects(effects)
    }

    pub(crate) fn snapshot(&self) -> SessionStatusSnapshot {
        let mut snapshot = self.core.snapshot();
        if let Some(github) = snapshot.github.as_mut() {
            github.latest_inspection = self.latest_github_inspection.clone();
        }
        snapshot
    }

    pub(crate) fn clarification_page(
        &self,
        run_id: &str,
        task_ordinal: u32,
        task_key: &str,
        after_sequence: u32,
        limit: u8,
    ) -> Result<crate::control_api::ClarificationPage> {
        self.core
            .clarification_page(run_id, task_ordinal, task_key, after_sequence, limit)
            .map_err(|error| anyhow!(error.to_string()))
    }

    pub(crate) fn progress_event_after(
        &self,
        run_id: &str,
        after_sequence: u32,
    ) -> Result<Option<crate::control_api::SessionProgressEvent>> {
        self.core
            .progress_event_after(run_id, after_sequence)
            .map_err(|error| anyhow!(error.to_string()))
    }

    pub(crate) fn poll_once(&mut self) -> Result<()> {
        let _ = self.cleanup_registry.retry_pending();
        let fallback_task_ordinal = self
            .active
            .values()
            .next()
            .map(|active| active.task_ordinal);
        let result = self.poll_once_inner();
        if result.is_ok() || self.core.session_state().is_terminal() {
            return result;
        }
        let task_ordinal = self
            .core
            .snapshot()
            .current_task_ordinal
            .and_then(|ordinal| usize::try_from(ordinal).ok())
            .or(fallback_task_ordinal)
            .ok_or_else(|| anyhow!("poll failure has no current task to terminalize"))?;
        let error = result.expect_err("poll result was checked");
        self.fail_driver_effect(task_ordinal, DriverFailureClass::Contract, error)
    }

    fn poll_once_inner(&mut self) -> Result<()> {
        self.service_pending_github_effect()?;
        if self.core.session_state() != SessionState::Running || self.active.is_empty() {
            return Ok(());
        }
        let lanes = self.active.keys().copied().collect::<Vec<_>>();
        for lane in lanes {
            let Some(active) = self.active.get(&lane).cloned() else {
                continue;
            };
            let polled = {
                let task_runtime = self.require_task_runtime_mut(active.task_ordinal)?;
                task_runtime
                    .runtime
                    .poll_turn(active.lane, active.local_turn)
            };
            let poll = polled.and_then(|polled| {
                if polled.lane != active.lane || polled.turn != active.local_turn {
                    return Err(RuntimeError::invalid_identity(
                        "task runtime returned a poll for another lane or turn",
                    ));
                }
                Ok(polled.poll)
            });
            let event = match poll {
                Ok(RuntimeTurnPoll::Pending { .. }) => continue,
                Ok(RuntimeTurnPoll::Completed {
                    outcome,
                    final_message_path,
                    ..
                }) => SupervisorEvent::TurnCompleted {
                    expected_version: self.core.version(),
                    task_ordinal: active.task_ordinal,
                    lane,
                    review_generation: active.review_generation,
                    session: active.logical_session,
                    turn: active.logical_turn,
                    completion_token: active.completion_token.clone(),
                    outcome,
                    final_message_path,
                },
                Ok(RuntimeTurnPoll::Failed { failure, .. }) => SupervisorEvent::TurnFailed {
                    expected_version: self.core.version(),
                    task_ordinal: active.task_ordinal,
                    lane,
                    review_generation: active.review_generation,
                    session: active.logical_session,
                    turn: active.logical_turn,
                    completion_token: active.completion_token.clone(),
                    failure,
                },
                Err(error) => SupervisorEvent::TurnFailed {
                    expected_version: self.core.version(),
                    task_ordinal: active.task_ordinal,
                    lane,
                    review_generation: active.review_generation,
                    session: active.logical_session,
                    turn: active.logical_turn,
                    completion_token: active.completion_token.clone(),
                    failure: runtime_error_failure(error)?,
                },
            };
            let mut next_core = self.core.clone();
            let effects = next_core
                .reduce(event)
                .map_err(|error| anyhow!(error.to_string()))?;
            self.active.remove(&lane);
            if let Some(task_ordinal) = successful_task_close(&next_core, &effects)
                && let Err(error) = self.close_task_runtime(task_ordinal)
            {
                return self.fail_driver_effect(task_ordinal, DriverFailureClass::Cleanup, error);
            }
            self.core = next_core;
            self.execute_effects(effects)?;
            if self.core.session_state().is_terminal() {
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn shutdown(&mut self) -> Result<()> {
        self.reconcile_pending_before_terminal()?;
        if !self.core.session_state().is_terminal() {
            let effects = self
                .core
                .reduce(SupervisorEvent::ParentStopping {
                    expected_version: self.core.version(),
                })
                .map_err(|error| anyhow!(error.to_string()))?;
            self.execute_effects(effects)?;
        }
        self.close_runtime_best_effort();
        match self.cleanup_registry.cleanup_for(Duration::from_secs(3)) {
            CleanupRegistryInterlock::Ready => Ok(()),
            CleanupRegistryInterlock::Pending { claims } => {
                bail!("Claude lifecycle cleanup remains pending for {claims} Guardian claim(s)")
            }
            CleanupRegistryInterlock::Poisoned { detail } => {
                bail!("Claude lifecycle ownership lost: {detail}")
            }
        }
    }

    fn execute_effects(&mut self, initial: Vec<SupervisorEffect>) -> Result<()> {
        let mut effects: VecDeque<_> = initial.into();
        while let Some(effect) = effects.pop_front() {
            let follow_up = match effect {
                SupervisorEffect::PublishStatus | SupervisorEffect::FinishSession { .. } => {
                    continue;
                }
                SupervisorEffect::PrepareGitHubRunRepository(request) => {
                    let workflow = self.github_workflow()?.clone();
                    let workspace = self.tasks_workspace.as_ref().ok_or_else(|| {
                        anyhow!("hcom-tasks workspace is not open for GitHub preparation")
                    })?;
                    let observed = match workflow.prepare_repository(workspace, &request) {
                        Ok(observed) => observed,
                        Err(error) => {
                            return self.fail_github_effect(&request.operation_id, error);
                        }
                    };
                    SupervisorEvent::RepositoryPrepared {
                        expected_version: self.core.version(),
                        operation_id: observed.operation_id,
                        base_sha: observed.base_sha,
                        branch: observed.branch,
                        worktree_path: observed.worktree_path,
                    }
                }
                SupervisorEffect::PublishDeveloperCandidate(request) => {
                    let workflow = self.github_workflow()?.clone();
                    let observed = match workflow.publish_candidate(&request) {
                        Ok(observed) => observed,
                        Err(error) => {
                            return self.fail_github_effect(&request.operation_id, error);
                        }
                    };
                    SupervisorEvent::CandidatePublished {
                        expected_version: self.core.version(),
                        operation_id: observed.operation_id,
                        task_ordinal: observed.task_ordinal,
                        generation: observed.generation,
                        previous_head_sha: observed.previous_head_sha,
                        head_sha: observed.head_sha,
                        pr_number: observed.pr_number,
                        pr_node_id: observed.pr_node_id,
                        pr_url: observed.pr_url,
                        pr_actor_bot_user_id: observed.pr_actor_bot_user_id,
                        check_run_id: observed.check_run_id,
                        check_url: observed.check_url,
                        check_actor_app_id: observed.check_actor_app_id,
                    }
                }
                SupervisorEffect::PublishReviewerReview(request) => {
                    let workflow = self.github_workflow()?.clone();
                    let observed = match workflow.publish_review(&request) {
                        Ok(observed) => observed,
                        Err(error) => {
                            return self.fail_github_effect(&request.operation_id, error);
                        }
                    };
                    SupervisorEvent::ReviewerReviewPublished {
                        expected_version: self.core.version(),
                        operation_id: observed.operation_id,
                        task_ordinal: observed.task_ordinal,
                        reviewer_id: observed.reviewer_id,
                        generation: observed.generation,
                        head_sha: observed.head_sha,
                        verdict: observed.verdict,
                        review_id: observed.review_id,
                        review_url: observed.review_url,
                        actor_bot_user_id: observed.actor_bot_user_id,
                        final_artifact_sha256: observed.final_artifact_sha256,
                    }
                }
                SupervisorEffect::OpenOrUpdateReviewCheck(request) => {
                    let workflow = self.github_workflow()?.clone();
                    let observed = match workflow.publish_check(&request) {
                        Ok(observed) => observed,
                        Err(error) => {
                            return self.fail_github_effect(&request.operation_id, error);
                        }
                    };
                    SupervisorEvent::ReviewCheckPublished {
                        expected_version: self.core.version(),
                        operation_id: observed.operation_id,
                        task_ordinal: observed.task_ordinal,
                        generation: observed.generation,
                        head_sha: observed.head_sha,
                        check_run_id: observed.check_run_id,
                        check_url: observed.check_url,
                        state: observed.state,
                        actor_app_id: observed.actor_app_id,
                    }
                }
                SupervisorEffect::WaitForMergeGate(request) => {
                    self.start_merge_gate_wait(request)?;
                    continue;
                }
                SupervisorEffect::MergePullRequest(request) => {
                    self.start_merge(request)?;
                    continue;
                }
                SupervisorEffect::FinalizeGitHubRun(request) => {
                    if self.task_runtime.is_some() || !self.active.is_empty() {
                        return self.fail_github_effect(
                            &request.operation_id,
                            anyhow!("GitHub finalization retained a live task worker runtime"),
                        );
                    }
                    let workflow = self.github_workflow()?.clone();
                    let observed = match workflow.finalize_run(&request) {
                        Ok(observed) => observed,
                        Err(error) => {
                            return self.fail_github_effect(&request.operation_id, error);
                        }
                    };
                    SupervisorEvent::GitHubRunFinalized {
                        expected_version: self.core.version(),
                        operation_id: observed.operation_id,
                        pr_number: observed.pr_number,
                        final_head_sha: observed.final_head_sha,
                        merge_sha: observed.merge_sha,
                        finalization: observed.finalization,
                    }
                }
                SupervisorEffect::PublishGitHubTerminal(request) => {
                    if let Ok(workflow) = self.github_workflow()
                        && let Err(error) = workflow.publish_terminal_best_effort(&request)
                    {
                        self.note(&format!(
                            "GitHub terminal publication was not confirmed: {}",
                            bounded_single_line(&error.to_string())
                        ));
                    }
                    continue;
                }
                SupervisorEffect::OpenTaskRuntime { task_ordinal } => {
                    if let Err(failure) = self.open_task_runtime(task_ordinal) {
                        return self.fail_driver_effect(task_ordinal, failure.class, failure.error);
                    }
                    SupervisorEvent::TaskRuntimeOpened {
                        expected_version: self.core.version(),
                        task_ordinal,
                    }
                }
                SupervisorEffect::OpenRoleSession { task_ordinal, lane } => {
                    let logical = match self.open_role_session(task_ordinal, lane) {
                        Ok(session) => session,
                        Err(error) => {
                            return self.fail_driver_effect(
                                task_ordinal,
                                DriverFailureClass::Runtime,
                                error,
                            );
                        }
                    };
                    SupervisorEvent::RoleSessionOpened {
                        expected_version: self.core.version(),
                        task_ordinal,
                        lane,
                        session: logical,
                    }
                }
                SupervisorEffect::StartTurn {
                    task_ordinal,
                    lane,
                    review_generation,
                    purpose,
                    session,
                } => {
                    let (logical_turn, completion_token) = match self.start_turn(
                        task_ordinal,
                        lane,
                        review_generation,
                        purpose,
                        session,
                    ) {
                        Ok(started) => started,
                        Err(error) => {
                            return self.fail_driver_effect(
                                task_ordinal,
                                DriverFailureClass::Runtime,
                                error,
                            );
                        }
                    };
                    self.note(&format!(
                        "task {task_ordinal}: started {} turn ({purpose:?})",
                        lane.as_str()
                    ));
                    SupervisorEvent::TurnStarted {
                        expected_version: self.core.version(),
                        task_ordinal,
                        lane,
                        review_generation,
                        purpose,
                        session,
                        turn: logical_turn,
                        completion_token,
                    }
                }
                SupervisorEffect::InterruptTurn {
                    task_ordinal,
                    lane,
                    turn,
                    ..
                } => {
                    self.interrupt_turn(task_ordinal, lane, turn);
                    continue;
                }
                SupervisorEffect::CloseTaskRuntime { task_ordinal } => {
                    self.close_task_runtime(task_ordinal)?;
                    continue;
                }
                SupervisorEffect::PrepareClarificationArtifact {
                    task_ordinal,
                    task_key,
                    sequence,
                    path,
                } => {
                    let prepared = match self.tasks_workspace.as_ref() {
                        Some(workspace) => {
                            match workspace.prepare_clarification_path(&task_key, sequence) {
                                Ok(path) => path,
                                Err(error) => {
                                    return self.fail_driver_effect(
                                        task_ordinal,
                                        DriverFailureClass::Contract,
                                        error,
                                    );
                                }
                            }
                        }
                        None => {
                            return self.fail_driver_effect(
                                task_ordinal,
                                DriverFailureClass::Contract,
                                anyhow!("hcom-tasks workspace is not open"),
                            );
                        }
                    };
                    if prepared != path {
                        return self.fail_driver_effect(
                            task_ordinal,
                            DriverFailureClass::Contract,
                            anyhow!("clarification artifact path differs from the core binding"),
                        );
                    }
                    self.note(&format!(
                        "task {task_ordinal}: awaiting Architect action {sequence}"
                    ));
                    continue;
                }
            };
            let mut next_core = self.core.clone();
            let next = next_core
                .reduce(follow_up)
                .map_err(|error| anyhow!(error.to_string()))?;
            if let Some(task_ordinal) = successful_task_close(&next_core, &next)
                && let Err(error) = self.close_task_runtime(task_ordinal)
            {
                return self.fail_driver_effect(task_ordinal, DriverFailureClass::Cleanup, error);
            }
            for effect in &next {
                if let SupervisorEffect::FinishSession { state, detail } = effect {
                    self.note(&format!("session finished: {state:?}: {detail}"));
                }
            }
            self.core = next_core;
            effects.extend(next);
        }
        Ok(())
    }

    fn start_merge_gate_wait(
        &mut self,
        request: crate::orchestrator::core::WaitForMergeGateRequest,
    ) -> Result<()> {
        if self.pending_github_effect.is_some() {
            bail!("another GitHub merge operation is already in flight");
        }
        let operation_id = request.operation_id.clone();
        let workflow = self.github_workflow()?.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = Arc::clone(&cancelled);
        let (sender, receiver) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("hcom-github-merge-gate".into())
            .spawn(move || {
                let result = workflow.wait_for_merge_gate(&request, &thread_cancelled);
                let _ = sender.send(PendingGitHubResult::MergeGate(result));
            })
            .context("failed to start GitHub merge-gate worker")?;
        self.pending_github_effect = Some(PendingGitHubEffect {
            operation_id,
            kind: PendingGitHubKind::MergeGate,
            cancelled,
            receiver,
            join,
        });
        Ok(())
    }

    fn start_merge(
        &mut self,
        request: crate::orchestrator::core::MergePullRequestRequest,
    ) -> Result<()> {
        if self.pending_github_effect.is_some() {
            bail!("another GitHub merge operation is already in flight");
        }
        let operation_id = request.operation_id.clone();
        let workflow = self.github_workflow()?.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = Arc::clone(&cancelled);
        let (sender, receiver) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("hcom-github-merge".into())
            .spawn(move || {
                let result = workflow.merge_pull_request(&request, &thread_cancelled);
                let _ = sender.send(PendingGitHubResult::Merge(result));
            })
            .context("failed to start GitHub merge worker")?;
        self.pending_github_effect = Some(PendingGitHubEffect {
            operation_id,
            kind: PendingGitHubKind::Merge,
            cancelled,
            receiver,
            join,
        });
        Ok(())
    }

    fn finish_pending_github_effect(
        pending: PendingGitHubEffect,
        mut result: PendingGitHubResult,
    ) -> (String, PendingGitHubResult) {
        if pending.join.join().is_err() {
            result = pending.kind.failure(anyhow!(
                "GitHub merge worker panicked before clean termination"
            ));
        }
        (pending.operation_id, result)
    }

    fn cancel_and_join_pending_github_effect(&mut self) -> Option<(String, PendingGitHubResult)> {
        let pending = self.pending_github_effect.take()?;
        pending.cancelled.store(true, Ordering::Release);
        let result = pending.receiver.recv().unwrap_or_else(|_| {
            pending.kind.failure(anyhow!(
                "GitHub merge worker exited without returning an observation"
            ))
        });
        Some(Self::finish_pending_github_effect(pending, result))
    }

    fn reconcile_pending_before_terminal(&mut self) -> Result<()> {
        let Some((operation_id, result)) = self.cancel_and_join_pending_github_effect() else {
            return Ok(());
        };
        match result {
            PendingGitHubResult::MergeGate(Ok(_)) => Ok(()),
            PendingGitHubResult::MergeGate(Err(error)) | PendingGitHubResult::Merge(Err(error))
                if error.chain().any(|cause| {
                    cause.is::<crate::orchestrator::github::workflow::GitHubWorkflowCancelled>()
                }) =>
            {
                Ok(())
            }
            result => self.apply_pending_github_result(&operation_id, result),
        }
    }

    fn service_pending_github_effect(&mut self) -> Result<()> {
        let Some(pending) = self.pending_github_effect.as_ref() else {
            return Ok(());
        };
        let result = match pending.receiver.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return Ok(()),
            Err(mpsc::TryRecvError::Disconnected) => pending.kind.failure(anyhow!(
                "GitHub merge worker exited without returning an observation"
            )),
        };
        let pending = self
            .pending_github_effect
            .take()
            .expect("the pending GitHub effect was just observed");
        let (operation_id, result) = Self::finish_pending_github_effect(pending, result);
        self.apply_pending_github_result(&operation_id, result)
    }

    fn apply_pending_github_result(
        &mut self,
        operation_id: &str,
        result: PendingGitHubResult,
    ) -> Result<()> {
        let follow_up = match result {
            PendingGitHubResult::MergeGate(Ok(observed)) => SupervisorEvent::MergeGateObserved {
                expected_version: self.core.version(),
                operation_id: observed.operation_id,
                pr_number: observed.pr_number,
                final_head_sha: observed.final_head_sha,
                base_sha: observed.base_sha,
                ruleset_attestation_sha256: observed.ruleset_attestation_sha256,
                check_run_id: observed.check_run_id,
            },
            PendingGitHubResult::Merge(Ok(observed)) => SupervisorEvent::PullRequestMerged {
                expected_version: self.core.version(),
                operation_id: observed.operation_id,
                pr_number: observed.pr_number,
                final_head_sha: observed.final_head_sha,
                merge_sha: observed.merge_sha,
                merge_url: observed.merge_url,
                actor_bot_user_id: observed.actor_bot_user_id,
                merge_evidence_durable: observed.merge_evidence_durable,
            },
            PendingGitHubResult::MergeGate(Err(error)) | PendingGitHubResult::Merge(Err(error)) => {
                return self.fail_github_effect(operation_id, error);
            }
        };
        let mut next_core = self.core.clone();
        let effects = next_core
            .reduce(follow_up)
            .map_err(|error| anyhow!(error.to_string()))?;
        self.core = next_core;
        self.execute_effects(effects)
    }

    fn fail_driver_effect(
        &mut self,
        task_ordinal: usize,
        class: DriverFailureClass,
        error: anyhow::Error,
    ) -> Result<()> {
        self.close_runtime_best_effort();
        let detail = bounded_single_line(&error.to_string());
        self.note(&format!(
            "task {task_ordinal}: driver failure ({class:?}): {detail}"
        ));
        let effects = self
            .core
            .reduce(SupervisorEvent::DriverFailed {
                expected_version: self.core.version(),
                task_ordinal,
                failure: DriverFailure { class, detail },
            })
            .map_err(|core_error| anyhow!(core_error.to_string()))?;
        for effect in effects {
            if let SupervisorEffect::CloseTaskRuntime { .. } = effect {
                self.close_runtime_best_effort();
            }
        }
        Err(error)
    }

    fn fail_github_effect(&mut self, operation_id: &str, error: anyhow::Error) -> Result<()> {
        let partial = self
            .github_workflow()
            .ok()
            .and_then(|workflow| workflow.take_partial_operation(operation_id).ok())
            .flatten();
        self.fail_github_effect_with_partial(operation_id, error, partial)
    }

    fn fail_github_effect_with_partial(
        &mut self,
        operation_id: &str,
        error: anyhow::Error,
        partial_operation: Option<crate::orchestrator::core::GitHubPartialOperation>,
    ) -> Result<()> {
        let detail = bounded_single_line(&error.to_string());
        self.note(&format!(
            "GitHub operation {operation_id} could not be reconciled: {detail}"
        ));
        let effects = self
            .core
            .reduce(SupervisorEvent::GitHubOperationFailed {
                expected_version: self.core.version(),
                operation_id: operation_id.into(),
                detail,
                partial_operation,
            })
            .map_err(|core_error| anyhow!(core_error.to_string()))?;
        self.execute_effects(effects)?;
        Err(error)
    }

    fn github_workflow(
        &self,
    ) -> Result<&std::sync::Arc<dyn crate::orchestrator::github::GitHubWorkflowProvider>> {
        self.sources
            .github_runtime
            .as_ref()
            .map(|runtime| &runtime.workflow)
            .ok_or_else(|| anyhow!("GitHub workflow driver is unavailable in local mode"))
    }

    fn effective_task_repository(&self, task_ordinal: usize) -> Result<PathBuf> {
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("task repository ordinal is out of range"))?;
        if self.sources.delivery_binding.is_github() {
            let worktree = self
                .core
                .snapshot()
                .github
                .and_then(|github| github.worktree_path)
                .ok_or_else(|| anyhow!("GitHub managed worktree is not prepared"))?;
            Ok(PathBuf::from(worktree))
        } else {
            Ok(PathBuf::from(&task.spec.repository_root))
        }
    }

    fn open_task_runtime(&mut self, task_ordinal: usize) -> Result<(), RuntimeOpenFailure> {
        self.prepare_and_open_task_runtime(task_ordinal)
    }

    fn prepare_and_open_task_runtime(
        &mut self,
        task_ordinal: usize,
    ) -> Result<(), RuntimeOpenFailure> {
        self.cleanup_registry
            .ensure_available()
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Cleanup, error))?;
        if self.task_runtime.is_some() || !self.active.is_empty() {
            return Err(RuntimeOpenFailure::new(
                DriverFailureClass::Contract,
                anyhow!("a task-local exec worker runtime is already open"),
            ));
        }
        let task = self.core.tasks().get(task_ordinal).ok_or_else(|| {
            RuntimeOpenFailure::new(
                DriverFailureClass::Contract,
                anyhow!("exec worker runtime task ordinal is out of range"),
            )
        })?;
        let repository_root = self
            .effective_task_repository(task_ordinal)
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Repository, error))?;
        let (root, paths) = TaskRuntimePaths::create(
            &self.run_root,
            task_ordinal,
            &task.spec.task_key,
            &repository_root,
            self.profiles.lanes(),
        )
        .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Environment, error))?;
        let environment = self
            .task_environment(&task.spec.task_key)
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Environment, error))?;
        let materialized = environment
            .materialize_task_runtime(&self.epoch)
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Environment, error))?;
        let request = RuntimeOpenRequest {
            task_ordinal,
            task_key: task.spec.task_key.clone(),
            repository_root,
            paths: clone_runtime_paths(&paths),
            environment: materialized_environment(&materialized),
            lease: environment.clone(),
            artifact_root: self
                .tasks_workspace
                .as_ref()
                .map(|workspace| workspace.root().to_path_buf())
                .ok_or_else(|| {
                    RuntimeOpenFailure::new(
                        DriverFailureClass::Environment,
                        anyhow!("hcom-tasks workspace is not open"),
                    )
                })?,
            run_id: self.startup.run_id.clone(),
            cleanup_registry: self.cleanup_registry.clone(),
            profiles: self.profiles.clone(),
            guardian_executable: self.sources.guardian_executable.clone(),
        };
        let runtime = self.factory.open(request).map_err(|error| {
            RuntimeOpenFailure::new(DriverFailureClass::Runtime, anyhow!(error.detail))
        })?;
        if runtime.contract() != &self.factory.contract(&self.profiles) {
            return Err(RuntimeOpenFailure::new(
                DriverFailureClass::Contract,
                anyhow!("opened task runtime differs from the frozen runtime contract"),
            ));
        }
        self.task_runtime = Some(OpenTaskRuntime {
            task_ordinal,
            _root: root,
            runtime,
            sessions: BTreeMap::new(),
        });
        Ok(())
    }

    fn open_role_session(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
    ) -> Result<RuntimeSessionKey> {
        self.cleanup_registry.ensure_available()?;
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("role session task ordinal is out of range"))?;
        let task_key = task.spec.task_key.clone();
        let repository_root = self.effective_task_repository(task_ordinal)?;
        let project_root = self.startup.project_root.clone();
        let role = lane.role();
        let profile = self.profile(lane).clone();
        let instructions = self.role_instructions_for_session(role)?;
        let local = self
            .require_task_runtime_mut(task_ordinal)?
            .runtime
            .open_session(
                lane,
                RoleSessionSpec {
                    role,
                    task_key,
                    cwd: project_root,
                    task_repository: repository_root,
                    profile,
                    developer_instructions: instructions,
                },
            )
            .map_err(|error| anyhow!(error.detail))?;
        let logical = self.allocate_session_key()?;
        let runtime = self.require_task_runtime_mut(task_ordinal)?;
        if runtime
            .sessions
            .insert(logical, LocalSession { lane, key: local })
            .is_some()
        {
            bail!("logical exec worker role session key collided");
        }
        Ok(logical)
    }

    fn start_turn(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        review_generation: Option<u32>,
        purpose: RuntimeTurnPurpose,
        logical_session: RuntimeSessionKey,
    ) -> Result<(RuntimeTurnKey, String)> {
        if self.active.contains_key(&lane)
            || (lane == WorkerLane::Developer && !self.active.is_empty())
            || (lane.role() == WorkerRole::Reviewer
                && self.active.contains_key(&WorkerLane::Developer))
        {
            bail!("worker lane conflicts with an active exec worker turn");
        }
        self.cleanup_registry.ensure_available()?;
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("turn task ordinal is out of range"))?;
        let task_key = task.spec.task_key.clone();
        let repository_root = self.effective_task_repository(task_ordinal)?;
        let role = lane.role();
        let prompt = self.build_turn_prompt(task_ordinal, lane, purpose)?;
        let profile = self.profile(lane).clone();
        let local_session = self
            .require_task_runtime_mut(task_ordinal)?
            .sessions
            .get(&logical_session)
            .copied()
            .ok_or_else(|| anyhow!("logical exec worker role session is not bound"))?;
        if local_session.lane != lane {
            bail!("logical exec worker session belongs to the wrong lane");
        }
        let project_root = self.startup.project_root.clone();
        let local_turn = self
            .require_task_runtime_mut(task_ordinal)?
            .runtime
            .start_turn(
                lane,
                local_session.key,
                RuntimeTurnSpec {
                    role,
                    task_key,
                    purpose,
                    cwd: project_root,
                    task_repository: repository_root,
                    prompt: prompt.clone(),
                    profile,
                    outcome_contract: match role {
                        WorkerRole::Developer => OutcomeContract::DeveloperV1,
                        WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
                    },
                    timeout: TURN_TIMEOUT,
                },
            )
            .map_err(|error| anyhow!(error.detail))?;
        let logical_turn = self.allocate_turn_key()?;
        let completion_token = format!("exec-turn-{}", Uuid::new_v4());
        self.active.insert(
            lane,
            ActiveTurn {
                task_ordinal,
                lane,
                review_generation,
                logical_session,
                logical_turn,
                local_turn,
                completion_token: completion_token.clone(),
            },
        );
        Ok((logical_turn, completion_token))
    }

    fn interrupt_turn(
        &mut self,
        task_ordinal: usize,
        lane: WorkerLane,
        logical_turn: RuntimeTurnKey,
    ) {
        let Some(active) = self.active.remove(&lane) else {
            return;
        };
        if active.task_ordinal != task_ordinal || active.logical_turn != logical_turn {
            debug_assert_eq!(
                (active.task_ordinal, active.logical_turn),
                (task_ordinal, logical_turn),
                "SupervisorCore emitted an interrupt for a different active exec worker turn"
            );
            self.active.insert(lane, active);
            return;
        }
        if let Some(runtime) = self.task_runtime.as_mut()
            && let Err(error) = runtime.runtime.cancel_turn(active.lane, active.local_turn)
        {
            // Cancellation that could not confirm the worker died is evidence,
            // not something to swallow: the run is ending either way, so
            // record it where a human will see it.
            self.note(&format!(
                "task {task_ordinal}: cancel could not confirm worker termination: {}",
                bounded_single_line(&error.detail)
            ));
        }
    }

    fn close_task_runtime(&mut self, task_ordinal: usize) -> Result<()> {
        let Some(mut runtime) = self.task_runtime.take() else {
            return Ok(());
        };
        if runtime.task_ordinal != task_ordinal {
            self.task_runtime = Some(runtime);
            bail!("close effect referenced the wrong task-local runtime");
        }
        self.active.clear();
        runtime
            .runtime
            .shutdown()
            .map_err(|error| anyhow!(error.detail))
    }

    fn close_runtime_best_effort(&mut self) {
        self.active.clear();
        if let Some(mut runtime) = self.task_runtime.take() {
            let _ = runtime.runtime.shutdown();
        }
    }

    fn task_environment(&self, task_key: &str) -> Result<ExecutionEnvironmentLease> {
        let policy = EnvironmentPolicy::new(Vec::new(), Vec::new())?;
        let lease = ExecutionEnvironmentLease::capture_complete(
            format!("exec-lease-{}", Uuid::new_v4()),
            &self.epoch,
            &policy,
            &self.sources.parent_environment,
            Vec::new(),
        )
        .with_context(|| format!("failed to capture exec worker environment for {task_key}"))?;
        Ok(lease)
    }

    fn build_turn_prompt(
        &self,
        task_ordinal: usize,
        lane: WorkerLane,
        purpose: RuntimeTurnPurpose,
    ) -> Result<String> {
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("turn prompt task ordinal is out of range"))?;
        let spec = &task.spec;
        let mut prompt = String::new();
        let role = lane.role();
        let role_name = match role {
            WorkerRole::Developer => "Developer",
            WorkerRole::Reviewer => lane.as_str(),
        };
        let repository_root = self.effective_task_repository(task_ordinal)?;
        prompt.push_str(&format!(
            "You are the task {role_name}.\n\nRepository:\n{}\n\nTask file:\n{}\n\nDesign files:\n",
            repository_root.display(),
            spec.task_document_path,
        ));
        if spec.design_document_paths.is_empty() {
            prompt.push_str("- (none)\n");
        } else {
            for path in &spec.design_document_paths {
                prompt.push_str(&format!("- {path}\n"));
            }
        }
        prompt.push_str("\nClarification records, oldest to newest:\n");
        if task.clarification_records().is_empty() {
            prompt.push_str("- (none)\n");
        } else {
            for record in task.clarification_records() {
                prompt.push_str(&format!(
                    "- sequence {} ({:?})\n  - Developer request: {}\n  - Architect clarification: {}\n  - human decision confirmed: {}\n",
                    record.sequence,
                    record.reason,
                    record.developer_request_path,
                    record.architect_clarification_path,
                    record.human_decision_confirmed,
                ));
            }
            prompt.push_str(
                "\nThese clarification documents supplement the approved task and design files. \
                 For the specific issue a clarification addresses, a newer clarification takes \
                 precedence over an older clarification or conflicting original wording. A \
                 clarification does not expand the task beyond the approved scope.\n",
            );
        }
        prompt.push_str(&format!("\nTask selector:\n{}\n", spec.task_selector));
        prompt.push_str(
            "\nRead the task and design files and work only on the selected task. Before doing \
             any work, inspect and follow every applicable instruction file (including AGENTS.md, \
             AGENTS.override.md, and nested instruction files for paths you touch). Codex has \
             already loaded its native user/project configuration; the repository is registered \
             as a native workspace root when it differs from the project directory.\n",
        );

        match role {
            WorkerRole::Developer => {
                if let Some(binding) = self.sources.delivery_binding.github() {
                    let identity = binding.developer_commit_identity();
                    let snapshot = self.core.snapshot();
                    let task_status = snapshot
                        .tasks
                        .get(task_ordinal)
                        .ok_or_else(|| anyhow!("GitHub prompt task status is unavailable"))?;
                    prompt.push_str(&format!(
                        "\nGitHub Pull Request commit contract:\n- delivery policy: {}\n- run branch: {}\n- task base: {}\n- previously published head: {}\n- Developer App: {} (app {}, bot user {})\n- exact commit author, committer, and Signed-off-by identity: {} <{}>\n\
                         Create exactly one new signed-off child commit in this turn. Never amend, rebase, squash, force-push, or rewrite an existing commit. Leave the index and worktree clean. Do not push: the foreground supervisor validates and publishes the commit after your final. Your final is opaque payload published byte-for-byte without redaction or secret scanning to the selected private repository; keep the complete generated GitHub body within its 60 KiB UTF-8 hard cap.\n",
                        binding.delivery_policy.as_str(),
                        task_status.branch.as_deref().unwrap_or("not-yet-published"),
                        task_status.base_revision.as_deref().unwrap_or("run-base-pending"),
                        snapshot
                            .github
                            .as_ref()
                            .and_then(|github| github.published_head_sha.as_deref())
                            .unwrap_or("run-base-pending"),
                        binding.developer_app.slug,
                        binding.developer_app.app_id,
                        binding.developer_app.bot_user_id,
                        identity.name,
                        identity.email,
                    ));
                }
                match purpose {
                    RuntimeTurnPurpose::DeveloperCorrection => {
                        if task.latest_reviewer_final_paths().is_empty() {
                            bail!("developer correction has no reviewer final message path");
                        }
                        prompt.push_str(&format!(
                            "\nReview generation {} responses, in fixed Reviewer order:\n",
                            task.review_generation
                        ));
                        for reviewer_id in self
                            .profiles
                            .reviewers
                            .iter()
                            .map(|binding| binding.reviewer_id)
                        {
                            let paths = task.reviewer_final_paths(reviewer_id);
                            if paths.is_empty() {
                                bail!("developer correction lacks one Reviewer response");
                            }
                            prompt.push_str(&format!("- {}:\n", reviewer_id.as_str()));
                            for path in paths {
                                prompt.push_str(&format!("  - {path}\n"));
                            }
                        }
                        prompt.push_str(
                            "\nRead and synthesize every active Reviewer response. Resolve conflicting \
                             suggestions against the approved task and disclose the choice in your \
                             final. Address valid requested changes from either response.",
                        );
                        if self.sources.delivery_binding.is_github() {
                            prompt.push_str(
                                " The approved GitHub lane requires this correction to be exactly \
                                 one new signed-off child commit on the published head. Do not \
                                 amend any existing commit or push. Then report as before.\n",
                            );
                        } else {
                            prompt.push_str(
                                " If an explicit human, task, design, or applicable instruction \
                                 still requires this run to remain uncommitted, do not modify or \
                                 amend anything: return `STATUS: CLARIFICATION_REQUIRED` for a \
                                 human authority decision. Otherwise, if your task commit exists, \
                                 fold the fix into that SAME commit with `git commit --amend`, \
                                 updating its message so it still describes the whole task and \
                                 ensuring it retains a valid `Signed-off-by` trailer for the \
                                 committing identity. If the Reviewer reported that your task \
                                 commit is missing, create it with that sign-off only after the \
                                 complete fix. This task must end as exactly one commit. Do not \
                                 amend anything older than your own commit. Then report as before.\n",
                            );
                        }
                    }
                    RuntimeTurnPurpose::DeveloperClarificationResume => {
                        let latest = task.clarification_records().last().ok_or_else(|| {
                            anyhow!("Developer clarification resume has no clarification record")
                        })?;
                        prompt.push_str(&format!(
                            "\nResume the same task using clarification sequence {}. Re-read your \
                             request at {} and the Architect response at {} before continuing.\n",
                            latest.sequence,
                            latest.developer_request_path,
                            latest.architect_clarification_path,
                        ));
                        if self.sources.delivery_binding.is_github() {
                            prompt.push_str(
                                "Preserve every published commit exactly. Complete this turn as \
                                 exactly one new signed-off child commit on the previously \
                                 published head; do not amend, rebase, squash, reword, force-push, \
                                 or push. If the request reported an explicit no-commit or no-push \
                                 conflict, proceed only when the newest clarification records the \
                                 human's decision resolving that authority conflict; otherwise \
                                 return `STATUS: CLARIFICATION_REQUIRED` again without modifying \
                                 or committing the repository.\n",
                            );
                        } else {
                            prompt.push_str(
                                "Preserve any existing task commit exactly until you have a \
                                 complete correction to amend into it. If the request reported an \
                                 explicit no-commit conflict, proceed only when the newest \
                                 clarification records the human's decision resolving that \
                                 authority conflict; otherwise return \
                                 `STATUS: CLARIFICATION_REQUIRED` again without modifying or \
                                 committing the repository.\n",
                            );
                        }
                        if !task.latest_reviewer_final_paths().is_empty() {
                            prompt.push_str(
                                "\nPreviously supplied Reviewer final messages remain applicable:\n",
                            );
                            for path in task.latest_reviewer_final_paths() {
                                prompt.push_str(&format!("- {path}\n"));
                            }
                        }
                    }
                    RuntimeTurnPurpose::InitialDevelopment => {}
                    _ => bail!("unsupported Developer turn purpose"),
                }
                prompt.push_str(if self.sources.delivery_binding.is_github() {
                    GITHUB_DEVELOPER_OUTPUT_CONTRACT
                } else {
                    DEVELOPER_OUTPUT_CONTRACT
                });
            }
            WorkerRole::Reviewer => {
                let reviewer_id = lane
                    .reviewer_id()
                    .ok_or_else(|| anyhow!("Reviewer prompt omitted its lane identity"))?;
                let developer_path = task
                    .latest_developer_final_path()
                    .ok_or_else(|| anyhow!("reviewer turn has no Developer final message path"))?;
                let label = if purpose == RuntimeTurnPurpose::ReviewerRereview {
                    "Latest Developer final message"
                } else {
                    "Developer final message"
                };
                prompt.push_str(&format!(
                    "\nReviewer identity: {}\nReview generation: {}\n\n{label}:\n{developer_path}\n\nRead the Developer final file and \
                     independently review the selected task. Check every `ASSUMPTION:` the \
                     Developer reported against the approved task, design, and clarification \
                     records. Reviewer1 and Reviewer2 are equal peers with the same complete \
                     review scope and authority; neither lane owns a category or may rely on \
                     peer coverage. Confirm the committed candidate follows the applicable \
                     delivery contract and that no hcom-tasks artifact was included in it. If the implementation follows an \
                     applicable clarification, do not report a defect merely because the original \
                     task or design wording was ambiguous. If a finding comes from unresolved \
                     task/design ambiguity rather than an implementation defect, label that \
                     finding `REQUIREMENT_AMBIGUITY:` while still returning the normal verdict.\n",
                    reviewer_id.as_str(),
                    task.review_generation,
                ));
                if let Some(binding) = self.sources.delivery_binding.github() {
                    let snapshot = self.core.snapshot();
                    let task_status = snapshot
                        .tasks
                        .get(task_ordinal)
                        .cloned()
                        .ok_or_else(|| anyhow!("GitHub review range is unavailable"))?;
                    let review_head = task_status
                        .head_revision
                        .clone()
                        .or_else(|| {
                            snapshot
                                .github
                                .as_ref()
                                .and_then(|github| github.published_head_sha.clone())
                        })
                        .ok_or_else(|| anyhow!("GitHub review head is unavailable"))?;
                    let reviewer = binding
                        .reviewer_apps
                        .iter()
                        .find(|reviewer| reviewer.reviewer_id == reviewer_id)
                        .ok_or_else(|| anyhow!("GitHub Reviewer App binding is unavailable"))?;
                    prompt.push_str(&format!(
                        "\nGitHub Pull Request review contract:\n- delivery policy: {}\n- exact range: {}..{}\n- run branch: {}\n- Reviewer App: {} (app {}, bot user {})\n\
                         Review this complete exact range in the managed worktree. It may contain one initial task commit plus append-only correction commits; confirm every commit is a direct child in the published chain, has the frozen Developer identity/sign-off, and no published commit was rewritten. Do not push or publish a GitHub review: the foreground supervisor publishes your exact final under the mapped Reviewer App only after proving the repository still matches this head. Your final is opaque payload published byte-for-byte without redaction or secret scanning to the selected private repository; keep the complete generated review body within its 60 KiB UTF-8 hard cap.\n",
                        binding.delivery_policy.as_str(),
                        task_status
                            .base_revision
                            .as_deref()
                            .ok_or_else(|| anyhow!("GitHub review base is unavailable"))?,
                        review_head,
                        task_status
                            .branch
                            .as_deref()
                            .ok_or_else(|| anyhow!("GitHub run branch is unavailable"))?,
                        reviewer.app.slug,
                        reviewer.app.app_id,
                        reviewer.app.bot_user_id,
                    ));
                }
                match purpose {
                    RuntimeTurnPurpose::InitialReview => {
                        prompt.push_str(INITIAL_REVIEW_INSTRUCTIONS);
                    }
                    RuntimeTurnPurpose::ReviewerRereview => {
                        prompt.push_str(REREVIEW_INSTRUCTIONS);
                    }
                    _ => bail!("unsupported Reviewer turn purpose"),
                }
                prompt.push_str(REVIEWER_OUTPUT_CONTRACT);
            }
        }

        if prompt.len() > crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES {
            bail!("rendered task turn prompt exceeds its 256 KiB bound");
        }
        Ok(prompt)
    }

    fn role_instructions_for_session(&self, role: WorkerRole) -> Result<String> {
        let Some(binding) = self.sources.delivery_binding.github() else {
            return Ok(role_instructions(role).into());
        };
        let instructions = match role {
            WorkerRole::Developer => {
                let identity = binding.developer_commit_identity();
                format!(
                    "{GITHUB_DEVELOPER_ROLE_INSTRUCTIONS} The exact frozen Developer commit identity is {} <{}>.",
                    identity.name, identity.email
                )
            }
            WorkerRole::Reviewer => GITHUB_REVIEWER_ROLE_INSTRUCTIONS.into(),
        };
        Ok(instructions)
    }

    fn profile(&self, lane: WorkerLane) -> &RuntimeProfile {
        self.profiles
            .profile_for_lane(lane)
            .expect("validated task lane profile exists")
    }

    fn require_task_runtime_mut(&mut self, task_ordinal: usize) -> Result<&mut OpenTaskRuntime> {
        let runtime = self
            .task_runtime
            .as_mut()
            .ok_or_else(|| anyhow!("task-local exec worker runtime is not open"))?;
        if runtime.task_ordinal != task_ordinal {
            bail!("task-local exec worker runtime belongs to another task");
        }
        Ok(runtime)
    }

    fn allocate_session_key(&mut self) -> Result<RuntimeSessionKey> {
        let counter = self.next_session;
        self.next_session = self
            .next_session
            .checked_add(1)
            .ok_or_else(|| anyhow!("logical runtime session key overflow"))?;
        RuntimeSessionKey::from_counter(counter).map_err(|error| anyhow!(error.detail))
    }

    fn allocate_turn_key(&mut self) -> Result<RuntimeTurnKey> {
        let counter = self.next_turn;
        self.next_turn = self
            .next_turn
            .checked_add(1)
            .ok_or_else(|| anyhow!("logical runtime turn key overflow"))?;
        RuntimeTurnKey::from_counter(counter).map_err(|error| anyhow!(error.detail))
    }
}

impl Drop for TaskLaneSupervisor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn successful_task_close(core: &SupervisorCore, effects: &[SupervisorEffect]) -> Option<usize> {
    effects.iter().find_map(|effect| {
        let SupervisorEffect::CloseTaskRuntime { task_ordinal } = effect else {
            return None;
        };
        core.tasks()
            .get(*task_ordinal)
            .filter(|task| matches!(task.state, TaskState::Lgtm | TaskState::ReviewExhausted))
            .map(|_| *task_ordinal)
    })
}

fn runtime_error_failure(error: RuntimeError) -> Result<SanitizedRuntimeFailure> {
    let class = match error.code {
        RuntimeErrorCode::Internal => RuntimeFailureClass::Process,
        RuntimeErrorCode::InvalidOutcome
        | RuntimeErrorCode::InvalidContract
        | RuntimeErrorCode::InvalidIdentity
        | RuntimeErrorCode::InvalidProfile
        | RuntimeErrorCode::InvalidTransition
        | RuntimeErrorCode::Unsupported => RuntimeFailureClass::Contract,
    };
    SanitizedRuntimeFailure::new(class, bounded_single_line(&error.detail), false)
        .map_err(|failure| anyhow!(failure.detail))
}

fn clone_runtime_paths(paths: &TaskRuntimePaths) -> TaskRuntimePaths {
    TaskRuntimePaths {
        runtimes: paths.runtimes.clone(),
    }
}

fn materialized_environment(
    environment: &MaterializedWorkerEnvironment,
) -> Vec<(OsString, OsString)> {
    environment
        .iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

/// Initial review deliberately front-loads complete coverage so one generation
/// returns as many independently confirmed blockers as practical. Both Reviewer
/// lanes receive this exact contract; neither lane owns a narrower category.
const INITIAL_REVIEW_INSTRUCTIONS: &str = "
This is the initial review generation for this task. Before deciding the verdict,
derive a task-specific coverage checklist from the approved task, design,
clarifications, implementation, and exact candidate range. Cover its invariants,
affected callers and consumers, and relevant success, failure, retry, cleanup,
and terminal paths. Complete that checklist across the exact candidate range.
Do not stop after finding the first blocker: continue through the remaining
coverage, then perform a second counterexample sweep. Return every independently
confirmed Major or Critical finding that you can substantiate in this turn. Do
not add speculative findings or treat missing test coverage alone as a blocker.
";

/// A re-review reuses still-valid independent coverage from the exact resumed
/// Reviewer session and expands when the append/amendment has broad impact.
const REREVIEW_INSTRUCTIONS: &str = "
The candidate changed because of the previous review generation for this task.
First verify every finding you raised in the previous generation. Then
independently audit the change and its transitive impact on invariants, callers
and consumers, and success, failure, retry, cleanup, and terminal paths. Reuse
your prior validated coverage only where the change cannot invalidate it, and
re-review every invalidated area. Perform a complete exact-range review when the
change affects a core invariant, state machine, or externally visible contract;
adds a caller or concurrency, retry, cleanup, or terminal path; crosses subsystem
boundaries; or has an impact you cannot bound reliably. Otherwise, do not repeat
unchanged low-risk coverage merely for ceremony. Your verdict still applies to
the current exact candidate range. Do not assume any finding was covered by the
peer Reviewer, and do not depend on or guess the peer response.
";

/// The reviewer's only output obligation. Deliberately narrow: hcom parses one
/// anchored line and treats everything else as opaque payload.
const REVIEWER_OUTPUT_CONTRACT: &str = "
## Required output format

The FIRST line of your final message must be exactly one of:

VERDICT: LGTM
VERDICT: REQUEST_CHANGES

on its own line, with no decoration and no other text on that line. After it,
write one consolidated set of all independently confirmed findings from this
turn as concise free-form markdown (path:line references are helpful but not
required). State the exact candidate range or commit you reviewed, and end with
a brief `COVERAGE:` summary of the invariants, callers/consumers, and failure or
lifecycle paths you inspected. On a re-review, also state whether the change
triggered a complete exact-range review and why. Do not emit the internal
checklist or a long review narrative. If no blocking finding remains, say so
directly. You have the same native host view as a human-launched Codex session,
but the reviewer role forbids modifying the reviewed source, Git state,
installed artifacts, or branches. You may copy the tree elsewhere and build or
test that copy when it helps obtain independent evidence.
";

/// The Developer's control output obligation, appended to every turn because
/// role instructions are only transported when a native session is created.
const DEVELOPER_OUTPUT_CONTRACT: &str = "
## Required output format

The FIRST line of your final message MUST be exactly one of:

STATUS: READY
STATUS: CLARIFICATION_REQUIRED
STATUS: BLOCKED

on its own line, with no decoration and no other text on that line.

Use READY when the task is implemented and ready for review, including when you
made a smallest-impact, defensible local assumption. After the first line,
report changes, verification, repository/commit state, remaining work, and each
such assumption on its own `ASSUMPTION:` line.

Use CLARIFICATION_REQUIRED only when no defensible implementation choice can be
derived from the task, design, clarification records, applicable instructions,
and existing implementation; when choosing would decide material behavior,
acceptance, or scope; or when an explicit human, task, design, or applicable
instruction requires the run to remain uncommitted, which conflicts with the
standard lane's required candidate commit. For that authority conflict, do not
modify or commit the repository, and require an explicit human resolution rather
than accepting an Architect-derived override. After the first line, state the
exact decision needed, viable alternatives, consequences, what you inspected,
and the current repository/commit state.

Use BLOCKED only for an external or mechanical blocker you actually attempted
to overcome. After the first line, state what you tried, the exact observed
evidence, why no in-scope path remains, what human or external action is needed,
and the current repository/commit state. Compilation failure, test failure,
dependency setup work, or work taking longer than expected is not by itself a
blocker.

`ASSUMPTION:` and `REQUIREMENT_AMBIGUITY:` are agent-readable conventions;
hcom does not parse them.
";

const GITHUB_DEVELOPER_OUTPUT_CONTRACT: &str = "
## Required output format

The FIRST line of your final message MUST be exactly one of:

STATUS: READY
STATUS: CLARIFICATION_REQUIRED
STATUS: BLOCKED

on its own line, with no decoration and no other text on that line.

Use READY only when this turn's complete change is committed as exactly one new
signed-off child of the previously published head, all required checks passed,
the index/worktree are clean, and the foreground supervisor can validate and
publish it. Your exact final is externally published byte-for-byte without
redaction or secret scanning inside a generated GitHub body capped at 60 KiB
UTF-8. Report changes,
verification, exact commit/head state, remaining work, and each defensible local
assumption on its own `ASSUMPTION:` line.

Use CLARIFICATION_REQUIRED only when no defensible implementation choice can be
derived or an explicit instruction requires this turn to remain uncommitted,
which conflicts with the approved append-only GitHub lane. For that authority
conflict do not modify or commit the repository; require an explicit human
resolution. Use BLOCKED only after concrete attempts establish an external or
mechanical obstacle. Compilation or test failure and work taking longer than
expected are not by themselves blockers. Do not push; publication belongs to
the foreground supervisor.

`ASSUMPTION:` and `REQUIREMENT_AMBIGUITY:` are agent-readable conventions;
hcom does not parse them.
";

const GITHUB_DEVELOPER_ROLE_INSTRUCTIONS: &str = "You are the task Developer for an approved hcom GitHub Pull Request lane. Execute only the concrete selected task. Every Developer turn creates exactly one new signed-off child commit on the supervisor-published head; corrections append and never amend, rebase, squash, force-push, or rewrite published history. Use the frozen Developer App bot identity for author, committer, and Signed-off-by. Preserve the generated branch and managed worktree, leave the index/worktree clean, and never commit hcom-tasks artifacts unless the approved task explicitly names such a repository file. Do not push or invoke GitHub: the foreground supervisor alone validates and publishes the exact child commit and your exact final. That final is opaque payload published byte-for-byte without redaction or secret scanning to the selected private repository, inside a generated body with a 60 KiB UTF-8 hard cap; do not put credential material in it. Do not install, release, deploy, edit task/design/clarification sources outside approved scope, or wait for interactive input. Make ordinary local implementation choices yourself and report defensible uncertainty as ASSUMPTION:. Ask for clarification only when no approved candidate exists or the choice decides material externally visible scope. Report BLOCKED only after actual attempts prove an external or mechanical obstacle. An explicit instruction to remain uncommitted or unpushed conflicts with this approved lane and requires STATUS: CLARIFICATION_REQUIRED without repository mutation.";

const GITHUB_REVIEWER_ROLE_INSTRUCTIONS: &str = "You are the task Reviewer for an approved hcom GitHub Pull Request lane. Independently review the exact task base..published head range against the approved task, design, ordered clarifications, applicable instructions, implementation, and tests. Reviewer1 and Reviewer2 are equal peers with the same complete review scope and authority: there is no specialization or division of responsibility, neither may assume the other covers a category, and neither receives the peer final. The range may contain an initial task commit plus append-only correction commits; verify the contiguous published chain, frozen Developer identity and matching sign-offs, clean repository, and exclusion of hcom-tasks artifacts. An LGTM applies only to the exact current range and head. Do not edit source, index, refs, commits, branch, or HEAD; do not push, publish a GitHub review, install, or release. The foreground supervisor proves the unchanged head and publishes your exact final through your mapped Reviewer App. That final is opaque payload published byte-for-byte without redaction or secret scanning to the selected private repository, inside a generated body with a 60 KiB UTF-8 hard cap; do not put credential material in it. Use REQUIREMENT_AMBIGUITY: for unresolved requirement conflicts and return the normal verdict. In dual mode both independent turns have the same contract, six-hour timeout, and no extra join deadline or resource cap. Do not mutate a shared Cargo target; copy the tree and use an isolated target for write-producing checks.";

fn role_instructions(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => {
            "You are the task Developer: execute the concrete approved task; do not redesign its product scope. First seek answers in the task file, design and clarification files, applicable instructions, existing implementation, and tests. Make ordinary local implementation decisions yourself. If an uncertain choice has a defensible candidate, is consistent with the approved behavior and scope, and can be corrected in review, choose the smallest-impact option, continue, and disclose it as `ASSUMPTION:` in your final. Ask for clarification only when you cannot derive any defensible candidate or the choice would decide material externally visible behavior, acceptance, or scope. Report BLOCKED only after actual attempts establish an external or mechanical obstacle; include concrete observations. Work directly in the exact repository and complete the bounded task. The human's execution approval for this standard hcom lane authorizes exactly one signed-off local candidate commit for this task; a general instruction that commits require human authorization is satisfied by that run approval. If an explicit human, task, design, or applicable instruction instead requires this run to remain uncommitted, do not modify or commit the repository: return `STATUS: CLARIFICATION_REQUIRED` because that requirement is incompatible with the standard review lane and requires an explicit human resolution. Otherwise run the required checks, then commit the complete work as ONE NEW commit whose message describes this task as a whole and whose `Signed-off-by` trailer matches the committing identity (for example, create it with `git commit --signoff`). Never amend, squash, reword, or otherwise rewrite a commit that existed when your first task turn began. On correction or clarification resume, amend your existing task commit if it exists and ensure that it retains a valid matching `Signed-off-by` trailer; if no task commit exists yet, create the one signed-off task commit only after the implementation is complete. Never create a second task commit. Do not create a commit merely to pause. If a pause is necessary after your task commit already exists, leave that commit unchanged and report the exact repository state. Never add any `hcom-tasks` artifact to the task commit. This local candidate commit and its same-task amendments do not authorize push, install, or release. Do not push, install, wait for interactive input, or modify the task/design/clarification source files."
        }
        WorkerRole::Reviewer => {
            "You are the task Reviewer. Independently inspect the committed task range and decide whether it is sound against the approved task, design files, and every ordered clarification record. In dual-review mode, Reviewer1 and Reviewer2 are equal peers with the same complete review scope and authority: there is no role specialization or division of review responsibility, and neither Reviewer may assume the other will inspect any category. The human's execution approval for this standard hcom lane includes exactly one signed-off local Developer candidate commit and same-commit amendments during correction; it never includes push, install, or release. Review disclosed Developer assumptions rather than accepting them automatically. Confirm the developer left the work committed as a single commit for this task, with a message covering it, a valid `Signed-off-by` trailer matching the committing identity, and no `hcom-tasks` artifact; uncommitted work, a missing or mismatched sign-off, or a task split across several commits is a reason to request changes. If an explicit human, task, design, or applicable instruction requires the run to remain uncommitted, return `VERDICT: REQUEST_CHANGES` and label the incompatible workflow requirement `REQUIREMENT_AMBIGUITY:` instead of accepting either side of the contradiction. Distinguish other requirement ambiguity from implementation defects and label the former `REQUIREMENT_AMBIGUITY:` in findings. An LGTM applies to the exact final candidate range already committed; it does not call for another post-LGTM commit or a human decision about retaining that reviewed commit. You must not edit reviewed source, the Git index or refs, the candidate commit, stage, commit, change branch or HEAD, push, or install. In dual-review mode, two Reviewer turns run concurrently; in single-review mode, only Reviewer1 runs. Every Reviewer turn has an independent six-hour timeout and there is no extra join deadline, cgroup, CPU, memory, or Cargo concurrency cap. Do not clean or mutate a shared Cargo target directory; for checks that write build artifacts, copy the tree into your own writable sandbox and use an isolated target directory."
        }
    }
}

fn bounded_single_line(input: &str) -> String {
    let mut output: String = input
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if output.is_empty() {
        output.push_str("bounded driver failure");
    }
    if output.len() > 1024 {
        let mut boundary = 1024;
        while !output.is_char_boundary(boundary) {
            boundary -= 1;
        }
        output.truncate(boundary);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::environment::ParentEnvironment;
    use crate::worker::fake_runtime::{FakeTaskWorkerRuntime, FakeTurnScript};
    use crate::worker::profile::{
        ArchitectAdapter, ClaudeInvocationProfile, CodexInvocationProfile,
        DeveloperInvocationProfile, ReviewerId, ReviewerInvocationProfile,
        SessionInvocationProfiles,
    };
    use crate::worker::runtime::{
        CODEX_TASK_WORKER_ADAPTER, DeveloperOutcomeStatus, DeveloperOutcomeV1, ReviewerOutcomeV1,
        ReviewerVerdict, RuntimeOutcome, RuntimeTelemetry, TaskWorkerRuntime,
    };
    use std::collections::BTreeSet;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::process::Command;
    use std::sync::{Arc, Mutex};

    fn pure_codex_profiles(adapter: ArchitectAdapter) -> SessionInvocationProfiles {
        let mut profiles = SessionInvocationProfiles::for_task_lane(adapter).unwrap();
        *profiles.reviewer1_mut() = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile::reviewer_default(),
        };
        *profiles.reviewer2_mut() = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile::reviewer_default(),
        };
        profiles
    }

    fn reviewer_adapter_bindings(reviewer1: &str, reviewer2: &str) -> Vec<ReviewerAdapterBinding> {
        vec![
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer1,
                adapter: reviewer1.into(),
            },
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer2,
                adapter: reviewer2.into(),
            },
        ]
    }

    fn pure_codex_reviewer_adapters() -> Vec<ReviewerAdapterBinding> {
        reviewer_adapter_bindings(CODEX_TASK_WORKER_ADAPTER, CODEX_TASK_WORKER_ADAPTER)
    }

    fn pure_codex_single_reviewer_adapters() -> Vec<ReviewerAdapterBinding> {
        vec![ReviewerAdapterBinding {
            reviewer_id: ReviewerId::Reviewer1,
            adapter: CODEX_TASK_WORKER_ADAPTER.into(),
        }]
    }

    fn reviewer_paths(task: &crate::control_api::TaskStatusSnapshot) -> Vec<String> {
        task.reviewers
            .iter()
            .flat_map(|reviewer| reviewer.current_final_message_paths.clone())
            .collect()
    }

    fn joined_reviewer_verdict(
        task: &crate::control_api::TaskStatusSnapshot,
    ) -> Option<ReviewerVerdict> {
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

    fn binding_hash(profiles: &SessionInvocationProfiles, roots: &[PathBuf]) -> String {
        let workers = TaskWorkerProfiles::from_session_profiles(profiles).unwrap();
        let contract = workers.contract_identity();
        session_binding_hash(
            profiles,
            &workers,
            &contract,
            roots,
            &DeliveryBinding::LocalCandidate,
        )
        .unwrap()
    }

    #[test]
    fn session_binding_hash_covers_active_profiles_topology_contracts_and_ordered_roots() {
        let base = SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        let base_hash = binding_hash(&base, &[]);
        let single =
            SessionInvocationProfiles::for_single_review_task_lane(ArchitectAdapter::Codex)
                .unwrap();
        assert_ne!(
            base_hash,
            binding_hash(&single, &[]),
            "session binding hash must distinguish single and dual topology"
        );

        let mut architect = base.clone();
        architect.architect = crate::worker::profile::ArchitectInvocationProfile::Codex {
            profile: crate::worker::profile::CodexInvocationProfile {
                model: "architect-override".into(),
                ..crate::worker::profile::CodexInvocationProfile::architect_default()
            },
        };
        assert_ne!(base_hash, binding_hash(&architect, &[]));

        let mut developer = base.clone();
        developer.developer = DeveloperInvocationProfile::Claude {
            profile: ClaudeInvocationProfile::developer_default(),
        };
        assert_ne!(base_hash, binding_hash(&developer, &[]));

        let mut reviewer = base.clone();
        *reviewer.reviewer1_mut() = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile {
                model: "reviewer-one-override".into(),
                ..CodexInvocationProfile::reviewer_default()
            },
        };
        assert_ne!(base_hash, binding_hash(&reviewer, &[]));

        let mut reviewer2 = base.clone();
        *reviewer2.reviewer2_mut() = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile::reviewer_default(),
        };
        assert_ne!(base_hash, binding_hash(&reviewer2, &[]));

        let roots = [PathBuf::from("/source/one"), PathBuf::from("/source/two")];
        assert_ne!(base_hash, binding_hash(&base, &roots));
        let reversed = [roots[1].clone(), roots[0].clone()];
        assert_ne!(binding_hash(&base, &roots), binding_hash(&base, &reversed));

        let workers = TaskWorkerProfiles::from_session_profiles(&base).unwrap();
        let contract = workers.contract_identity();
        let app = |id, slug: &str| crate::control_api::GitHubAppBinding {
            app_id: id,
            installation_id: id + 10,
            slug: slug.into(),
            bot_user_id: id + 20,
            effective_permissions: BTreeMap::new(),
        };
        let github = DeliveryBinding::GitHubPullRequest {
            binding: Box::new(crate::control_api::GitHubPullRequestBinding {
                delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
                owner: "owner".into(),
                repository: "repo".into(),
                repository_id: 99,
                visibility: "private".into(),
                local_repository_root: "/repo".into(),
                base_branch: "master".into(),
                merge_method: "squash".into(),
                merge_wait_seconds: 21_600,
                delete_remote_branch_after_merge: true,
                architect_app: app(1, "arch"),
                developer_app: app(2, "dev"),
                reviewer_apps: vec![
                    crate::control_api::GitHubReviewerAppBinding {
                        reviewer_id: ReviewerId::Reviewer1,
                        app: app(3, "reviewer1"),
                    },
                    crate::control_api::GitHubReviewerAppBinding {
                        reviewer_id: ReviewerId::Reviewer2,
                        app: app(4, "reviewer2"),
                    },
                ],
                review_check_name: "hcom/review".into(),
            }),
        };
        let github_hash = session_binding_hash(&base, &workers, &contract, &[], &github).unwrap();
        assert_ne!(
            base_hash, github_hash,
            "delivery mode must bind the session hash"
        );
        let mut manual = github.clone();
        let DeliveryBinding::GitHubPullRequest { binding } = &mut manual else {
            unreachable!()
        };
        binding.delivery_policy = crate::control_api::GitHubDeliveryPolicy::Manual;
        let manual_hash = session_binding_hash(&base, &workers, &contract, &[], &manual).unwrap();
        assert_ne!(
            github_hash, manual_hash,
            "GitHub delivery policy must bind the session hash"
        );
    }

    struct MutableGitHubInspector {
        result: Mutex<crate::orchestrator::github::GitHubInspectionResult>,
        requests: Mutex<Vec<(String, u64)>>,
    }

    impl crate::orchestrator::github::GitHubInspectionProvider for MutableGitHubInspector {
        fn inspect(
            &self,
            request: &crate::orchestrator::github::GitHubInspectionRequest,
        ) -> Result<crate::orchestrator::github::GitHubInspectionResult> {
            self.requests
                .lock()
                .unwrap()
                .push((request.run_id.clone(), request.session_version));
            Ok(self.result.lock().unwrap().clone())
        }
    }

    impl crate::orchestrator::github::GitHubWorkflowProvider for MutableGitHubInspector {
        fn prepare_repository(
            &self,
            _workspace: &TasksWorkspace,
            _request: &crate::orchestrator::core::PrepareGitHubRunRequest,
        ) -> Result<crate::orchestrator::github::RepositoryPreparedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn publish_candidate(
            &self,
            _request: &crate::orchestrator::core::PublishDeveloperCandidateRequest,
        ) -> Result<crate::orchestrator::github::CandidatePublishedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn publish_review(
            &self,
            _request: &crate::orchestrator::core::PublishReviewerReviewRequest,
        ) -> Result<crate::orchestrator::github::ReviewerReviewPublishedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn publish_check(
            &self,
            _request: &crate::orchestrator::core::PublishReviewCheckRequest,
        ) -> Result<crate::orchestrator::github::ReviewCheckPublishedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn wait_for_merge_gate(
            &self,
            _request: &crate::orchestrator::core::WaitForMergeGateRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<crate::orchestrator::github::MergeGateObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn merge_pull_request(
            &self,
            _request: &crate::orchestrator::core::MergePullRequestRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<crate::orchestrator::github::PullRequestMergedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn finalize_run(
            &self,
            _request: &crate::orchestrator::core::FinalizeGitHubRunRequest,
        ) -> Result<crate::orchestrator::github::GitHubRunFinalizedObservation> {
            bail!("GitHub workflow is not exercised by inspection-only tests")
        }

        fn publish_terminal_best_effort(
            &self,
            _request: &crate::orchestrator::core::PublishGitHubTerminalRequest,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ScriptedGitHubFailure {
        Preparation,
        Review,
        Check,
        Merge,
        Finalization,
    }

    struct ScriptedGitHubWorkflow {
        heads: Mutex<VecDeque<String>>,
        audit: Mutex<Vec<String>>,
        worktree: Mutex<Option<PathBuf>>,
        fail_after_confirmation: Option<ScriptedGitHubFailure>,
        partial_operations:
            Mutex<BTreeMap<String, crate::orchestrator::core::GitHubPartialOperation>>,
        merge_gate_entered: Option<Arc<AtomicBool>>,
        merge_gate_release: Option<Arc<AtomicBool>>,
        merge_entered: Option<Arc<AtomicBool>>,
        merge_release: Option<Arc<AtomicBool>>,
    }

    impl ScriptedGitHubWorkflow {
        fn record(&self, value: impl Into<String>) {
            self.audit.lock().unwrap().push(value.into());
        }
    }

    impl crate::orchestrator::github::GitHubWorkflowProvider for ScriptedGitHubWorkflow {
        fn begin_fresh_run(&self, terminal_run_id: &str, fresh_run_id: &str) -> Result<()> {
            self.record(format!("fresh:{terminal_run_id}:{fresh_run_id}"));
            Ok(())
        }

        fn prepare_repository(
            &self,
            workspace: &TasksWorkspace,
            request: &crate::orchestrator::core::PrepareGitHubRunRequest,
        ) -> Result<crate::orchestrator::github::RepositoryPreparedObservation> {
            fs::create_dir(workspace.repository_path())?;
            *self.worktree.lock().unwrap() = Some(workspace.repository_path());
            self.record("prepare");
            if self.fail_after_confirmation == Some(ScriptedGitHubFailure::Preparation) {
                self.partial_operations.lock().unwrap().insert(
                    request.operation_id.clone(),
                    crate::orchestrator::core::GitHubPartialOperation::RepositoryPreparation(
                        crate::orchestrator::core::GitHubPartialRepositoryPreparation {
                            base_sha: request.run_binding.expected_base_sha.clone(),
                            branch: request.run_binding.generated_run_branch.clone(),
                            worktree_path: workspace
                                .repository_path()
                                .to_string_lossy()
                                .into_owned(),
                            local_base_ref_created: true,
                            local_branch_created: true,
                            local_worktree_created: true,
                        },
                    ),
                );
                bail!("scripted preparation evidence write failed after confirmation");
            }
            Ok(crate::orchestrator::github::RepositoryPreparedObservation {
                operation_id: request.operation_id.clone(),
                base_sha: request.run_binding.expected_base_sha.clone(),
                branch: request.run_binding.generated_run_branch.clone(),
                worktree_path: workspace.repository_path().to_string_lossy().into_owned(),
            })
        }

        fn publish_candidate(
            &self,
            request: &crate::orchestrator::core::PublishDeveloperCandidateRequest,
        ) -> Result<crate::orchestrator::github::CandidatePublishedObservation> {
            let head_sha = self
                .heads
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow!("scripted GitHub candidate heads are exhausted"))?;
            self.record(format!(
                "candidate:{}:{}:{}",
                request.task_ordinal,
                request.generation,
                request.existing_pr_number.is_some()
            ));
            let check_run_id =
                100 + u64::try_from(request.task_ordinal)? * 20 + u64::from(request.generation);
            Ok(crate::orchestrator::github::CandidatePublishedObservation {
                operation_id: request.operation_id.clone(),
                task_ordinal: request.task_ordinal,
                generation: request.generation,
                previous_head_sha: request.previous_head_sha.clone(),
                head_sha,
                pr_number: request.existing_pr_number.unwrap_or(7),
                pr_node_id: "PR_scripted".into(),
                pr_url: "https://github.com/owner/repo/pull/7".into(),
                pr_actor_bot_user_id: 22,
                check_run_id,
                check_url: format!("https://github.com/owner/repo/runs/{check_run_id}"),
                check_actor_app_id: 1,
            })
        }

        fn publish_review(
            &self,
            request: &crate::orchestrator::core::PublishReviewerReviewRequest,
        ) -> Result<crate::orchestrator::github::ReviewerReviewPublishedObservation> {
            self.record(format!(
                "review:{}:{}:{}:{:?}",
                request.task_ordinal,
                request.generation,
                request.reviewer_id.as_str(),
                request.verdict
            ));
            let lane = match request.reviewer_id {
                ReviewerId::Reviewer1 => 1,
                ReviewerId::Reviewer2 => 2,
            };
            let review_id = 200
                + u64::try_from(request.task_ordinal)? * 40
                + u64::from(request.generation) * 2
                + lane;
            let observed = crate::orchestrator::github::ReviewerReviewPublishedObservation {
                operation_id: request.operation_id.clone(),
                task_ordinal: request.task_ordinal,
                reviewer_id: request.reviewer_id,
                generation: request.generation,
                head_sha: request.head_sha.clone(),
                verdict: request.verdict,
                review_id,
                review_url: format!(
                    "https://github.com/owner/repo/pull/7#pullrequestreview-{review_id}"
                ),
                actor_bot_user_id: if request.reviewer_id == ReviewerId::Reviewer1 {
                    23
                } else {
                    24
                },
                final_artifact_sha256: format!("{review_id:064x}"),
            };
            if self.fail_after_confirmation == Some(ScriptedGitHubFailure::Review) {
                self.partial_operations.lock().unwrap().insert(
                    request.operation_id.clone(),
                    crate::orchestrator::core::GitHubPartialOperation::ReviewerReview(
                        crate::orchestrator::core::GitHubPartialReviewerReview {
                            task_ordinal: observed.task_ordinal,
                            reviewer_id: observed.reviewer_id,
                            generation: observed.generation,
                            head_sha: observed.head_sha.clone(),
                            verdict: observed.verdict,
                            review_id: observed.review_id,
                            review_url: observed.review_url.clone(),
                            actor_bot_user_id: observed.actor_bot_user_id,
                            final_artifact_sha256: observed.final_artifact_sha256.clone(),
                        },
                    ),
                );
                bail!("scripted review evidence write failed after confirmation");
            }
            Ok(observed)
        }

        fn publish_check(
            &self,
            request: &crate::orchestrator::core::PublishReviewCheckRequest,
        ) -> Result<crate::orchestrator::github::ReviewCheckPublishedObservation> {
            self.record(format!(
                "check:{}:{}:{}",
                request.task_ordinal,
                request.generation,
                request.conclusion.as_str()
            ));
            let observed = crate::orchestrator::github::ReviewCheckPublishedObservation {
                operation_id: request.operation_id.clone(),
                task_ordinal: request.task_ordinal,
                generation: request.generation,
                head_sha: request.head_sha.clone(),
                check_run_id: request.check_run_id,
                check_url: request.check_url.clone(),
                state: request.conclusion.as_str().into(),
                actor_app_id: 1,
            };
            if self.fail_after_confirmation == Some(ScriptedGitHubFailure::Check) {
                self.partial_operations.lock().unwrap().insert(
                    request.operation_id.clone(),
                    crate::orchestrator::core::GitHubPartialOperation::ReviewCheck(
                        crate::orchestrator::core::GitHubPartialReviewCheck {
                            task_ordinal: observed.task_ordinal,
                            generation: observed.generation,
                            head_sha: observed.head_sha.clone(),
                            check_run_id: observed.check_run_id,
                            check_url: observed.check_url.clone(),
                            state: observed.state.clone(),
                            actor_app_id: observed.actor_app_id,
                        },
                    ),
                );
                bail!("scripted Check evidence write failed after confirmation");
            }
            Ok(observed)
        }

        fn wait_for_merge_gate(
            &self,
            request: &crate::orchestrator::core::WaitForMergeGateRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<crate::orchestrator::github::MergeGateObservation> {
            self.record("merge-gate");
            if let Some(entered) = self.merge_gate_entered.as_ref() {
                entered.store(true, Ordering::Release);
            }
            if let Some(release) = self.merge_gate_release.as_ref() {
                while !release.load(Ordering::Acquire) {
                    if _cancelled.load(Ordering::Acquire) {
                        return Err(
                            crate::orchestrator::github::workflow::GitHubWorkflowCancelled.into(),
                        );
                    }
                    std::thread::yield_now();
                }
            }
            Ok(crate::orchestrator::github::MergeGateObservation {
                operation_id: request.operation_id.clone(),
                pr_number: request.pr_number,
                final_head_sha: request.final_head_sha.clone(),
                base_sha: "a".repeat(40),
                ruleset_attestation_sha256: "b".repeat(64),
                check_run_id: request.check_run_id,
            })
        }

        fn merge_pull_request(
            &self,
            request: &crate::orchestrator::core::MergePullRequestRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<crate::orchestrator::github::PullRequestMergedObservation> {
            self.record("merge");
            if let Some(entered) = self.merge_entered.as_ref() {
                entered.store(true, Ordering::Release);
            }
            if let Some(release) = self.merge_release.as_ref() {
                while !release.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
            }
            let observed = crate::orchestrator::github::PullRequestMergedObservation {
                operation_id: request.operation_id.clone(),
                pr_number: request.pr_number,
                final_head_sha: request.final_head_sha.clone(),
                merge_sha: "f".repeat(40),
                merge_url: "https://github.com/owner/repo/commit/final".into(),
                actor_bot_user_id: 21,
                merge_evidence_durable: true,
            };
            if self.fail_after_confirmation == Some(ScriptedGitHubFailure::Merge) {
                self.partial_operations.lock().unwrap().insert(
                    request.operation_id.clone(),
                    crate::orchestrator::core::GitHubPartialOperation::Merge(
                        crate::orchestrator::core::GitHubPartialMerge {
                            pr_number: observed.pr_number,
                            final_head_sha: observed.final_head_sha.clone(),
                            merge_sha: observed.merge_sha.clone(),
                            merge_url: observed.merge_url.clone(),
                            actor_bot_user_id: observed.actor_bot_user_id,
                        },
                    ),
                );
                bail!("scripted merge evidence write failed after confirmation");
            }
            Ok(observed)
        }

        fn finalize_run(
            &self,
            request: &crate::orchestrator::core::FinalizeGitHubRunRequest,
        ) -> Result<crate::orchestrator::github::GitHubRunFinalizedObservation> {
            let worktree = self
                .worktree
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| anyhow!("scripted managed worktree is missing"))?;
            fs::remove_dir(&worktree)?;
            self.record("finalize");
            let observed = crate::orchestrator::github::GitHubRunFinalizedObservation {
                operation_id: request.operation_id.clone(),
                pr_number: request.pr_number,
                final_head_sha: request.final_head_sha.clone(),
                merge_sha: request.merge_sha.clone(),
                finalization: crate::control_api::GitHubFinalizationSnapshot {
                    local_worktree_removed: true,
                    local_ref_removed: true,
                    remote_ref_outcome: Some("deleted".into()),
                },
            };
            if self.fail_after_confirmation == Some(ScriptedGitHubFailure::Finalization) {
                self.partial_operations.lock().unwrap().insert(
                    request.operation_id.clone(),
                    crate::orchestrator::core::GitHubPartialOperation::Finalization(
                        crate::orchestrator::core::GitHubPartialFinalization {
                            pr_number: observed.pr_number,
                            final_head_sha: observed.final_head_sha.clone(),
                            merge_sha: observed.merge_sha.clone(),
                            finalization: crate::control_api::GitHubFinalizationSnapshot {
                                local_worktree_removed: true,
                                local_ref_removed: false,
                                remote_ref_outcome: None,
                            },
                        },
                    ),
                );
                bail!("scripted finalization evidence write failed after cleanup");
            }
            Ok(observed)
        }

        fn take_partial_operation(
            &self,
            operation_id: &str,
        ) -> Result<Option<crate::orchestrator::core::GitHubPartialOperation>> {
            Ok(self.partial_operations.lock().unwrap().remove(operation_id))
        }

        fn publish_terminal_best_effort(
            &self,
            request: &crate::orchestrator::core::PublishGitHubTerminalRequest,
        ) -> Result<()> {
            self.record(format!("terminal:{}", request.outcome));
            Ok(())
        }
    }

    fn github_runtime_for_test(
        repository: &Path,
    ) -> (
        crate::orchestrator::github::GitHubRuntimeBinding,
        Arc<MutableGitHubInspector>,
    ) {
        let app =
            |id, slug: &str, permissions: &[(&str, crate::control_api::GitHubPermissionLevel)]| {
                crate::control_api::GitHubAppBinding {
                    app_id: id,
                    installation_id: id + 10,
                    slug: slug.into(),
                    bot_user_id: id + 20,
                    effective_permissions: permissions
                        .iter()
                        .map(|(name, level)| ((*name).into(), *level))
                        .collect(),
                }
            };
        let reviewer_permissions = [(
            "pull_requests",
            crate::control_api::GitHubPermissionLevel::Write,
        )];
        let binding = crate::control_api::GitHubPullRequestBinding {
            delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
            owner: "owner".into(),
            repository: "repo".into(),
            repository_id: 99,
            visibility: "private".into(),
            local_repository_root: repository.to_string_lossy().into_owned(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 21_600,
            delete_remote_branch_after_merge: true,
            architect_app: app(
                1,
                "arch",
                &[
                    (
                        "administration",
                        crate::control_api::GitHubPermissionLevel::Read,
                    ),
                    ("checks", crate::control_api::GitHubPermissionLevel::Write),
                    ("contents", crate::control_api::GitHubPermissionLevel::Write),
                    (
                        "pull_requests",
                        crate::control_api::GitHubPermissionLevel::Write,
                    ),
                ],
            ),
            developer_app: app(
                2,
                "dev",
                &[
                    ("contents", crate::control_api::GitHubPermissionLevel::Write),
                    (
                        "pull_requests",
                        crate::control_api::GitHubPermissionLevel::Write,
                    ),
                ],
            ),
            reviewer_apps: vec![
                crate::control_api::GitHubReviewerAppBinding {
                    reviewer_id: ReviewerId::Reviewer1,
                    app: app(3, "reviewer1", &reviewer_permissions),
                },
                crate::control_api::GitHubReviewerAppBinding {
                    reviewer_id: ReviewerId::Reviewer2,
                    app: app(4, "reviewer2", &reviewer_permissions),
                },
            ],
            review_check_name: crate::control_api::GITHUB_REVIEW_CHECK_NAME.into(),
        };
        let initial_inspection = GitHubInspectionBinding {
            inspected_repository_id: 99,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: Some("b".repeat(64)),
            inspection_id: "inspection-initial".into(),
        };
        let refreshed = GitHubInspectionBinding {
            inspection_id: "inspection-refreshed".into(),
            ..initial_inspection.clone()
        };
        let inspector = Arc::new(MutableGitHubInspector {
            result: Mutex::new(crate::orchestrator::github::GitHubInspectionResult {
                delivery_binding: binding.clone(),
                inspection: refreshed,
            }),
            requests: Mutex::new(Vec::new()),
        });
        (
            crate::orchestrator::github::GitHubRuntimeBinding {
                binding,
                initial_inspection,
                inspector: inspector.clone(),
                workflow: inspector.clone(),
            },
            inspector,
        )
    }

    fn github_runtime_for_single_review_test(
        repository: &Path,
    ) -> (
        crate::orchestrator::github::GitHubRuntimeBinding,
        Arc<MutableGitHubInspector>,
    ) {
        let (mut runtime, inspector) = github_runtime_for_test(repository);
        runtime.binding.reviewer_apps.truncate(1);
        inspector
            .result
            .lock()
            .unwrap()
            .delivery_binding
            .reviewer_apps
            .truncate(1);
        (runtime, inspector)
    }

    fn github_runtime_for_manual_single_review_test(
        repository: &Path,
    ) -> (
        crate::orchestrator::github::GitHubRuntimeBinding,
        Arc<MutableGitHubInspector>,
    ) {
        let (mut runtime, inspector) = github_runtime_for_single_review_test(repository);
        runtime.binding.delivery_policy = crate::control_api::GitHubDeliveryPolicy::Manual;
        runtime.initial_inspection.ruleset_attestation_sha256 = None;
        {
            let mut result = inspector.result.lock().unwrap();
            result.delivery_binding.delivery_policy =
                crate::control_api::GitHubDeliveryPolicy::Manual;
            result.inspection.ruleset_attestation_sha256 = None;
        }
        (runtime, inspector)
    }

    #[test]
    fn github_inspection_is_read_only_and_plan_start_bind_exact_observation() {
        let fixture = Fixture::new();
        let (runtime, inspector) = github_runtime_for_test(&fixture.repository);
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            fixture.sources.profiles.clone().unwrap(),
            Vec::new(),
            runtime,
        )
        .unwrap();
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-test".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: VecDeque::new(),
                audit: Arc::new(Mutex::new(Audit::default())),
            }),
        )
        .unwrap();

        let before = supervisor.snapshot();
        assert_eq!(before.version, 0);
        assert_eq!(
            before
                .github
                .as_ref()
                .unwrap()
                .latest_inspection
                .as_ref()
                .unwrap()
                .inspection_id,
            "inspection-initial"
        );
        let refreshed = supervisor
            .inspect_github_delivery(0, "run-github-test")
            .unwrap();
        assert_eq!(refreshed.inspection_id, "inspection-refreshed");
        assert_eq!(supervisor.snapshot().version, 0);

        let task = fixture.task("github-binding", &[], 7);
        assert!(
            supervisor
                .replace_plan_with_inspection(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    Some("inspection-initial"),
                    vec![task.clone()],
                )
                .is_err()
        );
        let mut wrong_root = task.clone();
        wrong_root.repository_root = fixture.project_root.to_string_lossy().into_owned();
        assert!(
            supervisor
                .replace_plan_with_inspection(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    Some("inspection-refreshed"),
                    vec![wrong_root],
                )
                .is_err()
        );
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-refreshed"),
                vec![task],
            )
            .unwrap();
        let planned = supervisor.snapshot();
        let run_binding = planned.github.unwrap().run_binding.unwrap();
        assert_eq!(
            planned.tasks[0].branch.as_deref(),
            Some(run_binding.generated_run_branch.as_str())
        );
        assert_eq!(
            planned.tasks[0].base_revision.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

        let duplicate_error = supervisor
            .inspect_github_delivery(1, "run-github-test")
            .unwrap_err();
        assert!(duplicate_error.to_string().contains("new inspection ID"));
        assert!(
            supervisor
                .approve_and_start(1, plan_version, &plan_hash, true)
                .unwrap_err()
                .to_string()
                .contains("inspection was superseded")
        );

        inspector.result.lock().unwrap().inspection.inspection_id = "inspection-newest".into();
        supervisor
            .inspect_github_delivery(1, "run-github-test")
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(error.to_string().contains("inspection was superseded"));
        let task = fixture.task("github-binding", &[], 7);
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                1,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-newest"),
                vec![task],
            )
            .unwrap();
        inspector
            .result
            .lock()
            .unwrap()
            .inspection
            .expected_base_sha = "c".repeat(40);
        let error = supervisor
            .approve_and_start(2, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(error.to_string().contains("changed after plan binding"));
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        assert!(!fixture.project_root.join("hcom-tasks").exists());

        inspector
            .result
            .lock()
            .unwrap()
            .inspection
            .expected_base_sha = "a".repeat(40);
        let retry_error = supervisor
            .approve_and_start(2, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(
            retry_error
                .to_string()
                .contains("inspection was superseded")
        );

        inspector.result.lock().unwrap().inspection.inspection_id =
            "inspection-after-base-drift".into();
        supervisor
            .inspect_github_delivery(2, "run-github-test")
            .unwrap();
        let task = fixture.task("github-binding", &[], 7);
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                2,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-after-base-drift"),
                vec![task],
            )
            .unwrap();
        inspector
            .result
            .lock()
            .unwrap()
            .delivery_binding
            .architect_app
            .effective_permissions
            .insert(
                "workflows".into(),
                crate::control_api::GitHubPermissionLevel::Write,
            );
        let permission_error = supervisor
            .approve_and_start(3, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(
            permission_error
                .to_string()
                .contains("permission map drifted")
        );
        inspector
            .result
            .lock()
            .unwrap()
            .delivery_binding
            .architect_app
            .effective_permissions
            .remove("workflows");
        let retry_error = supervisor
            .approve_and_start(3, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(
            retry_error
                .to_string()
                .contains("inspection was superseded")
        );
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        assert!(!fixture.project_root.join("hcom-tasks").exists());
        assert_eq!(
            inspector.requests.lock().unwrap().as_slice(),
            [
                ("run-github-test".into(), 0),
                ("run-github-test".into(), 1),
                ("run-github-test".into(), 1),
                ("run-github-test".into(), 2),
                ("run-github-test".into(), 2),
                ("run-github-test".into(), 3)
            ]
        );
    }

    #[test]
    fn github_driver_executes_one_pr_multi_task_correction_and_finalization() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) = github_runtime_for_test(&fixture.repository);
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from([
                "c".repeat(40),
                "d".repeat(40),
                "e".repeat(40),
            ])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: None,
            merge_gate_release: None,
            merge_entered: None,
            merge_release: None,
        });
        runtime.workflow = workflow.clone();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            fixture.sources.profiles.clone().unwrap(),
            Vec::new(),
            runtime,
        )
        .unwrap();
        let worker_audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = vec![
            task_script(
                "github-one",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("github-one-initial")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [request_changes("github-one-finding")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::DeveloperCorrection,
                        [ready("github-one-correction")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::ReviewerRereview,
                        [lgtm("github-one-lgtm")],
                    ),
                ],
                vec![Mutation::None; 4],
            ),
            task_script(
                "github-two",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("github-two-initial")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("github-two-lgtm")],
                    ),
                ],
                vec![Mutation::None; 2],
            ),
        ];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-test".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: worker_audit.clone(),
            }),
        )
        .unwrap();
        let tasks = vec![
            fixture.task(
                "github-one",
                &[],
                crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS,
            ),
            fixture.task(
                "github-two",
                &[],
                crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS,
            ),
        ];
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-initial"),
                tasks,
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !supervisor.snapshot().state.is_terminal() {
            assert!(
                std::time::Instant::now() < deadline,
                "GitHub workflow did not become terminal before the bounded test deadline"
            );
            supervisor.poll_once().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::Completed);
        let github = snapshot.github.unwrap();
        assert_eq!(github.pr_number, Some(7));
        assert_eq!(
            github.outcome,
            Some(crate::control_api::GitHubDeliveryOutcome::Delivered)
        );
        assert_eq!(github.merge_sha, Some("f".repeat(40)));
        assert_eq!(snapshot.tasks[0].base_revision, Some("a".repeat(40)));
        assert_eq!(snapshot.tasks[0].head_revision, Some("d".repeat(40)));
        assert_eq!(snapshot.tasks[1].base_revision, Some("d".repeat(40)));
        assert_eq!(snapshot.tasks[1].head_revision, Some("e".repeat(40)));
        assert!(
            !fixture
                .project_root
                .join("hcom-tasks/run-github-test/repository")
                .exists()
        );

        let operations = workflow.audit.lock().unwrap().clone();
        assert_eq!(operations.first().map(String::as_str), Some("prepare"));
        assert_eq!(
            operations
                .iter()
                .filter(|entry| entry.starts_with("candidate:"))
                .count(),
            3
        );
        assert!(operations.contains(&"candidate:0:1:false".into()));
        assert!(operations.contains(&"candidate:0:2:true".into()));
        assert!(operations.contains(&"candidate:1:1:true".into()));
        assert_eq!(
            &operations[operations.len() - 3..],
            ["merge-gate", "merge", "finalize"]
        );

        let audit = worker_audit.lock().unwrap();
        assert_eq!(audit.opens, ["github-one", "github-two"]);
        let first_reviewer_poll = audit
            .lane_events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ScriptedLaneEvent::TurnPolled(WorkerLane::Reviewer(_))
                )
            })
            .unwrap();
        for reviewer_id in [ReviewerId::Reviewer1, ReviewerId::Reviewer2] {
            let start = audit
                .lane_events
                .iter()
                .position(|event| {
                    *event == ScriptedLaneEvent::TurnStarted(WorkerLane::Reviewer(reviewer_id))
                })
                .unwrap();
            assert!(
                start < first_reviewer_poll,
                "both GitHub Reviewer lanes must start before either is polled"
            );
        }
        for (lane, instructions) in &audit.role_instructions {
            for required in [
                "published byte-for-byte without redaction or secret scanning",
                "60 KiB UTF-8 hard cap",
                "do not put credential material in it",
            ] {
                assert!(
                    instructions.contains(required),
                    "{lane:?} GitHub role contract omitted {required}"
                );
            }
            match lane {
                WorkerLane::Developer => {
                    assert!(instructions.contains("exact frozen Developer commit identity"));
                    assert!(instructions.contains("remain uncommitted or unpushed"));
                }
                WorkerLane::Reviewer(_) => {
                    assert!(instructions.contains("neither receives the peer final"));
                    assert!(instructions.contains("mapped Reviewer App"));
                }
            }
        }
        let first_developer = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Developer && *purpose == RuntimeTurnPurpose::InitialDevelopment
            })
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        assert!(first_developer.contains("exact commit author, committer, and Signed-off-by"));
        assert!(first_developer.contains("published byte-for-byte without redaction"));
        assert!(first_developer.contains("60 KiB UTF-8 hard cap"));
        let correction = audit
            .prompts
            .iter()
            .find(|(_, purpose, _)| *purpose == RuntimeTurnPurpose::DeveloperCorrection)
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        assert!(correction.contains("exactly one new signed-off child commit"));
        assert!(!correction.contains("git commit --amend"));
        assert!(correction.contains("published byte-for-byte without redaction"));
        for (_, purpose, prompt) in audit
            .prompts
            .iter()
            .filter(|(role, _, _)| *role == WorkerRole::Reviewer)
        {
            assert!(prompt.contains("Reviewer1 and Reviewer2 are equal peers"));
            assert!(prompt.contains("exact range:"));
            assert!(prompt.contains("foreground supervisor publishes your exact final"));
            assert!(prompt.contains("published byte-for-byte without redaction"));
            assert!(prompt.contains("60 KiB UTF-8 hard cap"));
            if *purpose == RuntimeTurnPurpose::ReviewerRereview {
                assert!(prompt.contains("First verify every finding you raised"));
            }
        }
    }

    #[test]
    fn github_manual_driver_preserves_review_complete_pr_without_merge_or_finalization() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) =
            github_runtime_for_manual_single_review_test(&fixture.repository);
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from(["c".repeat(40)])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: None,
            merge_gate_release: None,
            merge_entered: None,
            merge_release: None,
        });
        runtime.workflow = workflow.clone();
        let profiles =
            SessionInvocationProfiles::for_single_review_task_lane(ArchitectAdapter::Codex)
                .unwrap();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            profiles,
            Vec::new(),
            runtime,
        )
        .unwrap();
        let scripts = vec![single_reviewer_task_script(
            "github-manual",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("github-manual-initial")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("github-manual-lgtm")],
                ),
            ],
            vec![Mutation::None; 2],
        )];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-manual".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: Arc::new(Mutex::new(Audit::default())),
            }),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_single_reviewer_adapters(),
                Some("inspection-initial"),
                vec![fixture.task(
                    "github-manual",
                    &[],
                    crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS,
                )],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !supervisor.snapshot().state.is_terminal() {
            assert!(
                std::time::Instant::now() < deadline,
                "manual GitHub workflow did not become terminal"
            );
            supervisor.poll_once().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }

        let snapshot = supervisor.snapshot();
        let DeliveryBinding::GitHubPullRequest { binding } = &snapshot.delivery_binding else {
            panic!("manual run lost its GitHub binding")
        };
        assert_eq!(
            binding.delivery_policy,
            crate::control_api::GitHubDeliveryPolicy::Manual
        );
        let github = snapshot.github.unwrap();
        assert_eq!(
            github.outcome,
            Some(crate::control_api::GitHubDeliveryOutcome::ReviewCompleteUnmerged)
        );
        assert_eq!(
            github.phase,
            Some(crate::control_api::GitHubDeliveryPhase::PreservedUnmerged)
        );
        assert_eq!(
            github
                .run_binding
                .as_ref()
                .and_then(|run| run.ruleset_attestation_sha256.as_ref()),
            None
        );
        assert!(github.merge_sha.is_none());
        assert!(github.finalization.is_none());
        assert!(github.preserved_branch.is_some());
        assert!(github.preserved_worktree.is_some());
        assert!(
            fixture
                .project_root
                .join("hcom-tasks/run-github-manual/repository")
                .exists()
        );
        let operations = workflow.audit.lock().unwrap();
        assert!(operations.iter().all(|operation| {
            !matches!(operation.as_str(), "merge-gate" | "merge" | "finalize")
        }));
        assert!(
            operations
                .iter()
                .any(|operation| operation.starts_with("check:"))
        );
        drop(operations);
        let plan = fs::read_to_string(
            fixture
                .project_root
                .join("hcom-tasks/run-github-manual/plan.md"),
        )
        .unwrap();
        assert!(plan.contains("delivery policy: manual"));
        assert!(plan.contains("server-side enforcement: not attested"));
        assert!(plan.contains("review_complete_unmerged"));
        assert!(plan.contains("no merge, remote branch deletion, or merged-run finalization"));
        assert!(!plan.contains("ruleset attestation SHA-256:"));
    }

    #[test]
    fn github_single_reviewer_driver_omits_reviewer2_from_runtime_publication_and_status() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) = github_runtime_for_single_review_test(&fixture.repository);
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from(["c".repeat(40)])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: None,
            merge_gate_release: None,
            merge_entered: None,
            merge_release: None,
        });
        runtime.workflow = workflow.clone();
        let profiles =
            SessionInvocationProfiles::for_single_review_task_lane(ArchitectAdapter::Codex)
                .unwrap();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            profiles,
            Vec::new(),
            runtime,
        )
        .unwrap();
        let worker_audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = vec![single_reviewer_task_script(
            "github-single",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("github-single-initial")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("github-single-lgtm")],
                ),
            ],
            vec![Mutation::None; 2],
        )];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-single".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: worker_audit.clone(),
            }),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_single_reviewer_adapters(),
                Some("inspection-initial"),
                vec![fixture.task(
                    "github-single",
                    &[],
                    crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS,
                )],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !supervisor.snapshot().state.is_terminal() {
            assert!(
                std::time::Instant::now() < deadline,
                "single-review GitHub workflow did not become terminal"
            );
            supervisor.poll_once().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }

        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.reviewer_bindings.len(), 1);
        assert_eq!(
            snapshot.reviewer_bindings[0].reviewer_id,
            ReviewerId::Reviewer1
        );
        let DeliveryBinding::GitHubPullRequest { binding } = &snapshot.delivery_binding else {
            panic!("single-review run lost its GitHub delivery binding")
        };
        assert_eq!(binding.reviewer_apps.len(), 1);
        assert_eq!(binding.reviewer_apps[0].reviewer_id, ReviewerId::Reviewer1);
        assert_eq!(snapshot.tasks[0].reviewers.len(), 1);
        assert_eq!(snapshot.tasks[0].github_reviews.len(), 1);
        assert_eq!(
            snapshot.tasks[0].github_reviews[0].reviewer_id,
            ReviewerId::Reviewer1
        );

        let operations = workflow.audit.lock().unwrap();
        let reviews = operations
            .iter()
            .filter(|entry| entry.starts_with("review:"))
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(reviews, ["review:0:1:reviewer1:Lgtm"]);
        assert!(operations.iter().all(|entry| !entry.contains("reviewer2")));
        drop(operations);

        let audit = worker_audit.lock().unwrap();
        assert!(
            audit
                .lane_sessions
                .iter()
                .all(|(_, lane, _)| { *lane != WorkerLane::Reviewer(ReviewerId::Reviewer2) })
        );
        assert!(
            audit
                .role_instructions
                .iter()
                .all(|(lane, _)| { *lane != WorkerLane::Reviewer(ReviewerId::Reviewer2) })
        );
    }

    #[test]
    fn github_correction_clarification_resume_keeps_append_only_commit_instructions() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) = github_runtime_for_single_review_test(&fixture.repository);
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from(["c".repeat(40), "d".repeat(40)])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: None,
            merge_gate_release: None,
            merge_entered: None,
            merge_release: None,
        });
        runtime.workflow = workflow.clone();
        let profiles =
            SessionInvocationProfiles::for_single_review_task_lane(ArchitectAdapter::Codex)
                .unwrap();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            profiles,
            Vec::new(),
            runtime,
        )
        .unwrap();
        let worker_audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = vec![single_reviewer_task_script(
            "github-clarify",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("github-clarify-initial")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes("github-clarify-finding")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [clarification_required("github-clarify-question")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperClarificationResume,
                    [ready("github-clarify-correction")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    [lgtm("github-clarify-lgtm")],
                ),
            ],
            vec![Mutation::None; 5],
        )];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-clarify".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: worker_audit.clone(),
            }),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_single_reviewer_adapters(),
                Some("inspection-initial"),
                vec![fixture.task(
                    "github-clarify",
                    &[],
                    crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS,
                )],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let (action_snapshot, pending) = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "GitHub correction clarification did not reach the Architect action"
            );
            supervisor.poll_once().unwrap();
            let snapshot = supervisor.snapshot();
            if let Some(pending) = snapshot.pending_architect_action.clone() {
                break (snapshot, pending);
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(action_snapshot.tasks[0].review_round, 1);
        fs::write(
            &pending.clarification_output_path,
            "Apply the approved narrow correction.\n",
        )
        .unwrap();
        supervisor
            .submit_clarification(
                action_snapshot.version,
                pending.task_ordinal,
                &pending.task_key,
                pending.sequence,
                &pending.developer_request_path,
                &pending.clarification_output_path,
                false,
            )
            .unwrap();

        let terminal = drive_terminal(&mut supervisor);
        assert_eq!(terminal.state, SessionState::Completed);
        assert_eq!(terminal.tasks[0].review_round, 2);
        let operations = workflow.audit.lock().unwrap();
        assert_eq!(
            operations
                .iter()
                .filter(|entry| entry.starts_with("candidate:"))
                .count(),
            2
        );
        drop(operations);

        let audit = worker_audit.lock().unwrap();
        let resume_prompt = audit
            .prompts
            .iter()
            .find(|(_, purpose, _)| *purpose == RuntimeTurnPurpose::DeveloperClarificationResume)
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        for required in [
            "Preserve every published commit exactly",
            "exactly one new signed-off child commit",
            "do not amend, rebase, squash, reword, force-push, or push",
            "explicit no-commit or no-push conflict",
        ] {
            assert!(
                resume_prompt.contains(required),
                "GitHub clarification resume omitted {required}"
            );
        }
        assert!(!resume_prompt.contains("correction to amend into it"));
    }

    #[test]
    fn github_driver_routes_confirmed_partial_operations_before_terminalizing() {
        for failure in [
            ScriptedGitHubFailure::Preparation,
            ScriptedGitHubFailure::Review,
            ScriptedGitHubFailure::Check,
            ScriptedGitHubFailure::Merge,
            ScriptedGitHubFailure::Finalization,
        ] {
            let fixture = Fixture::new();
            let (mut runtime, _inspector) = github_runtime_for_test(&fixture.repository);
            let workflow = Arc::new(ScriptedGitHubWorkflow {
                heads: Mutex::new(VecDeque::from(["c".repeat(40)])),
                audit: Mutex::new(Vec::new()),
                worktree: Mutex::new(None),
                fail_after_confirmation: Some(failure),
                partial_operations: Mutex::new(BTreeMap::new()),
                merge_gate_entered: None,
                merge_gate_release: None,
                merge_entered: None,
                merge_release: None,
            });
            runtime.workflow = workflow.clone();
            let sources = SessionRuntimeSources::capture_with_github(
                fixture.sources.parent_environment.clone(),
                fixture.sources.profiles.clone().unwrap(),
                Vec::new(),
                runtime,
            )
            .unwrap();
            let scripts = vec![task_script(
                "github-partial",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("github-partial-initial")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("github-partial-reviewer1")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("github-partial-reviewer2")],
                    ),
                ],
                vec![Mutation::None; 3],
            )];
            let mut supervisor = TaskLaneSupervisor::open_with_factory(
                format!("run-github-partial-{failure:?}").to_ascii_lowercase(),
                fixture.project_root.clone(),
                fixture.run_root.clone(),
                sources,
                Box::new(ScriptedFactory {
                    scripts: scripts.into(),
                    audit: Arc::new(Mutex::new(Audit::default())),
                }),
            )
            .unwrap();
            let (plan_version, plan_hash) = supervisor
                .replace_plan_with_inspection(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    Some("inspection-initial"),
                    vec![fixture.task(
                        "github-partial",
                        &[],
                        crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS,
                    )],
                )
                .unwrap();
            let start = supervisor.approve_and_start(1, plan_version, &plan_hash, true);

            let mut observed_error = start.err().map(|error| error.to_string());
            if failure != ScriptedGitHubFailure::Preparation {
                assert!(
                    observed_error.is_none(),
                    "{failure:?} failed during preparation"
                );
                for _ in 0..256 {
                    match supervisor.poll_once() {
                        Ok(()) => {}
                        Err(error) => {
                            observed_error = Some(error.to_string());
                            break;
                        }
                    }
                    if supervisor.snapshot().state.is_terminal() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            assert!(
                observed_error
                    .as_deref()
                    .is_some_and(|detail| detail.contains("scripted")),
                "{failure:?} did not return its original workflow failure"
            );
            let snapshot = supervisor.snapshot();
            assert_eq!(snapshot.state, SessionState::NeedsHuman, "{failure:?}");
            let github = snapshot.github.unwrap();
            match failure {
                ScriptedGitHubFailure::Preparation => {
                    assert!(!github.merge_already_confirmed);
                    assert_eq!(
                        github.phase,
                        Some(crate::control_api::GitHubDeliveryPhase::PreparingRepository)
                    );
                    assert!(github.preserved_branch.is_some());
                    assert!(github.preserved_worktree.is_some());
                }
                ScriptedGitHubFailure::Review => {
                    assert_eq!(snapshot.tasks[0].github_reviews.len(), 1);
                    assert!(!github.merge_already_confirmed);
                }
                ScriptedGitHubFailure::Check => {
                    assert_eq!(
                        github
                            .current_check
                            .as_ref()
                            .map(|check| check.state.as_str()),
                        Some("success")
                    );
                    assert_eq!(
                        snapshot.tasks[0]
                            .github_check
                            .as_ref()
                            .map(|check| check.state.as_str()),
                        Some("success")
                    );
                    assert!(!github.merge_already_confirmed);
                }
                ScriptedGitHubFailure::Merge => {
                    assert!(github.merge_already_confirmed);
                    assert_eq!(github.merge_sha, Some("f".repeat(40)));
                    assert_eq!(
                        github.phase,
                        Some(crate::control_api::GitHubDeliveryPhase::Finalizing)
                    );
                    assert!(github.finalization.is_none());
                    assert!(github.preserved_branch.is_some());
                    assert!(github.preserved_worktree.is_some());
                }
                ScriptedGitHubFailure::Finalization => {
                    assert!(github.merge_already_confirmed);
                    assert_eq!(
                        github.finalization,
                        Some(crate::control_api::GitHubFinalizationSnapshot {
                            local_worktree_removed: true,
                            local_ref_removed: false,
                            remote_ref_outcome: None,
                        })
                    );
                    assert!(github.preserved_branch.is_some());
                    assert!(github.preserved_worktree.is_none());
                }
            }
            let audit = workflow.audit.lock().unwrap();
            assert_eq!(
                audit
                    .iter()
                    .filter(|entry| entry.as_str() == "merge")
                    .count(),
                usize::from(matches!(
                    failure,
                    ScriptedGitHubFailure::Merge | ScriptedGitHubFailure::Finalization
                )),
                "a confirmed merge must never be retried"
            );
            assert_eq!(
                audit
                    .iter()
                    .filter(|entry| entry.as_str() == "finalize")
                    .count(),
                usize::from(failure == ScriptedGitHubFailure::Finalization)
            );
            assert!(workflow.partial_operations.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn github_merge_wait_keeps_supervisor_responsive_and_fresh_run_never_adopts_it() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) = github_runtime_for_test(&fixture.repository);
        let merge_gate_entered = Arc::new(AtomicBool::new(false));
        let merge_gate_release = Arc::new(AtomicBool::new(false));
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from(["c".repeat(40)])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: Some(Arc::clone(&merge_gate_entered)),
            merge_gate_release: Some(Arc::clone(&merge_gate_release)),
            merge_entered: None,
            merge_release: None,
        });
        runtime.workflow = workflow.clone();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            fixture.sources.profiles.clone().unwrap(),
            Vec::new(),
            runtime,
        )
        .unwrap();
        let scripts = vec![task_script(
            "github-wait",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("github-wait-initial")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("github-wait-lgtm")],
                ),
            ],
            vec![Mutation::None; 2],
        )];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-wait".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: Arc::new(Mutex::new(Audit::default())),
            }),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-initial"),
                vec![fixture.task(
                    "github-wait",
                    &[],
                    crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS,
                )],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        for _ in 0..64 {
            supervisor.poll_once().unwrap();
            if merge_gate_entered.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(merge_gate_entered.load(Ordering::Acquire));
        assert_eq!(supervisor.snapshot().state, SessionState::Running);
        assert!(supervisor.active.is_empty());
        assert!(supervisor.pending_github_effect.is_some());

        let started = std::time::Instant::now();
        supervisor.poll_once().unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        let version = supervisor.snapshot().version;
        supervisor
            .cancel(version, "human cancelled merge wait")
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::Canceled);

        merge_gate_release.store(true, Ordering::Release);
        for _ in 0..64 {
            supervisor.poll_once().unwrap();
            if supervisor.pending_github_effect.is_none() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(supervisor.pending_github_effect.is_none());
        let terminal = supervisor.snapshot();
        supervisor
            .begin_next_run_with_id(
                terminal.version,
                &terminal.run_id,
                "run-github-fresh".into(),
            )
            .unwrap();
        let fresh = supervisor.snapshot();
        assert_eq!(fresh.state, SessionState::AwaitingPlan);
        assert_eq!(fresh.run_id, "run-github-fresh");
        let fresh_github = fresh.github.unwrap();
        assert!(fresh_github.run_binding.is_none());
        assert!(fresh_github.pr_number.is_none());
        assert!(!fresh_github.merge_already_confirmed);
        assert!(
            workflow
                .audit
                .lock()
                .unwrap()
                .contains(&"fresh:run-github-wait:run-github-fresh".to_owned())
        );
    }

    #[test]
    fn cancellation_joins_an_inflight_merge_and_reconciles_success_before_terminalizing() {
        let fixture = Fixture::new();
        let (mut runtime, _inspector) = github_runtime_for_test(&fixture.repository);
        let merge_entered = Arc::new(AtomicBool::new(false));
        let merge_release = Arc::new(AtomicBool::new(false));
        let workflow = Arc::new(ScriptedGitHubWorkflow {
            heads: Mutex::new(VecDeque::from(["c".repeat(40)])),
            audit: Mutex::new(Vec::new()),
            worktree: Mutex::new(None),
            fail_after_confirmation: None,
            partial_operations: Mutex::new(BTreeMap::new()),
            merge_gate_entered: None,
            merge_gate_release: None,
            merge_entered: Some(Arc::clone(&merge_entered)),
            merge_release: Some(Arc::clone(&merge_release)),
        });
        runtime.workflow = workflow.clone();
        let sources = SessionRuntimeSources::capture_with_github(
            fixture.sources.parent_environment.clone(),
            fixture.sources.profiles.clone().unwrap(),
            Vec::new(),
            runtime,
        )
        .unwrap();
        let scripts = vec![task_script(
            "github-merge-race",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("github-merge-race-initial")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("github-merge-race-lgtm")],
                ),
            ],
            vec![Mutation::None; 2],
        )];
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-github-merge-race".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
            Box::new(ScriptedFactory {
                scripts: scripts.into(),
                audit: Arc::new(Mutex::new(Audit::default())),
            }),
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan_with_inspection(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                Some("inspection-initial"),
                vec![fixture.task(
                    "github-merge-race",
                    &[],
                    crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS,
                )],
            )
            .unwrap();
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        for _ in 0..128 {
            supervisor.poll_once().unwrap();
            if merge_entered.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(merge_entered.load(Ordering::Acquire));
        assert!(supervisor.pending_github_effect.is_some());

        let release = Arc::clone(&merge_release);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            release.store(true, Ordering::Release);
        });
        let version = supervisor.snapshot().version;
        supervisor
            .cancel(version, "cancel raced with exact merge")
            .unwrap();
        releaser.join().unwrap();
        assert!(supervisor.pending_github_effect.is_none());
        let terminal = supervisor.snapshot();
        assert_eq!(terminal.state, SessionState::Completed);
        assert!(terminal.github.unwrap().merge_already_confirmed);
        assert!(workflow.audit.lock().unwrap().contains(&"finalize".into()));
    }

    #[test]
    fn role_contract_separates_local_candidate_commits_from_release_authority() {
        let developer = role_instructions(WorkerRole::Developer);
        for required in [
            "execution approval for this standard hcom lane authorizes exactly one signed-off local candidate commit",
            "general instruction that commits require human authorization is satisfied",
            "STATUS: CLARIFICATION_REQUIRED",
            "incompatible with the standard review lane",
            "requires an explicit human resolution",
            "git commit --signoff",
            "Signed-off-by",
            "do not authorize push, install, or release",
        ] {
            assert!(
                developer.contains(required),
                "Developer role contract omitted {required}"
            );
        }

        let reviewer = role_instructions(WorkerRole::Reviewer);
        for required in [
            "exactly one signed-off local Developer candidate commit",
            "VERDICT: REQUEST_CHANGES",
            "REQUIREMENT_AMBIGUITY:",
            "valid `Signed-off-by` trailer matching the committing identity",
            "exact final candidate range already committed",
            "does not call for another post-LGTM commit",
            "must not edit reviewed source, the Git index or refs, the candidate commit",
            "In dual-review mode, two Reviewer turns run concurrently",
            "Every Reviewer turn has an independent six-hour timeout",
            "in single-review mode, only Reviewer1 runs",
            "no extra join deadline, cgroup, CPU, memory, or Cargo concurrency cap",
            "use an isolated target directory",
        ] {
            assert!(
                reviewer.contains(required),
                "Reviewer role contract omitted {required}"
            );
        }

        for required in [
            "human, task, design, or applicable",
            "requires the run to remain uncommitted",
            "standard lane's required candidate commit",
            "require an explicit human resolution",
        ] {
            assert!(
                DEVELOPER_OUTPUT_CONTRACT.contains(required),
                "per-turn Developer output contract omitted {required}"
            );
        }

        let github_developer = GITHUB_DEVELOPER_ROLE_INSTRUCTIONS;
        for required in [
            "exactly one new signed-off child commit",
            "never amend, rebase, squash, force-push, or rewrite",
            "frozen Developer App bot identity",
            "foreground supervisor alone validates and publishes",
            "published byte-for-byte without redaction or secret scanning",
            "60 KiB UTF-8 hard cap",
            "remain uncommitted or unpushed",
            "STATUS: CLARIFICATION_REQUIRED",
        ] {
            assert!(
                github_developer.contains(required),
                "GitHub Developer role contract omitted {required}"
            );
        }
        let github_reviewer = GITHUB_REVIEWER_ROLE_INSTRUCTIONS;
        for required in [
            "exact task base..published head range",
            "neither receives the peer final",
            "contiguous published chain",
            "mapped Reviewer App",
            "published byte-for-byte without redaction or secret scanning",
            "60 KiB UTF-8 hard cap",
            "Do not edit source, index, refs, commits, branch, or HEAD",
        ] {
            assert!(
                github_reviewer.contains(required),
                "GitHub Reviewer role contract omitted {required}"
            );
        }
        for required in [
            "exactly one new",
            "externally published byte-for-byte without",
            "redaction or secret scanning",
            "generated GitHub body capped at 60 KiB",
            "UTF-8. Report changes",
            "Do not push",
        ] {
            assert!(
                GITHUB_DEVELOPER_OUTPUT_CONTRACT.contains(required),
                "GitHub Developer output contract omitted {required}"
            );
        }
    }

    #[derive(Clone)]
    enum Mutation {
        None,
        Commit {
            path: &'static str,
            contents: &'static str,
        },
        Dirty {
            path: &'static str,
            contents: &'static str,
        },
        Stage {
            path: &'static str,
            contents: &'static str,
        },
        Branch {
            name: &'static str,
        },
        DeleteCommit {
            path: &'static str,
        },
        RenameCommit {
            from: &'static str,
            to: &'static str,
        },
        ModeCommit {
            path: &'static str,
            mode: u32,
        },
    }

    struct TaskScript {
        task_key: String,
        turns: Vec<FakeTurnScript>,
        mutations: VecDeque<Mutation>,
        shutdown_failure: bool,
    }

    #[derive(Default)]
    struct Audit {
        opens: Vec<String>,
        sessions: Vec<(String, WorkerRole, u64)>,
        lane_sessions: Vec<(String, WorkerLane, u64)>,
        role_instructions: Vec<(WorkerLane, String)>,
        turns: Vec<(String, WorkerRole, RuntimeTurnPurpose, u64)>,
        lane_events: Vec<ScriptedLaneEvent>,
        prompts: Vec<(WorkerRole, RuntimeTurnPurpose, String)>,
        shutdowns: Vec<String>,
        environments: Vec<Vec<(OsString, OsString)>>,
        profiles: Vec<(String, WorkerRole, RuntimeProfile)>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ScriptedLaneEvent {
        TurnStarted(WorkerLane),
        TurnPolled(WorkerLane),
    }

    struct ScriptedFactory {
        scripts: VecDeque<TaskScript>,
        audit: Arc<Mutex<Audit>>,
    }

    impl RuntimeFactory for ScriptedFactory {
        fn contract(&self, _profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity {
            RuntimeContractIdentity::codex_exec()
        }

        fn open(
            &mut self,
            request: RuntimeOpenRequest,
        ) -> Result<Box<dyn LaneTaskWorkerRuntime>, RuntimeError> {
            let script = self.scripts.pop_front().ok_or_else(|| {
                RuntimeError::invalid_transition("scripted runtime factory is exhausted")
            })?;
            if script.task_key != request.task_key {
                return Err(RuntimeError::invalid_identity(
                    "scripted runtime task key mismatch",
                ));
            }
            {
                let mut audit = self.audit.lock().unwrap();
                audit.opens.push(request.task_key.clone());
                audit.environments.push(request.environment.clone());
            }
            Ok(Box::new(ScriptedRuntime {
                task_key: request.task_key,
                repository: request.repository_root,
                inner: FakeTaskWorkerRuntime::new(script.turns),
                sessions: BTreeMap::new(),
                turns: BTreeMap::new(),
                mutations: script.mutations,
                shutdown_failure: script.shutdown_failure,
                audit: Arc::clone(&self.audit),
            }))
        }
    }

    struct ScriptedRuntime {
        task_key: String,
        repository: PathBuf,
        inner: FakeTaskWorkerRuntime,
        sessions: BTreeMap<RuntimeSessionKey, WorkerLane>,
        turns: BTreeMap<RuntimeTurnKey, WorkerLane>,
        mutations: VecDeque<Mutation>,
        shutdown_failure: bool,
        audit: Arc<Mutex<Audit>>,
    }

    impl LaneTaskWorkerRuntime for ScriptedRuntime {
        fn contract(&self) -> &RuntimeContractIdentity {
            self.inner.contract()
        }

        fn open_session(
            &mut self,
            lane: WorkerLane,
            spec: RoleSessionSpec,
        ) -> Result<RuntimeSessionKey, RuntimeError> {
            if lane.role() != spec.role {
                return Err(RuntimeError::invalid_identity(
                    "scripted session lane differs from its role",
                ));
            }
            let role = spec.role;
            let profile = spec.profile.clone();
            let role_instructions = spec.developer_instructions.clone();
            let session = self.inner.open_session(spec)?;
            self.sessions.insert(session, lane);
            let mut audit = self.audit.lock().unwrap();
            audit
                .sessions
                .push((self.task_key.clone(), role, session.counter()));
            audit
                .lane_sessions
                .push((self.task_key.clone(), lane, session.counter()));
            audit.role_instructions.push((lane, role_instructions));
            audit.profiles.push((self.task_key.clone(), role, profile));
            Ok(session)
        }

        fn start_turn(
            &mut self,
            lane: WorkerLane,
            session: RuntimeSessionKey,
            spec: RuntimeTurnSpec,
        ) -> Result<RuntimeTurnKey, RuntimeError> {
            if self.sessions.get(&session) != Some(&lane) || lane.role() != spec.role {
                return Err(RuntimeError::invalid_identity(
                    "scripted turn differs from its lane-scoped session",
                ));
            }
            let role = spec.role;
            let purpose = spec.purpose;
            let prompt = spec.prompt.clone();
            let turn = self.inner.start_turn(session, spec)?;
            self.turns.insert(turn, lane);
            let mut audit = self.audit.lock().unwrap();
            audit
                .turns
                .push((self.task_key.clone(), role, purpose, session.counter()));
            audit.lane_events.push(ScriptedLaneEvent::TurnStarted(lane));
            audit.prompts.push((role, purpose, prompt));
            drop(audit);
            Ok(turn)
        }

        fn poll_turn(
            &mut self,
            lane: WorkerLane,
            turn: RuntimeTurnKey,
        ) -> Result<crate::worker::role_router::LaneRuntimeTurnPoll, RuntimeError> {
            if self.turns.get(&turn) != Some(&lane) {
                return Err(RuntimeError::invalid_identity(
                    "scripted poll differs from its lane-scoped turn",
                ));
            }
            let poll = self.inner.poll_turn(turn)?;
            self.audit
                .lock()
                .unwrap()
                .lane_events
                .push(ScriptedLaneEvent::TurnPolled(lane));
            if poll.is_terminal() {
                self.turns.remove(&turn);
                let mutation = self.mutations.pop_front().ok_or_else(|| {
                    RuntimeError::internal("scripted runtime mutation inventory disappeared")
                })?;
                apply_mutation(&self.repository, mutation)
                    .map_err(|_| RuntimeError::internal("scripted Git mutation failed"))?;
            }
            Ok(crate::worker::role_router::LaneRuntimeTurnPoll { lane, turn, poll })
        }

        fn cancel_turn(
            &mut self,
            lane: WorkerLane,
            turn: RuntimeTurnKey,
        ) -> Result<(), RuntimeError> {
            if self.turns.get(&turn) != Some(&lane) {
                return Err(RuntimeError::invalid_identity(
                    "scripted cancel differs from its lane-scoped turn",
                ));
            }
            self.inner.cancel_turn(turn)?;
            self.turns.remove(&turn);
            Ok(())
        }

        fn cancel_all(&mut self) -> Result<(), RuntimeError> {
            let turns = self
                .turns
                .iter()
                .map(|(turn, lane)| (*lane, *turn))
                .collect::<Vec<_>>();
            let mut first_error = None;
            for (lane, turn) in turns {
                if let Err(error) = self.cancel_turn(lane, turn)
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), RuntimeError> {
            self.inner.shutdown()?;
            self.audit
                .lock()
                .unwrap()
                .shutdowns
                .push(self.task_key.clone());
            if self.shutdown_failure {
                return Err(RuntimeError::internal(
                    "scripted task-runtime cleanup failed",
                ));
            }
            Ok(())
        }
    }

    struct FailingFactory;

    impl RuntimeFactory for FailingFactory {
        fn contract(&self, _profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity {
            RuntimeContractIdentity::codex_exec()
        }

        fn open(
            &mut self,
            _request: RuntimeOpenRequest,
        ) -> Result<Box<dyn LaneTaskWorkerRuntime>, RuntimeError> {
            Err(RuntimeError::internal("runtime-factory-secret-sentinel"))
        }
    }

    struct Fixture {
        _temp: TempDir,
        run_root: PathBuf,
        project_root: PathBuf,
        repository: PathBuf,
        sources: SessionRuntimeSources,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temp.path()).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let run_root = root.join("run");
            let project_root = root.join("project");
            let repository = root.join("repository");
            let toolchain = root.join("toolchain");
            for directory in [&run_root, &project_root, &repository, &toolchain] {
                fs::create_dir(directory).unwrap();
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
            }
            git(&repository, &["init", "-b", "master"]);
            fs::create_dir(repository.join("src")).unwrap();
            fs::write(repository.join("src/seed.txt"), "seed\n").unwrap();
            fs::write(repository.join(".gitignore"), "target/\n").unwrap();
            git(&repository, &["add", "--", "src/seed.txt", ".gitignore"]);
            git_commit(&repository, "Initial fixture");
            let repository = fs::canonicalize(repository).unwrap();

            let mut sources = SessionRuntimeSources::fake(&toolchain);
            sources.profiles = Some(pure_codex_profiles(ArchitectAdapter::Codex));
            sources.parent_environment = ParentEnvironment::from_os(vec![
                (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
                (
                    OsString::from("UNKNOWN_PARENT_VALUE"),
                    OsString::from("unknown-value"),
                ),
                (
                    OsString::from("SERVICE_ACCESS_TOKEN"),
                    OsString::from("environment-secret-sentinel"),
                ),
                (OsString::from("EMPTY_PARENT_VALUE"), OsString::new()),
                (OsString::from("CASE_PAIR"), OsString::from("upper")),
                (OsString::from("case_pair"), OsString::from("lower")),
                (
                    OsString::from("HTTP_PROXY"),
                    OsString::from("http://proxy.example"),
                ),
                (
                    OsString::from("https_proxy"),
                    OsString::from("http://lower-proxy.example"),
                ),
                (
                    OsString::from_vec(b"RAW_\xff_NAME".to_vec()),
                    OsString::from_vec(b"value-\xfe".to_vec()),
                ),
                (
                    OsString::from("HCOM_DIR"),
                    OsString::from("/must/not/reach/task"),
                ),
                (
                    OsString::from("HCOM_WORKER_ROLE"),
                    OsString::from("parent-worker-role"),
                ),
                (
                    OsString::from("HCOM_RUN_ID"),
                    OsString::from("parent-run-id"),
                ),
                (
                    OsString::from("HCOM_TASK_ID"),
                    OsString::from("parent-task-id"),
                ),
                (OsString::from("HOME"), OsString::from("/native/home")),
                (
                    OsString::from("CODEX_HOME"),
                    OsString::from("/native/codex-home"),
                ),
                (OsString::from("TMPDIR"), OsString::from("/native/tmp")),
                (
                    OsString::from("XDG_RUNTIME_DIR"),
                    OsString::from("/native/xdg-runtime"),
                ),
                (
                    OsString::from("XDG_CACHE_HOME"),
                    OsString::from("/native/xdg-cache"),
                ),
            ])
            .unwrap();
            Self {
                _temp: temp,
                run_root,
                project_root,
                repository,
                sources,
            }
        }

        fn task(&self, key: &str, _legacy_scope: &[&str], max_rounds: u8) -> TaskDraft {
            let task_document_path = self.project_root.join(format!("task-{key}.md"));
            let design_document_path = self.project_root.join("design.md");
            fs::write(
                &task_document_path,
                format!("TASK-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT: {key}\n"),
            )
            .unwrap();
            fs::write(
                &design_document_path,
                "DESIGN-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT\n",
            )
            .unwrap();
            TaskDraft {
                task_key: key.into(),
                title: format!("Task {key}"),
                repository_root: self.repository.to_string_lossy().into_owned(),
                task_document_path: task_document_path.to_string_lossy().into_owned(),
                design_document_paths: vec![design_document_path.to_string_lossy().into_owned()],
                task_selector: key.into(),
                max_review_rounds: max_rounds
                    .max(crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS),
                max_clarification_rounds: 2,
            }
        }

        fn supervisor(
            &self,
            scripts: Vec<TaskScript>,
            audit: Arc<Mutex<Audit>>,
        ) -> TaskLaneSupervisor {
            TaskLaneSupervisor::open_with_factory(
                "run-driver-test".into(),
                self.project_root.clone(),
                self.run_root.clone(),
                self.sources.clone(),
                Box::new(ScriptedFactory {
                    scripts: scripts.into(),
                    audit,
                }),
            )
            .unwrap()
        }

        fn single_reviewer_supervisor(
            &self,
            scripts: Vec<TaskScript>,
            audit: Arc<Mutex<Audit>>,
        ) -> TaskLaneSupervisor {
            let mut sources = self.sources.clone();
            let mut profiles = pure_codex_profiles(ArchitectAdapter::Codex);
            profiles.retain_reviewer1().unwrap();
            sources.set_profiles_for_test(profiles);
            TaskLaneSupervisor::open_with_factory(
                "run-driver-test".into(),
                self.project_root.clone(),
                self.run_root.clone(),
                sources,
                Box::new(ScriptedFactory {
                    scripts: scripts.into(),
                    audit,
                }),
            )
            .unwrap()
        }
    }

    fn create_deep_directory(mut path: PathBuf, minimum_bytes: usize) -> PathBuf {
        while path.as_os_str().as_bytes().len() < minimum_bytes {
            let remaining = minimum_bytes - path.as_os_str().as_bytes().len();
            let component_bytes = remaining.saturating_sub(1).clamp(1, 200);
            path.push("p".repeat(component_bytes));
            fs::create_dir(&path).unwrap();
        }
        fs::canonicalize(path).unwrap()
    }

    /// Support for the opt-in real-Codex acceptance tests: a disposable
    /// project + repository driven by the production runtime factory.
    pub(super) mod real_support {
        use super::super::*;
        use crate::worker::claude_test::ClaudeModelTestGate;
        use crate::worker::environment::ParentEnvironment;
        use crate::worker::profile::{
            ArchitectAdapter, ClaudeInvocationProfile, CodexInvocationProfile,
            DeveloperInvocationProfile, ReviewerInvocationProfile, SessionInvocationProfiles,
        };
        use crate::worker::runtime::RuntimeProvider;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        /// Cheap model for acceptance runs; production defaults stay untouched.
        const CODEX_TEST_MODEL: &str = "gpt-5.3-codex-spark";
        const CODEX_TEST_EFFORT: &str = "medium";
        const REAL_HCOM_BINARY_ENV: &str = "HCOM_REAL_E2E_HCOM_BIN";

        pub(crate) struct RealFixture {
            pub(crate) _temp: Option<tempfile::TempDir>,
            pub(crate) project_root: PathBuf,
            pub(crate) repository: PathBuf,
            run_root: PathBuf,
            sources: SessionRuntimeSources,
            developer_adapter: String,
            reviewer_adapters: Vec<ReviewerAdapterBinding>,
        }

        impl RealFixture {
            pub(crate) fn new(label: &str) -> Self {
                Self::new_with_workers(
                    label,
                    ArchitectAdapter::Codex,
                    RuntimeProvider::CodexExec,
                    RuntimeProvider::CodexExec,
                    RuntimeProvider::CodexExec,
                )
            }

            pub(crate) fn new_with_workers(
                label: &str,
                architect: ArchitectAdapter,
                developer: RuntimeProvider,
                reviewer1: RuntimeProvider,
                reviewer2: RuntimeProvider,
            ) -> Self {
                let claude_gate = [developer, reviewer1, reviewer2]
                    .contains(&RuntimeProvider::ClaudeExec)
                    .then(|| ClaudeModelTestGate::capture().unwrap());
                let temp = tempfile::Builder::new()
                    .prefix(&format!("hcom-real-exec-{label}."))
                    .tempdir()
                    .unwrap();
                let root = fs::canonicalize(temp.path()).unwrap();
                let temp = if std::env::var_os("HCOM_REAL_E2E_KEEP").is_some() {
                    eprintln!("preserving real E2E fixture at {}", root.display());
                    let _ = temp.keep();
                    None
                } else {
                    Some(temp)
                };
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
                let run_root = root.join("run");
                let project_root = root.join("project");
                let repository = project_root.join("repository");
                for directory in [&run_root, &project_root, &repository] {
                    fs::create_dir_all(directory).unwrap();
                    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
                }
                super::git(&repository, &["init", "-b", "master"]);
                fs::write(repository.join("README.md"), "# fixture\n").unwrap();
                super::git(&repository, &["add", "--", "README.md"]);
                super::git_commit(&repository, "Initial acceptance fixture");
                let repository = fs::canonicalize(repository).unwrap();

                let home = std::env::var("HOME").expect("HOME");
                let mut profiles = SessionInvocationProfiles::for_task_lane(architect).unwrap();
                let cheap_codex = CodexInvocationProfile {
                    model: CODEX_TEST_MODEL.into(),
                    reasoning_effort: CODEX_TEST_EFFORT.into(),
                    sandbox: crate::worker::profile::CodexSandbox::DangerFullAccess,
                    approval_policy: crate::worker::profile::CodexApprovalPolicy::Never,
                };
                let cheap_claude = claude_gate
                    .as_ref()
                    .map(|gate| gate.profile().clone())
                    .unwrap_or_else(|| ClaudeInvocationProfile {
                        model: "haiku".into(),
                        effort: "medium".into(),
                        dangerously_skip_permissions: true,
                    });
                profiles.developer = match developer {
                    RuntimeProvider::CodexExec => DeveloperInvocationProfile::Codex {
                        profile: cheap_codex.clone(),
                    },
                    RuntimeProvider::ClaudeExec => DeveloperInvocationProfile::Claude {
                        profile: cheap_claude.clone(),
                    },
                };
                *profiles.reviewer1_mut() = match reviewer1 {
                    RuntimeProvider::CodexExec => ReviewerInvocationProfile::Codex {
                        profile: cheap_codex.clone(),
                    },
                    RuntimeProvider::ClaudeExec => ReviewerInvocationProfile::Claude {
                        profile: cheap_claude.clone(),
                    },
                };
                *profiles.reviewer2_mut() = match reviewer2 {
                    RuntimeProvider::CodexExec => ReviewerInvocationProfile::Codex {
                        profile: cheap_codex,
                    },
                    RuntimeProvider::ClaudeExec => ReviewerInvocationProfile::Claude {
                        profile: cheap_claude,
                    },
                };

                let mut sources = SessionRuntimeSources::fake(Path::new(&home));
                sources.set_profiles_for_test(profiles);
                // Complete parent inheritance, exactly like production.
                sources.parent_environment = claude_gate
                    .as_ref()
                    .map(|gate| gate.parent_environment().clone())
                    .unwrap_or_else(|| ParentEnvironment::capture_current().unwrap());
                if claude_gate.is_some() {
                    let configured = std::env::var_os(REAL_HCOM_BINARY_ENV).unwrap_or_else(|| {
                        panic!(
                            "{REAL_HCOM_BINARY_ENV} must name the freshly built hcom binary for real Claude task-lane tests"
                        )
                    });
                    let configured = PathBuf::from(configured);
                    let guardian = fs::canonicalize(&configured).unwrap_or_else(|error| {
                        panic!(
                            "{REAL_HCOM_BINARY_ENV} does not resolve to the hcom binary: {error}"
                        )
                    });
                    let metadata = fs::metadata(&guardian).unwrap();
                    assert!(
                        guardian.is_absolute()
                            && metadata.is_file()
                            && metadata.permissions().mode() & 0o111 != 0,
                        "{REAL_HCOM_BINARY_ENV} must resolve to an executable regular file"
                    );
                    sources.set_guardian_executable_for_test(guardian);
                }

                Self {
                    _temp: temp,
                    project_root,
                    repository,
                    run_root,
                    sources,
                    developer_adapter: developer.as_str().into(),
                    reviewer_adapters: super::reviewer_adapter_bindings(
                        reviewer1.as_str(),
                        reviewer2.as_str(),
                    ),
                }
            }

            pub(crate) fn supervisor(&self) -> TaskLaneSupervisor {
                TaskLaneSupervisor::open(
                    "run-real-exec".into(),
                    self.project_root.clone(),
                    self.run_root.clone(),
                    self.sources.clone(),
                )
                .unwrap()
            }

            pub(crate) fn task(
                &self,
                task_key: &str,
                title: &str,
                task_body: &str,
                max_review_rounds: u8,
            ) -> TaskDraft {
                let task_document_path = self.project_root.join(format!("task-{task_key}.md"));
                fs::write(
                    &task_document_path,
                    format!("# {title}\n\nTask selector: {task_key}\n\n{task_body}\n"),
                )
                .unwrap();
                TaskDraft {
                    task_key: task_key.into(),
                    title: title.into(),
                    repository_root: self.repository.to_string_lossy().into_owned(),
                    task_document_path: task_document_path.to_string_lossy().into_owned(),
                    design_document_paths: Vec::new(),
                    task_selector: task_key.into(),
                    max_review_rounds: max_review_rounds
                        .max(crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS),
                    max_clarification_rounds: 2,
                }
            }

            pub(crate) fn commit_fixture_instruction(&self, name: &str, contents: &str) {
                fs::write(self.repository.join(name), contents).unwrap();
                super::git(&self.repository, &["add", "--", name]);
                super::git_commit(&self.repository, "Add controlled E2E instruction");
            }

            /// Worker processes this fixture's own run leaves behind. A real
            /// run must not outlive its supervisor: an orphaned worker keeps
            /// burning tokens and pollutes whatever runs next, which is
            /// exactly the failure mode stale processes cause.
            pub(crate) fn stray_worker_pids(&self) -> Vec<u32> {
                let marker = self.project_root.to_string_lossy().into_owned();
                let mut strays = Vec::new();
                let Ok(entries) = fs::read_dir("/proc") else {
                    return strays;
                };
                for entry in entries.flatten() {
                    let Some(pid) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.parse::<u32>().ok())
                    else {
                        continue;
                    };
                    let Ok(cmdline) = fs::read(format!("/proc/{pid}/cmdline")) else {
                        continue;
                    };
                    let cmdline = String::from_utf8_lossy(&cmdline).replace('\0', " ");
                    if cmdline.contains(&marker) || {
                        fs::read_link(format!("/proc/{pid}/cwd"))
                            .map(|cwd| cwd.starts_with(&self.project_root))
                            .unwrap_or(false)
                    } {
                        strays.push(pid);
                    }
                }
                strays
            }

            /// Exact native Codex worker processes owned by this fixture.
            ///
            /// The `--output-last-message` target is allocated below this
            /// fixture's private run root, so this cannot select an unrelated
            /// interactive Codex or another real-E2E fixture.
            #[cfg(target_os = "linux")]
            pub(crate) fn live_codex_worker_pids(&self) -> Vec<u32> {
                let mut workers = Vec::new();
                let Ok(entries) = fs::read_dir("/proc") else {
                    return workers;
                };
                for entry in entries.flatten() {
                    let Some(pid) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.parse::<u32>().ok())
                    else {
                        continue;
                    };
                    let Ok(cmdline) = fs::read(format!("/proc/{pid}/cmdline")) else {
                        continue;
                    };
                    let arguments: Vec<_> = cmdline
                        .split(|byte| *byte == 0)
                        .filter(|argument| !argument.is_empty())
                        .map(|argument| String::from_utf8_lossy(argument).into_owned())
                        .collect();
                    let is_codex_exec = arguments
                        .first()
                        .and_then(|argument| Path::new(argument).file_name())
                        .and_then(|name| name.to_str())
                        == Some("codex")
                        && arguments.get(1).map(String::as_str) == Some("exec");
                    let owns_final_target = arguments.windows(2).any(|pair| {
                        pair[0] == "--output-last-message"
                            && Path::new(&pair[1]).starts_with(&self.run_root)
                    });
                    if is_codex_exec && owns_final_target {
                        workers.push(pid);
                    }
                }
                workers.sort_unstable();
                workers
            }

            pub(crate) fn start(&self, supervisor: &mut TaskLaneSupervisor, tasks: Vec<TaskDraft>) {
                let (plan_version, plan_hash) = supervisor
                    .replace_plan(0, &self.developer_adapter, &self.reviewer_adapters, tasks)
                    .unwrap();
                supervisor
                    .approve_and_start(1, plan_version, &plan_hash, true)
                    .unwrap();
            }

            pub(crate) fn drive(
                &self,
                supervisor: &mut TaskLaneSupervisor,
            ) -> SessionStatusSnapshot {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20 * 60);
                loop {
                    supervisor.poll_once().unwrap();
                    let snapshot = supervisor.snapshot();
                    if snapshot.state.is_terminal() {
                        return snapshot;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "real acceptance run did not finish within 20 minutes"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
            }

            pub(crate) fn run(
                &self,
                supervisor: &mut TaskLaneSupervisor,
                tasks: Vec<TaskDraft>,
            ) -> SessionStatusSnapshot {
                self.start(supervisor, tasks);
                self.drive(supervisor)
            }

            /// The native thread ids this run recorded, per role, in turn
            /// order — read back out of the sealed stdout evidence, so the
            /// assertion sees what Codex actually did rather than what the
            /// runtime believes.
            pub(crate) fn native_session_ids(&self, task_key: &str, role: &str) -> Vec<String> {
                let role_dir = self
                    .project_root
                    .join("hcom-tasks/run-real-exec")
                    .join(task_key)
                    .join(role);
                let mut found: Vec<(PathBuf, String)> = Vec::new();
                for path in walk_files(&role_dir) {
                    if path.file_name().and_then(|n| n.to_str()) != Some("native.stdout.partial") {
                        continue;
                    }
                    let Ok(text) = fs::read_to_string(&path) else {
                        continue;
                    };
                    let Some(first) = text.lines().next() else {
                        continue;
                    };
                    let Some(rest) = ["\"thread_id\":\"", "\"session_id\":\""]
                        .into_iter()
                        .find_map(|field| first.split(field).nth(1))
                    else {
                        continue;
                    };
                    if let Some(id) = rest.split('"').next() {
                        found.push((path, id.to_string()));
                    }
                }
                found.sort_by(|a, b| a.0.cmp(&b.0));
                found.into_iter().map(|(_, id)| id).collect()
            }

            pub(crate) fn thread_ids(&self, task_key: &str, role: &str) -> Vec<String> {
                self.native_session_ids(task_key, role)
            }

            /// Every role of a task left durable evidence in `hcom-tasks/`.
            pub(crate) fn assert_artifacts(&self, task_key: &str, roles: &[&str]) {
                let task_dir = self
                    .project_root
                    .join("hcom-tasks/run-real-exec")
                    .join(task_key);
                for role in roles {
                    let role_dir = task_dir.join(role);
                    assert!(role_dir.is_dir(), "missing artifacts for {task_key}/{role}");
                    let final_files: Vec<_> = walk_files(&role_dir)
                        .into_iter()
                        .filter(|path| {
                            path.file_name().and_then(|name| name.to_str())
                                == Some("native-final.partial")
                        })
                        .collect();
                    assert!(
                        !final_files.is_empty(),
                        "no sealed final message for {task_key}/{role}"
                    );
                }
            }

            /// Exact native Claude workers owned by this fixture.
            ///
            /// The disposable project cwd plus the fixed headless role name
            /// avoids selecting an existing interactive/user Claude session.
            #[cfg(target_os = "linux")]
            pub(crate) fn live_claude_worker_pids(&self) -> Vec<u32> {
                let mut workers = Vec::new();
                let Ok(entries) = fs::read_dir("/proc") else {
                    return workers;
                };
                for entry in entries.flatten() {
                    let Some(pid) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.parse::<u32>().ok())
                    else {
                        continue;
                    };
                    if fs::read_link(format!("/proc/{pid}/cwd")).ok().as_deref()
                        != Some(self.project_root.as_path())
                    {
                        continue;
                    }
                    let Ok(cmdline) = fs::read(format!("/proc/{pid}/cmdline")) else {
                        continue;
                    };
                    let arguments: Vec<_> = cmdline
                        .split(|byte| *byte == 0)
                        .filter(|argument| !argument.is_empty())
                        .map(|argument| String::from_utf8_lossy(argument).into_owned())
                        .collect();
                    let is_claude_print = arguments
                        .first()
                        .and_then(|argument| Path::new(argument).file_name())
                        .and_then(|name| name.to_str())
                        == Some("claude")
                        && arguments.iter().any(|argument| argument == "-p")
                        && arguments.windows(2).any(|pair| {
                            pair[0] == "--name"
                                && matches!(
                                    pair[1].as_str(),
                                    "hcom-task-developer" | "hcom-task-reviewer"
                                )
                        });
                    if is_claude_print {
                        workers.push(pid);
                    }
                }
                workers.sort_unstable();
                workers
            }
        }

        fn walk_files(root: &Path) -> Vec<PathBuf> {
            let mut out = Vec::new();
            let mut stack = vec![root.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(entries) = fs::read_dir(&dir) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else {
                        out.push(path);
                    }
                }
            }
            out
        }
    }

    fn task_script(
        task_key: &str,
        turns: Vec<FakeTurnScript>,
        mutations: Vec<Mutation>,
    ) -> TaskScript {
        assert_eq!(turns.len(), mutations.len());
        let mut dual_turns = Vec::with_capacity(turns.len().saturating_mul(2));
        let mut dual_mutations = VecDeque::with_capacity(mutations.len().saturating_mul(2));
        for (turn, mutation) in turns.into_iter().zip(mutations) {
            let reviewer2_turn =
                (turn.role == WorkerRole::Reviewer).then(|| reviewer2_script(turn.clone()));
            dual_turns.push(turn);
            dual_mutations.push_back(mutation);
            if let Some(reviewer2_turn) = reviewer2_turn {
                dual_turns.push(reviewer2_turn);
                dual_mutations.push_back(Mutation::None);
            }
        }
        TaskScript {
            task_key: task_key.into(),
            turns: dual_turns,
            mutations: dual_mutations,
            shutdown_failure: false,
        }
    }

    fn single_reviewer_task_script(
        task_key: &str,
        turns: Vec<FakeTurnScript>,
        mutations: Vec<Mutation>,
    ) -> TaskScript {
        assert_eq!(turns.len(), mutations.len());
        TaskScript {
            task_key: task_key.into(),
            turns,
            mutations: mutations.into(),
            shutdown_failure: false,
        }
    }

    fn exhausted_task_script(
        task_key: &str,
        path: &'static str,
        contents: &'static str,
        finding: &str,
    ) -> TaskScript {
        let mut turns = vec![FakeTurnScript::new(
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
            [ready("implemented")],
        )];
        let mut mutations = vec![Mutation::Commit { path, contents }];
        for generation in 1..=crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS {
            turns.push(FakeTurnScript::new(
                WorkerRole::Reviewer,
                if generation == 1 {
                    RuntimeTurnPurpose::InitialReview
                } else {
                    RuntimeTurnPurpose::ReviewerRereview
                },
                [request_changes(&format!("{finding}-{generation}"))],
            ));
            mutations.push(Mutation::None);
            if generation < crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS {
                turns.push(FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [ready(&format!("correction-{generation}"))],
                ));
                mutations.push(Mutation::None);
            }
        }
        task_script(task_key, turns, mutations)
    }

    fn reviewer2_script(mut script: FakeTurnScript) -> FakeTurnScript {
        for poll in &mut script.polls {
            if let RuntimeTurnPoll::Completed {
                final_message_path, ..
            } = poll
            {
                *final_message_path = reviewer2_final_path(final_message_path);
            }
        }
        script
    }

    fn reviewer2_final_path(path: &Path) -> PathBuf {
        let parent = path.parent().expect("scripted final path has a parent");
        let file_name = path
            .file_name()
            .expect("scripted final path has a file name");
        parent.join("reviewer2").join(file_name)
    }

    fn task_script_with_shutdown_failure(
        task_key: &str,
        turns: Vec<FakeTurnScript>,
        mutations: Vec<Mutation>,
    ) -> TaskScript {
        let mut script = task_script(task_key, turns, mutations);
        script.shutdown_failure = true;
        script
    }

    fn message_path(role: WorkerRole, seed: &str) -> PathBuf {
        let role = match role {
            WorkerRole::Developer => "developer",
            WorkerRole::Reviewer => "reviewer",
        };
        PathBuf::from(format!(
            "/artifacts/{role}/{}/native-final.partial",
            sha256_hex(seed.as_bytes())
        ))
    }

    fn completed(outcome: RuntimeOutcome, seed: &str) -> RuntimeTurnPoll {
        let final_message_path = message_path(outcome.role(), seed);
        RuntimeTurnPoll::Completed {
            outcome,
            final_message_path,
            telemetry: RuntimeTelemetry::default(),
        }
    }

    fn ready(message_seed: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
            }),
            message_seed,
        )
    }

    fn clarification_required(message_seed: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::ClarificationRequired,
            }),
            message_seed,
        )
    }

    fn blocked(message_seed: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Blocked,
            }),
            message_seed,
        )
    }

    fn lgtm(message_seed: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::Lgtm,
                preceding_final_message_paths: Vec::new(),
            }),
            message_seed,
        )
    }

    fn request_changes(message: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                preceding_final_message_paths: Vec::new(),
            }),
            message,
        )
    }

    fn request_changes_after_clarification(message: &str) -> RuntimeTurnPoll {
        completed(
            RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                verdict: ReviewerVerdict::RequestChanges,
                preceding_final_message_paths: vec![message_path(
                    WorkerRole::Reviewer,
                    &format!("{message}-original"),
                )],
            }),
            message,
        )
    }

    fn failed_retryable() -> RuntimeTurnPoll {
        RuntimeTurnPoll::Failed {
            failure: SanitizedRuntimeFailure::new(
                RuntimeFailureClass::Contract,
                "developer outcome was missing",
                true,
            )
            .unwrap(),
            telemetry: RuntimeTelemetry::default(),
        }
    }

    fn failed(class: RuntimeFailureClass, detail: &str) -> RuntimeTurnPoll {
        RuntimeTurnPoll::Failed {
            failure: SanitizedRuntimeFailure::new(class, detail, false).unwrap(),
            telemetry: RuntimeTelemetry::default(),
        }
    }

    fn start(supervisor: &mut TaskLaneSupervisor, tasks: Vec<TaskDraft>) {
        let developer_adapter = supervisor.developer_adapter.clone();
        let reviewer_adapters = supervisor.reviewer_adapters.clone();
        let (plan_version, plan_hash) = supervisor
            .replace_plan(0, &developer_adapter, &reviewer_adapters, tasks)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::Running);
    }

    fn drive_terminal(supervisor: &mut TaskLaneSupervisor) -> SessionStatusSnapshot {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            supervisor.poll_once().unwrap();
            let snapshot = supervisor.snapshot();
            if snapshot.state.is_terminal() {
                return snapshot;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "scripted exec worker supervisor did not become terminal before the bounded \
                 deadline; current_state={:?} version={} current_task_ordinal={:?} \
                 active_lanes={:?} pending_github_effect={}",
                snapshot.state,
                snapshot.version,
                snapshot.current_task_ordinal,
                supervisor.active.keys().collect::<Vec<_>>(),
                supervisor.pending_github_effect.is_some(),
            );
            std::thread::yield_now();
        }
    }

    fn apply_mutation(repository: &Path, mutation: Mutation) -> Result<()> {
        match mutation {
            Mutation::None => Ok(()),
            Mutation::Commit { path, contents } => {
                let target = repository.join(path);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&target, contents)?;
                git(repository, &["add", "--", path]);
                git_commit(repository, &format!("Implement {path}"));
                Ok(())
            }
            Mutation::Dirty { path, contents } => {
                let target = repository.join(path);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(target, contents)?;
                Ok(())
            }
            Mutation::Stage { path, contents } => {
                let target = repository.join(path);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(target, contents)?;
                git(repository, &["add", "--", path]);
                Ok(())
            }
            Mutation::Branch { name } => {
                git(repository, &["checkout", "-b", name]);
                Ok(())
            }
            Mutation::DeleteCommit { path } => {
                fs::remove_file(repository.join(path))?;
                git(repository, &["add", "-A", "--", path]);
                git_commit(repository, &format!("Delete {path}"));
                Ok(())
            }
            Mutation::RenameCommit { from, to } => {
                if let Some(parent) = repository.join(to).parent() {
                    fs::create_dir_all(parent)?;
                }
                git(repository, &["mv", "--", from, to]);
                git_commit(repository, &format!("Rename {from}"));
                Ok(())
            }
            Mutation::ModeCommit { path, mode } => {
                fs::set_permissions(repository.join(path), fs::Permissions::from_mode(mode))?;
                git(repository, &["add", "--", path]);
                git_commit(repository, &format!("Change mode for {path}"));
                Ok(())
            }
        }
    }

    fn git(repository: &Path, arguments: &[&str]) {
        let output = Command::new("/usr/bin/git")
            .args(arguments)
            .current_dir(repository)
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
            arguments,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_commit(repository: &Path, subject: &str) {
        let output = Command::new("/usr/bin/git")
            .args([
                "-c",
                "user.name=exec worker Driver Fixture",
                "-c",
                "user.email=exec-driver@example.invalid",
                "commit",
                "-m",
                subject,
            ])
            .current_dir(repository)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn task_runtime_creates_private_transport_directories_per_released_lane() {
        let fixture = Fixture::new();
        let lanes = [
            WorkerLane::Developer,
            WorkerLane::Reviewer(ReviewerId::Reviewer1),
            WorkerLane::Reviewer(ReviewerId::Reviewer2),
        ];
        let (root, paths) =
            TaskRuntimePaths::create(&fixture.run_root, 0, "config", &fixture.repository, lanes)
                .unwrap();
        let mut children = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        children.sort();
        assert_eq!(children, [OsString::from("run")]);
        for lane in lanes {
            assert!(paths.runtime_for(lane).unwrap().is_dir());
        }
    }

    #[test]
    fn driver_failure_details_remove_every_control_and_remain_utf8_bounded() {
        assert_eq!(
            bounded_single_line("prefix\tmiddle\nsuffix\r\u{0000}end"),
            "prefix middle suffix  end"
        );
        let bounded = bounded_single_line(&format!("{}界", "x".repeat(1023)));
        assert_eq!(bounded.len(), 1023);
        assert!(bounded.is_char_boundary(bounded.len()));
        assert!(!bounded.chars().any(char::is_control));
    }

    #[test]
    fn one_task_driver_commits_reviews_and_closes_one_task_runtime() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "one",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(&mut supervisor, vec![fixture.task("one", &["src"], 3)]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, crate::control_api::TaskState::Lgtm);
        assert_eq!(snapshot.tasks[0].review_round, 1);
        assert!(snapshot.tasks[0].developer_session_bound);
        assert!(
            snapshot.tasks[0]
                .reviewers
                .iter()
                .all(|reviewer| reviewer.session_bound)
        );
        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["one"]);
        assert_eq!(audit.shutdowns, ["one"]);
        assert_eq!(
            audit
                .sessions
                .iter()
                .map(|(_, role, key)| (*role, *key))
                .collect::<Vec<_>>(),
            [
                (WorkerRole::Developer, 1),
                (WorkerRole::Reviewer, 2),
                (WorkerRole::Reviewer, 3),
            ]
        );
        assert_eq!(
            audit
                .lane_sessions
                .iter()
                .map(|(_, lane, _)| *lane)
                .collect::<Vec<_>>(),
            [
                WorkerLane::Developer,
                WorkerLane::Reviewer(ReviewerId::Reviewer1),
                WorkerLane::Reviewer(ReviewerId::Reviewer2),
            ]
        );
        assert_eq!(
            audit
                .lane_events
                .iter()
                .copied()
                .filter(|event| {
                    matches!(
                        event,
                        ScriptedLaneEvent::TurnStarted(WorkerLane::Reviewer(_))
                            | ScriptedLaneEvent::TurnPolled(WorkerLane::Reviewer(_))
                    )
                })
                .collect::<Vec<_>>(),
            [
                ScriptedLaneEvent::TurnStarted(WorkerLane::Reviewer(ReviewerId::Reviewer1)),
                ScriptedLaneEvent::TurnStarted(WorkerLane::Reviewer(ReviewerId::Reviewer2)),
                ScriptedLaneEvent::TurnPolled(WorkerLane::Reviewer(ReviewerId::Reviewer1)),
                ScriptedLaneEvent::TurnPolled(WorkerLane::Reviewer(ReviewerId::Reviewer2)),
            ],
            "both Reviewer lanes must start before either lane is polled"
        );
        assert!(
            audit
                .profiles
                .iter()
                .all(|(_, _, profile)| profile == &RuntimeProfile::codex_exec_default())
        );
        assert!(
            !fixture
                .run_root
                .join("exec-workers")
                .read_dir()
                .unwrap()
                .any(|entry| entry.is_ok())
        );
    }

    #[test]
    fn terminal_driver_tolerates_more_than_one_fixed_poll_burst() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let delayed_developer = std::iter::repeat_n(
            RuntimeTurnPoll::Pending {
                telemetry: RuntimeTelemetry::default(),
            },
            128,
        )
        .chain([ready("scheduled-after-poll-burst")]);
        let script = task_script(
            "scheduled-terminal",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    delayed_developer,
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("scheduled-terminal-lgtm")],
                ),
            ],
            vec![Mutation::None; 2],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("scheduled-terminal", &["src"], 2)],
        );

        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert!(
            audit
                .lock()
                .unwrap()
                .lane_events
                .iter()
                .filter(|event| matches!(event, ScriptedLaneEvent::TurnPolled(_)))
                .count()
                > 64
        );
    }

    #[test]
    fn single_reviewer_driver_corrects_and_completes_without_reviewer2_artifacts() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = single_reviewer_task_script(
            "single",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("single-implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes("single-change")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [ready("single-corrected")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    [lgtm("single-sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/single.txt",
                    contents: "single\n",
                },
                Mutation::None,
                Mutation::None,
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.single_reviewer_supervisor(vec![script], Arc::clone(&audit));
        let mut task = fixture.task("single", &["src"], 5);
        task.max_review_rounds = crate::control_api::protocol::MIN_SINGLE_REVIEW_ROUNDS;
        start(&mut supervisor, vec![task]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.reviewer_bindings.len(), 1);
        assert_eq!(snapshot.tasks[0].reviewers.len(), 1);
        assert_eq!(snapshot.tasks[0].review_round, 2);
        assert_eq!(
            snapshot.tasks[0].reviewers[0].reviewer_id,
            ReviewerId::Reviewer1
        );
        let audit = audit.lock().unwrap();
        assert!(
            audit
                .lane_sessions
                .iter()
                .all(|(_, lane, _)| *lane != WorkerLane::Reviewer(ReviewerId::Reviewer2))
        );
        assert_eq!(
            audit
                .lane_sessions
                .iter()
                .filter(|(_, lane, _)| { *lane == WorkerLane::Reviewer(ReviewerId::Reviewer1) })
                .count(),
            1,
            "single-review re-review must exact-resume Reviewer1"
        );
        assert!(audit.lane_events.iter().all(|event| !matches!(
            event,
            ScriptedLaneEvent::TurnStarted(WorkerLane::Reviewer(ReviewerId::Reviewer2))
                | ScriptedLaneEvent::TurnPolled(WorkerLane::Reviewer(ReviewerId::Reviewer2))
        )));
        let correction_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Developer
                    && *purpose == RuntimeTurnPurpose::DeveloperCorrection
            })
            .map(|(_, _, prompt)| prompt)
            .expect("single-review correction prompt");
        assert!(correction_prompt.contains("- reviewer1:"));
        assert!(!correction_prompt.contains("reviewer2"));
    }

    #[test]
    fn one_foreground_supervisor_runs_two_immutable_runs_with_fresh_workers() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = vec![
            task_script(
                "first",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("first implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("first sound")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/first.txt",
                        contents: "first\n",
                    },
                    Mutation::None,
                ],
            ),
            task_script(
                "second",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("second implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("second sound")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/second.txt",
                        contents: "second\n",
                    },
                    Mutation::None,
                ],
            ),
        ];
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));

        start(&mut supervisor, vec![fixture.task("first", &["src"], 3)]);
        let first_terminal = drive_terminal(&mut supervisor);
        let first_run_id = first_terminal.run_id.clone();
        let first_plan_path = fixture
            .project_root
            .join("hcom-tasks")
            .join(&first_run_id)
            .join("plan.md");
        let first_plan = fs::read(&first_plan_path).unwrap();

        supervisor
            .begin_next_run_with_id(
                first_terminal.version,
                &first_run_id,
                "run-driver-next".into(),
            )
            .unwrap();
        let awaiting_plan = supervisor.snapshot();
        assert_eq!(awaiting_plan.run_id, "run-driver-next");
        assert_eq!(supervisor.startup().run_id, "run-driver-next");
        assert_eq!(awaiting_plan.version, first_terminal.version + 1);
        assert_eq!(awaiting_plan.state, SessionState::AwaitingPlan);
        assert!(awaiting_plan.tasks.is_empty());
        assert_eq!(awaiting_plan.plan_version, None);
        assert_eq!(awaiting_plan.plan_hash, None);
        assert_eq!(fs::read(&first_plan_path).unwrap(), first_plan);
        assert!(
            supervisor.project_tasks_workspace.is_some(),
            "the foreground Architect must retain the project lease between runs"
        );
        assert!(
            supervisor.tasks_workspace.is_none(),
            "the terminal run handle must not become the next run handle"
        );
        let competing_session = ProjectTasksWorkspace::open(&fixture.project_root).unwrap_err();
        assert!(
            competing_session.to_string().contains("already holds"),
            "a competing hcom session was not rejected between sequential runs: {competing_session}"
        );
        let competing_owner = Command::new("/usr/bin/flock")
            .args(["--nonblock", "--exclusive"])
            .arg(fixture.project_root.join("hcom-tasks/.lock"))
            .arg("true")
            .status()
            .expect("probe the project workspace lease from another process");
        assert!(
            !competing_owner.success(),
            "another process acquired the project workspace between sequential runs"
        );

        let before_rejected_restart = supervisor.snapshot();
        assert!(
            supervisor
                .begin_next_run_with_id(
                    before_rejected_restart.version,
                    "run-driver-next",
                    "run-too-early".into(),
                )
                .is_err()
        );
        assert_eq!(supervisor.snapshot(), before_rejected_restart);
        assert!(
            supervisor
                .replace_plan(
                    first_terminal.version,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    vec![fixture.task("stale", &["src"], 3)],
                )
                .is_err(),
            "the previous run's terminal version must not match the new run"
        );

        let second_task = fixture.task("second", &["src"], 3);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                awaiting_plan.version,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![second_task],
            )
            .unwrap();
        let awaiting_approval = supervisor.snapshot();
        supervisor
            .approve_and_start(awaiting_approval.version, plan_version, &plan_hash, true)
            .unwrap();
        let second_terminal = drive_terminal(&mut supervisor);
        assert_eq!(second_terminal.run_id, "run-driver-next");
        assert_eq!(second_terminal.state, SessionState::Completed);
        assert!(second_terminal.version > first_terminal.version);
        assert_eq!(fs::read(&first_plan_path).unwrap(), first_plan);
        assert!(
            fixture
                .project_root
                .join("hcom-tasks/run-driver-next/plan.md")
                .is_file()
        );

        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["first", "second"]);
        assert_eq!(audit.shutdowns, ["first", "second"]);
        assert_eq!(
            audit
                .sessions
                .iter()
                .map(|(task, role, key)| (task.as_str(), *role, *key))
                .collect::<Vec<_>>(),
            [
                ("first", WorkerRole::Developer, 1),
                ("first", WorkerRole::Reviewer, 2),
                ("first", WorkerRole::Reviewer, 3),
                ("second", WorkerRole::Developer, 1),
                ("second", WorkerRole::Reviewer, 2),
                ("second", WorkerRole::Reviewer, 3),
            ]
        );
    }

    #[test]
    fn pending_guardian_cleanup_blocks_next_run_until_completion() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "first",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/first.txt",
                    contents: "first\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![fixture.task("first", &["src"], 2)]);
        let terminal = drive_terminal(&mut supervisor);
        supervisor.cleanup_registry.inject_pending_for_test();

        let before = supervisor.snapshot();
        let error = supervisor
            .begin_next_run_with_id(
                terminal.version,
                &terminal.run_id,
                "run-cleanup-blocked".into(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("cleanup is pending"));
        assert_eq!(supervisor.snapshot(), before);

        supervisor.cleanup_registry.complete_pending_for_test();
        supervisor
            .begin_next_run_with_id(
                terminal.version,
                &terminal.run_id,
                "run-cleanup-released".into(),
            )
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingPlan);
    }

    #[test]
    fn lost_guardian_ownership_permanently_poisons_sequential_runs() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "poison",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/poison.txt",
                    contents: "poison\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![fixture.task("poison", &["src"], 2)]);
        let terminal = drive_terminal(&mut supervisor);
        supervisor
            .cleanup_registry
            .poison_for_test("protocol identity lost");

        let error = supervisor
            .begin_next_run_with_id(terminal.version, &terminal.run_id, "run-poisoned".into())
            .unwrap_err();
        assert!(error.to_string().contains("ownership lost"));
        assert_eq!(supervisor.snapshot(), terminal);
        assert!(supervisor.cleanup_registry.ensure_available().is_err());
    }

    #[test]
    fn correction_and_rereview_reuse_each_exact_role_session() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "correction",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("first")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes("correct it")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [ready("corrected")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    [lgtm("fixed")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "first\n",
                },
                Mutation::None,
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "corrected\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("correction", &["src"], 3)],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].review_round, 2);
        let audit = audit.lock().unwrap();
        assert_eq!(audit.sessions.len(), 3);
        assert_eq!(
            audit
                .turns
                .iter()
                .map(|(_, role, purpose, session)| (*role, *purpose, *session))
                .collect::<Vec<_>>(),
            [
                (
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    1
                ),
                (WorkerRole::Reviewer, RuntimeTurnPurpose::InitialReview, 2),
                (WorkerRole::Reviewer, RuntimeTurnPurpose::InitialReview, 3),
                (
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    1
                ),
                (
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    2
                ),
                (
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    3
                ),
            ]
        );
    }

    #[test]
    fn dirty_completion_routes_to_review_without_recovery() {
        // Task-agnostic lane: an uncommitted developer completion is not a
        // supervisor concern — the task routes straight to review with a
        // single developer turn and no recovery machinery.
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "dirty-routes",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("forgot commit")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("reviewer judged the dirty tree acceptable")],
                ),
            ],
            vec![
                Mutation::Dirty {
                    path: "src/task.txt",
                    contents: "dirty\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("dirty-routes", &["src"], 3)],
        );
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );
        let audit = audit.lock().unwrap();
        let developer_turns: Vec<_> = audit
            .turns
            .iter()
            .filter(|(_, role, _, _)| *role == WorkerRole::Developer)
            .collect();
        assert_eq!(developer_turns.len(), 1);
        assert_eq!(
            audit
                .sessions
                .iter()
                .filter(|(_, role, _)| *role == WorkerRole::Reviewer)
                .count(),
            2
        );
    }

    #[test]
    fn retryable_contract_failure_is_terminal_without_recovery() {
        // The retryable flag no longer buys a recovery turn: any contract
        // failure stops for the human with exactly one developer turn and no
        // reviewer session.
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "contract-terminal",
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [failed_retryable()],
            )],
            vec![Mutation::None],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("contract-terminal", &["src"], 3)],
        );

        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("worker runtime contract failed: developer outcome was missing")
        );
        let audit = audit.lock().unwrap();
        assert_eq!(
            audit
                .sessions
                .iter()
                .filter(|(_, role, _)| *role == WorkerRole::Developer)
                .count(),
            1
        );
        assert!(
            audit
                .sessions
                .iter()
                .all(|(_, role, _)| *role != WorkerRole::Reviewer)
        );
        let developer_turns: Vec<_> = audit
            .turns
            .iter()
            .filter(|(_, role, _, _)| *role == WorkerRole::Developer)
            .collect();
        assert_eq!(developer_turns.len(), 1);
        assert_eq!(audit.shutdowns, ["contract-terminal"]);
    }

    #[test]
    fn reviewer_failure_preserves_peer_evidence_cancels_join_and_never_respawns() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = TaskScript {
            task_key: "reviewer-failure".into(),
            turns: vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("committed")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("reviewer1 evidence")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [failed_retryable()],
                ),
            ],
            mutations: VecDeque::from([
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "committed\n",
                },
                Mutation::None,
                Mutation::None,
            ]),
            shutdown_failure: false,
        };
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("reviewer-failure", &["src"], 3)],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].review_round, 0);
        assert_eq!(snapshot.tasks[0].review_generation, 1);
        assert_eq!(
            snapshot.tasks[0].reviewers[0].current_verdict,
            Some(ReviewerVerdict::Lgtm)
        );
        assert_eq!(snapshot.tasks[0].reviewers[1].current_verdict, None);
        assert_eq!(
            snapshot.tasks[0].reviewers[0]
                .current_final_message_paths
                .len(),
            1
        );

        let audit = audit.lock().unwrap();
        assert_eq!(
            audit
                .turns
                .iter()
                .filter(|(_, role, _, _)| *role == WorkerRole::Reviewer)
                .count(),
            2,
            "retryable Reviewer failure must not spawn a replacement lane"
        );
        assert!(
            audit
                .turns
                .iter()
                .all(|(_, role, purpose, _)| *role != WorkerRole::Developer
                    || *purpose != RuntimeTurnPurpose::DeveloperCorrection)
        );
        assert_eq!(audit.shutdowns, ["reviewer-failure"]);
    }

    #[test]
    fn normalized_runtime_process_protocol_and_timeout_failures_stop_the_driver() {
        for (name, class, expected_detail) in [
            (
                "runtime-process",
                RuntimeFailureClass::Process,
                "worker runtime process failed",
            ),
            (
                "runtime-protocol",
                RuntimeFailureClass::Protocol,
                "worker runtime protocol failed",
            ),
            (
                "runtime-timeout",
                RuntimeFailureClass::Timeout,
                "worker runtime reported a timeout",
            ),
        ] {
            let fixture = Fixture::new();
            let audit = Arc::new(Mutex::new(Audit::default()));
            let script = task_script(
                name,
                vec![FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [failed(class, "sanitized-runtime-detail")],
                )],
                vec![Mutation::None],
            );
            let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
            start(&mut supervisor, vec![fixture.task(name, &["src"], 2)]);
            let snapshot = drive_terminal(&mut supervisor);
            assert_eq!(snapshot.state, SessionState::NeedsHuman);
            // The class label plus the runtime's sanitized detail: the report
            // must stay actionable instead of collapsing to a fixed string.
            let detail = snapshot.terminal_detail.clone().unwrap();
            assert_eq!(
                detail,
                format!("{expected_detail}: sanitized-runtime-detail")
            );
            assert_eq!(audit.lock().unwrap().shutdowns, [name]);
        }
    }

    #[test]
    fn explicit_cancel_and_parent_stop_interrupt_and_close_the_active_runtime() {
        for (name, parent_stop, expected_detail) in [
            (
                "explicit-cancel",
                false,
                "canceled by explicit Architect-session request",
            ),
            ("parent-stop", true, "foreground Architect parent stopped"),
        ] {
            let fixture = Fixture::new();
            let audit = Arc::new(Mutex::new(Audit::default()));
            let script = task_script(
                name,
                vec![FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [
                        RuntimeTurnPoll::Pending {
                            telemetry: RuntimeTelemetry::default(),
                        },
                        ready("must not be accepted"),
                    ],
                )],
                vec![Mutation::None],
            );
            let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
            start(&mut supervisor, vec![fixture.task(name, &["src"], 2)]);
            if parent_stop {
                supervisor.shutdown().unwrap();
            } else {
                supervisor
                    .cancel(
                        supervisor.snapshot().version,
                        "human requested cancellation",
                    )
                    .unwrap();
            }
            let snapshot = supervisor.snapshot();
            assert_eq!(snapshot.state, SessionState::Canceled);
            assert_eq!(snapshot.terminal_detail.as_deref(), Some(expected_detail));
            assert_eq!(audit.lock().unwrap().shutdowns, [name]);
        }
    }

    #[test]
    fn explicit_cancel_during_review_closes_the_same_task_runtime() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "cancel-review",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("committed")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [RuntimeTurnPoll::Pending {
                        telemetry: RuntimeTelemetry::default(),
                    }],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "committed\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("cancel-review", &["src"], 2)],
        );
        supervisor.poll_once().unwrap();
        let reviewing = supervisor.snapshot();
        assert_eq!(reviewing.state, SessionState::Running);
        assert_eq!(reviewing.tasks[0].state, TaskState::Reviewing);

        supervisor
            .cancel(reviewing.version, "human requested cancellation")
            .unwrap();
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::Canceled);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("canceled by explicit Architect-session request")
        );
        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["cancel-review"]);
        assert_eq!(audit.shutdowns, ["cancel-review"]);
    }

    #[test]
    fn request_changes_round_routes_only_ordered_durable_paths() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "review-loop",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("first attempt: added the module")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes_after_clarification(
                        "the overflow case is unhandled",
                    )],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [ready("second attempt: handled overflow")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    [lgtm("overflow handling is correct now")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "one\n",
                },
                Mutation::None,
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "two\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("review-loop", &["src"], 3)],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert_eq!(snapshot.tasks[0].review_round, 2);

        let audit = audit.lock().unwrap();
        // One Developer session and two fixed Reviewer sessions, each resumed.
        assert_eq!(
            audit
                .sessions
                .iter()
                .filter(|(_, role, _)| *role == WorkerRole::Developer)
                .count(),
            1
        );
        assert_eq!(
            audit
                .sessions
                .iter()
                .filter(|(_, role, _)| *role == WorkerRole::Reviewer)
                .count(),
            2
        );
        // Each role receives the peer's durable path, never the peer body.
        let review_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Reviewer && *purpose == RuntimeTurnPurpose::InitialReview
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("initial review prompt");
        let development_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Developer && *purpose == RuntimeTurnPurpose::InitialDevelopment
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("initial development prompt");
        for prompt in [&development_prompt, &review_prompt] {
            assert!(prompt.contains("AGENTS.md"));
            assert!(prompt.contains("AGENTS.override.md"));
            assert!(prompt.contains(fixture.project_root.to_str().unwrap()));
            assert!(prompt.contains(fixture.repository.to_str().unwrap()));
            assert!(prompt.contains("task-review-loop.md"));
            assert!(prompt.contains("design.md"));
            assert!(prompt.contains("Task selector:\nreview-loop"));
            assert!(!prompt.contains("TASK-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT"));
            assert!(!prompt.contains("DESIGN-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT"));
        }
        assert!(!development_prompt.contains("first attempt: added the module"));
        assert!(!review_prompt.contains("first attempt: added the module"));
        assert!(
            review_prompt.contains(
                message_path(WorkerRole::Developer, "first attempt: added the module")
                    .to_str()
                    .unwrap()
            )
        );
        assert!(review_prompt.contains("VERDICT: LGTM"));
        assert!(review_prompt.contains("VERDICT: REQUEST_CHANGES"));
        let correction_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Developer
                    && *purpose == RuntimeTurnPurpose::DeveloperCorrection
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("correction prompt");
        assert!(!correction_prompt.contains("the overflow case is unhandled"));
        assert!(correction_prompt.contains("do not modify or amend anything"));
        assert!(correction_prompt.contains("valid `Signed-off-by` trailer"));
        let original_review_path = message_path(
            WorkerRole::Reviewer,
            "the overflow case is unhandled-original",
        );
        let clarification_path =
            message_path(WorkerRole::Reviewer, "the overflow case is unhandled");
        let original_index = correction_prompt
            .find(original_review_path.to_str().unwrap())
            .expect("original Reviewer path");
        let clarification_index = correction_prompt
            .find(clarification_path.to_str().unwrap())
            .expect("clarification Reviewer path");
        assert!(original_index < clarification_index);
        let rereview_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Reviewer && *purpose == RuntimeTurnPurpose::ReviewerRereview
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("re-review prompt");
        assert!(!rereview_prompt.contains("second attempt: handled overflow"));
        assert!(
            rereview_prompt.contains(
                message_path(WorkerRole::Developer, "second attempt: handled overflow")
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            snapshot.tasks[0].latest_developer_final_path.as_deref(),
            message_path(WorkerRole::Developer, "second attempt: handled overflow").to_str()
        );
        assert_eq!(
            reviewer_paths(&snapshot.tasks[0]),
            vec![
                message_path(WorkerRole::Reviewer, "overflow handling is correct now")
                    .to_string_lossy()
                    .into_owned(),
                reviewer2_final_path(&message_path(
                    WorkerRole::Reviewer,
                    "overflow handling is correct now",
                ))
                .to_string_lossy()
                .into_owned(),
            ]
        );

        let plan = fs::read_to_string(
            fixture
                .project_root
                .join("hcom-tasks/run-driver-test/plan.md"),
        )
        .unwrap();
        assert!(plan.contains("task document path:"));
        assert!(plan.contains("task-review-loop.md"));
        assert!(plan.contains("design document paths:"));
        assert!(plan.contains("task selector: review-loop"));
        assert!(!plan.contains("TASK-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT"));
        assert!(!plan.contains("DESIGN-DOCUMENT-CONTENT-MUST-NOT-BE-IN-PROMPT"));
    }

    #[test]
    fn large_or_missing_task_documents_are_never_preflighted_or_copied_into_prompts() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));

        let large = fixture.task("large-documents", &["src"], 2);
        let large_task_marker = "LARGE-TASK-DOCUMENT-CONTENT-MUST-STAY-ON-DISK";
        let large_design_marker = "LARGE-DESIGN-DOCUMENT-CONTENT-MUST-STAY-ON-DISK";
        fs::write(
            &large.task_document_path,
            format!(
                "{large_task_marker}\n{}",
                "t".repeat(crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES * 2)
            ),
        )
        .unwrap();
        fs::write(
            &large.design_document_paths[0],
            format!(
                "{large_design_marker}\n{}",
                "d".repeat(crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES * 2)
            ),
        )
        .unwrap();

        let mut missing = fixture.task("missing-documents", &["src"], 2);
        missing.task_document_path = fixture
            .project_root
            .join("does-not-exist-task.md")
            .to_string_lossy()
            .into_owned();
        missing.design_document_paths = vec![
            fixture
                .project_root
                .join("does-not-exist-design.md")
                .to_string_lossy()
                .into_owned(),
        ];

        let scripts = ["large-documents", "missing-documents"]
            .into_iter()
            .map(|task_key| {
                task_script(
                    task_key,
                    vec![
                        FakeTurnScript::new(
                            WorkerRole::Developer,
                            RuntimeTurnPurpose::InitialDevelopment,
                            [ready("developer completed the selected task")],
                        ),
                        FakeTurnScript::new(
                            WorkerRole::Reviewer,
                            RuntimeTurnPurpose::InitialReview,
                            [lgtm("reviewer accepted the selected task")],
                        ),
                    ],
                    vec![Mutation::None, Mutation::None],
                )
            })
            .collect();
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));
        start(&mut supervisor, vec![large.clone(), missing.clone()]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.state == TaskState::Lgtm)
        );

        let audit = audit.lock().unwrap();
        assert_eq!(audit.prompts.len(), 6);
        for (_, _, prompt) in &audit.prompts {
            assert!(prompt.len() < crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES);
            assert!(!prompt.contains(large_task_marker));
            assert!(!prompt.contains(large_design_marker));
        }
        assert!(
            audit
                .prompts
                .iter()
                .any(|(_, _, prompt)| prompt.contains(&large.task_document_path))
        );
        assert!(
            audit
                .prompts
                .iter()
                .any(|(_, _, prompt)| prompt.contains(&missing.task_document_path))
        );
        assert!(
            audit
                .prompts
                .iter()
                .any(|(_, _, prompt)| prompt.contains(&missing.design_document_paths[0]))
        );
        drop(audit);

        let plan = fs::read_to_string(
            fixture
                .project_root
                .join("hcom-tasks/run-driver-test/plan.md"),
        )
        .unwrap();
        assert!(plan.contains(&large.task_document_path));
        assert!(plan.contains(&missing.task_document_path));
        assert!(!plan.contains(large_task_marker));
        assert!(!plan.contains(large_design_marker));
    }

    #[test]
    fn review_exhausted_advances_without_pretending_to_be_lgtm() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = exhausted_task_script("exhausted", "src/task.txt", "one\n", "still wrong");
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("exhausted", &["src"], 1)],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(
            reviewer_paths(&snapshot.tasks[0]),
            vec![
                message_path(WorkerRole::Reviewer, "still wrong-7")
                    .to_string_lossy()
                    .into_owned(),
                reviewer2_final_path(&message_path(WorkerRole::Reviewer, "still wrong-7"))
                    .to_string_lossy()
                    .into_owned(),
            ]
        );
        assert_eq!(
            joined_reviewer_verdict(&snapshot.tasks[0]),
            Some(ReviewerVerdict::RequestChanges)
        );
        assert!(
            snapshot.tasks[0]
                .outcome_detail
                .as_deref()
                .unwrap_or_default()
                .contains("exhausted")
        );
    }

    #[test]
    fn each_task_gets_fresh_developer_and_reviewer_sessions() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = [("task-one", "first\n"), ("task-two", "second\n")]
            .iter()
            .map(|(key, contents)| {
                task_script(
                    key,
                    vec![
                        FakeTurnScript::new(
                            WorkerRole::Developer,
                            RuntimeTurnPurpose::InitialDevelopment,
                            [ready("done")],
                        ),
                        FakeTurnScript::new(
                            WorkerRole::Reviewer,
                            RuntimeTurnPurpose::InitialReview,
                            [lgtm("sound")],
                        ),
                    ],
                    vec![
                        Mutation::Commit {
                            path: "src/task.txt",
                            contents,
                        },
                        Mutation::None,
                    ],
                )
            })
            .collect();
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![
                fixture.task("task-one", &["src"], 2),
                fixture.task("task-two", &["src"], 2),
            ],
        );
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );
        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["task-one", "task-two"]);
        assert_eq!(audit.shutdowns, ["task-one", "task-two"]);
        // Six sessions total, three per task, each opened against that task's
        // own fresh runtime — nothing is carried across the task boundary.
        assert_eq!(audit.sessions.len(), 6);
        for task in ["task-one", "task-two"] {
            let roles: BTreeSet<WorkerRole> = audit
                .sessions
                .iter()
                .filter(|(key, _, _)| key == task)
                .map(|(_, role, _)| *role)
                .collect();
            assert_eq!(
                roles,
                BTreeSet::from([WorkerRole::Developer, WorkerRole::Reviewer]),
                "{task} must open exactly one Developer and two Reviewer sessions"
            );
        }
    }

    #[test]
    fn reviewer_side_mutations_never_gate_the_verdict() {
        // Task-agnostic lane: reviewer-side repository observations are
        // diagnostic only. The reviewer's inability to mutate canonical
        // source is enforced by its read-only sandbox mounts, not by a
        // supervisor verdict gate.
        for (name, reviewer_mutation, expected) in [
            (
                "reviewer-dirty",
                Mutation::Dirty {
                    path: "src/reviewer.txt",
                    contents: "forbidden\n",
                },
                SessionState::Completed,
            ),
            (
                "reviewer-stage",
                Mutation::Stage {
                    path: "src/reviewer.txt",
                    contents: "forbidden\n",
                },
                SessionState::Completed,
            ),
            (
                "reviewer-commit",
                Mutation::Commit {
                    path: "src/reviewer.txt",
                    contents: "forbidden\n",
                },
                SessionState::Completed,
            ),
            (
                "reviewer-branch",
                Mutation::Branch {
                    name: "reviewer-mutated-branch",
                },
                SessionState::Completed,
            ),
            (
                "reviewer-cache",
                Mutation::Dirty {
                    path: "target/reviewer.cache",
                    contents: "ignored\n",
                },
                SessionState::Completed,
            ),
        ] {
            let fixture = Fixture::new();
            let audit = Arc::new(Mutex::new(Audit::default()));
            let script = task_script(
                name,
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("reviewed")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/task.txt",
                        contents: "done\n",
                    },
                    reviewer_mutation,
                ],
            );
            let mut supervisor = fixture.supervisor(vec![script], audit);
            start(&mut supervisor, vec![fixture.task(name, &["src"], 3)]);
            let snapshot = drive_terminal(&mut supervisor);
            assert_eq!(snapshot.state, expected, "{name}");
        }
    }

    #[test]
    fn two_tasks_get_fresh_runtimes_and_nonreused_logical_sessions() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = ["one", "two"]
            .into_iter()
            .map(|key| {
                task_script(
                    key,
                    vec![
                        FakeTurnScript::new(
                            WorkerRole::Developer,
                            RuntimeTurnPurpose::InitialDevelopment,
                            [ready("implemented")],
                        ),
                        FakeTurnScript::new(
                            WorkerRole::Reviewer,
                            RuntimeTurnPurpose::InitialReview,
                            [lgtm("sound")],
                        ),
                    ],
                    vec![
                        Mutation::Commit {
                            path: if key == "one" {
                                "src/one.txt"
                            } else {
                                "src/two.txt"
                            },
                            contents: "done\n",
                        },
                        Mutation::None,
                    ],
                )
            })
            .collect();
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![
                fixture.task("one", &["src"], 2),
                fixture.task("two", &["src"], 2),
            ],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks.len(), 2);
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.state == crate::control_api::TaskState::Lgtm)
        );
        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["one", "two"]);
        assert_eq!(audit.shutdowns, ["one", "two"]);
        assert_eq!(
            audit
                .sessions
                .iter()
                .map(|(task, role, local)| (task.as_str(), *role, *local))
                .collect::<Vec<_>>(),
            [
                ("one", WorkerRole::Developer, 1),
                ("one", WorkerRole::Reviewer, 2),
                ("one", WorkerRole::Reviewer, 3),
                ("two", WorkerRole::Developer, 1),
                ("two", WorkerRole::Reviewer, 2),
                ("two", WorkerRole::Reviewer, 3),
            ]
        );
        let first = supervisor.core.tasks()[0].developer_session.unwrap();
        let second = supervisor.core.tasks()[1].developer_session.unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn ordered_tasks_can_bind_distinct_canonical_repositories() {
        let fixture = Fixture::new();
        let second = fixture
            .project_root
            .parent()
            .unwrap()
            .join("repository-two");
        fs::create_dir(&second).unwrap();
        fs::set_permissions(&second, fs::Permissions::from_mode(0o700)).unwrap();
        git(&second, &["init", "-b", "master"]);
        fs::create_dir(second.join("src")).unwrap();
        fs::write(second.join("src/seed.txt"), "second seed\n").unwrap();
        git(&second, &["add", "--", "src/seed.txt"]);
        git_commit(&second, "Initial second fixture");
        let second = fs::canonicalize(second).unwrap();

        let scripts = vec![
            task_script(
                "first-repository",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("sound")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/first.txt",
                        contents: "first\n",
                    },
                    Mutation::None,
                ],
            ),
            task_script(
                "second-repository",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("sound")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/second.txt",
                        contents: "second\n",
                    },
                    Mutation::None,
                ],
            ),
        ];
        let audit = Arc::new(Mutex::new(Audit::default()));
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));
        let first_task = fixture.task("first-repository", &["src"], 2);
        let mut second_task = fixture.task("second-repository", &["src"], 2);
        second_task.repository_root = second.to_string_lossy().into_owned();
        start(&mut supervisor, vec![first_task, second_task]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(
            snapshot.tasks[1].repository_root,
            second.to_string_lossy().into_owned()
        );
        assert_eq!(
            audit.lock().unwrap().opens,
            ["first-repository", "second-repository"]
        );
    }

    #[test]
    fn review_exhausted_closes_the_first_runtime_and_advances_to_the_next_task() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let scripts = vec![
            exhausted_task_script(
                "exhausted",
                "src/exhausted.txt",
                "first\n",
                "bounded finding",
            ),
            task_script(
                "next",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("sound")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/next.txt",
                        contents: "second\n",
                    },
                    Mutation::None,
                ],
            ),
        ];
        let mut supervisor = fixture.supervisor(scripts, Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![
                fixture.task("exhausted", &["src"], 1),
                fixture.task("next", &["src"], 2),
            ],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(
            snapshot.tasks[0].review_round,
            u32::from(crate::control_api::protocol::MIN_DUAL_REVIEW_ROUNDS)
        );
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert_eq!(audit.lock().unwrap().shutdowns, ["exhausted", "next"]);
    }

    #[test]
    fn complete_parent_environment_is_preserved_byte_for_byte() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "environment",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("environment", &["src"], 2)],
        );
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );
        let audit = audit.lock().unwrap();
        let environment = &audit.environments[0];
        let get = |name: &[u8]| {
            environment
                .iter()
                .find(|(candidate, _)| candidate.as_os_str().as_bytes() == name)
                .map(|(_, value)| value.as_os_str().as_bytes())
        };
        assert_eq!(
            get(b"UNKNOWN_PARENT_VALUE"),
            Some(b"unknown-value".as_slice())
        );
        assert_eq!(
            get(b"SERVICE_ACCESS_TOKEN"),
            Some(b"environment-secret-sentinel".as_slice())
        );
        assert_eq!(get(b"EMPTY_PARENT_VALUE"), Some(b"".as_slice()));
        assert_eq!(get(b"CASE_PAIR"), Some(b"upper".as_slice()));
        assert_eq!(get(b"case_pair"), Some(b"lower".as_slice()));
        assert_eq!(get(b"RAW_\xff_NAME"), Some(b"value-\xfe".as_slice()));
        assert_eq!(get(b"HTTP_PROXY"), Some(b"http://proxy.example".as_slice()));
        assert_eq!(
            get(b"https_proxy"),
            Some(b"http://lower-proxy.example".as_slice())
        );
        assert_eq!(get(b"HCOM_DIR"), Some(b"/must/not/reach/task".as_slice()));
        assert_eq!(
            get(b"HCOM_WORKER_ROLE"),
            Some(b"parent-worker-role".as_slice())
        );
        assert_eq!(get(b"HCOM_RUN_ID"), Some(b"parent-run-id".as_slice()));
        assert_eq!(get(b"HCOM_TASK_ID"), Some(b"parent-task-id".as_slice()));
        assert_eq!(get(b"HOME"), Some(b"/native/home".as_slice()));
        assert_eq!(get(b"CODEX_HOME"), Some(b"/native/codex-home".as_slice()));
        assert_eq!(get(b"TMPDIR"), Some(b"/native/tmp".as_slice()));
        assert_eq!(
            get(b"XDG_RUNTIME_DIR"),
            Some(b"/native/xdg-runtime".as_slice())
        );
        assert_eq!(
            get(b"XDG_CACHE_HOME"),
            Some(b"/native/xdg-cache".as_slice())
        );
        assert_eq!(environment.len(), 18);
    }

    #[test]
    fn independent_codex_role_overrides_are_used_by_runtime_turns() {
        let mut fixture = Fixture::new();
        let profiles = fixture.sources.profiles.as_mut().unwrap();
        let DeveloperInvocationProfile::Codex { profile: developer } = &mut profiles.developer
        else {
            unreachable!()
        };
        developer.model = "developer-override".into();
        developer.reasoning_effort = "high".into();
        let ReviewerInvocationProfile::Codex { profile: reviewer } = profiles.reviewer1_mut()
        else {
            unreachable!()
        };
        reviewer.model = "reviewer1-override".into();
        reviewer.reasoning_effort = "max".into();
        let ReviewerInvocationProfile::Codex { profile: reviewer } = profiles.reviewer2_mut()
        else {
            unreachable!()
        };
        reviewer.model = "reviewer2-override".into();
        reviewer.reasoning_effort = "medium".into();

        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "profile",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(&mut supervisor, vec![fixture.task("profile", &["src"], 2)]);
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );
        let profiles = &audit.lock().unwrap().profiles;
        assert_eq!(profiles[0].1, WorkerRole::Developer);
        assert_eq!(profiles[0].2.model, "developer-override");
        assert_eq!(profiles[0].2.reasoning_effort, "high");
        assert_eq!(profiles[1].1, WorkerRole::Reviewer);
        assert_eq!(profiles[1].2.model, "reviewer1-override");
        assert_eq!(profiles[1].2.reasoning_effort, "max");
        assert_eq!(profiles[2].1, WorkerRole::Reviewer);
        assert_eq!(profiles[2].2.model, "reviewer2-override");
        assert_eq!(profiles[2].2.reasoning_effort, "medium");
    }

    #[test]
    fn out_of_scope_commit_and_secret_shaped_final_both_route_to_review() {
        // Task-agnostic lane: an out-of-allowlist commit is the reviewer's
        // and the human's call, not a supervisor gate.
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "outside",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("scope judged acceptable")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "outside.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![fixture.task("outside", &["src"], 2)]);
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );

        // Agent final contents are opaque and never trigger a sensitive-value
        // rejection in the task lane.
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "secret",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("environment-secret-sentinel")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("secret-shaped text is opaque")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![fixture.task("secret", &["src"], 2)]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(!encoded.contains("environment-secret-sentinel"));
    }

    #[test]
    fn allowed_delete_rename_and_mode_commits_reach_review() {
        for (name, mutation) in [
            (
                "allowed-delete",
                Mutation::DeleteCommit {
                    path: "src/seed.txt",
                },
            ),
            (
                "allowed-rename",
                Mutation::RenameCommit {
                    from: "src/seed.txt",
                    to: "src/renamed.txt",
                },
            ),
            (
                "allowed-mode",
                Mutation::ModeCommit {
                    path: "src/seed.txt",
                    mode: 0o700,
                },
            ),
        ] {
            let fixture = Fixture::new();
            let audit = Arc::new(Mutex::new(Audit::default()));
            let script = task_script(
                name,
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [lgtm("sound")],
                    ),
                ],
                vec![mutation, Mutation::None],
            );
            let mut supervisor = fixture.supervisor(vec![script], audit);
            start(&mut supervisor, vec![fixture.task(name, &["src"], 2)]);
            assert_eq!(
                drive_terminal(&mut supervisor).state,
                SessionState::Completed
            );
        }
    }

    #[test]
    fn a_dirty_plan_still_runs_and_reaches_review() {
        // The supervisor does not police the working tree: a task that starts
        // from a dirty checkout runs, and the reviewer sees the result.
        let fixture = Fixture::new();
        fs::write(fixture.repository.join("src/dirty.txt"), "dirty\n").unwrap();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "dirty",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("worked from an untidy tree")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("acceptable")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(&mut supervisor, vec![fixture.task("dirty", &["src"], 2)]);
        assert_eq!(
            drive_terminal(&mut supervisor).state,
            SessionState::Completed
        );
        assert_eq!(
            audit
                .lock()
                .unwrap()
                .sessions
                .iter()
                .filter(|(_, role, _)| *role == WorkerRole::Reviewer)
                .count(),
            2
        );
    }

    #[test]
    fn a_detached_or_dirty_checkout_still_binds() {
        // Working-tree condition is the reviewer's and the human's business.
        // hcom never inspects it: a task's repository_root is only a source
        // directory path, and any existing directory binds.
        for prepare in [
            |repository: &Path| git(repository, &["checkout", "--detach"]),
            |repository: &Path| {
                fs::write(repository.join("src/dirty.txt"), "uncommitted\n").unwrap()
            },
        ] {
            let fixture = Fixture::new();
            prepare(&fixture.repository);
            let mut supervisor =
                fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
            supervisor
                .replace_plan(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    vec![fixture.task("binds", &["src"], 2)],
                )
                .expect("an untidy checkout must still bind");
            assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        }

        // A subdirectory of a checkout is an ordinary existing directory, so it
        // binds too — there is no Git top-level identity check any more.
        let nested = Fixture::new();
        let mut task = nested.task("nested-root", &["src"], 2);
        task.repository_root = nested.repository.join("src").to_string_lossy().into_owned();
        let mut supervisor = nested.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .expect("a subdirectory is a valid task source directory");
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
    }

    #[test]
    fn a_task_source_directory_that_does_not_exist_is_rejected() {
        let fixture = Fixture::new();
        let mut task = fixture.task("missing-root", &["src"], 2);
        task.repository_root = fixture
            .repository
            .join("does-not-exist")
            .to_string_lossy()
            .into_owned();
        let mut supervisor = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        assert!(
            supervisor
                .replace_plan(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    vec![task],
                )
                .is_err()
        );
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingPlan);
    }

    #[test]
    fn claude_architect_requires_external_repository_roots_to_be_predeclared() {
        let mut fixture = Fixture::new();
        fixture.sources.profiles = Some(pure_codex_profiles(ArchitectAdapter::Claude));
        let task = fixture.task("external-root", &["src"], 2);

        let mut rejected = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let error = rejected
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task.clone()],
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("--add-dir"));
        assert_eq!(rejected.snapshot().state, SessionState::AwaitingPlan);

        fixture
            .sources
            .architect_additional_directories
            .push(fixture.repository.clone());
        let mut accepted = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        accepted
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .unwrap();
        assert_eq!(accepted.snapshot().state, SessionState::AwaitingApproval);

        let project_local = fixture.project_root.join("local-repository");
        fs::create_dir(&project_local).unwrap();
        fixture.sources.architect_additional_directories.clear();
        let mut local_task = fixture.task("project-local", &["src"], 2);
        local_task.repository_root = project_local.to_string_lossy().into_owned();
        let mut local = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        local
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![local_task],
            )
            .unwrap();
        assert_eq!(local.snapshot().state, SessionState::AwaitingApproval);
    }

    #[test]
    fn failed_plan_replacement_preserves_the_previous_plan() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "retained-plan",
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [RuntimeTurnPoll::Pending {
                    telemetry: RuntimeTelemetry::default(),
                }],
            )],
            vec![Mutation::None],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        let retained_task = fixture.task("retained-plan", &["src"], 2);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![retained_task.clone()],
            )
            .unwrap();
        let before = supervisor.snapshot();

        // The only remaining plan-binding gate is that each task names an
        // existing source directory; a rejected replacement must leave the
        // previously bound plan byte-for-byte intact.
        let mut rejected_task = fixture.task("missing-root", &["src"], 2);
        rejected_task.repository_root = fixture
            .repository
            .join("does-not-exist")
            .to_string_lossy()
            .into_owned();
        assert!(
            supervisor
                .replace_plan(
                    before.version,
                    CODEX_TASK_WORKER_ADAPTER,
                    &pure_codex_reviewer_adapters(),
                    vec![rejected_task],
                )
                .is_err()
        );
        assert_eq!(supervisor.snapshot(), before);

        // A second live supervisor may bind the very same source directory:
        // nothing locks it any more.
        let mut probe = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        probe
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![retained_task],
            )
            .expect("task source directories are not locked");

        supervisor
            .approve_and_start(before.version, plan_version, &plan_hash, true)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::Running);
    }

    #[test]
    fn plan_acknowledgement_binds_both_ordered_reviewer_adapters() {
        let fixture = Fixture::new();
        let mut supervisor = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let task = fixture.task("adapter-bundle", &["src"], 2);

        let wrong_reviewer2 = reviewer_adapter_bindings(CODEX_TASK_WORKER_ADAPTER, "claude-exec");
        assert!(
            supervisor
                .replace_plan(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &wrong_reviewer2,
                    vec![task.clone()],
                )
                .is_err(),
            "Reviewer2 must not be carried only through an implicit session hash"
        );

        let mut wrong_order = pure_codex_reviewer_adapters();
        wrong_order.swap(0, 1);
        assert!(
            supervisor
                .replace_plan(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    &wrong_order,
                    vec![task.clone()],
                )
                .is_err()
        );

        supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
    }

    #[test]
    fn runtime_factory_failure_is_distinct_from_environment_setup_and_is_sanitized() {
        let fixture = Fixture::new();
        let mut supervisor = TaskLaneSupervisor::open_with_factory(
            "run-factory-failure".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            fixture.sources.clone(),
            Box::new(FailingFactory),
        )
        .unwrap();
        let task = fixture.task("factory-failure", &["src"], 2);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("runtime-factory-secret-sentinel")
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("task worker runtime operation failed")
        );
        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(!encoded.contains("runtime-factory-secret-sentinel"));
        assert!(
            !fixture
                .run_root
                .join("exec-workers")
                .read_dir()
                .unwrap()
                .any(|entry| entry.is_ok())
        );
    }

    #[test]
    fn production_claude_role_requires_exact_proxy_before_any_provider_process_exists() {
        let fixture = Fixture::new();
        let mut sources = fixture.sources.clone();
        let mut profiles =
            SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        profiles.developer = DeveloperInvocationProfile::Claude {
            profile: ClaudeInvocationProfile {
                model: "haiku".into(),
                effort: "medium".into(),
                dangerously_skip_permissions: true,
            },
        };
        *profiles.reviewer1_mut() = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile::reviewer_default(),
        };
        sources.set_profiles_for_test(profiles);
        let mut supervisor = TaskLaneSupervisor::open(
            "run-invalid-claude-proxy".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                "claude-exec",
                &reviewer_adapter_bindings("codex-exec", "claude-exec"),
                vec![fixture.task("invalid-claude-proxy", &["src"], 2)],
            )
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Claude proxy environment variable HTTP_PROXY does not match the required value"
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert!(snapshot.active_workers.is_empty());
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("task worker runtime operation failed")
        );
    }

    #[test]
    fn workspace_staging_failure_terminalizes_instead_of_wedging_the_run() {
        let fixture = Fixture::new();
        // A foreign `hcom-tasks` directory without the ownership marker makes
        // workspace staging fail after authorization is already committed.
        fs::create_dir(fixture.project_root.join("hcom-tasks")).unwrap();
        let mut supervisor = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let task = fixture.task("foreign-workspace", &["src"], 2);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(error.to_string().contains("hcom-tasks"));
        // The run must not stay Running with no worker and no way to
        // re-authorize; it terminalizes as an explainable needs_human.
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("task-private environment setup failed")
        );
        supervisor.poll_once().unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::NeedsHuman);
    }

    #[test]
    fn run_claim_failure_keeps_the_project_lease_until_foreground_drop() {
        let fixture = Fixture::new();
        let owner = ProjectTasksWorkspace::open(&fixture.project_root).unwrap();
        let stale_run = owner.claim_run("run-driver-test").unwrap();
        drop(stale_run);
        drop(owner);

        let mut supervisor = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let task = fixture.task("run-claim-failure", &["src"], 2);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                &pure_codex_reviewer_adapters(),
                vec![task],
            )
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert!(error.to_string().contains("claim the hcom-tasks run"));
        assert_eq!(supervisor.snapshot().state, SessionState::NeedsHuman);
        assert!(supervisor.project_tasks_workspace.is_some());
        assert!(supervisor.tasks_workspace.is_none());

        let competing_session = ProjectTasksWorkspace::open(&fixture.project_root).unwrap_err();
        assert!(
            competing_session.to_string().contains("already holds"),
            "a failed per-run claim released the foreground project lease: {competing_session}"
        );
    }

    #[test]
    fn successful_task_transition_is_not_committed_when_runtime_cleanup_fails() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script_with_shutdown_failure(
            "cleanup-failure",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("implemented")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("sound")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "done\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("cleanup-failure", &["src"], 2)],
        );
        let mut saw_cleanup_error = false;
        for _ in 0..8 {
            if supervisor.poll_once().is_err() {
                saw_cleanup_error = true;
            }
            if supervisor.snapshot().state.is_terminal() {
                break;
            }
        }
        assert!(saw_cleanup_error);
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("task worker runtime cleanup failed")
        );
        assert_eq!(audit.lock().unwrap().shutdowns, ["cleanup-failure"]);
    }

    #[test]
    fn clarification_action_resumes_same_developer_and_persists_into_review() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "clarify",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [clarification_required("need-decision")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperClarificationResume,
                    [ready("implemented-after-clarification")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("reviewed-with-clarification")],
                ),
            ],
            vec![
                Mutation::None,
                Mutation::Commit {
                    path: "src/clarified.txt",
                    contents: "clarified\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(&mut supervisor, vec![fixture.task("clarify", &[], 3)]);

        supervisor.poll_once().unwrap();
        let action_snapshot = supervisor.snapshot();
        assert_eq!(action_snapshot.state, SessionState::Running);
        assert!(action_snapshot.active_workers.is_empty());
        let pending = action_snapshot
            .pending_architect_action
            .clone()
            .expect("Developer clarification must wake the Architect");
        assert_eq!(
            pending.reason,
            crate::control_api::ArchitectActionReason::Clarification
        );
        assert!(!pending.human_decision_required);
        let expected_path = fixture
            .project_root
            .join("hcom-tasks/run-driver-test/clarify/clarification/turn-1.md")
            .to_string_lossy()
            .into_owned();
        assert_eq!(pending.clarification_output_path, expected_path);
        fs::write(
            &pending.clarification_output_path,
            "# Clarification\n\nUse the bounded option already implied by the design.\n",
        )
        .unwrap();
        supervisor
            .submit_clarification(
                action_snapshot.version,
                pending.task_ordinal,
                &pending.task_key,
                pending.sequence,
                &pending.developer_request_path,
                &pending.clarification_output_path,
                false,
            )
            .unwrap();
        let resumed = supervisor.snapshot();
        assert!(resumed.pending_architect_action.is_none());
        assert_eq!(
            resumed
                .active_workers
                .first()
                .map(|worker| worker.worker_lane.role()),
            Some(WorkerRole::Developer)
        );
        assert_eq!(
            resumed
                .active_workers
                .first()
                .map(|worker| worker.purpose.as_str()),
            Some("developer_clarification_resume")
        );

        let terminal = drive_terminal(&mut supervisor);
        assert_eq!(terminal.state, SessionState::Completed);
        let task = &terminal.tasks[0];
        assert_eq!(task.state, TaskState::Lgtm);
        assert_eq!(task.review_round, 1);
        assert_eq!(task.clarification_rounds_used, 1);
        assert_eq!(task.clarification_record_count, 1);
        let clarification_page = supervisor
            .clarification_page(&terminal.run_id, 0, "clarify", 0, 8)
            .unwrap();
        assert_eq!(clarification_page.records.len(), 1);
        assert_eq!(
            clarification_page.records[0].architect_clarification_path,
            pending.clarification_output_path
        );
        assert!(!clarification_page.records[0].human_decision_confirmed);

        let audit = audit.lock().unwrap();
        let developer_sessions: Vec<_> = audit
            .turns
            .iter()
            .filter(|(_, role, _, _)| *role == WorkerRole::Developer)
            .map(|(_, _, purpose, session)| (*purpose, *session))
            .collect();
        assert_eq!(
            developer_sessions,
            vec![
                (RuntimeTurnPurpose::InitialDevelopment, 1),
                (RuntimeTurnPurpose::DeveloperClarificationResume, 1),
            ]
        );
        for (_, purpose, prompt) in audit
            .prompts
            .iter()
            .filter(|(role, _, _)| *role == WorkerRole::Developer)
        {
            assert!(
                prompt.contains("STATUS: CLARIFICATION_REQUIRED"),
                "{purpose:?} omitted the per-turn Developer output contract"
            );
        }
        let resume_prompt = audit
            .prompts
            .iter()
            .find(|(_, purpose, _)| *purpose == RuntimeTurnPurpose::DeveloperClarificationResume)
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        assert!(resume_prompt.contains(&pending.clarification_output_path));
        let reviewer_prompt = audit
            .prompts
            .iter()
            .find(|(role, _, _)| *role == WorkerRole::Reviewer)
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        assert!(reviewer_prompt.contains(&pending.clarification_output_path));
        assert!(reviewer_prompt.contains("newer clarification takes precedence"));
    }

    #[test]
    fn derived_clarification_path_overflow_terminalizes_instead_of_losing_the_active_turn() {
        let mut fixture = Fixture::new();
        fixture.project_root = create_deep_directory(fixture._temp.path().to_path_buf(), 3_950);
        let task_key = "k".repeat(128);
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            &task_key,
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [clarification_required("derived-path-overflow")],
            )],
            vec![Mutation::None],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(&mut supervisor, vec![fixture.task(&task_key, &[], 3)]);

        let error = supervisor.poll_once().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("clarification output path is empty or exceeds its bound"),
            "{error:#}"
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("session runtime contract failed")
        );
        assert!(snapshot.pending_architect_action.is_none());
        assert!(snapshot.active_workers.is_empty());
        assert!(supervisor.active.is_empty());
        assert!(supervisor.task_runtime.is_none());
        assert_eq!(audit.lock().unwrap().shutdowns, [task_key]);
        let decisions = fs::read_to_string(
            fixture
                .project_root
                .join("hcom-tasks/run-driver-test/decision.log"),
        )
        .unwrap();
        assert!(
            decisions.contains("clarification output path is empty or exceeds its bound"),
            "{decisions}"
        );
    }

    #[test]
    fn preexisting_clarification_artifact_terminalizes_instead_of_wedging_the_run() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "clarification-collision",
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [clarification_required("need-decision")],
            )],
            vec![Mutation::None],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(
            &mut supervisor,
            vec![fixture.task("clarification-collision", &[], 3)],
        );

        let collision = fixture
            .project_root
            .join("hcom-tasks/run-driver-test/clarification-collision/clarification/turn-1.md");
        fs::create_dir_all(collision.parent().unwrap()).unwrap();
        fs::write(&collision, "must not be reused\n").unwrap();

        let error = supervisor.poll_once().unwrap_err();
        assert!(error.to_string().contains("already exists"));
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert!(snapshot.pending_architect_action.is_none());
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("session runtime contract failed")
        );
        assert_eq!(
            fs::read_to_string(collision).unwrap(),
            "must not be reused\n"
        );
        supervisor.poll_once().unwrap();
        assert_eq!(supervisor.snapshot(), snapshot);
    }

    #[test]
    fn clarification_budget_exhaustion_requires_but_never_blocks_human_answer() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "human-answer",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [clarification_required("first-question")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperClarificationResume,
                    [blocked("second-question")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperClarificationResume,
                    [ready("ready-after-human")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [lgtm("lgtm-after-human")],
                ),
            ],
            vec![
                Mutation::None,
                Mutation::None,
                Mutation::Commit {
                    path: "src/human-answer.txt",
                    contents: "answered\n",
                },
                Mutation::None,
            ],
        );
        let mut task = fixture.task("human-answer", &[], 3);
        task.max_clarification_rounds = 1;
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![task]);

        supervisor.poll_once().unwrap();
        let first_snapshot = supervisor.snapshot();
        let first = first_snapshot.pending_architect_action.clone().unwrap();
        fs::write(&first.clarification_output_path, "Architect answer.\n").unwrap();
        supervisor
            .submit_clarification(
                first_snapshot.version,
                first.task_ordinal,
                &first.task_key,
                first.sequence,
                &first.developer_request_path,
                &first.clarification_output_path,
                false,
            )
            .unwrap();

        supervisor.poll_once().unwrap();
        let second_snapshot = supervisor.snapshot();
        let second = second_snapshot.pending_architect_action.clone().unwrap();
        assert_eq!(
            second.reason,
            crate::control_api::ArchitectActionReason::Blocker
        );
        assert!(second.human_decision_required);
        assert_eq!(second.clarification_rounds_used, 1);
        fs::write(
            &second.clarification_output_path,
            "Human decided to continue.\n",
        )
        .unwrap();
        assert!(
            supervisor
                .submit_clarification(
                    second_snapshot.version,
                    second.task_ordinal,
                    &second.task_key,
                    second.sequence,
                    &second.developer_request_path,
                    &second.clarification_output_path,
                    false,
                )
                .is_err()
        );
        assert_eq!(
            supervisor
                .snapshot()
                .pending_architect_action
                .as_ref()
                .map(|action| action.sequence),
            Some(second.sequence)
        );
        supervisor
            .submit_clarification(
                second_snapshot.version,
                second.task_ordinal,
                &second.task_key,
                second.sequence,
                &second.developer_request_path,
                &second.clarification_output_path,
                true,
            )
            .unwrap();

        let terminal = drive_terminal(&mut supervisor);
        assert_eq!(terminal.state, SessionState::Completed);
        let task = &terminal.tasks[0];
        assert_eq!(task.clarification_rounds_used, 1);
        assert_eq!(task.clarification_record_count, 2);
        let clarification_page = supervisor
            .clarification_page(&terminal.run_id, 0, "human-answer", 0, 8)
            .unwrap();
        assert!(!clarification_page.records[0].human_decision_confirmed);
        assert!(clarification_page.records[1].human_decision_confirmed);
    }

    #[test]
    fn clarification_during_correction_does_not_consume_a_review_round() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "correction-clarify",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("initial-ready")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes("changes")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [clarification_required("correction-question")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperClarificationResume,
                    [ready("corrected-ready")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::ReviewerRereview,
                    [lgtm("corrected-lgtm")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/correction.txt",
                    contents: "initial\n",
                },
                Mutation::None,
                Mutation::None,
                Mutation::Dirty {
                    path: "src/correction.txt",
                    contents: "corrected\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("correction-clarify", &[], 3)],
        );
        supervisor.poll_once().unwrap();
        supervisor.poll_once().unwrap();
        assert_eq!(supervisor.snapshot().tasks[0].review_round, 1);
        supervisor.poll_once().unwrap();
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.tasks[0].review_round, 1);
        let pending = snapshot.pending_architect_action.clone().unwrap();
        fs::write(
            &pending.clarification_output_path,
            "Apply the narrow correction.\n",
        )
        .unwrap();
        supervisor
            .submit_clarification(
                snapshot.version,
                pending.task_ordinal,
                &pending.task_key,
                pending.sequence,
                &pending.developer_request_path,
                &pending.clarification_output_path,
                false,
            )
            .unwrap();
        assert_eq!(supervisor.snapshot().tasks[0].review_round, 1);

        let terminal = drive_terminal(&mut supervisor);
        assert_eq!(terminal.state, SessionState::Completed);
        assert_eq!(terminal.tasks[0].review_round, 2);
        let audit = audit.lock().unwrap();
        let resume_prompt = audit
            .prompts
            .iter()
            .find(|(_, purpose, _)| *purpose == RuntimeTurnPurpose::DeveloperClarificationResume)
            .map(|(_, _, prompt)| prompt)
            .unwrap();
        assert!(resume_prompt.contains("Previously supplied Reviewer final messages"));
        assert!(resume_prompt.contains("human's decision resolving that authority conflict"));
        assert!(resume_prompt.contains("STATUS: CLARIFICATION_REQUIRED"));
    }
}

/// Real-Codex acceptance for the exec lane.
///
/// Opt-in; runs the native Codex selected from PATH against a disposable project with the
/// cheap test model. Never touches an existing user terminal or session.
///
///   cargo test --lib real_exec -- --ignored --nocapture --test-threads=1
#[cfg(test)]
mod real_exec_tests {
    use super::tests::real_support::*;
    use super::*;
    use crate::control_api::{ReviewerVerdict, TaskState};
    use crate::worker::profile::ArchitectAdapter;
    use crate::worker::runtime::RuntimeProvider;

    fn reviewer_paths(task: &crate::control_api::TaskStatusSnapshot) -> Vec<String> {
        task.reviewers
            .iter()
            .flat_map(|reviewer| reviewer.current_final_message_paths.clone())
            .collect()
    }

    fn joined_reviewer_verdict(
        task: &crate::control_api::TaskStatusSnapshot,
    ) -> Option<ReviewerVerdict> {
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

    fn real_claude_fixture(
        label: &str,
        developer: RuntimeProvider,
        reviewer: RuntimeProvider,
    ) -> RealFixture {
        RealFixture::new_with_workers(
            label,
            ArchitectAdapter::Codex,
            developer,
            reviewer,
            reviewer,
        )
    }

    fn process_birth(pid: u32) -> Option<u64> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let close = stat.rfind(')')?;
        stat[close + 1..].split_whitespace().nth(19)?.parse().ok()
    }

    fn read_process_identity(path: &Path) -> (u32, u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        loop {
            if let Ok(value) = std::fs::read_to_string(path) {
                let mut fields = value.split_ascii_whitespace();
                if let (Some(pid), Some(birth)) = (fields.next(), fields.next())
                    && let (Ok(pid), Ok(birth)) = (pid.parse(), birth.parse())
                {
                    return (pid, birth);
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "real Claude did not execute the escaped-descendant helper"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    fn wait_process_identity_gone(pid: u32, birth: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while process_birth(pid) == Some(birth) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_ne!(
            process_birth(pid),
            Some(birth),
            "escaped descendant {pid}/{birth} survived Guardian cleanup"
        );
    }

    fn write_escaped_descendant_helper(
        fixture: &RealFixture,
        label: &str,
        foreground_hang: bool,
    ) -> (PathBuf, PathBuf) {
        let helper = fixture.project_root.join(format!("{label}-helper.py"));
        let identity = fixture
            .project_root
            .join(format!("{label}-descendant.identity"));
        let foreground_hang = if foreground_hang { "True" } else { "False" };
        std::fs::write(
            &helper,
            format!(
                r#"#!/usr/bin/python3
import os
import sys
import time

identity_path = sys.argv[1]
first = os.fork()
if first == 0:
    os.setsid()
    second = os.fork()
    if second == 0:
        stat = open(f"/proc/{{os.getpid()}}/stat", encoding="ascii").read()
        birth = stat.rsplit(")", 1)[1].split()[19]
        with open(identity_path, "w", encoding="ascii") as output:
            output.write(f"{{os.getpid()}} {{birth}}\n")
            output.flush()
            os.fsync(output.fileno())
        time.sleep(300)
    os._exit(0)
os.waitpid(first, 0)
if {foreground_hang}:
    time.sleep(300)
"#,
            ),
        )
        .unwrap();
        (helper, identity)
    }

    fn write_dual_review_overlap_helper(fixture: &RealFixture) -> (PathBuf, PathBuf) {
        let helper = fixture.project_root.join("dual-review-overlap.py");
        let markers = fixture.project_root.join("dual-review-overlap");
        std::fs::create_dir(&markers).unwrap();
        std::fs::write(
            &helper,
            r#"#!/usr/bin/python3
import pathlib
import sys
import time

root = pathlib.Path(sys.argv[1])
reviewer = sys.argv[2]
generation = sys.argv[3]
if reviewer not in {"reviewer1", "reviewer2"}:
    raise SystemExit("reviewer identity must be reviewer1 or reviewer2")
if generation not in {"1", "2"}:
    raise SystemExit("review generation must be 1 or 2")
marker = root / f"generation-{generation}-{reviewer}"
marker.write_text("started\n", encoding="ascii")
peer = "reviewer2" if reviewer == "reviewer1" else "reviewer1"
peer_marker = root / f"generation-{generation}-{peer}"
deadline = time.monotonic() + 120
while not peer_marker.exists():
    if time.monotonic() >= deadline:
        raise SystemExit("peer Reviewer did not overlap this turn")
    time.sleep(0.05)
"#,
        )
        .unwrap();
        (helper, markers)
    }

    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_single_task_developer_then_reviewer_reaches_lgtm() {
        let fixture = RealFixture::new("fib");
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "fib",
            "Add a fibonacci function",
            "Create fib.py in the repository root containing a function fib(n) that returns the \
             nth Fibonacci number (fib(0)=0, fib(1)=1). Commit it with git. Acceptance: fib.py \
             exists and is committed. Check with `python3 -c \"import fib; print(fib.fib(10))\"`. \
             Do not push.",
            2,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert!(fixture.repository.join("fib.py").is_file());
        fixture.assert_artifacts("fib", &["developer", "reviewer"]);
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    /// Gate 1: one run, two ordered tasks — the first goes through a real
    /// REQUEST_CHANGES with an exact developer resume and an exact reviewer
    /// re-review, the second is approved on its first review, and all four
    /// role sessions are fresh.
    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_gate_one_review_loop_then_direct_approval_in_one_run() {
        let fixture = RealFixture::new("gate1");
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "staged",
                "Controlled add() correction and re-review probe",
                "This is a controlled lifecycle E2E. Follow the instruction for your current \
                 hcom turn purpose exactly.\n\n\
                 InitialDevelopment: create and commit ONLY calc.py with add(a, b) returning \
                 a + b. Deliberately do not create test_calc.py yet, and report that omission.\n\n\
                 InitialReview: test_calc.py being absent is the deliberately seeded blocking \
                 defect. You MUST return VERDICT: REQUEST_CHANGES and MUST NOT return LGTM, \
                 even though calc.py itself is correct. Require a test_calc.py that asserts \
                 add(2, 3) == 5.\n\n\
                 DeveloperCorrection: create and commit test_calc.py with that assertion and \
                 run `python3 -B test_calc.py` so verification leaves no bytecode artifacts.\n\n\
                 ReReview: independently run `python3 -B test_calc.py`. Return LGTM only when \
                 both calc.py and the passing test exist and are committed. Do not push.",
                3,
            ),
            fixture.task(
                "direct",
                "Add a direct-approval marker",
                "Create direct.txt in the repository root containing exactly DIRECT-READY followed \
                 by a newline, commit it, and check with `test \"$(cat direct.txt)\" = \
                 DIRECT-READY`. This check must not create any generated files. Do not push.",
                3,
            ),
        ];
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert_eq!(snapshot.tasks.len(), 2);
        for task in &snapshot.tasks {
            assert_eq!(
                task.state,
                TaskState::Lgtm,
                "task {} ended as {:?}; exhaustion is not success",
                task.task_key,
                task.state
            );
        }
        assert!(
            snapshot.tasks[0].review_round >= 2,
            "task 1 must have been rejected once first, got {}",
            snapshot.tasks[0].review_round
        );
        assert_eq!(
            snapshot.tasks[1].review_round, 1,
            "task 2 must be approved on its first review"
        );

        // Each role resumed its own exact native session within its task, and
        // no session was carried across the task boundary: four fresh ones.
        let mut first_of_each = Vec::new();
        for (task, role) in [
            ("staged", "developer"),
            ("staged", "reviewer"),
            ("direct", "developer"),
            ("direct", "reviewer"),
        ] {
            let ids = fixture.thread_ids(task, role);
            assert!(!ids.is_empty(), "{task}/{role} produced no evidence");
            assert!(
                ids.windows(2).all(|w| w[0] == w[1]),
                "{task}/{role} changed native session: {ids:?}"
            );
            first_of_each.push(ids[0].clone());
        }
        assert!(
            fixture.thread_ids("staged", "developer").len() >= 2,
            "task 1 developer did not run a correction turn"
        );
        let unique: std::collections::BTreeSet<_> = first_of_each.iter().collect();
        assert_eq!(
            unique.len(),
            4,
            "all four role sessions must be fresh: {first_of_each:?}"
        );

        assert!(fixture.repository.join("test_calc.py").is_file());
        assert!(fixture.repository.join("direct.txt").is_file());
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    /// Rejection through the dual minimum max_review_rounds=7 must be reported
    /// as review_exhausted, close its runtime, and advance to the next task
    /// without pretending the rejection was an approval.
    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_review_exhausted_advances_to_the_next_task() {
        let fixture = RealFixture::new("exhaustion");
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "exhausted",
                "Controlled review exhaustion probe",
                "This is a controlled lifecycle E2E with max_review_rounds=7.\n\n\
                 InitialDevelopment: create and commit ONLY incomplete.py defining value = 1. \
                 Deliberately do not create required_test.py, and report that omission.\n\n\
                 InitialReview: required_test.py being absent is the deliberately seeded blocking \
                 defect. You MUST return VERDICT: REQUEST_CHANGES and MUST NOT return LGTM. \
                 Require required_test.py to assert incomplete.value == 1. On every \
                 DeveloperCorrection, preserve the deliberately incomplete candidate and amend \
                 the same commit without adding required_test.py. At the seventh joined rejection, \
                 hcom should mark this task review_exhausted. Do not push.",
                7,
            ),
            fixture.task(
                "after-exhaustion",
                "Prove automatic advance after exhaustion",
                "Create recovery.txt in the repository root containing exactly RECOVERED followed \
                 by a newline, commit it, and check with `test \"$(cat recovery.txt)\" = \
                 RECOVERED`. This check must not create any generated files. Do not push.",
                2,
            ),
        ];
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert_eq!(snapshot.tasks.len(), 2);
        let exhausted = &snapshot.tasks[0];
        assert_eq!(exhausted.state, TaskState::ReviewExhausted);
        assert_eq!(exhausted.review_round, 7);
        assert_eq!(
            joined_reviewer_verdict(exhausted),
            Some(ReviewerVerdict::RequestChanges)
        );
        let developer_ids = fixture.thread_ids("exhausted", "developer");
        assert!(developer_ids.len() >= 7);
        assert!(developer_ids.windows(2).all(|ids| ids[0] == ids[1]));
        let reviewer_ids = fixture.thread_ids("exhausted", "reviewer");
        assert!(reviewer_ids.len() >= 7);
        assert!(reviewer_ids.windows(2).all(|ids| ids[0] == ids[1]));
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert!(fixture.repository.join("recovery.txt").is_file());
        for path in reviewer_paths(exhausted) {
            assert!(Path::new(&path).is_file(), "missing reviewer final: {path}");
        }
        fixture.assert_artifacts("exhausted", &["developer", "reviewer"]);
        fixture.assert_artifacts("after-exhaustion", &["developer", "reviewer"]);
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    /// Kill only the live Codex process whose final target belongs to this
    /// disposable fixture. The supervisor must terminalize as needs_human,
    /// route no partial final, and reap every descendant.
    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires native codex, auth, and network"]
    fn real_killed_developer_becomes_needs_human_without_routing_partial_final() {
        let fixture = RealFixture::new("killed-worker");
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "killed-worker",
            "Worker abnormal-exit probe",
            "Create killed_worker.py in the repository root defining reached = True, commit it, \
             and do not push.",
            2,
        );
        fixture.start(&mut supervisor, vec![task]);

        let discovery_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let worker_pid = loop {
            let workers = fixture.live_codex_worker_pids();
            if workers.len() == 1 {
                break workers[0];
            }
            assert!(
                workers.is_empty(),
                "fixture selected multiple live Codex workers: {workers:?}"
            );
            assert!(
                std::time::Instant::now() < discovery_deadline,
                "fixture's live Codex worker was not discoverable"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(worker_pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        )
        .expect("kill exact disposable Codex worker");

        let snapshot = fixture.drive(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert!(
            snapshot
                .terminal_detail
                .as_deref()
                .is_some_and(|detail| detail.starts_with("worker runtime process failed:")),
            "unexpected terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert!(snapshot.tasks[0].latest_developer_final_path.is_none());
        assert!(reviewer_paths(&snapshot.tasks[0]).is_empty());
        assert_eq!(joined_reviewer_verdict(&snapshot.tasks[0]), None);
        assert!(
            fixture.thread_ids("killed-worker", "reviewer").is_empty(),
            "reviewer must not start after a killed developer"
        );
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the failed run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    /// Default Codex Developer + Claude Reviewer: the Reviewer rejects once,
    /// then exact-resumes for the re-review after the Codex correction.
    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLIs, auth, and network"]
    fn real_claude_default_mixed_review_loop_and_reviewer_exact_resume() {
        let fixture = real_claude_fixture(
            "claude-default-mixed",
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        fixture.commit_fixture_instruction(
            "CLAUDE.md",
            "For task selector mixed-review, follow the current hcom turn purpose exactly. \
             On InitialReview, the first line MUST be VERDICT: REQUEST_CHANGES because the \
             missing test is a controlled transport defect, regardless of any other quality \
             judgment. On ReviewerRereview, run the requested check and return VERDICT: LGTM \
             only if it passes and the exact candidate commit remains signed off.\n",
        );
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "mixed-review",
            "Controlled mixed-provider review correction",
            "This is a controlled lifecycle E2E. Follow the instruction for the current hcom \
             turn purpose exactly.\n\n\
             InitialDevelopment: create calc.py with add(a, b) returning a + b. Create exactly \
             one signed-off commit containing calc.py. Deliberately omit test_calc.py and report \
             the omission.\n\n\
             InitialReview: test_calc.py is a deliberately seeded blocking defect. Return \
             VERDICT: REQUEST_CHANGES and require a test asserting add(2, 3) == 5.\n\n\
             DeveloperCorrection: add test_calc.py with that assertion, run \
             `python3 -B test_calc.py`, and amend the existing task commit with sign-off.\n\n\
             ReReview: independently run `python3 -B test_calc.py`; return VERDICT: LGTM only \
             if it passes and the exact single candidate commit is signed off. Do not push.",
            3,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert_eq!(snapshot.tasks[0].review_round, 2);
        let developer = fixture.native_session_ids("mixed-review", "developer");
        let reviewer = fixture.native_session_ids("mixed-review", "reviewer");
        assert!(developer.len() >= 2 && developer.windows(2).all(|ids| ids[0] == ids[1]));
        assert!(reviewer.len() >= 2 && reviewer.windows(2).all(|ids| ids[0] == ids[1]));
        assert_ne!(developer[0], reviewer[0]);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native Codex and Claude CLIs, auth, and network"]
    fn real_dual_review_mixed_provider_strict_generation_and_overlap() {
        let fixture = RealFixture::new_with_workers(
            "dual-review-mixed-strict",
            ArchitectAdapter::Codex,
            RuntimeProvider::CodexExec,
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        let (overlap_helper, overlap_markers) = write_dual_review_overlap_helper(&fixture);
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "dual-review-strict",
            "Concurrent strict-generation mixed-provider review",
            &format!(
                "This is a controlled dual-review E2E. Follow the contract for your current \
                 hcom turn purpose exactly.\n\n\
                 InitialDevelopment: create calc.py with add(a, b) returning a + b. Create \
                 exactly one signed-off task commit containing calc.py. Deliberately omit \
                 test_calc.py and disclose that omission.\n\n\
                 Every Reviewer turn: before deciding a verdict, run `python3 '{}' '{}' \
                 <your exact reviewer identity> <the exact review generation from your prompt>`. \
                 Use reviewer1 or reviewer2 and generation 1 or 2 exactly. The command must \
                 finish successfully; it proves both Reviewer processes overlap.\n\n\
                 InitialReview: Reviewer1 must return VERDICT: LGTM after the overlap probe. \
                 Reviewer2 must return VERDICT: REQUEST_CHANGES and require test_calc.py to \
                 assert add(2, 3) == 5. Do not infer or read the peer response.\n\n\
                 DeveloperCorrection: read and synthesize both ordered Reviewer responses, add \
                 test_calc.py with that assertion, run `python3 -B test_calc.py`, and amend the \
                 existing signed-off task commit.\n\n\
                 ReviewerRereview: run the generation-2 overlap probe, independently run \
                 `python3 -B test_calc.py`, and return VERDICT: LGTM only if it passes and the \
                 exact candidate remains one signed-off commit. Do not push.",
                overlap_helper.display(),
                overlap_markers.display(),
            ),
            3,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert_eq!(snapshot.tasks[0].review_round, 2);
        assert_eq!(snapshot.tasks[0].review_generation, 2);
        assert!(snapshot.tasks[0].reviewers.iter().all(|reviewer| {
            reviewer.current_generation == Some(2)
                && reviewer.current_verdict == Some(ReviewerVerdict::Lgtm)
        }));
        for generation in [1, 2] {
            for reviewer in ["reviewer1", "reviewer2"] {
                assert!(
                    overlap_markers
                        .join(format!("generation-{generation}-{reviewer}"))
                        .is_file(),
                    "missing overlap evidence for generation {generation} {reviewer}"
                );
            }
        }
        let reviewer1 = fixture.native_session_ids("dual-review-strict", "reviewer/reviewer1");
        let reviewer2 = fixture.native_session_ids("dual-review-strict", "reviewer/reviewer2");
        assert!(reviewer1.len() >= 2 && reviewer1.windows(2).all(|ids| ids[0] == ids[1]));
        assert!(reviewer2.len() >= 2 && reviewer2.windows(2).all(|ids| ids[0] == ids[1]));
        assert_ne!(reviewer1[0], reviewer2[0]);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native Codex and Claude CLIs, auth, and network"]
    fn real_dual_review_mixed_provider_minimum_exhaustion_advances() {
        let fixture = RealFixture::new_with_workers(
            "dual-review-mixed-exhaustion",
            ArchitectAdapter::Codex,
            RuntimeProvider::CodexExec,
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "dual-exhausted",
                "Dual review minimum-round exhaustion",
                "InitialDevelopment: create incomplete.py containing value = 1 in exactly one \
                 signed-off task commit and deliberately omit required_test.py.\n\n\
                 InitialReview: both Reviewer1 and Reviewer2 must independently return \
                 VERDICT: REQUEST_CHANGES because required_test.py is absent. On every \
                 DeveloperCorrection, preserve the deliberately incomplete candidate and amend \
                 the same commit without adding required_test.py. The synchronized review budget \
                 is seven, so the seventh joined rejection exhausts review. Do not push.",
                7,
            ),
            fixture.task(
                "after-dual-exhaustion",
                "Advance after dual review exhaustion",
                "Create recovered.txt containing exactly RECOVERED followed by a newline in one \
                 signed-off task commit. Both Reviewers return VERDICT: LGTM only when the exact \
                 content and sign-off are correct. Do not push.",
                2,
            ),
        ];
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(snapshot.tasks[0].review_round, 7);
        assert_eq!(snapshot.tasks[0].review_generation, 7);
        assert!(
            snapshot.tasks[0].reviewers.iter().all(|reviewer| {
                reviewer.current_verdict == Some(ReviewerVerdict::RequestChanges)
            })
        );
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native Codex and Claude CLIs, auth, and network"]
    fn real_dual_review_claude_reviewer_exit_cancels_codex_peer_without_residuals() {
        let fixture = RealFixture::new_with_workers(
            "dual-review-reviewer-exit",
            ArchitectAdapter::Codex,
            RuntimeProvider::CodexExec,
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "dual-reviewer-exit",
            "Mixed-provider Reviewer abnormal exit",
            "InitialDevelopment: create candidate.py containing ready = True in exactly one \
             signed-off task commit.\n\n\
             InitialReview: both Reviewers must first use the Bash tool to run `sleep 300`; do \
             not return a verdict before it exits. This is a controlled abnormal-exit probe. \
             Do not push.",
            2,
        );
        fixture.start(&mut supervisor, vec![task]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        let claude_reviewer = loop {
            supervisor.poll_once().unwrap();
            let reviewing = supervisor.snapshot();
            if reviewing.active_workers.len() == 2
                && reviewing
                    .active_workers
                    .iter()
                    .all(|worker| worker.worker_lane.role() == WorkerRole::Reviewer)
            {
                let workers = fixture.live_claude_worker_pids();
                if workers.len() == 1 {
                    break workers[0];
                }
                assert!(
                    workers.is_empty(),
                    "selected multiple fixture-owned Claude workers: {workers:?}"
                );
            }
            assert!(
                std::time::Instant::now() < deadline,
                "dual Reviewer processes did not become concurrently active"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(claude_reviewer as i32),
            nix::sys::signal::Signal::SIGKILL,
        )
        .unwrap();
        let snapshot = fixture.drive(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].review_round, 0);
        assert_eq!(snapshot.tasks[0].review_generation, 1);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native Codex and Claude CLIs, auth, and network"]
    fn real_dual_review_parent_stop_cleans_both_reviewer_trees() {
        let fixture = RealFixture::new_with_workers(
            "dual-review-parent-stop",
            ArchitectAdapter::Codex,
            RuntimeProvider::CodexExec,
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "dual-parent-stop",
            "Dual Reviewer foreground-parent stop",
            "InitialDevelopment: create candidate.py containing ready = True in exactly one \
             signed-off task commit.\n\n\
             InitialReview: both Reviewers must first use the Bash tool to run `sleep 300`; do \
             not return a verdict before it exits. This is a controlled foreground-parent stop \
             probe. Do not push.",
            2,
        );
        fixture.start(&mut supervisor, vec![task]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        loop {
            supervisor.poll_once().unwrap();
            let reviewing = supervisor.snapshot();
            if reviewing.active_workers.len() == 2
                && reviewing
                    .active_workers
                    .iter()
                    .all(|worker| worker.worker_lane.role() == WorkerRole::Reviewer)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "dual Reviewer processes did not become concurrently active"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        supervisor.shutdown().unwrap();
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::Canceled);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("foreground Architect parent stopped")
        );
        assert_eq!(snapshot.tasks[0].review_round, 0);
        assert_eq!(snapshot.tasks[0].review_generation, 1);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    /// Claude Developer + Codex Reviewer: the correction must resume the exact
    /// Claude session rather than silently starting a new conversation.
    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLIs, auth, and network"]
    fn real_claude_developer_codex_reviewer_exact_resume() {
        let fixture = real_claude_fixture(
            "claude-developer-resume",
            RuntimeProvider::ClaudeExec,
            RuntimeProvider::CodexExec,
        );
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "claude-developer",
            "Controlled Claude Developer correction",
            "Follow the instruction for the current hcom turn purpose exactly.\n\n\
             InitialDevelopment: create value.py containing value = 7, make exactly one \
             signed-off task commit, deliberately omit test_value.py, and report the omission.\n\n\
             InitialReview: the omitted test is deliberately blocking. Return \
             VERDICT: REQUEST_CHANGES and require test_value.py to assert value.value == 7.\n\n\
             DeveloperCorrection: add test_value.py, run `python3 -B test_value.py`, and amend \
             the existing signed-off candidate commit.\n\n\
             ReReview: return VERDICT: LGTM only after the check passes and the exact candidate \
             commit remains signed off. Do not push.",
            3,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        let developer = fixture.native_session_ids("claude-developer", "developer");
        assert!(developer.len() >= 2);
        assert!(developer.windows(2).all(|ids| ids[0] == ids[1]));
        assert!(fixture.stray_worker_pids().is_empty());
    }

    /// Claude Developer + Claude Reviewer across two tasks proves each role
    /// resumes only within one task and receives a fresh UUID for the next.
    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
    fn real_claude_pair_uses_fresh_cross_task_sessions() {
        let fixture = real_claude_fixture(
            "claude-pair-fresh",
            RuntimeProvider::ClaudeExec,
            RuntimeProvider::ClaudeExec,
        );
        let mut supervisor = fixture.supervisor();
        let tasks = ["one", "two"]
            .into_iter()
            .map(|key| {
                fixture.task(
                    key,
                    &format!("Create marker {key}"),
                    &format!(
                        "Create {key}.txt containing exactly {key}-READY followed by a newline. \
                         Create exactly one signed-off task commit, verify the exact content with \
                         a side-effect-free shell check, and do not push. Reviewer: return \
                         VERDICT: LGTM when the marker and signed-off commit are correct."
                    ),
                    2,
                )
            })
            .collect();
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.state == TaskState::Lgtm)
        );
        for role in ["developer", "reviewer"] {
            let first = fixture.native_session_ids("one", role);
            let second = fixture.native_session_ids("two", role);
            assert!(!first.is_empty() && !second.is_empty());
            assert_ne!(first[0], second[0], "{role} reused a cross-task session");
        }
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLIs, auth, and network"]
    fn real_claude_review_exhaustion_advances_to_next_task() {
        let fixture = real_claude_fixture(
            "claude-exhaustion",
            RuntimeProvider::CodexExec,
            RuntimeProvider::ClaudeExec,
        );
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "claude-exhausted",
                "Controlled Claude review exhaustion",
                "InitialDevelopment: create incomplete.py containing value = 1 in exactly one \
                 signed-off commit, and deliberately omit required_test.py.\n\n\
                 InitialReview: the omitted required_test.py is deliberately blocking. You MUST \
                 return VERDICT: REQUEST_CHANGES and MUST NOT return LGTM. On every \
                 DeveloperCorrection, preserve the deliberately incomplete candidate and amend \
                 the same commit without adding required_test.py. The configured review budget is \
                 seven. Do not push.",
                7,
            ),
            fixture.task(
                "after-claude-exhaustion",
                "Advance after Claude review exhaustion",
                "Create recovered.txt containing exactly RECOVERED followed by a newline in \
                 exactly one signed-off commit. Verify it with a side-effect-free shell check. \
                 Reviewer: return VERDICT: LGTM when correct. Do not push.",
                2,
            ),
        ];
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(snapshot.tasks[0].review_round, 7);
        assert_eq!(
            joined_reviewer_verdict(&snapshot.tasks[0]),
            Some(ReviewerVerdict::RequestChanges)
        );
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
    fn real_killed_claude_developer_never_routes_a_partial_final() {
        let fixture = real_claude_fixture(
            "killed-claude-worker",
            RuntimeProvider::ClaudeExec,
            RuntimeProvider::CodexExec,
        );
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "killed-claude-worker",
            "Exact disposable Claude abnormal-exit probe",
            "Create killed_claude.py containing reached = True in exactly one signed-off commit. \
             Do not push.",
            2,
        );
        fixture.start(&mut supervisor, vec![task]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let worker = loop {
            let workers = fixture.live_claude_worker_pids();
            if workers.len() == 1 {
                break workers[0];
            }
            assert!(
                workers.is_empty(),
                "selected multiple Claude workers: {workers:?}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "fixture's exact Claude worker was not discoverable"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(worker as i32),
            nix::sys::signal::Signal::SIGKILL,
        )
        .unwrap();
        let snapshot = fixture.drive(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(snapshot.tasks[0].state, TaskState::NeedsHuman);
        assert!(snapshot.tasks[0].latest_developer_final_path.is_none());
        assert!(reviewer_paths(&snapshot.tasks[0]).is_empty());
        assert!(
            fixture
                .native_session_ids("killed-claude-worker", "reviewer")
                .is_empty()
        );
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
    fn real_claude_nested_double_fork_is_reaped_and_success_is_not_routed() {
        let fixture = real_claude_fixture(
            "claude-nested-descendant",
            RuntimeProvider::ClaudeExec,
            RuntimeProvider::CodexExec,
        );
        let (helper, identity_path) = write_escaped_descendant_helper(&fixture, "nested", false);
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "nested-descendant",
            "Nested setsid/double-fork lifecycle probe",
            &format!(
                "Before any other task action, use the Bash tool to run exactly:\n\
                 `python3 '{}' '{}'`\n\
                 Wait until that command exits and the identity file exists. Then return a \
                 normal STATUS: READY final. The helper intentionally creates a setsid + \
                 double-fork descendant, so hcom must reject the success-shaped turn after \
                 Guardian cleanup. Do not push.",
                helper.display(),
                identity_path.display()
            ),
            2,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        let identity = read_process_identity(&identity_path);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert!(snapshot.tasks[0].latest_developer_final_path.is_none());
        assert!(
            snapshot
                .terminal_detail
                .as_deref()
                .is_some_and(|detail| detail.contains("residual descendants")),
            "{:?}",
            snapshot.terminal_detail
        );
        wait_process_identity_gone(identity.0, identity.1);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    #[test]
    #[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
    fn real_claude_cancel_reaps_escaped_descendants() {
        let fixture = real_claude_fixture(
            "claude-cancel-descendant",
            RuntimeProvider::ClaudeExec,
            RuntimeProvider::CodexExec,
        );
        let (helper, identity_path) = write_escaped_descendant_helper(&fixture, "cancel", true);
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "cancel-descendant",
            "Cancel an active Claude tool tree",
            &format!(
                "Your first action must be to use the Bash tool to run exactly:\n\
                 `python3 '{}' '{}'`\n\
                 The command intentionally remains active; do not substitute another command.",
                helper.display(),
                identity_path.display()
            ),
            2,
        );
        fixture.start(&mut supervisor, vec![task]);
        let identity = read_process_identity(&identity_path);
        let version = supervisor.snapshot().version;
        supervisor
            .cancel(version, "real Claude cancellation lifecycle probe")
            .unwrap();
        let snapshot = fixture.drive(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Canceled);
        wait_process_identity_gone(identity.0, identity.1);
        assert!(fixture.stray_worker_pids().is_empty());
    }

    /// End-to-end acceptance on a real Rust project: the developer writes and
    /// commits a hello-world crate, the reviewer independently judges it, and
    /// the run reaches LGTM with durable evidence on disk.
    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_rust_hello_world_task_reaches_lgtm_with_evidence() {
        let fixture = RealFixture::new("hello");
        let mut supervisor = fixture.supervisor();
        let task = fixture.task(
            "hello",
            "Create a hello world Rust binary",
            "In the repository root create a Cargo binary crate: Cargo.toml naming the package \
             `hello` with edition 2021, and src/main.rs whose main function prints exactly \
             `Hello, world!`. Verify it with `cargo run`, then commit both files with git. Do not \
             push.",
            2,
        );
        let snapshot = fixture.run(&mut supervisor, vec![task]);
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert_eq!(snapshot.tasks[0].state, TaskState::Lgtm);
        assert!(fixture.repository.join("Cargo.toml").is_file());
        assert!(fixture.repository.join("src/main.rs").is_file());
        let main = std::fs::read_to_string(fixture.repository.join("src/main.rs")).unwrap();
        assert!(main.contains("Hello, world!"), "{main}");
        fixture.assert_artifacts("hello", &["developer", "reviewer"]);
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_two_task_run_advances_automatically() {
        let fixture = RealFixture::new("two");
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "greet",
                "Add a greeting module",
                "Create greet.py in the repository root with a function greet(name) returning the \
                 string \"hello <name>\". Commit it and check with `python3 -c \"import greet\"`. \
                 Do not push.",
                2,
            ),
            fixture.task(
                "square",
                "Add a square module",
                "Create square.py in the repository root with a function square(n) returning n*n. \
                 Commit it and check with `python3 -c \"import square\"`. Do not push.",
                2,
            ),
        ];
        let snapshot = fixture.run(&mut supervisor, tasks);
        assert_eq!(
            snapshot.state,
            SessionState::Completed,
            "terminal detail: {:?}",
            snapshot.terminal_detail
        );
        assert_eq!(snapshot.tasks.len(), 2);
        for task in &snapshot.tasks {
            assert_eq!(
                task.state,
                TaskState::Lgtm,
                "task {} ended as {:?}; exhaustion is not success",
                task.task_key,
                task.state
            );
        }
        // Each task ran in its own fresh native sessions.
        let first = fixture.thread_ids("greet", "developer");
        let second = fixture.thread_ids("square", "developer");
        assert!(!first.is_empty() && !second.is_empty());
        assert_ne!(first[0], second[0], "tasks shared a developer session");
        assert!(fixture.repository.join("greet.py").is_file());
        assert!(fixture.repository.join("square.py").is_file());
        fixture.assert_artifacts("greet", &["developer", "reviewer"]);
        fixture.assert_artifacts("square", &["developer", "reviewer"]);
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }
}
