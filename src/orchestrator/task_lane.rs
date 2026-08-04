//! Effect driver for the `hcom arch codex` Codex exec worker task lane.
//!
//! The driver is the only owner of Git, filesystem, environment, and runtime
//! I/O. Scheduling decisions remain in [`SupervisorCore`].

use super::core::{
    DriverFailure, DriverFailureClass, SupervisorCore, SupervisorEffect, SupervisorEvent,
};
use super::workspace::{ProjectTasksWorkspace, TasksWorkspace};
use super::{SessionRuntimeSources, SessionStartup, ensure_private_directory, sha256_hex};
use crate::control_api::{SessionState, SessionStatusSnapshot, TaskDraft, TaskState, WorkerRole};
use crate::worker::environment::{
    EnvironmentPolicy, ExecutionEnvironmentLease, MaterializedWorkerEnvironment,
};
use crate::worker::exec_runtime::{ExecRuntimeConfig, ExecTaskPaths, ExecTaskWorkerRuntime};
use crate::worker::guardian::{CleanupRegistryInterlock, GuardianCleanupRegistry};
use crate::worker::profile::ArchitectAdapter;
use crate::worker::role_router::{ProviderRuntimeSlot, RoleRoutedTaskWorkerRuntime};
use crate::worker::runtime::{
    OutcomeContract, RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeErrorCode,
    RuntimeFailureClass, RuntimeProfile, RuntimeProvider, RuntimeSessionKey, RuntimeTurnKey,
    RuntimeTurnPoll, RuntimeTurnPurpose, RuntimeTurnSpec, SanitizedRuntimeFailure,
    TaskWorkerProfiles, TaskWorkerRuntime,
};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const TURN_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

trait RuntimeFactory: Send {
    fn contract(&self, profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity;

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError>;
}

struct ProductionRuntimeFactory;

impl ProductionRuntimeFactory {
    fn new() -> Self {
        Self
    }
}

impl RuntimeFactory for ProductionRuntimeFactory {
    fn contract(&self, profiles: &TaskWorkerProfiles) -> RuntimeContractIdentity {
        profiles.contract_identity()
    }

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError> {
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
            cleanup_registry: _,
            profiles,
        } = request;
        let uses_codex = [profiles.developer.provider, profiles.reviewer.provider]
            .contains(&RuntimeProvider::CodexExec);
        let uses_claude = [profiles.developer.provider, profiles.reviewer.provider]
            .contains(&RuntimeProvider::ClaudeExec);

        let codex_runtime = if uses_codex {
            Some(ExecTaskWorkerRuntime::open(ExecRuntimeConfig {
                codex: PathBuf::from("codex"),
                repository_root,
                paths: ExecTaskPaths {
                    runtime: paths.runtime,
                },
                environment,
                lease,
                artifact_root_path: artifact_root,
                run_id,
                task_id: task_key,
            })?)
        } else {
            None
        };

        // Preserve the exact direct Codex child for the released pure-Codex
        // lane. Mixed bindings use the provider-neutral key router; its Claude
        // slot remains deliberately unavailable until CLAUDE-03 supplies the
        // native stream-json transport.
        if !uses_claude {
            return Ok(Box::new(
                codex_runtime.expect("a pure Codex binding creates the Codex runtime"),
            ));
        }
        let mut slots = Vec::new();
        if let Some(runtime) = codex_runtime {
            slots.push(ProviderRuntimeSlot::available(
                RuntimeProvider::CodexExec,
                Box::new(runtime),
            )?);
        }
        slots.push(ProviderRuntimeSlot::unavailable(
            RuntimeProvider::ClaudeExec,
            "Claude task worker runtime is not implemented; CLAUDE-03 is required",
        ));
        Ok(Box::new(RoleRoutedTaskWorkerRuntime::new(
            &profiles, slots,
        )?))
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
    runtime: PathBuf,
}

impl TaskRuntimePaths {
    fn create(
        run_root: &Path,
        task_ordinal: usize,
        task_key: &str,
        _repository_root: &Path,
    ) -> Result<(TempDir, Self)> {
        let workers = run_root.join("exec-workers");
        ensure_private_directory(&workers)?;
        let root = tempfile::Builder::new()
            .prefix(&format!("task-{task_ordinal}-{task_key}."))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(&workers)
            .context("failed to create task-private exec worker root")?;
        let root_path = fs::canonicalize(root.path())?;
        let paths = Self {
            runtime: root_path.join("run"),
        };
        fs::create_dir(&paths.runtime).with_context(|| {
            format!(
                "failed to create task-private exec worker directory {}",
                paths.runtime.display()
            )
        })?;
        fs::set_permissions(&paths.runtime, fs::Permissions::from_mode(0o700))?;
        Ok((root, paths))
    }
}

struct OpenTaskRuntime {
    task_ordinal: usize,
    _root: TempDir,
    runtime: Box<dyn TaskWorkerRuntime>,
    sessions: BTreeMap<RuntimeSessionKey, LocalSession>,
}

#[derive(Clone, Copy)]
struct LocalSession {
    role: WorkerRole,
    key: RuntimeSessionKey,
}

#[derive(Clone)]
struct ActiveTurn {
    task_ordinal: usize,
    role: WorkerRole,
    logical_session: RuntimeSessionKey,
    logical_turn: RuntimeTurnKey,
    local_turn: RuntimeTurnKey,
    completion_token: String,
}

pub(crate) struct TaskLaneSupervisor {
    startup: SessionStartup,
    epoch: String,
    core: SupervisorCore,
    run_root: PathBuf,
    sources: SessionRuntimeSources,
    profiles: TaskWorkerProfiles,
    developer_adapter: String,
    reviewer_adapter: String,
    factory: Box<dyn RuntimeFactory>,
    task_runtime: Option<OpenTaskRuntime>,
    active: Option<ActiveTurn>,
    next_session: u64,
    next_turn: u64,
    project_tasks_workspace: Option<ProjectTasksWorkspace>,
    tasks_workspace: Option<TasksWorkspace>,
    cleanup_registry: GuardianCleanupRegistry,
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
            .ok_or_else(|| anyhow!("the Codex exec worker lane requires loaded profiles"))?;
        let profiles = TaskWorkerProfiles::from_session_profiles(&session_profiles)
            .map_err(|error| anyhow!(error.detail))?;
        profiles.validate().map_err(|error| anyhow!(error.detail))?;
        let contract = factory.contract(&profiles);
        contract.validate().map_err(|error| anyhow!(error.detail))?;
        let profile_hash = sha256_hex(&serde_json::to_vec(&(
            "hcom-codex-exec-session-binding-v2",
            session_profiles.canonical_hash(),
            profiles.canonical_hash(),
            contract.canonical_hash(),
            &sources.architect_additional_directories,
        ))?);
        let startup = SessionStartup {
            run_id: run_id.clone(),
            project_root: project_root.clone(),
        };
        let core = SupervisorCore::new(run_id, project_root, profile_hash)
            .map_err(|error| anyhow!(error.to_string()))?;
        let developer_adapter = profiles.developer.provider.as_str().into();
        let reviewer_adapter = profiles.reviewer.provider.as_str().into();
        Ok(Self {
            startup,
            epoch: format!("exec-supervisor-{}", Uuid::new_v4()),
            core,
            run_root,
            sources,
            profiles,
            developer_adapter,
            reviewer_adapter,
            factory,
            task_runtime: None,
            active: None,
            next_session: 1,
            next_turn: 1,
            project_tasks_workspace: None,
            tasks_workspace: None,
            cleanup_registry: GuardianCleanupRegistry::default(),
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
        if self.task_runtime.is_some() || self.active.is_some() {
            bail!("terminal run retained a live task runtime");
        }
        self.cleanup_registry.ensure_available()?;
        crate::worker::validation::validate_opaque_id("run id", &run_id)?;

        let next_core = self
            .core
            .next_run(run_id.clone())
            .map_err(|error| anyhow!(error.to_string()))?;
        self.startup.run_id = run_id;
        self.epoch = format!("exec-supervisor-{}", Uuid::new_v4());
        self.core = next_core;
        self.next_session = 1;
        self.next_turn = 1;
        self.tasks_workspace = None;
        Ok(())
    }

    pub(crate) fn replace_plan(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapter: &str,
        tasks: Vec<TaskDraft>,
    ) -> Result<(u64, String)> {
        if developer_adapter != self.developer_adapter || reviewer_adapter != self.reviewer_adapter
        {
            bail!("task plan adapters differ from the Codex exec worker session binding");
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

        // A task's repository_root is just the source directory handed to its
        // workers: hcom neither opens it, locks it, nor inspects its Git state.
        for task in &tasks {
            let root = PathBuf::from(&task.repository_root);
            if !root.is_dir() {
                bail!(
                    "task {} names a source directory that does not exist: {}",
                    task.task_key,
                    root.display()
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
        let plan_hash = self.core.expected_plan_hash(plan_version, &tasks);
        let event = SupervisorEvent::PlanBound {
            expected_version: expected_session_version,
            plan_version,
            plan_hash: plan_hash.clone(),
            tasks,
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
        self.core.snapshot()
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
        let fallback_task_ordinal = self.active.as_ref().map(|active| active.task_ordinal);
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
        if self.core.session_state() != SessionState::Running || self.active.is_none() {
            return Ok(());
        }
        let active = self
            .active
            .clone()
            .ok_or_else(|| anyhow!("active exec worker turn disappeared"))?;
        let poll = {
            let task_runtime = self.require_task_runtime_mut(active.task_ordinal)?;
            task_runtime.runtime.poll_turn(active.local_turn)
        };
        let event = match poll {
            Ok(RuntimeTurnPoll::Pending { .. }) => return Ok(()),
            Ok(RuntimeTurnPoll::Completed {
                outcome,
                final_message_path,
                ..
            }) => SupervisorEvent::TurnCompleted {
                expected_version: self.core.version(),
                task_ordinal: active.task_ordinal,
                role: active.role,
                session: active.logical_session,
                turn: active.logical_turn,
                completion_token: active.completion_token.clone(),
                outcome,
                final_message_path,
            },
            Ok(RuntimeTurnPoll::Failed { failure, .. }) => SupervisorEvent::TurnFailed {
                expected_version: self.core.version(),
                task_ordinal: active.task_ordinal,
                role: active.role,
                session: active.logical_session,
                turn: active.logical_turn,
                completion_token: active.completion_token.clone(),
                failure,
            },
            Err(error) => SupervisorEvent::TurnFailed {
                expected_version: self.core.version(),
                task_ordinal: active.task_ordinal,
                role: active.role,
                session: active.logical_session,
                turn: active.logical_turn,
                completion_token: active.completion_token.clone(),
                failure: runtime_error_failure(error)?,
            },
        };
        // Two-phase commit, same as `execute_effects`: a verdict that closes a
        // task must not be committed unless the runtime actually closes. The
        // reviewer's verdict now lands here directly, so the guard has to live
        // here too.
        let mut next_core = self.core.clone();
        let effects = next_core
            .reduce(event)
            .map_err(|error| anyhow!(error.to_string()))?;
        self.active = None;
        if let Some(task_ordinal) = successful_task_close(&next_core, &effects)
            && let Err(error) = self.close_task_runtime(task_ordinal)
        {
            return self.fail_driver_effect(task_ordinal, DriverFailureClass::Cleanup, error);
        }
        self.core = next_core;
        self.execute_effects(effects)
    }

    pub(crate) fn shutdown(&mut self) -> Result<()> {
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
                SupervisorEffect::OpenTaskRuntime { task_ordinal } => {
                    if let Err(failure) = self.open_task_runtime(task_ordinal) {
                        return self.fail_driver_effect(task_ordinal, failure.class, failure.error);
                    }
                    SupervisorEvent::TaskRuntimeOpened {
                        expected_version: self.core.version(),
                        task_ordinal,
                    }
                }
                SupervisorEffect::OpenRoleSession { task_ordinal, role } => {
                    let logical = match self.open_role_session(task_ordinal, role) {
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
                        role,
                        session: logical,
                    }
                }
                SupervisorEffect::StartTurn {
                    task_ordinal,
                    role,
                    purpose,
                    session,
                } => {
                    let (logical_turn, completion_token) =
                        match self.start_turn(task_ordinal, role, purpose, session) {
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
                        "task {task_ordinal}: started {role:?} turn ({purpose:?})"
                    ));
                    SupervisorEvent::TurnStarted {
                        expected_version: self.core.version(),
                        task_ordinal,
                        role,
                        purpose,
                        session,
                        turn: logical_turn,
                        completion_token,
                    }
                }
                SupervisorEffect::InterruptTurn {
                    task_ordinal, turn, ..
                } => {
                    self.interrupt_turn(task_ordinal, turn);
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
        if self.task_runtime.is_some() || self.active.is_some() {
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
        let repository_root = PathBuf::from(&task.spec.repository_root);
        let (root, paths) = TaskRuntimePaths::create(
            &self.run_root,
            task_ordinal,
            &task.spec.task_key,
            &repository_root,
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
        role: WorkerRole,
    ) -> Result<RuntimeSessionKey> {
        self.cleanup_registry.ensure_available()?;
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("role session task ordinal is out of range"))?;
        let task_key = task.spec.task_key.clone();
        let repository_root = PathBuf::from(&task.spec.repository_root);
        let project_root = self.startup.project_root.clone();
        let profile = self.profile(role).clone();
        let instructions = role_instructions(role).to_owned();
        let local = self
            .require_task_runtime_mut(task_ordinal)?
            .runtime
            .open_session(RoleSessionSpec {
                role,
                task_key,
                cwd: project_root,
                task_repository: repository_root,
                profile,
                developer_instructions: instructions,
            })
            .map_err(|error| anyhow!(error.detail))?;
        let logical = self.allocate_session_key()?;
        let runtime = self.require_task_runtime_mut(task_ordinal)?;
        if runtime
            .sessions
            .insert(logical, LocalSession { role, key: local })
            .is_some()
        {
            bail!("logical exec worker role session key collided");
        }
        Ok(logical)
    }

    fn start_turn(
        &mut self,
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        logical_session: RuntimeSessionKey,
    ) -> Result<(RuntimeTurnKey, String)> {
        if self.active.is_some() {
            bail!("a second exec worker turn cannot start");
        }
        self.cleanup_registry.ensure_available()?;
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("turn task ordinal is out of range"))?;
        let task_key = task.spec.task_key.clone();
        let repository_root = PathBuf::from(&task.spec.repository_root);
        let prompt = self.build_turn_prompt(task_ordinal, role, purpose)?;
        let profile = self.profile(role).clone();
        let local_session = self
            .require_task_runtime_mut(task_ordinal)?
            .sessions
            .get(&logical_session)
            .copied()
            .ok_or_else(|| anyhow!("logical exec worker role session is not bound"))?;
        if local_session.role != role {
            bail!("logical exec worker session belongs to the wrong role");
        }
        let project_root = self.startup.project_root.clone();
        let local_turn = self
            .require_task_runtime_mut(task_ordinal)?
            .runtime
            .start_turn(
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
        self.active = Some(ActiveTurn {
            task_ordinal,
            role,
            logical_session,
            logical_turn,
            local_turn,
            completion_token: completion_token.clone(),
        });
        Ok((logical_turn, completion_token))
    }

    fn interrupt_turn(&mut self, task_ordinal: usize, logical_turn: RuntimeTurnKey) {
        let Some(active) = self.active.take() else {
            return;
        };
        if active.task_ordinal != task_ordinal || active.logical_turn != logical_turn {
            debug_assert_eq!(
                (active.task_ordinal, active.logical_turn),
                (task_ordinal, logical_turn),
                "SupervisorCore emitted an interrupt for a different active exec worker turn"
            );
            self.active = Some(active);
            return;
        }
        if let Some(runtime) = self.task_runtime.as_mut()
            && let Err(error) = runtime.runtime.cancel_turn(active.local_turn)
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
        self.active = None;
        runtime
            .runtime
            .shutdown()
            .map_err(|error| anyhow!(error.detail))
    }

    fn close_runtime_best_effort(&mut self) {
        self.active = None;
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
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
    ) -> Result<String> {
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("turn prompt task ordinal is out of range"))?;
        let spec = &task.spec;
        let mut prompt = String::new();
        let role_name = match role {
            WorkerRole::Developer => "Developer",
            WorkerRole::Reviewer => "Reviewer",
        };
        prompt.push_str(&format!(
            "You are the task {role_name}.\n\nRepository:\n{}\n\nTask file:\n{}\n\nDesign files:\n",
            spec.repository_root, spec.task_document_path,
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
                match purpose {
                    RuntimeTurnPurpose::DeveloperCorrection => {
                        let paths = task.latest_reviewer_final_paths();
                        if paths.is_empty() {
                            bail!("developer correction has no reviewer final message path");
                        }
                        prompt.push_str("\nReviewer final messages, in order:\n");
                        for path in paths {
                            prompt.push_str(&format!("- {path}\n"));
                        }
                        prompt.push_str(
                            "\nRead every Reviewer final file and address the requested changes. \
                             If an explicit human, task, design, or applicable instruction still \
                             requires this run to remain uncommitted, do not modify or amend \
                             anything: return `STATUS: CLARIFICATION_REQUIRED` for a human \
                             authority decision. Otherwise, if your task commit exists, fold the \
                             fix into that SAME commit with \
                             `git commit --amend`, updating its message so it still describes the \
                             whole task and ensuring it retains a valid `Signed-off-by` trailer for \
                             the committing identity. If the Reviewer reported that your task \
                             commit is missing, create it with that sign-off only after the complete \
                             fix. This task must end as exactly one commit. Do not amend anything \
                             older than your own commit. Then report as before.\n",
                        );
                    }
                    RuntimeTurnPurpose::DeveloperClarificationResume => {
                        let latest = task.clarification_records().last().ok_or_else(|| {
                            anyhow!("Developer clarification resume has no clarification record")
                        })?;
                        prompt.push_str(&format!(
                            "\nResume the same task using clarification sequence {}. Re-read your \
                             request at {} and the Architect response at {} before continuing. \
                             Preserve any existing task commit exactly until you have a complete \
                             correction to amend into it. If the request reported an explicit \
                             no-commit conflict, proceed only when the newest clarification records \
                             the human's decision resolving that authority conflict; otherwise \
                             return `STATUS: CLARIFICATION_REQUIRED` again without modifying or \
                             committing the repository.\n",
                            latest.sequence,
                            latest.developer_request_path,
                            latest.architect_clarification_path,
                        ));
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
                prompt.push_str(DEVELOPER_OUTPUT_CONTRACT);
            }
            WorkerRole::Reviewer => {
                let developer_path = task
                    .latest_developer_final_path()
                    .ok_or_else(|| anyhow!("reviewer turn has no Developer final message path"))?;
                let label = if purpose == RuntimeTurnPurpose::ReviewerRereview {
                    "Latest Developer final message"
                } else {
                    "Developer final message"
                };
                prompt.push_str(&format!(
                    "\n{label}:\n{developer_path}\n\nRead the Developer final file and \
                     independently review the selected task. Check every `ASSUMPTION:` the \
                     Developer reported against the approved task, design, and clarification \
                     records. Confirm the task is represented by one task commit and that no \
                     hcom-tasks artifact was included in it. If the implementation follows an \
                     applicable clarification, do not report a defect merely because the original \
                     task or design wording was ambiguous. If a finding comes from unresolved \
                     task/design ambiguity rather than an implementation defect, label that \
                     finding `REQUIREMENT_AMBIGUITY:` while still returning the normal verdict.\n"
                ));
                prompt.push_str(REVIEWER_OUTPUT_CONTRACT);
            }
        }

        if prompt.len() > crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES {
            bail!("rendered task turn prompt exceeds its 256 KiB bound");
        }
        Ok(prompt)
    }

    fn profile(&self, role: WorkerRole) -> &RuntimeProfile {
        match role {
            WorkerRole::Developer => &self.profiles.developer,
            WorkerRole::Reviewer => &self.profiles.reviewer,
        }
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
        runtime: paths.runtime.clone(),
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

/// The reviewer's only output obligation. Deliberately narrow: hcom parses one
/// anchored line and treats everything else as opaque payload.
const REVIEWER_OUTPUT_CONTRACT: &str = "
## Required output format

The FIRST line of your final message must be exactly one of:

VERDICT: LGTM
VERDICT: REQUEST_CHANGES

on its own line, with no decoration and no other text on that line. After it,
write your findings as free-form markdown (path:line references are helpful but
not required). Judge how deeply to verify. You have the same native host view as
a human-launched Codex session, but the reviewer role forbids modifying the
reviewed source, Git state, installed artifacts, or branches. You may copy the
tree elsewhere and build or test that copy when it helps obtain independent
evidence.
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

fn role_instructions(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => {
            "You are the task Developer: execute the concrete approved task; do not redesign its product scope. First seek answers in the task file, design and clarification files, applicable instructions, existing implementation, and tests. Make ordinary local implementation decisions yourself. If an uncertain choice has a defensible candidate, is consistent with the approved behavior and scope, and can be corrected in review, choose the smallest-impact option, continue, and disclose it as `ASSUMPTION:` in your final. Ask for clarification only when you cannot derive any defensible candidate or the choice would decide material externally visible behavior, acceptance, or scope. Report BLOCKED only after actual attempts establish an external or mechanical obstacle; include concrete observations. Work directly in the exact repository and complete the bounded task. The human's execution approval for this standard hcom lane authorizes exactly one signed-off local candidate commit for this task; a general instruction that commits require human authorization is satisfied by that run approval. If an explicit human, task, design, or applicable instruction instead requires this run to remain uncommitted, do not modify or commit the repository: return `STATUS: CLARIFICATION_REQUIRED` because that requirement is incompatible with the standard review lane and requires an explicit human resolution. Otherwise run the required checks, then commit the complete work as ONE NEW commit whose message describes this task as a whole and whose `Signed-off-by` trailer matches the committing identity (for example, create it with `git commit --signoff`). Never amend, squash, reword, or otherwise rewrite a commit that existed when your first task turn began. On correction or clarification resume, amend your existing task commit if it exists and ensure that it retains a valid matching `Signed-off-by` trailer; if no task commit exists yet, create the one signed-off task commit only after the implementation is complete. Never create a second task commit. Do not create a commit merely to pause. If a pause is necessary after your task commit already exists, leave that commit unchanged and report the exact repository state. Never add any `hcom-tasks` artifact to the task commit. This local candidate commit and its same-task amendments do not authorize push, install, or release. Do not push, install, wait for interactive input, or modify the task/design/clarification source files."
        }
        WorkerRole::Reviewer => {
            "You are the task Reviewer. Independently inspect the committed task range and decide whether it is sound against the approved task, design files, and every ordered clarification record. The human's execution approval for this standard hcom lane includes exactly one signed-off local Developer candidate commit and same-commit amendments during correction; it never includes push, install, or release. Review disclosed Developer assumptions rather than accepting them automatically. Confirm the developer left the work committed as a single commit for this task, with a message covering it, a valid `Signed-off-by` trailer matching the committing identity, and no `hcom-tasks` artifact; uncommitted work, a missing or mismatched sign-off, or a task split across several commits is a reason to request changes. If an explicit human, task, design, or applicable instruction requires the run to remain uncommitted, return `VERDICT: REQUEST_CHANGES` and label the incompatible workflow requirement `REQUIREMENT_AMBIGUITY:` instead of accepting either side of the contradiction. Distinguish other requirement ambiguity from implementation defects and label the former `REQUIREMENT_AMBIGUITY:` in findings. An LGTM applies to the exact final candidate range already committed; it does not call for another post-LGTM commit or a human decision about retaining that reviewed commit. You must not edit reviewed source, stage, commit, change branch or HEAD, push, or install; verifying by copying the tree into your own writable sandbox is allowed and encouraged when it helps."
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
        ArchitectAdapter, ClaudeInvocationProfile, DeveloperInvocationProfile,
        ReviewerInvocationProfile, SessionInvocationProfiles,
    };
    use crate::worker::runtime::{
        CODEX_TASK_WORKER_ADAPTER, DeveloperOutcomeStatus, DeveloperOutcomeV1, ReviewerOutcomeV1,
        ReviewerVerdict, RuntimeOutcome, RuntimeTelemetry,
    };
    use std::collections::BTreeSet;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::process::Command;
    use std::sync::{Arc, Mutex};

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
        turns: Vec<(String, WorkerRole, RuntimeTurnPurpose, u64)>,
        prompts: Vec<(WorkerRole, RuntimeTurnPurpose, String)>,
        shutdowns: Vec<String>,
        environments: Vec<Vec<(OsString, OsString)>>,
        profiles: Vec<(String, WorkerRole, RuntimeProfile)>,
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
        ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError> {
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
        mutations: VecDeque<Mutation>,
        shutdown_failure: bool,
        audit: Arc<Mutex<Audit>>,
    }

    impl TaskWorkerRuntime for ScriptedRuntime {
        fn contract(&self) -> &RuntimeContractIdentity {
            self.inner.contract()
        }

        fn open_session(
            &mut self,
            spec: RoleSessionSpec,
        ) -> Result<RuntimeSessionKey, RuntimeError> {
            let role = spec.role;
            let profile = spec.profile.clone();
            let session = self.inner.open_session(spec)?;
            let mut audit = self.audit.lock().unwrap();
            audit
                .sessions
                .push((self.task_key.clone(), role, session.counter()));
            audit.profiles.push((self.task_key.clone(), role, profile));
            Ok(session)
        }

        fn start_turn(
            &mut self,
            session: RuntimeSessionKey,
            spec: RuntimeTurnSpec,
        ) -> Result<RuntimeTurnKey, RuntimeError> {
            let role = spec.role;
            let purpose = spec.purpose;
            let prompt = spec.prompt.clone();
            let turn = self.inner.start_turn(session, spec)?;
            let mut audit = self.audit.lock().unwrap();
            audit
                .turns
                .push((self.task_key.clone(), role, purpose, session.counter()));
            audit.prompts.push((role, purpose, prompt));
            drop(audit);
            Ok(turn)
        }

        fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
            let poll = self.inner.poll_turn(turn)?;
            if poll.is_terminal() {
                let mutation = self.mutations.pop_front().ok_or_else(|| {
                    RuntimeError::internal("scripted runtime mutation inventory disappeared")
                })?;
                apply_mutation(&self.repository, mutation)
                    .map_err(|_| RuntimeError::internal("scripted Git mutation failed"))?;
            }
            Ok(poll)
        }

        fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
            self.inner.cancel_turn(turn)
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
        ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError> {
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
            sources.profiles =
                Some(SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap());
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
                max_review_rounds: max_rounds,
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
        use crate::worker::environment::ParentEnvironment;
        use crate::worker::profile::{
            ArchitectAdapter, CodexInvocationProfile, DeveloperInvocationProfile,
            ReviewerInvocationProfile, SessionInvocationProfiles,
        };
        use crate::worker::runtime::CODEX_TASK_WORKER_ADAPTER;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        /// Cheap model for acceptance runs; production defaults stay untouched.
        const TEST_MODEL: &str = "gpt-5.3-codex-spark";
        const TEST_EFFORT: &str = "medium";

        pub(crate) struct RealFixture {
            pub(crate) _temp: Option<tempfile::TempDir>,
            pub(crate) project_root: PathBuf,
            pub(crate) repository: PathBuf,
            run_root: PathBuf,
            sources: SessionRuntimeSources,
        }

        impl RealFixture {
            pub(crate) fn new(label: &str) -> Self {
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
                let mut profiles =
                    SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
                let cheap = CodexInvocationProfile {
                    model: TEST_MODEL.into(),
                    reasoning_effort: TEST_EFFORT.into(),
                    sandbox: crate::worker::profile::CodexSandbox::DangerFullAccess,
                    approval_policy: crate::worker::profile::CodexApprovalPolicy::Never,
                };
                profiles.developer = DeveloperInvocationProfile::Codex {
                    profile: cheap.clone(),
                };
                profiles.reviewer = ReviewerInvocationProfile::Codex { profile: cheap };

                let mut sources = SessionRuntimeSources::fake(Path::new(&home));
                sources.set_profiles_for_test(profiles);
                // Complete parent inheritance, exactly like production.
                sources.parent_environment = ParentEnvironment::capture_current().unwrap();

                Self {
                    _temp: temp,
                    project_root,
                    repository,
                    run_root,
                    sources,
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
                    max_review_rounds,
                    max_clarification_rounds: 2,
                }
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
                    .replace_plan(
                        0,
                        CODEX_TASK_WORKER_ADAPTER,
                        CODEX_TASK_WORKER_ADAPTER,
                        tasks,
                    )
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
            pub(crate) fn thread_ids(&self, task_key: &str, role: &str) -> Vec<String> {
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
                    let Some(rest) = first.split("\"thread_id\":\"").nth(1) else {
                        continue;
                    };
                    if let Some(id) = rest.split('"').next() {
                        found.push((path, id.to_string()));
                    }
                }
                found.sort_by(|a, b| a.0.cmp(&b.0));
                found.into_iter().map(|(_, id)| id).collect()
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
        TaskScript {
            task_key: task_key.into(),
            turns,
            mutations: mutations.into(),
            shutdown_failure: false,
        }
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
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                CODEX_TASK_WORKER_ADAPTER,
                tasks,
            )
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingApproval);
        supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::Running);
    }

    fn drive_terminal(supervisor: &mut TaskLaneSupervisor) -> SessionStatusSnapshot {
        for _ in 0..64 {
            supervisor.poll_once().unwrap();
            let snapshot = supervisor.snapshot();
            if snapshot.state.is_terminal() {
                return snapshot;
            }
        }
        panic!("scripted exec worker supervisor did not become terminal");
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
    fn task_runtime_only_creates_its_raw_transport_directory() {
        let fixture = Fixture::new();
        let (root, paths) =
            TaskRuntimePaths::create(&fixture.run_root, 0, "config", &fixture.repository).unwrap();
        let mut children = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        children.sort();
        assert_eq!(children, [OsString::from("run")]);
        assert!(paths.runtime.is_dir());
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
        assert!(snapshot.tasks[0].reviewer_session_bound);
        let audit = audit.lock().unwrap();
        assert_eq!(audit.opens, ["one"]);
        assert_eq!(audit.shutdowns, ["one"]);
        assert_eq!(
            audit
                .sessions
                .iter()
                .map(|(_, role, key)| (*role, *key))
                .collect::<Vec<_>>(),
            [(WorkerRole::Developer, 1), (WorkerRole::Reviewer, 2)]
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
                    CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
                ("second", WorkerRole::Developer, 1),
                ("second", WorkerRole::Reviewer, 2),
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
        assert_eq!(audit.sessions.len(), 2);
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
            1
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
        // One developer session and one reviewer session, each resumed.
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
            1
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
            snapshot.tasks[0].final_reviewer_message_paths,
            vec![
                message_path(WorkerRole::Reviewer, "overflow handling is correct now")
                    .to_string_lossy()
                    .into_owned()
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
        assert_eq!(audit.prompts.len(), 4);
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
        let script = task_script(
            "exhausted",
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [ready("attempt")],
                ),
                FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [request_changes("still wrong")],
                ),
            ],
            vec![
                Mutation::Commit {
                    path: "src/task.txt",
                    contents: "one\n",
                },
                Mutation::None,
            ],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("exhausted", &["src"], 1)],
        );
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::Completed);
        assert_eq!(snapshot.tasks[0].state, TaskState::ReviewExhausted);
        assert_eq!(
            snapshot.tasks[0].final_reviewer_message_paths,
            vec![
                message_path(WorkerRole::Reviewer, "still wrong")
                    .to_string_lossy()
                    .into_owned()
            ]
        );
        assert_eq!(
            snapshot.tasks[0].reviewer_verdict,
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
        // Four sessions total, two per task, each opened against that task's
        // own fresh runtime — nothing is carried across the task boundary.
        assert_eq!(audit.sessions.len(), 4);
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
                "{task} must open exactly one developer and one reviewer session"
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
                ("two", WorkerRole::Developer, 1),
                ("two", WorkerRole::Reviewer, 2),
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
            task_script(
                "exhausted",
                vec![
                    FakeTurnScript::new(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment,
                        [ready("implemented")],
                    ),
                    FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [request_changes("bounded finding")],
                    ),
                ],
                vec![
                    Mutation::Commit {
                        path: "src/exhausted.txt",
                        contents: "first\n",
                    },
                    Mutation::None,
                ],
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
        assert_eq!(snapshot.tasks[0].review_round, 1);
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
        let ReviewerInvocationProfile::Codex { profile: reviewer } = &mut profiles.reviewer else {
            unreachable!()
        };
        reviewer.model = "reviewer-override".into();
        reviewer.reasoning_effort = "max".into();

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
        assert_eq!(profiles[1].2.model, "reviewer-override");
        assert_eq!(profiles[1].2.reasoning_effort, "max");
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
            1
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
                    CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
                    CODEX_TASK_WORKER_ADAPTER,
                    vec![task],
                )
                .is_err()
        );
        assert_eq!(supervisor.snapshot().state, SessionState::AwaitingPlan);
    }

    #[test]
    fn claude_architect_requires_external_repository_roots_to_be_predeclared() {
        let mut fixture = Fixture::new();
        fixture.sources.profiles =
            Some(SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Claude).unwrap());
        let task = fixture.task("external-root", &["src"], 2);

        let mut rejected = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let error = rejected
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
                    CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
                vec![retained_task],
            )
            .expect("task source directories are not locked");

        supervisor
            .approve_and_start(before.version, plan_version, &plan_hash, true)
            .unwrap();
        assert_eq!(supervisor.snapshot().state, SessionState::Running);
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
                CODEX_TASK_WORKER_ADAPTER,
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
    fn production_claude_role_fails_closed_before_any_provider_process_exists() {
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
        sources.set_profiles_for_test(profiles);
        let mut supervisor = TaskLaneSupervisor::open(
            "run-unavailable-claude".into(),
            fixture.project_root.clone(),
            fixture.run_root.clone(),
            sources,
        )
        .unwrap();
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                "claude-exec",
                "codex-exec",
                vec![fixture.task("unavailable-claude", &["src"], 2)],
            )
            .unwrap();
        let error = supervisor
            .approve_and_start(1, plan_version, &plan_hash, true)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Claude task worker runtime is not implemented; CLAUDE-03 is required"
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert!(snapshot.active_worker.is_none());
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
                CODEX_TASK_WORKER_ADAPTER,
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
                CODEX_TASK_WORKER_ADAPTER,
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
        assert!(action_snapshot.active_worker.is_none());
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
            resumed.active_worker.as_ref().map(|worker| worker.role),
            Some(WorkerRole::Developer)
        );
        assert_eq!(
            resumed
                .active_worker
                .as_ref()
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
        assert!(snapshot.active_worker.is_none());
        assert!(supervisor.active.is_none());
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

    /// A mandatory first-round rejection with max_review_rounds=1 must be
    /// reported as review_exhausted, close its runtime, and advance to the
    /// next task without pretending the rejection was an approval.
    #[test]
    #[ignore = "requires native codex, auth, and network"]
    fn real_review_exhausted_advances_to_the_next_task() {
        let fixture = RealFixture::new("exhaustion");
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            fixture.task(
                "exhausted",
                "Controlled review exhaustion probe",
                "This is a controlled lifecycle E2E with max_review_rounds=1.\n\n\
                 InitialDevelopment: create and commit ONLY incomplete.py defining value = 1. \
                 Deliberately do not create required_test.py, and report that omission.\n\n\
                 InitialReview: required_test.py being absent is the deliberately seeded blocking \
                 defect. You MUST return VERDICT: REQUEST_CHANGES and MUST NOT return LGTM. \
                 Require required_test.py to assert incomplete.value == 1. Because the configured \
                 review limit is one, hcom should mark this task review_exhausted without a \
                 correction turn. Do not push.",
                1,
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
        assert_eq!(exhausted.review_round, 1);
        assert_eq!(
            exhausted.reviewer_verdict,
            Some(ReviewerVerdict::RequestChanges)
        );
        assert_eq!(
            fixture.thread_ids("exhausted", "developer").len(),
            1,
            "an exhausted one-round task must not start a correction turn"
        );
        assert_eq!(fixture.thread_ids("exhausted", "reviewer").len(), 1);
        assert_eq!(snapshot.tasks[1].state, TaskState::Lgtm);
        assert!(fixture.repository.join("recovery.txt").is_file());
        for path in &exhausted.final_reviewer_message_paths {
            assert!(Path::new(path).is_file(), "missing reviewer final: {path}");
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
        assert!(snapshot.tasks[0].final_reviewer_message_paths.is_empty());
        assert_eq!(snapshot.tasks[0].reviewer_verdict, None);
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
