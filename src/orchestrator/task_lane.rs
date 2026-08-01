//! Effect driver for the `hcom arch codex` Codex exec worker task lane.
//!
//! The driver is the only owner of Git, filesystem, environment, and runtime
//! I/O. Scheduling decisions remain in [`SupervisorCore`].

use super::core::{
    DriverFailure, DriverFailureClass, RepositoryObservation, SupervisorCore, SupervisorEffect,
    SupervisorEvent, TaskRepositoryBinding,
};
use super::workspace::TasksWorkspace;
use super::{
    GitRunner, ManagedRepository, SessionRuntimeSources, SessionStartup, ensure_private_directory,
    path_value, prepare_auth_mount_target, sha256_hex,
};
use crate::control_api::{SessionState, SessionStatusSnapshot, TaskDraft, TaskState, WorkerRole};
use crate::worker::environment::{
    EnvironmentPolicy, ExecutionEnvironmentLease, MaterializedWorkerEnvironment,
};
use crate::worker::exec_runtime::{
    ExecPreflight, ExecRuntimeConfig, ExecTaskPaths, ExecTaskWorkerRuntime,
    codex_exec_contract_identity,
};
use crate::worker::runtime::{
    CODEX_TASK_WORKER_ADAPTER, OutcomeContract, RoleSessionSpec, RuntimeContractIdentity,
    RuntimeError, RuntimeErrorCode, RuntimeFailureClass, RuntimeOutcome, RuntimeProfile,
    RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnPurpose, RuntimeTurnSpec,
    SanitizedRuntimeFailure, TaskWorkerProfiles, TaskWorkerRuntime,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const TURN_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
/// The supervisor no longer hashes working-tree state, so the observation's
/// content-digest fields carry a fixed placeholder. They remain in the type
/// because the reducer's identity plumbing is shared.
const EMPTY_OBSERVATION_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_AUTH_FILE_BYTES: usize = 1024 * 1024;
const MAX_AUTH_REDACTION_VALUES: usize = 64;

trait RuntimeFactory: Send {
    fn contract(&self) -> RuntimeContractIdentity;

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError>;
}

struct ProductionRuntimeFactory {
    preflight: Option<ExecPreflight>,
}

impl ProductionRuntimeFactory {
    fn new() -> Self {
        Self { preflight: None }
    }
}

impl RuntimeFactory for ProductionRuntimeFactory {
    fn contract(&self) -> RuntimeContractIdentity {
        codex_exec_contract_identity()
    }

    fn open(
        &mut self,
        request: RuntimeOpenRequest,
    ) -> Result<Box<dyn TaskWorkerRuntime>, RuntimeError> {
        if request.task_ordinal >= 64 {
            return Err(RuntimeError::invalid_contract(
                "task runtime ordinal exceeds the session bound",
            ));
        }
        crate::worker::validation::validate_opaque_id("task runtime key", &request.task_key)
            .map_err(|_| RuntimeError::invalid_contract("task runtime key was invalid"))?;
        let preflight = match &self.preflight {
            Some(preflight) => preflight.clone(),
            None => {
                let preflight = ExecPreflight::verify_pinned()?;
                self.preflight = Some(preflight.clone());
                preflight
            }
        };
        let runtime = ExecTaskWorkerRuntime::open(ExecRuntimeConfig {
            codex: preflight.codex().to_path_buf(),
            bwrap: Some(preflight.bwrap().to_path_buf()),
            repository_root: request.repository_root,
            paths: ExecTaskPaths {
                home: request.paths.home,
                codex_home: request.paths.codex_home,
                temp: request.paths.temp,
                runtime: request.paths.runtime,
            },
            auth_source: request.auth_source,
            cargo_bin_source: request.cargo_bin_source,
            rustup_home_source: request.rustup_home_source,
            environment: request.environment,
            lease: request.lease,
            artifact_root_path: request.artifact_root,
            run_id: request.run_id,
            task_id: request.task_key,
        })?;
        Ok(Box::new(runtime))
    }
}

struct RuntimeOpenRequest {
    task_ordinal: usize,
    task_key: String,
    repository_root: PathBuf,
    paths: TaskRuntimePaths,
    auth_source: PathBuf,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
    environment: Vec<(OsString, OsString)>,
    lease: ExecutionEnvironmentLease,
    artifact_root: PathBuf,
    run_id: String,
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
    home: PathBuf,
    codex_home: PathBuf,
    temp: PathBuf,
    runtime: PathBuf,
    artifacts: PathBuf,
    hcom: PathBuf,
    xdg_config: PathBuf,
    xdg_state: PathBuf,
    xdg_cache: PathBuf,
    xdg_data: PathBuf,
}

impl TaskRuntimePaths {
    fn create(
        run_root: &Path,
        task_ordinal: usize,
        task_key: &str,
        repository_root: &Path,
    ) -> Result<(TempDir, Self)> {
        let workers = run_root.join("exec-workers");
        ensure_private_directory(&workers)?;
        let root = tempfile::Builder::new()
            .prefix(&format!("task-{task_ordinal}-{task_key}."))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(&workers)
            .context("failed to create task-private exec worker root")?;
        let root_path = fs::canonicalize(root.path())?;
        let home = root_path.join("home");
        let paths = Self {
            codex_home: home.join(".codex"),
            temp: root_path.join("tmp"),
            runtime: root_path.join("run"),
            artifacts: root_path.join("artifacts"),
            hcom: root_path.join("hcom"),
            xdg_config: home.join(".config"),
            xdg_state: home.join(".state"),
            xdg_cache: home.join(".cache"),
            xdg_data: home.join(".data"),
            home,
        };
        for directory in [
            &paths.home,
            &paths.codex_home,
            &paths.temp,
            &paths.runtime,
            &paths.artifacts,
            &paths.hcom,
            &paths.xdg_config,
            &paths.xdg_state,
            &paths.xdg_cache,
            &paths.xdg_data,
        ] {
            fs::create_dir(directory).with_context(|| {
                format!(
                    "failed to create task-private exec worker directory {}",
                    directory.display()
                )
            })?;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        }
        prepare_auth_mount_target(&paths.codex_home.join("auth.json"))?;
        write_private_codex_config(&paths.codex_home.join("config.toml"), repository_root)?;
        Ok((root, paths))
    }
}

struct OpenTaskRuntime {
    task_ordinal: usize,
    _root: TempDir,
    environment: ExecutionEnvironmentLease,
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
    prompt: String,
}

pub(crate) struct TaskLaneSupervisor {
    startup: SessionStartup,
    epoch: String,
    core: SupervisorCore,
    repositories: BTreeMap<PathBuf, ManagedRepository>,
    lock_root: PathBuf,
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
    tasks_workspace: Option<TasksWorkspace>,
}

impl TaskLaneSupervisor {
    pub(crate) fn open(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        lock_root: PathBuf,
        sources: SessionRuntimeSources,
    ) -> Result<Self> {
        Self::open_with_factory(
            run_id,
            project_root,
            run_root,
            lock_root,
            sources,
            Box::new(ProductionRuntimeFactory::new()),
        )
    }

    fn open_with_factory(
        run_id: String,
        project_root: PathBuf,
        run_root: PathBuf,
        lock_root: PathBuf,
        sources: SessionRuntimeSources,
        factory: Box<dyn RuntimeFactory>,
    ) -> Result<Self> {
        crate::worker::validation::validate_opaque_id("run id", &run_id)?;
        let project_root = super::canonical_project_directory(&project_root)?;
        let run_root = super::canonical_private_directory(&run_root, "session runtime root")?;
        let lock_root = super::canonical_private_directory(&lock_root, "repository lock root")?;
        let session_profiles = sources.profiles.clone().ok_or_else(|| {
            anyhow!("the Codex exec worker lane requires session-frozen profiles")
        })?;
        let profiles = TaskWorkerProfiles::from_session_profiles(&session_profiles)
            .map_err(|error| anyhow!(error.detail))?;
        profiles.validate().map_err(|error| anyhow!(error.detail))?;
        let contract = factory.contract();
        contract.validate().map_err(|error| anyhow!(error.detail))?;
        let profile_hash = sha256_hex(&serde_json::to_vec(&(
            "hcom-codex-exec-session-binding-v1",
            session_profiles.canonical_hash(),
            profiles.canonical_hash(),
            contract.canonical_hash(),
        ))?);
        let startup = SessionStartup {
            run_id: run_id.clone(),
            project_root: project_root.clone(),
        };
        let core = SupervisorCore::new(run_id, project_root, profile_hash)
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(Self {
            startup,
            epoch: format!("exec-supervisor-{}", Uuid::new_v4()),
            core,
            repositories: BTreeMap::new(),
            lock_root,
            run_root,
            sources,
            profiles,
            developer_adapter: CODEX_TASK_WORKER_ADAPTER.into(),
            reviewer_adapter: CODEX_TASK_WORKER_ADAPTER.into(),
            factory,
            task_runtime: None,
            active: None,
            next_session: 1,
            next_turn: 1,
            tasks_workspace: None,
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

        let roots: BTreeSet<PathBuf> = tasks
            .iter()
            .map(|task| PathBuf::from(&task.repository_root))
            .collect();
        let mut staged = BTreeMap::new();
        for root in &roots {
            if self.repositories.contains_key(root) {
                continue;
            }
            let repository = ManagedRepository::open(root, &self.lock_root)
                .with_context(|| format!("failed to bind task repository {}", root.display()))?;
            if repository.repository.root != *root {
                bail!(
                    "task repository_root must name the exact canonical Git top level: {}",
                    root.display()
                );
            }
            staged.insert(root.clone(), repository);
        }
        let bindings = plan_bindings(&tasks, &self.repositories, &staged)?;
        let plan_version = self
            .core
            .plan_version()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("plan version overflow"))?;
        let plan_hash = self
            .core
            .expected_plan_hash(plan_version, &tasks, &bindings);
        let event = SupervisorEvent::PlanBound {
            expected_version: expected_session_version,
            plan_version,
            plan_hash: plan_hash.clone(),
            tasks,
            repositories: bindings,
        };
        let effects = self
            .core
            .reduce(event)
            .map_err(|error| anyhow!(error.to_string()))?;
        let mut previous = std::mem::take(&mut self.repositories);
        let mut repositories = BTreeMap::new();
        for root in roots {
            let repository = previous
                .remove(&root)
                .or_else(|| staged.remove(&root))
                .ok_or_else(|| anyhow!("staged task repository disappeared"))?;
            repositories.insert(root, repository);
        }
        self.repositories = repositories;
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
        let effects = self
            .core
            .reduce(SupervisorEvent::ExecutionAuthorized {
                expected_version: expected_session_version,
                plan_version: Some(plan_version),
                plan_hash: Some(plan_hash.into()),
            })
            .map_err(|error| anyhow!(error.to_string()))?;
        if self.tasks_workspace.is_none() {
            let workspace = TasksWorkspace::open(&self.startup.project_root, &self.startup.run_id)
                .context("failed to open the hcom-tasks workspace")?;
            workspace.write_run_file(
                "plan.md",
                self.render_plan(plan_version, plan_hash).as_bytes(),
            )?;
            let _ = workspace.append_decision(&format!(
                "execution authorized: plan version {plan_version} hash {plan_hash}"
            ));
            self.tasks_workspace = Some(workspace);
        }
        self.execute_effects(effects)
    }

    fn render_plan(&self, plan_version: u64, plan_hash: &str) -> String {
        let mut plan = format!(
            "# hcom arch run {}\n\nplan version: {plan_version}\nplan hash: {plan_hash}\n\n",
            self.startup.run_id
        );
        for (ordinal, task) in self.core.tasks().iter().enumerate() {
            let spec = &task.spec;
            plan.push_str(&format!(
                "## task {ordinal}: {}\n\n- title: {}\n- repository: {}\n- max review rounds: {}\n\n{}\n\n",
                spec.task_key, spec.title, spec.repository_root, spec.max_review_rounds, spec.objective
            ));
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

    pub(crate) fn snapshot(&self) -> SessionStatusSnapshot {
        self.core.snapshot()
    }

    pub(crate) fn poll_once(&mut self) -> Result<()> {
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
            Ok(RuntimeTurnPoll::Completed { outcome, .. }) => {
                if self.outcome_contains_sensitive_value(
                    active.task_ordinal,
                    &active.prompt,
                    &outcome,
                ) {
                    SupervisorEvent::TurnFailed {
                        expected_version: self.core.version(),
                        task_ordinal: active.task_ordinal,
                        role: active.role,
                        session: active.logical_session,
                        turn: active.logical_turn,
                        completion_token: active.completion_token.clone(),
                        failure: SanitizedRuntimeFailure::new(
                            RuntimeFailureClass::Contract,
                            "typed worker outcome contained protected session data",
                            false,
                        )
                        .map_err(|error| anyhow!(error.detail))?,
                    }
                } else {
                    SupervisorEvent::TurnCompleted {
                        expected_version: self.core.version(),
                        task_ordinal: active.task_ordinal,
                        role: active.role,
                        session: active.logical_session,
                        turn: active.logical_turn,
                        completion_token: active.completion_token.clone(),
                        outcome,
                    }
                }
            }
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
        self.active = None;
        // Two-phase commit, same as `execute_effects`: a verdict that closes a
        // task must not be committed unless the runtime actually closes. The
        // reviewer's verdict now lands here directly, so the guard has to live
        // here too.
        let mut next_core = self.core.clone();
        let effects = next_core
            .reduce(event)
            .map_err(|error| anyhow!(error.to_string()))?;
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
        Ok(())
    }

    fn execute_effects(&mut self, initial: Vec<SupervisorEffect>) -> Result<()> {
        let mut effects: VecDeque<_> = initial.into();
        while let Some(effect) = effects.pop_front() {
            let follow_up = match effect {
                SupervisorEffect::PublishStatus | SupervisorEffect::FinishSession { .. } => {
                    continue;
                }
                SupervisorEffect::ObserveRepository {
                    task_ordinal,
                    checkpoint,
                } => {
                    let observation = match self.observe_repository(task_ordinal) {
                        Ok(observation) => observation,
                        Err(error) => {
                            return self.fail_driver_effect(
                                task_ordinal,
                                DriverFailureClass::Repository,
                                error,
                            );
                        }
                    };
                    SupervisorEvent::RepositoryObserved {
                        expected_version: self.core.version(),
                        task_ordinal,
                        checkpoint,
                        observation,
                    }
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
            .task_environment(&task.spec.task_key, &paths)
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Environment, error))?;
        let materialized = environment
            .materialize_task_runtime(&self.epoch, &self.startup.run_id, &task.spec.task_key)
            .map_err(|error| RuntimeOpenFailure::new(DriverFailureClass::Environment, error))?;
        let request = RuntimeOpenRequest {
            task_ordinal,
            task_key: task.spec.task_key.clone(),
            repository_root,
            paths: clone_runtime_paths(&paths),
            auth_source: self.sources.codex_auth_source.clone().ok_or_else(|| {
                RuntimeOpenFailure::new(
                    DriverFailureClass::Environment,
                    anyhow!("Codex auth source is unavailable"),
                )
            })?,
            cargo_bin_source: self.sources.cargo_bin_source.clone(),
            rustup_home_source: self.sources.rustup_home_source.clone(),
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
        };
        let runtime = self.factory.open(request).map_err(|error| {
            RuntimeOpenFailure::new(DriverFailureClass::Runtime, anyhow!(error.detail))
        })?;
        if runtime.contract() != &self.factory.contract() {
            return Err(RuntimeOpenFailure::new(
                DriverFailureClass::Contract,
                anyhow!("opened task runtime differs from the frozen runtime contract"),
            ));
        }
        self.task_runtime = Some(OpenTaskRuntime {
            task_ordinal,
            _root: root,
            environment,
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
            prompt,
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

    fn observe_repository(&self, task_ordinal: usize) -> Result<RepositoryObservation> {
        let task = self
            .core
            .tasks()
            .get(task_ordinal)
            .ok_or_else(|| anyhow!("repository observation task ordinal is out of range"))?;
        let repository = self
            .repositories
            .get(Path::new(&task.spec.repository_root))
            .ok_or_else(|| anyhow!("task repository binding disappeared"))?;
        stable_repository_observation(repository, &task.expected_repository().head)
    }

    fn task_environment(
        &self,
        task_key: &str,
        paths: &TaskRuntimePaths,
    ) -> Result<ExecutionEnvironmentLease> {
        let cargo_home = self
            .sources
            .cargo_bin_source
            .parent()
            .ok_or_else(|| anyhow!("Rust cargo-bin source has no parent"))?;
        let mut override_names = vec![
            "CARGO_HOME",
            "CODEX_HOME",
            "HCOM_DIR",
            "HOME",
            "PYTHONPYCACHEPREFIX",
            "RUSTUP_HOME",
            "TMPDIR",
            "XDG_CACHE_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_RUNTIME_DIR",
            "XDG_STATE_HOME",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        override_names.sort();
        let mut required_names = override_names.clone();
        required_names.push("PATH".into());
        required_names.sort();
        let policy = EnvironmentPolicy::new(override_names, required_names)?;
        let overrides = vec![
            (
                "CARGO_HOME".into(),
                path_value("worker Cargo home", cargo_home)?,
            ),
            (
                "CODEX_HOME".into(),
                path_value("worker private CODEX_HOME", &paths.codex_home)?,
            ),
            (
                "HCOM_DIR".into(),
                path_value("worker private hcom directory", &paths.hcom)?,
            ),
            (
                "HOME".into(),
                path_value("worker private HOME", &paths.home)?,
            ),
            (
                "PYTHONPYCACHEPREFIX".into(),
                path_value(
                    "worker Python bytecode cache",
                    &paths.temp.join("python-pycache"),
                )?,
            ),
            (
                "RUSTUP_HOME".into(),
                path_value("worker Rustup home", &self.sources.rustup_home_source)?,
            ),
            (
                "TMPDIR".into(),
                path_value("worker temporary directory", &paths.temp)?,
            ),
            (
                "XDG_CACHE_HOME".into(),
                path_value("worker XDG cache", &paths.xdg_cache)?,
            ),
            (
                "XDG_CONFIG_HOME".into(),
                path_value("worker XDG config", &paths.xdg_config)?,
            ),
            (
                "XDG_DATA_HOME".into(),
                path_value("worker XDG data", &paths.xdg_data)?,
            ),
            (
                "XDG_RUNTIME_DIR".into(),
                path_value("worker XDG runtime", &paths.runtime)?,
            ),
            (
                "XDG_STATE_HOME".into(),
                path_value("worker XDG state", &paths.xdg_state)?,
            ),
        ];
        let lease = ExecutionEnvironmentLease::capture_complete(
            format!("exec-lease-{}", Uuid::new_v4()),
            &self.epoch,
            &policy,
            &self.sources.parent_environment,
            overrides,
        )
        .with_context(|| format!("failed to capture exec worker environment for {task_key}"))?;
        let auth_source = self
            .sources
            .codex_auth_source
            .as_deref()
            .ok_or_else(|| anyhow!("Codex auth source is unavailable"))?;
        lease.with_secret_redaction_values(codex_auth_redaction_values(auth_source)?)
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
        prompt.push_str(&format!(
            "# Task {task_ordinal}: {}\n\n{}\n\n## Context\n\n- repository: {}\n- project directory: {}\n",
            spec.title,
            spec.objective,
            spec.repository_root,
            self.startup.project_root.display()
        ));
        if let Some(base) = task.base_revision.as_deref() {
            prompt.push_str(&format!("- base revision: {base}\n"));
        }
        if let Some(head) = task.head_revision.as_deref() {
            prompt.push_str(&format!("- current head: {head}\n"));
        }
        if !spec.acceptance_criteria.is_empty() {
            prompt.push_str("\n## Acceptance criteria\n\n");
            for item in &spec.acceptance_criteria {
                prompt.push_str(&format!("- {item}\n"));
            }
        }
        if !spec.required_checks.is_empty() {
            prompt.push_str("\n## Required checks\n\n");
            for item in &spec.required_checks {
                prompt.push_str(&format!("- {item}\n"));
            }
        }
        if !spec.allowed_paths.is_empty() {
            prompt.push_str("\n## Paths in scope\n\n");
            for item in &spec.allowed_paths {
                prompt.push_str(&format!("- {item}\n"));
            }
        }
        if !spec.forbidden_actions.is_empty() {
            prompt.push_str("\n## Forbidden actions\n\n");
            for item in &spec.forbidden_actions {
                prompt.push_str(&format!("- {item}\n"));
            }
        }

        // Relay the peer's previous message verbatim (already redacted and
        // bounded upstream). hcom does not interpret it.
        match role {
            WorkerRole::Developer => {
                if purpose == RuntimeTurnPurpose::DeveloperCorrection
                    && let Some(review) = task.last_reviewer_outcome()
                {
                    prompt.push_str(
                        "\n## Reviewer response to your previous work\n\nThe reviewer requested \
                         changes. Their full message follows verbatim.\n\n---\n\n",
                    );
                    prompt.push_str(&bounded_relay(&review.summary));
                    prompt.push_str("\n\n---\n\nAddress it, then commit and report as before.\n");
                }
            }
            WorkerRole::Reviewer => {
                if let Some(developer) = task.last_developer_outcome() {
                    prompt.push_str(
                        "\n## Developer report\n\nThe developer's full final message follows \
                         verbatim.\n\n---\n\n",
                    );
                    prompt.push_str(&bounded_relay(&developer.summary));
                    prompt.push_str("\n\n---\n");
                }
                prompt.push_str(REVIEWER_OUTPUT_CONTRACT);
            }
        }

        if prompt.len() > crate::worker::runtime::MAX_RUNTIME_PROMPT_BYTES {
            bail!("rendered task turn prompt exceeds its 256 KiB bound");
        }
        Ok(prompt)
    }

    fn outcome_contains_sensitive_value(
        &self,
        task_ordinal: usize,
        prompt: &str,
        outcome: &RuntimeOutcome,
    ) -> bool {
        let Some(runtime) = self
            .task_runtime
            .as_ref()
            .filter(|runtime| runtime.task_ordinal == task_ordinal)
        else {
            return true;
        };
        let redactor = runtime.environment.redactor().with_value(prompt);
        let value = match serde_json::to_value(outcome) {
            Ok(value) => value,
            Err(_) => return true,
        };
        json_contains_sensitive_value(&value, &redactor)
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

fn plan_bindings(
    tasks: &[TaskDraft],
    retained: &BTreeMap<PathBuf, ManagedRepository>,
    staged: &BTreeMap<PathBuf, ManagedRepository>,
) -> Result<Vec<TaskRepositoryBinding>> {
    tasks
        .iter()
        .map(|task| {
            let repository = retained
                .get(Path::new(&task.repository_root))
                .or_else(|| staged.get(Path::new(&task.repository_root)))
                .ok_or_else(|| anyhow!("task repository binding disappeared"))?;
            let observation = stable_repository_observation(repository, &repository.current_head)?;
            Ok(TaskRepositoryBinding {
                task_key: task.task_key.clone(),
                observation,
            })
        })
        .collect()
}

fn stable_repository_observation(
    repository: &ManagedRepository,
    expected_head: &str,
) -> Result<RepositoryObservation> {
    capture_repository_observation(repository, expected_head)
}

/// Routing data, not a verdict.
///
/// The task-agnostic supervisor needs one thing from Git: the revision to hand
/// the reviewer as the base of its diff. It deliberately does NOT collect
/// status, diffs, or changed-path sets — a dirty tree, an out-of-scope commit,
/// a detached HEAD, or a rewritten history are all things the reviewer and the
/// human judge, and collecting them here would make an unstable working tree
/// fail the run instead of reaching review.
fn capture_repository_observation(
    managed: &ManagedRepository,
    expected_head: &str,
) -> Result<RepositoryObservation> {
    crate::worker::validation::validate_git_oid("expected repository revision", expected_head)?;
    let repository = &managed.repository;
    repository.revalidate_identity()?;
    let runner = GitRunner {
        git: &repository.git,
        root: &repository.root,
    };
    let head = repository.head()?;
    // A detached HEAD is legitimate; report it as such instead of failing.
    let branch = repository.branch().unwrap_or_else(|_| "HEAD".to_string());
    let ancestry = runner.run(&["merge-base", "--is-ancestor", expected_head, &head])?;
    if !ancestry.stderr.is_empty() || !matches!(ancestry.status.code(), Some(0) | Some(1)) {
        bail!("failed to compare the task revision with HEAD");
    }
    let head_descends_from_expected = ancestry.status.success();
    Ok(RepositoryObservation {
        repository_root: path_value("task repository root", &repository.root)?,
        identity_hash: sha256_hex(&serde_json::to_vec(&(
            "hcom-exec-repository-identity-v1",
            &repository.root,
            (
                repository.root_identity.device,
                repository.root_identity.inode,
                repository.root_identity.uid,
                repository.root_identity.mode,
            ),
            (
                repository.git_dir.device,
                repository.git_dir.inode,
                repository.git_dir.uid,
                repository.git_dir.mode,
            ),
            &repository.git,
        ))?),
        branch,
        head,
        tracked_diff_hash: EMPTY_OBSERVATION_HASH.into(),
        index_diff_hash: EMPTY_OBSERVATION_HASH.into(),
        untracked_status_hash: EMPTY_OBSERVATION_HASH.into(),
        clean: true,
        changed_paths: Vec::new(),
        head_descends_from_expected,
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

#[derive(Serialize)]
struct TaskCodexConfig {
    mcp_servers: BTreeMap<String, toml::Value>,
    projects: BTreeMap<String, TaskCodexProject>,
    shell_environment_policy: TaskShellEnvironmentPolicy,
}

#[derive(Serialize)]
struct TaskCodexProject {
    trust_level: &'static str,
}

#[derive(Serialize)]
struct TaskShellEnvironmentPolicy {
    inherit: &'static str,
    ignore_default_excludes: bool,
}

fn write_private_codex_config(path: &Path, repository_root: &Path) -> Result<()> {
    let repository_root = path_value("task repository root", repository_root)?;
    let config = TaskCodexConfig {
        mcp_servers: BTreeMap::new(),
        projects: [(
            repository_root,
            TaskCodexProject {
                trust_level: "untrusted",
            },
        )]
        .into_iter()
        .collect(),
        shell_environment_policy: TaskShellEnvironmentPolicy {
            inherit: "all",
            ignore_default_excludes: true,
        },
    };
    let contents = toml::to_string(&config)?.into_bytes();
    if contents.len() > 16 * 1024 {
        bail!("task-private Codex config exceeds its 16 KiB bound");
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    file.write_all(&contents)?;
    file.flush()?;
    Ok(())
}

fn clone_runtime_paths(paths: &TaskRuntimePaths) -> TaskRuntimePaths {
    TaskRuntimePaths {
        home: paths.home.clone(),
        codex_home: paths.codex_home.clone(),
        temp: paths.temp.clone(),
        runtime: paths.runtime.clone(),
        artifacts: paths.artifacts.clone(),
        hcom: paths.hcom.clone(),
        xdg_config: paths.xdg_config.clone(),
        xdg_state: paths.xdg_state.clone(),
        xdg_cache: paths.xdg_cache.clone(),
        xdg_data: paths.xdg_data.clone(),
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
not required). Judge how deeply to verify: the repository is read-only to you,
but your sandbox is writable, so you may copy it elsewhere and build or test it
if you want independent evidence.
";

fn role_instructions(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => {
            "You are the task Developer. Work directly in the exact repository and complete the bounded task. Run whatever checks the task requires, commit your work, and do not push, install, or wait for interactive input. Your final message is your report to the reviewer: state what you changed, what you verified, and anything you left undone."
        }
        WorkerRole::Reviewer => {
            "You are the task Reviewer. Independently inspect the committed task range and decide whether it is sound. You must not edit reviewed source, stage, commit, change branch or HEAD, push, or install; verifying by copying the tree into your own writable sandbox is allowed and encouraged when it helps."
        }
    }
}

/// Relay a peer message into the next prompt: bounded view of an already
/// redacted string, truncated with an explicit marker rather than rejected.
fn bounded_relay(text: &str) -> String {
    const RELAY_CAP: usize = 64 * 1024;
    if text.len() <= RELAY_CAP {
        return text.to_string();
    }
    let mut end = RELAY_CAP;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n\n[hcom: truncated at {end} bytes; the full message is on disk in this run's artifacts]",
        &text[..end]
    )
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

fn codex_auth_redaction_values(path: &Path) -> Result<Vec<String>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take((MAX_AUTH_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_AUTH_FILE_BYTES {
        bail!("Codex auth source exceeds its bounded size");
    }
    let value: Value = serde_json::from_slice(&bytes).context("Codex auth source is not JSON")?;
    let mut values = BTreeSet::new();
    collect_auth_strings(&value, &mut values)?;
    Ok(values.into_iter().collect())
}

fn collect_auth_strings(value: &Value, values: &mut BTreeSet<String>) -> Result<()> {
    match value {
        Value::String(value)
            if value.len() >= 8
                && value.len() <= 16 * 1024
                && !value.chars().any(char::is_control) =>
        {
            if values.len() >= MAX_AUTH_REDACTION_VALUES && !values.contains(value) {
                bail!("Codex auth redaction inventory exceeds 64 values");
            }
            values.insert(value.clone());
        }
        Value::Array(items) => {
            for item in items {
                collect_auth_strings(item, values)?;
            }
        }
        Value::Object(object) => {
            for item in object.values() {
                collect_auth_strings(item, values)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn json_contains_sensitive_value(
    value: &Value,
    redactor: &crate::worker::environment::SecretRedactor,
) -> bool {
    match value {
        Value::String(value) => redactor.would_redact(value),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_sensitive_value(value, redactor)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_sensitive_value(value, redactor)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::environment::ParentEnvironment;
    use crate::worker::fake_runtime::{FakeTaskWorkerRuntime, FakeTurnScript};
    use crate::worker::profile::{
        ArchitectAdapter, DeveloperInvocationProfile, ReviewerInvocationProfile,
        SessionInvocationProfiles,
    };
    use crate::worker::runtime::{
        DeveloperOutcomeStatus, DeveloperOutcomeV1, ReviewFindingSeverity, ReviewFindingV1,
        ReviewerOutcomeV1, ReviewerVerdict, RuntimeTelemetry,
    };
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::process::Command;
    use std::sync::{Arc, Mutex};

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
        ReplaceGitDirectory,
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
        fn contract(&self) -> RuntimeContractIdentity {
            RuntimeContractIdentity::codex_exec_0_146()
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
        fn contract(&self) -> RuntimeContractIdentity {
            RuntimeContractIdentity::codex_exec_0_146()
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
        lock_root: PathBuf,
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
            let lock_root = root.join("locks");
            let project_root = root.join("project");
            let repository = root.join("repository");
            let toolchain = root.join("toolchain");
            for directory in [
                &run_root,
                &lock_root,
                &project_root,
                &repository,
                &toolchain,
            ] {
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

            let auth = root.join("codex-auth.json");
            fs::write(
                &auth,
                br#"{"OPENAI_API_KEY":"fixture-auth-secret-value","account_id":"fixture-account-value"}"#,
            )
            .unwrap();
            fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
            let auth = fs::canonicalize(auth).unwrap();

            let mut sources = SessionRuntimeSources::fake(&toolchain);
            sources.profiles =
                Some(SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap());
            sources.codex_auth_source = Some(auth);
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
            ]);
            Self {
                _temp: temp,
                run_root,
                lock_root,
                project_root,
                repository,
                sources,
            }
        }

        fn task(&self, key: &str, allowed_paths: &[&str], max_rounds: u8) -> TaskDraft {
            TaskDraft {
                task_key: key.into(),
                title: format!("Task {key}"),
                objective: format!("Implement {key}"),
                repository_root: self.repository.to_string_lossy().into_owned(),
                acceptance_criteria: vec![format!("{key} is complete")],
                required_checks: vec!["cargo test --locked".into()],
                allowed_paths: allowed_paths.iter().map(|path| (*path).into()).collect(),
                forbidden_actions: vec!["do not push or install".into()],
                max_review_rounds: max_rounds,
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
                self.lock_root.clone(),
                self.sources.clone(),
                Box::new(ScriptedFactory {
                    scripts: scripts.into(),
                    audit,
                }),
            )
            .unwrap()
        }
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
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        /// Cheap model for acceptance runs; production defaults stay untouched.
        const TEST_MODEL: &str = "gpt-5.3-codex-spark";
        const TEST_EFFORT: &str = "medium";

        pub(crate) struct RealFixture {
            pub(crate) _temp: tempfile::TempDir,
            pub(crate) project_root: PathBuf,
            pub(crate) repository: PathBuf,
            run_root: PathBuf,
            lock_root: PathBuf,
            sources: SessionRuntimeSources,
        }

        impl RealFixture {
            pub(crate) fn new(label: &str) -> Self {
                let temp = tempfile::Builder::new()
                    .prefix(&format!("hcom-real-exec-{label}."))
                    .tempdir()
                    .unwrap();
                let root = fs::canonicalize(temp.path()).unwrap();
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
                let run_root = root.join("run");
                let lock_root = root.join("locks");
                let project_root = root.join("project");
                let repository = project_root.join("repository");
                for directory in [&run_root, &lock_root, &project_root, &repository] {
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
                sources.codex_auth_source =
                    Some(fs::canonicalize(format!("{home}/.codex/auth.json")).unwrap());
                sources.cargo_bin_source = PathBuf::from(format!("{home}/.cargo/bin"));
                sources.rustup_home_source = PathBuf::from(format!("{home}/.rustup"));
                // Complete parent inheritance, exactly like production.
                sources.parent_environment = ParentEnvironment::capture_current();

                Self {
                    _temp: temp,
                    project_root,
                    repository,
                    run_root,
                    lock_root,
                    sources,
                }
            }

            pub(crate) fn supervisor(&self) -> TaskLaneSupervisor {
                TaskLaneSupervisor::open(
                    "run-real-exec".into(),
                    self.project_root.clone(),
                    self.run_root.clone(),
                    self.lock_root.clone(),
                    self.sources.clone(),
                )
                .unwrap()
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

            pub(crate) fn run(
                &self,
                supervisor: &mut TaskLaneSupervisor,
                tasks: Vec<TaskDraft>,
            ) -> SessionStatusSnapshot {
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

    fn completed(outcome: RuntimeOutcome) -> RuntimeTurnPoll {
        RuntimeTurnPoll::Completed {
            outcome,
            telemetry: RuntimeTelemetry::default(),
        }
    }

    fn ready(summary: &str) -> RuntimeTurnPoll {
        completed(RuntimeOutcome::Developer(DeveloperOutcomeV1 {
            status: DeveloperOutcomeStatus::Ready,
            summary: summary.into(),
            questions: Vec::new(),
        }))
    }

    fn lgtm(summary: &str) -> RuntimeTurnPoll {
        completed(RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
            verdict: ReviewerVerdict::Lgtm,
            summary: summary.into(),
            findings: Vec::new(),
        }))
    }

    fn request_changes(message: &str) -> RuntimeTurnPoll {
        completed(RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
            verdict: ReviewerVerdict::RequestChanges,
            summary: "changes required".into(),
            findings: vec![ReviewFindingV1 {
                severity: ReviewFindingSeverity::Major,
                path: Some("src/task.txt".into()),
                line: Some(1),
                message: message.into(),
            }],
        }))
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
            Mutation::ReplaceGitDirectory => {
                let original = repository.join(".git");
                let moved = repository.join(".git-replaced");
                fs::rename(&original, &moved)?;
                let output = Command::new("/bin/cp")
                    .args(["-a", "--"])
                    .arg(&moved)
                    .arg(&original)
                    .env_clear()
                    .env("LC_ALL", "C")
                    .output()?;
                if !output.status.success() {
                    bail!("failed to replace disposable Git directory");
                }
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

    fn git_output(repository: &Path, arguments: &[&str]) -> String {
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
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
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
    fn task_private_codex_config_is_closed_and_marks_the_exact_repository_untrusted() {
        let fixture = Fixture::new();
        let (_root, paths) =
            TaskRuntimePaths::create(&fixture.run_root, 0, "config", &fixture.repository).unwrap();
        let config_path = paths.codex_home.join("config.toml");
        let encoded = fs::read_to_string(&config_path).unwrap();
        let config: toml::Table = toml::from_str(&encoded).unwrap();
        let repository = fixture.repository.to_str().unwrap();
        assert_eq!(
            config["projects"][repository]["trust_level"].as_str(),
            Some("untrusted")
        );
        assert_eq!(
            config["shell_environment_policy"]["inherit"].as_str(),
            Some("all")
        );
        assert_eq!(
            config["shell_environment_policy"]["ignore_default_excludes"].as_bool(),
            Some(true)
        );
        assert!(config["mcp_servers"].as_table().unwrap().is_empty());
        assert_eq!(
            fs::metadata(config_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
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
    fn auth_redaction_file_and_value_inventory_bounds_are_exact() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        for (bytes, accepted) in [
            (MAX_AUTH_FILE_BYTES - 1, true),
            (MAX_AUTH_FILE_BYTES, true),
            (MAX_AUTH_FILE_BYTES + 1, false),
        ] {
            let encoded = format!("\"{}\"", "x".repeat(bytes - 2));
            assert_eq!(encoded.len(), bytes);
            fs::write(&path, encoded).unwrap();
            assert_eq!(codex_auth_redaction_values(&path).is_ok(), accepted);
        }
        for (values, accepted) in [
            (MAX_AUTH_REDACTION_VALUES - 1, true),
            (MAX_AUTH_REDACTION_VALUES, true),
            (MAX_AUTH_REDACTION_VALUES + 1, false),
        ] {
            let encoded: Vec<_> = (0..values)
                .map(|index| format!("secret-{index:04}"))
                .collect();
            fs::write(&path, serde_json::to_vec(&encoded).unwrap()).unwrap();
            assert_eq!(codex_auth_redaction_values(&path).is_ok(), accepted);
        }
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
    fn request_changes_round_relays_the_full_reviewer_message_to_the_developer() {
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
                    [request_changes("the overflow case is unhandled")],
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
        // The reviewer sees the developer's report; the correction turn sees
        // the reviewer's message verbatim.
        let review_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Reviewer && *purpose == RuntimeTurnPurpose::InitialReview
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("initial review prompt");
        assert!(review_prompt.contains("first attempt: added the module"));
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
        assert!(correction_prompt.contains("changes required"));
        let rereview_prompt = audit
            .prompts
            .iter()
            .find(|(role, purpose, _)| {
                *role == WorkerRole::Reviewer && *purpose == RuntimeTurnPurpose::ReviewerRereview
            })
            .map(|(_, _, prompt)| prompt.clone())
            .expect("re-review prompt");
        assert!(rereview_prompt.contains("second attempt: handled overflow"));
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
        let second_base = git_output(&second, &["rev-parse", "HEAD"]);

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
            snapshot.tasks[1].base_revision.as_deref(),
            Some(second_base.as_str())
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
    fn complete_parent_environment_is_materialized_with_task_private_overrides() {
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
        assert_ne!(get(b"HCOM_DIR"), Some(b"/must/not/reach/task".as_slice()));
        assert_eq!(get(b"HCOM_WORKER_ROLE"), Some(b"task-runtime".as_slice()));
        for name in [
            b"HOME".as_slice(),
            b"CODEX_HOME".as_slice(),
            b"TMPDIR".as_slice(),
            b"XDG_RUNTIME_DIR".as_slice(),
            b"XDG_CACHE_HOME".as_slice(),
        ] {
            assert!(
                get(name)
                    .unwrap()
                    .starts_with(fixture.run_root.as_os_str().as_bytes())
            );
        }
    }

    #[test]
    fn independent_codex_role_overrides_are_frozen_into_runtime_turns() {
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
    fn out_of_scope_commit_routes_to_review_and_sensitive_outcome_fails_closed() {
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

        // The secret-leak screen on outcomes remains a hard stop.
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "secret",
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [ready("environment-secret-sentinel")],
            )],
            vec![Mutation::Commit {
                path: "src/task.txt",
                contents: "done\n",
            }],
        );
        let mut supervisor = fixture.supervisor(vec![script], audit);
        start(&mut supervisor, vec![fixture.task("secret", &["src"], 2)]);
        let snapshot = drive_terminal(&mut supervisor);
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        let detail = snapshot.terminal_detail.clone().unwrap();
        assert!(
            detail.starts_with("worker runtime contract failed"),
            "{detail}"
        );
        // The leak screen fires before any outcome text is relayed, so the
        // sentinel must not appear anywhere in the report.
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
    fn repository_identity_replacement_is_a_driver_failure_not_a_worker_verdict() {
        let fixture = Fixture::new();
        let audit = Arc::new(Mutex::new(Audit::default()));
        let script = task_script(
            "identity-drift",
            vec![FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [ready("must not be accepted")],
            )],
            vec![Mutation::ReplaceGitDirectory],
        );
        let mut supervisor = fixture.supervisor(vec![script], Arc::clone(&audit));
        start(
            &mut supervisor,
            vec![fixture.task("identity-drift", &["src"], 2)],
        );
        assert!(supervisor.poll_once().is_err());
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("repository observation failed")
        );
        assert!(
            audit
                .lock()
                .unwrap()
                .sessions
                .iter()
                .all(|(_, role, _)| *role == WorkerRole::Developer)
        );
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
    fn a_detached_or_dirty_checkout_still_binds_but_a_non_top_level_root_does_not() {
        // Working-tree condition is the reviewer's and the human's business.
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

        // Repository identity is still exact: a subdirectory is not a root.
        let nested = Fixture::new();
        let mut task = nested.task("nested-root", &["src"], 2);
        task.repository_root = nested.repository.join("src").to_string_lossy().into_owned();
        let mut supervisor = nested.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
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
    fn failed_plan_replacement_preserves_the_previous_plan_and_repository_lock() {
        let fixture = Fixture::new();
        let second = fixture
            .project_root
            .parent()
            .unwrap()
            .join("replacement-locked-repository");
        fs::create_dir(&second).unwrap();
        fs::set_permissions(&second, fs::Permissions::from_mode(0o700)).unwrap();
        git(&second, &["init", "-b", "master"]);
        fs::create_dir(second.join("src")).unwrap();
        fs::write(second.join("src/seed.txt"), "seed\n").unwrap();
        git(&second, &["add", "--", "src/seed.txt"]);
        git_commit(&second, "Initial replacement fixture");
        let second = fs::canonicalize(second).unwrap();

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

        let mut blocker = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let mut blocked_task = fixture.task("blocked-root", &["src"], 2);
        blocked_task.repository_root = second.to_string_lossy().into_owned();
        blocker
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                CODEX_TASK_WORKER_ADAPTER,
                vec![blocked_task.clone()],
            )
            .unwrap();

        assert!(
            supervisor
                .replace_plan(
                    before.version,
                    CODEX_TASK_WORKER_ADAPTER,
                    CODEX_TASK_WORKER_ADAPTER,
                    vec![blocked_task],
                )
                .is_err()
        );
        assert_eq!(supervisor.snapshot(), before);

        let mut probe = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        assert!(
            probe
                .replace_plan(
                    0,
                    CODEX_TASK_WORKER_ADAPTER,
                    CODEX_TASK_WORKER_ADAPTER,
                    vec![retained_task],
                )
                .is_err()
        );

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
            fixture.lock_root.clone(),
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
    fn missing_runtime_source_is_classified_as_environment_setup_failure() {
        let mut fixture = Fixture::new();
        fixture.sources.codex_auth_source = None;
        let mut supervisor = fixture.supervisor(Vec::new(), Arc::new(Mutex::new(Audit::default())));
        let task = fixture.task("missing-auth", &["src"], 2);
        let (plan_version, plan_hash) = supervisor
            .replace_plan(
                0,
                CODEX_TASK_WORKER_ADAPTER,
                CODEX_TASK_WORKER_ADAPTER,
                vec![task],
            )
            .unwrap();
        assert!(
            supervisor
                .approve_and_start(1, plan_version, &plan_hash, true)
                .is_err()
        );
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.state, SessionState::NeedsHuman);
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("task-private environment setup failed")
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
}

/// Real-Codex acceptance for the exec lane.
///
/// Opt-in; runs the pinned 0.146 binary against a disposable project with the
/// cheap test model. Never touches an existing user terminal or session.
///
///   cargo test --lib real_exec -- --ignored --nocapture --test-threads=1
#[cfg(test)]
mod real_exec_tests {
    use super::tests::real_support::*;
    use super::*;
    use crate::control_api::TaskState;

    #[test]
    #[ignore = "requires the pinned real codex binary, auth, and network"]
    fn real_single_task_developer_then_reviewer_reaches_lgtm() {
        let fixture = RealFixture::new("fib");
        let mut supervisor = fixture.supervisor();
        let task = TaskDraft {
            task_key: "fib".into(),
            title: "Add a fibonacci function".into(),
            objective: "Create fib.py in the repository root containing a function \
                        fib(n) that returns the nth Fibonacci number (fib(0)=0, fib(1)=1). \
                        Commit it with git."
                .into(),
            repository_root: fixture.repository.to_string_lossy().into_owned(),
            acceptance_criteria: vec!["fib.py exists and is committed".into()],
            required_checks: vec!["python3 -c \"import fib; print(fib.fib(10))\"".into()],
            allowed_paths: vec!["fib.py".into()],
            forbidden_actions: vec!["do not push".into()],
            max_review_rounds: 2,
        };
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
    #[ignore = "requires the pinned real codex binary, auth, and network"]
    fn real_gate_one_review_loop_then_direct_approval_in_one_run() {
        let fixture = RealFixture::new("gate1");
        let mut supervisor = fixture.supervisor();
        let repository = fixture.repository.to_string_lossy().into_owned();
        let tasks = vec![
            TaskDraft {
                task_key: "staged".into(),
                title: "Add add() with its test".into(),
                objective: "Create calc.py in the repository root containing a function \
                            add(a, b) that returns a + b, and commit it. On this first \
                            turn create ONLY calc.py — do not write any test file yet; \
                            a later turn covers the tests."
                    .into(),
                repository_root: repository.clone(),
                acceptance_criteria: vec![
                    "calc.py defines add(a, b) returning a + b".into(),
                    "test_calc.py exists and asserts add(2, 3) == 5; the task is \
                     incomplete without it and must be rejected"
                        .into(),
                ],
                required_checks: vec!["python3 test_calc.py".into()],
                allowed_paths: vec!["calc.py".into(), "test_calc.py".into()],
                forbidden_actions: vec!["do not push".into()],
                max_review_rounds: 3,
            },
            TaskDraft {
                task_key: "direct".into(),
                title: "Add a greeting helper".into(),
                objective: "Create greet.py in the repository root with a function \
                            greet(name) returning the string \"hello <name>\", and \
                            commit it."
                    .into(),
                repository_root: repository,
                acceptance_criteria: vec!["greet.py defines greet(name)".into()],
                required_checks: vec!["python3 -c \"import greet\"".into()],
                allowed_paths: vec!["greet.py".into()],
                forbidden_actions: vec!["do not push".into()],
                max_review_rounds: 3,
            },
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
        assert!(fixture.repository.join("greet.py").is_file());
        assert!(
            fixture.stray_worker_pids().is_empty(),
            "workers outlived the run: {:?}",
            fixture.stray_worker_pids()
        );
    }

    /// End-to-end acceptance on a real Rust project: the developer writes and
    /// commits a hello-world crate, the reviewer independently judges it, and
    /// the run reaches LGTM with durable evidence on disk.
    #[test]
    #[ignore = "requires the pinned real codex binary, auth, and network"]
    fn real_rust_hello_world_task_reaches_lgtm_with_evidence() {
        let fixture = RealFixture::new("hello");
        let mut supervisor = fixture.supervisor();
        let task = TaskDraft {
            task_key: "hello".into(),
            title: "Create a hello world Rust binary".into(),
            objective: "In the repository root create a Cargo binary crate: \
                        Cargo.toml naming the package `hello` with edition 2021, and \
                        src/main.rs whose main function prints exactly `Hello, world!`. \
                        Verify it with `cargo run`, then commit both files with git."
                .into(),
            repository_root: fixture.repository.to_string_lossy().into_owned(),
            acceptance_criteria: vec![
                "cargo run prints Hello, world!".into(),
                "Cargo.toml and src/main.rs are committed".into(),
            ],
            required_checks: vec!["cargo run".into()],
            allowed_paths: vec!["Cargo.toml".into(), "src".into()],
            forbidden_actions: vec!["do not push".into()],
            max_review_rounds: 2,
        };
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
    #[ignore = "requires the pinned real codex binary, auth, and network"]
    fn real_two_task_run_advances_automatically() {
        let fixture = RealFixture::new("two");
        let mut supervisor = fixture.supervisor();
        let tasks = vec![
            TaskDraft {
                task_key: "greet".into(),
                title: "Add a greeting module".into(),
                objective: "Create greet.py in the repository root with a function \
                            greet(name) returning the string \"hello <name>\". Commit it."
                    .into(),
                repository_root: fixture.repository.to_string_lossy().into_owned(),
                acceptance_criteria: vec!["greet.py exists and is committed".into()],
                required_checks: vec!["python3 -c \"import greet\"".into()],
                allowed_paths: vec!["greet.py".into()],
                forbidden_actions: vec!["do not push".into()],
                max_review_rounds: 2,
            },
            TaskDraft {
                task_key: "square".into(),
                title: "Add a square module".into(),
                objective: "Create square.py in the repository root with a function \
                            square(n) returning n*n. Commit it."
                    .into(),
                repository_root: fixture.repository.to_string_lossy().into_owned(),
                acceptance_criteria: vec!["square.py exists and is committed".into()],
                required_checks: vec!["python3 -c \"import square\"".into()],
                allowed_paths: vec!["square.py".into()],
                forbidden_actions: vec!["do not push".into()],
                max_review_rounds: 2,
            },
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
