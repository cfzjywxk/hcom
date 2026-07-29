//! Durable single-writer scheduler and the `hcomd` service entry point.
//
// The daemon endpoint owns this engine and its sole Store v1 handle. Phase 4
// exercises the same engine through fake workers; typed public project actions
// intentionally remain disconnected until Phase 8.
#![cfg_attr(not(test), allow(dead_code))]

use crate::artifact::{ArtifactRoot, ArtifactScope, ManifestMetadata};
use crate::control_api::daemon::{ControlPaths, DaemonEndpoint};
use crate::control_api::peer::{boot_identity, process_birth_identity};
use crate::control_api::{ControlErrorCode, ControlResponse, WorkerRole};
use crate::project_store::{
    ClaimedTurn, DaemonEpoch, DaemonStore, ProjectControlLayout, ProjectExecutionState, ReadyTurn,
    SchedulerSnapshot, TurnKind, now_epoch_seconds, sha256_hex,
};
use crate::worker::contract::{NativeResult, TurnControl, WorkerAdapterRegistry};
use crate::worker::environment::{ExecutionEnvironmentLease, WorkerEnvironmentIdentity};
use crate::worker::process::{HeartbeatControl, ProcessRunner, WorkerTermination};
use crate::worker::{prepare_create_turn, prepare_resume_turn};
use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use uuid::Uuid;

const TURN_LEASE_DURATION: Duration = Duration::from_secs(30);

pub fn run_hcomd_service() -> Result<()> {
    if [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO]
        .into_iter()
        .any(fd_is_tty)
    {
        bail!("hcomd refuses to attach to an interactive terminal");
    }
    let mut endpoint = DaemonEndpoint::bind(ControlPaths::discover()?)?;
    endpoint.set_nonblocking(true)?;
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, stopping.clone())?;
    }
    while !stopping.load(Ordering::Acquire) {
        if !endpoint.try_serve_one()? {
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    Ok(())
}

fn fd_is_tty(fd: i32) -> bool {
    // SAFETY: isatty only inspects the supplied integer file descriptor.
    unsafe { libc::isatty(fd) == 1 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchedulerStep {
    Progress,
    Idle,
    Terminal,
    InjectedStop,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SchedulerMetrics {
    pub(crate) current_developers: u64,
    pub(crate) current_reviewers: u64,
    pub(crate) max_live_developers: u64,
    pub(crate) max_live_reviewers: u64,
    pub(crate) developer_spawns: u64,
    pub(crate) reviewer_spawns: u64,
    pub(crate) heartbeats: u64,
    pub(crate) controlled_cancellations: u64,
    pub(crate) controlled_pauses: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpawnAudit {
    pub(crate) task_id: String,
    pub(crate) role: WorkerRole,
    pub(crate) turn_sequence: u32,
    pub(crate) workspace_cwd: PathBuf,
    pub(crate) argv_hash: String,
    pub(crate) prompt_hash: String,
    pub(crate) prompt_in_argv: bool,
    pub(crate) developer_path_exposed: bool,
}

pub(crate) struct DurableScheduler {
    store: DaemonStore,
    epoch: String,
    retire_epoch_on_drop: bool,
    adapters: WorkerAdapterRegistry,
    artifacts: ArtifactRoot,
    environments: BTreeMap<String, ExecutionEnvironmentLease>,
    runner: ProcessRunner,
    metrics: SchedulerMetrics,
    spawn_audit: Vec<SpawnAudit>,
    fail_after_result_ready_once: bool,
    cancel_on_next_heartbeat: bool,
    pause_on_next_heartbeat: bool,
}

impl DurableScheduler {
    pub(crate) fn open(
        layout: &ProjectControlLayout,
        artifact_root: impl AsRef<Path>,
        adapters: WorkerAdapterRegistry,
        runner: ProcessRunner,
    ) -> Result<Self> {
        let mut store = DaemonStore::open(layout)?;
        let artifact_root = artifact_root.as_ref();
        match fs::symlink_metadata(artifact_root) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(artifact_root)?;
                fs::set_permissions(artifact_root, fs::Permissions::from_mode(0o700))?;
            }
            Err(error) => return Err(error).context("failed to inspect scheduler artifact root"),
        }
        let artifacts = ArtifactRoot::open(artifact_root)?;
        let epoch = format!("daemon-{}", Uuid::new_v4());
        let interrupted = store.start_daemon_epoch(&DaemonEpoch {
            id: epoch.clone(),
            boot_id: boot_identity()?,
            daemon_pid: std::process::id(),
            process_birth: process_birth_identity(std::process::id())?,
        })?;
        for request in interrupted {
            let response = ControlResponse::error(
                &request.request_id,
                ControlErrorCode::NeedsRecovery,
                "control request was interrupted by a daemon restart; inspect durable state",
            );
            let response_json = serde_json::to_string(&response)?;
            let response_hash = sha256_hex(response_json.as_bytes());
            store.complete_control_request(
                &request.daemon_epoch,
                &request.caller_key_hash,
                &request.request_id,
                &request.payload_hash,
                &response_json,
                &response_hash,
            )?;
        }
        store.recover_daemon_state(&epoch)?;
        Ok(Self {
            store,
            epoch,
            retire_epoch_on_drop: true,
            adapters,
            artifacts,
            environments: BTreeMap::new(),
            runner,
            metrics: SchedulerMetrics::default(),
            spawn_audit: Vec::new(),
            fail_after_result_ready_once: false,
            cancel_on_next_heartbeat: false,
            pause_on_next_heartbeat: false,
        })
    }

    pub(crate) fn daemon_epoch(&self) -> &str {
        &self.epoch
    }

    pub(crate) fn store(&self) -> &DaemonStore {
        &self.store
    }

    pub(crate) fn store_mut(&mut self) -> &mut DaemonStore {
        &mut self.store
    }

    pub(crate) fn attach_environment(
        &mut self,
        project_id: &str,
        environment: ExecutionEnvironmentLease,
    ) -> Result<()> {
        environment.descriptor().require_daemon_epoch(&self.epoch)?;
        self.store
            .record_environment_lease(project_id, environment.descriptor())?;
        self.environments.insert(project_id.to_owned(), environment);
        Ok(())
    }

    pub(crate) fn request_run(
        &mut self,
        project_id: &str,
        expected_version: u64,
        plan_version: u64,
        plan_hash: &str,
        environment: ExecutionEnvironmentLease,
    ) -> Result<()> {
        self.attach_environment(project_id, environment)?;
        self.store.request_project_run(
            project_id,
            expected_version,
            plan_version,
            plan_hash,
            &self.epoch,
        )
    }

    pub(crate) fn resume(
        &mut self,
        project_id: &str,
        expected_version: u64,
        environment: ExecutionEnvironmentLease,
    ) -> Result<()> {
        self.attach_environment(project_id, environment)?;
        self.store
            .resume_project_with_epoch(project_id, expected_version, &self.epoch)
    }

    pub(crate) fn run_until_blocked(&mut self, project_id: &str, max_steps: usize) -> Result<()> {
        for _ in 0..max_steps {
            match self.step(project_id)? {
                SchedulerStep::Progress => {}
                SchedulerStep::Idle | SchedulerStep::Terminal | SchedulerStep::InjectedStop => {
                    return Ok(());
                }
            }
        }
        bail!("durable scheduler exceeded its deterministic step bound")
    }

    pub(crate) fn pause(
        &mut self,
        project_id: &str,
        expected_version: u64,
        reason: &str,
    ) -> Result<()> {
        self.store
            .pause_project(project_id, expected_version, reason)
    }

    pub(crate) fn cancel(
        &mut self,
        project_id: &str,
        expected_version: u64,
        reason: &str,
    ) -> Result<()> {
        self.store
            .cancel_project(project_id, expected_version, reason)
    }

    fn step(&mut self, project_id: &str) -> Result<SchedulerStep> {
        if let Some(ready) = self.store.next_result_ready_turn(project_id)? {
            if let Err(error) = self.verify_persisted_manifest(&ready) {
                self.store.mark_result_ready_invalid(
                    &ready,
                    "manifest_validation",
                    "persisted result_ready manifest failed validation",
                )?;
                return Err(error);
            }
            if let Err(error) = self.store.apply_result_ready(&ready) {
                self.store.mark_result_ready_invalid(
                    &ready,
                    "result_apply",
                    "persisted result_ready failed its durable apply transaction",
                )?;
                return Err(error);
            }
            return Ok(SchedulerStep::Progress);
        }
        if self.store.finalize_next_task(project_id)? {
            return Ok(SchedulerStep::Progress);
        }
        let (state, _) = self.store.project_state(project_id)?;
        match state {
            ProjectExecutionState::Completed
            | ProjectExecutionState::Failed
            | ProjectExecutionState::Canceled => return Ok(SchedulerStep::Terminal),
            ProjectExecutionState::Paused
            | ProjectExecutionState::NeedsInput
            | ProjectExecutionState::NeedsRecovery => return Ok(SchedulerStep::Idle),
            ProjectExecutionState::Approved => return Ok(SchedulerStep::Idle),
            ProjectExecutionState::Running => {}
        }
        if let Some(ready) = self.store.next_queued_turn(project_id)? {
            if !self.environments.contains_key(project_id) {
                self.store
                    .mark_missing_environment(project_id, &self.epoch)?;
                return Ok(SchedulerStep::Progress);
            }
            return self.execute_turn(ready);
        }
        if self.store.enqueue_next_ready_task(project_id)? {
            return Ok(SchedulerStep::Progress);
        }
        Ok(SchedulerStep::Idle)
    }

    fn execute_turn(&mut self, ready: ReadyTurn) -> Result<SchedulerStep> {
        let environment = self
            .environments
            .get(&ready.project_id)
            .ok_or_else(|| anyhow!("worker environment lease is unavailable"))?;
        let environment_hash = environment.descriptor().environment_hash.clone();
        let claim = self
            .store
            .claim_turn(&ready.turn_id, &self.epoch, TURN_LEASE_DURATION)?;
        let adapter_result = self.adapters.resolve(&claim.ready.profile.adapter);
        let adapter = pre_spawn_result(
            &mut self.store,
            &claim,
            adapter_result,
            "adapter_unavailable",
            "pinned worker adapter is unavailable before spawn",
        )?;
        let scope = artifact_scope(&claim);
        let prompt_result = build_turn_prompt(&claim);
        let prompt = pre_spawn_result(
            &mut self.store,
            &claim,
            prompt_result,
            "prompt_validation",
            "durable worker prompt could not be built before spawn",
        )?;
        let control = TurnControl {
            project_id: claim.ready.project_id.clone(),
            task_id: claim.ready.task_id.clone(),
            role: claim.ready.role,
            logical_session_id: claim.ready.session_id.clone(),
            native_session_id: claim.ready.native_session_id.clone(),
            turn_sequence: claim.ready.sequence,
            attempt: claim.attempt,
            task_version: claim.ready.task_version,
            review_round: claim.ready.review_round,
            base_revision: claim.ready.base_revision.clone(),
            head_revision: claim.ready.head_revision.clone(),
            artifact_dir: scope.relative_path(),
        };
        let prepared = (|| -> Result<_> {
            Ok(match claim.ready.kind {
                TurnKind::Create => prepare_create_turn(
                    adapter.as_ref(),
                    &claim.ready.profile,
                    &control,
                    prompt.clone(),
                )?,
                TurnKind::Resume => {
                    let native_session_id = claim
                        .ready
                        .native_session_id
                        .as_deref()
                        .ok_or_else(|| anyhow!("resume turn lost its exact native session"))?;
                    prepare_resume_turn(
                        adapter.as_ref(),
                        &claim.ready.profile,
                        &control,
                        native_session_id,
                        prompt.clone(),
                    )?
                }
            })
        })();
        let prepared = pre_spawn_result(
            &mut self.store,
            &claim,
            prepared,
            "profile_validation",
            "worker profile or command validation failed before spawn",
        )?;
        let command = prepared.command();
        let argv = command.materialized_control_argv();
        let developer_worktree_result = self.store.project_worktree_root(&claim.ready.project_id);
        let developer_worktree = pre_spawn_result(
            &mut self.store,
            &claim,
            developer_worktree_result,
            "workspace_validation",
            "developer workspace identity is unavailable before spawn",
        )?;
        let workspace_before = if claim.ready.role == WorkerRole::Reviewer {
            let digest_result = snapshot_digest(&command.workspace_cwd);
            Some(pre_spawn_result(
                &mut self.store,
                &claim,
                digest_result,
                "review_snapshot_validation",
                "review snapshot failed validation before spawn",
            )?)
        } else {
            None
        };
        self.spawn_audit.push(SpawnAudit {
            task_id: claim.ready.task_id.clone(),
            role: claim.ready.role,
            turn_sequence: claim.ready.sequence,
            workspace_cwd: command.workspace_cwd.clone(),
            argv_hash: sha256_hex(argv.join("\0").as_bytes()),
            prompt_hash: sha256_hex(&prompt),
            prompt_in_argv: argv.iter().any(|argument| {
                argument
                    .as_bytes()
                    .windows(prompt.len())
                    .any(|w| w == prompt)
            }),
            developer_path_exposed: claim.ready.role == WorkerRole::Reviewer
                && (argv
                    .iter()
                    .any(|argument| argument.contains(&developer_worktree))
                    || String::from_utf8_lossy(&prompt).contains(&developer_worktree)),
        });
        let attempt_result =
            crate::artifact::ArtifactAttempt::create(&self.artifacts, scope, environment, &prompt);
        let attempt = pre_spawn_result(
            &mut self.store,
            &claim,
            attempt_result,
            "artifact_prepare",
            "artifact attempt could not be prepared before spawn",
        )?;
        let materialized_result = environment.materialize(
            &self.epoch,
            &WorkerEnvironmentIdentity {
                role: claim.ready.role,
                project_id: claim.ready.project_id.clone(),
                task_id: claim.ready.task_id.clone(),
            },
        );
        let materialized = pre_spawn_result(
            &mut self.store,
            &claim,
            materialized_result,
            "environment_validation",
            "worker environment lease failed validation before spawn",
        )?;
        let worker = match self
            .runner
            .spawn(claim.ready.role, prepared, &materialized, attempt)
        {
            Ok(worker) => worker,
            Err(error) => {
                self.store.mark_spawn_failed(
                    &claim,
                    "spawn_failed",
                    "worker process did not start",
                )?;
                return Err(error);
            }
        };
        let identity = worker.identity().clone();
        if let Err(error) =
            self.store
                .bind_spawned_turn(&claim, identity.pid, &identity.process_birth)
        {
            drop(worker);
            self.store.mark_turn_indeterminate(
                &claim,
                "spawn_bind",
                "worker spawned but its durable process identity could not be bound",
            )?;
            return Err(error);
        }
        self.start_metric(claim.ready.role);
        let completion = worker.wait(|live| {
            self.metrics.heartbeats = self.metrics.heartbeats.saturating_add(1);
            if self.cancel_on_next_heartbeat {
                let (_, version) = self.store.project_state(&claim.ready.project_id)?;
                self.store.cancel_project(
                    &claim.ready.project_id,
                    version,
                    "test_controlled_cancel",
                )?;
                self.cancel_on_next_heartbeat = false;
            }
            if self.pause_on_next_heartbeat {
                let (_, version) = self.store.project_state(&claim.ready.project_id)?;
                self.store.pause_project(
                    &claim.ready.project_id,
                    version,
                    "test_controlled_pause",
                )?;
                self.pause_on_next_heartbeat = false;
            }
            Ok(
                if self.store.heartbeat_turn(
                    &claim,
                    live.pid,
                    &live.process_birth,
                    TURN_LEASE_DURATION,
                )? {
                    HeartbeatControl::Continue
                } else {
                    HeartbeatControl::Cancel
                },
            )
        });
        self.finish_metric(claim.ready.role);
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                self.store.mark_turn_indeterminate(
                    &claim,
                    "process_transport",
                    "worker transport outcome is indeterminate",
                )?;
                return Err(error);
            }
        };
        let project_state = self.store.project_state(&claim.ready.project_id)?.0;
        if project_state == ProjectExecutionState::Canceled {
            self.store.mark_canceled_after_signal(&claim)?;
            self.metrics.controlled_cancellations =
                self.metrics.controlled_cancellations.saturating_add(1);
            return Ok(SchedulerStep::Progress);
        }
        if project_state == ProjectExecutionState::Paused {
            self.store.mark_paused_attempt_indeterminate(&claim)?;
            self.metrics.controlled_pauses = self.metrics.controlled_pauses.saturating_add(1);
            return Ok(SchedulerStep::Progress);
        }
        if completion.exit.termination == WorkerTermination::Canceled {
            self.store.mark_turn_indeterminate(
                &claim,
                "lease_revoked",
                "worker lease ended without a supported durable control state",
            )?;
            return Ok(SchedulerStep::Progress);
        }
        if project_state != ProjectExecutionState::Running {
            self.store.mark_turn_indeterminate(
                &claim,
                "project_state_changed",
                "worker completed after its durable project left running state",
            )?;
            return Ok(SchedulerStep::Progress);
        }
        if completion.exit.termination != WorkerTermination::Exited
            || completion.exit.code != Some(0)
            || completion.exit.signal.is_some()
        {
            self.store.mark_turn_indeterminate(
                &claim,
                "worker_exit",
                "worker turn may have executed but did not produce an admissible exit",
            )?;
            return Ok(SchedulerStep::Progress);
        }
        if let Some(before) = workspace_before {
            let after_result = snapshot_digest(
                &self
                    .spawn_audit
                    .last()
                    .expect("spawn audit was recorded")
                    .workspace_cwd,
            );
            let after = post_turn_result(
                &mut self.store,
                &claim,
                after_result,
                "review_snapshot_validation",
                "review snapshot could not be revalidated after the worker turn",
            )?;
            if before != after {
                self.store.mark_turn_indeterminate(
                    &claim,
                    "review_snapshot_changed",
                    "reviewer snapshot changed during its read-only turn",
                )?;
                return Ok(SchedulerStep::Progress);
            }
        }
        let native_result = adapter
            .extract_result(&control, &completion.artifacts)
            .context("worker result does not satisfy its adapter contract");
        let native = post_turn_result(
            &mut self.store,
            &claim,
            native_result,
            "result_validation",
            "worker result failed its adapter contract",
        )?;
        if native.role() != claim.ready.role {
            return post_turn_result(
                &mut self.store,
                &claim,
                Err(anyhow!("worker native result role mismatch")),
                "result_validation",
                "worker result role did not match the durable turn",
            );
        }
        let result_json_result = match &native {
            NativeResult::Developer { result, .. } => result.canonical_json(),
            NativeResult::Reviewer { result, .. } => result.canonical_json(),
        };
        let result_json = post_turn_result(
            &mut self.store,
            &claim,
            result_json_result,
            "result_validation",
            "worker result could not be canonicalized",
        )?;
        let result_hash = sha256_hex(&result_json);
        let result_receipt_result = completion.artifact_attempt.write_result_json(&result_json);
        let result_receipt = post_turn_result(
            &mut self.store,
            &claim,
            result_receipt_result,
            "result_artifact",
            "validated worker result could not be persisted",
        )?;
        if result_receipt.sha256 != result_hash {
            return post_turn_result(
                &mut self.store,
                &claim,
                Err(anyhow!(
                    "artifact result hash differs from the durable result"
                )),
                "result_artifact",
                "validated result artifact hash mismatch",
            );
        }
        let completed_at_result = now_epoch_seconds();
        let completed_at = post_turn_result(
            &mut self.store,
            &claim,
            completed_at_result,
            "manifest_finalize",
            "worker completion timestamp could not be recorded",
        )?;
        let manifest_result = completion
            .artifact_attempt
            .finalize_manifest(ManifestMetadata {
                native_session_id: native.native_session_id().to_owned(),
                daemon_epoch: self.epoch.clone(),
                environment_hash,
                adapter_contract_hash: claim.ready.profile.capability.contract_hash.clone(),
                result_hash: result_hash.clone(),
                created_at: completed_at,
                completed_at,
            });
        let manifest = post_turn_result(
            &mut self.store,
            &claim,
            manifest_result,
            "manifest_finalize",
            "worker artifact manifest could not be finalized",
        )?;
        let ready_result = self.store.record_result_ready(
            &claim,
            identity.pid,
            &identity.process_birth,
            native.native_session_id(),
            std::str::from_utf8(&result_json).expect("canonical JSON is UTF-8"),
            &result_hash,
            manifest.activity_truncated,
        );
        post_turn_result(
            &mut self.store,
            &claim,
            ready_result,
            "result_ready",
            "worker result could not enter durable result_ready state",
        )?;
        if self.fail_after_result_ready_once {
            self.fail_after_result_ready_once = false;
            return Ok(SchedulerStep::InjectedStop);
        }
        Ok(SchedulerStep::Progress)
    }

    fn verify_persisted_manifest(&self, ready: &ReadyTurn) -> Result<()> {
        let scope = ArtifactScope {
            project_id: ready.project_id.clone(),
            task_id: ready.task_id.clone(),
            role: ready.role,
            logical_session_id: ready.session_id.clone(),
            turn_sequence: ready.sequence,
            attempt: ready.attempt,
        };
        let expected_prefix = format!(
            "{}/{}/{}/{}/turn-{}",
            ready.project_id,
            ready.task_id,
            role_name(ready.role),
            ready.session_id,
            ready.sequence
        );
        if ready.artifact_dir != expected_prefix {
            bail!("durable artifact prefix does not match the turn scope");
        }
        let manifest = self.artifacts.load_turn_manifest(&scope)?;
        if ready.result_hash.as_deref() != Some(&manifest.result_hash) {
            bail!("durable result hash does not match the persisted manifest");
        }
        if ready.native_session_id.as_deref() != Some(&manifest.native_session_id) {
            bail!("durable native session does not match the persisted manifest");
        }
        if ready.profile.capability.contract_hash != manifest.adapter_contract_hash {
            bail!("durable adapter contract does not match the persisted manifest");
        }
        let lease_owner = ready
            .lease_owner
            .as_deref()
            .ok_or_else(|| anyhow!("result_ready turn lost its lease owner"))?;
        if !lease_owner.starts_with(&format!("{}/", manifest.daemon_epoch)) {
            bail!("durable turn lease does not match the persisted daemon epoch");
        }
        if !self.store.manifest_environment_matches(
            &ready.project_id,
            &manifest.daemon_epoch,
            &manifest.environment_hash,
        )? {
            bail!("persisted manifest environment is not durably bound to the project");
        }
        Ok(())
    }

    fn start_metric(&mut self, role: WorkerRole) {
        match role {
            WorkerRole::Developer => {
                self.metrics.current_developers += 1;
                self.metrics.developer_spawns += 1;
                self.metrics.max_live_developers = self
                    .metrics
                    .max_live_developers
                    .max(self.metrics.current_developers);
            }
            WorkerRole::Reviewer => {
                self.metrics.current_reviewers += 1;
                self.metrics.reviewer_spawns += 1;
                self.metrics.max_live_reviewers = self
                    .metrics
                    .max_live_reviewers
                    .max(self.metrics.current_reviewers);
            }
        }
    }

    fn finish_metric(&mut self, role: WorkerRole) {
        match role {
            WorkerRole::Developer => self.metrics.current_developers -= 1,
            WorkerRole::Reviewer => self.metrics.current_reviewers -= 1,
        }
    }

    pub(crate) fn metrics(&self) -> &SchedulerMetrics {
        &self.metrics
    }

    pub(crate) fn spawn_audit(&self) -> &[SpawnAudit] {
        &self.spawn_audit
    }

    pub(crate) fn snapshot(&self, project_id: &str) -> Result<SchedulerSnapshot> {
        self.store.project_snapshot(project_id)
    }

    pub(crate) fn set_fail_after_result_ready_once(&mut self) {
        self.fail_after_result_ready_once = true;
    }

    #[cfg(test)]
    fn cancel_worker_on_next_heartbeat(&mut self) {
        self.cancel_on_next_heartbeat = true;
    }

    #[cfg(test)]
    fn pause_worker_on_next_heartbeat(&mut self) {
        self.pause_on_next_heartbeat = true;
    }

    pub(crate) fn simulate_crash_on_drop(&mut self) {
        self.retire_epoch_on_drop = false;
    }

    #[cfg(test)]
    fn seed_fake_project(&mut self, seed: &crate::project_store::FakeProjectSeed) -> Result<()> {
        self.store.seed_fake_project(seed)
    }
}

impl Drop for DurableScheduler {
    fn drop(&mut self) {
        if self.retire_epoch_on_drop {
            let _ = self.store.retire_daemon_epoch(&self.epoch);
        }
    }
}

fn pre_spawn_result<T>(
    store: &mut DaemonStore,
    claim: &ClaimedTurn,
    result: Result<T>,
    error_kind: &str,
    durable_message: &str,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            store
                .mark_spawn_failed(claim, error_kind, durable_message)
                .context("failed to commit pre-spawn failure state")?;
            Err(error)
        }
    }
}

fn post_turn_result<T>(
    store: &mut DaemonStore,
    claim: &ClaimedTurn,
    result: Result<T>,
    error_kind: &str,
    durable_message: &str,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            store
                .mark_turn_indeterminate(claim, error_kind, durable_message)
                .context("failed to commit indeterminate worker state")?;
            Err(error)
        }
    }
}

fn artifact_scope(claim: &ClaimedTurn) -> ArtifactScope {
    ArtifactScope {
        project_id: claim.ready.project_id.clone(),
        task_id: claim.ready.task_id.clone(),
        role: claim.ready.role,
        logical_session_id: claim.ready.session_id.clone(),
        turn_sequence: claim.ready.sequence,
        attempt: claim.attempt,
    }
}

fn build_turn_prompt(claim: &ClaimedTurn) -> Result<Vec<u8>> {
    let task_spec: serde_json::Value =
        serde_json::from_str(&claim.ready.spec_json).context("stored TaskSpec is malformed")?;
    let previous_result: Option<serde_json::Value> = claim
        .ready
        .previous_result_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .context("stored previous result is malformed")?;
    let value = serde_json::json!({
        "contract": "hcom-durable-worker-turn-v1",
        "project_id": claim.ready.project_id,
        "task_id": claim.ready.task_id,
        "role": role_name(claim.ready.role),
        "turn": match claim.ready.kind {
            TurnKind::Create => "create",
            TurnKind::Resume => "resume",
        },
        "turn_sequence": claim.ready.sequence,
        "review_round": claim.ready.review_round,
        "base_revision": claim.ready.base_revision,
        "head_revision": claim.ready.head_revision,
        "task_spec": task_spec,
        "previous_result": previous_result,
    });
    serde_json::to_vec(&value).context("failed to encode bounded worker turn prompt")
}

fn role_name(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => "developer",
        WorkerRole::Reviewer => "reviewer",
    }
}

fn snapshot_digest(root: &Path) -> Result<String> {
    let canonical = fs::canonicalize(root)?;
    if canonical != root || !canonical.is_dir() {
        bail!("review snapshot root is not canonical");
    }
    let mut entries = Vec::new();
    collect_snapshot_entries(&canonical, &canonical, &mut entries, 0)?;
    entries.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"hcom-review-snapshot-v1\0");
    for entry in entries {
        hasher.update((entry.len() as u64).to_be_bytes());
        hasher.update(entry);
    }
    Ok(hex_bytes(&hasher.finalize()))
}

fn collect_snapshot_entries(
    root: &Path,
    directory: &Path,
    entries: &mut Vec<Vec<u8>>,
    depth: usize,
) -> Result<()> {
    if depth > 32 || entries.len() > 4096 {
        bail!("review snapshot exceeds its bounded manifest shape");
    }
    let mut children = fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bail!("review snapshot contains a symlink");
        }
        let relative = path.strip_prefix(root)?;
        let relative = relative
            .to_str()
            .ok_or_else(|| anyhow!("review snapshot path is not UTF-8"))?;
        if relative.len() > 4096 {
            bail!("review snapshot path exceeds its bound");
        }
        if metadata.is_dir() {
            entries
                .push(format!("d\0{relative}\0{:o}", metadata.permissions().mode()).into_bytes());
            collect_snapshot_entries(root, &path, entries, depth + 1)?;
        } else if metadata.is_file() {
            if metadata.len() > 16 * 1024 * 1024 {
                bail!("review snapshot file exceeds its bound");
            }
            let bytes = fs::read(&path)?;
            entries.push(
                format!(
                    "f\0{relative}\0{}\0{:o}\0{}",
                    metadata.len(),
                    metadata.permissions().mode(),
                    sha256_hex(&bytes)
                )
                .into_bytes(),
            );
        } else {
            bail!("review snapshot contains an unsupported file type");
        }
    }
    Ok(())
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_store::{FakeProjectSeed, FakeTaskSeed};
    use crate::worker::ExecutableIdentity;
    use crate::worker::environment::{EnvironmentPolicy, ExecutionEnvironmentLease};
    use crate::worker::fake::FakeWorkerAdapter;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        _temp: tempfile::TempDir,
        layout: ProjectControlLayout,
        artifact_root: PathBuf,
        developer: PathBuf,
        reviewer: PathBuf,
        executable: ExecutableIdentity,
    }

    impl Fixture {
        fn new() -> Self {
            Self::new_with_script(fake_worker_script())
        }

        fn new_with_script(script_contents: Vec<u8>) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let state = temp.path().join("state/hcom-project-control");
            let runtime = temp.path().join("run/hcom-project-control");
            let config = temp.path().join("config/hcom-project-control/config.toml");
            let artifact_root = state.join("control-v1/artifacts");
            fs::create_dir_all(&artifact_root).unwrap();
            fs::set_permissions(&artifact_root, fs::Permissions::from_mode(0o700)).unwrap();
            let developer = temp.path().join("developer");
            let reviewer = temp.path().join("reviewer");
            fs::create_dir(&developer).unwrap();
            fs::create_dir(&reviewer).unwrap();
            fs::write(developer.join("tracked.txt"), b"developer worktree").unwrap();
            fs::write(reviewer.join("tracked.txt"), b"read-only snapshot").unwrap();
            fs::set_permissions(&reviewer, fs::Permissions::from_mode(0o500)).unwrap();
            fs::set_permissions(
                reviewer.join("tracked.txt"),
                fs::Permissions::from_mode(0o400),
            )
            .unwrap();
            let script = temp.path().join("fake-worker");
            fs::write(&script, script_contents).unwrap();
            fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
            let executable =
                ExecutableIdentity::capture(fs::canonicalize(&script).unwrap()).unwrap();
            Self {
                _temp: temp,
                layout: ProjectControlLayout::from_app_roots(state, runtime, config),
                artifact_root: fs::canonicalize(artifact_root).unwrap(),
                developer: fs::canonicalize(developer).unwrap(),
                reviewer: fs::canonicalize(reviewer).unwrap(),
                executable,
            }
        }

        fn scheduler(&self) -> DurableScheduler {
            let adapter = Arc::new(
                FakeWorkerAdapter::isolated_preassigned(
                    self.executable.clone(),
                    self.developer.clone(),
                    self.reviewer.clone(),
                )
                .unwrap(),
            );
            let mut registry = WorkerAdapterRegistry::default();
            registry.register(adapter).unwrap();
            DurableScheduler::open(
                &self.layout,
                &self.artifact_root,
                registry,
                ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(50)).unwrap(),
            )
            .unwrap()
        }

        fn seed(&self, scheduler: &mut DurableScheduler, project_id: &str) {
            let adapter = FakeWorkerAdapter::isolated_preassigned(
                self.executable.clone(),
                self.developer.clone(),
                self.reviewer.clone(),
            )
            .unwrap();
            scheduler
                .seed_fake_project(&FakeProjectSeed {
                    project_id: project_id.into(),
                    source_repo_root: self.developer.to_string_lossy().into_owned(),
                    developer_worktree: self.developer.to_string_lossy().into_owned(),
                    base_revision: "0".repeat(40),
                    developer_profile: adapter.profile(WorkerRole::Developer),
                    reviewer_profile: adapter.profile(WorkerRole::Reviewer),
                    approved_tasks: vec![
                        task("task-1", "approved-one", 0, vec![]),
                        task("task-2", "approved-two", 1, vec!["task-1".into()]),
                    ],
                    unapproved_task: Some(task("task-3", "unapproved-three", 0, vec![])),
                })
                .unwrap();
        }
    }

    #[test]
    fn fake_two_task_run_is_fresh_serial_snapshot_isolated_and_approval_bound() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-e2e");
        let environment = environment("lease-e2e", scheduler.daemon_epoch());
        scheduler
            .request_run(
                "project-e2e",
                0,
                1,
                &sha256_hex(b"project-e2e:approved-plan"),
                environment,
            )
            .unwrap();
        scheduler.run_until_blocked("project-e2e", 64).unwrap();
        let snapshot = scheduler.snapshot("project-e2e").unwrap();
        assert_eq!(snapshot.project_state, "completed");
        assert_eq!(snapshot.checkpoint_sha, "c".repeat(40));
        assert_eq!(snapshot.developer_native_sessions.len(), 2);
        assert_eq!(snapshot.reviewer_native_sessions.len(), 2);
        assert_ne!(
            snapshot.developer_native_sessions[0],
            snapshot.developer_native_sessions[1]
        );
        assert_ne!(
            snapshot.reviewer_native_sessions[0],
            snapshot.reviewer_native_sessions[1]
        );
        assert_eq!(snapshot.turn_count, 6);
        assert_eq!(snapshot.applied_turn_count, 6);
        assert_eq!(snapshot.result_ready_turn_count, 0);
        assert!(snapshot.transition_count >= 16);
        assert!(
            snapshot
                .task_states
                .contains(&("unapproved-three".into(), "draft".into()))
        );
        let metrics = scheduler.metrics();
        assert_eq!(metrics.max_live_developers, 1);
        assert_eq!(metrics.max_live_reviewers, 1);
        assert_eq!(metrics.developer_spawns, 3);
        assert_eq!(metrics.reviewer_spawns, 3);
        assert!(metrics.heartbeats >= 6);
        assert_eq!(metrics.current_developers, 0);
        assert_eq!(metrics.current_reviewers, 0);
        assert!(scheduler.spawn_audit().iter().all(|audit| {
            !audit.prompt_in_argv
                && !audit.developer_path_exposed
                && match audit.role {
                    WorkerRole::Developer => audit.workspace_cwd == fixture.developer,
                    WorkerRole::Reviewer => {
                        audit.workspace_cwd == fixture.reviewer
                            && audit.workspace_cwd != fixture.developer
                    }
                }
        }));
        assert_eq!(
            scheduler
                .spawn_audit()
                .iter()
                .filter(|audit| audit.task_id == "task-3")
                .count(),
            0
        );
    }

    #[test]
    fn daemon_restart_applies_result_ready_without_repeating_worker_turn() {
        let fixture = Fixture::new();
        let mut first = fixture.scheduler();
        fixture.seed(&mut first, "project-recovery");
        first
            .request_run(
                "project-recovery",
                0,
                1,
                &sha256_hex(b"project-recovery:approved-plan"),
                environment("lease-before", first.daemon_epoch()),
            )
            .unwrap();
        first.set_fail_after_result_ready_once();
        first.run_until_blocked("project-recovery", 8).unwrap();
        assert_eq!(first.metrics().developer_spawns, 1);
        let before = first.snapshot("project-recovery").unwrap();
        assert_eq!(before.result_ready_turn_count, 1);
        first.simulate_crash_on_drop();
        drop(first);

        let mut recovered = fixture.scheduler();
        let recovered_state = recovered.snapshot("project-recovery").unwrap();
        assert_eq!(recovered_state.project_state, "needs_recovery");
        assert_eq!(recovered_state.result_ready_turn_count, 1);
        let lease_states: (i64, i64) = recovered
            .store
            .connection()
            .query_row(
                "SELECT
                     sum(CASE WHEN state = 'lost' THEN 1 ELSE 0 END),
                     sum(CASE WHEN state = 'active' THEN 1 ELSE 0 END)
                 FROM execution_environment_leases
                 WHERE project_id = 'project-recovery'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(lease_states, (1, 0));
        recovered.run_until_blocked("project-recovery", 8).unwrap();
        let applied = recovered.snapshot("project-recovery").unwrap();
        assert_eq!(applied.result_ready_turn_count, 0);
        assert_eq!(recovered.metrics().developer_spawns, 0);
        recovered
            .resume(
                "project-recovery",
                applied.project_version,
                environment("lease-after", recovered.daemon_epoch()),
            )
            .unwrap();
        recovered.run_until_blocked("project-recovery", 64).unwrap();
        let final_state = recovered.snapshot("project-recovery").unwrap();
        assert_eq!(final_state.project_state, "completed");
        assert_eq!(final_state.turn_count, 6);
        assert_eq!(recovered.metrics().developer_spawns, 2);
        assert_eq!(recovered.metrics().reviewer_spawns, 3);
    }

    #[test]
    fn canceling_result_ready_prevents_late_apply_or_worker_restart() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-result-cancel");
        scheduler
            .request_run(
                "project-result-cancel",
                0,
                1,
                &sha256_hex(b"project-result-cancel:approved-plan"),
                environment("lease-result-cancel", scheduler.daemon_epoch()),
            )
            .unwrap();
        scheduler.set_fail_after_result_ready_once();
        scheduler
            .run_until_blocked("project-result-cancel", 8)
            .unwrap();
        let ready = scheduler.snapshot("project-result-cancel").unwrap();
        assert_eq!(ready.result_ready_turn_count, 1);
        scheduler
            .cancel(
                "project-result-cancel",
                ready.project_version,
                "cancel_after_result",
            )
            .unwrap();
        scheduler
            .run_until_blocked("project-result-cancel", 8)
            .unwrap();
        let canceled = scheduler.snapshot("project-result-cancel").unwrap();
        assert_eq!(canceled.project_state, "canceled");
        assert_eq!(canceled.result_ready_turn_count, 0);
        assert_eq!(canceled.applied_turn_count, 0);
        assert_eq!(scheduler.metrics().developer_spawns, 1);
        assert_eq!(scheduler.metrics().reviewer_spawns, 0);
    }

    #[test]
    fn tampered_result_ready_manifest_is_staled_and_recovery_gated() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-manifest-tamper");
        scheduler
            .request_run(
                "project-manifest-tamper",
                0,
                1,
                &sha256_hex(b"project-manifest-tamper:approved-plan"),
                environment("lease-manifest-tamper", scheduler.daemon_epoch()),
            )
            .unwrap();
        scheduler.set_fail_after_result_ready_once();
        scheduler
            .run_until_blocked("project-manifest-tamper", 8)
            .unwrap();
        let artifact_dir: String = scheduler
            .store
            .connection()
            .query_row(
                "SELECT artifact_dir FROM worker_turns WHERE status = 'result_ready'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let manifest_path = fixture
            .artifact_root
            .join(artifact_dir)
            .join("attempt-1/manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["adapter_contract_hash"] = serde_json::Value::String("f".repeat(64));
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        assert!(
            scheduler
                .run_until_blocked("project-manifest-tamper", 8)
                .is_err()
        );
        let snapshot = scheduler.snapshot("project-manifest-tamper").unwrap();
        assert_eq!(snapshot.project_state, "needs_recovery");
        assert_eq!(snapshot.result_ready_turn_count, 0);
        assert_eq!(snapshot.applied_turn_count, 0);
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "indeterminate".into()))
        );
        assert_eq!(scheduler.metrics().developer_spawns, 1);
    }

    #[test]
    fn duplicate_result_ready_apply_cannot_advance_twice() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-duplicate-apply");
        scheduler
            .request_run(
                "project-duplicate-apply",
                0,
                1,
                &sha256_hex(b"project-duplicate-apply:approved-plan"),
                environment("lease-duplicate-apply", scheduler.daemon_epoch()),
            )
            .unwrap();
        scheduler.set_fail_after_result_ready_once();
        scheduler
            .run_until_blocked("project-duplicate-apply", 8)
            .unwrap();
        let ready = scheduler
            .store
            .next_result_ready_turn("project-duplicate-apply")
            .unwrap()
            .unwrap();
        scheduler.verify_persisted_manifest(&ready).unwrap();
        scheduler.store.apply_result_ready(&ready).unwrap();
        assert!(scheduler.store.apply_result_ready(&ready).is_err());
        let snapshot = scheduler.snapshot("project-duplicate-apply").unwrap();
        assert_eq!(snapshot.applied_turn_count, 1);
        assert_eq!(snapshot.result_ready_turn_count, 0);
        assert_eq!(snapshot.turn_count, 2);
    }

    #[test]
    fn pause_and_cancel_commit_durable_state_before_any_worker_exists() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-control");
        scheduler
            .request_run(
                "project-control",
                0,
                1,
                &sha256_hex(b"project-control:approved-plan"),
                environment("lease-control-1", scheduler.daemon_epoch()),
            )
            .unwrap();
        let running = scheduler.snapshot("project-control").unwrap();
        assert_eq!(running.project_state, "running");
        scheduler
            .pause("project-control", running.project_version, "human_pause")
            .unwrap();
        let paused = scheduler.snapshot("project-control").unwrap();
        assert_eq!(paused.project_state, "paused");
        assert_eq!(scheduler.metrics().developer_spawns, 0);
        scheduler
            .resume(
                "project-control",
                paused.project_version,
                environment("lease-control-2", scheduler.daemon_epoch()),
            )
            .unwrap();
        let resumed = scheduler.snapshot("project-control").unwrap();
        scheduler
            .cancel("project-control", resumed.project_version, "human_cancel")
            .unwrap();
        let canceled = scheduler.snapshot("project-control").unwrap();
        assert_eq!(canceled.project_state, "canceled");
        scheduler.run_until_blocked("project-control", 4).unwrap();
        assert_eq!(scheduler.metrics().developer_spawns, 0);
    }

    #[test]
    fn claimed_attempt_crossing_daemon_epoch_is_indeterminate_not_respawned() {
        let fixture = Fixture::new();
        let mut first = fixture.scheduler();
        fixture.seed(&mut first, "project-claimed");
        first
            .request_run(
                "project-claimed",
                0,
                1,
                &sha256_hex(b"project-claimed:approved-plan"),
                environment("lease-claimed", first.daemon_epoch()),
            )
            .unwrap();
        assert!(
            first
                .store
                .enqueue_next_ready_task("project-claimed")
                .unwrap()
        );
        let ready = first
            .store
            .next_queued_turn("project-claimed")
            .unwrap()
            .unwrap();
        let epoch = first.daemon_epoch().to_owned();
        first
            .store
            .claim_turn(&ready.turn_id, &epoch, TURN_LEASE_DURATION)
            .unwrap();
        assert_eq!(first.metrics().developer_spawns, 0);
        first.simulate_crash_on_drop();
        drop(first);

        let recovered = fixture.scheduler();
        let snapshot = recovered.snapshot("project-claimed").unwrap();
        assert_eq!(snapshot.project_state, "needs_recovery");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "indeterminate".into()))
        );
        assert_eq!(recovered.metrics().developer_spawns, 0);
        assert_eq!(snapshot.turn_count, 1);
    }

    #[test]
    fn paused_claim_crossing_daemon_epoch_is_recovery_gated() {
        let fixture = Fixture::new();
        let mut first = fixture.scheduler();
        fixture.seed(&mut first, "project-paused-claim");
        first
            .request_run(
                "project-paused-claim",
                0,
                1,
                &sha256_hex(b"project-paused-claim:approved-plan"),
                environment("lease-paused-claim", first.daemon_epoch()),
            )
            .unwrap();
        assert!(
            first
                .store
                .enqueue_next_ready_task("project-paused-claim")
                .unwrap()
        );
        let ready = first
            .store
            .next_queued_turn("project-paused-claim")
            .unwrap()
            .unwrap();
        let epoch = first.daemon_epoch().to_owned();
        first
            .store
            .claim_turn(&ready.turn_id, &epoch, TURN_LEASE_DURATION)
            .unwrap();
        let snapshot = first.snapshot("project-paused-claim").unwrap();
        first
            .pause(
                "project-paused-claim",
                snapshot.project_version,
                "pause_before_spawn",
            )
            .unwrap();
        first.simulate_crash_on_drop();
        drop(first);

        let recovered = fixture.scheduler();
        let snapshot = recovered.snapshot("project-paused-claim").unwrap();
        assert_eq!(snapshot.project_state, "needs_recovery");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "indeterminate".into()))
        );
        assert_eq!(recovered.metrics().developer_spawns, 0);
    }

    #[test]
    fn systemd_unit_is_no_tty_restartable_and_cgroup_scoped() {
        let unit = include_str!("../../contrib/systemd/hcomd.service");
        for required in [
            "ExecStart=%h/.local/libexec/hcomd",
            "Restart=on-failure",
            "KillMode=control-group",
            "StandardInput=null",
            "StandardOutput=journal",
            "StandardError=journal",
            "UMask=0077",
            "RuntimeDirectoryMode=0700",
        ] {
            assert!(
                unit.contains(required),
                "missing systemd contract: {required}"
            );
        }
        assert!(!unit.contains("chain"));
        assert!(!unit.contains("handoff"));
        assert!(!unit.contains("TTYPath"));
    }

    #[test]
    fn running_cancel_commits_store_state_before_validated_group_signal() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-running-cancel");
        scheduler
            .request_run(
                "project-running-cancel",
                0,
                1,
                &sha256_hex(b"project-running-cancel:approved-plan"),
                environment("lease-running-cancel", scheduler.daemon_epoch()),
            )
            .unwrap();
        scheduler.cancel_worker_on_next_heartbeat();
        scheduler
            .run_until_blocked("project-running-cancel", 8)
            .unwrap();
        let snapshot = scheduler.snapshot("project-running-cancel").unwrap();
        assert_eq!(snapshot.project_state, "canceled");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "canceled".into()))
        );
        assert_eq!(scheduler.metrics().developer_spawns, 1);
        assert_eq!(scheduler.metrics().controlled_cancellations, 1);
        assert_eq!(scheduler.metrics().current_developers, 0);
    }

    #[test]
    fn running_pause_commits_state_then_stops_as_indeterminate() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-running-pause");
        scheduler
            .request_run(
                "project-running-pause",
                0,
                1,
                &sha256_hex(b"project-running-pause:approved-plan"),
                environment("lease-running-pause", scheduler.daemon_epoch()),
            )
            .unwrap();
        scheduler.pause_worker_on_next_heartbeat();
        scheduler
            .run_until_blocked("project-running-pause", 8)
            .unwrap();
        let snapshot = scheduler.snapshot("project-running-pause").unwrap();
        assert_eq!(snapshot.project_state, "needs_recovery");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "indeterminate".into()))
        );
        assert_eq!(scheduler.metrics().developer_spawns, 1);
        assert_eq!(scheduler.metrics().controlled_pauses, 1);
        assert_eq!(scheduler.metrics().current_developers, 0);
    }

    #[test]
    fn pre_spawn_profile_drift_reaches_explicit_terminal_state() {
        let fixture = Fixture::new();
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-profile-drift");
        scheduler
            .request_run(
                "project-profile-drift",
                0,
                1,
                &sha256_hex(b"project-profile-drift:approved-plan"),
                environment("lease-profile-drift", scheduler.daemon_epoch()),
            )
            .unwrap();
        fs::set_permissions(
            &fixture.executable.canonical_path,
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();

        assert!(
            scheduler
                .run_until_blocked("project-profile-drift", 8)
                .is_err()
        );
        let snapshot = scheduler.snapshot("project-profile-drift").unwrap();
        assert_eq!(snapshot.project_state, "failed");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "failed".into()))
        );
        assert_eq!(scheduler.metrics().developer_spawns, 0);
    }

    #[test]
    fn invalid_post_turn_result_is_indeterminate_not_stuck_running() {
        let fixture = Fixture::new_with_script(
            br#"#!/bin/sh
set -eu
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
sed -n '1,$p' >/dev/null
printf '%s' '{"not":"a worker result"}'
"#
            .to_vec(),
        );
        let mut scheduler = fixture.scheduler();
        fixture.seed(&mut scheduler, "project-invalid-result");
        scheduler
            .request_run(
                "project-invalid-result",
                0,
                1,
                &sha256_hex(b"project-invalid-result:approved-plan"),
                environment("lease-invalid-result", scheduler.daemon_epoch()),
            )
            .unwrap();

        assert!(
            scheduler
                .run_until_blocked("project-invalid-result", 8)
                .is_err()
        );
        let snapshot = scheduler.snapshot("project-invalid-result").unwrap();
        assert_eq!(snapshot.project_state, "needs_recovery");
        assert!(
            snapshot
                .task_states
                .contains(&("approved-one".into(), "indeterminate".into()))
        );
        assert_eq!(scheduler.metrics().developer_spawns, 1);
        assert_eq!(scheduler.metrics().current_developers, 0);
    }

    fn task(id: &str, key: &str, ordinal: u32, dependencies: Vec<String>) -> FakeTaskSeed {
        FakeTaskSeed {
            id: id.into(),
            task_key: key.into(),
            ordinal,
            spec_json: serde_json::json!({
                "task_key": key,
                "objective": format!("complete {key}"),
                "allowed_paths": ["tracked.txt"]
            })
            .to_string(),
            max_review_rounds: 2,
            dependencies,
        }
    }

    fn environment(id: &str, epoch: &str) -> ExecutionEnvironmentLease {
        ExecutionEnvironmentLease::capture(
            id,
            epoch,
            &EnvironmentPolicy::baseline(),
            vec![("PATH".into(), "/usr/bin:/bin".into())],
        )
        .unwrap()
    }

    fn fake_worker_script() -> Vec<u8> {
        br#"#!/bin/sh
set -eu
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
mode="$2"
session=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --session-id|--resume)
            shift
            session="$1"
            ;;
    esac
    shift
done
[ -n "$session" ]
prompt=$(sed -n '1,$p')
[ -n "$prompt" ]
role="$HCOM_WORKER_ROLE"
task="$HCOM_TASK_ID"
if [ "$role" = "reviewer" ]; then
    if touch reviewer-write-probe 2>/dev/null; then
        exit 73
    fi
fi
if [ "$role" = "developer" ]; then
    sleep 0.04
    if [ "$task" = "task-1" ] && [ "$mode" = "create" ]; then
        head=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    elif [ "$task" = "task-1" ]; then
        head=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    else
        head=cccccccccccccccccccccccccccccccccccccccc
    fi
    printf '{"session_id":"%s","role":"developer","result":{"decision":"completed","summary":"fake completed","head_revision":"%s","commits":[{"sha":"%s","subject":"fake commit"}],"checks":[{"command":"fake-check","status":"passed","summary":"passed"}],"questions":[],"risks":[],"changed_paths":["tracked.txt"]}}' "$session" "$head" "$head"
elif [ "$task" = "task-1" ] && [ "$mode" = "create" ]; then
    printf '{"session_id":"%s","role":"reviewer","result":{"decision":"request_changes","summary":"one fake major","findings":[{"severity":"major","title":"fix required","body":"bounded fake finding","file":"tracked.txt","line":1}],"checks":[]}}' "$session"
else
    printf '{"session_id":"%s","role":"reviewer","result":{"decision":"lgtm","summary":"fake lgtm","findings":[],"checks":[]}}' "$session"
fi
"#
        .to_vec()
    }
}
