// Phase 4's durable scheduler is exercised by the fake-worker E2E. Its public
// control-plane callers are intentionally added in Phase 8.
#![cfg_attr(not(test), allow(dead_code))]

use super::{DaemonStore, now_epoch_seconds, sha256_hex};
use crate::control_api::{CapabilitySnapshot, NativeSessionMode, WorkerRole};
use crate::worker::contract::{ExecutableIdentity, WorkerProfile};
use crate::worker::environment::EnvironmentLeaseDescriptor;
use crate::worker::result::{DeveloperDecision, DeveloperResult, ReviewDecision, ReviewerResult};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectExecutionState {
    Approved,
    Running,
    Paused,
    NeedsInput,
    NeedsRecovery,
    Completed,
    Failed,
    Canceled,
}

#[derive(Clone)]
pub(crate) struct ReadyTurn {
    pub(crate) turn_id: String,
    pub(crate) session_id: String,
    pub(crate) project_id: String,
    pub(crate) task_id: String,
    pub(crate) role: WorkerRole,
    pub(crate) sequence: u32,
    pub(crate) kind: TurnKind,
    pub(crate) attempt: u32,
    pub(crate) task_version: u64,
    pub(crate) review_round: u32,
    pub(crate) base_revision: String,
    pub(crate) head_revision: Option<String>,
    pub(crate) spec_json: String,
    pub(crate) previous_result_json: Option<String>,
    pub(crate) native_session_id: Option<String>,
    pub(crate) artifact_dir: String,
    pub(crate) profile: WorkerProfile,
    pub(crate) result_hash: Option<String>,
    pub(crate) lease_owner: Option<String>,
    pub(crate) review_snapshot_digest: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnKind {
    Create,
    Resume,
}

#[derive(Clone)]
pub(crate) struct ClaimedTurn {
    pub(crate) ready: ReadyTurn,
    pub(crate) attempt: u32,
    pub(crate) lease_owner: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DaemonRecovery {
    pub(crate) interrupted_projects: u64,
    pub(crate) indeterminate_turns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchedulerSnapshot {
    pub(crate) project_state: String,
    pub(crate) project_version: u64,
    pub(crate) checkpoint_sha: String,
    pub(crate) task_states: Vec<(String, String)>,
    pub(crate) developer_native_sessions: Vec<String>,
    pub(crate) reviewer_native_sessions: Vec<String>,
    pub(crate) turn_count: u64,
    pub(crate) applied_turn_count: u64,
    pub(crate) result_ready_turn_count: u64,
    pub(crate) transition_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyOutcome {
    DeveloperCompleted,
    ReviewerChangesRequested,
    ReviewerLgtm,
    NeedsInput,
    Failed,
}

impl DaemonStore {
    pub(crate) fn recover_daemon_state(&mut self, current_epoch: &str) -> Result<DaemonRecovery> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        transaction.execute(
            "UPDATE execution_environment_leases
             SET state = 'lost', lost_at = ?1
             WHERE state = 'active' AND daemon_epoch != ?2",
            params![now, current_epoch],
        )?;
        let projects = {
            let mut statement = transaction.prepare(
                "SELECT id, state
                 FROM project_runs
                 WHERE active_daemon_epoch IS NOT NULL
                   AND active_daemon_epoch != ?1
                   AND (
                       state = 'running'
                       OR (
                           state = 'paused'
                           AND EXISTS (
                               SELECT 1
                               FROM worker_sessions s
                               JOIN worker_turns wt ON wt.session_id = s.id
                               WHERE s.project_id = project_runs.id
                                 AND wt.status IN ('claimed', 'running')
                           )
                       )
                   )
                 ORDER BY created_at, id",
            )?;
            statement
                .query_map([current_epoch], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut recovery = DaemonRecovery::default();
        for (project_id, project_state) in projects {
            let turns = {
                let mut statement = transaction.prepare(
                    "SELECT wt.id, wt.session_id, s.task_id
                     FROM worker_turns wt
                     JOIN worker_sessions s ON s.id = wt.session_id
                     WHERE s.project_id = ?1 AND wt.status IN ('claimed', 'running')
                     ORDER BY wt.created_at, wt.id",
                )?;
                statement
                    .query_map([&project_id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (turn_id, session_id, task_id) in turns {
                let changed = transaction.execute(
                    "UPDATE worker_turns
                     SET status = 'indeterminate', progress_phase = 'done',
                         error_kind = 'daemon_restart',
                         error_message = 'worker attempt crossed a daemon epoch',
                         updated_at = ?1
                     WHERE id = ?2 AND status IN ('claimed', 'running')",
                    params![now, turn_id],
                )?;
                recovery.indeterminate_turns += changed as u64;
                transaction.execute(
                    "UPDATE worker_sessions
                     SET state = 'indeterminate', updated_at = ?1
                     WHERE id = ?2 AND state IN ('creating', 'active')",
                    params![now, session_id],
                )?;
                transition_task_state(
                    &transaction,
                    &project_id,
                    &task_id,
                    &[
                        "queued",
                        "developing",
                        "awaiting_review",
                        "changes_requested",
                        "finalizing",
                    ],
                    "indeterminate",
                    "daemon_restart",
                    "recovery",
                    current_epoch,
                    &sha256_hex(turn_id.as_bytes()),
                    Some(&turn_id),
                    None,
                    now,
                )?;
            }
            transition_project_state(
                &transaction,
                &project_id,
                &project_state,
                "needs_recovery",
                Some("daemon_restart"),
                Some(current_epoch),
                "daemon_restart",
                "recovery",
                current_epoch,
                &sha256_hex(project_id.as_bytes()),
                now,
            )?;
            recovery.interrupted_projects += 1;
        }
        transaction.commit()?;
        Ok(recovery)
    }

    pub(crate) fn record_environment_lease(
        &mut self,
        project_id: &str,
        descriptor: &EnvironmentLeaseDescriptor,
    ) -> Result<()> {
        descriptor.validate()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active_epoch: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM daemon_epochs WHERE id = ?1 AND state = 'active'
             )",
            [&descriptor.daemon_epoch],
            |row| row.get(0),
        )?;
        if !active_epoch {
            bail!("environment lease daemon epoch is not active");
        }
        let now = now_epoch_seconds()?;
        transaction.execute(
            "UPDATE execution_environment_leases
             SET state = 'lost', lost_at = ?1
             WHERE project_id = ?2 AND state = 'active'",
            params![now, project_id],
        )?;
        transaction.execute(
            "INSERT INTO execution_environment_leases (
                 project_id, lease_id, daemon_epoch, environment_hash,
                 inherited_names_json, required_names_json, state, created_at, lost_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, NULL)",
            params![
                project_id,
                descriptor.lease_id,
                descriptor.daemon_epoch,
                descriptor.environment_hash,
                serde_json::to_string(&descriptor.inherited_names)?,
                serde_json::to_string(&descriptor.required_names)?,
                now,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn request_project_run(
        &mut self,
        project_id: &str,
        expected_version: u64,
        plan_version: u64,
        plan_hash: &str,
        daemon_epoch: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let plan_matches: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM project_plans
                 WHERE project_id = ?1 AND version = ?2 AND plan_hash = ?3
                   AND state = 'approved'
             )",
            params![project_id, to_i64(plan_version)?, plan_hash],
            |row| row.get(0),
        )?;
        if !plan_matches {
            bail!("approved plan snapshot does not match the run request");
        }
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE project_runs
             SET state = 'running', version = version + 1,
                 run_requested_at = ?1, active_daemon_epoch = ?2,
                 pause_reason = NULL, updated_at = ?1
             WHERE id = ?3 AND version = ?4 AND state = 'approved'
               AND approved_plan_version = ?5 AND approved_plan_hash = ?6",
            params![
                now,
                daemon_epoch,
                project_id,
                to_i64(expected_version)?,
                to_i64(plan_version)?,
                plan_hash,
            ],
        )?;
        if changed != 1 {
            bail!("project run intent CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "project",
            project_id,
            expected_version,
            "approved",
            "running",
            "project_run",
            "human",
            "human",
            plan_hash,
            None,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn resume_project_with_epoch(
        &mut self,
        project_id: &str,
        expected_version: u64,
        daemon_epoch: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let from_state: String = transaction.query_row(
            "SELECT state FROM project_runs WHERE id = ?1 AND version = ?2",
            params![project_id, to_i64(expected_version)?],
            |row| row.get(0),
        )?;
        if !matches!(from_state.as_str(), "paused" | "needs_recovery") {
            bail!("project is not resumable");
        }
        let changed = transaction.execute(
            "UPDATE project_runs
             SET state = 'running', version = version + 1, pause_reason = NULL,
                 active_daemon_epoch = ?1, updated_at = ?2
             WHERE id = ?3 AND version = ?4
               AND state IN ('paused', 'needs_recovery')",
            params![daemon_epoch, now, project_id, to_i64(expected_version)?],
        )?;
        if changed != 1 {
            bail!("project resume CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "project",
            project_id,
            expected_version,
            &from_state,
            "running",
            "project_resume",
            "human",
            "human",
            &sha256_hex(daemon_epoch.as_bytes()),
            None,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_missing_environment(
        &mut self,
        project_id: &str,
        daemon_epoch: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transition_project_state(
            &transaction,
            project_id,
            "running",
            "needs_recovery",
            Some("environment_lease_missing"),
            Some(daemon_epoch),
            "environment_lease_missing",
            "recovery",
            daemon_epoch,
            &sha256_hex(project_id.as_bytes()),
            now_epoch_seconds()?,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn enqueue_next_ready_task(&mut self, project_id: &str) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let project: Option<(i64, String, String, i64, Option<i64>)> = transaction
            .query_row(
                "SELECT p.version, p.checkpoint_sha, plan.id, plan.version,
                        plan.automatic_through_ordinal
                 FROM project_runs p
                 JOIN project_plans plan
                   ON plan.project_id = p.id
                  AND plan.version = p.approved_plan_version
                  AND plan.plan_hash = p.approved_plan_hash
                  AND plan.state = 'approved'
                 WHERE p.id = ?1 AND p.state = 'running'",
                [project_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((project_version, checkpoint, plan_id, _, automatic_through)) = project else {
            return Ok(false);
        };
        let active_count: i64 = transaction.query_row(
            "SELECT count(*)
             FROM project_tasks
             WHERE project_id = ?1 AND plan_id = ?2
               AND state NOT IN ('draft', 'completed', 'superseded', 'canceled')",
            params![project_id, plan_id],
            |row| row.get(0),
        )?;
        if active_count != 0 {
            transaction.commit()?;
            return Ok(false);
        }
        let ready: Option<(String, i64, i64, String, String, i64)> = transaction
            .query_row(
                "SELECT t.id, t.version, t.ordinal, t.spec_json, t.spec_hash,
                        t.review_round
                 FROM project_tasks t
                 WHERE t.project_id = ?1 AND t.plan_id = ?2 AND t.state = 'draft'
                   AND NOT EXISTS (
                       SELECT 1
                       FROM task_dependencies dep
                       JOIN project_tasks prerequisite
                         ON prerequisite.id = dep.depends_on_task_id
                       WHERE dep.task_id = t.id
                         AND prerequisite.state != 'completed'
                   )
                 ORDER BY t.ordinal, t.id
                 LIMIT 1",
                params![project_id, plan_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((task_id, task_version, ordinal, spec_json, spec_hash, review_round)) = ready
        else {
            let unfinished: i64 = transaction.query_row(
                "SELECT count(*) FROM project_tasks
                 WHERE project_id = ?1 AND plan_id = ?2 AND state != 'completed'",
                params![project_id, plan_id],
                |row| row.get(0),
            )?;
            if unfinished == 0 {
                let now = now_epoch_seconds()?;
                transition_project_state(
                    &transaction,
                    project_id,
                    "running",
                    "completed",
                    None,
                    None,
                    "project_completed",
                    "scheduler",
                    "scheduler",
                    &sha256_hex(checkpoint.as_bytes()),
                    now,
                )?;
                transaction.commit()?;
                return Ok(true);
            }
            transaction.commit()?;
            return Ok(false);
        };
        if automatic_through.is_some_and(|last| ordinal > last) {
            let now = now_epoch_seconds()?;
            transition_project_state(
                &transaction,
                project_id,
                "running",
                "paused",
                Some("run_boundary"),
                None,
                "automatic_boundary",
                "scheduler",
                "scheduler",
                &spec_hash,
                now,
            )?;
            transaction.commit()?;
            return Ok(true);
        }
        let now = now_epoch_seconds()?;
        let queued_version = task_version + 1;
        let changed = transaction.execute(
            "UPDATE project_tasks
             SET state = 'queued', version = version + 1, base_revision = ?1,
                 updated_at = ?2
             WHERE id = ?3 AND project_id = ?4 AND plan_id = ?5
               AND version = ?6 AND state = 'draft'",
            params![checkpoint, now, task_id, project_id, plan_id, task_version,],
        )?;
        if changed != 1 {
            bail!("ready task enqueue CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "task",
            &task_id,
            to_u64(task_version)?,
            "draft",
            "queued",
            "task_enqueue",
            "scheduler",
            "scheduler",
            &spec_hash,
            None,
            None,
            now,
        )?;
        let (profile_id, adapter, native_mode): (String, String, String) = transaction.query_row(
            "SELECT profile.id, profile.adapter, profile.native_session_mode
                 FROM project_plans plan
                 JOIN worker_profiles profile
                   ON profile.id = plan.developer_profile_id
                 WHERE plan.id = ?1",
            [&plan_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let session_id = format!("session-{}", Uuid::new_v4());
        let native_session_id = (native_mode == "preassigned").then(|| Uuid::new_v4().to_string());
        transaction.execute(
            "INSERT INTO worker_sessions (
                 id, project_id, task_id, role, profile_id, adapter,
                 native_session_id, state, created_at, closed_at, updated_at
             ) VALUES (?1, ?2, ?3, 'developer', ?4, ?5, ?6,
                       'creating', ?7, NULL, ?7)",
            params![
                session_id,
                project_id,
                task_id,
                profile_id,
                adapter,
                native_session_id,
                now,
            ],
        )?;
        let developing_version = queued_version + 1;
        let changed = transaction.execute(
            "UPDATE project_tasks
             SET state = 'developing', developer_session_id = ?1,
                 version = version + 1, updated_at = ?2
             WHERE id = ?3 AND version = ?4 AND state = 'queued'",
            params![session_id, now, task_id, queued_version],
        )?;
        if changed != 1 {
            bail!("developer session bind CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "task",
            &task_id,
            to_u64(queued_version)?,
            "queued",
            "developing",
            "developer_session_create",
            "scheduler",
            "scheduler",
            &sha256_hex(session_id.as_bytes()),
            None,
            None,
            now,
        )?;
        insert_turn(
            &transaction,
            project_id,
            &task_id,
            &session_id,
            WorkerRole::Developer,
            1,
            TurnKind::Create,
            developing_version,
            review_round,
            &spec_json,
            now,
        )?;
        let _ = project_version;
        transaction.commit()?;
        Ok(true)
    }

    pub(crate) fn next_queued_turn(&self, project_id: &str) -> Result<Option<ReadyTurn>> {
        self.connection
            .query_row(
                READY_TURN_SQL,
                params![project_id, "queued"],
                read_ready_turn,
            )
            .optional()
            .context("failed to select queued worker turn")
    }

    pub(crate) fn claim_turn(
        &mut self,
        turn_id: &str,
        daemon_epoch: &str,
        lease_duration: Duration,
    ) -> Result<ClaimedTurn> {
        if lease_duration.is_zero() || lease_duration > Duration::from_secs(300) {
            bail!("worker lease duration is outside its bound");
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ready = transaction
            .query_row(
                READY_TURN_BY_ID_SQL,
                params![turn_id, "queued"],
                read_ready_turn,
            )
            .optional()?
            .ok_or_else(|| anyhow!("queued worker turn is unavailable"))?;
        let attempt = ready
            .attempt
            .checked_add(1)
            .ok_or_else(|| anyhow!("worker attempt overflow"))?;
        let lease_owner = format!("{daemon_epoch}/lease-{}", Uuid::new_v4());
        let now = now_epoch_seconds()?;
        let expires = now
            .checked_add(
                i64::try_from(lease_duration.as_secs()).context("lease duration overflow")?,
            )
            .ok_or_else(|| anyhow!("lease expiration overflow"))?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'claimed', attempt = ?1, lease_owner = ?2,
                 expires_at = ?3, progress_phase = 'spawn',
                 last_progress_at = ?4, updated_at = ?4
             WHERE id = ?5 AND status = 'queued' AND attempt = ?6",
            params![attempt, lease_owner, expires, now, turn_id, ready.attempt,],
        )?;
        if changed != 1 {
            bail!("worker turn claim CAS failed");
        }
        transaction.commit()?;
        Ok(ClaimedTurn {
            ready,
            attempt,
            lease_owner,
        })
    }

    pub(crate) fn bind_spawned_turn(
        &mut self,
        claim: &ClaimedTurn,
        pid: u32,
        process_birth: &str,
    ) -> Result<()> {
        let changed = self.connection.execute(
            "UPDATE worker_turns
             SET status = 'running', worker_pid = ?1, process_birth = ?2,
                 progress_phase = 'running', last_progress_at = ?3,
                 started_at = ?3, updated_at = ?3
             WHERE id = ?4 AND status = 'claimed' AND attempt = ?5
               AND lease_owner = ?6",
            params![
                pid,
                process_birth,
                now_epoch_seconds()?,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
            ],
        )?;
        if changed != 1 {
            bail!("spawned worker bind CAS failed");
        }
        Ok(())
    }

    pub(crate) fn bind_review_snapshot(
        &mut self,
        claim: &ClaimedTurn,
        snapshot_digest: &str,
    ) -> Result<()> {
        if claim.ready.role != WorkerRole::Reviewer {
            bail!("only a reviewer turn may bind a review snapshot");
        }
        crate::worker::validation::validate_sha256("review snapshot digest", snapshot_digest)?;
        let changed = self.connection.execute(
            "UPDATE worker_turns
             SET review_snapshot_digest = ?1, updated_at = ?2
             WHERE id = ?3 AND status = 'claimed' AND attempt = ?4
               AND lease_owner = ?5 AND review_snapshot_digest IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM worker_sessions s
                   JOIN project_tasks t
                     ON t.id = s.task_id AND t.project_id = s.project_id
                   WHERE s.id = worker_turns.session_id
                     AND s.role = 'reviewer'
                     AND t.state = 'awaiting_review'
                     AND t.version = worker_turns.task_version
                     AND t.review_round = worker_turns.review_round
                     AND t.base_revision = ?6
                     AND t.head_revision = ?7
               )",
            params![
                snapshot_digest,
                now_epoch_seconds()?,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
                claim.ready.base_revision,
                claim
                    .ready
                    .head_revision
                    .as_deref()
                    .ok_or_else(|| anyhow!("reviewer turn lost its exact head revision"))?,
            ],
        )?;
        if changed != 1 {
            bail!("review snapshot bind CAS failed");
        }
        Ok(())
    }

    pub(crate) fn heartbeat_turn(
        &mut self,
        claim: &ClaimedTurn,
        pid: u32,
        process_birth: &str,
        lease_duration: Duration,
    ) -> Result<bool> {
        let now = now_epoch_seconds()?;
        let expires = now
            .checked_add(
                i64::try_from(lease_duration.as_secs()).context("lease duration overflow")?,
            )
            .ok_or_else(|| anyhow!("lease expiration overflow"))?;
        let changed = self.connection.execute(
            "UPDATE worker_turns
             SET expires_at = ?1, last_progress_at = ?2, updated_at = ?2
             WHERE id = ?3 AND status = 'running' AND attempt = ?4
               AND lease_owner = ?5 AND worker_pid = ?6 AND process_birth = ?7
               AND EXISTS (
                   SELECT 1 FROM worker_sessions s
                   JOIN project_runs p ON p.id = s.project_id
                   WHERE s.id = worker_turns.session_id AND p.state = 'running'
               )",
            params![
                expires,
                now,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
                pid,
                process_birth,
            ],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn mark_spawn_failed(
        &mut self,
        claim: &ClaimedTurn,
        error_kind: &str,
        error_message: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'failed', progress_phase = 'done',
                 error_kind = ?1, error_message = ?2, updated_at = ?3
             WHERE id = ?4 AND status = 'claimed' AND attempt = ?5
               AND lease_owner = ?6",
            params![
                error_kind,
                error_message,
                now,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
            ],
        )?;
        if changed != 1 {
            bail!("spawn failure CAS failed");
        }
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'failed', updated_at = ?1
             WHERE id = ?2 AND state IN ('creating', 'active')",
            params![now, claim.ready.session_id],
        )?;
        transition_task_state(
            &transaction,
            &claim.ready.project_id,
            &claim.ready.task_id,
            &["developing", "awaiting_review"],
            "failed",
            "worker_spawn_failed",
            "scheduler",
            "scheduler",
            &sha256_hex(error_message.as_bytes()),
            Some(&claim.ready.turn_id),
            None,
            now,
        )?;
        transition_project_state(
            &transaction,
            &claim.ready.project_id,
            "running",
            "failed",
            Some(error_kind),
            None,
            "worker_spawn_failed",
            "scheduler",
            "scheduler",
            &sha256_hex(error_message.as_bytes()),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_turn_indeterminate(
        &mut self,
        claim: &ClaimedTurn,
        error_kind: &str,
        error_message: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'indeterminate', progress_phase = 'done',
                 error_kind = ?1, error_message = ?2, updated_at = ?3
             WHERE id = ?4 AND status IN ('claimed', 'running') AND attempt = ?5
               AND lease_owner = ?6",
            params![
                error_kind,
                error_message,
                now,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
            ],
        )?;
        if changed != 1 {
            bail!("indeterminate turn CAS failed");
        }
        let project_state: String = transaction.query_row(
            "SELECT state FROM project_runs WHERE id = ?1",
            [&claim.ready.project_id],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'indeterminate', updated_at = ?1
             WHERE id = ?2 AND state IN ('creating', 'active')",
            params![now, claim.ready.session_id],
        )?;
        transition_task_state(
            &transaction,
            &claim.ready.project_id,
            &claim.ready.task_id,
            &[
                "queued",
                "developing",
                "awaiting_review",
                "changes_requested",
                "finalizing",
            ],
            "indeterminate",
            "turn_indeterminate",
            "scheduler",
            "scheduler",
            &sha256_hex(error_message.as_bytes()),
            Some(&claim.ready.turn_id),
            None,
            now,
        )?;
        match project_state.as_str() {
            "running" | "paused" => {
                transition_project_state(
                    &transaction,
                    &claim.ready.project_id,
                    &project_state,
                    "needs_recovery",
                    Some(error_kind),
                    None,
                    "turn_indeterminate",
                    "scheduler",
                    "scheduler",
                    &sha256_hex(error_message.as_bytes()),
                    now,
                )?;
            }
            "needs_recovery" => {}
            _ => bail!("indeterminate turn conflicts with terminal project state"),
        }
        transaction.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_result_ready(
        &mut self,
        claim: &ClaimedTurn,
        pid: u32,
        process_birth: &str,
        native_session_id: &str,
        result_json: &str,
        result_hash: &str,
        activity_truncated: bool,
    ) -> Result<()> {
        if sha256_hex(result_json.as_bytes()) != result_hash {
            bail!("turn result hash mismatch");
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (expected_native, session_state, task_state, task_version, project_state): (
            Option<String>,
            String,
            String,
            i64,
            String,
        ) = transaction.query_row(
            "SELECT s.native_session_id, s.state, t.state, t.version, p.state
             FROM worker_sessions s
             JOIN project_tasks t ON t.id = s.task_id AND t.project_id = s.project_id
             JOIN project_runs p ON p.id = s.project_id
             WHERE s.id = ?1",
            [&claim.ready.session_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        let expected_task_state = match claim.ready.role {
            WorkerRole::Developer => "developing",
            WorkerRole::Reviewer => "awaiting_review",
        };
        if project_state != "running"
            || !matches!(session_state.as_str(), "creating" | "active")
            || task_state != expected_task_state
            || task_version != to_i64(claim.ready.task_version)?
        {
            bail!("worker result is stale relative to durable project state");
        }
        match expected_native {
            Some(expected) if expected == native_session_id => {}
            None => {
                let changed = transaction.execute(
                    "UPDATE worker_sessions
                     SET native_session_id = ?1, updated_at = ?2
                     WHERE id = ?3 AND state = 'creating' AND native_session_id IS NULL",
                    params![
                        native_session_id,
                        now_epoch_seconds()?,
                        claim.ready.session_id
                    ],
                )?;
                if changed != 1 {
                    bail!("native session bind CAS failed");
                }
            }
            Some(_) => bail!("native session result does not match its durable binding"),
        }
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'result_ready', progress_phase = 'applying',
                 result_json = ?1, result_hash = ?2, result_at = ?3,
                 activity_truncated = ?4, updated_at = ?3
             WHERE id = ?5 AND status = 'running' AND attempt = ?6
               AND lease_owner = ?7 AND worker_pid = ?8 AND process_birth = ?9",
            params![
                result_json,
                result_hash,
                now,
                activity_truncated,
                claim.ready.turn_id,
                claim.attempt,
                claim.lease_owner,
                pid,
                process_birth,
            ],
        )?;
        if changed != 1 {
            bail!("turn result_ready CAS failed");
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn next_result_ready_turn(&self, project_id: &str) -> Result<Option<ReadyTurn>> {
        self.connection
            .query_row(
                READY_TURN_SQL,
                params![project_id, "result_ready"],
                read_ready_turn,
            )
            .optional()
            .context("failed to select result_ready turn")
    }

    pub(crate) fn apply_result_ready(&mut self, ready: &ReadyTurn) -> Result<ApplyOutcome> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (result_json, result_hash): (String, String) = transaction.query_row(
            "SELECT result_json, result_hash FROM worker_turns
             WHERE id = ?1 AND status = 'result_ready'",
            [&ready.turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if sha256_hex(result_json.as_bytes()) != result_hash {
            bail!("stored result_ready hash mismatch");
        }
        if ready.role == WorkerRole::Reviewer {
            let (task_state, task_version, review_round, base_revision, head_revision): (
                String,
                i64,
                i64,
                String,
                String,
            ) = transaction.query_row(
                "SELECT state, version, review_round, base_revision, head_revision
                 FROM project_tasks
                 WHERE id = ?1 AND project_id = ?2",
                params![ready.task_id, ready.project_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?;
            if task_state != "awaiting_review"
                || task_version != to_i64(ready.task_version)?
                || review_round != i64::from(ready.review_round)
                || base_revision != ready.base_revision
                || Some(head_revision.as_str()) != ready.head_revision.as_deref()
                || ready.review_snapshot_digest.is_none()
            {
                bail!("reviewer result_ready lost its exact task and revision binding");
            }
        }
        let now = now_epoch_seconds()?;
        let applied = transaction.execute(
            "UPDATE worker_turns
             SET status = 'applied', progress_phase = 'done',
                 applied_at = ?1, updated_at = ?1
             WHERE id = ?2 AND status = 'result_ready' AND result_hash = ?3",
            params![now, ready.turn_id, result_hash],
        )?;
        if applied != 1 {
            bail!("result_ready apply CAS failed");
        }
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'active', updated_at = ?1
             WHERE id = ?2 AND state = 'creating' AND native_session_id IS NOT NULL",
            params![now, ready.session_id],
        )?;
        let outcome = match ready.role {
            WorkerRole::Developer => {
                let result = DeveloperResult::parse(result_json.as_bytes())?;
                match result.decision {
                    DeveloperDecision::Completed => {
                        apply_completed_developer(
                            &transaction,
                            ready,
                            &result,
                            &result_json,
                            &result_hash,
                            now,
                        )?;
                        ApplyOutcome::DeveloperCompleted
                    }
                    DeveloperDecision::NeedsInput => {
                        transition_task_state(
                            &transaction,
                            &ready.project_id,
                            &ready.task_id,
                            &["developing"],
                            "needs_input",
                            "developer_needs_input",
                            "developer_result",
                            &ready.session_id,
                            &result_hash,
                            Some(&ready.turn_id),
                            Some(&result_hash),
                            now,
                        )?;
                        transition_project_state(
                            &transaction,
                            &ready.project_id,
                            "running",
                            "needs_input",
                            Some("developer_question"),
                            None,
                            "developer_needs_input",
                            "developer_result",
                            &ready.session_id,
                            &result_hash,
                            now,
                        )?;
                        ApplyOutcome::NeedsInput
                    }
                    DeveloperDecision::Blocked => {
                        transition_task_state(
                            &transaction,
                            &ready.project_id,
                            &ready.task_id,
                            &["developing"],
                            "failed",
                            "developer_blocked",
                            "developer_result",
                            &ready.session_id,
                            &result_hash,
                            Some(&ready.turn_id),
                            Some(&result_hash),
                            now,
                        )?;
                        transition_project_state(
                            &transaction,
                            &ready.project_id,
                            "running",
                            "failed",
                            Some("developer_blocked"),
                            None,
                            "developer_blocked",
                            "developer_result",
                            &ready.session_id,
                            &result_hash,
                            now,
                        )?;
                        ApplyOutcome::Failed
                    }
                }
            }
            WorkerRole::Reviewer => {
                let result = ReviewerResult::parse(result_json.as_bytes())?;
                match result.decision {
                    ReviewDecision::RequestChanges => {
                        let resumed = apply_request_changes(
                            &transaction,
                            ready,
                            &result_hash,
                            &result_json,
                            now,
                        )?;
                        if resumed {
                            ApplyOutcome::ReviewerChangesRequested
                        } else {
                            ApplyOutcome::NeedsInput
                        }
                    }
                    ReviewDecision::Lgtm => {
                        transition_task_state(
                            &transaction,
                            &ready.project_id,
                            &ready.task_id,
                            &["awaiting_review"],
                            "finalizing",
                            "reviewer_lgtm",
                            "reviewer_result",
                            &ready.session_id,
                            &result_hash,
                            Some(&ready.turn_id),
                            Some(&result_hash),
                            now,
                        )?;
                        ApplyOutcome::ReviewerLgtm
                    }
                }
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    pub(crate) fn mark_result_ready_invalid(
        &mut self,
        ready: &ReadyTurn,
        error_kind: &str,
        error_message: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'stale', progress_phase = 'done',
                 error_kind = ?1, error_message = ?2, updated_at = ?3
             WHERE id = ?4 AND status = 'result_ready' AND result_hash = ?5",
            params![
                error_kind,
                error_message,
                now,
                ready.turn_id,
                ready.result_hash,
            ],
        )?;
        if changed != 1 {
            bail!("invalid result_ready turn CAS failed");
        }
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'indeterminate', updated_at = ?1
             WHERE id = ?2 AND state IN ('creating', 'active')",
            params![now, ready.session_id],
        )?;
        transition_task_state(
            &transaction,
            &ready.project_id,
            &ready.task_id,
            &["developing", "awaiting_review"],
            "indeterminate",
            "invalid_result_ready",
            "recovery",
            "scheduler",
            &sha256_hex(error_message.as_bytes()),
            Some(&ready.turn_id),
            ready.result_hash.as_deref(),
            now,
        )?;
        let project_state: String = transaction.query_row(
            "SELECT state FROM project_runs WHERE id = ?1",
            [&ready.project_id],
            |row| row.get(0),
        )?;
        match project_state.as_str() {
            "running" => {
                transition_project_state(
                    &transaction,
                    &ready.project_id,
                    "running",
                    "needs_recovery",
                    Some(error_kind),
                    None,
                    "invalid_result_ready",
                    "recovery",
                    "scheduler",
                    &sha256_hex(error_message.as_bytes()),
                    now,
                )?;
            }
            "needs_recovery" => {}
            _ => bail!("invalid result_ready conflicts with project state"),
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn finalize_next_task(&mut self, project_id: &str) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task: Option<(String, i64, String, String)> = transaction
            .query_row(
                "SELECT id, version, head_revision, result_hash
                 FROM project_tasks
                 WHERE project_id = ?1 AND state = 'finalizing'
                 ORDER BY ordinal, id LIMIT 1",
                [project_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((task_id, version, head_revision, result_hash)) = task else {
            return Ok(false);
        };
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE project_tasks
             SET state = 'completed', version = version + 1,
                 completed_at = ?1, updated_at = ?1
             WHERE id = ?2 AND version = ?3 AND state = 'finalizing'",
            params![now, task_id, version],
        )?;
        if changed != 1 {
            bail!("task finalize CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "task",
            &task_id,
            to_u64(version)?,
            "finalizing",
            "completed",
            "task_finalize",
            "scheduler",
            "scheduler",
            &result_hash,
            None,
            Some(&result_hash),
            now,
        )?;
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'closed', closed_at = ?1, updated_at = ?1
             WHERE task_id = ?2 AND state = 'active'",
            params![now, task_id],
        )?;
        let (project_version, project_state): (i64, String) = transaction.query_row(
            "SELECT version, state FROM project_runs WHERE id = ?1",
            [project_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if !matches!(project_state.as_str(), "running" | "needs_recovery") {
            bail!("task finalize requires a live durable project");
        }
        let project_changed = transaction.execute(
            "UPDATE project_runs
             SET checkpoint_sha = ?1, version = version + 1, updated_at = ?2
             WHERE id = ?3 AND version = ?4 AND state = ?5",
            params![
                head_revision,
                now,
                project_id,
                project_version,
                project_state
            ],
        )?;
        if project_changed != 1 {
            bail!("project checkpoint CAS failed");
        }
        insert_transition(
            &transaction,
            project_id,
            "project",
            project_id,
            to_u64(project_version)?,
            &project_state,
            &project_state,
            "checkpoint_advance",
            "scheduler",
            "scheduler",
            &sha256_hex(head_revision.as_bytes()),
            None,
            Some(&result_hash),
            now,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub(crate) fn project_snapshot(&self, project_id: &str) -> Result<SchedulerSnapshot> {
        let (state, version, checkpoint): (String, i64, String) = self.connection.query_row(
            "SELECT state, version, checkpoint_sha FROM project_runs WHERE id = ?1",
            [project_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let task_states = {
            let mut statement = self.connection.prepare(
                "SELECT task_key, state FROM project_tasks
                 WHERE project_id = ?1 ORDER BY plan_id, ordinal",
            )?;
            statement
                .query_map([project_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let native_sessions = |role: &str| -> Result<Vec<String>> {
            let mut statement = self.connection.prepare(
                "SELECT native_session_id FROM worker_sessions
                 WHERE project_id = ?1 AND role = ?2 AND native_session_id IS NOT NULL
                 ORDER BY created_at, id",
            )?;
            Ok(statement
                .query_map(params![project_id, role], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        };
        let counts: (i64, i64, i64) = self.connection.query_row(
            "SELECT count(*),
                    COALESCE(sum(CASE WHEN wt.status = 'applied' THEN 1 ELSE 0 END), 0),
                    COALESCE(sum(CASE WHEN wt.status = 'result_ready' THEN 1 ELSE 0 END), 0)
             FROM worker_turns wt
             JOIN worker_sessions s ON s.id = wt.session_id
             WHERE s.project_id = ?1",
            [project_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let transition_count: i64 = self.connection.query_row(
            "SELECT count(*) FROM state_transitions WHERE project_id = ?1",
            [project_id],
            |row| row.get(0),
        )?;
        Ok(SchedulerSnapshot {
            project_state: state,
            project_version: to_u64(version)?,
            checkpoint_sha: checkpoint,
            task_states,
            developer_native_sessions: native_sessions("developer")?,
            reviewer_native_sessions: native_sessions("reviewer")?,
            turn_count: to_u64(counts.0)?,
            applied_turn_count: to_u64(counts.1)?,
            result_ready_turn_count: to_u64(counts.2)?,
            transition_count: to_u64(transition_count)?,
        })
    }

    pub(crate) fn project_worktree_root(&self, project_id: &str) -> Result<String> {
        self.connection
            .query_row(
                "SELECT worktree_root FROM project_runs WHERE id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .context("failed to read project worktree root")
    }

    pub(crate) fn manifest_environment_matches(
        &self,
        project_id: &str,
        daemon_epoch: &str,
        environment_hash: &str,
    ) -> Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1
                     FROM execution_environment_leases
                     WHERE project_id = ?1 AND daemon_epoch = ?2
                       AND environment_hash = ?3
                 )",
                params![project_id, daemon_epoch, environment_hash],
                |row| row.get(0),
            )
            .context("failed to verify manifest environment binding")
    }

    pub(crate) fn project_state(&self, project_id: &str) -> Result<(ProjectExecutionState, u64)> {
        let (state, version): (String, i64) = self.connection.query_row(
            "SELECT state, version FROM project_runs WHERE id = ?1",
            [project_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((
            match state.as_str() {
                "approved" => ProjectExecutionState::Approved,
                "running" => ProjectExecutionState::Running,
                "paused" => ProjectExecutionState::Paused,
                "needs_input" => ProjectExecutionState::NeedsInput,
                "needs_recovery" => ProjectExecutionState::NeedsRecovery,
                "completed" => ProjectExecutionState::Completed,
                "failed" => ProjectExecutionState::Failed,
                "canceled" => ProjectExecutionState::Canceled,
                _ => bail!("project is not in an executable state"),
            },
            to_u64(version)?,
        ))
    }

    pub(crate) fn pause_project(
        &mut self,
        project_id: &str,
        expected_version: u64,
        reason: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transition_project_state_at_version(
            &transaction,
            project_id,
            expected_version,
            "running",
            "paused",
            Some(reason),
            None,
            "project_pause",
            "human",
            "human",
            &sha256_hex(reason.as_bytes()),
            now_epoch_seconds()?,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn cancel_project(
        &mut self,
        project_id: &str,
        expected_version: u64,
        reason: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transition_project_state_at_version(
            &transaction,
            project_id,
            expected_version,
            "running",
            "canceled",
            Some(reason),
            None,
            "project_cancel",
            "human",
            "human",
            &sha256_hex(reason.as_bytes()),
            now_epoch_seconds()?,
        )?;
        let active_tasks = {
            let mut statement = transaction.prepare(
                "SELECT id FROM project_tasks
                 WHERE project_id = ?1 AND state IN (
                     'queued', 'developing', 'awaiting_review',
                     'changes_requested', 'finalizing', 'needs_input', 'indeterminate'
                 )
                 ORDER BY ordinal, id",
            )?;
            statement
                .query_map([project_id], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let now = now_epoch_seconds()?;
        for task_id in active_tasks {
            transition_task_state(
                &transaction,
                project_id,
                &task_id,
                &[
                    "queued",
                    "developing",
                    "awaiting_review",
                    "changes_requested",
                    "finalizing",
                    "needs_input",
                    "indeterminate",
                ],
                "canceled",
                "project_cancel",
                "human",
                "human",
                &sha256_hex(reason.as_bytes()),
                None,
                None,
                now,
            )?;
        }
        transaction.execute(
            "UPDATE worker_turns
             SET status = 'canceled', progress_phase = 'done',
                 error_kind = 'human_cancel', error_message = 'project stopped',
                 updated_at = ?1
             WHERE session_id IN (
                 SELECT id FROM worker_sessions WHERE project_id = ?2
             ) AND status IN ('queued', 'result_ready')",
            params![now, project_id],
        )?;
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'closed', closed_at = ?1, updated_at = ?1
             WHERE project_id = ?2 AND state IN ('creating', 'active')",
            params![now, project_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_canceled_after_signal(&mut self, claim: &ClaimedTurn) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'canceled', progress_phase = 'done',
                 error_kind = 'human_cancel', error_message = 'project stopped',
                 updated_at = ?1
             WHERE id = ?2 AND status IN ('claimed', 'running') AND attempt = ?3
               AND lease_owner = ?4",
            params![now, claim.ready.turn_id, claim.attempt, claim.lease_owner],
        )?;
        if changed != 1 {
            bail!("canceled worker turn CAS failed");
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_paused_attempt_indeterminate(&mut self, claim: &ClaimedTurn) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let changed = transaction.execute(
            "UPDATE worker_turns
             SET status = 'indeterminate', progress_phase = 'done',
                 error_kind = 'human_pause',
                 error_message = 'running worker stopped after durable pause',
                 updated_at = ?1
             WHERE id = ?2 AND status = 'running' AND attempt = ?3
               AND lease_owner = ?4",
            params![now, claim.ready.turn_id, claim.attempt, claim.lease_owner],
        )?;
        if changed != 1 {
            bail!("paused worker turn CAS failed");
        }
        transaction.execute(
            "UPDATE worker_sessions
             SET state = 'indeterminate', updated_at = ?1
             WHERE id = ?2 AND state IN ('creating', 'active')",
            params![now, claim.ready.session_id],
        )?;
        transition_task_state(
            &transaction,
            &claim.ready.project_id,
            &claim.ready.task_id,
            &["developing", "awaiting_review"],
            "indeterminate",
            "worker_stopped_after_pause",
            "scheduler",
            "scheduler",
            &sha256_hex(claim.ready.turn_id.as_bytes()),
            Some(&claim.ready.turn_id),
            None,
            now,
        )?;
        transition_project_state(
            &transaction,
            &claim.ready.project_id,
            "paused",
            "needs_recovery",
            Some("paused_running_turn"),
            None,
            "worker_stopped_after_pause",
            "scheduler",
            "scheduler",
            &sha256_hex(claim.ready.turn_id.as_bytes()),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn seed_fake_project(&mut self, seed: &FakeProjectSeed) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let plan_hash = sha256_hex(format!("{}:approved-plan", seed.project_id).as_bytes());
        let developer_profile_id = format!("profile-{}-developer", seed.project_id);
        let reviewer_profile_id = format!("profile-{}-reviewer", seed.project_id);
        let approved_plan_id = format!("plan-{}-approved", seed.project_id);
        transaction.execute(
            "INSERT INTO project_runs (
                 id, state, version, pause_reason, source_repo_root,
                 source_git_dir_identity, target_ref, target_expected_sha,
                 worktree_root, worktree_branch, checkpoint_sha, applied_target_sha,
                 approved_plan_version, approved_plan_hash, run_requested_at,
                 active_daemon_epoch, created_at, updated_at
             ) VALUES (
                 ?1, 'approved', 0, NULL, ?2, 'fake-git-dir',
                 'refs/heads/main', ?3, ?4, ?5, ?3, NULL,
                 1, ?6, NULL, NULL, ?7, ?7
             )",
            params![
                seed.project_id,
                seed.source_repo_root,
                seed.base_revision,
                seed.developer_worktree,
                format!("refs/heads/hcom-project/{}", seed.project_id),
                plan_hash,
                now,
            ],
        )?;
        insert_profile(
            &transaction,
            &seed.project_id,
            &developer_profile_id,
            &seed.developer_profile,
            now,
        )?;
        insert_profile(
            &transaction,
            &seed.project_id,
            &reviewer_profile_id,
            &seed.reviewer_profile,
            now,
        )?;
        transaction.execute(
            "INSERT INTO project_plans (
                 id, project_id, version, state, base_checkpoint_sha, plan_hash,
                 developer_profile_id, reviewer_profile_id,
                 automatic_through_ordinal, created_by_binding, created_at,
                 approved_at, superseded_at
             ) VALUES (
                 ?1, ?2, 1, 'approved', ?3, ?4,
                 ?5, ?6, NULL, NULL, ?7, ?7, NULL
             )",
            params![
                approved_plan_id,
                seed.project_id,
                seed.base_revision,
                plan_hash,
                developer_profile_id,
                reviewer_profile_id,
                now
            ],
        )?;
        for task in &seed.approved_tasks {
            insert_fake_task(&transaction, &seed.project_id, &approved_plan_id, task, now)?;
        }
        for task in &seed.approved_tasks {
            for dependency in &task.dependencies {
                transaction.execute(
                    "INSERT INTO task_dependencies (task_id, depends_on_task_id)
                     VALUES (?1, ?2)",
                    params![task.id, dependency],
                )?;
            }
        }
        if let Some(unapproved) = &seed.unapproved_task {
            let draft_hash = sha256_hex(format!("{}:draft-plan", seed.project_id).as_bytes());
            let draft_plan_id = format!("plan-{}-draft", seed.project_id);
            transaction.execute(
                "INSERT INTO project_plans (
                     id, project_id, version, state, base_checkpoint_sha, plan_hash,
                     developer_profile_id, reviewer_profile_id,
                     automatic_through_ordinal, created_by_binding, created_at,
                     approved_at, superseded_at
                 ) VALUES (
                     ?1, ?2, 2, 'draft', ?3, ?4,
                     ?5, ?6, NULL, NULL, ?7, NULL, NULL
                 )",
                params![
                    draft_plan_id,
                    seed.project_id,
                    seed.base_revision,
                    draft_hash,
                    developer_profile_id,
                    reviewer_profile_id,
                    now
                ],
            )?;
            insert_fake_task(
                &transaction,
                &seed.project_id,
                &draft_plan_id,
                unapproved,
                now,
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

const READY_TURN_SQL: &str = r#"
SELECT wt.id, wt.session_id, s.project_id, s.task_id, s.role, wt.sequence,
       wt.kind, wt.attempt, wt.task_version, wt.review_round,
       t.base_revision, t.head_revision, t.spec_json,
       (
           SELECT previous.result_json
           FROM worker_turns previous
           JOIN worker_sessions previous_session ON previous_session.id = previous.session_id
           WHERE previous_session.task_id = s.task_id
             AND previous.status = 'applied'
             AND previous.id != wt.id
           ORDER BY previous.applied_at DESC, previous.id DESC
           LIMIT 1
       ),
       s.native_session_id, wt.artifact_dir,
       p.role, p.adapter, p.model, p.reasoning, p.policy, p.cli_path,
       p.executable_identity_json, p.cli_version, p.adapter_contract_ver,
       p.native_session_mode, p.capability_json, wt.result_hash, wt.lease_owner,
       wt.review_snapshot_digest
FROM worker_turns wt
JOIN worker_sessions s ON s.id = wt.session_id
JOIN project_tasks t ON t.id = s.task_id
JOIN worker_profiles p ON p.id = s.profile_id
WHERE s.project_id = ?1 AND wt.status = ?2
ORDER BY t.ordinal, wt.created_at, wt.id
LIMIT 1
"#;

const READY_TURN_BY_ID_SQL: &str = r#"
SELECT wt.id, wt.session_id, s.project_id, s.task_id, s.role, wt.sequence,
       wt.kind, wt.attempt, wt.task_version, wt.review_round,
       t.base_revision, t.head_revision, t.spec_json,
       (
           SELECT previous.result_json
           FROM worker_turns previous
           JOIN worker_sessions previous_session ON previous_session.id = previous.session_id
           WHERE previous_session.task_id = s.task_id
             AND previous.status = 'applied'
             AND previous.id != wt.id
           ORDER BY previous.applied_at DESC, previous.id DESC
           LIMIT 1
       ),
       s.native_session_id, wt.artifact_dir,
       p.role, p.adapter, p.model, p.reasoning, p.policy, p.cli_path,
       p.executable_identity_json, p.cli_version, p.adapter_contract_ver,
       p.native_session_mode, p.capability_json, wt.result_hash, wt.lease_owner,
       wt.review_snapshot_digest
FROM worker_turns wt
JOIN worker_sessions s ON s.id = wt.session_id
JOIN project_tasks t ON t.id = s.task_id
JOIN worker_profiles p ON p.id = s.profile_id
WHERE wt.id = ?1 AND wt.status = ?2
LIMIT 1
"#;

fn read_ready_turn(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReadyTurn> {
    let role_text: String = row.get(4)?;
    let profile_role_text: String = row.get(16)?;
    let role = parse_role(&role_text)?;
    if role_text != profile_role_text {
        return Err(conversion_error("worker role/profile mismatch"));
    }
    let kind_text: String = row.get(6)?;
    let native_mode_text: String = row.get(25)?;
    let executable_json: String = row.get(22)?;
    let capability_json: String = row.get(26)?;
    let executable: ExecutableIdentity =
        serde_json::from_str(&executable_json).map_err(|_| conversion_error("bad executable"))?;
    let capability: CapabilitySnapshot =
        serde_json::from_str(&capability_json).map_err(|_| conversion_error("bad capability"))?;
    let cli_path: String = row.get(21)?;
    if executable.canonical_path.to_string_lossy() != cli_path {
        return Err(conversion_error("worker executable path mismatch"));
    }
    Ok(ReadyTurn {
        turn_id: row.get(0)?,
        session_id: row.get(1)?,
        project_id: row.get(2)?,
        task_id: row.get(3)?,
        role,
        sequence: to_u32_sql(row.get(5)?, 5)?,
        kind: match kind_text.as_str() {
            "create" => TurnKind::Create,
            "resume" => TurnKind::Resume,
            _ => return Err(conversion_error("bad worker turn kind")),
        },
        attempt: to_u32_sql(row.get(7)?, 7)?,
        task_version: to_u64_sql(row.get(8)?, 8)?,
        review_round: to_u32_sql(row.get(9)?, 9)?,
        base_revision: row
            .get::<_, Option<String>>(10)?
            .ok_or_else(|| conversion_error("turn base revision is missing"))?,
        head_revision: row.get(11)?,
        spec_json: row.get(12)?,
        previous_result_json: row.get(13)?,
        native_session_id: row.get(14)?,
        artifact_dir: row.get(15)?,
        profile: WorkerProfile {
            role,
            adapter: row.get(17)?,
            model: row.get(18)?,
            reasoning: row.get(19)?,
            policy: row.get(20)?,
            executable,
            cli_version: row.get(23)?,
            adapter_contract_version: to_u32_sql(row.get(24)?, 24)?,
            native_session_mode: match native_mode_text.as_str() {
                "preassigned" => NativeSessionMode::Preassigned,
                "discovered" => NativeSessionMode::Discovered,
                _ => return Err(conversion_error("bad native session mode")),
            },
            capability,
        },
        result_hash: row.get(27)?,
        lease_owner: row.get(28)?,
        review_snapshot_digest: row.get(29)?,
    })
}

fn apply_completed_developer(
    transaction: &Transaction<'_>,
    ready: &ReadyTurn,
    result: &DeveloperResult,
    result_json: &str,
    result_hash: &str,
    now: i64,
) -> Result<()> {
    let head = result
        .head_revision
        .as_deref()
        .ok_or_else(|| anyhow!("completed developer result lost its head revision"))?;
    let (task_version, review_round, max_rounds, reviewer_profile, existing_reviewer_session): (
        i64,
        i64,
        i64,
        String,
        Option<String>,
    ) = transaction.query_row(
        "SELECT t.version, t.review_round, t.max_review_rounds,
                    plan.reviewer_profile_id, t.reviewer_session_id
             FROM project_tasks t
             JOIN project_plans plan ON plan.id = t.plan_id
             WHERE t.id = ?1 AND t.project_id = ?2 AND t.state = 'developing'",
        params![ready.task_id, ready.project_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    if review_round >= max_rounds {
        bail!("developer completion exceeds the approved review-round bound");
    }
    let (reviewer_session, reviewer_turn_kind, reviewer_sequence) = if let Some(existing) =
        existing_reviewer_session
    {
        let state: String = transaction.query_row(
            "SELECT state FROM worker_sessions
                 WHERE id = ?1 AND task_id = ?2 AND role = 'reviewer'",
            params![existing, ready.task_id],
            |row| row.get(0),
        )?;
        if state != "active" {
            bail!("reviewer resume requires the exact active task session");
        }
        let sequence: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1
                 FROM worker_turns WHERE session_id = ?1",
            [&existing],
            |row| row.get(0),
        )?;
        (existing, TurnKind::Resume, sequence)
    } else {
        let (adapter, native_mode): (String, String) = transaction.query_row(
            "SELECT adapter, native_session_mode FROM worker_profiles WHERE id = ?1",
            [&reviewer_profile],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let reviewer_session = format!("session-{}", Uuid::new_v4());
        let reviewer_native = (native_mode == "preassigned").then(|| Uuid::new_v4().to_string());
        transaction.execute(
            "INSERT INTO worker_sessions (
                     id, project_id, task_id, role, profile_id, adapter,
                     native_session_id, state, created_at, closed_at, updated_at
                 ) VALUES (?1, ?2, ?3, 'reviewer', ?4, ?5, ?6,
                           'creating', ?7, NULL, ?7)",
            params![
                reviewer_session,
                ready.project_id,
                ready.task_id,
                reviewer_profile,
                adapter,
                reviewer_native,
                now,
            ],
        )?;
        (reviewer_session, TurnKind::Create, 1)
    };
    let changed = if reviewer_turn_kind == TurnKind::Create {
        transaction.execute(
            "UPDATE project_tasks
             SET state = 'awaiting_review', reviewer_session_id = ?1,
                 head_revision = ?2, review_round = review_round + 1,
                 result_json = ?3, result_hash = ?4,
                 version = version + 1, updated_at = ?5
             WHERE id = ?6 AND version = ?7 AND state = 'developing'",
            params![
                reviewer_session,
                head,
                result_json,
                result_hash,
                now,
                ready.task_id,
                task_version,
            ],
        )?
    } else {
        transaction.execute(
            "UPDATE project_tasks
             SET state = 'awaiting_review', head_revision = ?1,
                 review_round = review_round + 1,
                 result_json = ?2, result_hash = ?3,
                 version = version + 1, updated_at = ?4
             WHERE id = ?5 AND version = ?6 AND state = 'developing'
               AND reviewer_session_id = ?7",
            params![
                head,
                result_json,
                result_hash,
                now,
                ready.task_id,
                task_version,
                reviewer_session,
            ],
        )?
    };
    if changed != 1 {
        bail!("developer result task apply CAS failed");
    }
    insert_transition(
        transaction,
        &ready.project_id,
        "task",
        &ready.task_id,
        to_u64(task_version)?,
        "developing",
        "awaiting_review",
        "developer_result_apply",
        "developer_result",
        &ready.session_id,
        result_hash,
        Some(&ready.turn_id),
        Some(result_hash),
        now,
    )?;
    insert_turn(
        transaction,
        &ready.project_id,
        &ready.task_id,
        &reviewer_session,
        WorkerRole::Reviewer,
        reviewer_sequence,
        reviewer_turn_kind,
        task_version + 1,
        review_round + 1,
        result_json,
        now,
    )?;
    Ok(())
}

fn apply_request_changes(
    transaction: &Transaction<'_>,
    ready: &ReadyTurn,
    result_hash: &str,
    result_json: &str,
    now: i64,
) -> Result<bool> {
    let (version, review_round, max_rounds, developer_session): (i64, i64, i64, String) =
        transaction.query_row(
            "SELECT version, review_round, max_review_rounds, developer_session_id
             FROM project_tasks
             WHERE id = ?1 AND project_id = ?2 AND state = 'awaiting_review'",
            params![ready.task_id, ready.project_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    if review_round >= max_rounds {
        transition_task_state(
            transaction,
            &ready.project_id,
            &ready.task_id,
            &["awaiting_review"],
            "needs_input",
            "max_review_rounds",
            "reviewer_result",
            &ready.session_id,
            result_hash,
            Some(&ready.turn_id),
            Some(result_hash),
            now,
        )?;
        transition_project_state(
            transaction,
            &ready.project_id,
            "running",
            "needs_input",
            Some("max_review_rounds"),
            None,
            "max_review_rounds",
            "reviewer_result",
            &ready.session_id,
            result_hash,
            now,
        )?;
        return Ok(false);
    }
    let changed = transaction.execute(
        "UPDATE project_tasks
         SET state = 'changes_requested', version = version + 1, updated_at = ?1
         WHERE id = ?2 AND version = ?3 AND state = 'awaiting_review'",
        params![now, ready.task_id, version],
    )?;
    if changed != 1 {
        bail!("request_changes task CAS failed");
    }
    insert_transition(
        transaction,
        &ready.project_id,
        "task",
        &ready.task_id,
        to_u64(version)?,
        "awaiting_review",
        "changes_requested",
        "reviewer_request_changes",
        "reviewer_result",
        &ready.session_id,
        result_hash,
        Some(&ready.turn_id),
        Some(result_hash),
        now,
    )?;
    let developing_version = version + 2;
    let changed = transaction.execute(
        "UPDATE project_tasks
         SET state = 'developing', version = version + 1, updated_at = ?1
         WHERE id = ?2 AND version = ?3 AND state = 'changes_requested'",
        params![now, ready.task_id, version + 1],
    )?;
    if changed != 1 {
        bail!("developer resume task CAS failed");
    }
    insert_transition(
        transaction,
        &ready.project_id,
        "task",
        &ready.task_id,
        to_u64(version + 1)?,
        "changes_requested",
        "developing",
        "developer_resume_enqueue",
        "scheduler",
        "scheduler",
        result_hash,
        Some(&ready.turn_id),
        Some(result_hash),
        now,
    )?;
    let sequence: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence), 0) + 1
         FROM worker_turns WHERE session_id = ?1",
        [&developer_session],
        |row| row.get(0),
    )?;
    insert_turn(
        transaction,
        &ready.project_id,
        &ready.task_id,
        &developer_session,
        WorkerRole::Developer,
        sequence,
        TurnKind::Resume,
        developing_version,
        review_round,
        result_json,
        now,
    )?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn insert_turn(
    transaction: &Transaction<'_>,
    project_id: &str,
    task_id: &str,
    session_id: &str,
    role: WorkerRole,
    sequence: i64,
    kind: TurnKind,
    task_version: i64,
    review_round: i64,
    request_material: &str,
    now: i64,
) -> Result<String> {
    let turn_id = format!("turn-{}", Uuid::new_v4());
    let role_text = role_name(role);
    let artifact_dir = format!("{project_id}/{task_id}/{role_text}/{session_id}/turn-{sequence}");
    let request_hash = sha256_hex(
        format!(
            "hcom-durable-turn-v1\0{project_id}\0{task_id}\0{session_id}\0{sequence}\0{request_material}"
        )
        .as_bytes(),
    );
    transaction.execute(
        "INSERT INTO worker_turns (
             id, session_id, sequence, kind, task_version, review_round,
             request_hash, status, attempt, lease_owner, expires_at,
             worker_pid, process_birth, progress_phase, last_progress_at,
             activity_truncated, artifact_dir, result_json, result_hash,
             error_kind, error_message, created_at, started_at, result_at,
             applied_at, updated_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', 0, NULL, NULL,
             NULL, NULL, 'queued', NULL, 0, ?8, NULL, NULL,
             NULL, NULL, ?9, NULL, NULL, NULL, ?9
         )",
        params![
            turn_id,
            session_id,
            sequence,
            match kind {
                TurnKind::Create => "create",
                TurnKind::Resume => "resume",
            },
            task_version,
            review_round,
            request_hash,
            artifact_dir,
            now,
        ],
    )?;
    Ok(turn_id)
}

#[allow(clippy::too_many_arguments)]
fn transition_task_state(
    transaction: &Transaction<'_>,
    project_id: &str,
    task_id: &str,
    allowed_from: &[&str],
    to_state: &str,
    action: &str,
    actor_kind: &str,
    actor_identity: &str,
    payload_hash: &str,
    turn_id: Option<&str>,
    result_hash: Option<&str>,
    now: i64,
) -> Result<()> {
    let (version, from_state): (i64, String) = transaction.query_row(
        "SELECT version, state FROM project_tasks WHERE id = ?1 AND project_id = ?2",
        params![task_id, project_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if !allowed_from.contains(&from_state.as_str()) {
        bail!("task transition source state is stale");
    }
    let completed_at = (to_state == "completed").then_some(now);
    let changed = transaction.execute(
        "UPDATE project_tasks
         SET state = ?1, version = version + 1, completed_at = ?2, updated_at = ?3
         WHERE id = ?4 AND project_id = ?5 AND version = ?6 AND state = ?7",
        params![
            to_state,
            completed_at,
            now,
            task_id,
            project_id,
            version,
            from_state,
        ],
    )?;
    if changed != 1 {
        bail!("task transition CAS failed");
    }
    insert_transition(
        transaction,
        project_id,
        "task",
        task_id,
        to_u64(version)?,
        &from_state,
        to_state,
        action,
        actor_kind,
        actor_identity,
        payload_hash,
        turn_id,
        result_hash,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn transition_project_state(
    transaction: &Transaction<'_>,
    project_id: &str,
    from_state: &str,
    to_state: &str,
    pause_reason: Option<&str>,
    active_epoch: Option<&str>,
    action: &str,
    actor_kind: &str,
    actor_identity: &str,
    payload_hash: &str,
    now: i64,
) -> Result<()> {
    let version: i64 = transaction.query_row(
        "SELECT version FROM project_runs WHERE id = ?1 AND state = ?2",
        params![project_id, from_state],
        |row| row.get(0),
    )?;
    transition_project_state_at_version(
        transaction,
        project_id,
        to_u64(version)?,
        from_state,
        to_state,
        pause_reason,
        active_epoch,
        action,
        actor_kind,
        actor_identity,
        payload_hash,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn transition_project_state_at_version(
    transaction: &Transaction<'_>,
    project_id: &str,
    version: u64,
    from_state: &str,
    to_state: &str,
    pause_reason: Option<&str>,
    active_epoch: Option<&str>,
    action: &str,
    actor_kind: &str,
    actor_identity: &str,
    payload_hash: &str,
    now: i64,
) -> Result<()> {
    let changed = transaction.execute(
        "UPDATE project_runs
         SET state = ?1, version = version + 1, pause_reason = ?2,
             active_daemon_epoch = COALESCE(?3, active_daemon_epoch),
             updated_at = ?4
         WHERE id = ?5 AND version = ?6 AND state = ?7",
        params![
            to_state,
            pause_reason,
            active_epoch,
            now,
            project_id,
            to_i64(version)?,
            from_state,
        ],
    )?;
    if changed != 1 {
        bail!("project transition CAS failed");
    }
    insert_transition(
        transaction,
        project_id,
        "project",
        project_id,
        version,
        from_state,
        to_state,
        action,
        actor_kind,
        actor_identity,
        payload_hash,
        None,
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_transition(
    transaction: &Transaction<'_>,
    project_id: &str,
    scope_kind: &str,
    scope_id: &str,
    from_version: u64,
    from_state: &str,
    to_state: &str,
    action: &str,
    actor_kind: &str,
    actor_identity: &str,
    payload_hash: &str,
    turn_id: Option<&str>,
    result_hash: Option<&str>,
    now: i64,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO state_transitions (
             project_id, scope_kind, scope_id, from_version, to_version,
             from_state, to_state, action, actor_kind, actor_identity,
             payload_hash, turn_id, result_hash, created_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?4 + 1, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13
         )",
        params![
            project_id,
            scope_kind,
            scope_id,
            to_i64(from_version)?,
            from_state,
            to_state,
            action,
            actor_kind,
            actor_identity,
            payload_hash,
            turn_id,
            result_hash,
            now,
        ],
    )?;
    Ok(())
}

fn role_name(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => "developer",
        WorkerRole::Reviewer => "reviewer",
    }
}

fn parse_role(value: &str) -> rusqlite::Result<WorkerRole> {
    match value {
        "developer" => Ok(WorkerRole::Developer),
        "reviewer" => Ok(WorkerRole::Reviewer),
        _ => Err(conversion_error("bad worker role")),
    }
}

fn conversion_error(message: &'static str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("durable version does not fit SQLite integer")
}

fn to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).context("negative durable counter")
}

fn to_u32_sql(value: i64, column: usize) -> rusqlite::Result<u32> {
    u32::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

fn to_u64_sql(value: i64, column: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

#[cfg(test)]
pub(crate) struct FakeProjectSeed {
    pub(crate) project_id: String,
    pub(crate) source_repo_root: String,
    pub(crate) developer_worktree: String,
    pub(crate) base_revision: String,
    pub(crate) developer_profile: WorkerProfile,
    pub(crate) reviewer_profile: WorkerProfile,
    pub(crate) approved_tasks: Vec<FakeTaskSeed>,
    pub(crate) unapproved_task: Option<FakeTaskSeed>,
}

#[cfg(test)]
pub(crate) struct FakeTaskSeed {
    pub(crate) id: String,
    pub(crate) task_key: String,
    pub(crate) ordinal: u32,
    pub(crate) spec_json: String,
    pub(crate) max_review_rounds: u8,
    pub(crate) dependencies: Vec<String>,
}

#[cfg(test)]
fn insert_profile(
    transaction: &Transaction<'_>,
    project_id: &str,
    id: &str,
    profile: &WorkerProfile,
    now: i64,
) -> Result<()> {
    let capability_json = serde_json::to_string(&profile.capability)?;
    let executable_json = serde_json::to_string(&profile.executable)?;
    transaction.execute(
        "INSERT INTO worker_profiles (
             id, project_id, role, adapter, model, reasoning, policy,
             cli_path, executable_identity_json, cli_version,
             adapter_contract_ver, native_session_mode, capability_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            id,
            project_id,
            role_name(profile.role),
            profile.adapter,
            profile.model,
            profile.reasoning,
            profile.policy,
            profile.executable.canonical_path.to_string_lossy(),
            executable_json,
            profile.cli_version,
            profile.adapter_contract_version,
            match profile.native_session_mode {
                NativeSessionMode::Preassigned => "preassigned",
                NativeSessionMode::Discovered => "discovered",
            },
            capability_json,
            now,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
fn insert_fake_task(
    transaction: &Transaction<'_>,
    project_id: &str,
    plan_id: &str,
    task: &FakeTaskSeed,
    now: i64,
) -> Result<()> {
    let spec_hash = sha256_hex(task.spec_json.as_bytes());
    transaction.execute(
        "INSERT INTO project_tasks (
             id, project_id, plan_id, task_key, ordinal, spec_json, spec_hash,
             state, version, base_revision, head_revision, review_round,
             max_review_rounds, developer_session_id, reviewer_session_id,
             result_json, result_hash, created_at, updated_at, completed_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'draft', 0, NULL, NULL, 0,
             ?8, NULL, NULL, NULL, NULL, ?9, ?9, NULL
         )",
        params![
            task.id,
            project_id,
            plan_id,
            task.task_key,
            task.ordinal,
            task.spec_json,
            spec_hash,
            task.max_review_rounds,
            now,
        ],
    )?;
    Ok(())
}
