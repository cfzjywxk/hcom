//! Durable persistence primitives for background review workers.
//!
//! Phase 1 deliberately stops at the SQLite boundary. This module does not
//! spawn processes, construct adapter commands, parse native output, or branch
//! the existing interactive review state machine.

use std::fmt;
use std::str::FromStr;

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use crate::db::HcomDb;
use crate::shared::time::now_epoch_f64;

#[derive(Debug, thiserror::Error)]
pub enum ReviewWorkerStateError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
}

pub type ReviewWorkerStateResult<T> = Result<T, ReviewWorkerStateError>;

fn invalid(message: impl Into<String>) -> ReviewWorkerStateError {
    ReviewWorkerStateError::Invalid(message.into())
}

fn conflict(message: impl Into<String>) -> ReviewWorkerStateError {
    ReviewWorkerStateError::Conflict(message.into())
}

fn row_enum_error(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewRouteBinding {
    Pending,
    Bound,
}

impl ReviewRouteBinding {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Bound => "bound",
        }
    }
}

impl fmt::Display for ReviewRouteBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReviewRouteBinding {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "bound" => Ok(Self::Bound),
            _ => Err(format!("unknown review route binding '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSessionMode {
    Preassigned,
    Discovered,
}

impl NativeSessionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preassigned => "preassigned",
            Self::Discovered => "discovered",
        }
    }
}

impl fmt::Display for NativeSessionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NativeSessionMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "preassigned" => Ok(Self::Preassigned),
            "discovered" => Ok(Self::Discovered),
            _ => Err(format!("unknown native session mode '{value}'")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewRoute {
    pub id: String,
    pub developer_name: String,
    pub developer_session_id: Option<String>,
    pub launch_generation: String,
    pub binding_state: ReviewRouteBinding,
    pub reviewer_alias: String,
    pub adapter: String,
    pub native_session_mode: NativeSessionMode,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub workspace_root: String,
    pub base_checkpoint: String,
    pub cli_path: String,
    pub cli_version: String,
    pub adapter_contract_ver: i64,
    pub capability_json: String,
    pub created_at: f64,
    pub updated_at: f64,
}

impl ReviewRoute {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let binding: String = row.get("binding_state")?;
        let native_session_mode: String = row.get("native_session_mode")?;
        Ok(Self {
            id: row.get("id")?,
            developer_name: row.get("developer_name")?,
            developer_session_id: row.get("developer_session_id")?,
            launch_generation: row.get("launch_generation")?,
            binding_state: binding
                .parse()
                .map_err(|message| row_enum_error(4, message))?,
            reviewer_alias: row.get("reviewer_alias")?,
            adapter: row.get("adapter")?,
            native_session_mode: native_session_mode
                .parse()
                .map_err(|message| row_enum_error(7, message))?,
            model: row.get("model")?,
            reasoning: row.get("reasoning")?,
            policy: row.get("policy")?,
            workspace_root: row.get("workspace_root")?,
            base_checkpoint: row.get("base_checkpoint")?,
            cli_path: row.get("cli_path")?,
            cli_version: row.get("cli_version")?,
            adapter_contract_ver: row.get("adapter_contract_ver")?,
            capability_json: row.get("capability_json")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NewReviewRoute {
    pub id: String,
    pub developer_name: String,
    pub launch_generation: String,
    pub reviewer_alias: String,
    pub adapter: String,
    pub native_session_mode: NativeSessionMode,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub workspace_root: String,
    pub base_checkpoint: String,
    pub cli_path: String,
    pub cli_version: String,
    pub adapter_contract_ver: i64,
    pub capability_json: String,
}

const REVIEW_ROUTE_COLUMNS: &str = "
    id, developer_name, developer_session_id, launch_generation,
    binding_state, reviewer_alias, adapter, native_session_mode,
    model, reasoning, policy,
    workspace_root, base_checkpoint, cli_path, cli_version,
    adapter_contract_ver, capability_json, created_at, updated_at
";

fn load_review_route_on(
    conn: &rusqlite::Connection,
    predicate: &str,
    value: &str,
) -> ReviewWorkerStateResult<Option<ReviewRoute>> {
    let sql = format!("SELECT {REVIEW_ROUTE_COLUMNS} FROM review_routes WHERE {predicate} = ?1");
    conn.query_row(&sql, params![value], ReviewRoute::from_row)
        .optional()
        .map_err(Into::into)
}

/// Insert a pending route inside the caller's launch transaction.
///
/// The pre-registered instance must already be visible in this transaction
/// and must not have a native session yet. Task 6 will call this from the same
/// operation that creates the launch placeholder.
pub fn insert_pending_review_route_on(
    tx: &Transaction<'_>,
    route: &NewReviewRoute,
) -> ReviewWorkerStateResult<ReviewRoute> {
    let pre_registered_session = tx
        .query_row(
            "SELECT session_id FROM instances WHERE name = ?1",
            params![route.developer_name],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    match pre_registered_session {
        None => {
            return Err(ReviewWorkerStateError::NotFound(format!(
                "pre-registered developer '{}' does not exist",
                route.developer_name
            )));
        }
        Some(Some(_)) => {
            return Err(conflict(format!(
                "developer '{}' already has a bound session",
                route.developer_name
            )));
        }
        Some(None) => {}
    }

    let now = now_epoch_f64();
    tx.execute(
        "INSERT INTO review_routes (
             id, developer_name, developer_session_id, launch_generation,
             binding_state, reviewer_alias, adapter, native_session_mode,
             model, reasoning, policy, workspace_root, base_checkpoint,
             cli_path, cli_version, adapter_contract_ver, capability_json,
             created_at, updated_at
         ) VALUES (
             ?1, ?2, NULL, ?3, 'pending', ?4, ?5, ?6, ?7, ?8,
             ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16
         )",
        params![
            route.id,
            route.developer_name,
            route.launch_generation,
            route.reviewer_alias,
            route.adapter,
            route.native_session_mode.as_str(),
            route.model,
            route.reasoning,
            route.policy,
            route.workspace_root,
            route.base_checkpoint,
            route.cli_path,
            route.cli_version,
            route.adapter_contract_ver,
            route.capability_json,
            now,
        ],
    )?;
    load_review_route_on(tx, "id", &route.id)?.ok_or_else(|| {
        ReviewWorkerStateError::NotFound(format!(
            "newly inserted review route '{}' disappeared",
            route.id
        ))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteBindOutcome {
    Bound,
    AlreadyBound,
}

impl HcomDb {
    pub fn load_review_route_by_id(
        &self,
        route_id: &str,
    ) -> ReviewWorkerStateResult<Option<ReviewRoute>> {
        load_review_route_on(self.conn(), "id", route_id)
    }

    pub fn load_review_route_for_session(
        &self,
        developer_session_id: &str,
    ) -> ReviewWorkerStateResult<Option<ReviewRoute>> {
        load_review_route_on(self.conn(), "developer_session_id", developer_session_id)
    }

    pub fn load_pending_review_route_for_launch(
        &self,
        launch_generation: &str,
    ) -> ReviewWorkerStateResult<Option<ReviewRoute>> {
        let route = load_review_route_on(self.conn(), "launch_generation", launch_generation)?;
        Ok(route.filter(|route| route.binding_state == ReviewRouteBinding::Pending))
    }
}

/// Bind a pending route inside the trusted instance/session transaction.
///
/// The exact `instances` and `session_bindings` identity must already be
/// visible in this transaction. This prevents the route from committing
/// before ordinary hcom identity, while preserving exact idempotent replay for
/// that same session.
pub fn bind_pending_review_route_on(
    tx: &Transaction<'_>,
    route_id: &str,
    developer_name: &str,
    launch_generation: &str,
    developer_session_id: &str,
) -> ReviewWorkerStateResult<(RouteBindOutcome, ReviewRoute)> {
    if developer_session_id.is_empty() {
        return Err(invalid("developer session id must not be empty"));
    }
    let identity_is_bound: bool = tx.query_row(
        "SELECT EXISTS (
             SELECT 1
             FROM instances instance
             JOIN session_bindings binding
               ON binding.instance_name = instance.name
              AND binding.session_id = instance.session_id
             WHERE instance.name = ?1
               AND instance.session_id = ?2
         )",
        params![developer_name, developer_session_id],
        |row| row.get(0),
    )?;
    if !identity_is_bound {
        return Err(conflict(format!(
            "developer '{developer_name}' has no exact durable session binding"
        )));
    }

    let now = now_epoch_f64();
    let changed = tx.execute(
        "UPDATE review_routes
         SET developer_session_id = ?1,
             binding_state = 'bound',
             updated_at = ?2
         WHERE id = ?3
           AND developer_name = ?4
           AND launch_generation = ?5
           AND binding_state = 'pending'
           AND developer_session_id IS NULL",
        params![
            developer_session_id,
            now,
            route_id,
            developer_name,
            launch_generation,
        ],
    )?;
    let route = load_review_route_on(tx, "id", route_id)?.ok_or_else(|| {
        ReviewWorkerStateError::NotFound(format!("review route '{route_id}' was not found"))
    })?;
    let outcome = if changed == 1 {
        RouteBindOutcome::Bound
    } else if route.developer_name == developer_name
        && route.launch_generation == launch_generation
        && route.binding_state == ReviewRouteBinding::Bound
        && route.developer_session_id.as_deref() == Some(developer_session_id)
    {
        RouteBindOutcome::AlreadyBound
    } else {
        return Err(conflict(format!(
            "review route '{route_id}' is not the expected pending launch generation"
        )));
    };
    Ok((outcome, route))
}

/// Remove an unbound route inside the caller's failed-launch cleanup
/// transaction. Bound routes and mismatched launch generations are untouched.
pub fn remove_pending_review_route_on(
    tx: &Transaction<'_>,
    route_id: &str,
    launch_generation: &str,
) -> ReviewWorkerStateResult<bool> {
    let changed = tx.execute(
        "DELETE FROM review_routes
         WHERE id = ?1
           AND launch_generation = ?2
           AND binding_state = 'pending'
           AND developer_session_id IS NULL",
        params![route_id, launch_generation],
    )?;
    Ok(changed == 1)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    struct TestDb {
        db: HcomDb,
        _dir: tempfile::TempDir,
        path: std::path::PathBuf,
    }

    impl TestDb {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("hcom.db");
            let db = HcomDb::open_at(&path).unwrap();
            Self {
                db,
                _dir: dir,
                path,
            }
        }
    }

    fn revision(byte: char) -> String {
        std::iter::repeat_n(byte, 40).collect()
    }

    fn route_input(id: &str, developer: &str, generation: &str) -> NewReviewRoute {
        NewReviewRoute {
            id: id.to_string(),
            developer_name: developer.to_string(),
            launch_generation: generation.to_string(),
            reviewer_alias: "dev2".to_string(),
            adapter: "codex".to_string(),
            native_session_mode: NativeSessionMode::Discovered,
            model: "gpt-5.6-sol".to_string(),
            reasoning: "max".to_string(),
            policy: "read-only".to_string(),
            workspace_root: "/tmp/review-worker-test".to_string(),
            base_checkpoint: revision('a'),
            cli_path: "/usr/bin/codex".to_string(),
            cli_version: "codex-cli 0.145.0".to_string(),
            adapter_contract_ver: 1,
            capability_json: r#"{"observability":"structured"}"#.to_string(),
        }
    }

    fn insert_pending_route(
        db: &HcomDb,
        route_id: &str,
        developer: &str,
        generation: &str,
    ) -> ReviewRoute {
        insert_pending_route_with_mode(
            db,
            route_id,
            developer,
            generation,
            NativeSessionMode::Discovered,
        )
    }

    fn insert_pending_route_with_mode(
        db: &HcomDb,
        route_id: &str,
        developer: &str,
        generation: &str,
        native_session_mode: NativeSessionMode,
    ) -> ReviewRoute {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
        tx.execute(
            "INSERT INTO instances (
                 name, session_id, tool, status, status_context, created_at
             ) VALUES (?1, NULL, 'codex', 'pending', 'new', ?2)",
            params![developer, now_epoch_f64()],
        )
        .unwrap();
        let mut input = route_input(route_id, developer, generation);
        input.native_session_mode = native_session_mode;
        let route = insert_pending_review_route_on(&tx, &input).unwrap();
        tx.commit().unwrap();
        route
    }

    fn bind_route(
        db: &HcomDb,
        route_id: &str,
        developer: &str,
        generation: &str,
        session: &str,
    ) -> (RouteBindOutcome, ReviewRoute) {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
        tx.execute(
            "UPDATE instances SET session_id = ?1 WHERE name = ?2",
            params![session, developer],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO session_bindings (
                 session_id, instance_name, created_at
             ) VALUES (?1, ?2, ?3)",
            params![session, developer, now_epoch_f64()],
        )
        .unwrap();
        let bound =
            bind_pending_review_route_on(&tx, route_id, developer, generation, session).unwrap();
        tx.commit().unwrap();
        bound
    }

    fn apply_job_result(
        db: &HcomDb,
        job_id: &str,
        expected_attempt: i64,
        expected_result_hash: &str,
        kind: &ReviewApplyKind,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
        let applied =
            apply_review_job_result_on(&tx, job_id, expected_attempt, expected_result_hash, kind)?;
        tx.commit()?;
        Ok(applied)
    }

    fn cancel_job(
        db: &HcomDb,
        job_id: &str,
        expected_attempt: i64,
        reason: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
        let canceled = cancel_review_job_on(&tx, job_id, expected_attempt, reason)?;
        tx.commit()?;
        Ok(canceled)
    }

    fn retry_job(
        db: &HcomDb,
        job_id: &str,
        expected_attempt: i64,
        expected_head_revision: &str,
        allow_indeterminate: bool,
        new_artifact_dir: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
        let retry = retry_review_job_on(
            &tx,
            job_id,
            expected_attempt,
            expected_head_revision,
            allow_indeterminate,
            new_artifact_dir,
        )?;
        tx.commit()?;
        Ok(retry)
    }

    fn assert_cancelable_without_commit(
        db: &HcomDb,
        job_id: &str,
        expected_attempt: i64,
        expected_status: ReviewJobStatus,
    ) {
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
        let canceled =
            cancel_review_job_on(&tx, job_id, expected_attempt, "rollback probe").unwrap();
        assert_eq!(canceled.status, ReviewJobStatus::Canceled);
        drop(tx);
        assert_eq!(
            db.load_review_job(job_id).unwrap().unwrap().status,
            expected_status,
            "dropping the caller transaction must roll back the cancel primitive"
        );
    }

    fn insert_worker_workflow_and_job(
        db: &HcomDb,
        route: &ReviewRoute,
        workflow_id: &str,
        job_id: &str,
        head_revision: &str,
        native_session_id: Option<&str>,
    ) -> (ReviewWorker, ReviewJob) {
        let base = db
            .load_review_route_by_id(&route.id)
            .unwrap()
            .unwrap()
            .base_checkpoint;
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
        tx.execute(
            "INSERT INTO review_runs (
                 id, task, workspace, thread, developer_name,
                 developer_session_id, reviewer_name, reviewer_session_id,
                 state, round, max_rounds, version, last_message_event_id,
                 created_at, updated_at, reviewer_mode, base_revision,
                 review_route_id
             ) VALUES (
                 ?1, 'review task', ?2, ?3, ?4, ?5, ?6, ?7,
                 'awaiting_review', 1, 5, 0, NULL, ?8, ?8,
                 'worker', ?9, ?10
             )",
            params![
                workflow_id,
                route.workspace_root,
                format!("review-{workflow_id}"),
                route.developer_name,
                route.developer_session_id.as_deref().unwrap(),
                route.reviewer_alias,
                format!("worker:{workflow_id}"),
                now_epoch_f64(),
                base,
                route.id,
            ],
        )
        .unwrap();
        let worker = insert_review_worker_on(&tx, workflow_id, native_session_id).unwrap();
        let job = insert_review_job_on(
            &tx,
            &NewReviewJob {
                id: job_id.to_string(),
                workflow_id: workflow_id.to_string(),
                round: 1,
                request_version: 0,
                base_revision: base,
                head_revision: head_revision.to_string(),
                developer_submission: "committed change".to_string(),
                artifact_dir: format!("review-workers/{workflow_id}/{job_id}/attempt-0"),
            },
        )
        .unwrap();
        tx.commit().unwrap();
        (worker, job)
    }

    fn setup_bound_route(test: &TestDb) -> ReviewRoute {
        setup_bound_route_with_mode(test, NativeSessionMode::Discovered)
    }

    fn setup_bound_route_with_mode(
        test: &TestDb,
        native_session_mode: NativeSessionMode,
    ) -> ReviewRoute {
        insert_pending_route_with_mode(
            &test.db,
            "route-1",
            "developer",
            "launch-1",
            native_session_mode,
        );
        bind_route(
            &test.db,
            "route-1",
            "developer",
            "launch-1",
            "developer-session",
        )
        .1
    }

    #[test]
    fn route_creation_binding_resume_and_cleanup_are_generation_scoped() {
        let test = TestDb::new();

        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        tx.execute(
            "INSERT INTO instances (name, tool, created_at)
             VALUES ('rolled-back', 'codex', 1.0)",
            [],
        )
        .unwrap();
        let mut invalid = route_input("route-invalid", "rolled-back", "launch-invalid");
        invalid.capability_json = "not-json".to_string();
        assert!(insert_pending_review_route_on(&tx, &invalid).is_err());
        drop(tx);
        assert_eq!(
            test.db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = 'rolled-back'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0,
            "instance and route creation must roll back as one launch operation"
        );

        let pending = insert_pending_route(&test.db, "route-1", "developer", "launch-1");
        assert_eq!(pending.binding_state, ReviewRouteBinding::Pending);
        assert!({
            let tx =
                Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
            tx.execute(
                "UPDATE instances SET session_id = 'session-1'
                     WHERE name = 'developer'",
                [],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO session_bindings (
                         session_id, instance_name, created_at
                     ) VALUES ('session-1', 'developer', 1.0)",
                [],
            )
            .unwrap();
            bind_pending_review_route_on(&tx, "route-1", "developer", "wrong-launch", "session-1")
                .is_err()
        });
        assert_eq!(
            test.db
                .load_pending_review_route_for_launch("launch-1")
                .unwrap()
                .unwrap()
                .id,
            "route-1"
        );
        assert!(
            test.db
                .conn()
                .query_row(
                    "SELECT session_id FROM instances WHERE name = 'developer'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .unwrap()
                .is_none(),
            "a failed route bind must roll back the ordinary instance binding too"
        );

        let (outcome, bound) =
            bind_route(&test.db, "route-1", "developer", "launch-1", "session-1");
        assert_eq!(outcome, RouteBindOutcome::Bound);
        assert_eq!(bound.developer_session_id.as_deref(), Some("session-1"));
        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        let (outcome, _) =
            bind_pending_review_route_on(&tx, "route-1", "developer", "launch-1", "session-1")
                .unwrap();
        tx.commit().unwrap();
        assert_eq!(outcome, RouteBindOutcome::AlreadyBound);
        assert!(
            {
                let tx = Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate)
                    .unwrap();
                bind_pending_review_route_on(
                    &tx,
                    "route-1",
                    "developer",
                    "launch-1",
                    "fork-session",
                )
                .is_err()
            },
            "a fork or new session must not take over a bound route"
        );
        assert!(
            test.db
                .load_review_route_for_session("session-1")
                .unwrap()
                .is_some()
        );
        assert!(
            test.db
                .load_review_route_for_session("fork-session")
                .unwrap()
                .is_none()
        );
        assert!(
            test.db
                .conn()
                .execute(
                    "UPDATE review_routes SET model = 'changed' WHERE id = 'route-1'",
                    []
                )
                .is_err(),
            "route profile must be immutable even to direct SQL"
        );
        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        assert!(
            !remove_pending_review_route_on(&tx, "route-1", "launch-1").unwrap(),
            "bound routes cannot be removed by launch cleanup"
        );
        tx.commit().unwrap();

        test.db
            .conn()
            .execute("DELETE FROM instances WHERE name = 'developer'", [])
            .unwrap();
        let second = insert_pending_route(&test.db, "route-2", "developer", "launch-2");
        assert_eq!(second.developer_name, "developer");
        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        assert!(remove_pending_review_route_on(&tx, "route-2", "launch-2").unwrap());
        tx.execute("DELETE FROM instances WHERE name = 'developer'", [])
            .unwrap();
        tx.commit().unwrap();
        assert!(
            test.db
                .load_review_route_by_id("route-2")
                .unwrap()
                .is_none(),
            "failed launch cleanup must not leave an orphan pending route"
        );
        assert_eq!(
            test.db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM instances WHERE name = 'developer'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "failed launch cleanup must remove the placeholder in the same transaction"
        );
    }

    #[test]
    fn worker_snapshot_job_identity_and_cross_mode_conflict_are_persistent() {
        let test = TestDb::new();
        let route = setup_bound_route_with_mode(&test, NativeSessionMode::Preassigned);
        let (worker, job) = insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-worker",
            "job-1",
            &revision('b'),
            Some("native-preassigned"),
        );
        assert_eq!(worker.adapter, route.adapter);
        assert_eq!(worker.native_session_mode, NativeSessionMode::Preassigned);
        assert_eq!(worker.cli_path, route.cli_path);
        assert_eq!(
            worker.native_session_id.as_deref(),
            Some("native-preassigned")
        );
        assert_eq!(job.status, ReviewJobStatus::Queued);
        assert_eq!(job.progress_phase, ReviewProgressPhase::Queued);
        assert!(!job.activity_truncated);
        assert!(job.last_progress_at.is_none());
        test.db
            .claim_review_job(
                "job-1",
                0,
                "lease-preassigned",
                now_epoch_f64() + 3600.0,
                900,
                "birth-preassigned",
            )
            .unwrap()
            .unwrap();
        assert!(
            test.db
                .bind_discovered_native_session(
                    "job-1",
                    0,
                    "lease-preassigned",
                    "native-preassigned",
                )
                .is_err(),
            "a preassigned worker must not enter the discovered-session bind path"
        );

        assert!(
            test.db
                .conn()
                .execute(
                    "UPDATE review_workers SET cli_version = 'changed'
                     WHERE workflow_id = 'rv-worker'",
                    []
                )
                .is_err(),
            "workflow profile snapshots must be immutable"
        );
        assert!(
            test.db
                .conn()
                .execute(
                    "INSERT INTO review_jobs (
                         id, workflow_id, round, request_version,
                         base_revision, head_revision, developer_submission,
                         status, attempt, progress_phase, activity_truncated,
                         artifact_dir, created_at, updated_at
                     ) SELECT
                         'job-duplicate', workflow_id, round, request_version,
                         base_revision, head_revision, developer_submission,
                         'queued', 0, 'queued', 0,
                         'review-workers/duplicate/attempt-0', 2.0, 2.0
                       FROM review_jobs WHERE id = 'job-1'",
                    []
                )
                .is_err(),
            "(workflow, round, request version) must be unique"
        );
        assert_eq!(
            test.db
                .find_non_final_review_for_developer("developer-session")
                .unwrap()
                .unwrap(),
            ActiveReviewConflict {
                workflow_id: "rv-worker".to_string(),
                reviewer_mode: "worker".to_string(),
            }
        );
        assert!(
            test.db
                .conn()
                .execute(
                    "INSERT INTO review_runs (
                         id, task, workspace, thread, developer_name,
                         developer_session_id, reviewer_name,
                         reviewer_session_id, state, round, max_rounds,
                         version, created_at, updated_at, reviewer_mode,
                         base_revision, review_route_id
                     ) VALUES (
                         'rv-worker-2', 'task', '/tmp', 'review-rv-worker-2',
                         'developer', 'developer-session', 'dev2',
                         'worker:rv-worker-2', 'awaiting_review', 1, 3,
                         0, 1.0, 1.0, 'worker', ?1, 'route-1'
                     )",
                    params![revision('a')]
                )
                .is_err(),
            "partial unique index must reject a second non-final worker"
        );

        test.db
            .conn()
            .execute(
                "INSERT INTO review_runs (
                     id, task, workspace, thread, developer_name,
                     developer_session_id, reviewer_name, reviewer_session_id,
                     state, round, max_rounds, version, created_at, updated_at
                 ) VALUES (
                     'rv-interactive', 'task', '/tmp', 'review-rv-interactive',
                     'other-dev', 'other-session', 'reviewer', 'review-session',
                     'awaiting_review', 1, 3, 0, 1.0, 1.0
                 )",
                [],
            )
            .unwrap();
        assert_eq!(
            test.db
                .find_non_final_review_for_developer("other-session")
                .unwrap()
                .unwrap()
                .reviewer_mode,
            "interactive",
            "cross-mode exclusion primitive must see existing interactive work"
        );

        for table in ["review_routes", "review_workers", "review_jobs"] {
            let columns: Vec<String> = test
                .db
                .conn()
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(
                columns.iter().all(|column| {
                    !["env", "auth", "token", "cookie", "argv"]
                        .iter()
                        .any(|secret| column.contains(secret))
                }),
                "{table} must not persist environment or authentication fields"
            );
        }
    }

    #[test]
    fn concurrent_claim_has_one_winner_and_heartbeat_is_not_progress() {
        let test = TestDb::new();
        let route = setup_bound_route(&test);
        insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-claim",
            "job-claim",
            &revision('b'),
            None,
        );

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|index| {
                let barrier = Arc::clone(&barrier);
                let path = test.path.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&path).unwrap();
                    barrier.wait();
                    db.claim_review_job(
                        "job-claim",
                        0,
                        &format!("lease-{index}"),
                        now_epoch_f64() + 3600.0,
                        1000 + index,
                        &format!("birth-{index}"),
                    )
                    .unwrap()
                    .map(|job| job.lease_owner.unwrap())
                })
            })
            .collect();
        let winners: Vec<String> = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(winners.len(), 1);
        let owner = &winners[0];

        let before = test.db.load_review_job("job-claim").unwrap().unwrap();
        assert_eq!(before.status, ReviewJobStatus::Running);
        assert!(before.last_progress_at.is_none());
        assert!(
            test.db
                .heartbeat_review_job("job-claim", 0, owner, now_epoch_f64() + 7200.0)
                .unwrap()
        );
        let after_heartbeat = test.db.load_review_job("job-claim").unwrap().unwrap();
        assert!(after_heartbeat.last_progress_at.is_none());
        assert_eq!(after_heartbeat.progress_phase, ReviewProgressPhase::Spawn);

        let progress_at = now_epoch_f64();
        let progressed = test
            .db
            .update_review_job_progress(
                "job-claim",
                0,
                owner,
                ReviewProgressPhase::Running,
                progress_at,
                true,
            )
            .unwrap();
        assert_eq!(progressed.last_progress_at, Some(progress_at));
        assert!(progressed.activity_truncated);
        assert!(
            !test
                .db
                .heartbeat_review_job("job-claim", 0, "losing-owner", now_epoch_f64() + 7200.0)
                .unwrap()
        );
        assert!(
            test.db
                .update_review_job_progress(
                    "job-claim",
                    0,
                    owner,
                    ReviewProgressPhase::Spawn,
                    progress_at,
                    false,
                )
                .is_err(),
            "progress phase must be monotonic and independent of heartbeat"
        );
    }

    #[test]
    fn native_session_result_and_lgtm_checkpoint_use_distinct_cas_steps() {
        let test = TestDb::new();
        let route = setup_bound_route(&test);
        insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-result",
            "job-result",
            &revision('b'),
            None,
        );
        let claimed = test
            .db
            .claim_review_job(
                "job-result",
                0,
                "lease-result",
                now_epoch_f64() + 3600.0,
                1234,
                "birth-result",
            )
            .unwrap()
            .unwrap();
        assert_eq!(claimed.status, ReviewJobStatus::Running);
        assert!(
            test.db
                .publish_review_job_result(
                    "job-result",
                    0,
                    "lease-result",
                    r#"{"decision":"lgtm"}"#,
                    &"1".repeat(64),
                )
                .is_err(),
            "a discovered session must be bound before result publication"
        );
        let (outcome, worker) = test
            .db
            .bind_discovered_native_session("job-result", 0, "lease-result", "native-result")
            .unwrap();
        assert_eq!(outcome, NativeSessionBindOutcome::Bound);
        assert_eq!(worker.native_session_id.as_deref(), Some("native-result"));
        assert_eq!(
            test.db
                .bind_discovered_native_session("job-result", 0, "lease-result", "native-result",)
                .unwrap()
                .0,
            NativeSessionBindOutcome::AlreadyBound
        );
        assert!(
            test.db
                .bind_discovered_native_session(
                    "job-result",
                    0,
                    "lease-result",
                    "different-native",
                )
                .is_err()
        );

        let hash = "2".repeat(64);
        let ready = test
            .db
            .publish_review_job_result(
                "job-result",
                0,
                "lease-result",
                r#"{"decision":"request_changes"}"#,
                &hash,
            )
            .unwrap();
        assert_eq!(ready.status, ReviewJobStatus::ResultReady);
        assert!(
            test.db
                .publish_review_job_result(
                    "job-result",
                    0,
                    "lease-result",
                    r#"{"decision":"request_changes"}"#,
                    &hash,
                )
                .is_err(),
            "a result may be published only once"
        );
        assert_cancelable_without_commit(&test.db, "job-result", 0, ReviewJobStatus::ResultReady);
        assert!(
            retry_job(
                &test.db,
                "job-result",
                0,
                &revision('b'),
                false,
                "review-workers/rv-result/job-result/retry-result-ready",
            )
            .is_err(),
            "result-ready jobs cannot retry"
        );
        assert_eq!(
            test.db
                .load_review_route_by_id("route-1")
                .unwrap()
                .unwrap()
                .base_checkpoint,
            revision('a'),
            "result persistence must not advance the route checkpoint"
        );
        let applied =
            apply_job_result(&test.db, "job-result", 0, &hash, &ReviewApplyKind::NonLgtm).unwrap();
        assert_eq!(applied.status, ReviewJobStatus::Applied);
        assert!(
            apply_job_result(&test.db, "job-result", 0, &hash, &ReviewApplyKind::NonLgtm,).is_err(),
            "an applied result cannot be replayed"
        );
        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        assert!(cancel_review_job_on(&tx, "job-result", 0, "late cancel").is_err());
        drop(tx);
        assert_eq!(
            test.db
                .load_review_route_by_id("route-1")
                .unwrap()
                .unwrap()
                .base_checkpoint,
            revision('a'),
            "non-LGTM apply must not advance the base"
        );
        assert!(
            retry_job(
                &test.db,
                "job-result",
                0,
                &revision('b'),
                false,
                "review-workers/rv-result/job-result/attempt-1",
            )
            .is_err(),
            "applied jobs cannot retry"
        );

        test.db
            .conn()
            .execute(
                "UPDATE review_runs SET state = 'approved'
                 WHERE id = 'rv-result'",
                [],
            )
            .unwrap();
        insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-lgtm",
            "job-lgtm",
            &revision('c'),
            None,
        );
        test.db
            .claim_review_job(
                "job-lgtm",
                0,
                "lease-lgtm",
                now_epoch_f64() + 3600.0,
                2234,
                "birth-lgtm",
            )
            .unwrap()
            .unwrap();
        test.db
            .bind_discovered_native_session("job-lgtm", 0, "lease-lgtm", "native-lgtm")
            .unwrap();
        let lgtm_hash = "3".repeat(64);
        test.db
            .publish_review_job_result(
                "job-lgtm",
                0,
                "lease-lgtm",
                r#"{"decision":"lgtm"}"#,
                &lgtm_hash,
            )
            .unwrap();
        assert!(
            apply_job_result(
                &test.db,
                "job-lgtm",
                0,
                &lgtm_hash,
                &ReviewApplyKind::Lgtm {
                    expected_base: revision('d'),
                    approved_head: revision('c'),
                },
            )
            .is_err()
        );
        assert_eq!(
            test.db.load_review_job("job-lgtm").unwrap().unwrap().status,
            ReviewJobStatus::ResultReady,
            "failed checkpoint CAS must roll back applied state"
        );
        let applied = apply_job_result(
            &test.db,
            "job-lgtm",
            0,
            &lgtm_hash,
            &ReviewApplyKind::Lgtm {
                expected_base: revision('a'),
                approved_head: revision('c'),
            },
        )
        .unwrap();
        assert_eq!(applied.status, ReviewJobStatus::Applied);
        assert_eq!(
            test.db
                .load_review_route_by_id("route-1")
                .unwrap()
                .unwrap()
                .base_checkpoint,
            revision('c')
        );
    }

    #[test]
    fn failure_indeterminate_stale_cancel_and_retry_follow_legal_table() {
        let test = TestDb::new();
        let route = setup_bound_route(&test);
        insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-retry",
            "job-retry",
            &revision('b'),
            None,
        );

        assert_cancelable_without_commit(&test.db, "job-retry", 0, ReviewJobStatus::Queued);
        assert!(
            retry_job(
                &test.db,
                "job-retry",
                0,
                &revision('b'),
                false,
                "review-workers/rv-retry/job-retry/queued-retry",
            )
            .is_err(),
            "queued jobs cannot retry"
        );
        let failed = test
            .db
            .fail_review_job("job-retry", 0, None, "spawn", "spawn failed")
            .unwrap();
        assert_eq!(failed.status, ReviewJobStatus::Failed);
        assert_cancelable_without_commit(&test.db, "job-retry", 0, ReviewJobStatus::Failed);
        assert!(
            retry_job(
                &test.db,
                "job-retry",
                0,
                &revision('b'),
                false,
                &failed.artifact_dir,
            )
            .is_err()
        );
        let retry = retry_job(
            &test.db,
            "job-retry",
            0,
            &revision('b'),
            false,
            "review-workers/rv-retry/job-retry/attempt-1",
        )
        .unwrap();
        assert_eq!(retry.status, ReviewJobStatus::Queued);
        assert_eq!(retry.attempt, 1);
        assert!(retry.lease_owner.is_none());
        assert!(retry.error_kind.is_none());

        test.db
            .claim_review_job(
                "job-retry",
                1,
                "lease-retry",
                now_epoch_f64() + 3600.0,
                3234,
                "birth-retry",
            )
            .unwrap()
            .unwrap();
        assert_cancelable_without_commit(&test.db, "job-retry", 1, ReviewJobStatus::Running);
        assert!(
            retry_job(
                &test.db,
                "job-retry",
                1,
                &revision('b'),
                false,
                "review-workers/rv-retry/job-retry/running-retry",
            )
            .is_err(),
            "running jobs cannot retry"
        );
        assert!(
            test.db
                .fail_review_job("job-retry", 1, Some("wrong-owner"), "adapter", "failed",)
                .is_err()
        );
        let indeterminate = test
            .db
            .mark_review_job_indeterminate(
                "job-retry",
                1,
                "lease-retry",
                "partial_output",
                "completion unknown",
            )
            .unwrap();
        assert_eq!(indeterminate.status, ReviewJobStatus::Indeterminate);
        assert_cancelable_without_commit(&test.db, "job-retry", 1, ReviewJobStatus::Indeterminate);
        assert!(
            retry_job(
                &test.db,
                "job-retry",
                1,
                &revision('b'),
                false,
                "review-workers/rv-retry/job-retry/attempt-2",
            )
            .is_err(),
            "indeterminate retry requires an explicit safety proof"
        );
        let retry = retry_job(
            &test.db,
            "job-retry",
            1,
            &revision('b'),
            true,
            "review-workers/rv-retry/job-retry/attempt-2",
        )
        .unwrap();
        assert_eq!(retry.attempt, 2);
        let canceled = cancel_job(&test.db, "job-retry", 2, "operator canceled").unwrap();
        assert_eq!(canceled.status, ReviewJobStatus::Canceled);
        let tx =
            Transaction::new_unchecked(test.db.conn(), TransactionBehavior::Immediate).unwrap();
        assert!(cancel_review_job_on(&tx, "job-retry", 2, "duplicate cancel").is_err());
        drop(tx);
        assert_eq!(
            test.db
                .load_review_route_by_id("route-1")
                .unwrap()
                .unwrap()
                .base_checkpoint,
            revision('a'),
            "failure, indeterminate, retry, and cancel must not move the route base"
        );
        assert!(
            retry_job(
                &test.db,
                "job-retry",
                2,
                &revision('b'),
                true,
                "review-workers/rv-retry/job-retry/attempt-3",
            )
            .is_err()
        );

        assert!(
            test.db
                .conn()
                .execute(
                    "UPDATE review_jobs
                     SET status = 'applied', progress_phase = 'done',
                         applied_at = 2.0
                     WHERE id = 'job-retry'",
                    []
                )
                .is_err(),
            "DB trigger must reject illegal terminal transitions"
        );

        test.db
            .conn()
            .execute(
                "UPDATE review_runs SET state = 'canceled'
                 WHERE id = 'rv-retry'",
                [],
            )
            .unwrap();
        insert_worker_workflow_and_job(
            &test.db,
            &route,
            "rv-stale",
            "job-stale",
            &revision('c'),
            None,
        );
        test.db
            .claim_review_job(
                "job-stale",
                0,
                "lease-stale",
                now_epoch_f64() + 3600.0,
                4234,
                "birth-stale",
            )
            .unwrap()
            .unwrap();
        assert!(
            test.db
                .mark_review_job_stale("job-stale", 0, None, "revision changed")
                .is_err(),
            "a claimed stale transition must carry the exact lease owner"
        );
        let stale = test
            .db
            .mark_review_job_stale("job-stale", 0, Some("lease-stale"), "revision changed")
            .unwrap();
        assert_eq!(stale.status, ReviewJobStatus::Stale);
        assert_cancelable_without_commit(&test.db, "job-stale", 0, ReviewJobStatus::Stale);
        assert!(
            retry_job(
                &test.db,
                "job-stale",
                0,
                &revision('c'),
                true,
                "review-workers/rv-stale/job-stale/attempt-1",
            )
            .is_err(),
            "stale jobs cannot retry"
        );
        assert_eq!(
            test.db
                .load_review_route_by_id("route-1")
                .unwrap()
                .unwrap()
                .base_checkpoint,
            revision('a'),
            "stale must not move the route base"
        );
    }
}

pub fn advance_review_route_checkpoint_on(
    tx: &Transaction<'_>,
    route_id: &str,
    expected_base: &str,
    approved_head: &str,
) -> ReviewWorkerStateResult<()> {
    let changed = tx.execute(
        "UPDATE review_routes
         SET base_checkpoint = ?1, updated_at = ?2
         WHERE id = ?3
           AND binding_state = 'bound'
           AND base_checkpoint = ?4",
        params![approved_head, now_epoch_f64(), route_id, expected_base],
    )?;
    if changed != 1 {
        return Err(conflict(format!(
            "review route '{route_id}' base checkpoint changed"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewWorker {
    pub workflow_id: String,
    pub adapter: String,
    pub native_session_mode: NativeSessionMode,
    pub native_session_id: Option<String>,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub cli_path: String,
    pub cli_version: String,
    pub adapter_contract_ver: i64,
    pub capability_json: String,
    pub created_at: f64,
    pub updated_at: f64,
}

impl ReviewWorker {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let native_session_mode: String = row.get("native_session_mode")?;
        Ok(Self {
            workflow_id: row.get("workflow_id")?,
            adapter: row.get("adapter")?,
            native_session_mode: native_session_mode
                .parse()
                .map_err(|message| row_enum_error(2, message))?,
            native_session_id: row.get("native_session_id")?,
            model: row.get("model")?,
            reasoning: row.get("reasoning")?,
            policy: row.get("policy")?,
            cli_path: row.get("cli_path")?,
            cli_version: row.get("cli_version")?,
            adapter_contract_ver: row.get("adapter_contract_ver")?,
            capability_json: row.get("capability_json")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

const REVIEW_WORKER_COLUMNS: &str = "
    workflow_id, adapter, native_session_mode, native_session_id, model,
    reasoning, policy, cli_path, cli_version, adapter_contract_ver,
    capability_json, created_at, updated_at
";

fn load_review_worker_on(
    conn: &rusqlite::Connection,
    workflow_id: &str,
) -> ReviewWorkerStateResult<Option<ReviewWorker>> {
    conn.query_row(
        &format!(
            "SELECT {REVIEW_WORKER_COLUMNS}
             FROM review_workers WHERE workflow_id = ?1"
        ),
        params![workflow_id],
        ReviewWorker::from_row,
    )
    .optional()
    .map_err(Into::into)
}

pub fn insert_review_worker_on(
    tx: &Transaction<'_>,
    workflow_id: &str,
    preassigned_native_session_id: Option<&str>,
) -> ReviewWorkerStateResult<ReviewWorker> {
    if preassigned_native_session_id == Some("") {
        return Err(invalid("preassigned native session id must not be empty"));
    }
    let now = now_epoch_f64();
    let changed = tx.execute(
        "INSERT INTO review_workers (
             workflow_id, adapter, native_session_mode, native_session_id,
             model, reasoning, policy, cli_path, cli_version,
             adapter_contract_ver, capability_json, created_at, updated_at
         )
         SELECT rr.id, route.adapter, route.native_session_mode, ?1,
                route.model, route.reasoning, route.policy, route.cli_path,
                route.cli_version, route.adapter_contract_ver,
                route.capability_json, ?2, ?2
         FROM review_runs rr
         JOIN review_routes route ON route.id = rr.review_route_id
         WHERE rr.id = ?3
           AND rr.reviewer_mode = 'worker'
           AND rr.reviewer_name = route.reviewer_alias
           AND rr.developer_name = route.developer_name
           AND rr.developer_session_id = route.developer_session_id
           AND rr.base_revision = route.base_checkpoint
           AND route.binding_state = 'bound'
           AND (
               (route.native_session_mode = 'preassigned' AND ?1 IS NOT NULL)
               OR
               (route.native_session_mode = 'discovered' AND ?1 IS NULL)
           )",
        params![preassigned_native_session_id, now, workflow_id],
    )?;
    if changed != 1 {
        return Err(conflict(format!(
            "workflow '{workflow_id}' does not match one bound review route"
        )));
    }
    load_review_worker_on(tx, workflow_id)?.ok_or_else(|| {
        ReviewWorkerStateError::NotFound(format!(
            "newly inserted review worker '{workflow_id}' disappeared"
        ))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSessionBindOutcome {
    Bound,
    AlreadyBound,
}

impl HcomDb {
    pub fn load_review_worker(
        &self,
        workflow_id: &str,
    ) -> ReviewWorkerStateResult<Option<ReviewWorker>> {
        load_review_worker_on(self.conn(), workflow_id)
    }

    pub fn bind_discovered_native_session(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        native_session_id: &str,
    ) -> ReviewWorkerStateResult<(NativeSessionBindOutcome, ReviewWorker)> {
        if lease_owner.is_empty() || native_session_id.is_empty() {
            return Err(invalid(
                "lease owner and native session id must not be empty",
            ));
        }
        let tx = Transaction::new_unchecked(self.conn(), TransactionBehavior::Immediate)?;
        let workflow_id = tx
            .query_row(
                "SELECT review_jobs.workflow_id
                 FROM review_jobs
                 JOIN review_workers
                   ON review_workers.workflow_id = review_jobs.workflow_id
                 WHERE review_jobs.id = ?1
                   AND review_jobs.attempt = ?2
                   AND review_jobs.status = 'running'
                   AND review_jobs.lease_owner = ?3
                   AND review_workers.native_session_mode = 'discovered'",
                params![job_id, expected_attempt, lease_owner],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| {
                conflict(format!(
                    "job '{job_id}' is not owned by this attempt and lease"
                ))
            })?;
        let changed = tx.execute(
            "UPDATE review_workers
             SET native_session_id = ?1, updated_at = ?2
             WHERE workflow_id = ?3 AND native_session_id IS NULL",
            params![native_session_id, now_epoch_f64(), workflow_id],
        )?;
        let worker = load_review_worker_on(&tx, &workflow_id)?.ok_or_else(|| {
            ReviewWorkerStateError::NotFound(format!(
                "review worker for job '{job_id}' was not found"
            ))
        })?;
        let outcome = if changed == 1 {
            NativeSessionBindOutcome::Bound
        } else if worker.native_session_id.as_deref() == Some(native_session_id) {
            NativeSessionBindOutcome::AlreadyBound
        } else {
            return Err(conflict(format!(
                "workflow '{}' already has a different native session",
                worker.workflow_id
            )));
        };
        tx.commit()?;
        Ok((outcome, worker))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewJobStatus {
    Queued,
    Running,
    ResultReady,
    Applied,
    Failed,
    Indeterminate,
    Stale,
    Canceled,
}

impl ReviewJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::ResultReady => "result_ready",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
            Self::Stale => "stale",
            Self::Canceled => "canceled",
        }
    }
}

impl fmt::Display for ReviewJobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReviewJobStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "result_ready" => Ok(Self::ResultReady),
            "applied" => Ok(Self::Applied),
            "failed" => Ok(Self::Failed),
            "indeterminate" => Ok(Self::Indeterminate),
            "stale" => Ok(Self::Stale),
            "canceled" => Ok(Self::Canceled),
            _ => Err(format!("unknown review job status '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReviewProgressPhase {
    Queued,
    Spawn,
    Running,
    Validating,
    Applying,
    Done,
}

impl ReviewProgressPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Spawn => "spawn",
            Self::Running => "running",
            Self::Validating => "validating",
            Self::Applying => "applying",
            Self::Done => "done",
        }
    }
}

impl fmt::Display for ReviewProgressPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReviewProgressPhase {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "spawn" => Ok(Self::Spawn),
            "running" => Ok(Self::Running),
            "validating" => Ok(Self::Validating),
            "applying" => Ok(Self::Applying),
            "done" => Ok(Self::Done),
            _ => Err(format!("unknown review progress phase '{value}'")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewJob {
    pub id: String,
    pub workflow_id: String,
    pub round: i64,
    pub request_version: i64,
    pub base_revision: String,
    pub head_revision: String,
    pub developer_submission: String,
    pub status: ReviewJobStatus,
    pub attempt: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<f64>,
    pub worker_pid: Option<i64>,
    pub worker_process_birth: Option<String>,
    pub progress_phase: ReviewProgressPhase,
    pub last_progress_at: Option<f64>,
    pub activity_truncated: bool,
    pub artifact_dir: String,
    pub result_json: Option<String>,
    pub result_hash: Option<String>,
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub result_at: Option<f64>,
    pub applied_at: Option<f64>,
    pub updated_at: f64,
}

impl ReviewJob {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let status: String = row.get("status")?;
        let phase: String = row.get("progress_phase")?;
        Ok(Self {
            id: row.get("id")?,
            workflow_id: row.get("workflow_id")?,
            round: row.get("round")?,
            request_version: row.get("request_version")?,
            base_revision: row.get("base_revision")?,
            head_revision: row.get("head_revision")?,
            developer_submission: row.get("developer_submission")?,
            status: status
                .parse()
                .map_err(|message| row_enum_error(7, message))?,
            attempt: row.get("attempt")?,
            lease_owner: row.get("lease_owner")?,
            lease_expires_at: row.get("lease_expires_at")?,
            worker_pid: row.get("worker_pid")?,
            worker_process_birth: row.get("worker_process_birth")?,
            progress_phase: phase
                .parse()
                .map_err(|message| row_enum_error(13, message))?,
            last_progress_at: row.get("last_progress_at")?,
            activity_truncated: row.get::<_, i64>("activity_truncated")? != 0,
            artifact_dir: row.get("artifact_dir")?,
            result_json: row.get("result_json")?,
            result_hash: row.get("result_hash")?,
            error_kind: row.get("error_kind")?,
            error_message: row.get("error_message")?,
            created_at: row.get("created_at")?,
            started_at: row.get("started_at")?,
            result_at: row.get("result_at")?,
            applied_at: row.get("applied_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NewReviewJob {
    pub id: String,
    pub workflow_id: String,
    pub round: i64,
    pub request_version: i64,
    pub base_revision: String,
    pub head_revision: String,
    pub developer_submission: String,
    pub artifact_dir: String,
}

const REVIEW_JOB_COLUMNS: &str = "
    id, workflow_id, round, request_version, base_revision, head_revision,
    developer_submission, status, attempt, lease_owner, lease_expires_at,
    worker_pid, worker_process_birth, progress_phase, last_progress_at,
    activity_truncated, artifact_dir, result_json, result_hash, error_kind,
    error_message, created_at, started_at, result_at, applied_at, updated_at
";

fn load_review_job_on(
    conn: &rusqlite::Connection,
    job_id: &str,
) -> ReviewWorkerStateResult<Option<ReviewJob>> {
    conn.query_row(
        &format!("SELECT {REVIEW_JOB_COLUMNS} FROM review_jobs WHERE id = ?1"),
        params![job_id],
        ReviewJob::from_row,
    )
    .optional()
    .map_err(Into::into)
}

pub fn insert_review_job_on(
    tx: &Transaction<'_>,
    job: &NewReviewJob,
) -> ReviewWorkerStateResult<ReviewJob> {
    let now = now_epoch_f64();
    let changed = tx.execute(
        "INSERT INTO review_jobs (
             id, workflow_id, round, request_version, base_revision,
             head_revision, developer_submission, status, attempt,
             progress_phase, activity_truncated, artifact_dir,
             created_at, updated_at
         )
         SELECT ?1, rr.id, ?2, ?3, ?4, ?5, ?6, 'queued', 0,
                'queued', 0, ?7, ?8, ?8
         FROM review_runs rr
         JOIN review_workers worker ON worker.workflow_id = rr.id
         WHERE rr.id = ?9
           AND rr.reviewer_mode = 'worker'
           AND rr.state = 'awaiting_review'
           AND rr.round = ?2
           AND rr.version = ?3
           AND rr.base_revision = ?4",
        params![
            job.id,
            job.round,
            job.request_version,
            job.base_revision,
            job.head_revision,
            job.developer_submission,
            job.artifact_dir,
            now,
            job.workflow_id,
        ],
    )?;
    if changed != 1 {
        return Err(conflict(format!(
            "workflow '{}' is not awaiting this exact review request",
            job.workflow_id
        )));
    }
    load_review_job_on(tx, &job.id)?.ok_or_else(|| {
        ReviewWorkerStateError::NotFound(format!(
            "newly inserted review job '{}' disappeared",
            job.id
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveReviewConflict {
    pub workflow_id: String,
    pub reviewer_mode: String,
}

pub fn find_non_final_review_for_developer_on(
    tx: &Transaction<'_>,
    developer_session_id: &str,
) -> ReviewWorkerStateResult<Option<ActiveReviewConflict>> {
    tx.query_row(
        "SELECT id, reviewer_mode
         FROM review_runs
         WHERE developer_session_id = ?1
           AND state NOT IN ('approved', 'canceled')
         ORDER BY created_at, id
         LIMIT 1",
        params![developer_session_id],
        |row| {
            Ok(ActiveReviewConflict {
                workflow_id: row.get(0)?,
                reviewer_mode: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

impl HcomDb {
    pub fn load_review_job(&self, job_id: &str) -> ReviewWorkerStateResult<Option<ReviewJob>> {
        load_review_job_on(self.conn(), job_id)
    }

    pub fn find_non_final_review_for_developer(
        &self,
        developer_session_id: &str,
    ) -> ReviewWorkerStateResult<Option<ActiveReviewConflict>> {
        let tx = Transaction::new_unchecked(self.conn(), TransactionBehavior::Deferred)?;
        let active = find_non_final_review_for_developer_on(&tx, developer_session_id)?;
        tx.commit()?;
        Ok(active)
    }

    pub fn claim_review_job(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        lease_expires_at: f64,
        worker_pid: i64,
        worker_process_birth: &str,
    ) -> ReviewWorkerStateResult<Option<ReviewJob>> {
        if lease_owner.is_empty() || worker_process_birth.is_empty() {
            return Err(invalid(
                "lease owner and worker process birth identity must not be empty",
            ));
        }
        if worker_pid <= 0 {
            return Err(invalid("worker pid must be positive"));
        }
        let now = now_epoch_f64();
        if lease_expires_at <= now {
            return Err(invalid("lease expiry must be in the future"));
        }
        let tx = Transaction::new_unchecked(self.conn(), TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE review_jobs
             SET status = 'running',
                 lease_owner = ?1,
                 lease_expires_at = ?2,
                 worker_pid = ?3,
                 worker_process_birth = ?4,
                 progress_phase = 'spawn',
                 started_at = ?5,
                 updated_at = ?5
             WHERE id = ?6
               AND status = 'queued'
               AND attempt = ?7
               AND lease_owner IS NULL
               AND worker_pid IS NULL",
            params![
                lease_owner,
                lease_expires_at,
                worker_pid,
                worker_process_birth,
                now,
                job_id,
                expected_attempt,
            ],
        )?;
        let claimed = if changed == 1 {
            load_review_job_on(&tx, job_id)?
        } else {
            None
        };
        tx.commit()?;
        Ok(claimed)
    }

    pub fn heartbeat_review_job(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        lease_expires_at: f64,
    ) -> ReviewWorkerStateResult<bool> {
        if lease_owner.is_empty() || lease_expires_at <= now_epoch_f64() {
            return Err(invalid(
                "heartbeat requires a non-empty owner and future expiry",
            ));
        }
        let changed = self.conn().execute(
            "UPDATE review_jobs
             SET lease_expires_at = ?1, updated_at = ?2
             WHERE id = ?3
               AND status = 'running'
               AND attempt = ?4
               AND lease_owner = ?5",
            params![
                lease_expires_at,
                now_epoch_f64(),
                job_id,
                expected_attempt,
                lease_owner,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn update_review_job_progress(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        phase: ReviewProgressPhase,
        progress_at: f64,
        activity_truncated: bool,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        if !matches!(
            phase,
            ReviewProgressPhase::Spawn
                | ReviewProgressPhase::Running
                | ReviewProgressPhase::Validating
        ) {
            return Err(invalid("running job progress has an invalid phase"));
        }
        let tx = Transaction::new_unchecked(self.conn(), TransactionBehavior::Immediate)?;
        let current = load_review_job_on(&tx, job_id)?
            .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' not found")))?;
        if current.status != ReviewJobStatus::Running
            || current.attempt != expected_attempt
            || current.lease_owner.as_deref() != Some(lease_owner)
        {
            return Err(conflict(format!(
                "job '{job_id}' is not owned by this running attempt"
            )));
        }
        if phase < current.progress_phase
            || current
                .last_progress_at
                .is_some_and(|previous| progress_at < previous)
        {
            return Err(conflict(format!(
                "job '{job_id}' progress would move backwards"
            )));
        }
        tx.execute(
            "UPDATE review_jobs
             SET progress_phase = ?1,
                 last_progress_at = ?2,
                 activity_truncated =
                     CASE WHEN activity_truncated = 1 OR ?3 = 1 THEN 1 ELSE 0 END,
                 updated_at = ?4
             WHERE id = ?5
               AND status = 'running'
               AND attempt = ?6
               AND lease_owner = ?7",
            params![
                phase.as_str(),
                progress_at,
                i64::from(activity_truncated),
                now_epoch_f64(),
                job_id,
                expected_attempt,
                lease_owner,
            ],
        )?;
        let updated = load_review_job_on(&tx, job_id)?.ok_or_else(|| {
            ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared"))
        })?;
        tx.commit()?;
        Ok(updated)
    }

    pub fn publish_review_job_result(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        result_json: &str,
        result_hash: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        if result_json.is_empty() || result_hash.len() != 64 {
            return Err(invalid(
                "validated result and its 64-byte hash are required",
            ));
        }
        let tx = Transaction::new_unchecked(self.conn(), TransactionBehavior::Immediate)?;
        let now = now_epoch_f64();
        let changed = tx.execute(
            "UPDATE review_jobs
             SET status = 'result_ready',
                 progress_phase = 'applying',
                 result_json = ?1,
                 result_hash = ?2,
                 result_at = ?3,
                 updated_at = ?3
             WHERE id = ?4
               AND status = 'running'
               AND attempt = ?5
               AND lease_owner = ?6
               AND EXISTS (
                   SELECT 1 FROM review_workers worker
                   WHERE worker.workflow_id = review_jobs.workflow_id
                     AND worker.native_session_id IS NOT NULL
               )",
            params![
                result_json,
                result_hash,
                now,
                job_id,
                expected_attempt,
                lease_owner,
            ],
        )?;
        if changed != 1 {
            return Err(conflict(format!(
                "job '{job_id}' cannot publish a result for this attempt and lease"
            )));
        }
        let updated = load_review_job_on(&tx, job_id)?.ok_or_else(|| {
            ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared"))
        })?;
        tx.commit()?;
        Ok(updated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewApplyKind {
    NonLgtm,
    Lgtm {
        expected_base: String,
        approved_head: String,
    },
}

pub fn apply_review_job_result_on(
    tx: &Transaction<'_>,
    job_id: &str,
    expected_attempt: i64,
    expected_result_hash: &str,
    kind: &ReviewApplyKind,
) -> ReviewWorkerStateResult<ReviewJob> {
    let job = load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' not found")))?;
    if job.status != ReviewJobStatus::ResultReady
        || job.attempt != expected_attempt
        || job.result_hash.as_deref() != Some(expected_result_hash)
    {
        return Err(conflict(format!(
            "job '{job_id}' is not the expected result-ready attempt"
        )));
    }
    if let ReviewApplyKind::Lgtm {
        expected_base,
        approved_head,
    } = kind
    {
        if &job.base_revision != expected_base || &job.head_revision != approved_head {
            return Err(conflict(format!(
                "job '{job_id}' revision snapshot does not match LGTM apply"
            )));
        }
        let route_id = tx
            .query_row(
                "SELECT review_route_id
                 FROM review_runs
                 WHERE id = ?1 AND reviewer_mode = 'worker'",
                params![job.workflow_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .ok_or_else(|| {
                conflict(format!(
                    "job '{}' has no worker review route",
                    job.workflow_id
                ))
            })?;
        advance_review_route_checkpoint_on(tx, &route_id, expected_base, approved_head)?;
    }

    let now = now_epoch_f64();
    let changed = tx.execute(
        "UPDATE review_jobs
         SET status = 'applied',
             progress_phase = 'done',
             applied_at = ?1,
             updated_at = ?1
         WHERE id = ?2
           AND status = 'result_ready'
           AND attempt = ?3
           AND result_hash = ?4",
        params![now, job_id, expected_attempt, expected_result_hash],
    )?;
    if changed != 1 {
        return Err(conflict(format!(
            "job '{job_id}' result was already advanced"
        )));
    }
    load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared")))
}

fn check_claimed_guard(
    job: &ReviewJob,
    expected_attempt: i64,
    expected_lease_owner: Option<&str>,
) -> ReviewWorkerStateResult<()> {
    if job.attempt != expected_attempt {
        return Err(conflict(format!("job '{}' attempt changed", job.id)));
    }
    if let Some(owner) = expected_lease_owner
        && job.lease_owner.as_deref() != Some(owner)
    {
        return Err(conflict(format!("job '{}' lease owner changed", job.id)));
    }
    Ok(())
}

fn mark_review_job_terminal(
    db: &HcomDb,
    job_id: &str,
    expected_attempt: i64,
    expected_lease_owner: Option<&str>,
    terminal: ReviewJobStatus,
    error_kind: &str,
    error_message: &str,
) -> ReviewWorkerStateResult<ReviewJob> {
    if !matches!(
        terminal,
        ReviewJobStatus::Failed | ReviewJobStatus::Indeterminate | ReviewJobStatus::Stale
    ) {
        return Err(invalid("unsupported terminal job state"));
    }
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let job = load_review_job_on(&tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' not found")))?;
    check_claimed_guard(&job, expected_attempt, expected_lease_owner)?;
    let allowed = match terminal {
        ReviewJobStatus::Failed => matches!(
            job.status,
            ReviewJobStatus::Queued | ReviewJobStatus::Running
        ),
        ReviewJobStatus::Indeterminate => job.status == ReviewJobStatus::Running,
        ReviewJobStatus::Stale => matches!(
            job.status,
            ReviewJobStatus::Queued | ReviewJobStatus::Running | ReviewJobStatus::ResultReady
        ),
        _ => false,
    };
    if !allowed {
        return Err(conflict(format!(
            "job '{job_id}' cannot move from {} to {terminal}",
            job.status
        )));
    }
    if job.status == ReviewJobStatus::Queued && expected_lease_owner.is_some() {
        return Err(conflict("queued job must not have a lease guard"));
    }
    if job.status != ReviewJobStatus::Queued && expected_lease_owner.is_none() {
        return Err(conflict("claimed job requires its lease owner"));
    }

    let changed = tx.execute(
        "UPDATE review_jobs
         SET status = ?1,
             progress_phase = 'done',
             error_kind = ?2,
             error_message = ?3,
             updated_at = ?4
         WHERE id = ?5 AND status = ?6 AND attempt = ?7",
        params![
            terminal.as_str(),
            error_kind,
            error_message,
            now_epoch_f64(),
            job_id,
            job.status.as_str(),
            expected_attempt,
        ],
    )?;
    if changed != 1 {
        return Err(conflict(format!(
            "job '{job_id}' changed during terminal transition"
        )));
    }
    let updated = load_review_job_on(&tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared")))?;
    tx.commit()?;
    Ok(updated)
}

impl HcomDb {
    pub fn fail_review_job(
        &self,
        job_id: &str,
        expected_attempt: i64,
        expected_lease_owner: Option<&str>,
        error_kind: &str,
        error_message: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        mark_review_job_terminal(
            self,
            job_id,
            expected_attempt,
            expected_lease_owner,
            ReviewJobStatus::Failed,
            error_kind,
            error_message,
        )
    }

    pub fn mark_review_job_indeterminate(
        &self,
        job_id: &str,
        expected_attempt: i64,
        lease_owner: &str,
        error_kind: &str,
        error_message: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        mark_review_job_terminal(
            self,
            job_id,
            expected_attempt,
            Some(lease_owner),
            ReviewJobStatus::Indeterminate,
            error_kind,
            error_message,
        )
    }

    pub fn mark_review_job_stale(
        &self,
        job_id: &str,
        expected_attempt: i64,
        expected_lease_owner: Option<&str>,
        error_message: &str,
    ) -> ReviewWorkerStateResult<ReviewJob> {
        mark_review_job_terminal(
            self,
            job_id,
            expected_attempt,
            expected_lease_owner,
            ReviewJobStatus::Stale,
            "revision_stale",
            error_message,
        )
    }
}

/// Cancel a job inside the caller's review-workflow transition transaction.
pub fn cancel_review_job_on(
    tx: &Transaction<'_>,
    job_id: &str,
    expected_attempt: i64,
    reason: &str,
) -> ReviewWorkerStateResult<ReviewJob> {
    let job = load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' not found")))?;
    if job.attempt != expected_attempt
        || matches!(
            job.status,
            ReviewJobStatus::Applied | ReviewJobStatus::Canceled
        )
    {
        return Err(conflict(format!(
            "job '{job_id}' is not cancelable at this attempt"
        )));
    }
    let changed = tx.execute(
        "UPDATE review_jobs
         SET status = 'canceled',
             progress_phase = 'done',
             error_kind = 'canceled',
             error_message = ?1,
             updated_at = ?2
         WHERE id = ?3 AND status = ?4 AND attempt = ?5",
        params![
            reason,
            now_epoch_f64(),
            job_id,
            job.status.as_str(),
            expected_attempt
        ],
    )?;
    if changed != 1 {
        return Err(conflict(format!("job '{job_id}' changed during cancel")));
    }
    load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared")))
}

/// Queue a new attempt inside the caller's workflow/revision validation
/// transaction. Indeterminate work requires an explicit external safety proof.
pub fn retry_review_job_on(
    tx: &Transaction<'_>,
    job_id: &str,
    expected_attempt: i64,
    expected_head_revision: &str,
    allow_indeterminate: bool,
    new_artifact_dir: &str,
) -> ReviewWorkerStateResult<ReviewJob> {
    let job = load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' not found")))?;
    if job.attempt != expected_attempt || job.head_revision != expected_head_revision {
        return Err(conflict(format!(
            "job '{job_id}' attempt or revision changed"
        )));
    }
    let retryable = job.status == ReviewJobStatus::Failed
        || (job.status == ReviewJobStatus::Indeterminate && allow_indeterminate);
    if !retryable {
        return Err(conflict(format!(
            "job '{job_id}' in state {} is not safely retryable",
            job.status
        )));
    }
    if new_artifact_dir == job.artifact_dir {
        return Err(invalid("retry must use a new attempt artifact directory"));
    }
    let changed = tx.execute(
        "UPDATE review_jobs
         SET status = 'queued',
             attempt = attempt + 1,
             lease_owner = NULL,
             lease_expires_at = NULL,
             worker_pid = NULL,
             worker_process_birth = NULL,
             progress_phase = 'queued',
             last_progress_at = NULL,
             activity_truncated = 0,
             artifact_dir = ?1,
             error_kind = NULL,
             error_message = NULL,
             started_at = NULL,
             applied_at = NULL,
             updated_at = ?2
         WHERE id = ?3
           AND status = ?4
           AND attempt = ?5
           AND head_revision = ?6",
        params![
            new_artifact_dir,
            now_epoch_f64(),
            job_id,
            job.status.as_str(),
            expected_attempt,
            expected_head_revision,
        ],
    )?;
    if changed != 1 {
        return Err(conflict(format!("job '{job_id}' changed during retry")));
    }
    load_review_job_on(tx, job_id)?
        .ok_or_else(|| ReviewWorkerStateError::NotFound(format!("job '{job_id}' disappeared")))
}
