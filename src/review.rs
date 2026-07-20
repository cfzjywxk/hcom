//! Persistent, deterministic two-party review workflow.
//!
//! Free-form messages are deliberately outside the control plane. Only the
//! structured operations in this module can advance a review run.

use std::fmt;
use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::db::HcomDb;
use crate::db::subscriptions::add_thread_memberships_on;
use crate::messages::validate_message;
use crate::shared::time::{now_epoch_f64, now_iso};

pub const DEFAULT_MAX_ROUNDS: i64 = 3;
pub const MAX_ROUNDS_LIMIT: i64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewState {
    AwaitingReview,
    AwaitingDeveloper,
    MaxRounds,
    Approved,
    Canceled,
}

impl ReviewState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingReview => "awaiting_review",
            Self::AwaitingDeveloper => "awaiting_developer",
            Self::MaxRounds => "max_rounds",
            Self::Approved => "approved",
            Self::Canceled => "canceled",
        }
    }

    pub fn is_final(self) -> bool {
        matches!(self, Self::Approved | Self::Canceled)
    }
}

impl fmt::Display for ReviewState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReviewState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "awaiting_review" => Ok(Self::AwaitingReview),
            "awaiting_developer" => Ok(Self::AwaitingDeveloper),
            "max_rounds" => Ok(Self::MaxRounds),
            "approved" => Ok(Self::Approved),
            "canceled" => Ok(Self::Canceled),
            _ => Err(format!("Unknown review state '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewAction {
    Start,
    RequestChanges,
    Lgtm,
    Fixed,
    Rebut,
    Extend,
    Cancel,
}

impl ReviewAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::RequestChanges => "request_changes",
            Self::Lgtm => "lgtm",
            Self::Fixed => "fixed",
            Self::Rebut => "rebut",
            Self::Extend => "extend",
            Self::Cancel => "cancel",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewRole {
    Developer,
    Reviewer,
}

impl ReviewRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Developer => "developer",
            Self::Reviewer => "reviewer",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReviewActor {
    pub name: String,
    pub session_id: String,
}

#[derive(Debug, Clone)]
pub struct ReviewRun {
    pub id: String,
    pub task: String,
    pub workspace: String,
    pub thread: String,
    pub developer_name: String,
    pub developer_session_id: String,
    pub reviewer_name: String,
    pub reviewer_session_id: String,
    pub state: ReviewState,
    pub round: i64,
    pub max_rounds: i64,
    pub version: i64,
    pub last_message_event_id: Option<i64>,
    pub created_at: f64,
    pub updated_at: f64,
}

impl ReviewRun {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let state: String = row.get("state")?;
        let state = state.parse().map_err(|e: String| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            )
        })?;
        Ok(Self {
            id: row.get("id")?,
            task: row.get("task")?,
            workspace: row.get("workspace")?,
            thread: row.get("thread")?,
            developer_name: row.get("developer_name")?,
            developer_session_id: row.get("developer_session_id")?,
            reviewer_name: row.get("reviewer_name")?,
            reviewer_session_id: row.get("reviewer_session_id")?,
            state,
            round: row.get("round")?,
            max_rounds: row.get("max_rounds")?,
            version: row.get("version")?,
            last_message_event_id: row.get("last_message_event_id")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReviewOutcome {
    pub run: ReviewRun,
    pub replayed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ReviewError {
    #[error("{0}")]
    Regular(String),
    #[error("{0}")]
    Conflict(String),
}

impl ReviewError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Regular(_) => 1,
            Self::Conflict(_) => 2,
        }
    }
}

impl From<rusqlite::Error> for ReviewError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Regular(format!("Database error: {value}"))
    }
}

#[derive(Debug, Clone)]
pub struct MutationRequest {
    pub action: ReviewAction,
    pub round: Option<i64>,
    pub summary: String,
    pub new_max_rounds: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NextState {
    state: ReviewState,
    round: i64,
    max_rounds: i64,
}

fn compute_next(run: &ReviewRun, request: &MutationRequest) -> Result<NextState, ReviewError> {
    let next = match request.action {
        ReviewAction::RequestChanges if run.state == ReviewState::AwaitingReview => {
            let state = if run.round == run.max_rounds {
                ReviewState::MaxRounds
            } else {
                ReviewState::AwaitingDeveloper
            };
            NextState {
                state,
                round: run.round,
                max_rounds: run.max_rounds,
            }
        }
        ReviewAction::Lgtm
            if matches!(
                run.state,
                ReviewState::AwaitingReview
                    | ReviewState::AwaitingDeveloper
                    | ReviewState::MaxRounds
            ) =>
        {
            NextState {
                state: ReviewState::Approved,
                round: run.round,
                max_rounds: run.max_rounds,
            }
        }
        ReviewAction::Fixed | ReviewAction::Rebut
            if run.state == ReviewState::AwaitingDeveloper =>
        {
            NextState {
                state: ReviewState::AwaitingReview,
                round: run.round + 1,
                max_rounds: run.max_rounds,
            }
        }
        ReviewAction::Extend if run.state == ReviewState::MaxRounds => {
            let new_max = request
                .new_max_rounds
                .ok_or_else(|| ReviewError::Regular("extend requires --max-rounds".to_string()))?;
            if new_max <= run.max_rounds || new_max > MAX_ROUNDS_LIMIT {
                return Err(ReviewError::Regular(format!(
                    "New max rounds must be greater than {} and no more than {MAX_ROUNDS_LIMIT}",
                    run.max_rounds
                )));
            }
            NextState {
                state: ReviewState::AwaitingDeveloper,
                round: run.round,
                max_rounds: new_max,
            }
        }
        ReviewAction::Cancel if !run.state.is_final() => NextState {
            state: ReviewState::Canceled,
            round: run.round,
            max_rounds: run.max_rounds,
        },
        _ => {
            return Err(ReviewError::Conflict(format!(
                "REVIEW_CONFLICT {} state={} round={}/{} action={}",
                run.id,
                run.state,
                run.round,
                run.max_rounds,
                request.action.as_str()
            )));
        }
    };

    if next.round < 1
        || next.round > next.max_rounds
        || next.max_rounds > MAX_ROUNDS_LIMIT
        || (next.state == ReviewState::AwaitingDeveloper && next.round >= next.max_rounds)
        || (next.state == ReviewState::MaxRounds && next.round != next.max_rounds)
    {
        return Err(ReviewError::Regular(
            "Internal review state invariant failed".to_string(),
        ));
    }
    Ok(next)
}

fn load_run(conn: &Connection, id: &str) -> Result<Option<ReviewRun>, ReviewError> {
    conn.query_row(
        "SELECT * FROM review_runs WHERE id = ?1",
        params![id],
        ReviewRun::from_row,
    )
    .optional()
    .map_err(Into::into)
}

pub fn get_run(db: &HcomDb, id: &str) -> Result<Option<ReviewRun>, ReviewError> {
    load_run(db.conn(), id)
}

pub fn list_runs(db: &HcomDb, actor_session_id: &str) -> Result<Vec<ReviewRun>, ReviewError> {
    let mut stmt = db.conn().prepare(
        "SELECT * FROM review_runs
         WHERE (developer_session_id = ?1 OR reviewer_session_id = ?1)
           AND state NOT IN ('approved', 'canceled')
         ORDER BY updated_at DESC",
    )?;
    let rows = stmt
        .query_map(params![actor_session_id], ReviewRun::from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn actor_role(run: &ReviewRun, actor: &ReviewActor) -> Result<ReviewRole, ReviewError> {
    if actor.session_id == run.developer_session_id {
        Ok(ReviewRole::Developer)
    } else if actor.session_id == run.reviewer_session_id {
        Ok(ReviewRole::Reviewer)
    } else {
        Err(ReviewError::Regular(format!(
            "Current session is not a participant in review {}",
            run.id
        )))
    }
}

fn required_role(action: ReviewAction) -> Option<ReviewRole> {
    match action {
        ReviewAction::Start | ReviewAction::Fixed | ReviewAction::Rebut | ReviewAction::Extend => {
            Some(ReviewRole::Developer)
        }
        ReviewAction::RequestChanges | ReviewAction::Lgtm => Some(ReviewRole::Reviewer),
        ReviewAction::Cancel => None,
    }
}

fn requires_round(action: ReviewAction) -> bool {
    matches!(
        action,
        ReviewAction::RequestChanges
            | ReviewAction::Lgtm
            | ReviewAction::Fixed
            | ReviewAction::Rebut
    )
}

fn hash_payload(
    workflow_id: &str,
    round: i64,
    actor_session_id: &str,
    action: ReviewAction,
    new_max_rounds: Option<i64>,
    summary: &str,
) -> String {
    let round = round.to_string();
    let new_max_rounds = new_max_rounds
        .map(|value| value.to_string())
        .unwrap_or_default();
    let mut hasher = Sha256::new();
    for part in [
        workflow_id,
        &round,
        actor_session_id,
        action.as_str(),
        &new_max_rounds,
        summary,
    ] {
        hasher.update(part.len().to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn last_transition_is_replay(
    tx: &Transaction<'_>,
    run: &ReviewRun,
    action: ReviewAction,
    actor_session_id: &str,
    round: i64,
    payload_hash: &str,
) -> Result<bool, ReviewError> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM review_transitions
             WHERE workflow_id = ?1 AND to_version = ?2
               AND action = ?3 AND actor_session_id = ?4
               AND round = ?5 AND payload_hash = ?6
         )",
        params![
            run.id,
            run.version,
            action.as_str(),
            actor_session_id,
            round,
            payload_hash
        ],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn participant_is_deliverable(
    conn: &Connection,
    name: &str,
    session_id: &str,
) -> Result<bool, ReviewError> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM instances
             WHERE name = ?1 AND session_id = ?2
               AND COALESCE(origin_device_id, '') = ''
               AND COALESCE(parent_name, '') = ''
               AND tool IN ('claude', 'codex')
               AND status != 'stopped'
               AND status_context != 'launch_failed'
               AND NOT (status = 'inactive' AND status_context LIKE 'exit:%')
         )",
        params![name, session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

#[derive(Debug)]
struct ReviewMessage<'a> {
    from: &'a str,
    to: &'a str,
    to_session_id: &'a str,
    text: &'a str,
    thread: &'a str,
    intent: &'a str,
    workflow_id: &'a str,
    action: &'a str,
    round: i64,
    max_rounds: i64,
    transition_version: i64,
}

fn insert_review_message(
    tx: &Transaction<'_>,
    message: &ReviewMessage<'_>,
) -> Result<i64, ReviewError> {
    validate_message(message.text).map_err(ReviewError::Regular)?;
    if !participant_is_deliverable(tx, message.to, message.to_session_id)? {
        return Err(ReviewError::Regular(format!(
            "Review participant '@{}' is not currently deliverable with its bound session",
            message.to
        )));
    }

    let data = json!({
        "from": message.from,
        "sender_kind": "instance",
        "scope": "mentions",
        "mentions": [message.to],
        "delivered_to": [message.to],
        "text": message.text,
        "intent": message.intent,
        "thread": message.thread,
        "workflow": {
            "kind": "review",
            "id": message.workflow_id,
            "round": message.round,
            "max_rounds": message.max_rounds,
            "action": message.action,
            "transition_version": message.transition_version,
        }
    });
    tx.execute(
        "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, 'message', ?2, ?3)",
        params![now_iso(), message.from, data.to_string()],
    )?;
    let event_id = tx.last_insert_rowid();
    add_thread_memberships_on(
        tx,
        message.thread,
        Some(message.from),
        &[message.to.to_string()],
        event_id,
    )
    .map_err(|e| ReviewError::Regular(format!("Failed to store review thread routing: {e}")))?;
    Ok(event_id)
}

struct PendingReviewMessage<'a> {
    to: &'a str,
    to_session_id: &'a str,
    intent: &'a str,
    text: String,
    action: &'a str,
}

fn reviewer_message(run: &ReviewRun, submission: Option<(&str, &str)>, version: i64) -> String {
    let submission_text = submission
        .map(|(label, value)| format!("\n{label}:\n{value}\n"))
        .unwrap_or_default();
    format!(
        "[hcom-review {} | round {}/{} | awaiting_review]\n\n\
         This structured request overrides generic hcom response rules. Do not acknowledge it with `hcom send` and do not probe the developer for progress.\n\n\
         Role: reviewer. Review the change; do not modify it.\n\
         Workspace: {}\n\
         Task: {}\n{}\n\
         Submit exactly one structured verdict:\n  \
         hcom review verdict {} --round {} --lgtm --name {} -- '<summary>'\n  \
         hcom review verdict {} --round {} --request-changes --name {} -- '<findings>'\n\n\
         Ordinary message text does not change workflow state.\n\
         Transition version: {}",
        run.id,
        run.round,
        run.max_rounds,
        run.workspace,
        run.task,
        submission_text,
        run.id,
        run.round,
        run.reviewer_name,
        run.id,
        run.round,
        run.reviewer_name,
        version
    )
}

fn developer_changes_message(run: &ReviewRun, summary: &str, maxed: bool) -> String {
    if maxed {
        format!(
            "[hcom-review {} | round {}/{} | max_rounds]\n\n\
             This structured message overrides generic hcom response rules. Do not acknowledge it with `hcom send` and do not probe the reviewer for progress.\n\n\
             Reviewer requested changes:\n{}\n\n\
             The automatic review loop has reached its limit. Stop and report the unresolved findings to the user.\n\
             To continue only when directed, run:\n  \
             hcom review extend {} --max-rounds <new-total> --name {}\n\n\
             The reviewer may still withdraw the finding with:\n  \
             hcom review verdict {} --round {} --lgtm --name {} -- '<summary>'",
            run.id,
            run.round,
            run.max_rounds,
            summary,
            run.id,
            run.developer_name,
            run.id,
            run.round,
            run.reviewer_name
        )
    } else {
        format!(
            "[hcom-review {} | round {}/{} | awaiting_developer]\n\n\
             This structured request overrides generic hcom response rules. Do not acknowledge it with `hcom send` and do not probe the reviewer for progress.\n\n\
             Reviewer requested changes:\n{}\n\n\
             After acting, use exactly one command:\n  \
             hcom review fixed {} --round {} --name {} -- '<changes and tests>'\n  \
             hcom review rebut {} --round {} --name {} -- '<why no change>'\n\n\
             The reviewer may also submit LGTM directly if they withdraw the finding.",
            run.id,
            run.round,
            run.max_rounds,
            summary,
            run.id,
            run.round,
            run.developer_name,
            run.id,
            run.round,
            run.developer_name
        )
    }
}

fn approved_message(run: &ReviewRun, summary: &str) -> String {
    format!(
        "[hcom-review {} | round {}/{} | approved]\n\n\
         Reviewer submitted LGTM:\n{}\n\n\
         The review loop is complete. Report completion to the user.",
        run.id, run.round, run.max_rounds, summary
    )
}

fn canceled_message(run: &ReviewRun, actor: &ReviewActor, reason: &str) -> String {
    format!(
        "[hcom-review {} | round {}/{} | canceled]\n\n\
         Review canceled by {}:\n{}",
        run.id, run.round, run.max_rounds, actor.name, reason
    )
}

pub fn start_review(
    db: &HcomDb,
    developer: &ReviewActor,
    reviewer: &ReviewActor,
    task: &str,
    workspace: &str,
    max_rounds: i64,
) -> Result<ReviewOutcome, ReviewError> {
    if task.trim().is_empty() {
        return Err(ReviewError::Regular("Review task is required".to_string()));
    }
    if workspace.trim().is_empty() {
        return Err(ReviewError::Regular("Workspace is required".to_string()));
    }
    if !(1..=MAX_ROUNDS_LIMIT).contains(&max_rounds) {
        return Err(ReviewError::Regular(format!(
            "--max-rounds must be between 1 and {MAX_ROUNDS_LIMIT}"
        )));
    }
    if developer.session_id == reviewer.session_id {
        return Err(ReviewError::Regular(
            "Self-review is not supported".to_string(),
        ));
    }

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    if !participant_is_deliverable(&tx, &developer.name, &developer.session_id)? {
        return Err(ReviewError::Regular(format!(
            "Developer '@{}' is not a supported, deliverable top-level Claude/Codex instance",
            developer.name
        )));
    }
    if !participant_is_deliverable(&tx, &reviewer.name, &reviewer.session_id)? {
        return Err(ReviewError::Regular(format!(
            "Reviewer '@{}' is not a supported, deliverable top-level Claude/Codex instance",
            reviewer.name
        )));
    }

    let active = tx
        .query_row(
            "SELECT id, state, round, max_rounds FROM review_runs
             WHERE developer_session_id = ?1 AND reviewer_session_id = ?2
               AND state NOT IN ('approved', 'canceled')
             LIMIT 1",
            params![developer.session_id, reviewer.session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    if let Some((id, state, round, max)) = active {
        return Err(ReviewError::Conflict(format!(
            "ACTIVE_REVIEW_EXISTS {id} state={state} round={round}/{max}\n\
             Run: hcom review status {id}\n\
             Or:  hcom review cancel {id} -- '<reason>'"
        )));
    }

    let mut id = None;
    for _ in 0..8 {
        let candidate = format!("rv-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        if load_run(&tx, &candidate)?.is_none() {
            id = Some(candidate);
            break;
        }
    }
    let id = id.ok_or_else(|| ReviewError::Regular("Failed to allocate review ID".to_string()))?;
    let thread = format!("review-{id}");
    let now = now_epoch_f64();
    let mut run = ReviewRun {
        id: id.clone(),
        task: task.to_string(),
        workspace: workspace.to_string(),
        thread,
        developer_name: developer.name.clone(),
        developer_session_id: developer.session_id.clone(),
        reviewer_name: reviewer.name.clone(),
        reviewer_session_id: reviewer.session_id.clone(),
        state: ReviewState::AwaitingReview,
        round: 1,
        max_rounds,
        version: 0,
        last_message_event_id: None,
        created_at: now,
        updated_at: now,
    };

    tx.execute(
        "INSERT INTO review_runs (
             id, task, workspace, thread,
             developer_name, developer_session_id, reviewer_name, reviewer_session_id,
             state, round, max_rounds, version, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, 0, ?11, ?11)",
        params![
            run.id,
            run.task,
            run.workspace,
            run.thread,
            run.developer_name,
            run.developer_session_id,
            run.reviewer_name,
            run.reviewer_session_id,
            run.state.as_str(),
            run.max_rounds,
            now
        ],
    )?;
    let text = reviewer_message(&run, None, 0);
    let event_id = insert_review_message(
        &tx,
        &ReviewMessage {
            from: &developer.name,
            to: &reviewer.name,
            to_session_id: &reviewer.session_id,
            text: &text,
            thread: &run.thread,
            intent: "request",
            workflow_id: &run.id,
            action: "review_request",
            round: 1,
            max_rounds,
            transition_version: 0,
        },
    )?;
    tx.execute(
        "UPDATE review_runs SET last_message_event_id = ?1 WHERE id = ?2",
        params![event_id, run.id],
    )?;
    let payload_hash = hash_payload(
        &run.id,
        1,
        &developer.session_id,
        ReviewAction::Start,
        Some(max_rounds),
        task,
    );
    tx.execute(
        "INSERT INTO review_transitions (
             workflow_id, from_version, to_version, round,
             actor_name, actor_session_id, actor_role, action,
             from_state, to_state, summary, payload_hash, message_event_id, created_at
         ) VALUES (?1, -1, 0, 1, ?2, ?3, 'developer', 'start',
                   NULL, 'awaiting_review', ?4, ?5, ?6, ?7)",
        params![
            run.id,
            developer.name,
            developer.session_id,
            task,
            payload_hash,
            event_id,
            now
        ],
    )?;
    tx.commit()?;
    crate::notify::wake_all(db);
    run.last_message_event_id = Some(event_id);
    Ok(ReviewOutcome {
        run,
        replayed: false,
    })
}

pub fn mutate_review(
    db: &HcomDb,
    actor: &ReviewActor,
    workflow_id: &str,
    request: &MutationRequest,
) -> Result<ReviewOutcome, ReviewError> {
    if request.action == ReviewAction::Start {
        return Err(ReviewError::Regular(
            "start uses start_review, not mutate_review".to_string(),
        ));
    }
    if request.action != ReviewAction::Extend && request.summary.trim().is_empty() {
        return Err(ReviewError::Regular("Summary is required".to_string()));
    }

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let mut run = load_run(&tx, workflow_id)?.ok_or_else(|| {
        ReviewError::Regular(format!("Review workflow '{workflow_id}' not found"))
    })?;
    if !participant_is_deliverable(&tx, &actor.name, &actor.session_id)? {
        return Err(ReviewError::Regular(
            "Review commands require a deliverable top-level Claude/Codex instance with the bound session"
                .to_string(),
        ));
    }
    let role = actor_role(&run, actor)?;
    if let Some(required) = required_role(request.action)
        && role != required
    {
        return Err(ReviewError::Regular(format!(
            "Only the {} can run review {}",
            required.as_str(),
            request.action.as_str()
        )));
    }

    let requested_round = request.round.unwrap_or(run.round);
    let payload_hash = hash_payload(
        &run.id,
        requested_round,
        &actor.session_id,
        request.action,
        request.new_max_rounds,
        &request.summary,
    );
    if last_transition_is_replay(
        &tx,
        &run,
        request.action,
        &actor.session_id,
        requested_round,
        &payload_hash,
    )? {
        tx.commit()?;
        return Ok(ReviewOutcome {
            run,
            replayed: true,
        });
    }

    if requires_round(request.action) && request.round != Some(run.round) {
        return Err(ReviewError::Conflict(format!(
            "REVIEW_CONFLICT {} expected_round={} supplied_round={}; run: hcom review status {}",
            run.id,
            run.round,
            request
                .round
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_string()),
            run.id
        )));
    }

    let from_state = run.state;
    let from_round = run.round;
    let from_version = run.version;
    let next = compute_next(&run, request)?;
    let to_version = from_version + 1;

    let pending_message = match request.action {
        ReviewAction::RequestChanges => Some(PendingReviewMessage {
            to: &run.developer_name,
            to_session_id: &run.developer_session_id,
            intent: "request",
            text: developer_changes_message(
                &run,
                &request.summary,
                next.state == ReviewState::MaxRounds,
            ),
            action: "request_changes",
        }),
        ReviewAction::Lgtm => Some(PendingReviewMessage {
            to: &run.developer_name,
            to_session_id: &run.developer_session_id,
            intent: "inform",
            text: approved_message(&run, &request.summary),
            action: "lgtm",
        }),
        ReviewAction::Fixed | ReviewAction::Rebut => {
            let mut next_run = run.clone();
            next_run.round = next.round;
            next_run.state = next.state;
            let (label, action) = if request.action == ReviewAction::Fixed {
                ("Developer submission", "fixed")
            } else {
                ("Developer rebuttal", "rebut")
            };
            Some(PendingReviewMessage {
                to: &run.reviewer_name,
                to_session_id: &run.reviewer_session_id,
                intent: "request",
                text: reviewer_message(&next_run, Some((label, &request.summary)), to_version),
                action,
            })
        }
        ReviewAction::Cancel => {
            let peer = if role == ReviewRole::Developer {
                (run.reviewer_name.as_str(), run.reviewer_session_id.as_str())
            } else {
                (
                    run.developer_name.as_str(),
                    run.developer_session_id.as_str(),
                )
            };
            if participant_is_deliverable(&tx, peer.0, peer.1)? {
                Some(PendingReviewMessage {
                    to: peer.0,
                    to_session_id: peer.1,
                    intent: "inform",
                    text: canceled_message(&run, actor, &request.summary),
                    action: "cancel",
                })
            } else {
                None
            }
        }
        ReviewAction::Extend => None,
        ReviewAction::Start => unreachable!(),
    };

    let message_event_id = if let Some(message) = pending_message {
        Some(insert_review_message(
            &tx,
            &ReviewMessage {
                from: &actor.name,
                to: message.to,
                to_session_id: message.to_session_id,
                text: &message.text,
                thread: &run.thread,
                intent: message.intent,
                workflow_id: &run.id,
                action: message.action,
                round: next.round,
                max_rounds: next.max_rounds,
                transition_version: to_version,
            },
        )?)
    } else {
        None
    };

    let now = now_epoch_f64();
    let updated = tx.execute(
        "UPDATE review_runs
         SET state = ?1, round = ?2, max_rounds = ?3, version = ?4,
             last_message_event_id = COALESCE(?5, last_message_event_id), updated_at = ?6
         WHERE id = ?7 AND version = ?8",
        params![
            next.state.as_str(),
            next.round,
            next.max_rounds,
            to_version,
            message_event_id,
            now,
            run.id,
            from_version
        ],
    )?;
    if updated != 1 {
        return Err(ReviewError::Conflict(format!(
            "REVIEW_CONFLICT {} changed concurrently; run: hcom review status {}",
            run.id, run.id
        )));
    }
    tx.execute(
        "INSERT INTO review_transitions (
             workflow_id, from_version, to_version, round,
             actor_name, actor_session_id, actor_role, action,
             from_state, to_state, summary, payload_hash, message_event_id, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            run.id,
            from_version,
            to_version,
            from_round,
            actor.name,
            actor.session_id,
            role.as_str(),
            request.action.as_str(),
            from_state.as_str(),
            next.state.as_str(),
            request.summary,
            payload_hash,
            message_event_id,
            now
        ],
    )?;
    tx.commit()?;
    if message_event_id.is_some() {
        crate::notify::wake_all(db);
    }

    run.state = next.state;
    run.round = next.round;
    run.max_rounds = next.max_rounds;
    run.version = to_version;
    if message_event_id.is_some() {
        run.last_message_event_id = message_event_id;
    }
    run.updated_at = now;
    Ok(ReviewOutcome {
        run,
        replayed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    fn run(state: ReviewState, round: i64, max_rounds: i64) -> ReviewRun {
        ReviewRun {
            id: "rv-test".into(),
            task: "task".into(),
            workspace: "/tmp".into(),
            thread: "review-rv-test".into(),
            developer_name: "dev1".into(),
            developer_session_id: "s1".into(),
            reviewer_name: "dev2".into(),
            reviewer_session_id: "s2".into(),
            state,
            round,
            max_rounds,
            version: 0,
            last_message_event_id: None,
            created_at: 0.0,
            updated_at: 0.0,
        }
    }

    fn request(action: ReviewAction) -> MutationRequest {
        MutationRequest {
            action,
            round: Some(1),
            summary: "summary".into(),
            new_max_rounds: None,
        }
    }

    fn test_db() -> (TempDir, HcomDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        (dir, db)
    }

    fn add_agent(db: &HcomDb, name: &str, session_id: &str) {
        add_agent_at_cursor(db, name, session_id, 0);
    }

    fn add_agent_at_cursor(db: &HcomDb, name: &str, session_id: &str, last_event_id: i64) {
        db.conn()
            .execute(
                "INSERT INTO instances (
                     name, session_id, status, status_context, tool, created_at,
                     parent_name, origin_device_id, last_event_id
                 ) VALUES (?1, ?2, 'listening', '', 'codex', 1.0, '', '', ?3)",
                params![name, session_id, last_event_id],
            )
            .unwrap();
    }

    fn actors() -> (ReviewActor, ReviewActor) {
        (
            ReviewActor {
                name: "dev1".into(),
                session_id: "session-dev1".into(),
            },
            ReviewActor {
                name: "dev2".into(),
                session_id: "session-dev2".into(),
            },
        )
    }

    fn started(db: &HcomDb, max_rounds: i64) -> (ReviewActor, ReviewActor, ReviewOutcome) {
        let (developer, reviewer) = actors();
        add_agent(db, &developer.name, &developer.session_id);
        add_agent(db, &reviewer.name, &reviewer.session_id);
        let outcome = start_review(
            db,
            &developer,
            &reviewer,
            "Review implementation and tests",
            "/tmp/workspace",
            max_rounds,
        )
        .unwrap();
        (developer, reviewer, outcome)
    }

    #[test]
    fn transition_table_covers_review_loop() {
        let cases = [
            (
                run(ReviewState::AwaitingReview, 1, 3),
                request(ReviewAction::RequestChanges),
                NextState {
                    state: ReviewState::AwaitingDeveloper,
                    round: 1,
                    max_rounds: 3,
                },
            ),
            (
                run(ReviewState::AwaitingReview, 3, 3),
                request(ReviewAction::RequestChanges),
                NextState {
                    state: ReviewState::MaxRounds,
                    round: 3,
                    max_rounds: 3,
                },
            ),
            (
                run(ReviewState::AwaitingDeveloper, 1, 3),
                request(ReviewAction::Fixed),
                NextState {
                    state: ReviewState::AwaitingReview,
                    round: 2,
                    max_rounds: 3,
                },
            ),
            (
                run(ReviewState::AwaitingDeveloper, 1, 3),
                request(ReviewAction::Rebut),
                NextState {
                    state: ReviewState::AwaitingReview,
                    round: 2,
                    max_rounds: 3,
                },
            ),
        ];
        for (run, request, expected) in cases {
            assert_eq!(compute_next(&run, &request).unwrap(), expected);
        }
    }

    #[test]
    fn lgtm_is_allowed_from_all_reviewable_states() {
        for state in [
            ReviewState::AwaitingReview,
            ReviewState::AwaitingDeveloper,
            ReviewState::MaxRounds,
        ] {
            let run = run(state, 1, 3);
            assert_eq!(
                compute_next(&run, &request(ReviewAction::Lgtm))
                    .unwrap()
                    .state,
                ReviewState::Approved
            );
        }
    }

    #[test]
    fn extend_requires_strictly_larger_bounded_total() {
        let run = run(ReviewState::MaxRounds, 3, 3);
        for max in [3, 21] {
            let mut request = request(ReviewAction::Extend);
            request.round = None;
            request.new_max_rounds = Some(max);
            assert!(compute_next(&run, &request).is_err());
        }
        let mut request = request(ReviewAction::Extend);
        request.round = None;
        request.new_max_rounds = Some(5);
        assert_eq!(
            compute_next(&run, &request).unwrap(),
            NextState {
                state: ReviewState::AwaitingDeveloper,
                round: 3,
                max_rounds: 5,
            }
        );
    }

    #[test]
    fn start_message_is_deliverable_and_has_required_shape() {
        let (_dir, db) = test_db();
        let (_developer, _reviewer, outcome) = started(&db, 3);

        let unread = db.get_unread_messages("dev2");
        assert_eq!(unread.len(), 1);
        assert_eq!(
            unread[0].thread.as_deref(),
            Some(outcome.run.thread.as_str())
        );
        assert!(
            unread[0]
                .text
                .contains("Submit exactly one structured verdict")
        );
        assert!(
            unread[0]
                .text
                .contains("Do not acknowledge it with `hcom send`")
        );
        assert!(
            unread[0]
                .text
                .contains("do not probe the developer for progress")
        );

        let data: String = db
            .conn()
            .query_row(
                "SELECT data FROM events WHERE id = ?1",
                params![outcome.run.last_message_event_id],
                |row| row.get(0),
            )
            .unwrap();
        let data: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(data["from"], "dev1");
        assert_eq!(data["sender_kind"], "instance");
        assert_eq!(data["scope"], "mentions");
        assert_eq!(data["mentions"], json!(["dev2"]));
        assert_eq!(data["delivered_to"], json!(["dev2"]));
        assert_eq!(data["intent"], "request");
        assert_eq!(data["workflow"]["kind"], "review");
        assert_eq!(data["workflow"]["id"], outcome.run.id);

        let memberships: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv
                 WHERE key LIKE 'events_sub:%'
                   AND json_extract(value, '$.thread_name') = ?1",
                params![outcome.run.thread],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(memberships, 2);
    }

    #[test]
    fn developer_review_message_suppresses_ack_and_peer_probing() {
        let (_dir, db) = test_db();
        let (_developer, reviewer, outcome) = started(&db, 3);

        mutate_review(
            &db,
            &reviewer,
            &outcome.run.id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "Fix the race".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();

        let unread = db.get_unread_messages("dev1");
        assert_eq!(unread.len(), 1);
        assert!(
            unread[0]
                .text
                .contains("Do not acknowledge it with `hcom send`")
        );
        assert!(
            unread[0]
                .text
                .contains("do not probe the reviewer for progress")
        );
    }

    #[test]
    fn normal_send_and_review_message_share_delivery_contract() {
        use clap::Parser;

        use crate::commands::send::{SendArgs, cmd_send};
        use crate::shared::{CommandContext, SenderIdentity, SenderKind};

        let (_dir, db) = test_db();
        let (developer, _reviewer, outcome) = started(&db, 3);
        let review_data: String = db
            .conn()
            .query_row(
                "SELECT data FROM events WHERE id = ?1",
                params![outcome.run.last_message_event_id],
                |row| row.get(0),
            )
            .unwrap();
        let review_data: serde_json::Value = serde_json::from_str(&review_data).unwrap();

        let mut args = SendArgs::try_parse_from([
            "send",
            "@dev2",
            "--intent",
            "request",
            "--thread",
            "shape-test",
            "--quiet",
            "--",
            "hello",
        ])
        .unwrap();
        args.had_separator = true;
        let ctx = CommandContext {
            explicit_name: Some(developer.name.clone()),
            identity: Some(SenderIdentity {
                kind: SenderKind::Instance,
                name: developer.name,
                instance_data: db.get_instance("dev1").unwrap(),
                session_id: Some(developer.session_id),
            }),
            go: false,
        };
        assert_eq!(cmd_send(&db, &args, Some(&ctx)), 0);
        let normal_data: String = db
            .conn()
            .query_row(
                "SELECT data FROM events ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let normal_data: serde_json::Value = serde_json::from_str(&normal_data).unwrap();

        for field in ["from", "sender_kind", "scope", "mentions", "delivered_to"] {
            assert_eq!(review_data[field], normal_data[field], "field={field}");
        }
        assert_eq!(review_data["intent"], normal_data["intent"]);
        assert!(review_data["thread"].is_string());
        assert!(normal_data["thread"].is_string());
    }

    #[test]
    fn full_loop_is_idempotent_and_never_creates_round_four() {
        let (_dir, db) = test_db();
        let (developer, reviewer, outcome) = started(&db, 3);
        let id = outcome.run.id;
        let changes = MutationRequest {
            action: ReviewAction::RequestChanges,
            round: Some(1),
            summary: "Fix the race".into(),
            new_max_rounds: None,
        };
        let changed = mutate_review(&db, &reviewer, &id, &changes).unwrap();
        assert_eq!(changed.run.state, ReviewState::AwaitingDeveloper);
        let events_after_first: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let replay = mutate_review(&db, &reviewer, &id, &changes).unwrap();
        assert!(replay.replayed);
        let events_after_replay: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_after_first, events_after_replay);

        for round in 1..=2 {
            let fixed = mutate_review(
                &db,
                &developer,
                &id,
                &MutationRequest {
                    action: ReviewAction::Fixed,
                    round: Some(round),
                    summary: format!("fixed round {round}"),
                    new_max_rounds: None,
                },
            )
            .unwrap();
            assert_eq!(fixed.run.round, round + 1);
            let changes = mutate_review(
                &db,
                &reviewer,
                &id,
                &MutationRequest {
                    action: ReviewAction::RequestChanges,
                    round: Some(round + 1),
                    summary: format!("finding round {}", round + 1),
                    new_max_rounds: None,
                },
            )
            .unwrap();
            if round == 2 {
                assert_eq!(changes.run.state, ReviewState::MaxRounds);
                assert_eq!(changes.run.round, 3);
            }
        }
        let too_far = mutate_review(
            &db,
            &developer,
            &id,
            &MutationRequest {
                action: ReviewAction::Fixed,
                round: Some(3),
                summary: "try round four".into(),
                new_max_rounds: None,
            },
        );
        assert!(matches!(too_far, Err(ReviewError::Conflict(_))));
        assert_eq!(get_run(&db, &id).unwrap().unwrap().round, 3);
    }

    #[test]
    fn reviewer_can_withdraw_changes_with_late_lgtm() {
        let (_dir, db) = test_db();
        let (_developer, reviewer, outcome) = started(&db, 1);
        let id = outcome.run.id;
        let maxed = mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "initial concern".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        assert_eq!(maxed.run.state, ReviewState::MaxRounds);
        let approved = mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::Lgtm,
                round: Some(1),
                summary: "Concern withdrawn".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        assert_eq!(approved.run.state, ReviewState::Approved);
    }

    #[test]
    fn ordinary_lgtm_message_does_not_change_review_state() {
        let (_dir, db) = test_db();
        let (_developer, _reviewer, outcome) = started(&db, 3);
        db.log_event(
            "message",
            "dev2",
            &json!({
                "from": "dev2",
                "sender_kind": "instance",
                "scope": "mentions",
                "mentions": ["dev1"],
                "delivered_to": ["dev1"],
                "text": "LGTM",
                "intent": "inform",
                "thread": outcome.run.thread,
            }),
        )
        .unwrap();
        let run = get_run(&db, &outcome.run.id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingReview);
        assert_eq!(run.version, 0);
    }

    #[test]
    fn cancel_commits_without_message_when_peer_is_stopped() {
        let (_dir, db) = test_db();
        let (developer, _reviewer, outcome) = started(&db, 3);
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev2'", [])
            .unwrap();
        let events_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let canceled = mutate_review(
            &db,
            &developer,
            &outcome.run.id,
            &MutationRequest {
                action: ReviewAction::Cancel,
                round: None,
                summary: "reviewer stopped".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        assert_eq!(canceled.run.state, ReviewState::Canceled);
        let events_after: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_before, events_after);
    }

    #[test]
    fn crash_before_receipt_redelivers_after_same_session_resume() {
        let (_dir, db) = test_db();
        let (_developer, reviewer, outcome) = started(&db, 3);
        assert_eq!(db.get_unread_messages("dev2").len(), 1);

        // PTY exit removes the live row. Plain resume restores the stopped
        // snapshot's session and last_event_id; emulate that final row here.
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev2'", [])
            .unwrap();
        add_agent_at_cursor(&db, "dev2", &reviewer.session_id, 0);

        let unread = db.get_unread_messages("dev2");
        assert_eq!(unread.len(), 1);
        assert_eq!(unread[0].event_id, outcome.run.last_message_event_id);
        assert_eq!(
            get_run(&db, &outcome.run.id).unwrap().unwrap().state,
            ReviewState::AwaitingReview
        );
    }

    #[test]
    fn crash_after_receipt_does_not_redeliver_or_auto_advance() {
        let (_dir, db) = test_db();
        let (_developer, reviewer, outcome) = started(&db, 3);
        let event_id = outcome.run.last_message_event_id.unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET last_event_id = ?1 WHERE name = 'dev2'",
                params![event_id],
            )
            .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev2'", [])
            .unwrap();
        add_agent_at_cursor(&db, "dev2", &reviewer.session_id, event_id);

        assert!(db.get_unread_messages("dev2").is_empty());
        let run = get_run(&db, &outcome.run.id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingReview);
        assert_eq!(run.version, 0);
    }

    #[test]
    fn stopped_recipient_makes_required_transition_fail_atomically() {
        let (_dir, db) = test_db();
        let (developer, reviewer, outcome) = started(&db, 3);
        let id = outcome.run.id;
        mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "finding".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev2'", [])
            .unwrap();
        let events_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();

        let result = mutate_review(
            &db,
            &developer,
            &id,
            &MutationRequest {
                action: ReviewAction::Fixed,
                round: Some(1),
                summary: "fixed while reviewer is down".into(),
                new_max_rounds: None,
            },
        );
        assert!(matches!(result, Err(ReviewError::Regular(_))));
        let run = get_run(&db, &id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingDeveloper);
        assert_eq!(run.round, 1);
        assert_eq!(run.version, 1);
        let events_after: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_before, events_after);
    }

    #[test]
    fn waiting_review_has_no_wall_clock_timeout() {
        let (dir, db) = test_db();
        let path = dir.path().join("hcom.db");
        let (_developer, _reviewer, outcome) = started(&db, 3);
        db.conn()
            .execute(
                "UPDATE review_runs SET updated_at = 0 WHERE id = ?1",
                params![outcome.run.id],
            )
            .unwrap();
        drop(db);

        let db = HcomDb::open_at(&path).unwrap();
        let run = get_run(&db, &outcome.run.id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingReview);
        assert_eq!(run.round, 1);
        assert_eq!(run.version, 0);
    }

    #[test]
    fn workflow_persists_when_both_participants_are_gone() {
        let (_dir, db) = test_db();
        let (developer, _reviewer, outcome) = started(&db, 3);
        db.conn().execute("DELETE FROM instances", []).unwrap();

        let run = get_run(&db, &outcome.run.id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingReview);
        let cancel = mutate_review(
            &db,
            &developer,
            &outcome.run.id,
            &MutationRequest {
                action: ReviewAction::Cancel,
                round: None,
                summary: "no live participant".into(),
                new_max_rounds: None,
            },
        );
        assert!(matches!(cancel, Err(ReviewError::Regular(_))));
        assert_eq!(
            get_run(&db, &outcome.run.id).unwrap().unwrap().state,
            ReviewState::AwaitingReview
        );
    }

    #[test]
    fn extend_changes_total_without_sending_then_allows_fixed() {
        let (_dir, db) = test_db();
        let (developer, reviewer, outcome) = started(&db, 1);
        let id = outcome.run.id;
        mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "needs another pass".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        let events_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let extended = mutate_review(
            &db,
            &developer,
            &id,
            &MutationRequest {
                action: ReviewAction::Extend,
                round: None,
                summary: String::new(),
                new_max_rounds: Some(3),
            },
        )
        .unwrap();
        assert_eq!(extended.run.state, ReviewState::AwaitingDeveloper);
        assert_eq!(extended.run.round, 1);
        assert_eq!(extended.run.max_rounds, 3);
        let events_after: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events_before, events_after);

        let fixed = mutate_review(
            &db,
            &developer,
            &id,
            &MutationRequest {
                action: ReviewAction::Fixed,
                round: Some(1),
                summary: "fixed after extension".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        assert_eq!(fixed.run.state, ReviewState::AwaitingReview);
        assert_eq!(fixed.run.round, 2);
    }

    #[test]
    fn changed_task_text_cannot_open_a_second_active_review() {
        let (_dir, db) = test_db();
        let (developer, reviewer, first) = started(&db, 3);
        let second = start_review(
            &db,
            &developer,
            &reviewer,
            "Same work, different wording",
            "/another/workspace",
            2,
        );
        match second {
            Err(ReviewError::Conflict(message)) => assert!(message.contains(&first.run.id)),
            other => panic!("expected active-review conflict, got {other:?}"),
        }
        let count: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM review_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn resume_with_same_session_continues_but_new_session_cannot_take_over() {
        let (_dir, db) = test_db();
        let (developer, reviewer, outcome) = started(&db, 3);
        let id = outcome.run.id;
        mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "fix this".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();

        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev1'", [])
            .unwrap();
        add_agent(&db, "dev1", &developer.session_id);
        let continued = mutate_review(
            &db,
            &developer,
            &id,
            &MutationRequest {
                action: ReviewAction::Fixed,
                round: Some(1),
                summary: "fixed after resume".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        assert_eq!(continued.run.round, 2);

        db.conn()
            .execute("DELETE FROM instances WHERE name = 'dev1'", [])
            .unwrap();
        add_agent(&db, "dev1", "different-session");
        let takeover = mutate_review(
            &db,
            &ReviewActor {
                name: "dev1".into(),
                session_id: "different-session".into(),
            },
            &id,
            &MutationRequest {
                action: ReviewAction::Cancel,
                round: Some(2),
                summary: "take over".into(),
                new_max_rounds: None,
            },
        );
        assert!(matches!(takeover, Err(ReviewError::Regular(_))));
    }

    #[test]
    fn transition_failure_rolls_back_message_state_and_audit() {
        let (_dir, db) = test_db();
        let (_developer, reviewer, outcome) = started(&db, 3);
        let before_events: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_review_transition
                 BEFORE INSERT ON review_transitions
                 WHEN NEW.action = 'request_changes'
                 BEGIN
                     SELECT RAISE(ABORT, 'injected transition failure');
                 END;",
            )
            .unwrap();

        let result = mutate_review(
            &db,
            &reviewer,
            &outcome.run.id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "will roll back".into(),
                new_max_rounds: None,
            },
        );
        assert!(matches!(result, Err(ReviewError::Regular(_))));
        let run = get_run(&db, &outcome.run.id).unwrap().unwrap();
        assert_eq!(run.state, ReviewState::AwaitingReview);
        assert_eq!(run.version, 0);
        let after_events: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before_events, after_events);
        let transitions: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM review_transitions WHERE workflow_id = ?1",
                params![outcome.run.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(transitions, 1);
    }

    #[test]
    fn concurrent_starts_create_exactly_one_workflow() {
        let (dir, db) = test_db();
        let path = dir.path().join("hcom.db");
        let (developer, reviewer) = actors();
        add_agent(&db, &developer.name, &developer.session_id);
        add_agent(&db, &reviewer.name, &reviewer.session_id);
        drop(db);

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|index| {
                let path = path.clone();
                let barrier = barrier.clone();
                let developer = developer.clone();
                let reviewer = reviewer.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&path).unwrap();
                    barrier.wait();
                    start_review(
                        &db,
                        &developer,
                        &reviewer,
                        &format!("wording {index}"),
                        "/tmp/workspace",
                        3,
                    )
                })
            })
            .collect();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ReviewError::Conflict(_))))
                .count(),
            1
        );
        let db = HcomDb::open_at(&path).unwrap();
        let count: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM review_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn late_lgtm_and_fixed_concurrency_has_one_winner() {
        let (dir, db) = test_db();
        let path = dir.path().join("hcom.db");
        let (developer, reviewer, outcome) = started(&db, 3);
        let id = outcome.run.id;
        mutate_review(
            &db,
            &reviewer,
            &id,
            &MutationRequest {
                action: ReviewAction::RequestChanges,
                round: Some(1),
                summary: "finding".into(),
                new_max_rounds: None,
            },
        )
        .unwrap();
        drop(db);

        let barrier = Arc::new(Barrier::new(2));
        let fixed = {
            let path = path.clone();
            let barrier = barrier.clone();
            let id = id.clone();
            std::thread::spawn(move || {
                let db = HcomDb::open_at(&path).unwrap();
                barrier.wait();
                mutate_review(
                    &db,
                    &developer,
                    &id,
                    &MutationRequest {
                        action: ReviewAction::Fixed,
                        round: Some(1),
                        summary: "fixed".into(),
                        new_max_rounds: None,
                    },
                )
            })
        };
        let lgtm = {
            let path = path.clone();
            let barrier = barrier.clone();
            let id = id.clone();
            std::thread::spawn(move || {
                let db = HcomDb::open_at(&path).unwrap();
                barrier.wait();
                mutate_review(
                    &db,
                    &reviewer,
                    &id,
                    &MutationRequest {
                        action: ReviewAction::Lgtm,
                        round: Some(1),
                        summary: "withdrawn".into(),
                        new_max_rounds: None,
                    },
                )
            })
        };
        let results = [fixed.join().unwrap(), lgtm.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ReviewError::Conflict(_))))
                .count(),
            1
        );
        let db = HcomDb::open_at(&path).unwrap();
        let run = get_run(&db, &id).unwrap().unwrap();
        assert!(
            (run.state == ReviewState::Approved && run.round == 1)
                || (run.state == ReviewState::AwaitingReview && run.round == 2)
        );
    }
}
