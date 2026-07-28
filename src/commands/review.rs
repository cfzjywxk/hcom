//! `hcom review` — deterministic developer/reviewer workflow commands.

use clap::{ArgGroup, Args, Parser, Subcommand};
use serde_json::json;
use std::io::Write;
use std::time::Duration;

use crate::db::HcomDb;
use crate::identity;
use crate::review::{
    DEFAULT_MAX_ROUNDS, MutationRequest, ReviewAction, ReviewActor, ReviewError, ReviewOutcome,
    ReviewRun, ReviewState, actor_role, get_run, list_runs, mutate_review, start_review,
};
use crate::shared::{CommandContext, SenderKind};

const REVIEW_AFTER_HELP: &str = "\
Examples:
  hcom review start @dev2 --max-rounds 3 -- 'Review the implementation and tests'
  hcom review verdict rv-1234abcd --round 1 --request-changes -- 'Fix the race'
  hcom review fixed rv-1234abcd --round 1 -- 'Fixed and tested'
  hcom review verdict rv-1234abcd --round 2 --lgtm -- 'LGTM'

Only these structured commands change review state. Ordinary message text does not.";

const REVIEW_QUIET_WAIT: &str = "The peer notification is queued for automatic delivery. End your turn now; hcom will wake you if another [hcom-review] message arrives. Do not run any hcom command—including `hcom status`, `hcom review status`, `hcom events`, `hcom listen`, or `hcom send`—merely to check progress.";
const ATTACHED_REVIEW_WAIT: &str = "The peer notification is queued. This hcom review command remains attached until the peer advances the durable workflow; keep waiting on this same foreground tool process and do not submit another user prompt or run a polling command.";
const ATTACHED_MAX_ROUNDS_WAIT: &str = "The peer notification is queued. This hcom review command remains attached across a developer extension until the peer resubmits or ends the durable workflow. To withdraw the finding with the late-LGTM command shown above, first interrupt only this foreground waiter with Ctrl-C; the durable max_rounds state remains intact.";
const REVIEW_OBSERVER_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Parser, Debug)]
#[command(
    name = "review",
    about = "Run a persistent review/fix/re-review loop",
    after_help = REVIEW_AFTER_HELP
)]
pub struct ReviewArgs {
    #[command(subcommand)]
    pub command: ReviewCommand,
}

#[derive(Subcommand, Debug)]
pub enum ReviewCommand {
    /// Start a review loop with one reviewer
    Start(ReviewStartArgs),
    /// Submit a structured reviewer verdict
    Verdict(ReviewVerdictArgs),
    /// Declare requested changes fixed and request the next review
    Fixed(ReviewRoundSummaryArgs),
    /// Rebut requested changes without modifying code and request the next review
    Rebut(ReviewRoundSummaryArgs),
    /// Show one review workflow
    Status(ReviewStatusArgs),
    /// List active review workflows for the current agent
    List(ReviewListArgs),
    /// Cancel a non-final review workflow
    Cancel(ReviewCancelArgs),
    /// Increase the total round limit after max rounds is reached
    Extend(ReviewExtendArgs),
}

#[derive(Args, Debug)]
pub struct ReviewStartArgs {
    /// Exact local reviewer target, including leading @
    pub reviewer: String,
    /// Maximum number of reviewer verdict rounds
    #[arg(long, default_value_t = DEFAULT_MAX_ROUNDS)]
    pub max_rounds: i64,
    /// Review task text after --
    #[arg(last = true, required = true)]
    pub task: Vec<String>,
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("verdict")
        .required(true)
        .multiple(false)
        .args(["lgtm", "request_changes"])
))]
pub struct ReviewVerdictArgs {
    pub id: String,
    #[arg(long)]
    pub round: i64,
    #[arg(long)]
    pub lgtm: bool,
    #[arg(long)]
    pub request_changes: bool,
    /// Verdict summary after --
    #[arg(last = true, required = true)]
    pub summary: Vec<String>,
}

#[derive(Args, Debug)]
pub struct ReviewRoundSummaryArgs {
    pub id: String,
    #[arg(long)]
    pub round: i64,
    /// Action summary after --
    #[arg(last = true, required = true)]
    pub summary: Vec<String>,
}

#[derive(Args, Debug)]
pub struct ReviewStatusArgs {
    pub id: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct ReviewListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct ReviewCancelArgs {
    pub id: String,
    /// Cancellation reason after --
    #[arg(last = true, required = true)]
    pub reason: Vec<String>,
}

#[derive(Args, Debug)]
pub struct ReviewExtendArgs {
    pub id: String,
    /// New total number of review rounds (not an increment)
    #[arg(long)]
    pub max_rounds: i64,
}

fn actor_from_ctx(ctx: Option<&CommandContext>) -> Result<ReviewActor, ReviewError> {
    let identity = ctx
        .and_then(|context| context.identity.as_ref())
        .ok_or_else(|| ReviewError::Regular("Review requires an hcom agent identity".into()))?;
    if identity.kind != SenderKind::Instance {
        return Err(ReviewError::Regular(
            "Review only supports registered hcom agent instances".into(),
        ));
    }
    let instance = identity
        .instance_data
        .as_ref()
        .ok_or_else(|| ReviewError::Regular("Review requires a live registered instance".into()))?;
    let tool = instance.get("tool").and_then(|value| value.as_str());
    let is_subagent = instance
        .get("parent_name")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty());
    let is_remote = instance
        .get("origin_device_id")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty());
    if is_subagent || is_remote || !matches!(tool, Some("claude" | "codex")) {
        return Err(ReviewError::Regular(
            "Review only supports local top-level Claude/Codex instances".into(),
        ));
    }
    let session_id = identity
        .session_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ReviewError::Regular(
                "Review requires a top-level Claude/Codex instance with a session_id".into(),
            )
        })?;
    Ok(ReviewActor {
        name: identity.name.clone(),
        session_id: session_id.to_string(),
    })
}

fn resolve_reviewer(db: &HcomDb, target: &str) -> Result<ReviewActor, ReviewError> {
    let Some(target) = target.strip_prefix('@') else {
        return Err(ReviewError::Regular(
            "Reviewer must be an exact @instance target".into(),
        ));
    };
    if target.is_empty() || target.contains(':') || target.ends_with('-') {
        return Err(ReviewError::Regular(
            "Reviewer must be one exact local instance; remote and tag fan-out targets are unsupported"
                .into(),
        ));
    }
    let base = identity::resolve_display_name(db, target).ok_or_else(|| {
        ReviewError::Regular(format!(
            "Reviewer '@{target}' does not match a live instance"
        ))
    })?;
    let row = db
        .get_instance_full(&base)
        .map_err(|e| ReviewError::Regular(format!("Database error: {e}")))?
        .ok_or_else(|| ReviewError::Regular(format!("Reviewer '@{target}' not found")))?;
    let full_name = identity::get_full_name(&row);
    if target != row.name && target != full_name {
        return Err(ReviewError::Regular(
            "Reviewer target must be an exact base or full display name".into(),
        ));
    }
    let session_id = row.session_id.clone().ok_or_else(|| {
        ReviewError::Regular(format!(
            "Reviewer '@{target}' has no session_id; only top-level Claude/Codex instances are supported"
        ))
    })?;
    Ok(ReviewActor {
        name: row.name,
        session_id,
    })
}

fn join_text(parts: &[String]) -> String {
    parts.join(" ")
}

fn run_json(run: &ReviewRun) -> serde_json::Value {
    json!({
        "id": run.id,
        "task": run.task,
        "workspace": run.workspace,
        "thread": run.thread,
        "developer": {
            "name": run.developer_name,
            "session_id": run.developer_session_id,
        },
        "reviewer": {
            "name": run.reviewer_name,
            "session_id": run.reviewer_session_id,
        },
        "state": run.state.as_str(),
        "round": run.round,
        "max_rounds": run.max_rounds,
        "version": run.version,
        "last_message_event_id": run.last_message_event_id,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    })
}

fn print_run(run: &ReviewRun) {
    println!(
        "{} state={} round={}/{} version={}",
        run.id, run.state, run.round, run.max_rounds, run.version
    );
    println!(
        "developer=@{} reviewer=@{}",
        run.developer_name, run.reviewer_name
    );
    println!("workspace={}", run.workspace);
    println!("thread={}", run.thread);
    println!("task={}", run.task);
}

fn print_error(error: ReviewError) -> i32 {
    let exit = error.exit_code();
    eprintln!("Error: {error}");
    exit
}

fn mutate(
    db: &HcomDb,
    actor: &ReviewActor,
    id: &str,
    request: MutationRequest,
) -> Result<ReviewOutcome, ReviewError> {
    mutate_review(db, actor, id, &request)
}

fn mutation_output(
    action: ReviewAction,
    outcome: &ReviewOutcome,
    attached_observer: bool,
) -> String {
    let replay = if outcome.replayed { " (replayed)" } else { "" };
    let run = &outcome.run;
    let wait_message = if attached_observer && run.state == ReviewState::MaxRounds {
        ATTACHED_MAX_ROUNDS_WAIT
    } else if attached_observer {
        ATTACHED_REVIEW_WAIT
    } else {
        REVIEW_QUIET_WAIT
    };
    match action {
        ReviewAction::RequestChanges => {
            let mut output = format!(
                "Recorded request changes for {} round {}/{}; state={}{}.",
                run.id, run.round, run.max_rounds, run.state, replay
            );
            if matches!(run.state.as_str(), "awaiting_developer" | "max_rounds") {
                output.push_str(&format!(
                    "\nWhile the workflow remains on this round, you may withdraw the finding with:\n  hcom review verdict {} --round {} --lgtm --name {} -- '<summary>'",
                    run.id, run.round, run.reviewer_name
                ));
                output.push_str(&format!("\n{wait_message}"));
            }
            output
        }
        ReviewAction::Lgtm => format!(
            "Approved {} at round {}/{}{}.",
            run.id, run.round, run.max_rounds, replay
        ),
        ReviewAction::Fixed | ReviewAction::Rebut => format!(
            "Submitted {} for {}; state={} round={}/{}{}.\n{}",
            action.as_str(),
            run.id,
            run.state,
            run.round,
            run.max_rounds,
            replay,
            wait_message
        ),
        ReviewAction::Cancel => format!("Canceled {}{}.", run.id, replay),
        ReviewAction::Extend => {
            format!(
                "Extended {} to {} rounds; state={}{}.\nNext:\n  hcom review fixed {} --round {} --name {} -- '<what changed>'\n  hcom review rebut {} --round {} --name {} -- '<why no change>'",
                run.id,
                run.max_rounds,
                run.state,
                replay,
                run.id,
                run.round,
                run.developer_name,
                run.id,
                run.round,
                run.developer_name
            )
        }
        ReviewAction::Start => unreachable!(),
    }
}

fn start_output(outcome: &ReviewOutcome, attached_observer: bool) -> String {
    let wait_message = if attached_observer {
        ATTACHED_REVIEW_WAIT
    } else {
        REVIEW_QUIET_WAIT
    };
    format!(
        "Started {} reviewer=@{} state={} round={}/{}.\nThread: {}\n{}",
        outcome.run.id,
        outcome.run.reviewer_name,
        outcome.run.state,
        outcome.run.round,
        outcome.run.max_rounds,
        outcome.run.thread,
        wait_message
    )
}

fn print_mutation(action: ReviewAction, outcome: &ReviewOutcome, attached_observer: bool) {
    println!("{}", mutation_output(action, outcome, attached_observer));
}

fn actor_waits_for_peer_run(actor: &ReviewActor, run: &ReviewRun) -> bool {
    match run.state {
        ReviewState::AwaitingReview => {
            run.developer_name == actor.name && run.developer_session_id == actor.session_id
        }
        ReviewState::AwaitingDeveloper | ReviewState::MaxRounds => {
            run.reviewer_name == actor.name && run.reviewer_session_id == actor.session_id
        }
        ReviewState::Approved | ReviewState::Canceled => false,
    }
}

fn actor_waits_for_peer(actor: &ReviewActor, outcome: &ReviewOutcome) -> bool {
    actor_waits_for_peer_run(actor, &outcome.run)
}

fn current_process_has_delivery_binding(db: &HcomDb, actor: &ReviewActor) -> bool {
    std::env::var("HCOM_PROCESS_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|process_id| db.get_process_binding(&process_id).ok().flatten())
        .as_deref()
        == Some(actor.name.as_str())
}

fn should_attach_review_observer_with(
    actor: &ReviewActor,
    outcome: &ReviewOutcome,
    current_process_has_delivery: bool,
) -> bool {
    actor_waits_for_peer(actor, outcome) && !current_process_has_delivery
}

fn should_attach_review_observer(
    db: &HcomDb,
    _ctx: Option<&CommandContext>,
    actor: &ReviewActor,
    outcome: &ReviewOutcome,
) -> bool {
    should_attach_review_observer_with(
        actor,
        outcome,
        current_process_has_delivery_binding(db, actor),
    )
}

fn wait_until_actor_turn_with<L, W>(
    initial: &ReviewRun,
    actor: &ReviewActor,
    mut load: L,
    mut wait: W,
) -> Result<ReviewRun, ReviewError>
where
    L: FnMut() -> Result<Option<ReviewRun>, ReviewError>,
    W: FnMut(),
{
    let mut observed = initial.clone();
    loop {
        let current = load()?.ok_or_else(|| {
            ReviewError::Regular(format!(
                "Review workflow '{}' disappeared while its foreground observer was attached",
                initial.id
            ))
        })?;
        if current.developer_name != initial.developer_name
            || current.developer_session_id != initial.developer_session_id
            || current.reviewer_name != initial.reviewer_name
            || current.reviewer_session_id != initial.reviewer_session_id
        {
            return Err(ReviewError::Conflict(format!(
                "REVIEW_CONFLICT {} participant identity changed while waiting",
                initial.id
            )));
        }
        if current.version < observed.version
            || (current.version == observed.version && current.state != observed.state)
        {
            return Err(ReviewError::Conflict(format!(
                "REVIEW_CONFLICT {} state/version regressed while waiting",
                initial.id
            )));
        }
        if current.version > observed.version || current.state != observed.state {
            if !actor_waits_for_peer_run(actor, &current) {
                return Ok(current);
            }
            observed = current;
        } else if !actor_waits_for_peer_run(actor, &current) {
            return Ok(current);
        }
        wait();
    }
}

fn attach_review_observer(db: &HcomDb, actor: &ReviewActor, outcome: &ReviewOutcome) {
    let _ = std::io::stdout().flush();
    match wait_until_actor_turn_with(
        &outcome.run,
        actor,
        || get_run(db, &outcome.run.id),
        || std::thread::sleep(REVIEW_OBSERVER_POLL_INTERVAL),
    ) {
        Ok(current) => println!(
            "Review {} advanced to state={} round={}/{} version={}; resuming the current tool turn.",
            current.id, current.state, current.round, current.max_rounds, current.version
        ),
        Err(error) => eprintln!(
            "[hcom] The durable review transition succeeded, but its foreground observer detached: {error}. Resume with `hcom review status {}`.",
            outcome.run.id
        ),
    }
}

pub fn cmd_review(db: &HcomDb, args: &ReviewArgs, ctx: Option<&CommandContext>) -> i32 {
    let actor = match actor_from_ctx(ctx) {
        Ok(actor) => actor,
        Err(error) => return print_error(error),
    };

    match &args.command {
        ReviewCommand::Start(start) => {
            let reviewer = match resolve_reviewer(db, &start.reviewer) {
                Ok(reviewer) => reviewer,
                Err(error) => return print_error(error),
            };
            let workspace = match std::env::current_dir().and_then(std::fs::canonicalize) {
                Ok(path) => path.to_string_lossy().to_string(),
                Err(error) => {
                    return print_error(ReviewError::Regular(format!(
                        "Cannot resolve current workspace: {error}"
                    )));
                }
            };
            match start_review(
                db,
                &actor,
                &reviewer,
                &join_text(&start.task),
                &workspace,
                start.max_rounds,
            ) {
                Ok(outcome) => {
                    let observe = should_attach_review_observer(db, ctx, &actor, &outcome);
                    println!("{}", start_output(&outcome, observe));
                    if observe {
                        attach_review_observer(db, &actor, &outcome);
                    }
                    0
                }
                Err(error) => print_error(error),
            }
        }
        ReviewCommand::Verdict(verdict) => {
            let action = if verdict.lgtm {
                ReviewAction::Lgtm
            } else {
                ReviewAction::RequestChanges
            };
            match mutate(
                db,
                &actor,
                &verdict.id,
                MutationRequest {
                    action,
                    round: Some(verdict.round),
                    summary: join_text(&verdict.summary),
                    new_max_rounds: None,
                },
            ) {
                Ok(outcome) => {
                    let observe = should_attach_review_observer(db, ctx, &actor, &outcome);
                    print_mutation(action, &outcome, observe);
                    if observe {
                        attach_review_observer(db, &actor, &outcome);
                    }
                    0
                }
                Err(error) => print_error(error),
            }
        }
        ReviewCommand::Fixed(action) | ReviewCommand::Rebut(action) => {
            let review_action = if matches!(&args.command, ReviewCommand::Fixed(_)) {
                ReviewAction::Fixed
            } else {
                ReviewAction::Rebut
            };
            match mutate(
                db,
                &actor,
                &action.id,
                MutationRequest {
                    action: review_action,
                    round: Some(action.round),
                    summary: join_text(&action.summary),
                    new_max_rounds: None,
                },
            ) {
                Ok(outcome) => {
                    let observe = should_attach_review_observer(db, ctx, &actor, &outcome);
                    print_mutation(review_action, &outcome, observe);
                    if observe {
                        attach_review_observer(db, &actor, &outcome);
                    }
                    0
                }
                Err(error) => print_error(error),
            }
        }
        ReviewCommand::Status(status) => match get_run(db, &status.id) {
            Ok(Some(run)) => {
                if let Err(error) = actor_role(&run, &actor) {
                    return print_error(error);
                }
                if status.json {
                    println!("{}", run_json(&run));
                } else {
                    print_run(&run);
                }
                0
            }
            Ok(None) => print_error(ReviewError::Regular(format!(
                "Review workflow '{}' not found",
                status.id
            ))),
            Err(error) => print_error(error),
        },
        ReviewCommand::List(list) => match list_runs(db, &actor.session_id) {
            Ok(runs) => {
                if list.json {
                    let values: Vec<_> = runs.iter().map(run_json).collect();
                    println!("{}", json!(values));
                } else if runs.is_empty() {
                    println!("No active review workflows");
                } else {
                    for run in &runs {
                        println!(
                            "{} state={} round={}/{} developer=@{} reviewer=@{} task={}",
                            run.id,
                            run.state,
                            run.round,
                            run.max_rounds,
                            run.developer_name,
                            run.reviewer_name,
                            run.task
                        );
                    }
                }
                0
            }
            Err(error) => print_error(error),
        },
        ReviewCommand::Cancel(cancel) => match mutate(
            db,
            &actor,
            &cancel.id,
            MutationRequest {
                action: ReviewAction::Cancel,
                round: None,
                summary: join_text(&cancel.reason),
                new_max_rounds: None,
            },
        ) {
            Ok(outcome) => {
                print_mutation(ReviewAction::Cancel, &outcome, false);
                0
            }
            Err(error) => print_error(error),
        },
        ReviewCommand::Extend(extend) => match mutate(
            db,
            &actor,
            &extend.id,
            MutationRequest {
                action: ReviewAction::Extend,
                round: None,
                summary: String::new(),
                new_max_rounds: Some(extend.max_rounds),
            },
        ) {
            Ok(outcome) => {
                print_mutation(ReviewAction::Extend, &outcome, false);
                0
            }
            Err(error) => print_error(error),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review::ReviewState;
    use clap::Parser;

    fn outcome(state: ReviewState) -> ReviewOutcome {
        let round = if state == ReviewState::MaxRounds {
            3
        } else {
            1
        };
        ReviewOutcome {
            run: ReviewRun {
                id: "rv-test".into(),
                task: "Review implementation".into(),
                workspace: "/tmp/workspace".into(),
                thread: "review-rv-test".into(),
                developer_name: "dev1".into(),
                developer_session_id: "session-dev1".into(),
                reviewer_name: "dev2".into(),
                reviewer_session_id: "session-dev2".into(),
                state,
                round,
                max_rounds: 3,
                version: 0,
                last_message_event_id: None,
                created_at: 0.0,
                updated_at: 0.0,
            },
            replayed: false,
        }
    }

    fn developer_actor() -> ReviewActor {
        ReviewActor {
            name: "dev1".into(),
            session_id: "session-dev1".into(),
        }
    }

    fn reviewer_actor() -> ReviewActor {
        ReviewActor {
            name: "dev2".into(),
            session_id: "session-dev2".into(),
        }
    }

    fn assert_quiet_wait(output: &str) {
        assert!(output.contains("queued for automatic delivery"));
        assert!(output.contains("End your turn now"));
        for command in [
            "`hcom status`",
            "`hcom review status`",
            "`hcom events`",
            "`hcom listen`",
            "`hcom send`",
        ] {
            assert!(output.contains(command), "missing command guard: {command}");
        }
    }

    #[test]
    fn parses_natural_language_protocol_commands() {
        let start = ReviewArgs::try_parse_from([
            "review",
            "start",
            "@dev2",
            "--max-rounds",
            "3",
            "--",
            "Review implementation",
        ])
        .unwrap();
        assert!(matches!(start.command, ReviewCommand::Start(_)));

        let verdict = ReviewArgs::try_parse_from([
            "review",
            "verdict",
            "rv-1234abcd",
            "--round",
            "1",
            "--lgtm",
            "--",
            "Looks good",
        ])
        .unwrap();
        assert!(matches!(verdict.command, ReviewCommand::Verdict(_)));
    }

    #[test]
    fn verdict_flags_are_mutually_exclusive() {
        assert!(
            ReviewArgs::try_parse_from([
                "review",
                "verdict",
                "rv-1234abcd",
                "--round",
                "1",
                "--lgtm",
                "--request-changes",
                "--",
                "summary",
            ])
            .is_err()
        );
    }

    #[test]
    fn peer_handoffs_end_the_turn_without_polling() {
        let awaiting_review = outcome(ReviewState::AwaitingReview);
        assert_quiet_wait(&start_output(&awaiting_review, false));
        assert_quiet_wait(&mutation_output(
            ReviewAction::Fixed,
            &awaiting_review,
            false,
        ));
        assert_quiet_wait(&mutation_output(
            ReviewAction::Rebut,
            &awaiting_review,
            false,
        ));

        let awaiting_developer = outcome(ReviewState::AwaitingDeveloper);
        assert_quiet_wait(&mutation_output(
            ReviewAction::RequestChanges,
            &awaiting_developer,
            false,
        ));

        let max_rounds = outcome(ReviewState::MaxRounds);
        assert_quiet_wait(&mutation_output(
            ReviewAction::RequestChanges,
            &max_rounds,
            false,
        ));
    }

    #[test]
    fn attached_observer_output_keeps_the_same_foreground_tool() {
        let awaiting_review = outcome(ReviewState::AwaitingReview);
        for output in [
            start_output(&awaiting_review, true),
            mutation_output(ReviewAction::Fixed, &awaiting_review, true),
            mutation_output(ReviewAction::Rebut, &awaiting_review, true),
            mutation_output(
                ReviewAction::RequestChanges,
                &outcome(ReviewState::AwaitingDeveloper),
                true,
            ),
        ] {
            assert!(output.contains("remains attached"));
            assert!(!output.contains("End your turn now"));
            assert!(output.contains("polling command"));
        }
    }

    #[test]
    fn observer_policy_preserves_async_delivery_and_covers_hook_only_callers() {
        let awaiting_review = outcome(ReviewState::AwaitingReview);
        assert!(
            should_attach_review_observer_with(&developer_actor(), &awaiting_review, false),
            "a hook-only developer has no transport that can wake a new turn"
        );
        assert!(
            !should_attach_review_observer_with(&developer_actor(), &awaiting_review, true),
            "an ordinary hcom-launched agent retains asynchronous delivery"
        );

        let awaiting_developer = outcome(ReviewState::AwaitingDeveloper);
        assert!(should_attach_review_observer_with(
            &reviewer_actor(),
            &awaiting_developer,
            false
        ));
        assert!(should_attach_review_observer_with(
            &reviewer_actor(),
            &outcome(ReviewState::MaxRounds),
            false
        ));
        assert!(!should_attach_review_observer_with(
            &developer_actor(),
            &awaiting_developer,
            false
        ));
        assert!(!should_attach_review_observer_with(
            &reviewer_actor(),
            &outcome(ReviewState::Approved),
            false
        ));
    }

    #[test]
    fn max_rounds_attached_output_preserves_the_late_lgtm_escape() {
        let output = mutation_output(
            ReviewAction::RequestChanges,
            &outcome(ReviewState::MaxRounds),
            true,
        );
        assert!(output.contains("late-LGTM command shown above"));
        assert!(output.contains("Ctrl-C"));
        assert!(output.contains("durable max_rounds state remains intact"));
    }

    #[test]
    fn observer_rechecks_state_before_waiting_to_close_lost_wake() {
        let initial = outcome(ReviewState::AwaitingReview).run;
        let mut advanced = initial.clone();
        advanced.state = ReviewState::Approved;
        advanced.version = initial.version + 1;
        let observed = wait_until_actor_turn_with(
            &initial,
            &developer_actor(),
            || Ok(Some(advanced.clone())),
            || panic!("observer waited after the durable verdict was already visible"),
        )
        .unwrap();
        assert_eq!(observed.state, ReviewState::Approved);
        assert_eq!(observed.version, 1);
    }

    #[test]
    fn observer_ignores_unchanged_reads_then_returns_on_peer_transition() {
        let initial = outcome(ReviewState::AwaitingReview).run;
        let mut reads = 0;
        let mut waits = 0;
        let observed = wait_until_actor_turn_with(
            &initial,
            &developer_actor(),
            || {
                reads += 1;
                let mut current = initial.clone();
                if reads >= 3 {
                    current.state = ReviewState::AwaitingDeveloper;
                    current.version += 1;
                }
                Ok(Some(current))
            },
            || waits += 1,
        )
        .unwrap();
        assert_eq!(reads, 3);
        assert_eq!(waits, 2);
        assert_eq!(observed.state, ReviewState::AwaitingDeveloper);
    }

    #[test]
    fn reviewer_observer_waits_while_awaiting_the_developer() {
        let initial = outcome(ReviewState::AwaitingDeveloper).run;
        let mut reads = 0;
        let observed = wait_until_actor_turn_with(
            &initial,
            &reviewer_actor(),
            || {
                reads += 1;
                let mut current = initial.clone();
                if reads >= 2 {
                    current.state = ReviewState::AwaitingReview;
                    current.version += 1;
                }
                Ok(Some(current))
            },
            || {},
        )
        .unwrap();
        assert_eq!(reads, 2);
        assert_eq!(observed.state, ReviewState::AwaitingReview);
    }

    #[test]
    fn reviewer_observer_survives_max_rounds_extend_until_fixed() {
        let initial = outcome(ReviewState::MaxRounds).run;
        let mut reads = 0;
        let mut waits = 0;
        let observed = wait_until_actor_turn_with(
            &initial,
            &reviewer_actor(),
            || {
                reads += 1;
                let mut current = initial.clone();
                if reads == 2 {
                    current.state = ReviewState::AwaitingDeveloper;
                    current.max_rounds += 1;
                    current.version += 1;
                } else if reads >= 3 {
                    current.state = ReviewState::AwaitingReview;
                    current.round += 1;
                    current.max_rounds += 1;
                    current.version += 2;
                }
                Ok(Some(current))
            },
            || waits += 1,
        )
        .unwrap();
        assert_eq!(reads, 3);
        assert_eq!(waits, 2);
        assert_eq!(observed.state, ReviewState::AwaitingReview);
        assert_eq!(observed.round, initial.round + 1);
    }
}
