//! `hcom review` — deterministic developer/reviewer workflow commands.

use clap::{ArgGroup, Args, Parser, Subcommand};
use serde_json::json;

use crate::db::HcomDb;
use crate::identity;
use crate::review::{
    DEFAULT_MAX_ROUNDS, MutationRequest, ReviewAction, ReviewActor, ReviewError, ReviewOutcome,
    ReviewRun, actor_role, get_run, list_runs, mutate_review, start_review,
};
use crate::shared::{CommandContext, SenderKind};

const REVIEW_AFTER_HELP: &str = "\
Examples:
  hcom review start @dev2 --max-rounds 3 -- 'Review the implementation and tests'
  hcom review verdict rv-1234abcd --round 1 --request-changes -- 'Fix the race'
  hcom review fixed rv-1234abcd --round 1 -- 'Fixed and tested'
  hcom review verdict rv-1234abcd --round 2 --lgtm -- 'LGTM'

Only these structured commands change review state. Ordinary message text does not.";

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

fn print_mutation(action: ReviewAction, outcome: &ReviewOutcome) {
    let replay = if outcome.replayed { " (replayed)" } else { "" };
    let run = &outcome.run;
    match action {
        ReviewAction::RequestChanges => {
            println!(
                "Recorded request changes for {} round {}/{}; state={}{}.",
                run.id, run.round, run.max_rounds, run.state, replay
            );
            if matches!(run.state.as_str(), "awaiting_developer" | "max_rounds") {
                println!(
                    "While the workflow remains on this round, you may withdraw the finding with:\n  hcom review verdict {} --round {} --lgtm --name {} -- '<summary>'",
                    run.id, run.round, run.reviewer_name
                );
            }
        }
        ReviewAction::Lgtm => println!(
            "Approved {} at round {}/{}{}.",
            run.id, run.round, run.max_rounds, replay
        ),
        ReviewAction::Fixed | ReviewAction::Rebut => println!(
            "Submitted {} for {}; state={} round={}/{}{}.",
            action.as_str(),
            run.id,
            run.state,
            run.round,
            run.max_rounds,
            replay
        ),
        ReviewAction::Cancel => println!("Canceled {}{}.", run.id, replay),
        ReviewAction::Extend => {
            println!(
                "Extended {} to {} rounds; state={}{}.",
                run.id, run.max_rounds, run.state, replay
            );
            println!(
                "Next:\n  hcom review fixed {} --round {} --name {} -- '<what changed>'\n  hcom review rebut {} --round {} --name {} -- '<why no change>'",
                run.id, run.round, run.developer_name, run.id, run.round, run.developer_name
            );
        }
        ReviewAction::Start => unreachable!(),
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
                    println!(
                        "Started {} reviewer=@{} state={} round={}/{}.",
                        outcome.run.id,
                        outcome.run.reviewer_name,
                        outcome.run.state,
                        outcome.run.round,
                        outcome.run.max_rounds
                    );
                    println!("Thread: {}", outcome.run.thread);
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
                    print_mutation(action, &outcome);
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
                    print_mutation(review_action, &outcome);
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
                print_mutation(ReviewAction::Cancel, &outcome);
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
                print_mutation(ReviewAction::Extend, &outcome);
                0
            }
            Err(error) => print_error(error),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
}
