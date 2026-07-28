//! `hcom handoff` — typed same-terminal handoff state transitions.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use serde_json::json;

use crate::db::HcomDb;
use crate::handoff::{
    HandoffActor, HandoffError, HandoffInspection, HandoffOutcome, MAX_HANDOFF_BUNDLE_BYTES,
    MAX_INSTRUCTIONS_BYTES, MAX_STATUS_HUMAN_BYTES, MAX_STATUS_JSON_BYTES, ManagedActorMarkers,
    TerminalHandoff, abort_handoff, accept_handoff, commit_handoff, current_chain_for_actor,
    effective_handoff_target_generation, handoff_status_for_actor,
    handoff_status_for_terminal_owner, inspect_handoff, prepare_handoff, reject_handoff,
    resolve_managed_actor, uses_bundle_repository_snapshot,
};
use crate::shared::{CommandContext, SenderKind};

const HANDOFF_AFTER_HELP: &str = "\
Mutations are accepted only from the exact supervisor-owned Codex generation.
Human-shell status is read-only and limited to the owning foreground terminal.
Start and crash recovery are exposed only through `hcom chain`.

Examples:
  hcom handoff prepare --bundle-event 123 --json
  hcom handoff commit ho-0123456789abcdef --version 0
  hcom handoff abort ho-0123456789abcdef --version 0 -- 'no longer needed'
  hcom handoff status ho-0123456789abcdef --json
  hcom handoff inspect ho-0123456789abcdef --version 5 --json
  hcom handoff accept ho-0123456789abcdef --version 6
  hcom handoff reject ho-0123456789abcdef --version 5 -- 'workspace mismatch'";

#[derive(Parser, Debug)]
#[command(
    name = "handoff",
    about = "Manage typed same-terminal handoff state",
    after_help = HANDOFF_AFTER_HELP
)]
pub struct HandoffArgs {
    #[command(subcommand)]
    pub command: HandoffCommand,
}

#[derive(Subcommand, Debug)]
pub enum HandoffCommand {
    /// Prepare a handoff from one exact numeric bundle event
    Prepare(HandoffPrepareArgs),
    /// Commit a prepared handoff at an expected version
    Commit(HandoffVersionArgs),
    /// Abort a prepared handoff
    Abort(HandoffReasonArgs),
    /// Show the current or selected handoff
    Status(HandoffStatusArgs),
    /// Read and validate a ready target's bundle and project instructions
    Inspect(HandoffVersionArgs),
    /// Explicitly accept a previously inspected target generation
    Accept(HandoffVersionArgs),
    /// Reject a ready target generation
    Reject(HandoffReasonArgs),
}

#[derive(Args, Debug)]
pub struct HandoffPrepareArgs {
    #[arg(long)]
    pub bundle_event: i64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct HandoffVersionArgs {
    pub id: String,
    #[arg(long)]
    pub version: i64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct HandoffReasonArgs {
    pub id: String,
    #[arg(long)]
    pub version: i64,
    #[arg(long)]
    pub json: bool,
    /// Sanitized bounded reason after --
    #[arg(last = true, required = true)]
    pub reason: Vec<String>,
}

#[derive(Args, Debug)]
pub struct HandoffStatusArgs {
    pub id: Option<String>,
    #[arg(long)]
    pub json: bool,
}

pub(crate) fn actor_from_ctx(
    db: &HcomDb,
    ctx: Option<&CommandContext>,
) -> Result<HandoffActor, HandoffError> {
    let identity = ctx
        .and_then(|context| context.identity.as_ref())
        .ok_or(HandoffError::NotManaged)?;
    if identity.kind != SenderKind::Instance {
        return Err(HandoffError::NotManaged);
    }
    let instance = identity
        .instance_data
        .as_ref()
        .ok_or(HandoffError::NotManaged)?;
    let is_top_level = instance
        .get("parent_name")
        .and_then(|value| value.as_str())
        .is_none_or(str::is_empty);
    let is_local = instance
        .get("origin_device_id")
        .and_then(|value| value.as_str())
        .is_none_or(str::is_empty);
    if instance.get("tool").and_then(|value| value.as_str()) != Some("codex")
        || !is_top_level
        || !is_local
    {
        return Err(HandoffError::NotManaged);
    }
    let session_id = identity
        .session_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(HandoffError::NotManaged)?;
    let process_id = std::env::var("HCOM_PROCESS_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(HandoffError::NotManaged)?;
    let chain_id = std::env::var("HCOM_CHAIN_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(HandoffError::NotManaged)?;
    let generation = std::env::var("HCOM_CHAIN_GENERATION")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .ok_or(HandoffError::NotManaged)?;
    let launch_nonce = std::env::var("HCOM_CHAIN_LAUNCH_NONCE")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(HandoffError::NotManaged)?;
    let process_birth_identity = std::env::var("HCOM_CHAIN_PROCESS_BIRTH_IDENTITY")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(HandoffError::NotManaged)?;
    hcom::chain_supervisor::verify_current_process_scope(&process_birth_identity)
        .map_err(|_| HandoffError::NotManaged)?;
    resolve_managed_actor(
        db,
        &identity.name,
        session_id,
        &process_id,
        &ManagedActorMarkers {
            chain_id,
            generation,
            launch_nonce,
            process_birth_identity,
        },
    )
}

pub(crate) fn managed_actor_from_ctx(
    db: &HcomDb,
    ctx: Option<&CommandContext>,
) -> Result<HandoffActor, HandoffError> {
    let actor = actor_from_ctx(db, ctx)?;
    match current_chain_for_actor(db, &actor)? {
        Some(_) => Ok(actor),
        None => Err(HandoffError::NotManaged),
    }
}

fn cwd() -> Result<PathBuf, HandoffError> {
    std::env::current_dir()
        .map_err(|_| HandoffError::Invalid("current launch directory is unavailable".to_string()))
}

fn handoff_json(handoff: &TerminalHandoff, replayed: Option<bool>) -> serde_json::Value {
    let bundle_repositories = uses_bundle_repository_snapshot(handoff);
    let mut value = json!({
        "id": handoff.id,
        "chain_id": handoff.chain_id,
        "state": handoff.state.as_str(),
        "version": handoff.version,
        "source_generation": handoff.source_generation,
        "target_generation": handoff.target_generation,
        "bundle": {
            "event_id": handoff.bundle_event_id,
            "digest": handoff.bundle_digest,
            "size_bytes": handoff.bundle_size_bytes,
        },
        "launch_cwd": handoff.workspace,
        "workspace": handoff.workspace,
        "snapshot_mode": if bundle_repositories {
            "bundle_repositories_v1"
        } else {
            "legacy_git_workspace"
        },
        "repository_manifest": if bundle_repositories {
            json!({
                "digest": handoff.revision,
                "summary": handoff.dirty_summary,
            })
        } else {
            serde_json::Value::Null
        },
        "revision": handoff.revision,
        "branch": handoff.branch,
        "dirty_summary": handoff.dirty_summary,
        "policy_ref": handoff.policy_ref,
        "failure": {
            "kind": handoff.failure_kind,
            "reason": handoff.failure_reason,
        },
        "quiesce_evidence": {
            "sigterm_requested_wall_at": handoff.sigterm_requested_wall_at,
            "sigterm_requested_monotonic_ns": handoff.sigterm_requested_monotonic_ns,
            "sigterm_request_result": handoff.sigterm_request_result,
            "child_exit_observed_wall_at": handoff.child_exit_observed_wall_at,
            "child_exit_observed_monotonic_ns": handoff.child_exit_observed_monotonic_ns,
            "exit_code": handoff.child_exit_code,
            "exit_signal": handoff.child_exit_signal,
            "sigterm_to_exit_ms": handoff.sigterm_to_exit_ms,
            "delivery_exit_context": handoff.delivery_exit_context,
            "waitpid_reaped": handoff.waitpid_reaped,
            "cleanup": {
                "inject": handoff.inject_cleanup_succeeded,
                "delivery": handoff.delivery_cleanup_succeeded,
                "pty": handoff.pty_cleanup_succeeded,
                "screen": handoff.screen_cleanup_succeeded,
                "write_queue": handoff.write_queue_cleanup_succeeded,
                "completed_at": handoff.cleanup_completed_at,
            },
        },
        "created_at": handoff.created_at,
        "updated_at": handoff.updated_at,
        "committed_at": handoff.committed_at,
        "accepted_at": handoff.accepted_at,
    });
    if let Some(replayed) = replayed {
        value["replayed"] = json!(replayed);
    }
    value
}

pub(crate) fn bounded_json(value: &serde_json::Value) -> Result<String, HandoffError> {
    let output = serde_json::to_string(value).map_err(|_| HandoffError::Storage)?;
    if output.len() > MAX_STATUS_JSON_BYTES {
        return Err(HandoffError::Storage);
    }
    Ok(output)
}

fn inspection_json(inspection: &HandoffInspection) -> serde_json::Value {
    json!({
        "handoff": handoff_json(&inspection.handoff, Some(inspection.replayed)),
        "validation": {
            "instructions_digest": inspection.instructions_digest,
        },
        "bundle": inspection.bundle,
        "instructions": inspection.instructions.iter().map(|instruction| json!({
            "scope": instruction.scope,
            "path": instruction.path,
            "digest": instruction.digest,
            "content": instruction.content,
        })).collect::<Vec<_>>(),
    })
}

fn print_inspection(inspection: &HandoffInspection, json_mode: bool) -> i32 {
    let value = inspection_json(inspection);
    let output = if json_mode {
        serde_json::to_string(&value)
    } else {
        serde_json::to_string_pretty(&value)
    };
    let output = match output {
        Ok(output)
            if output.len() <= MAX_HANDOFF_BUNDLE_BYTES + MAX_INSTRUCTIONS_BYTES + 128 * 1024 =>
        {
            output
        }
        _ => return print_error(HandoffError::Storage, json_mode),
    };
    println!("{output}");
    0
}

pub(crate) fn print_error(error: HandoffError, json_mode: bool) -> i32 {
    let exit = error.exit_code();
    if json_mode {
        let value = json!({
            "error": {
                "code": error.code(),
                "message": error.to_string(),
            }
        });
        match bounded_json(&value) {
            Ok(output) => eprintln!("{output}"),
            Err(_) => eprintln!(
                "{}",
                json!({"error":{"code":"storage_error","message":"bounded output failed"}})
            ),
        }
    } else {
        eprintln!("Error: {error}");
    }
    exit
}

fn print_outcome(action: &str, outcome: &HandoffOutcome, json_mode: bool) -> i32 {
    if json_mode {
        match bounded_json(&handoff_json(&outcome.handoff, Some(outcome.replayed))) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, true),
        }
    } else {
        let replay = if outcome.replayed {
            " replayed=true"
        } else {
            ""
        };
        let output = format!(
            "{action} {} state={} version={} source_generation={} target_generation={}{}",
            outcome.handoff.id,
            outcome.handoff.state,
            outcome.handoff.version,
            outcome.handoff.source_generation,
            outcome.handoff.target_generation,
            replay
        );
        if output.len() > MAX_STATUS_HUMAN_BYTES {
            return print_error(HandoffError::Storage, false);
        }
        println!("{output}");
        0
    }
}

fn status_recovery_reason(handoff: &TerminalHandoff) -> &'static str {
    if handoff.state != crate::handoff::HandoffState::NeedsRecovery {
        return "none";
    }
    match handoff.failure_kind.as_str() {
        "exit_without_stop" => "source_exit_without_stop",
        "sigterm_timeout" => "sigterm_timeout",
        "target_rejected" => "target_rejected",
        "supervisor_shutdown" | "outer_hangup" => "supervisor_shutdown",
        value if value.starts_with("target_") => "target_launch_failed",
        _ => "manual_intervention_required",
    }
}

fn handoff_status_json(db: &HcomDb, handoff: &TerminalHandoff) -> serde_json::Value {
    let target_generation =
        effective_handoff_target_generation(db, &handoff.id).unwrap_or(handoff.target_generation);
    let next_command = match handoff.state {
        crate::handoff::HandoffState::AwaitingAcceptance => format!(
            "hcom handoff inspect {} --version {}",
            handoff.id, handoff.version
        ),
        crate::handoff::HandoffState::NeedsRecovery => {
            format!("hcom chain status {}", handoff.chain_id)
        }
        _ => format!("hcom handoff status {}", handoff.id),
    };
    json!({
        "id": handoff.id,
        "chain_id": handoff.chain_id,
        "state": handoff.state.as_str(),
        "version": handoff.version,
        "transition": handoff.state.as_str(),
        "source_generation": handoff.source_generation,
        "target_generation": target_generation,
        "launch_cwd": handoff.workspace,
        "workspace": handoff.workspace,
        "policy": handoff.policy_ref,
        "recovery": {
            "required": handoff.state == crate::handoff::HandoffState::NeedsRecovery,
            "reason_code": status_recovery_reason(handoff),
        },
        "next_command": next_command,
    })
}

fn print_status(db: &HcomDb, handoff: &TerminalHandoff, json_mode: bool) -> i32 {
    if json_mode {
        match bounded_json(&handoff_status_json(db, handoff)) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, true),
        }
    } else {
        match handoff_human(db, handoff) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, false),
        }
    }
}

fn handoff_human(db: &HcomDb, handoff: &TerminalHandoff) -> Result<String, HandoffError> {
    let target_generation =
        effective_handoff_target_generation(db, &handoff.id).unwrap_or(handoff.target_generation);
    let next_command = match handoff.state {
        crate::handoff::HandoffState::AwaitingAcceptance => format!(
            "hcom handoff inspect {} --version {}",
            handoff.id, handoff.version
        ),
        crate::handoff::HandoffState::NeedsRecovery => {
            format!("hcom chain status {}", handoff.chain_id)
        }
        _ => format!("hcom handoff status {}", handoff.id),
    };
    let output = format!(
        "{} state={} version={} transition={} source_generation={} target_generation={}\n\
         chain={}\nlaunch_cwd={}\npolicy={}\n\
         recovery_required={} recovery_reason={}\nnext={}",
        handoff.id,
        handoff.state,
        handoff.version,
        handoff.state,
        handoff.source_generation,
        target_generation,
        handoff.chain_id,
        handoff.workspace,
        handoff.policy_ref,
        handoff.state == crate::handoff::HandoffState::NeedsRecovery,
        status_recovery_reason(handoff),
        next_command,
    );
    if output.len() > MAX_STATUS_HUMAN_BYTES {
        return Err(HandoffError::Storage);
    }
    Ok(output)
}

pub fn cmd_handoff(db: &HcomDb, args: &HandoffArgs, ctx: Option<&CommandContext>) -> i32 {
    let json_mode = match &args.command {
        HandoffCommand::Prepare(args) => args.json,
        HandoffCommand::Commit(args) | HandoffCommand::Inspect(args) => args.json,
        HandoffCommand::Accept(args) => args.json,
        HandoffCommand::Abort(args) | HandoffCommand::Reject(args) => args.json,
        HandoffCommand::Status(args) => args.json,
    };
    match &args.command {
        HandoffCommand::Prepare(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match prepare_handoff(db, &actor, args.bundle_event, &cwd) {
                Ok(outcome) => print_outcome("prepared", &outcome, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Commit(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match commit_handoff(db, &actor, &args.id, args.version, &cwd) {
                Ok(outcome) => print_outcome("committed", &outcome, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Abort(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match abort_handoff(
                db,
                &actor,
                &args.id,
                args.version,
                &args.reason.join(" "),
                &cwd,
            ) {
                Ok(outcome) => print_outcome("aborted", &outcome, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Status(args) => {
            let handoff = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => handoff_status_for_actor(db, &actor, args.id.as_deref()),
                Err(HandoffError::NotManaged)
                    if ctx.and_then(|value| value.identity.as_ref()).is_none() =>
                {
                    match crate::commands::chain::capture_human_terminal() {
                        Ok(terminal) => handoff_status_for_terminal_owner(
                            db,
                            args.id.as_deref(),
                            &terminal.owner,
                        ),
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            };
            match handoff {
                Ok(handoff) => print_status(db, &handoff, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Inspect(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match inspect_handoff(db, &actor, &args.id, args.version, &cwd) {
                Ok(inspection) => print_inspection(&inspection, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Accept(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match accept_handoff(db, &actor, &args.id, args.version, &cwd) {
                Ok(outcome) => print_outcome("accepted", &outcome, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
        HandoffCommand::Reject(args) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, json_mode),
            };
            let cwd = match cwd() {
                Ok(cwd) => cwd,
                Err(error) => return print_error(error, args.json),
            };
            match reject_handoff(
                db,
                &actor,
                &args.id,
                args.version,
                &args.reason.join(" "),
                &cwd,
            ) {
                Ok(outcome) => print_outcome("rejected", &outcome, args.json),
                Err(error) => print_error(error, args.json),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_typed_handoff_commands() {
        let prepare =
            HandoffArgs::try_parse_from(["handoff", "prepare", "--bundle-event", "42", "--json"])
                .unwrap();
        assert!(matches!(prepare.command, HandoffCommand::Prepare(_)));

        let commit =
            HandoffArgs::try_parse_from(["handoff", "commit", "ho-123", "--version", "0"]).unwrap();
        assert!(matches!(commit.command, HandoffCommand::Commit(_)));

        let inspect = HandoffArgs::try_parse_from([
            "handoff",
            "inspect",
            "ho-123",
            "--version",
            "5",
            "--json",
        ])
        .unwrap();
        assert!(matches!(inspect.command, HandoffCommand::Inspect(_)));

        let accept =
            HandoffArgs::try_parse_from(["handoff", "accept", "ho-123", "--version", "6"]).unwrap();
        assert!(matches!(accept.command, HandoffCommand::Accept(_)));

        let reject = HandoffArgs::try_parse_from([
            "handoff",
            "reject",
            "ho-123",
            "--version",
            "5",
            "--",
            "workspace mismatch",
        ])
        .unwrap();
        assert!(matches!(reject.command, HandoffCommand::Reject(_)));
    }

    #[test]
    fn mutation_version_is_required() {
        assert!(HandoffArgs::try_parse_from(["handoff", "commit", "ho-123"]).is_err());
        assert!(HandoffArgs::try_parse_from(["handoff", "accept", "ho-123"]).is_err());
    }

    #[test]
    fn typed_error_codes_and_exit_codes_are_stable() {
        let cases = [
            (
                HandoffError::Invalid("invalid".to_string()),
                "invalid_request",
                1,
            ),
            (
                HandoffError::Conflict("HANDOFF_CONFLICT".to_string()),
                "conflict",
                2,
            ),
            (HandoffError::NotManaged, "not_managed", 3),
            (HandoffError::Storage, "storage_error", 1),
        ];
        for (error, code, exit) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.exit_code(), exit);
        }
    }

    fn sample_handoff() -> TerminalHandoff {
        TerminalHandoff {
            id: "ho-sample".to_string(),
            chain_id: "tc-sample".to_string(),
            source_generation: 1,
            target_generation: 2,
            source_launch_nonce: "secret-source-nonce".to_string(),
            source_instance_name: "secret-source-instance".to_string(),
            source_hcom_session_id: "secret-hcom-session".to_string(),
            source_native_session_id: "secret-native-session".to_string(),
            source_wrapper_process_id: "secret-process".to_string(),
            source_process_birth_identity: "secret-birth".to_string(),
            bundle_event_id: 42,
            bundle_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            bundle_size_bytes: 128,
            workspace: "/workspace".to_string(),
            revision: "revision".to_string(),
            branch: "main".to_string(),
            dirty_summary: "staged=0,unstaged=0,untracked=0,conflicted=0".to_string(),
            policy_ref: "policy".to_string(),
            state: crate::handoff::HandoffState::Prepared,
            version: 0,
            quiesce_token: Some("secret-quiesce-token".to_string()),
            quiesce_generation: Some(1),
            quiesce_native_session_id: Some("secret-native-session".to_string()),
            quiesce_process_id: Some("secret-process".to_string()),
            quiesce_process_birth_identity: Some("secret-birth".to_string()),
            quiesce_committed_version: Some(1),
            stop_observed_at: None,
            stop_turn_id: None,
            sigterm_requested_wall_at: None,
            sigterm_requested_monotonic_ns: None,
            sigterm_request_result: String::new(),
            child_exit_observed_wall_at: None,
            child_exit_observed_monotonic_ns: None,
            child_exit_code: None,
            child_exit_signal: None,
            sigterm_to_exit_ms: None,
            delivery_exit_context: String::new(),
            waitpid_reaped: None,
            inject_cleanup_succeeded: None,
            delivery_cleanup_succeeded: None,
            pty_cleanup_succeeded: None,
            screen_cleanup_succeeded: None,
            write_queue_cleanup_succeeded: None,
            cleanup_completed_at: None,
            target_validation_token: Some("secret-validation-token".to_string()),
            target_instructions_digest: Some(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            ),
            target_validated_at: Some(1.0),
            failure_kind: String::new(),
            failure_reason: String::new(),
            created_at: 1.0,
            updated_at: 1.0,
            committed_at: None,
            accepted_at: None,
        }
    }

    #[test]
    fn status_outputs_are_bounded_and_omit_identity_and_authorization_material() {
        let directory = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&directory.path().join("hcom.db")).unwrap();
        let handoff = sample_handoff();
        let json = bounded_json(&handoff_status_json(&db, &handoff)).unwrap();
        let human = handoff_human(&db, &handoff).unwrap();
        assert!(json.len() <= MAX_STATUS_JSON_BYTES);
        assert!(human.len() <= MAX_STATUS_HUMAN_BYTES);
        for secret in [
            "secret-source-nonce",
            "secret-source-instance",
            "secret-hcom-session",
            "secret-native-session",
            "secret-process",
            "secret-birth",
            "secret-quiesce-token",
            "secret-validation-token",
        ] {
            assert!(!json.contains(secret), "JSON leaked {secret}");
            assert!(!human.contains(secret), "human output leaked {secret}");
        }

        let exact = serde_json::Value::String("x".repeat(MAX_STATUS_JSON_BYTES - 2));
        assert_eq!(bounded_json(&exact).unwrap().len(), MAX_STATUS_JSON_BYTES);
        let oversized = serde_json::Value::String("x".repeat(MAX_STATUS_JSON_BYTES - 1));
        assert!(matches!(
            bounded_json(&oversized),
            Err(HandoffError::Storage)
        ));
    }
}
