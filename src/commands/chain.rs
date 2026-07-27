//! `hcom chain` — read-only typed chain status for Phase 1.

use clap::{Args, Parser, Subcommand};
use serde_json::json;

use crate::commands::handoff::{bounded_json, managed_actor_from_ctx, print_error};
use crate::db::HcomDb;
use crate::handoff::{HandoffError, MAX_STATUS_HUMAN_BYTES, TerminalChain, chain_status_for_actor};
use crate::shared::CommandContext;

const CHAIN_AFTER_HELP: &str = "\
Phase 1 provides status only. There is no chain start, Codex launch, recovery,
or hidden production initialization command.";

#[derive(Parser, Debug)]
#[command(
    name = "chain",
    about = "Inspect a typed same-terminal handoff chain",
    after_help = CHAIN_AFTER_HELP
)]
pub struct ChainArgs {
    #[command(subcommand)]
    pub command: ChainCommand,
}

#[derive(Subcommand, Debug)]
pub enum ChainCommand {
    /// Show the current or selected handoff chain
    Status(ChainStatusArgs),
}

#[derive(Args, Debug)]
pub struct ChainStatusArgs {
    pub id: Option<String>,
    #[arg(long)]
    pub json: bool,
}

fn chain_json(chain: &TerminalChain) -> serde_json::Value {
    json!({
        "id": chain.id,
        "state": chain.state.as_str(),
        "version": chain.version,
        "current_generation": chain.current_generation,
        "workspace": chain.workspace,
        "policy": {
            "tool": chain.tool,
            "model_ref": chain.model_ref,
            "reasoning_ref": chain.reasoning_ref,
            "permission_policy_ref": chain.permission_policy_ref,
            "policy_ref": chain.policy_ref,
        },
        "created_at": chain.created_at,
        "updated_at": chain.updated_at,
    })
}

fn print_chain(chain: &TerminalChain, json_mode: bool) -> i32 {
    if json_mode {
        match bounded_json(&chain_json(chain)) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, true),
        }
    } else {
        match chain_human(chain) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => print_error(error, false),
        }
    }
}

fn chain_human(chain: &TerminalChain) -> Result<String, HandoffError> {
    let output = format!(
        "{} state={} version={} current_generation={}\n\
         workspace={}\ntool={} model_ref={} reasoning_ref={}\n\
         permission_policy_ref={} policy_ref={}",
        chain.id,
        chain.state,
        chain.version,
        chain.current_generation,
        chain.workspace,
        chain.tool,
        chain.model_ref,
        chain.reasoning_ref,
        chain.permission_policy_ref,
        chain.policy_ref,
    );
    if output.len() > MAX_STATUS_HUMAN_BYTES {
        return Err(HandoffError::Storage);
    }
    Ok(output)
}

pub fn cmd_chain(db: &HcomDb, args: &ChainArgs, ctx: Option<&CommandContext>) -> i32 {
    match &args.command {
        ChainCommand::Status(status) => {
            let actor = match managed_actor_from_ctx(db, ctx) {
                Ok(actor) => actor,
                Err(error) => return print_error(error, status.json),
            };
            match chain_status_for_actor(db, &actor, status.id.as_deref()) {
                Ok(chain) => print_chain(&chain, status.json),
                Err(error) => print_error(error, status.json),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_status_only() {
        let args = ChainArgs::try_parse_from(["chain", "status", "tc-123", "--json"]).unwrap();
        assert!(matches!(args.command, ChainCommand::Status(_)));
        assert!(ChainArgs::try_parse_from(["chain", "start"]).is_err());
        assert!(ChainArgs::try_parse_from(["chain", "codex"]).is_err());
        assert!(ChainArgs::try_parse_from(["chain", "recover"]).is_err());
    }

    #[test]
    fn status_outputs_omit_supervisor_identity() {
        let chain = TerminalChain {
            id: "tc-sample".to_string(),
            workspace: "/workspace".to_string(),
            tool: "codex".to_string(),
            model_ref: "model".to_string(),
            reasoning_ref: "reasoning".to_string(),
            permission_policy_ref: "permission".to_string(),
            policy_ref: "policy".to_string(),
            supervisor_process_id: "secret-supervisor-process".to_string(),
            supervisor_process_birth_identity: "secret-supervisor-birth".to_string(),
            current_generation: 1,
            state: crate::handoff::ChainState::Active,
            version: 0,
            created_at: 1.0,
            updated_at: 1.0,
        };
        let json = bounded_json(&chain_json(&chain)).unwrap();
        let human = chain_human(&chain).unwrap();
        assert!(json.len() <= crate::handoff::MAX_STATUS_JSON_BYTES);
        assert!(human.len() <= MAX_STATUS_HUMAN_BYTES);
        for secret in ["secret-supervisor-process", "secret-supervisor-birth"] {
            assert!(!json.contains(secret));
            assert!(!human.contains(secret));
        }
    }
}
