//! Blank interactive architect and its capability-bound session-task bridge.

mod bridge;
mod launch;
mod profile;
mod tools;

use anyhow::{Result, bail};
use std::path::Path;

pub fn run_component(args: &[String]) -> Result<()> {
    match args {
        [mode, flag, fd] if mode == "bridge" && flag == "--bootstrap-fd" => {
            let fd = fd
                .parse::<i32>()
                .map_err(|_| anyhow::anyhow!("invalid bridge bootstrap fd"))?;
            bridge::run_bridge(fd)
        }
        [mode, flag, socket] if mode == "relay" && flag == "--socket" => {
            bridge::run_relay(Path::new(socket))
        }
        _ => bail!("invalid hcom-architect-mcp invocation"),
    }
}

pub fn run_cli(args: &[String]) -> Result<i32> {
    launch::run_cli(args, None)
}

pub fn run_cli_with_config(args: &[String], config_path: &Path) -> Result<i32> {
    launch::run_cli(args, Some(config_path))
}

pub fn help_text() -> &'static str {
    r#"Usage:
  hcom architect codex --repo <canonical-git-root> [architect-profile-options]

Launch one blank, foreground Codex architect with capability-bound in-memory
session-task tools. --repo is required and must name the exact absolute,
canonical, clean Git top level.

Profile configuration is read once from $HCOM_DIR/config.toml (default:
~/.hcom/config.toml):
  [architect.profile]    interactive Codex architect
  [architect.developer]  fresh per-task Codex developer
  [architect.reviewer]   fresh per-task Codex or Claude reviewer

Architect CLI overrides (higher priority than TOML):
  --model <model>
  --reasoning <none|minimal|low|medium|high|xhigh|max>
  --sandbox <read-only|workspace-write|danger-full-access>
  --approval <untrusted|on-request|never>
  --ask-for-approval is an alias for --approval

Only typed profile fields are accepted; arbitrary native argv is not. The
effective sanitized profiles and their SHA-256 hash are printed at startup and
frozen into the approved plan.

No prompt argument, stdin payload, terminal injection, or automatic first turn
is used. The human owns every architect terminal input. The architect outer
workspace and every reviewer checkout remain read-only regardless of the
configured native sandbox/permission mode. Approved developer tasks commit
directly in the canonical checkout; drift stops the run without reset, rebase,
merge, or final apply. The parent owns all worker lifetime and no run is
recovered after exit.

See docs/architect.md for the complete TOML schema and examples."#
}
