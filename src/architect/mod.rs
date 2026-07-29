//! Blank interactive architect and its capability-bound session-task bridge.

mod bridge;
mod launch;
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
    launch::run_cli(args)
}

pub fn help_text() -> &'static str {
    "Usage:\n\
  hcom architect codex --repo <canonical-git-root>\n\
\n\
Launch one blank, foreground Codex architect with an isolated read-only workspace\n\
and capability-bound in-memory session-task tools. The human owns the first and every\n\
subsequent terminal input.\n\
\n\
Exact architect profile:\n\
  --model gpt-5.6-sol\n\
  --reasoning high\n\
  --sandbox read-only\n\
  --approval never\n\
\n\
No prompt argument, stdin payload, terminal injection, or automatic first turn is used.\n\
Approved developer tasks commit directly in the canonical checkout; any branch,\n\
HEAD, or worktree drift stops the run without reset, rebase, merge, or final apply.\n\
The architect parent owns all worker lifetime and no run is recovered after exit."
}
