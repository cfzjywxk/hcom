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
  cd <project-directory>
  hcom arch codex [architect-profile-options]
  hcom arch claude [architect-profile-options]

Launch one blank, foreground Codex or Claude architect with capability-bound
in-memory session-task tools. The invocation's exact current directory is the
project context seen by the Architect and every task worker. It does not need
to be a Git repository.

Profile configuration is read once from $HCOM_DIR/config.toml (default:
~/.hcom/config.toml):
  [architect.profile]    interactive architect selected by the command
  [architect.developer]  fresh per-task Codex or Claude developer
  [architect.reviewer]   fresh per-task Codex or Claude reviewer

Architect CLI overrides (higher priority than TOML):
  --model <model>
  Codex:  --reasoning <none|minimal|low|medium|high|xhigh|max>
          --sandbox <read-only|workspace-write|danger-full-access>
          --approval <untrusted|on-request|never>
  --ask-for-approval is an alias for --approval
  Claude: --effort <low|medium|high|xhigh|max>

Built-in defaults are Codex gpt-5.6-sol/xhigh with danger-full-access/never,
or Claude opus/xhigh with dangerously-skip-permissions. The capability-bound
session-control MCP tools do not add a second native approval prompt. The
independent built-in developer is Codex gpt-5.6-sol/xhigh. The Codex
Architect's per-run private config records the exact project as native
untrusted, avoiding a repeated folder-trust prompt without loading project-local
.codex config, hooks, rules, or extra MCP servers. This does not override the
explicit danger-full-access/never profile. The Architect may start in the same
turn when the human explicitly directs it to
follow or execute a named existing detailed plan/specification/current_todo;
analysis or drafting alone still waits for approval. hcom validates the exact
version/hash and confirmation bit, not OS-level keyboard provenance. When
[architect.reviewer] is absent, the reviewer is always Claude opus/xhigh with
dangerously-skip-permissions, independent of the selected Architect and its
TOML or CLI overrides. An explicit [architect.reviewer] table takes priority.

Only typed profile fields are accepted; arbitrary native argv is not. The
effective sanitized profiles and their SHA-256 hash are printed at startup and
frozen into the approved plan.

No prompt argument, stdin payload, terminal injection, or automatic first turn
is used. The human owns every architect terminal input. The Architect has a
path-preserving whole-host read-write view for project plans, current_todo, and
coordination records. Live hcom state, parent Codex/Claude config, hcom control
runtimes, and exact credentials/tools stay isolated, masked, or read-only;
HCOM_DIR inside the Architect points to private per-run state. Other same-user
files, including SSH and cloud credentials, remain writable. Every reviewer's
canonical project/source/Git view remains read-only, while its session-private
HOME, temporary directory, and generated language caches remain writable. Each
authorized task names the exact canonical Git root discovered by the Architect
from project documentation. Developer tasks commit directly there; hcom has no
repository-root allowlist. An in-scope uncommitted developer result gets one
exact-session recovery before review; out-of-scope changes, rewrite, external
drift, or uncertain session identity stop the run without reset, rebase, merge,
or final apply. The parent owns all worker lifetime and no run is recovered
after exit. After dispatch, the Architect should check worker status only every
3 to 5 minutes unless the human asks or immediate intervention is required;
the foreground supervisor monitors lifecycle internally without model calls,
so short-cadence polling only wastes Architect requests and tokens.

See docs/architect.md for the complete TOML schema and examples."#
}
