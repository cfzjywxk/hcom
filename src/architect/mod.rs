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
Architect's project context and does not need to be a Git repository. Every
task carries an exact canonical source directory; exec worker role threads use
the project directory as native cwd and receive that directory through
--add-dir when it is distinct.

Profile configuration is read once from $HCOM_DIR/config.toml (default:
~/.hcom/config.toml):
  [architect.profile]    interactive architect selected by the command
  [architect.developer]  fresh per-task developer profile
  [architect.reviewer]   fresh per-task reviewer profile

Each table is a partial override: omitted fields keep the built-in role
default. Codex accepts reasoning_effort or its effort alias. Worker adapter
defaults to codex.

Architect CLI overrides (higher priority than TOML):
  --model <model>
  Codex:  --reasoning <none|minimal|low|medium|high|xhigh|max>
          --sandbox <read-only|workspace-write|danger-full-access>
          --approval <untrusted|on-request|never>
  --ask-for-approval is an alias for --approval
  Claude: --effort <low|medium|high|xhigh|max>

Built-in Architect defaults are Codex gpt-5.6-sol/xhigh with
danger-full-access/never, or Claude opus/xhigh with
dangerously-skip-permissions. Both entrypoints share one worker lane: a fresh
codex-exec process per turn. Developer and Reviewer both
default to Codex gpt-5.6-sol/xhigh with danger-full-access/never. Explicit
worker tables must also select Codex with danger-full-access/never; Claude
workers are unsupported and fail closed.

The capability-bound session-control MCP tools do not add a second native
approval prompt. For Codex, hcom adds one exact task-control MCP config leaf;
all other native user/project config, trust, AGENTS.md, rules, hooks, skills,
plugins, feature flags, providers, and MCP servers remain loaded. This does not
override the explicit danger-full-access/never profile.
The Architect may start in the same turn when the human explicitly directs it
to follow or execute a named existing detailed plan/specification/current_todo;
analysis or drafting alone still waits for approval. hcom validates the typed
plan revision/hash and confirmation bit, not OS-level keyboard provenance.

Only typed profile fields are accepted; arbitrary native argv is not. The
effective sanitized profiles and their SHA-256 hash are printed at startup and
used for that foreground run.

No prompt argument, stdin payload, terminal injection, or automatic first turn
is used. The human owns every architect terminal input. The Architect has a
path-preserving whole-host read-write view for project plans, current_todo, and
coordination records. A Codex Architect and every Codex exec worker inherit the
complete parent environment, real HOME/CODEX_HOME, native config, auth, caches,
session history, and ordinary same-user host view. hcom does not replace any
parent environment variable, including HCOM_DIR, and does not add role/run/task
environment variables to Codex worker processes.
Native shell_environment_policy decides what model-started commands inherit.
hcom does not inspect, register, or freeze a native Codex Architect session.
The existing Claude foreground Architect keeps its separately documented
private config/containment behavior; Claude task workers remain unsupported.

Each authorized task names the exact canonical source directory discovered by
the Architect from project documentation. Developer tasks work and commit
directly there; hcom has no repository-root allowlist and does not inspect or
repair Git.
The Reviewer receives the same native host view; source/Git/install
non-mutation is a role instruction rather than an OS read-only mount. hcom
sequences the reports and verdicts but never pushes, installs, resets, rebases,
merges, or applies changes.

The foreground parent owns every worker and task-local exec runtime; it keeps
state only in memory and performs no daemon, project store, or cross-session
recovery. Same-task corrections use the exact native Developer/Reviewer thread.
After dispatch, the Architect should check worker status only every 3 to 5
minutes unless the human asks or immediate intervention is required; the
foreground supervisor monitors lifecycle internally without model calls, so
short-cadence polling only wastes Architect requests and tokens.

See docs/architect.md for the complete TOML schema and examples."#
}
