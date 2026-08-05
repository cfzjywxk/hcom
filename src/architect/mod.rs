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
task carries an exact canonical source directory; task worker role sessions use
the project directory as native cwd and receive that directory through
--add-dir when it is distinct.

Profile configuration is read once from $HCOM_DIR/config.toml (default:
~/.hcom/config.toml):
  [architect.profile]    interactive architect selected by the command
  [architect.developer]  fresh per-task developer profile
  [architect.reviewer1]  fresh per-task Reviewer1 profile
  [architect.reviewer2]  fresh per-task Reviewer2 profile

Each table is a partial override: omitted fields keep the built-in role
default. Codex accepts reasoning_effort or its effort alias. Developer,
Reviewer1, and Reviewer2 adapters are selected independently; their defaults
are codex, codex, and claude respectively. A legacy-only
[architect.reviewer] profile is resolved once and copied to both Reviewer
lanes with a deprecation notice; mixing it with either canonical Reviewer
table fails closed.

Architect CLI overrides (higher priority than TOML):
  --model <model>
  Codex:  --reasoning <none|minimal|low|medium|high|xhigh|max>
          --sandbox <read-only|workspace-write|danger-full-access>
          --approval <untrusted|on-request|never>
  --ask-for-approval is an alias for --approval
  Claude: --effort <low|medium|high|xhigh|max>
          --add-dir <absolute-external-repository> (repeatable)

Built-in Architect defaults are Codex gpt-5.6-sol/xhigh with
danger-full-access/never, or Claude opus/xhigh with
dangerously-skip-permissions. Both entrypoints share one provider-routed worker
lane bundle. Developer and Reviewer1 default to Codex gpt-5.6-sol/xhigh with
danger-full-access/never; Reviewer2 defaults to Claude opus/xhigh with
dangerously-skip-permissions. Any worker lane can explicitly select codex or
claude. An unavailable selected adapter fails closed without fallback.

The capability-bound session-control MCP tools do not add a second native
approval prompt. For Codex, hcom adds one exact task-control MCP config leaf.
For Claude, hcom adds one non-strict --mcp-config server and preserves native
MCP configuration. All other native user/project config, trust, instructions,
rules, hooks, skills/plugins, feature flags, providers, and managed policy
remain loaded.
The Architect may start in the same turn when the human explicitly directs it
to follow or execute a named existing detailed plan/specification/current_todo,
or directs it to plan/define the solution and then implement, proceed, finish,
or drive the requested work. The latter authorization remains valid after a
faithful plan is derived and displayed; it does not need to be repeated merely
because the exact plan did not exist when the human spoke. A new unresolved
material decision still requires approval, and analysis or drafting without an
execution directive still waits. A bare generic implement/proceed/finish/drive
request selects delegation rather than Architect-side development, but does not
by itself authorize starting the delegated loop. hcom validates the typed plan
revision/hash and confirmation bit, not OS-level keyboard provenance.

Only typed profile fields are accepted; arbitrary native argv is not. The
effective sanitized profiles, their SHA-256 hash, and the exact session binding
hash are printed at startup and frozen for every sequential run in that
foreground Architect invocation. The private task-control protocol is v8:
its plan hash binds the ordered Reviewer identities and exact session
profile/runtime contract; the closed bridge bootstrap carries the same binding
hash. A mismatched hcom and
hcom-architect-mcp pair fails closed rather than weakening the binding.

No prompt argument, stdin payload, terminal injection, or automatic first turn
is used. The human owns every architect terminal input. The Architect has a
path-preserving whole-host read-write view for project plans, current_todo, and
coordination records. Codex and Claude Architects, and every Codex or Claude
task worker, inherit the complete parent environment, real HOME and native
config/auth/cache/session state, plus the ordinary same-user host view. hcom
does not replace any parent environment variable, including HCOM_DIR. Claude
adds only
CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1 and
CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1, refusing conflicting parent values.
Native shell_environment_policy decides what model-started commands inherit.
hcom does not inspect or freeze native Architect session storage. Every Claude
launch requires unique exact http_proxy/https_proxy/HTTP_PROXY/HTTPS_PROXY
entries set to http://127.0.0.1:7890 before any Claude executable starts, and
runs through the Linux per-invocation descendant Guardian. That Guardian uses
PR_SET_CHILD_SUBREAPER while live; external service-manager resources and
unexpected Guardian death remain outside its cleanup guarantee.

Each authorized task binds the exact canonical source directory, absolute task
document path, ordered absolute design document paths, and task selector
discovered by the Architect from project documentation. hcom transports those
strings without reading, copying, hashing, locking, or drift-checking the
documents. Developer tasks read the original files, then work and commit
directly in the source directory; hcom has no repository-root allowlist and
does not inspect or repair Git.
For a Claude Architect, an external task source root must exactly match an
ordered --add-dir root declared at foreground startup. This gate ensures native
external instructions were loaded; it is not a filesystem access restriction.
Execution approval for the standard lane includes exactly one signed-off local
candidate commit per task and amendments of that same commit during correction.
A general instruction requiring human authorization for commits is satisfied
by that run approval. An explicit no-commit requirement is incompatible with
the standard lane and must be resolved before start. If that conflict reaches a
Developer turn, the Developer requests clarification without modifying the
repository and the Architect must escalate it to the human regardless of
remaining autonomous clarification budget. The Developer is instructed to add
and preserve a matching Signed-off-by trailer, and both Reviewers check it.
Local candidate commits do not authorize push, install, release, or an extra
commit after LGTM.
Both Reviewers receive the same native host view; source/Git/install
non-mutation is a role instruction rather than an OS read-only mount. Each
independently reads the Developer's exact durable final through its path and
never receives the peer Reviewer's response. Corrections carry both
same-generation Reviewer response paths in stable Reviewer1-then-Reviewer2
order; re-reviews carry only the latest Developer path. hcom never pushes,
installs, resets, rebases, merges, or applies changes.

The foreground parent owns every worker and task-local exec runtime; it keeps
state only in memory and performs no daemon, project store, or cross-session
recovery. Same-task corrections use the exact native Developer thread and
re-reviews resume each Reviewer's own exact native thread. Different tasks and
different runs use fresh workers. Each review generation starts Reviewer1 and
Reviewer2 concurrently, waits for both logical responses, and completes only
on same-generation dual LGTM; a Developer amendment invalidates both previous
verdicts. After dispatch, the Codex
Architect opens one blocking session_wait MCP call bound to the exact run_id
and a run-local progress cursor. The foreground supervisor advances Developer
and both Reviewers without Architect model calls and completes that wait for one
retained progress event, a terminal state, or a latched Developer
clarification/blocker action. Progress events are emitted for each review
generation, each Reviewer response, and task completion. They carry task
position, completed review round, current generation, Reviewer identity,
response counts and verdict/outcome where applicable, the exact Developer
final path, and ordered current-generation Reviewer final paths where
applicable; review requests also carry the task/design paths, selector, and
both session-level Reviewer bindings. The Architect displays each event once
without reading the durable response body, treats the first Reviewer response
as partial progress, and immediately re-arms session_wait with that event's
sequence. Worker execution continues while the update is displayed, and
events produced during the display-to-wait gap remain queued in order.
The Architect answers a defensible action through the exact new clarification
document path and immediately re-arms session_wait in the same turn. When a
material human decision is required, it ends the turn after asking the human
and resumes only after the human answers. session_status is reserved for an
explicit human status request. While a wait is pending Codex is not issuing
status polls. Interrupting the wait cancels only that subscription, never the
run. A still-latched action has priority over queued progress and is
redelivered only to a wait from a version older than its published_version; a
same-version repeated wait is rejected until the action is resolved. Queued
progress, including both Reviewer response events, is delivered before a
retained terminal result. Status snapshots expose bounded concurrent
active_workers, session-level ordered Reviewer bindings, each task's
review_round/review_generation, and typed current-generation Reviewer results;
they carry clarification counts rather than record bodies.
session_clarifications_list reads the ordered records in bounded pages bound
to that run_id.
The terminal snapshot contains each task's latest Developer final path and
Reviewer1/Reviewer2 current-generation identities, verdicts, and final-message
path chains, never either Reviewer body. Only then does the Architect read
both final Reviewer evidence chains and deliver their original verdicts and
findings, distinguishing same-generation dual LGTM, review exhaustion, and
lifecycle failure. It does not rerun tests, review, or validation unless the
human explicitly requests that work. An LGTM task's final local candidate
commit is already reviewed at the reported exact range; the Architect reports
it without asking whether to retain or revert it merely because commit was not
separately authorized. Push, install, and release remain separate. A terminal
run remains immutable but does not end the foreground Architect. After
completing that evidence handoff, a later human request can call
session_run_begin with the exact terminal run_id
and version to create a fresh empty run under the same parent. The new run gets
a new run_id, monotonically advances the session version, resets run-local
task/worker identity, and still requires a separately bound and approved plan.

See docs/architect.md for the complete TOML schema and examples."#
}
