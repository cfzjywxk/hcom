# Architect and in-session task workers

`hcom arch` starts one blank foreground Codex or Claude Architect plus a
foreground-local, in-memory task supervisor. One foreground Architect may
execute multiple sequential runs. After the human authorizes a typed ordered
plan, the parent starts fresh no-TTY Codex Developer and Reviewer sessions for
each task. Same-task correction/re-review resumes the exact original role
session; a later task or later run starts fresh sessions.

There is no daemon, Project Store, cross-Architect recovery, final apply, push,
or install. Parent exit stops the workers and loses the current in-memory
control state. Each terminal run and its durable artifacts remain immutable;
starting another run does not revive or modify it.

## Start

```bash
cd /path/to/project
hcom arch codex
# or
hcom arch claude
```

The current directory is the project context. It must be an existing canonical
directory but need not be a Git repository. hcom starts the blank Architect
there without a prompt argument, stdin content, PTY write, paste, key event, or
Enter. The human owns the first and every later interactive input.

Each task names an absolute, lexically normalized `repository_root` that exists
as a directory. It may be `/home/user/src/hcom` while the project is
`/home/user/work/data/hcom-interactive`, or it may be nested below the project.
hcom passes this source path to both roles; it does not infer it from plan
markdown or search the filesystem for a repository.

Both public entrypoints currently use the same native Codex exec worker lane:

- `hcom arch codex`: Codex foreground Architect, Codex Developer, Codex
  Reviewer.
- `hcom arch claude`: Claude foreground Architect, Codex Developer, Codex
  Reviewer.

Configuring a Claude Developer or Reviewer fails before the Architect starts.
Claude worker support is not part of this lane.

## Native Codex projection

The Codex Architect and workers behave like directly launched native Codex
sessions:

- complete parent OS environment;
- real HOME and CODEX_HOME;
- native config.toml, auth, trust and custom model providers;
- global and project AGENTS.md;
- rules, hooks, skills, plugins, apps, MCP servers and feature flags;
- ordinary host filesystem view;
- native caches and session history.

hcom does not write a Codex config, preselect project trust, clear MCP servers,
ignore user config/rules, disable features, or create a private HOME,
CODEX_HOME, TMPDIR, XDG, Cargo or Rustup tree.

The intentional exceptions are small:

- Codex Architect/Developer/Reviewer built-in model and effort are passed
  explicitly as `gpt-5.6-sol` and `xhigh`, so those two defaults do not come
  from native config;
- typed sandbox and approval values are explicit;
- the Architect receives one exact hcom task-control MCP table in addition to
  native MCP servers;
- every Architect and worker parent environment variable, including
  `HCOM_DIR`, is preserved byte-for-byte; hcom adds or replaces none;
- workers start from the complete parent environment; native Codex
  `shell_environment_policy` controls what model-started tool commands receive.

The Codex Architect is a direct child process launched as the bare program name
`codex`. There is no bubblewrap, mount/user/PID namespace, launch gate, private
environment reconstruction, or Codex HOME/auth/session-store preflight. hcom
records the spawned PID for its task-control relay and uses parent-death
lifecycle handling; neither changes the child's host capabilities.

The Claude foreground Architect retains its existing adapter-specific
containment. This Codex-native change does not add Claude worker support or
change Claude worker semantics.

## Project and source instructions

The primary project is the Codex cwd, so native global/project instruction
discovery applies normally. When `repository_root` differs, hcom passes
`--add-dir <repository_root>` to both Developer and Reviewer.

A secondary `--add-dir` root is not the primary Codex instruction-discovery
chain. Therefore every task prompt explicitly tells both roles to inspect and
follow applicable AGENTS.md, AGENTS.override.md, and nested instructions in:

1. the project directory; and
2. the task repository and every path they touch.

hcom transmits the two exact paths and that requirement. It does not read,
parse, merge, truncate, summarize, or resolve instruction files itself.

For example, with:

```text
project: /home/ywxk/src/work/data/hcom-interactive
source:  /home/ywxk/src/hcom
```

native Codex loads the real user/project context from the first path, while
the hcom prompt requires the worker to inspect source instructions under the
second path before development or review.

## Profile configuration

Profiles live in `$HCOM_DIR/config.toml`; without `HCOM_DIR`, the path is
`~/.hcom/config.toml`. The parent reads and validates it once before starting
the Architect. It prints the effective profiles and a SHA-256 profile hash;
editing the file later does not change any run in that foreground invocation.

Built-in defaults are:

| Command | Architect | Developer | Reviewer |
|---|---|---|---|
| `hcom arch codex` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` |
| `hcom arch claude` | Claude `opus`, `xhigh`, skip permissions | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` |

Every table is a partial override. Omitted fields retain that role's built-in
default, so overriding only model/effort is sufficient:

```toml
[architect.profile]
model = "architect-model-override"

[architect.developer]
model = "developer-model-override"
effort = "high" # alias for reasoning_effort

[architect.reviewer]
reasoning_effort = "xhigh"
```

`adapter` is optional in worker tables and defaults to the current Codex role.
The current exec worker lane rejects `adapter = "claude"` and, if explicitly
set, requires `sandbox = "danger-full-access"` and
`ask_for_approval = "never"` because workers have no human approval channel.
Codex accepts either `reasoning_effort` or the shorter `effort` alias, but not
both in one table.

For a Claude foreground Architect:

```toml
[architect.profile]
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

Explicit `hcom arch` model/reasoning/sandbox/approval options change only the
foreground Architect. Developer and Reviewer values come from their merged
TOML tables or the built-in defaults. There is deliberately no arbitrary argv
field.

Precedence is:

```text
built-in defaults < $HCOM_DIR/config.toml < explicit Architect CLI options
```

The production Codex defaults remain `gpt-5.6-sol`/`xhigh`. Model-backed
contract and E2E tests deliberately default to the cheaper
`gpt-5.3-codex-spark`/`medium` pair.

## Session identity

The task-control relay authenticates the hcom-spawned Architect/bridge
processes without registering or freezing a Codex Architect session identity.
hcom does not inspect or bind to Codex's shared session store: it does not scan
rollout files, parse native session metadata, or reject concurrent or unusual
Codex session history.

Exec workers use the `thread.started.thread_id` emitted by their own
`codex exec --json` process solely to resume the same Developer or Reviewer
conversation for the current task. A resumed turn returning a different ID
fails that turn instead of silently becoming a fresh conversation.

## Task handoff and authorization

The Architect submits an ordered list of file bindings, not copied plan
content:

```text
session_plan_replace({
  expected_session_version,
  developer_adapter,
  reviewer_adapter,
  tasks: [{
    task_key,
    title,
    repository_root,
    task_document_path,
    design_document_paths,
    task_selector,
    max_review_rounds
  }]
})

session_approve_and_start({
  expected_session_version,
  plan_version,
  plan_hash,
  approval_confirmed: true
})

session_wait({
  run_id,
  after_session_version
})
```

The supervisor validates the exact session version, plan version/hash, frozen
adapter pair, confirmation bit, field shape, and lexical absolute-path
syntax. It checks only that `repository_root` is an existing directory. hcom
does not open, copy, snapshot, hash, lock, or drift-check the task/design
documents, and it does not parse Markdown to infer a task.

A human message explicitly directing the Architect to
follow/execute/implement a named existing detailed plan, specification, or
`current_todo` authorizes same-turn plan derivation and start.
Read/analyze/discuss/summarize/draft/update alone does not authorize execution,
and an explicit “do not start” always wins.

Both role prompts carry the exact project/source paths, task document path,
ordered design document paths, selector, instruction-discovery rule, and fixed
role contract. The workers read the original files. Peer messages use one
file-only route:

- the initial Reviewer prompt names the Developer's durable final path;
- a correction prompt names the current ordered Reviewer final path or paths;
- a re-review prompt names only the latest Developer final path;
- verdict clarification names the original Reviewer final path.

No peer body, redacted summary, or inline/file alternative enters these
prompts. Same-task correction and re-review resume the exact respective role
session.

After a run reaches a terminal state, the Architect first completes its
Reviewer and clarification evidence handoff. If the human later requests more
delegated work, the same foreground Architect creates a new empty run:

```text
session_run_begin({
  expected_session_version: <terminal version>,
  terminal_run_id: <terminal run_id>
})
```

This does not bind a plan, approve execution, or start a worker. It returns a
new `run_id` in `awaiting_plan`; plan versioning restarts for that run, while
the session version continues monotonically across the foreground invocation.
The plan hash includes the new run identity. The old run remains unchanged
under its original `<project>/hcom-tasks/<run-id>/` directory.
After the first approved run opens `hcom-tasks`, its project-wide ownership
lock stays with the foreground supervisor through every terminal handoff and
run transition. `session_run_begin` drops only the old per-run evidence handle;
the next approval claims a new run directory without releasing or reacquiring
the project lock. The lock is released only when the foreground parent exits.

## Worker process and filesystem behavior

Every worker turn is one direct native `codex exec` process selected from the
session's inherited `PATH`:

```text
codex exec
  --sandbox danger-full-access
  --skip-git-repo-check
  [--add-dir <task repository>]
  [resume <exact thread id>]
  --json
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --config approval_policy="never"
  [--cd <project root>]
  --output-last-message <private raw file>
  -
```

Create gets `--cd`; resume inherits the process cwd and does not get `--cd`.
The task brief is written to stdin and then EOF. Only the documented
thread-start event and Reviewer verdict line are parsed.

There is no outer worker filesystem sandbox. Developer and Reviewer see what a
native process launched by the same user sees. Reviewer non-mutation is a
model-facing role contract, not a read-only bind mount. hcom still owns process
groups, cancellation, timeout, descendant cleanup, exact resume, and the
private raw-final/evidence transport. Parent `HCOM_DIR` is unchanged; the only
environment passed to the worker is the byte-for-byte parent environment, with
no hcom additions or replacements.

Native hooks/plugins/MCP can create descendants or alter behavior; that is
intentional native equivalence. A turn routes only after exit 0, exact session
proof, a non-empty final message, complete prompt/drain handling, and no
surviving process-group descendants.

## Evidence, lifetime, and terminal states

Per-turn artifacts are stored under:

```text
<project>/hcom-tasks/<run-id>/
```

Diagnostic prompt/stdout/stderr evidence retains its existing bounded
redaction behavior. The agent final is different: the native raw target stays
in a mode-0700 per-run runtime, then hcom validates its identity, size,
non-emptiness, and UTF-8 and writes it byte-for-byte to the attempt's durable
`native-final.partial`. hcom does not redact, scan, truncate, or lossily
convert a legal Developer/Reviewer final. Empty, invalid UTF-8, oversized, or
otherwise unsuccessful turns do not publish a routable final path. The
evidence directory is human handoff material, not a recovery store or
tamper-proof security boundary; a native-equivalent worker has ordinary
same-user host access.

An LGTM or `review_exhausted` closes the current task runtime before advancing.
Runtime failure, identity mismatch, cleanup failure, or a second
unclassifiable Reviewer verdict moves the run to a human-visible terminal
state. Parent exit/cancel stops the active process group. hcom never pushes,
installs, resets, rebases, or automatically recovers after the parent exits.

After dispatch, the Codex Architect calls `session_wait` with the returned
run ID and session version. This blocking MCP subscription completes when the
run becomes `completed`, `needs_human`, `failed`, or `canceled`, or when a
Developer clarification/blocker action is latched. The local supervisor
continues lifecycle monitoring and advances normal Developer-to-Reviewer and
correction transitions without Architect model calls. Codex may display
`Working` for the duration, but it does not sleep, poll `session_status`, or
repeatedly infer. A wait bound to an earlier run ID is rejected and can never
subscribe to a later run.

Esc or MCP cancellation closes only the current wait subscription; it does not
cancel the supervisor run. A pending Architect action records the session
version at which it was published. While it remains unresolved, a reconnect
from an older version immediately redelivers it; a repeated wait at or after
that published version is rejected so it cannot spin on the same action. A
terminal snapshot is retained in memory and likewise returns immediately if
the run finished during the gap.

Task-lane polling is fail-closed at both ownership layers. The driver does not
drop its active-turn handle until the cloned core accepts the completion
event. Any poll/reducer bookkeeping error closes the task runtime, records a
bounded driver diagnostic, and moves the run to `needs_human` before returning
the error. The outer control loop additionally converts any future backend
violation of that terminal-on-error contract into a shutdown terminal and
services the pending wait, so `session_wait` cannot be stranded by a discarded
poll error.

For an action the Architect can answer defensibly from approved sources, it
writes only the exact new clarification path supplied by hcom, submits it, and
immediately re-arms `session_wait` with the returned version in the same turn.
If the action needs a material human decision, the Architect marks it as such,
reports the question and current repository state, and ends the turn without
calling `session_wait`. After the human answers, it submits the exact pending
clarification as human-confirmed and re-arms the wait. `session_status` remains
available only for an explicit human progress query. `human_decision_confirmed`
is an Architect attestation, like execution approval; hcom does not identify
the physical keyboard source. Independent hard limits of 64 clarification
records per task and 1280 per run prevent that attestation from bypassing
control-plane resource bounds.

Mutating control requests use a bounded recent replay window of 1024 completed
responses. A retained request ID remains payload-bound and replays its exact
response. When the window is full, the supervisor evicts the oldest completed
record before accepting another mutation; it never evicts an in-progress
record. Every mutation still carries an exact expected session version, so an
evicted successful request cannot execute again after its original state
transition. Cancellation remains available even if no completed replay record
can be evicted, so replay bookkeeping cannot wedge the run without a
protocol-level exit.

Every terminal snapshot carries, for every task, its
`latest_developer_final_path`, ordered `final_reviewer_message_paths`, and
`reviewer_verdict`. It carries `clarification_record_count` rather than the
accumulating record vector; the Architect uses `session_clarifications_list`
with the exact run ID and pages of at most eight to read the ordered chain. The
Reviewer body is not copied into either the MCP compatibility text or
`structuredContent`. After `session_wait` returns, the
Architect reads every non-empty Reviewer path in order and uses the original
verdict and findings for the human-facing delivery. It distinguishes `lgtm`,
`review_exhausted`, and lifecycle failure from the typed task/session state;
an empty list means that no Reviewer final was successfully published. The
Architect does not rerun tests, perform another review, or repeat validation
unless the human explicitly requests that extra work.

The Architect must finish clarification pagination before
`session_run_begin`: the old files remain durable, but the in-memory
clarification control target moves to the new run. Beginning the next run
resets logical Developer/Reviewer session and turn counters, allocates a new
artifact namespace, and retains the same captured parent environment and
frozen role profiles. It does not require a new terminal or a new native
Architect process.

## Verification

The source gate is:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --quiet --locked --all-targets
git diff --check
cargo build --quiet --release --locked
```

Real model-backed tests are opt-in and use disposable paths:

```bash
scripts/codex-exec-contract-smokes
cargo test --lib real_exec -- --ignored --nocapture --test-threads=1
```

They must never reuse, focus, type into, signal, or close an existing user
window/tab/pane. A real blank Architect TUI smoke requires a newly authorized
disposable terminal because automated submission of its first prompt would
violate user input ownership.

Implementation details and test mappings:

- [Codex Architect adapter contract](codex-adapter-contract.md)
- [Codex exec worker lane](codex-exec-worker-lane.md)
