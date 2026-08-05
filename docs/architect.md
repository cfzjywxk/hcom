# Architect and in-session task workers

`hcom arch` starts one blank foreground Codex or Claude Architect plus a
foreground-local, in-memory task supervisor. One foreground Architect may
execute multiple sequential runs. After the human authorizes a typed ordered
plan, the parent starts fresh no-TTY Developer and the configured active
Reviewer sessions through independently selected Codex or Claude adapters for
each task. The canonical ordered topology is either Reviewer1 alone or
Reviewer1 followed by Reviewer2. Dual mode runs both concurrently for each
review generation. Same-task correction resumes the exact Developer
session and re-review resumes each Reviewer's own exact native session; a
later task or later run starts fresh sessions.

There is no daemon, Project Store, cross-Architect recovery, final apply, push,
or install. Parent exit stops the workers and loses the current in-memory
control state. Each terminal run and its durable artifacts remain immutable;
starting another run does not revive or modify it.

## Start

```bash
cd /path/to/project
hcom arch codex
# Codex Architect with only Reviewer1:
hcom arch codex --single-review
# or
hcom arch claude [--add-dir /absolute/external/repository]...
```

The current directory is the project context. It must be an existing canonical
directory but need not be a Git repository. hcom starts the blank Architect
there without a prompt argument, stdin content, PTY write, paste, key event, or
Enter. The human owns the first and every later interactive input.

Each task names an absolute, lexically normalized `repository_root` that exists
as a directory. It may be `/home/user/src/hcom` while the project is
`/home/user/work/data/hcom-interactive`, or it may be nested below the project.
hcom passes this source path to every task worker; it does not infer it from plan
markdown or search the filesystem for a repository.

Both public entrypoints use the same provider-routed worker lane bundle. The
command selects only the foreground Architect; worker tables remain
independent:

- `hcom arch codex`: Codex foreground Architect, Codex Developer, Codex
  Reviewer1, and Claude Reviewer2 by default.
- `hcom arch codex --single-review`: Codex foreground Architect, Codex
  Developer, and Codex Reviewer1 by default; Reviewer2 is absent.
- `hcom arch claude`: Claude foreground Architect, Codex Developer, Codex
  Reviewer1, and Claude Reviewer2 by default.

Each worker lane may explicitly select Codex or Claude. An unavailable selected
adapter fails closed; hcom never silently falls back to another provider.
`--single-review` is rejected for a Claude foreground Architect before any
interactive process is spawned.

## Native Architect projection

The foreground Architect and all workers behave like directly launched native
CLI sessions:

- complete parent OS environment;
- real HOME and native config directory;
- native config.toml, auth, trust and custom model providers;
- global and project instruction files;
- rules, hooks, skills, plugins, apps, MCP servers and feature flags;
- ordinary host filesystem view;
- native caches and session history.

hcom does not write a native config, preselect project trust, clear MCP servers,
ignore user config/rules, or create a private HOME, CODEX_HOME,
CLAUDE_CONFIG_DIR, TMPDIR, XDG, Cargo or Rustup tree. It selects bare `codex`
or `claude` from inherited `PATH` without executable, version, or help pinning.

The intentional exceptions are small:

- Codex Architect/Developer/Reviewer built-in model and effort are passed
  explicitly as `gpt-5.6-sol` and `xhigh`, so those two defaults do not come
  from native config;
- every Claude role's built-in model and effort are passed explicitly as
  `opus` and `xhigh`;
- typed sandbox/approval or Claude permission values are explicit;
- the Architect receives one additive hcom task-control MCP binding in addition
  to native MCP servers;
- every Codex Architect and worker parent environment variable, including
  `HCOM_DIR`, is preserved byte-for-byte; hcom adds or replaces none;
- Claude additionally requires exact inherited `http_proxy`, `https_proxy`,
  `HTTP_PROXY`, and `HTTPS_PROXY` values of `http://127.0.0.1:7890`, and adds
  only `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` and
  `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`; every other parent entry is
  preserved and conflicting pin values fail closed;
- workers start from the complete parent environment; native Codex
  `shell_environment_policy` controls what model-started tool commands receive.

The Codex Architect is a direct child process. Every Claude role is launched
through a Linux per-invocation `PR_SET_CHILD_SUBREAPER` Guardian; the Claude
Architect uses `ForegroundArchitect` mode while retaining the same terminal
fds and human input ownership, and workers use `HeadlessWorker` mode. Guardian
cleanup covers owned descendants while the Guardian remains live; external
service-manager resources and unexpected Guardian death are outside that
guarantee. There is no
bubblewrap, mount/user/PID namespace, private environment reconstruction, or
HOME/auth/session-store preflight. hcom records the native PID for its
task-control relay; neither relay nor lifecycle ownership changes the native
process's host capabilities.

## Project and source instructions

The primary project is every worker's native cwd, so provider-global and
project instruction discovery applies normally. When `repository_root`
differs, hcom passes `--add-dir <repository_root>` to the Developer and every
active Reviewer. Claude workers also receive
`CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1`, so external-source
`CLAUDE.md` instructions load alongside project instructions.

For a Claude Architect, external task repositories must be declared before
launch with repeatable `hcom arch claude --add-dir <canonical-absolute-root>`.
The ordered roots are frozen into the session and bridge binding and shown in
the startup summary. A later typed plan may use a project-local repository or
an exact declared external root; any other external root is rejected before
plan approval. This is an instruction-loading contract, not a filesystem
allowlist, and hcom neither infers roots from task documents nor restarts the
Architect.

A secondary `--add-dir` root is not every provider's primary instruction
discovery chain. Therefore every task prompt explicitly tells every worker to
inspect and follow applicable AGENTS.md, AGENTS.override.md, and nested
instructions in:

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
the Architect. It prints the effective profiles, their SHA-256 invocation
profile hash, and the exact session binding hash; editing the file later does
not change any run in that foreground invocation.

Built-in defaults are:

| Command | Architect | Developer | Reviewer1 | Reviewer2 |
|---|---|---|---|---|
| `hcom arch codex` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Claude `opus`, `xhigh`, skip permissions |
| `hcom arch claude` | Claude `opus`, `xhigh`, skip permissions | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, `never` | Claude `opus`, `xhigh`, skip permissions |

Every table is a partial override. Omitted fields retain that role's built-in
default, so overriding only model/effort is sufficient:

```toml
[architect.profile]
model = "architect-model-override"

[architect.developer]
adapter = "claude"
model = "opus"
effort = "high"
dangerously_skip_permissions = true

[architect.reviewer1]
adapter = "codex"
model = "reviewer1-model-override"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.reviewer2]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

`adapter` is optional in worker tables. Developer and Reviewer1 default to
`codex`; Reviewer2 defaults to `claude`. Setting an adapter switches that table
to the selected role's complete built-in default before applying its remaining
partial overrides. Canonical configurations use `[architect.reviewer1]` and
`[architect.reviewer2]`. For migration only, a legacy-only
`[architect.reviewer]` table is resolved once using the released single-table
rules and copied completely to both Reviewer lanes in dual mode. In single
mode it is applied once to Reviewer1. Startup prints a mode-specific
deprecation notice. Combining the legacy table with either canonical Reviewer
table fails closed. In single mode an explicit `[architect.reviewer2]` table is
rejected; `[architect.reviewer1]` remains a normal partial override and may
select Claude, which activates the Claude environment gate. With no override,
single mode is pure Codex and does not activate that gate. Codex workers
require `sandbox = "danger-full-access"` and
`ask_for_approval = "never"` because they have no human approval channel.
Claude accepts `effort` and `dangerously_skip_permissions`; Codex accepts
either `reasoning_effort` or the shorter `effort` alias, but not both in one
table. Provider-specific fields in the wrong adapter table fail closed.

For a Claude foreground Architect:

```toml
[architect.profile]
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

Explicit `hcom arch` model/reasoning/sandbox/approval options change only the
foreground Architect. Developer, Reviewer1, and Reviewer2 values come from
their merged TOML tables or the built-in defaults. There is deliberately no
arbitrary argv field.

Precedence is:

```text
built-in defaults < $HCOM_DIR/config.toml < explicit Architect CLI options
```

The production Codex defaults remain `gpt-5.6-sol`/`xhigh`. Model-backed
contract and E2E tests deliberately default to the cheaper
`gpt-5.3-codex-spark`/`medium` pair. Production Claude defaults remain
`opus`/`xhigh`, but every model-backed Claude test must explicitly use
`haiku`/`medium`; ordinary source tests use fake executables and never call a
provider.

The serial, opt-in contract and task-lane entry points are documented in
[Claude task-lane test tooling](claude-task-lane-testing.md). They validate the
explicit Haiku/medium profile and exact inherited proxy gate before every real
Claude spawn and never run an interactive Architect TUI.

At startup hcom prints `review mode: single` or `review mode: dual`, followed
by every effective role profile, the invocation
profile hash, the exact session binding hash, the Guardian platform boundary,
and the additional-directory instruction policy. The session binding hash
includes the exact Architect/worker profiles, ordered Reviewer identities,
worker runtime contracts, and ordered Claude Architect `--add-dir` roots; the
approved plan hash binds it. The bridge bootstrap carries the same hash under a
closed schema. Private task-control protocol v9 exposes the ordered active
Reviewer state; an old or mismatched `hcom`/`hcom-architect-mcp` pair rejects the
bootstrap or protocol version instead of falling back to weaker/default
profiles.

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
  reviewer_adapters: [
    { reviewer_id: "reviewer1", adapter: reviewer1_adapter }
    // dual mode has one additional ordered Reviewer2 binding
  ],
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
  after_session_version,
  after_progress_sequence
})
```

The supervisor validates the exact session version, plan version/hash, frozen
session worker profiles, confirmation bit, field shape, and lexical
absolute-path syntax. It checks only that `repository_root` is an existing
directory. hcom does not open, copy, snapshot, hash, lock, or drift-check the
task/design documents, and it does not parse Markdown to infer a task.
`max_review_rounds` is a synchronized generation budget. Single mode accepts
5 through 20 and routes each generation to Reviewer1. Dual mode accepts 7
through 20 and fans out Reviewer1 and Reviewer2 concurrently. A round is
consumed only after every active logical response joins. The closed tool schema,
control validation, and supervisor core all enforce the mode-specific lower
bound before a worker can start.

A human message explicitly directing the Architect to
follow/execute/implement a named existing detailed plan, specification, or
`current_todo` authorizes same-turn plan derivation and start.
A message directing it to plan or define the solution and then implement,
proceed, finish, or drive the requested work also authorizes same-turn start of
the faithful derived plan. That prospective authorization remains valid after
the complete typed binding is displayed even though the detailed plan did not
exist when the human spoke; the Architect does not ask for duplicate approval
unless it introduced a new unresolved material decision.
Read/analyze/discuss/summarize/draft/update alone does not authorize execution,
and an explicit “do not start” always wins. A bare generic
implement/proceed/finish/drive request selects the delegated workflow rather
than Architect-side implementation, but does not by itself authorize start.

Execution approval for the standard task lane includes exactly one signed-off
local Developer candidate commit per task. Reviewer corrections amend that same
commit, and LGTM applies to the final exact candidate range; there is no extra
post-LGTM commit. A general instruction that commits require human
authorization is satisfied by approval of this run. An explicit requirement
that the run remain uncommitted is incompatible with the lane and must be
resolved before binding or start. If that conflict reaches a Developer, the
Developer returns `CLARIFICATION_REQUIRED` without modifying the repository,
and the Architect must call `session_clarification_require_human` regardless of
remaining autonomous clarification budget. It cannot autonomously reinterpret
run approval as overriding the explicit instruction. Developer commit and
amend instructions require a matching `Signed-off-by` trailer; every active
Reviewer checks it. Candidate commits do not authorize push, install, or
release.

All role prompts carry the exact project/source paths, task document path,
ordered design document paths, selector, instruction-discovery rule, and fixed
role contract. The workers read the original files. Peer messages use one
file-only route:

- every initial Reviewer prompt names the same Developer durable final path and
  current generation, but never names another Reviewer's response;
- a correction prompt names every active same-generation Reviewer
  logical-response path chain in stable binding order;
- every re-review prompt names only the latest Developer final path and states
  that the candidate was amended, without peer evidence;
- verdict clarification names only that Reviewer's original final path.

No peer body, redacted summary, or inline/file alternative enters these
prompts. A partial Reviewer response never starts the Developer. Same-task
correction resumes the exact Developer session, and every active Reviewer lane
resumes its own exact session after every amendment. Any amendment invalidates
all previous verdicts; only same-generation LGTM from every active Reviewer
completes the task.

After a run reaches a terminal state, the Architect first completes its
Reviewer and clarification evidence handoff. For LGTM it reports the final
local candidate commit as already reviewed at the exact range; it neither asks
whether to retain or revert that commit merely for lack of separate commit
authorization nor creates another commit after LGTM. Push, install, and release
remain separate human decisions. If the human later requests more delegated
work, the same foreground Architect creates a new empty run:

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

Every worker turn belongs to one exact Developer or active Reviewer lane and
starts one native process for that lane's selected provider. The lane runtimes
are independent even when they use the same provider, which is what permits
Reviewer1 and Reviewer2 to overlap in dual mode. A Codex turn uses the bare
`codex exec` selected from the session's inherited `PATH`:

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
thread-start event and Reviewer verdict line are parsed. A Claude turn uses
the native stream-json/in-band UUID contract through its own per-invocation
Guardian, with exact-cwd resume and the same durable-final routing rules.

There is no outer worker filesystem sandbox. Developer and active Reviewers see
what a native process launched by the same user sees. Reviewer non-mutation is
a model-facing role contract, not a read-only bind mount. hcom still owns
process groups or Guardians, cancellation, each lane's six-hour timeout,
descendant cleanup, exact resume, and the private raw-final/evidence transport.
Parent `HCOM_DIR` is unchanged. Codex receives the byte-for-byte parent
environment with no hcom additions or replacements; Claude receives that
environment plus only the two documented policy pins after the exact inherited
four-proxy gate succeeds.

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

A same-generation all-active-reviewer LGTM or `review_exhausted` closes every
task lane runtime before advancing. Runtime failure, identity mismatch, cleanup failure,
or a second unclassifiable verdict in either Reviewer lane cancels any live
peer and moves the run to a human-visible terminal state without aggregating a
single response. Parent exit/cancel stops every active owned process tree. hcom
never pushes, installs, resets, rebases, or automatically recovers after the
parent exits.

After dispatch, the Codex Architect calls `session_wait` with the returned run
ID and session version and `after_progress_sequence: 0`. This blocking MCP
subscription completes for one retained `review_requested`,
`review_responded`, or `task_completed` event; when the run becomes
`completed`, `needs_human`, `failed`, or `canceled`; or when a Developer
clarification/blocker action is latched. The local supervisor continues
lifecycle monitoring and advances Developer-to-active-Reviewer and correction
transitions without Architect model calls. For a progress result, the
Architect displays one concise update and immediately waits again using the
returned `session_version` and the event's `sequence`. It does not sleep, poll
`session_status`, or repeatedly infer. A wait bound to an earlier run ID is
rejected and can never subscribe to a later run.

Every progress event identifies the task ordinal/key, completed and total task
counts, completed `review_round`, and current `review_generation`.
`review_requested` is emitted only after every active Reviewer turn has started
and carries the exact `developer_final_path` they read, the approved task document,
ordered design document paths, selector, clarification-record count, and
ordered session-level Reviewer bindings. `review_responded` is emitted once
per logical Reviewer response and carries `reviewer_id`, verdict, Developer
final path, that Reviewer's ordered final-message path chain, and
`responses_received`/`responses_expected`. A response is partial while the
received count is below the expected count: the Architect displays its identity
and counts, says that another response is pending, and immediately waits again;
it does not describe the review generation as complete or imply that Developer
correction has started. In single mode the Reviewer1 response completes the
join immediately. `task_completed` separately records LGTM or review exhaustion
with every active current-generation typed Reviewer result. The Architect displays exact paths
but does not read or summarize their contents merely to produce a progress
update.

Esc or MCP cancellation closes only the current wait subscription; it does not
cancel the supervisor run. A pending Architect action records the session
version at which it was published. While it remains unresolved, a reconnect
from an older version immediately redelivers it; a repeated wait at or after
that published version is rejected so it cannot spin on the same action. A
run-local ordered event list retains progress produced between wait calls.
Pending Architect actions take priority; after they are resolved, queued
progress resumes at the last displayed sequence. Queued progress is drained
before a retained terminal snapshot, so a run finishing during the gap does
not hide any final Reviewer response or the task-completion event.

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
clarification as human-confirmed and re-arms the wait with the last displayed
progress sequence. `session_status` remains available only for an explicit
human progress query. `human_decision_confirmed` is an Architect attestation,
like execution approval; hcom does not identify the physical keyboard source.
Independent hard limits of 64 clarification records per task and 1280 per run
prevent that attestation from bypassing control-plane resource bounds.

Mutating control requests use a bounded recent replay window of 1024 completed
responses. A retained request ID remains payload-bound and replays its exact
response. When the window is full, the supervisor evicts the oldest completed
record before accepting another mutation; it never evicts an in-progress
record. Every mutation still carries an exact expected session version, so an
evicted successful request cannot execute again after its original state
transition. Cancellation remains available even if no completed replay record
can be evicted, so replay bookkeeping cannot wedge the run without a
protocol-level exit.

Every session snapshot exposes a bounded `active_workers` list (one Reviewer in
single mode, at most two during dual review), ordered session-level
`reviewer_bindings`, and
for every task its `latest_developer_final_path`, `review_round`,
`review_generation`, and ordered active Reviewer typed results. Each typed
result contains only that Reviewer's session-bound flag, current generation,
current verdict, and bounded current logical-response path chain; historical
generation paths and Reviewer bodies are not copied into the response.
Snapshots carry `clarification_record_count` rather than the accumulating
record vector; the Architect uses `session_clarifications_list` with the exact
run ID and pages of at most eight to read the ordered chain. MCP compatibility
text and `structuredContent` carry the same v9 metadata without any Reviewer
body. Only after a terminal `session_wait` response does the Architect read
every active Reviewer's non-empty current-generation path chain and use the original
verdicts and findings for the human-facing delivery. It distinguishes
same-generation `lgtm`, `review_exhausted`, and lifecycle failure from the
typed task/session state; an empty path chain means that Reviewer did not
successfully publish a current logical response. The Architect does not rerun
tests, perform another review, or repeat validation unless the human explicitly
requests that extra work.

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
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --quiet --release --locked
git show --check
bash -n scripts/dual-review-e2e
target/release/hcom --version
```

Real model-backed tests are opt-in and use disposable paths:

```bash
scripts/codex-exec-contract-smokes
cargo test --lib real_exec -- --ignored --nocapture --test-threads=1
scripts/dual-review-e2e strict-generation
```

They must never reuse, focus, type into, signal, or close an existing user
window/tab/pane. A real blank Architect TUI smoke requires a newly authorized
disposable terminal because automated submission of its first prompt would
violate user input ownership.
When Architect MCP schemas or the supported native Codex schema adapter
changes, that separately authorized smoke should use
`gpt-5.3-codex-spark`/`medium`, single-review mode, read-only sandboxing, and a
human-submitted non-executing first prompt. Its first success criterion is that
the service accepts every advertised tool schema without a 400; it must exit
without approving a plan or starting workers. The local source gate already
checks a narrow fail-closed schema policy and a Codex-0.145/0.146 compatibility
projection, so this real canary is confirmation of the external service rather
than the primary regression test.
The protocol-v9 dual-review runner is separately authorized, serial, and
Haiku/medium-only for Claude; its definitions are present but have not been run.
Earlier protocol results are not v9 dual-review evidence.

Implementation details and test mappings:

- [Codex Architect adapter contract](codex-adapter-contract.md)
- [Codex exec worker lane](codex-exec-worker-lane.md)
