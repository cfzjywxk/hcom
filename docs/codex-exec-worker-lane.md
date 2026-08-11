# Codex exec worker provider

The Codex provider inside hcom's task worker lane: one native `codex exec`
process per selected Codex turn, no protocol conversation, and a supervisor
that never judges the work. Developer, Reviewer1, and Reviewer2 adapters are
independent; the built-in lanes are Codex Developer + Codex Reviewer1 + Claude
Reviewer2, while explicit canonical overrides can retain this provider for any
or all worker lanes.

## The two rules

**Native Codex semantics, thin hcom transport.** A worker starts from the
launching terminal's complete environment, real HOME/CODEX_HOME, native Codex
configuration and ordinary host filesystem view. hcom fixes the typed
model/effort/permission profile and adds only the transport needed to automate
the human copy/paste loop. Afterwards hcom observes the world (process exit,
one documented stdout event, one native output file), publishes exact durable
final-message paths, and retains diagnostic evidence. Model text is payload,
never protocol.

**The supervisor is task-agnostic.** It sequences processes and carries
messages between them. It does not run checks, inspect commits, or judge
whether the work is any good. Developers verify their own work, reviewers
verify independently under the fixed review contract below, and the human plus
real CI are the last word. In the default local-candidate mode hcom neither
pushes nor installs, so a wrong judgment costs a review round or a local commit,
both cheap and revertible. The explicit
[GitHub Pull Request lane](github-pr-lane.md) adds a supervisor-owned bound
publication workflow; workers still receive no GitHub credentials or direct
push authority.

**The approved local lane includes its candidate commit.** Each Developer
creates exactly one signed-off local task commit before review and amends only
that commit for corrections. Its create/amend instructions require a matching
`Signed-off-by` trailer, which every active Reviewer checks. A general instruction
requiring human authorization for commits is satisfied by approval of the
standard run. An explicit no-commit requirement is incompatible and routes to
clarification instead of being silently ignored. That authority conflict is
included in the per-turn `CLARIFICATION_REQUIRED` output contract; the Architect
must escalate it to the human regardless of remaining autonomous clarification
budget and cannot autonomously override it. Reviewer LGTM applies to the exact
final candidate range already committed only when every active Reviewer returns LGTM
for the same generation; it does not authorize or require another commit.
Local-lane push, install, and release always remain separate. GitHub-lane plan
approval covers only the disclosed bound PR workflow; install and release
remain separate there as well.

## Invocation

`ExecTaskWorkerRuntime` (`src/worker/exec_runtime.rs`) builds one bounded
transport shape on top of native Codex configuration.
Ordering is load-bearing: `--sandbox`, `--skip-git-repo-check` and `--add-dir`
belong to the `exec` parent and precede `resume`.

```text
codex exec
  --sandbox danger-full-access
  --skip-git-repo-check                     # project cwd may be non-Git
  [--add-dir <task repository>]             # external/nested repository scope
  [resume <exact thread id>]                # same task only; never with --cd
  --json                                    # only thread.started is parsed
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --config approval_policy="never"
  [--cd <project root>]                     # create only
  --output-last-message <private raw target>
  -                                         # bounded stdin prompt, then EOF
```

`--cd` applies to create turns only. A **resume takes no `--cd` and inherits
the process working directory**, so every invocation is launched directly
from the project directory with `Command::current_dir` — otherwise a resumed
turn would silently work somewhere else. `--add-dir` belongs to the exec
parent, ahead of `resume`, and is passed for the Developer and either Reviewer
lane when the task repository differs from the project directory.

hcom does not pass `--strict-config`, `--ignore-user-config`,
`--ignore-rules`, `mcp_servers={}`, feature-disable flags, or a private
HOME/CODEX_HOME. Native user/project configuration, global and project
AGENTS.md, trust, auth, rules, hooks, skills, plugins, MCP servers, feature
flags, and session history therefore behave as they do in a directly launched
Codex CLI. This includes `shell_environment_policy`: hcom gives the Codex
process the complete parent environment, while native user configuration
decides what Codex passes onward to model-started tool commands.

Codex discovers instructions automatically only in its primary project chain.
An external task repository is a secondary `--add-dir` root, so every hcom
Codex task-worker prompt explicitly requires inspecting applicable
AGENTS.md, AGENTS.override.md, and nested instructions in both the project and
the task repository. hcom forwards the exact paths; it does not parse or
resolve those instruction files itself.

No parent environment entry is redirected: `HCOM_DIR`, HOME, CODEX_HOME,
TMPDIR, XDG, Cargo/Rustup, authentication, proxies, and every other entry remain
byte-for-byte native. hcom neither adds nor replaces worker environment
entries. Workers run directly on the host rather than in an outer bubblewrap
filesystem sandbox. Reviewer non-mutation is a role instruction, not an OS
read-only mount, matching a manually launched review session.

## What hcom parses

Exactly two things, and nothing else:

1. **`thread.started.thread_id`** — the first stdout line of a `--json` run.
   This is the session identity proof. After the first valid one, JSON parsing
   stops for the life of the turn and the remaining stdout is drained as raw
   bytes into evidence, so an arbitrarily large later event can neither grow a
   line buffer nor fail a turn. A resume that returns a different `thread_id`
   fails closed; identity is never assumed.
2. **The reviewer's verdict line** — see below.

Everything else on stdout/stderr is diagnostic evidence. The native final is
stored exactly and routed only by its durable file path; its body is never
copied into the next role's prompt or the Architect control response.

## Routing preconditions

A turn routes onward only when all three hold:

- the process exited 0;
- a `thread.started` proof was captured;
- the native final-message file is non-empty, valid UTF-8, and within the hard
  cap.

Plus three integrity conditions: the prompt must have been delivered in full (a
partial prompt means the worker answered a different question), the drain threads must report no read/write/seal
error (losing evidence stops the run rather than routing an incomplete record),
and the process group must have no surviving descendants. Descendants are
signalled individually after verifying each one's session *and* group id, never
via `kill(-pgid)`: a bare group id can be recycled between the scan and the
signal. A background child of
the worker would otherwise hold the pipes open — blocking the drain joins — or
outlive the run as an orphan that keeps burning tokens.

Otherwise it is a process-level stop for a human, and the artifacts stay as
evidence without routing. A run that exits non-zero but leaves a plausible
final message does **not** route: a Codex whose tool layer is failing will
still narrate progress and emit a confident-looking summary.

## Verdict grammar

The reviewer prompt asks for a first line of exactly `VERDICT: LGTM` or
`VERDICT: REQUEST_CHANGES`. The classifier (`src/worker/verdict.rs`) reads only
the bytes before the first newline, strips at most one trailing carriage return
so CRLF works, and byte-matches exactly those two strings. An empty first line
is `NoVerdictFound`; every other first line is `UnrecognizedForm`.

There is no whitespace or case normalization, Markdown decoration, synonym,
trailing explanation, or later-line tolerance. Later lines are opaque payload,
so hcom neither searches them for a verdict nor performs conflict detection.
This strictness is intentional: a false LGTM can advance a task incorrectly,
while a malformed or ambiguous first line safely enters the one clarification
path.

Undetermined gets **one** clarification: a fresh native invocation resuming the
same thread, recorded as a new attempt. It does not consume a review round, both
attempts keep their exact final artifacts, and the outcome carries the ordered
original and clarification paths plus the clarified verdict. A second failure
stops for a human; its successfully published paths remain in the snapshot.

## Repository observation

hcom checks only that `repository_root` is an existing directory. It never
opens Git, records a branch or revision, checks cleanliness, or drift-checks
the bound task/design documents. Source state and the appropriate review range
are for the Developer, active Reviewers, and human to establish from the original
files and repository.

## File-backed task and peer routing

The Architect binds each task with an absolute `repository_root`, absolute
`task_document_path`, ordered absolute `design_document_paths`, and exact
`task_selector`. hcom validates the typed shape and that the repository is an
existing directory. It does not open, copy, snapshot, hash, lock, or
drift-check the task/design files.

Every role reads those original files. Peer handoff is path-only:

- both initial reviews name the latest Developer `native-final.partial`;
- correction names both same-generation Reviewer logical-response path chains
  in stable Reviewer1-then-Reviewer2 order;
- both re-reviews name the latest corrected Developer final but no peer
  Reviewer evidence;
- clarification names only that Reviewer's original final.

There is no inline summary route, request/response manifest, or copied peer
body. A successful new role final replaces that role's current task pointer;
historical attempt artifacts stay on disk. The first Reviewer response never
starts Developer correction; both logical responses must join, and every
Developer amendment invalidates both prior verdicts.

Reviewer1 and Reviewer2 remain equal peers: the contract does not specialize
their roles or divide review categories. Each initial Reviewer turn completes a
task-derived invariant, caller/consumer, and failure/lifecycle pass across the
exact candidate range, continues after finding a blocker, performs a second
counterexample sweep, and consolidates all substantiated Major/Critical findings.
The final records the exact range and a concise coverage summary rather than the
internal checklist.

An exact-session re-review verifies that Reviewer's prior findings and audits
the amendment plus its transitive impact. Still-valid prior coverage may be
reused; every area invalidated by the amendment is reviewed again. A core
invariant, state-machine or externally visible contract change, a new caller or
concurrency/retry/cleanup/terminal path, a cross-subsystem amendment, or an
impact that cannot be bounded triggers a complete exact-range review. Otherwise
the Reviewer does not repeat unchanged low-risk coverage merely for ceremony.
The resulting verdict still applies to the current exact candidate range.

## Evidence

Durable artifacts live in `<project>/hcom-tasks/<run-id>/…`
(`src/orchestrator/workspace.rs`). It is hcom-owned handoff material, not a
tamper-proof boundary: a native-equivalent worker has the operator's ordinary
host access and can reach it. The raw `--output-last-message` target sits in
the per-run private runtime instead. Ingestion pins that file's identity,
enforces the hard size cap, validates non-empty UTF-8, copies all bytes exactly
to the attempt's durable `native-final.partial`, and removes the raw source.
The exact final does not pass through redaction, sensitive-value scanning,
lossy conversion, or a leading-window truncation. A transport or process
failure never publishes its path for routing.

Prompts are streamed as bounded artifacts rather than written as adapter control
files, so a legal full-size prompt cannot fail the turn — and the prompt is
**not** used to seed the redactor. It is an hcom-generated task description, not
a credential; seeding with it would replace prompt.md with `[REDACTED]` and
destroy reproducibility. Existing stdout/stderr diagnostic evidence still uses
the environment-backed redactor; that diagnostic policy does not alter agent
finals.

The workspace is handoff material, not a recovery store. A restarted hcom never
reads it to resume. The live in-memory snapshot carries only the latest
Developer path and the current-generation typed result/path chain for each
ordered Reviewer lane.

## Failures

`needs_human` keeps the runtime's sanitized detail alongside the class label
(`worker runtime process failed: codex exec exited with status 7; stderr tail: …`).
Collapsing failures to a fixed string destroys the only evidence of what broke —
the exact defect that made the previous protocol lane's failures unreadable.

Driver poll and reducer bookkeeping failures follow the same terminal
discipline. The local active-turn handle is cleared only after a cloned core
accepts the completion event; an error before that point closes the runtime,
writes a bounded decision-log diagnostic, and transitions the original core to
`needs_human`. The outer control loop has a final shutdown fallback for any
backend that returns a poll error while still non-terminal, and services the
pending `session_wait` before propagating containment failure.

Per-turn wall clock is 6 hours, monotonic, never reset by output; on expiry the
whole process group is terminated, evidence is drained and redacted, and the
turn fails as a timeout. If the group survives SIGKILL the failure says so and
the drain threads are left detached rather than joined — joining an unkillable
group would hang the supervisor itself.

## Testing

- **Unit** (`cargo test --lib`) — the flow state machine, the verdict grammar,
  and the runtime against fake CLI scripts covering exit codes, missing final
  messages, giant events, resume identity, clarification, timeout and secret
  redaction in diagnostic evidence, plus byte-exact Unicode/Markdown and fake
  secret-shaped agent finals.
- **Contract smokes** (`scripts/codex-exec-contract-smokes`) — the external
  behavior, against the native `codex` selected from `PATH`. Unit tests cannot
  cover these: a fake CLI reproduces whatever hcom already believes. Run before
  every release and after material CLI behavior changes. The environment probe
  reads only allowlisted synthetic variables; dumping the real environment
  would ship live credentials into the model context and to the provider. Its
  disposable native config selects the tool-command environment policy; hcom
  itself does not override that policy. These smokes default
  to `gpt-5.3-codex-spark` with `medium` reasoning and verify native global
  plus project AGENTS.md loading as well as explicit external-repository
  instruction discovery.
- **Real acceptance** (`cargo test --lib real_exec -- --ignored`) — six runs
  against the real binary on disposable projects: a single task, a Rust
  hello-world, a two-task run, **Gate 1**, review exhaustion, and a Linux-only
  abnormal worker exit. Gate 1 uses an explicit controlled lifecycle probe:
  its first task must go through REQUEST_CHANGES with an exact developer resume
  and exact reviewer re-review, its second task must be approved on the first
  review, and all role sessions must be fresh. The exhaustion run requires
  rejection through the dual-mode minimum `max_review_rounds=7`, proves exact
  correction and re-review resumes, and proves the next task still runs. The
  abnormal-exit run SIGKILLs
  only the Codex process whose `--output-last-message` target belongs to that
  fixture, then requires `needs_human`, no routed partial final, no Reviewer,
  and no surviving descendant. Gate 1 reads native thread ids back out of the
  sealed stdout evidence rather than trusting the runtime's own bookkeeping.
  Every run also asserts it left no stray worker processes behind.

Set `HCOM_REAL_E2E_KEEP=1` to preserve each disposable fixture after the test
for failure diagnosis. By default the fixtures are deleted. The controlled
review tasks and side-effect-free checks are intentional: model-backed
infrastructure tests must not depend on a Reviewer inferring an unstated staged
contract, and their own verification commands must not dirty the checkout.

### Reusable real-E2E entry points

The real-model coverage is intentionally opt-in and split by cost and purpose:

```bash
# Native Codex CLI/config/session/path contract; ten disposable probes.
SMOKE_MODEL=gpt-5.3-codex-spark \
SMOKE_EFFORT=medium \
SMOKE_TIMEOUT=280 \
scripts/codex-exec-contract-smokes

# Ordered tasks, mandatory correction, exact Developer/Reviewer resume,
# cross-task fresh sessions, and direct approval of the second task.
cargo test --lib \
  real_gate_one_review_loop_then_direct_approval_in_one_run \
  -- --ignored --nocapture --test-threads=1

# Review exhaustion must remain distinct from LGTM and advance to the next task.
cargo test --lib \
  real_review_exhausted_advances_to_the_next_task \
  -- --ignored --nocapture --test-threads=1

# Linux only: kill the exact disposable Developer and verify needs_human.
cargo test --lib \
  real_killed_developer_becomes_needs_human_without_routing_partial_final \
  -- --ignored --nocapture --test-threads=1
```

The Rust fixtures fix every real role to `gpt-5.3-codex-spark` with `medium`
reasoning; they do not inherit the production role defaults. Run the expensive
tests serially. Prefix a command with `HCOM_REAL_E2E_KEEP=1` only when retained
artifacts are useful: it deliberately disables the fixture's automatic
`TempDir` deletion and prints the retained root.

Reusable support lives in `real_support` beside the task-lane tests. It
provides disposable project/repository/run roots, `start` plus `drive` for
failure injection between supervisor polls, sealed-native-stdout thread ID
inspection, durable artifact assertions, and stray-worker checks. On Linux,
the abnormal-exit helper selects a worker only when it is a `codex exec` whose
`--output-last-message` path is below that fixture's private run root; it must
never target a process by name alone.

There is currently no aggregate `scripts/real-session-lane-e2e` wrapper. The
named tests above are the durable entry points and let an operator run only the
scenario whose model cost is justified. A future wrapper should remain
explicitly opt-in, preserve the Spark/medium defaults and serial execution,
report retained fixture paths, and summarize each scenario independently.

Claude worker contract, mixed-provider task-lane, exact-resume, exhaustion,
abnormal-exit, and Guardian lifecycle probes have separate serial opt-in
entry points in
[Claude task-lane test tooling](claude-task-lane-testing.md). Those tests
require explicit Haiku/medium selection and the exact inherited four-proxy
gate; they do not change these Codex/Spark defaults.

## Capability boundaries

What this lane deliberately does **not** guarantee. These are decided
positions, not open defects: a reviewer who finds one of them has found a
documented boundary, and re-litigating it needs a product decision, not a
patch. Each entry says what breaks, what stands in for it, and what it would
take to change.

### The local-candidate supervisor never inspects the work

It does not run checks, read commits, or look at Git at all. A developer that
misreports what it did, omits a sign-off, commits outside the task's scope,
rewrites history, or leaves the tree dirty will still reach review.

*Stands in for it:* each reviewer independently follows the fixed initial and
change-aware review contract, the human reads the final report, and real CI runs
after a separately authorized push. The local lane neither pushes nor installs,
so the worst case is a wasted review round or a local commit to revert — both
cheap. GitHub mode is deliberately different: its delivery adapter validates
append-only exact commit topology and identity before publishing, while still
leaving code quality judgment to the Reviewers.

*To change it:* that is a different product. Adding any of those checks back
turns an untidy checkout into a failed run, which is exactly the failure mode
this design removed.

### Git is not a concept the supervisor knows

Beyond "this path is an existing directory", hcom does not open, lock, or read
a repository. Two runs against one checkout are not prevented, a task whose
directory is not a repository at all is accepted, and the reviewer is given no
diff range — it works out what changed from the source, task/design files, and
the Developer's durable final file.

### `hcom-tasks/` is evidence, not a security boundary

A worker has the same native HOME/TMP and host filesystem authority as a
directly launched Codex process. It can therefore alter files under
`hcom-tasks/`, including its own `prompt.md` and stream logs.

*Consequence:* artifacts are trustworthy as a record of a cooperating worker,
not as tamper-proof audit. Do not build a security argument on them.

### Non-UTF-8 environment values expose an upstream native limitation

hcom inherits the parent environment byte-for-byte, including non-UTF-8 names
and values. An observed native Codex tool executor fails when it meets one, so
**every tool call can silently fail while the turn still exits 0**: the model
narrates progress and returns a confident summary having done nothing. This is
upstream native behavior, and hcom neither filters the parent environment nor
overrides native environment policy to work around it.

The byte-exact hcom-to-Codex process boundary is covered by unit tests.
`C4`/`C4b` separately verify that a native user
`shell_environment_policy.inherit = "all"` setting controls create and resume
tool commands. They do not claim that Codex itself supports non-UTF-8 tool
environments.

*To change it:* use a fixed upstream Codex. Filtering or rewriting the parent
environment inside hcom would violate native-equivalent semantics.

### `review_exhausted` still advances

A task whose reviewer keeps requesting changes until `max_review_rounds` is
marked `review_exhausted` and the run moves to the next task. It is never
disguised as an approval, but nothing stops the run.

### The interactive Architect launch is not covered by automated model tests

Gate 1 drives the supervisor and native exec workers directly. It does not
start the blank interactive Architect TUI, so foreground-terminal plumbing,
the launch gate, and native configuration interaction still need a human
smoke.

*Stands in for it:* a human starting `hcom arch codex` once in a disposable
terminal before release. It cannot be automated without either violating the
input-ownership rule (a script must never submit the first TUI prompt) or
bypassing the foreground launch path under test.

### Timeouts are coarse

A turn is killed after 6 hours of wall clock, monotonic and never reset by
output. A genuinely slow turn is indistinguishable from a hung one; a wedged
worker burns up to six hours before the watchdog fires. The foreground
supervisor reports the resulting terminal state through the pending
`session_wait`. The same wait returns only for a normal Developer result, each
normal Reviewer response, a Developer clarification/blocker action, or a
terminal state. Internal task completion, status publication, poll/timer ticks,
and transport yields do not release it. Worker-result progress exposes Reviewer
identity, generation, and received/expected response counts. The first response is
partial progress and the Architect immediately waits for the peer response
without reading the durable response body. The wait also returns for a latched
Developer clarification/blocker action. Progress is retained under a run-local
sequence across the short display/re-wait gap. An action survives an
interrupted wait and is immediately redelivered when the reconnect uses a
version older than the action's `published_version`; a same-version repeat is
rejected until the action is resolved.

A terminal snapshot supersedes queued progress and contains final worker
evidence. The final Reviewer response, derived task completion, and successful
terminal transition therefore produce one wakeup; an abnormal terminal
transition also releases the wait immediately.

The six-hour watchdog applies only to an active Developer or Reviewer turn.
`AwaitingArchitectAction` has no timeout: it may be waiting for a human
decision, consumes no resident worker process, and remains owned by the
foreground in-memory parent. Parent or terminal exit still cancels the run.

At terminal return, every task snapshot exposes
`latest_developer_final_path`, `review_round`, `review_generation`, and ordered
active Reviewer current-generation typed result/path chains, plus a bounded
clarification record count. Ordered clarification records are read separately
in pages of at most eight. Only then does the Architect read every active Reviewer's
non-empty current-generation evidence and report their original
verdicts/findings, distinguishing same-generation LGTM,
`review_exhausted`, and lifecycle failure. Neither MCP response shape embeds
either Reviewer body, and the Architect does not rerun tests, review, or
validation unless the human asks. For LGTM, it reports the final reviewed local
candidate without asking whether to retain/revert it or creating a post-LGTM
commit; local push/install/release remain separately authorized. GitHub mode
uses its separate terminal delivery handoff and bound publication contract.
