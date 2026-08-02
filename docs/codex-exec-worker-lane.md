# Codex exec worker lane

The task worker lane hcom actually runs: one native `codex exec` process per
turn, no protocol conversation, and a supervisor that never judges the work.

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
verify independently at whatever depth they choose, and the human plus real CI
are the last word. hcom neither pushes nor installs, so a wrong judgment costs
a review round or a local commit, both cheap and revertible.

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
parent, ahead of `resume`, and is passed for both Developer and Reviewer when
the task repository differs from the project directory.

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
Developer/Reviewer prompt explicitly requires inspecting applicable
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
are for the Developer, Reviewer, and human to establish from the original
files and repository.

## File-backed task and peer routing

The Architect binds each task with an absolute `repository_root`, absolute
`task_document_path`, ordered absolute `design_document_paths`, and exact
`task_selector`. hcom validates the typed shape and that the repository is an
existing directory. It does not open, copy, snapshot, hash, lock, or
drift-check the task/design files.

Every role reads those original files. Peer handoff is path-only:

- initial review names the latest Developer `native-final.partial`;
- correction names the current Reviewer final path or ordered
  original-plus-clarification paths;
- re-review names the latest corrected Developer final;
- clarification names the original Reviewer final.

There is no inline summary route, request/response manifest, or copied peer
body. A successful new role final replaces that role's current task pointer;
historical attempt artifacts stay on disk.

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
Developer path and the current ordered Reviewer path or paths for each task.

## Failures

`needs_human` keeps the runtime's sanitized detail alongside the class label
(`worker runtime process failed: codex exec exited with status 7; stderr tail: …`).
Collapsing failures to a fixed string destroys the only evidence of what broke —
the exact defect that made the previous protocol lane's failures unreadable.

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
- **Real acceptance** (`cargo test --lib real_exec -- --ignored`) — four runs
  against the real binary on disposable projects: a single task, a Rust
  hello-world, a two-task run, and **Gate 1**: one run whose first task goes
  through a real REQUEST_CHANGES with an exact developer resume and an exact
  reviewer re-review, whose second task is approved on its first review, and
  whose four role sessions are all fresh. Every task must reach LGTM —
  exhaustion is not success. Gate 1 reads the native thread ids back out of the
  sealed stdout evidence rather than trusting the runtime's own bookkeeping. Every run also
  asserts it left no stray worker processes behind.

## Capability boundaries

What this lane deliberately does **not** guarantee. These are decided
positions, not open defects: a reviewer who finds one of them has found a
documented boundary, and re-litigating it needs a product decision, not a
patch. Each entry says what breaks, what stands in for it, and what it would
take to change.

### The supervisor never inspects the work

It does not run checks, read commits, or look at Git at all. A developer that
misreports what it did, omits a sign-off, commits outside the task's scope,
rewrites history, or leaves the tree dirty will still reach review.

*Stands in for it:* the reviewer verifies independently at whatever depth it
chooses, the human reads the final report, and real CI runs after a push. hcom
neither pushes nor installs, so the worst case is a wasted review round or a
local commit to revert — both cheap.

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
supervisor reports the resulting terminal state through the one pending
`session_wait`; a human who needs an earlier progress check can interrupt that
wait and explicitly request `session_status` or cancellation.

At terminal return, every task snapshot exposes
`latest_developer_final_path`, ordered `final_reviewer_message_paths`, and
`reviewer_verdict`. The Architect reads every non-empty Reviewer file in order
and reports its original verdict/findings, distinguishing LGTM,
`review_exhausted`, and lifecycle failure. Neither MCP response shape embeds
the Reviewer body, and the Architect does not rerun tests, review, or
validation unless the human asks.
