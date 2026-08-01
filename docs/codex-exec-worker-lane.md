# Codex exec worker lane

The task worker lane hcom actually runs: one pinned `codex exec` process per
turn, no protocol conversation, and a supervisor that never judges the work.

## The two rules

**Strict about capability, lenient about output.** A turn's powers are fixed
before it starts — argv, environment, mount namespace. Afterwards hcom observes
the world (process exit, one documented stdout event, one native output file)
and relays bytes. Model text is payload, never protocol.

**The supervisor is task-agnostic.** It sequences processes and carries
messages between them. It does not run checks, inspect commits, or judge
whether the work is any good. Developers verify their own work, reviewers
verify independently at whatever depth they choose, and the human plus real CI
are the last word. hcom neither pushes nor installs, so a wrong judgment costs
a review round or a local commit, both cheap and revertible.

## Invocation

`ExecTaskWorkerRuntime` (`src/worker/exec_runtime.rs`) builds one closed shape.
Ordering is load-bearing: `--sandbox`, `--skip-git-repo-check` and `--add-dir`
belong to the `exec` parent and precede `resume`.

```text
codex exec
  --sandbox danger-full-access
  --skip-git-repo-check                     # project cwd may be non-Git
  [--add-dir <task repository>]             # external/nested repository scope
  [resume <exact thread id>]                # same task only; never with --cd
  --json                                    # only thread.started is parsed
  --strict-config
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --config approval_policy="never"
  --config mcp_servers={}
  --config shell_environment_policy.inherit="all"
  --config shell_environment_policy.ignore_default_excludes=true
  --ignore-user-config
  --ignore-rules
  --disable <each closed disabled feature>
  [--cd <project root>]                     # create only
  --output-last-message <private raw target>
  -                                         # bounded stdin prompt, then EOF
```

`--cd` applies to create turns only. A **resume takes no `--cd` and inherits
the process working directory**, so every invocation is launched from the
project directory (bwrap `--chdir`, or `Command::current_dir` when unsandboxed)
— otherwise a resumed turn would silently work somewhere else. `--add-dir`
belongs to the exec parent, ahead of `resume`, and is passed only for a
developer whose task repository is not the project directory itself.

`--ignore-user-config` makes argv the single source of configuration truth
(Codex writes its own `config.toml` into the private `CODEX_HOME`). That is why
the two `shell_environment_policy` entries must travel as `--config`: without
them Codex filters KEY/SECRET/TOKEN-shaped names out of the environment its
tool commands see, which would silently break complete parent inheritance.

## What hcom parses

Exactly two things, and nothing else:

1. **`thread.started.thread_id`** — the first stdout line of a `--json` run.
   This is the session identity proof. After the first valid one, JSON parsing
   stops for the life of the turn and the remaining stdout is drained as raw
   bytes into evidence, so an arbitrarily large later event can neither grow a
   line buffer nor fail a turn. A resume that returns a different `thread_id`
   fails closed; identity is never assumed.
2. **The reviewer's verdict line** — see below.

Everything else the model writes is relayed verbatim (after redaction) or
stored as evidence.

## Routing preconditions

A turn routes onward only when all three hold:

- the process exited 0;
- a `thread.started` proof was captured;
- the native final-message file is non-empty.

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
`VERDICT: REQUEST_CHANGES`. The classifier (`src/worker/verdict.rs`) tolerates
drift from that, but only in the safe direction:

- tokens must be anchored at the normalized line start or directly after
  `VERDICT:`, with word boundaries — `NOT LGTM`, `I cannot APPROVE` and
  `The developer claims LGTM, but…` therefore never match;
- the LGTM direction accepts only a closed tail allowlist (empty, non-question
  punctuation, `with minor comments`, `with non-blocking comments`);
- the REQUEST_CHANGES direction accepts any tail, because a false
  REQUEST_CHANGES costs one round while a false LGTM advances a task wrongly;
- conflicts and anything unrecognized are undetermined.

Undetermined gets **one** clarification: a fresh native invocation resuming the
same thread, recorded as a new attempt. It does not consume a review round, both
attempts keep their artifacts, and the relayed outcome carries the original
findings *and* the clarified verdict. A second failure stops for a human with
the full text.

## Repository observation

Exactly two, both routing data: the head at task start (the reviewer's diff
base) and the head at developer completion. Review takes **no** observation at
all — whether the tree drifted, went detached, or is dirty is the reviewer's
and the human's judgment, and observing it here would turn an untidy checkout
into a failed run.

## Evidence

Durable evidence lives in `<project>/hcom-tasks/<run-id>/…`
(`src/orchestrator/workspace.rs`), which only hcom writes: workers have no
writable mount anywhere in that tree. The raw `--output-last-message` target
sits in the per-run private runtime instead, so if hcom is killed between the
CLI writing it and hcom ingesting it, no unredacted bytes ever land in the
project directory. Ingestion reads the file bounded, redacts it, writes the
sealed copy atomically, and deletes the raw one.

The final message is redacted by **streaming the whole file**: memory is
bounded, the file is not, so a legal long message keeps its tail. Chunks overlap
by `max-secret-length` bytes so a credential straddling a read boundary is still
matched. Only a leading window is quoted onward; the sealed artifact holds
everything.

Prompts are streamed as bounded artifacts rather than written as adapter control
files, so a legal full-size prompt cannot fail the turn — and the prompt is
**not** used to seed the redactor. It is an hcom-generated task description, not
a credential; seeding with it would replace prompt.md with `[REDACTED]` and
destroy reproducibility. Real secrets come from the environment inventory and
are redacted everywhere regardless.

The workspace is handoff material, not a recovery store. A restarted hcom never
reads it to resume; a human reads `latest/`, then authorizes a new run.

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
  redaction.
- **Contract smokes** (`scripts/codex-exec-contract-smokes`) — the external
  assumptions, against the real pinned binary. Unit tests structurally cannot
  cover these: a fake CLI reproduces whatever hcom already believes. Run before
  every release and after every pin bump. A known upstream block exits 2 and
  fails the gate: releasing over it needs an explicit human decision
  (`SMOKE_ACCEPT_KNOWN_BLOCKS=1`) to narrow the inheritance requirement. The environment probe reads only
  allowlisted synthetic variables; dumping the real environment would ship live
  credentials into the model context and to the provider.
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
diff range — it works out what changed from the source and the developer's
report.

*To change it:* re-introducing a repository lock is small; re-introducing diff
ranges means the supervisor must observe HEAD again, with all the drift
questions that follow.

### `hcom-tasks/` is evidence, not a security boundary

A worker's own writable scopes (its task repository, its private HOME/TMP)
overlap the evidence tree when the project *is* the repository. A worker can
therefore alter files under `hcom-tasks/`, including its own `prompt.md` and
stream logs.

*Consequence:* artifacts are trustworthy as a record of a cooperating worker,
not as tamper-proof audit. Do not build a security argument on them.

*Known narrow gap:* the supervisor opens `decision.log` by path, so a worker
that replaced it with a symlink could aim the supervisor's own (larger)
authority at a file outside the sandbox. Closing it means `openat` from a
pinned directory fd, `O_NOFOLLOW`, and an inode check before every write —
worth doing, not yet done.

### Non-UTF-8 environment values break the pinned Codex

hcom inherits the parent environment byte-for-byte, including non-UTF-8 names
and values. Codex 0.146's tool executor panics on `std::env::vars()` when it
meets one, so **every tool call silently fails while the turn still exits 0**:
the model narrates progress and returns a confident summary having done
nothing. This is upstream, and hcom does not work around it.

*Stands in for it:* contract smoke `C4c` probes it and reports `BLOCK` — never
a pass — so a release cannot claim complete inheritance while it stands.
Ordinary UTF-8 variables (`GH_TOKEN`, proxies, secret-shaped names, empty
values, case pairs) are covered by `C4`/`C4b` and do inherit correctly, on both
create and resume.

*To change it:* either a fixed upstream Codex, or a human decision to narrow
the inheritance requirement (recorded by running the smokes with
`SMOKE_ACCEPT_KNOWN_BLOCKS=1`).

### `review_exhausted` still advances

A task whose reviewer keeps requesting changes until `max_review_rounds` is
marked `review_exhausted` and the run moves to the next task. It is never
disguised as an approval, but nothing stops the run.

### The outer Architect namespace is not covered by automated tests

Gate 1 drives the supervisor and the worker's inner bwrap directly. It does not
start the Architect's outer bwrap, so mount ownership, protected surfaces, and
long `TMPDIR` shapes are unverified by CI — and those have broken real runs
before.

*Stands in for it:* a human starting `hcom arch codex` once in a disposable
terminal before release. It cannot be automated without either violating the
input-ownership rule (a script must never submit the first TUI prompt) or
bypassing the very namespace under test.

### Timeouts are coarse

A turn is killed after 6 hours of wall clock, monotonic and never reset by
output. A genuinely slow turn is indistinguishable from a hung one; a wedged
worker burns up to six hours before the watchdog fires. Faster detection is
meant to come from the 180–300 s status pulse and a human cancelling.
