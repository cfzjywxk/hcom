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

## Evidence

Durable evidence lives in `<project>/hcom-tasks/<run-id>/…`
(`src/orchestrator/workspace.rs`), which only hcom writes: workers have no
writable mount anywhere in that tree. The raw `--output-last-message` target
sits in the per-run private runtime instead, so if hcom is killed between the
CLI writing it and hcom ingesting it, no unredacted bytes ever land in the
project directory. Ingestion reads the file bounded, redacts it, writes the
sealed copy atomically, and deletes the raw one.

The workspace is handoff material, not a recovery store. A restarted hcom never
reads it to resume; a human reads `latest/`, then authorizes a new run.

## Failures

`needs_human` keeps the runtime's sanitized detail alongside the class label
(`worker runtime process failed: codex exec exited with status 7; stderr tail: …`).
Collapsing failures to a fixed string destroys the only evidence of what broke —
the exact defect that made the previous protocol lane's failures unreadable.

Per-turn wall clock is 6 hours, monotonic, never reset by output; on expiry the
whole process group is terminated, evidence is drained and redacted, and the
turn fails as a timeout.

## Testing

- **Unit** (`cargo test --lib`) — the flow state machine, the verdict grammar,
  and the runtime against fake CLI scripts covering exit codes, missing final
  messages, giant events, resume identity, clarification, timeout and secret
  redaction.
- **Contract smokes** (`scripts/codex-exec-contract-smokes`) — the external
  assumptions, against the real pinned binary. Unit tests structurally cannot
  cover these: a fake CLI reproduces whatever hcom already believes. Run before
  every release and after every pin bump. The environment probe reads only
  allowlisted synthetic variables; dumping the real environment would ship live
  credentials into the model context and to the provider.
- **Real acceptance** (`cargo test --lib real_exec -- --ignored`) — a single
  task and a two-task run against the real binary on a disposable project.

Known upstream limitation: Codex 0.146's tool executor panics when the
inherited environment contains a non-UTF-8 value (`std::env::vars()` unwrap),
which fails every tool call while the turn still exits 0. hcom inherits the
parent environment byte-for-byte and does not work around this.
