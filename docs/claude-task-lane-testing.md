# Claude task-lane test tooling

Real Claude tests are explicit, serial, headless opt-ins. Ordinary
`cargo test` uses fake executables and never makes a provider/network request.
No test launches the interactive Architect TUI.

The named task-lane scenarios below are the released protocol-v7
single-Reviewer evidence. They do not constitute protocol-v11 concurrent
dual-review acceptance, and the former 10/10 result must not be reported as
v9 evidence. The explicit v9 dual-review E2E definitions are listed below,
but they have not been authorized or executed: real-model v9 dual-review E2E
is **NOT RUN**.

## Mandatory caller environment

Every real entry requires the caller to set:

```bash
export CLAUDE_TEST_MODEL=haiku
export CLAUDE_TEST_EFFORT=medium

export http_proxy=http://127.0.0.1:7890
export https_proxy=http://127.0.0.1:7890
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
```

The model and effort must be explicitly and exactly `haiku`/`medium`.
Production remains `opus`/`xhigh`. The reusable Rust gate captures the raw
parent environment, rejects duplicate/non-UTF-8/missing/mismatched proxy
entries, and validates the two Claude policy pins before any native executable
lookup or spawn. The scripts and fixtures do not add, repair, normalize, or
override the four proxy variables. A failed profile or proxy gate therefore
has a Claude spawn count of zero, including before any version/help probe.

## Native contract smoke

```bash
scripts/claude-exec-contract-smokes
```

This runs one disposable create/resume contract using the native `claude`
selected from inherited `PATH`. It checks stream-json UUID identity, exact
resume, project plus external `CLAUDE.md`, a disposable native
project settings tree, SessionStart hook context, additive project MCP startup,
synthetic tool-environment inheritance, exact finals, Guardian cleanup, and
no retained model session in the source tree.

## Task-lane and lifecycle scenarios

Run one scenario at a time:

```bash
scripts/claude-task-lane-e2e mixed
scripts/claude-task-lane-e2e developer-resume
scripts/claude-task-lane-e2e claude-pair
scripts/claude-task-lane-e2e exhaustion
scripts/claude-task-lane-e2e abnormal-exit
scripts/claude-task-lane-e2e nested
scripts/claude-task-lane-e2e cancel
scripts/claude-task-lane-e2e timeout
scripts/claude-task-lane-e2e parent-death
scripts/claude-task-lane-e2e native-contract
```

`all` runs that list serially. Each scenario first builds the exact debug
`hcom` used as the private same-binary Guardian. Fixtures inherit the caller's
real native config/auth and add only disposable project-local settings/MCP
canaries where that contract is under test.

Coverage:

| Scenario | Contract |
|---|---|
| `mixed` | released-v7 Codex Developer + Claude Reviewer default, REQUEST_CHANGES, correction, exact Reviewer resume |
| `developer-resume` | Claude Developer + Codex Reviewer, exact Developer resume |
| `claude-pair` | Claude Developer + Claude Reviewer, cross-task fresh sessions |
| `exhaustion` | Claude rejection reaches `review_exhausted` and advances |
| `abnormal-exit` | exact fixture-owned Claude PID dies; no partial final or Reviewer route |
| `nested` | Bash tool setsid/double-fork descendant is reaped; success-shaped final is rejected |
| `cancel` | cancellation reaps an active escaped descendant tree |
| `timeout` | timeout reaps an active escaped descendant tree |
| `parent-death` | hcom-parent SIGKILL reaches Guardian PDEATHSIG cleanup |
| `native-contract` | native instructions/settings/hook/MCP/environment and create/resume transport |

These historical real scenarios cover all four released-v7
Developer/Reviewer pairs. Current deterministic tests cover the v9
Architect/Developer/Reviewer1/Reviewer2 profile matrix without making provider
calls. Real scenarios add model evidence only where native behavior matters.

## Protocol-v9 concurrent dual-review scenarios

Run one separately authorized scenario at a time:

```bash
scripts/dual-review-e2e strict-generation
scripts/dual-review-e2e exhaustion
scripts/dual-review-e2e reviewer-exit
scripts/dual-review-e2e parent-stop
```

`all` runs those four scenarios serially. Every scenario uses a Codex
Developer, Codex Reviewer1, and Claude Reviewer2. The strict-generation
scenario uses a two-party filesystem barrier in each generation to prove both
native Reviewer turns overlap. Generation 1 produces one LGTM and one
REQUEST_CHANGES, the Developer reads both ordered responses and amends the
single signed-off commit, and both Reviewer sessions exact-resume before
generation 2 can finish with dual LGTM. The exhaustion scenario proves a
synchronized `max_review_rounds=7` rejection advances to the next task. The
reviewer-exit scenario kills only the fixture-owned Claude Reviewer2 and
requires peer cancellation, `needs_human`, zero consumed review rounds, and no
residual process. The parent-stop scenario stops the foreground supervisor only
after both native Reviewer trees are active, then requires both trees to be
cleaned with a canceled, zero-round result.

The runner enforces explicit `haiku`/`medium`, never sets or repairs the proxy,
and uses `--test-threads=1`. Codex roles use the existing
`gpt-5.3-codex-spark`/`medium` test profile. It never invokes Opus or launches
an interactive TUI. Defining these ignored tests does not authorize running
them, a TUI, push, install, or release.

Set `HCOM_REAL_E2E_KEEP=1` to retain a failed disposable fixture and print its
path. The default removes fixtures automatically after completion. Process
selection is always rooted in a scenario-private cwd or exact private artifact
target; no test searches for or signals an existing user Claude/Codex session
by process name alone.
