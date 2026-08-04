# Claude task-lane test tooling

Real Claude tests are explicit, serial, headless opt-ins. Ordinary
`cargo test` uses fake executables and never makes a provider/network request.
No test launches the interactive Architect TUI.

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
| `mixed` | default Codex Developer + Claude Reviewer, REQUEST_CHANGES, correction, exact Reviewer resume |
| `developer-resume` | Claude Developer + Codex Reviewer, exact Developer resume |
| `claude-pair` | Claude Developer + Claude Reviewer, cross-task fresh sessions |
| `exhaustion` | Claude rejection reaches `review_exhausted` and advances |
| `abnormal-exit` | exact fixture-owned Claude PID dies; no partial final or Reviewer route |
| `nested` | Bash tool setsid/double-fork descendant is reaped; success-shaped final is rejected |
| `cancel` | cancellation reaps an active escaped descendant tree |
| `timeout` | timeout reaps an active escaped descendant tree |
| `parent-death` | hcom-parent SIGKILL reaches Guardian PDEATHSIG cleanup |
| `native-contract` | native instructions/settings/hook/MCP/environment and create/resume transport |

The existing deterministic provider-router test covers all four
Developer/Reviewer pairs, and the Architect profile/CLI matrix covers both
Architect adapters across those pairs. Real scenarios add model evidence only
where native behavior matters.

Set `HCOM_REAL_E2E_KEEP=1` to retain a failed disposable fixture and print its
path. The default removes fixtures automatically after completion. Process
selection is always rooted in a scenario-private cwd or exact private artifact
target; no test searches for or signals an existing user Claude/Codex session
by process name alone.
