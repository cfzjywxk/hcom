# Codex Architect adapter maintenance contract

This is the maintainer contract for the native Codex blank Architect started
by `hcom arch codex`. A background role configured for Codex uses the native
Codex exec provider documented in
[codex-exec-worker-lane.md](codex-exec-worker-lane.md); a Claude role is routed
to its independent native provider. Existing tagged
interactive hcom products are independent and must not be routed through this
session lane.

## Product rule

hcom is a thin automation layer over a native Codex launch. It owns process
lifetime, task-control transport, typed file bindings, durable peer-final
paths, exact resume, and diagnostic evidence. It does not replace the
operator's Codex installation semantics with a generated HOME, CODEX_HOME,
config, trust decision, MCP allowlist, feature allowlist, or reduced host
filesystem.

The deliberate exceptions are:

- any selected Codex Architect, Developer, or Reviewer model/effort defaults
  are `gpt-5.6-sol` and `xhigh`, passed explicitly rather than inherited from
  user config;
- typed sandbox/approval values are also explicit;
- the Architect gets one hcom-owned task-control MCP table;
- Architect and worker parent environments, including `HCOM_DIR`, remain
  byte-for-byte unchanged; hcom adds or replaces no environment entry;
- exec workers add the transport required for thread identity/final-message
  capture.

## Native executable and typed profile

- Architect and worker launches execute the program name `codex` with the
  complete parent environment, so normal `PATH` resolution selects the native
  installation at each process start.
- hcom does not pin an absolute Codex path, CLI version, or executable hash. It
  does not freeze executable identity across a session or reject a CLI because
  a help probe differs. A missing or incompatible CLI fails that launch/turn
  through the normal process error path.
- Model names are 1–128 ASCII bytes, cannot begin with `-`, and may contain
  only letters, digits, `.`, `_`, `-`, `/`, `:`, or `@`.
- Reasoning effort is one of `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`.
- Sandbox is `read-only`, `workspace-write`, or `danger-full-access`.
- Approval policy is `untrusted`, `on-request`, or `never`.
- Profile TOML is a closed schema with no arbitrary argv field. Role tables
  are partial overrides merged onto built-in defaults (`reasoning_effort` and
  `effort` are Codex aliases); the effective typed profile and SHA-256 hash are
  frozen before launch and bound into the approved plan.

## Blank Architect invocation

hcom supplies no positional prompt, stdin content, PTY injection, paste, key
event, or Enter. The Codex argv is:

```text
codex
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --sandbox <typed sandbox>
  --ask-for-approval <typed policy>
  --cd <exact project root>
  --no-alt-screen
  --config 'mcp_servers.hcom_session_task_control={ ...exact relay... }'
```

That last override replaces only the hcom-reserved MCP leaf. Every other
native MCP server remains loaded. Replacing the whole leaf prevents a stale
user table with the reserved name from merging incompatible transport fields
into the relay.

The Architect inherits the complete parent environment and uses the real
HOME/CODEX_HOME. Consequently native config.toml, AGENTS.md, project
instructions, trust, auth, rules, hooks, skills, plugins, feature flags, MCP
servers, custom providers, caches, and session history behave like a direct
Codex invocation. hcom does not pass `--strict-config`, disable features, or
write a private Codex config.

The Architect is spawned directly as `codex` from the project directory. hcom
does not insert bubblewrap or another launcher, rebuild the environment, change
`HCOM_DIR`, create namespaces/mounts, or run Codex HOME/auth/session-store
preflights. Parent-death handling and PID registration only tie orchestration
lifetime and the task-control relay to this child.

## No Architect session binding

The task-control relay authenticates its directly spawned Architect and bridge
processes; it does not bind, freeze, or register a Codex session identity.
hcom does not enumerate or parse `CODEX_HOME/sessions`, count rollout files,
inspect `auth.json`, or require a unique native Codex rollout. Concurrent or
unusual native session history therefore cannot block a direct Codex launch or
its first task-control call.

## Background Codex workers

Both `hcom arch codex` and `hcom arch claude` bind one provider-routed worker
lane bundle. The foreground Architect adapter does not change the independently
configured worker adapters. The defaults are Codex Developer, Codex Reviewer1,
and Claude Reviewer2. Canonical `[architect.reviewer1]` and
`[architect.reviewer2]` tables independently select either provider; a
legacy-only `[architect.reviewer]` profile is copied completely to both lanes
in dual mode, or applied once to Reviewer1 with `--single-review`. The latter
flag is available only to the Codex Architect, rejects an explicit
`[architect.reviewer2]` table, and otherwise keeps normal Reviewer1 overrides.
A selected unavailable provider fails closed without fallback.

The exec lane:

- launches directly from the project directory with the complete parent
  environment and real native config;
- passes `--add-dir <task repository>` to every Codex task worker when source
  is outside the project;
- tells every task worker to inspect applicable project and repository AGENTS.md,
  AGENTS.override.md, and nested instructions before work;
- proves create/resume identity from `thread.started.thread_id`;
- captures final output with `--output-last-message`, validates it, and
  publishes the exact durable `native-final.partial` path without redacting or
  copying its body into another prompt;
- keeps Reviewer non-mutation as a role contract, not an OS read-only mount;
- preserves parent `HCOM_DIR` and keeps bounded lifecycle/reaping plus redacted
  stdout/stderr diagnostic evidence.

See the exec-lane document for exact argv ordering, verdict classification,
artifact bounds, and contract smokes.

## File-backed task and terminal contract

An approved task binds `repository_root`, `task_document_path`, ordered
`design_document_paths`, and `task_selector`. The adapter transports those
exact strings; it does not read, snapshot, hash, lock, or drift-check the
documents. Developer and every active Reviewer read the originals.

Developer-to-Reviewer fan-out, every Reviewer-to-correction handoff,
correction-to-active-reviews, and per-Reviewer verdict clarification contain only
durable final-message paths. Reviewers never receive peer evidence. There is
no peer-body summary or inline/file dual route. The in-memory v9 terminal
snapshot exposes each task's latest Developer path plus ordered active
Reviewer current-generation identities, verdicts, and path chains. Both MCP
response representations contain that metadata but never a Reviewer body.

Developer clarification/blocker requests also route only through durable
paths. Each accepted clarification becomes ordered task runtime evidence and
is supplied by path to every later Developer and Reviewer turn; it does not
change the approved plan hash. `session_wait` is bound to the exact current run
ID and a run-local progress sequence. It returns one retained review-request,
per-Reviewer response, or task-completion event, a latched action, or a
terminal state. Review-request events carry the exact Developer final path read
by every active Reviewer, the approved task/design paths and selector, current
generation, and ordered session-level Reviewer bindings. Review-response
events carry exact Reviewer identity, generation, verdict, response counts,
Developer path, and that Reviewer's ordered path chain. A response is partial
while fewer responses have arrived than the active topology expects. Task
completion carries every active current-generation typed result; no peer body is copied into any
event. The Architect displays each event without reading response bodies and
re-waits with its sequence. Events created between waits are retained, and
queued progress—including every response event—is delivered before terminal.

Each action has a `published_version`: an interrupted client can recover it by
waiting from an older version in that run, while a repeated wait at the
published version is rejected until the action is resolved. A pending action
takes priority over queued progress. Status snapshots carry the bounded active
worker list, session-level Reviewer bindings, and current-generation typed
Reviewer results; for clarifications they carry only each task's count. The
ordered clarification records are available through run-bound pages of at most
eight. The runtime enforces 64 records per task and 1280 per run
independently of whether an answer is Architect-derived or human-confirmed. At
terminal return, the Architect reads every active Reviewer's non-empty
current-generation final chains in binding order and delivers the original
verdicts and findings. It distinguishes same-generation LGTM,
`review_exhausted`, and lifecycle failure and does not rerun tests, review, or
validation unless the human explicitly asks. For LGTM, the final signed-off
local Developer candidate commit is already approved execution output reviewed
at its exact range by every active Reviewer. The Architect reports it without asking
whether to retain or revert it and without creating a post-LGTM commit.
Push, install, and release remain separate authorizations.

A bare generic implement/proceed/finish/drive request selects delegation but
does not by itself authorize starting a run; approval is limited to the exact
displayed-plan, named-plan, and plan-then-execute forms. The fixed Developer
contract requires the one candidate commit and its amendments to retain a
matching `Signed-off-by` trailer, which every active Reviewer checks. If an explicit
no-commit instruction conflicts with that contract after start, the per-turn
Developer output contract requires `CLARIFICATION_REQUIRED` without repository
modification. The Architect must call `session_clarification_require_human`
regardless of autonomous budget and cannot submit an Architect-derived
override.

The terminal run stays immutable. After its evidence handoff, a later human
request may use `session_run_begin` to allocate a new run ID under the same
foreground Architect. The new run resets task and logical worker identity but
keeps a monotonically increasing session version and the frozen
project/profile binding. Its plan hash is run-bound, and it still needs a fresh
plan plus explicit approval before any worker starts.
The first approved run acquires the project `hcom-tasks` ownership lock. That
project lease remains held by the foreground supervisor across every terminal
run and `session_run_begin`; per-run evidence directories are claimed
separately, and only the foreground parent exit releases the lease.

## Test map

| Contract | Regression |
|---|---|
| defaults are explicit | `architect::profile::tests::missing_file_uses_reviewed_defaults`, `worker::profile::tests::task_lane_defaults_to_codex_developer_and_claude_reviewer` |
| no prompt or input injection | `architect::launch::tests::native_profile_has_no_prompt_or_secret_transport`, `blank_codex_launch_keeps_input_empty_and_preserves_native_host_semantics` |
| native config plus one MCP leaf | `architect::launch::tests::codex_control_server_is_an_additive_cli_overlay_not_a_private_config` |
| native worker argv/config | `worker::exec_runtime::tests::happy_developer_turn_completes_and_captures_thread_id`, `reviewer_registers_the_external_repository_as_a_native_workspace_root` |
| byte-exact native environment with no additions | `orchestrator::task_lane::tests::complete_parent_environment_is_preserved_byte_for_byte` |
| path-only peer and terminal handoff | `orchestrator::task_lane::tests::request_changes_round_routes_only_ordered_durable_paths`, `architect::bridge::tests::session_wait_keeps_mcp_responsive_and_returns_terminal_result` |
| retained progress paths and wait priority | `orchestrator::core::tests::progress_events_preserve_review_paths_rounds_and_terminal_order`, `control_api::supervisor::tests::a_progress_event_releases_an_already_pending_wait`, `control_api::supervisor::tests::pending_action_precedes_progress_and_progress_precedes_terminal`, `architect::bridge::tests::progress_result_reaches_both_mcp_response_representations_without_peer_body` |
| sequential immutable runs in one Architect | `orchestrator::core::tests::terminal_core_creates_a_fresh_run_without_mutating_terminal_evidence`, `orchestrator::task_lane::tests::one_foreground_supervisor_runs_two_immutable_runs_with_fresh_workers`, `control_api::supervisor::tests::terminal_run_begin_creates_a_new_run_and_old_wait_identity_is_rejected` |
| project ownership survives run transition | `orchestrator::task_lane::tests::one_foreground_supervisor_runs_two_immutable_runs_with_fresh_workers`, `orchestrator::workspace::tests::a_second_process_cannot_open_the_project_workspace_while_it_is_locked` |
| bounded clarification control plane | `orchestrator::core::tests::status_snapshot_is_bounded_and_clarification_records_are_exactly_paginated`, `control_api::codec::tests::maximum_clarification_page_stays_within_the_control_response_frame`, `orchestrator::core::tests::clarification_capacity_exhaustion_terminalizes_instead_of_latching_more_state` |
| clarification artifact failure closes the run | `orchestrator::task_lane::tests::preexisting_clarification_artifact_terminalizes_instead_of_wedging_the_run` |
| real CLI assumptions | `scripts/codex-exec-contract-smokes` |

The standard source gate is:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --quiet --locked --all-targets
git diff --check
cargo build --quiet --release --locked
```

The model-backed E2E/contract default is
`gpt-5.3-codex-spark` with `medium` reasoning; production defaults remain
`gpt-5.6-sol`/`xhigh`.

Before changing argv, the configuration overlay, or session observation:

1. cover blank Architect, Developer, Reviewer, create, and exact resume where
   applicable;
2. keep hcom-owned config to the smallest exact leaf—never replace the whole
   native user config;
3. run targeted tests, then the full source gate;
4. do not automate a real Architect TUI by submitting its first prompt.
