# Codex App Server task-runtime contract

This document freezes the provider boundary introduced in development phase P0
and its phase-P3 production integration. `hcom arch codex` uses this task-local
runtime for background Developer and Reviewer turns. The retained CLI adapters
remain available to the unchanged `hcom arch claude` lane and existing hcom
products.

## Fixed identity

- CLI: `codex-cli 0.146.0`
- executable SHA-256:
  `2e863156ed35ecc5253b1e2f907a9143077b9f7cb51942070c61996471ff6e04`
- generated stable v2 schema exemplar SHA-256:
  `8a1e451c6244f9d954cc2b19aeef2cb33b03fbcd33d21002bd8875e4ead4bd40`
- canonical stable v2 schema SHA-256:
  `2f402b7d1356adccc1a4785c0656db457578ca9ea5d5b08953487a410c630ce8`

Codex 0.146 emits semantically identical schema definitions in nondeterministic
map order. A fresh raw bundle can therefore have a different byte hash. Sorting
all JSON object keys with `jq -S -c` makes the preserved exemplar and a fresh
generation byte-identical and produces the canonical hash above. Runtime
preflight uses the canonical hash; the raw exemplar remains provenance.

The selected stable method inventory is closed to:

- `initialize`
- `initialized`
- `thread/start`
- `turn/start`
- `turn/interrupt`
- `turn/completed`
- `item/completed`

No experimental API capability is enabled.

The corresponding field inventory is frozen in
`RuntimeContractIdentity::codex_app_server_0_146`. It covers only client
identity/notification opt-out, role-thread cwd/profile/instructions/ephemeral
state, turn thread/input/cwd/profile/output schema, returned native IDs,
terminal status/items, and interrupt identity. Adding another field requires
an explicit contract revision and schema rebaseline.

## Provider-neutral seam

`TaskWorkerRuntime` exposes only:

- a fresh logical role session;
- a turn on an exact logical session;
- pending, completed, or failed polling;
- cancellation;
- shutdown.

The seam carries hcom-owned opaque session and turn keys, an exact runtime
profile, a bounded prompt, and either `DeveloperV1` or `ReviewerV1`. JSON-RPC
IDs, Codex thread/turn IDs, process handles, stdio, notifications, and provider
raw events remain private to the Codex implementation.

P0 introduced only the seam and `FakeTaskWorkerRuntime`; the production
Codex implementation and effect driver described below were added in later
reviewed phases.

## Exact default profiles

Developer and Reviewer both resolve to:

```text
provider           codex-app-server
model              gpt-5.6-sol
reasoning_effort   xhigh
sandbox            danger-full-access
approval_policy    never
```

The intended native semantics are equivalent to:

```bash
codex --sandbox danger-full-access \
  --ask-for-approval never \
  --model gpt-5.6-sol \
  --config 'model_reasoning_effort="xhigh"'
```

The provider mapping is also frozen as typed thread/turn views:

```text
thread model             gpt-5.6-sol
thread sandbox           danger-full-access
thread approvalPolicy    never
turn model               gpt-5.6-sol
turn effort              xhigh
turn sandboxPolicy.type  dangerFullAccess
turn approvalPolicy      never
```

The new lane accepts only Codex worker profiles using
`danger-full-access`/`never`. Claude, legacy CLI, and unknown providers fail
closed with an unsupported diagnostic. This does not remove or modify the
retained Claude and Codex CLI adapters.

## Closed outcomes

`DeveloperV1` has exactly `status`, `summary`, and `questions`.
`ready` requires no questions; `needs_human` requires at least one.

`ReviewerV1` has exactly `verdict`, `summary`, and `findings`. `lgtm` requires
no findings; `request_changes` requires at least one unique Major finding.
Finding paths, when present, are normalized repository-relative paths.

Both outcomes have a 64 KiB encoded bound plus field/count bounds enforced in
Rust after deny-unknown deserialization. Provider output cannot supply Git
authority; the Supervisor observes repository state separately.

## Pure SupervisorCore

`SupervisorCore::reduce(event)` is a transactional deterministic reducer. Every
mutating event carries the exact session version, advances it once on success,
and returns an ordered effect list ending in `PublishStatus`. Rejection leaves
the core unchanged. `StatusRequested` is a pure read: it returns no effect and
does not advance the version.

The core owns:

- exact plan version/hash and ordered task binding;
- one current task and one pending or active operation;
- fresh cross-task logical session identities and same-task reuse;
- globally at-most-once completion tokens;
- Developer completion recovery, review rounds, and terminal task ordering;
- normalized repository checkpoints and final Reviewer Git equality;
- closed status snapshots and stable bounded domain errors.

`TurnFailed` retains the complete provider-neutral
`SanitizedRuntimeFailure`, including its retryability bit. Only a retryable
Developer contract failure (the missing/invalid structured-final-result case)
may request `DeveloperRecoveryPreflight`; identity, branch, ancestry and the
changed-path allowlist must pass before the core schedules one
`DeveloperCompletionRecovery` turn on the same logical session. A second such
failure, every non-retryable failure and every Reviewer failure fails closed.
`DriverFailed` separately normalizes repository, runtime-factory,
task-environment, runtime-contract, and cleanup failures. A successful task
transition is committed only after its task runtime shuts down successfully;
cleanup failure leaves the session at a sanitized `needs_human` terminal.

It does not own filesystem, Git, process, clock, network, terminal, or provider
transport I/O. The current matrix has explicit rows for all 91
`SessionState × SupervisorEventKind` combinations and all 56
`TaskState × relevant lifecycle event` combinations. Ordered pure journeys
cover one/two task completion, different and shared repositories,
request-changes/re-review, exact review exhaustion, safe completion recovery,
identity/duplicate/race failures, and all closed plan/task bounds.

## Production topology and effect driver

The blank foreground Architect remains the existing interactive Codex CLI. It
owns no background-worker stdio and receives only the private bounded
session-control API. After exact plan binding and human execution authority,
the in-process `AppServerSessionSupervisor` drives the pure core effects:

```text
typed control request
  -> SupervisorCore::reduce(normalized event)
  -> ordered SupervisorEffect list
  -> AppServerSessionSupervisor local Git/process/environment action
  -> normalized event back into SupervisorCore
```

The driver, not the model or provider, captures canonical repository identity,
the attached branch, exact HEAD/ancestry, tracked and index diff hashes,
non-ignored-untracked hash, clean state, and the bounded changed-path
inventory. It double-captures repository observations and rejects unstable
evidence. Repository locks are acquired while a plan is staged; a failed plan
replacement leaves the previous plan and locks unchanged.

After the approved task-start observation, the driver lazily creates one
task-private process. The process opens one fresh native Developer thread and
one fresh native Reviewer thread and permits exactly one in-flight turn.
Correction, re-review, or the one Developer completion-recovery turn uses the
same native thread for that role. A terminal task shuts down the process before
the next task is observed or opened; cleanup failure prevents a false LGTM or
review-exhausted transition and yields sanitized `needs_human`.

An explicit cancel interrupts the active turn and closes the task process.
Parent exit performs the same owned-process-group cleanup. App Server crash,
EOF, timeout, protocol drift, identity mismatch, repository drift, invalid
Reviewer outcome, or persistent Reviewer mutation fails closed. No daemon,
Project Store, global App Server, restart, or cross-Architect-session recovery
exists.

## Task-private environment and Reviewer invariant

The task process starts from the complete parent OS environment snapshot:
unknown names, secret-shaped names, upper/lower-case proxy pairs, empty values,
and non-UTF-8 names/values remain byte-exact. Only the documented
task-private HOME, CODEX_HOME, HCOM_DIR, TMP, XDG/cache/runtime, Cargo/Rustup,
and run/task identity entries are replaced. Environment values do not grant
filesystem access by themselves.

The outer bubblewrap namespace exposes the exact canonical task repository
read-write plus pinned system/toolchain inputs and task-private state. It hides
the host root, live hcom/Codex/Claude control surfaces, sibling session roots,
and unrelated HOME state. Stdio is pipes, not a TTY; `/dev/tty` is unavailable.
The private Codex config contains an empty `mcp_servers` table, inherits the
materialized environment, and records only the exact repository as
`trust_level = "untrusted"`.

Developer and Reviewer deliberately receive the same writable namespace and
exact runtime profile. This allows the Reviewer to build, test, and create
ignored caches normally. The Reviewer role instructions prohibit persistent
source, index, branch, or HEAD changes. Independently of that instruction, the
Supervisor captures pre/post Git evidence and accepts a verdict only when
repository identity, branch, HEAD, tracked diff, index diff,
non-ignored-untracked state, and clean state are exactly equal. A Reviewer
dirty file, stage, commit, branch change, or other residue ends the run at
`needs_human`; ignored cache output is accepted.

## Resource and semantic bounds

Transport and semantic bounds remain separate:

| Layer | Bound |
|---|---:|
| control request / response | 256 KiB each |
| ordered tasks | 64 |
| review rounds per task | 1–20 |
| runtime prompt | 256 KiB |
| role instructions / encoded outcome | 64 KiB each |
| summary / question count / finding count | 8192 chars / 8 / 32 |
| runtime/core diagnostic | 1024 bytes |
| JSON-RPC line / turn protocol aggregate | 16 MiB / 64 MiB |
| queued protocol events / bytes | 64 / 64 MiB |
| outgoing JSON-RPC message | 1 MiB |
| stderr tail / preflight output | 1 MiB / 2 MiB |
| unknown notifications / client request IDs | 4096 / 256 |
| native task turns | 64 |
| native session/turn ID | 256 bytes |
| repository changed paths / path bytes | 256 / 4096 |
| auth source / redaction values | 1 MiB / 64 |

Boundary tables exercise just below, exactly at, and just above closed limits.
The decoder continuously drains stdout/stderr, caps queued events and bytes,
and drops opted-out item payloads before they can accumulate. Diagnostics and
status never contain raw provider events, command output, the private prompt,
auth data, or secret-shaped environment values.

## Checked test inventory

The source-maintained inventory is deliberately split by layer:

| Inventory | Count / checked regression |
|---|---|
| session states × event kinds | 7 × 13 = 91 rows in `every_session_state_by_event_kind_has_an_explicit_accept_or_reject_row` |
| task states × lifecycle events | 8 × 7 = 56 rows in `every_task_state_by_relevant_lifecycle_event_has_an_explicit_matrix_row` |
| task transition states/edges | all 8 states in `task_transition_inventory_covers_every_state_and_rejects_terminal_lifecycle` |
| ordered effect variants | all 8 real production paths in `every_effect_kind_has_a_real_core_production_path` |
| plan/task/text/path/version bounds | `plan_task_count_review_round_and_status_ordinal_bounds_are_exact`, `every_task_text_list_and_path_bound_has_below_equal_and_above_cases`, `session_and_plan_versions_hashes_and_overflow_fail_without_mutation` |
| repository/outcome/diagnostic bounds | `repository_observation_bounds_ordering_and_plan_cleanliness_fail_closed`, `typed_outcome_cross_field_failures_are_rejected_without_consuming_the_turn`, `every_driver_failure_class_has_a_distinct_bounded_secret_free_terminal` |
| races, at-most-once, invariants | `completion_identity_ordering_and_at_most_once_are_transactional`, `cancel_completion_and_parent_failure_races_have_one_deterministic_winner`, `invariant_audit_rejects_corrupted_operation_task_identity_and_terminal_state` |
| RPC transport bounds/correlation | `exact_line_and_turn_protocol_bounds_are_closed`, `queue_outgoing_message_and_request_id_bounds_are_exact`, `unknown_notification_bound_is_exact_and_opted_out_events_do_not_consume_it`, `response_correlation_rejects_mismatch_duplicate_and_stale_ids` |
| fake App Server lifecycle/protocol | `fresh_role_threads_same_thread_followup_and_exact_wire_fields`, `malformed_ids_eof_and_bound_violations_fail_closed_without_raw_output`, `every_server_request_class_and_unknown_request_terminate_the_runtime`, `interrupt_timeout_kills_the_entire_owned_process_group` |
| production driver + disposable Git | `orchestrator::app_server::tests::*`, including multi-task, recovery, Reviewer mutation, locks, environment, auth bounds, cancel, cleanup, and drift |
| real outer sandbox without model | `real_outer_sandbox_is_writable_for_both_roles_and_hides_unbound_host_state` |

All fake-runtime and exact-binary contract tests are local and must not send a
model turn or network request.

## Human-only product acceptance

Automated development/review ends at a source candidate. It does not run the
real Fibonacci journey and does not authorize push or installation. After all
phase and aggregate reviews are complete, a human may choose to start a new
disposable terminal and follow the exact procedure in
[architect.md](architect.md#human-only-fibonacci-acceptance). The target
Architect must begin with an empty input buffer; only the human submits the
first prompt.
