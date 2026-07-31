# Codex App Server task-runtime contract

This document freezes the provider boundary introduced in development phase P0.
Production dispatch remains on the released CLI adapters until the later
integration phase.

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

The only P0 implementations are the seam and `FakeTaskWorkerRuntime`.

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

It does not own filesystem, Git, process, clock, network, terminal, or provider
transport I/O. The P1 matrix has explicit rows for all 84
`SessionState × SupervisorEventKind` combinations and all 56
`TaskState × relevant lifecycle event` combinations. Ordered pure journeys
cover one/two task completion, different and shared repositories,
request-changes/re-review, exact review exhaustion, safe completion recovery,
identity/duplicate/race failures, and all closed plan/task bounds.
