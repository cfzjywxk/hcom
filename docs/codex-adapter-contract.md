# Codex adapter maintenance contract

This is the maintainer checklist for the pinned Codex 0.145 integration used by
`hcom architect` and no-TUI session workers. It records which native arguments
and configuration hcom owns, where size bounds are enforced, and which tests
must change with the contract.

The user-facing profile syntax remains in [architect.md](architect.md). This
document is normative for implementation changes: do not infer compatibility
from one successful `--help` command, role, or turn mode.

## Pinned identity and frozen profile

- Architect and worker adapters pin the absolute Codex 0.145 executable and
  require `codex-cli 0.145.0`.
- Model names are 1–128 ASCII bytes, cannot begin with `-`, and may contain
  only letters, digits, `.`, `_`, `-`, `/`, `:`, or `@`.
- Reasoning effort is one of `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`.
- Sandbox is `read-only`, `workspace-write`, or `danger-full-access`. A
  developer cannot use `read-only`, because completion must write and commit.
  A reviewer remains outer-filesystem read-only even if native Codex is
  configured more broadly.
- Approval policy is `untrusted`, `on-request`, or `never`. Workers have no
  interactive approval channel; hcom never answers a native prompt for the
  human.
- Profile TOML is a closed tagged schema with no arbitrary argv field. The
  effective typed profile and SHA-256 hash are frozen before launch and bound
  into the approved plan.

An acceptance change to a frozen worker adapter requires a contract-version
bump and, when useful for negotiation, an updated capability feature. Codex
developer and reviewer share the JSONL parser, so parser acceptance changes
must update and test both descriptors together.

## Interactive Architect invocation

The Codex Architect is a blank interactive CLI. hcom supplies no positional
prompt, stdin content, PTY injection, paste, key event, or Enter.

Its owned native argv is:

```text
codex
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --sandbox <typed sandbox>
  --ask-for-approval <typed policy>
  --cd <exact project_root>
  --no-alt-screen
  --strict-config
  --disable <each closed disabled feature>
```

The Architect gets a fresh private `CODEX_HOME`. Its generated `config.toml`
contains only:

- an empty terminal-title configuration;
- the exact project path with `trust_level = "untrusted"`, resolving folder
  trust without enabling project-local `.codex` config, hooks, rules, or MCP;
- one enabled `hcom_session_task_control` MCP server, bound to the private
  per-run relay and preapproved only for that server.

Parent Codex configuration and the exact auth source are read-only. Explicit
CLI sandbox/approval options remain authoritative. Unrelated native notices
are not hidden.

## Worker create/resume invocation

Developer and reviewer use one closed Codex `exec` shape. Shared probes live in
`CODEX_EXEC_HELP_REQUIREMENTS` and `CODEX_RESUME_HELP_REQUIREMENTS`; adding a
runtime option without adding it to the right probe is a test failure.

```text
codex exec
  --sandbox <typed sandbox>                 # exec-parent option
  --skip-git-repo-check                     # project_root may be non-Git
  [--add-dir <task repository>]             # workspace-write external/nested scope
  [resume <exact native session id>]        # same task only
  --json
  --strict-config
  --model <typed model>
  --config model_reasoning_effort="<typed effort>"
  --config approval_policy="<typed policy>"
  --config mcp_servers={}
  --ignore-user-config
  --ignore-rules
  --disable <each closed disabled feature>
  [--cd <exact project_root>]                # create only
  --output-schema <private schema file>
  --output-last-message <private final file>
  -                                         # bounded stdin + EOF
```

Ordering is load-bearing in Codex 0.145:

- `--sandbox`, `--skip-git-repo-check`, and optional `--add-dir` belong to the
  `exec` parent and precede `resume`;
- `--add-dir` is required when `workspace-write` must cover a task repository
  distinct from the project directory;
- `resume` is followed by the exact already-bound native session ID;
- resume never carries `--cd`; create carries `--cd <project_root>`;
- the prompt is private bounded stdin, never argv.

Every worker session uses a role-private HOME/CODEX_HOME/TMP/runtime view,
exact read-only auth overlay, complete parent environment snapshot followed by
declared role-local overrides, no TTY, and the reviewed bubblewrap policy.
`mcp_servers={}`, ignored user config/rules, and the closed disabled-feature
inventory prevent parent/project capabilities from entering a worker.

## Artifact and JSONL bounds

Bounds are layered. A transport limit must not be reused as a semantic-field
limit unless the formats have the same shape.

| Layer | Bound | Rule |
|---|---:|---|
| prompt | 256 KiB | Private stdin only |
| argv item | 4 KiB | No terminal controls or newlines |
| aggregate argv | 64 KiB | At most the shared bounded item count |
| schema file | 64 KiB | UTF-8 JSON object |
| native stdout | 1 MiB | Hard artifact cap |
| native stderr | 1 MiB | Hard artifact cap; contents remain closed |
| final structured result | 256 KiB | Strict developer/reviewer JSON |
| JSONL event count | 4096 | The 4097th nonblank event fails closed |
| ordinary JSONL event | 128 KiB | Per-event shape bound |
| native observation record | 128 KiB | Best-effort activity/session record |
| native session ID | 256 bytes | Bounded ASCII opaque identifier |
| event `type` | 128 bytes | No control characters |
| item ID | 256 bytes | No control characters |
| item type/status | 128 bytes | No control characters |
| command evidence | 4096 bytes | Exact newline-aware terminal-safe text |

Codex 0.145 places `aggregated_output` inside an
`item.completed`/`command_execution` event. That one known event may exceed
128 KiB while complete stdout remains within 1 MiB. The parser ignores that
field without copying it into evidence, diagnostics, results, or review
summaries, while still validating:

- event ordering and exactly one initial native session;
- exact resume session equality;
- item ID/type/status and command bounds;
- exit-code/status consistency;
- failed/error transitions and events after the terminal event;
- forbidden MCP or collaboration/delegation activity;
- the successful terminal event.

No other oversized event is accepted. A large command event must contain the
known `aggregated_output` field; an unrelated oversized unknown field does not
qualify. Aggregate, event-count, and per-event-shape overflow have distinct
sanitized diagnostics and never include raw provider payload.

Structured check claims require exact successful command evidence from the
same turn. A displayed `/bin/bash -c` or `/bin/bash -lc` wrapper may
additionally yield its one exact parsed payload; there is no prefix, substring,
or multi-command normalization.

## Test coverage map

| Contract area | Required regression |
|---|---|
| typed defaults, closed values, profile hash | `worker::profile::tests::defaults_preserve_reviewed_outer_safety_and_native_profiles`, `typed_profiles_reject_argv_and_config_injection_material`, `worker_toml_is_adapter_tagged_and_hash_binds_every_option` |
| config precedence and explicit reviewer independence | `architect::profile::tests::*`, `architect::launch::tests::explicit_architect_cli_overrides_toml_profile_only`, `explicit_reviewers_are_not_replaced_by_architect_cli_overrides` |
| Architect help, blank argv, isolated config/trust | `pinned_codex_root_help_matches_architect_command_contract_when_installed`, `native_profile_has_no_prompt_or_secret_transport`, `isolated_codex_config_decides_project_trust_and_preapproves_only_control_server`, `blank_launch_keeps_input_empty_and_grants_path_preserving_architect_write` |
| shared worker help/runtime options | `worker::codex::tests::pinned_codex_exec_help_matches_configurable_command_contract_when_installed`, `worker_cli_help_requirements_cover_every_runtime_option`, `worker::reviewer::tests::pinned_reviewer_help_matches_configurable_command_contract_when_installed` |
| developer create/resume and no-TTY isolation | `worker::codex::tests::exact_profile_outer_envelope_and_fake_create_resume_are_closed` |
| reviewer create/resume and exact session | `worker::reviewer::tests::exact_profiles_fake_create_and_same_task_workspace_refresh_resume_are_closed` |
| non-Git project and external repository | `project_cwd_is_distinct_from_the_writable_task_repository`, `codex_workspace_write_reviewer_declares_an_external_task_repository` |
| prompt/argv/schema/stream/result size layers | `worker::contract::tests::transport_size_bounds_are_independent_and_fail_closed`, `native_artifact_inputs_are_bounded_before_adapter_parsing` |
| JSONL transitions and forbidden activity | `jsonl_requires_one_exact_session_and_successful_terminal_event`, `native_observations_never_forward_model_or_provider_text`, `command_completion_status_must_match_its_exit_code` |
| JSONL aggregate/event/semantic bounds | `jsonl_accepts_large_ignored_command_output_for_create_and_exact_resume`, `jsonl_reports_sanitized_distinct_aggregate_count_and_event_shape_bounds`, `jsonl_large_event_exception_does_not_relax_semantic_field_bounds` |
| reviewer parser path and exact HEAD | `codex_reviewer_accepts_large_ignored_command_output_and_keeps_head_validation` |
| strict result/check and Git evidence | `completed_result_requires_exact_git_and_current_turn_check_evidence`, `resumed_completed_result_requires_the_full_task_range_not_only_the_turn_delta`, `strict_native_results_reject_wrong_model_session_semantics_and_check_claims` |
| executable/auth/config/environment drift | `exact_discovery_rejects_version_mismatch_and_external_git_admin_paths`, `auth_quota_session_result_and_identity_drift_fail_closed`, `revision_git_identity_tool_auth_and_environment_drift_fail_closed` |

The standard gate is:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --quiet --locked --all-targets
git diff --check <review-base>..HEAD
cargo build --quiet --release --locked
```

Large-payload tests construct data only in memory/private files and assert
sizes, counts, evidence, and sanitized errors. Do not print the payload or run
these tests with `--nocapture`.

## Change checklist

Before changing Codex arguments, config, parsing, or bounds:

1. Confirm the exact pinned executable/version and inspect root help, `exec`
   help, and the ordered `exec ... resume --help` path.
2. Update the shared capability inventory before adding/removing a runtime
   option.
3. Cover Architect, developer, reviewer, create, and exact resume as
   applicable; one successful path is not evidence for another.
4. Keep profile/config input typed and closed. Never add arbitrary native
   args, prompt transport, output paths, or arbitrary MCP config.
5. Keep aggregate transport, event-count, per-event, and semantic bounds
   separate. An exception must name one known event/field and retain the
   aggregate cap.
6. Keep diagnostics sanitized and specific enough to identify the failed
   layer.
7. If accepted frozen behavior changes, bump every affected adapter contract
   version and update capability assertions.
8. Run mapped targeted tests, then the standard gate. Never hide a new failure
   with `ignore` or test serialization.
