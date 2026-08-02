# Codex Architect adapter maintenance contract

This is the maintainer contract for the pinned Codex 0.145 blank Architect
started by `hcom arch codex`. Background Developer and Reviewer turns use the
pinned Codex 0.146 exec lane documented in
[codex-exec-worker-lane.md](codex-exec-worker-lane.md). Existing tagged
interactive hcom products are independent and must not be routed through this
session lane.

## Product rule

hcom is a thin automation layer over a native Codex launch. It owns process
lifetime, task-control transport, typed task handoff, exact resume, and
redacted evidence. It does not replace the operator's Codex installation
semantics with a generated HOME, CODEX_HOME, config, trust decision, MCP
allowlist, feature allowlist, or reduced host filesystem.

The deliberate exceptions are:

- the built-in Codex Architect, Developer, and Reviewer model/effort defaults
  are `gpt-5.6-sol` and `xhigh`, passed explicitly rather than inherited from
  user config;
- typed sandbox/approval values are also explicit;
- the Architect gets one hcom-owned task-control MCP table;
- `HCOM_DIR` and hcom run/task/role identity are private to this invocation;
- exec workers add the transport required for thread identity/final-message
  capture.

## Pinned identity and typed profile

- The Architect pins the absolute Codex 0.145 executable and requires
  `codex-cli 0.145.0`.
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

`HCOM_DIR` alone points at per-run private hcom state, so hcom commands inside
the Architect cannot address the user's live retained-agent store. The
launch-control bubblewrap process still supplies the pre-registration gate,
but binds the host root read-write and does not mask the user's Codex config,
HOME, project, source repositories, XDG paths, or unrelated host files.

## Exact native-session binding with a shared CODEX_HOME

A private CODEX_HOME previously made “exactly one rollout exists” sufficient
for Architect identity. In the real native session store many old and
concurrent sessions can exist, so hcom now:

1. snapshots the bounded `(device, inode)` identities of rollout files before
   starting the Architect;
2. considers only post-snapshot rollout files whose first native
   `session_meta` record matches the exact project root and pinned CLI version;
3. requires one unique candidate for the first task-control call;
4. after binding, keeps selecting that exact candidate even if another
   same-project Codex session starts later;
5. routes missing, ambiguous, or changed evidence through the existing closed
   native-session refusal path.

The snapshot is identity-only; hcom never reads old rollout contents. The
bounded list fits the existing 256 KiB bridge bootstrap frame.

## Background Codex workers

Both `hcom arch codex` and `hcom arch claude` currently bind the same
Codex-only exec worker runtime. A configured Claude Developer or Reviewer is
rejected before the Architect starts. The foreground Architect adapter does
not change the worker adapter.

The exec lane:

- launches directly from the project directory with the complete parent
  environment and real native config;
- passes `--add-dir <task repository>` to both roles when source is outside the
  project;
- tells both roles to inspect applicable project and repository AGENTS.md,
  AGENTS.override.md, and nested instructions before work;
- proves create/resume identity from `thread.started.thread_id`;
- captures final output with `--output-last-message`;
- keeps Reviewer non-mutation as a role contract, not an OS read-only mount;
- keeps private HCOM_DIR, bounded lifecycle/reaping, and redacted evidence.

See the exec-lane document for exact argv ordering, verdict classification,
artifact bounds, and contract smokes.

## Test map

| Contract | Regression |
|---|---|
| defaults are explicit | `architect::profile::tests::missing_file_uses_reviewed_defaults`, `worker::profile::tests::codex_exec_worker_lane_defaults_both_workers_to_exact_codex_profiles` |
| no prompt or input injection | `architect::launch::tests::native_profile_has_no_prompt_or_secret_transport`, `blank_codex_launch_keeps_input_empty_and_preserves_native_host_semantics` |
| native config plus one MCP leaf | `architect::launch::tests::codex_control_server_is_an_additive_cli_overlay_not_a_private_config` |
| shared-store session identity | `architect::bridge::tests::native_session_baseline_excludes_old_rollouts_and_keeps_the_bound_session` |
| native worker argv/config | `worker::exec_runtime::tests::happy_developer_turn_completes_and_captures_thread_id`, `reviewer_registers_the_external_repository_as_a_native_workspace_root` |
| native environment/HCOM exception | `orchestrator::task_lane::tests::complete_parent_environment_changes_only_hcom_owned_identity_and_state` |
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

Before changing the pin, argv, configuration overlay, or session observation:

1. inspect the exact root/exec/resume help for the pinned binary;
2. update both capability probes and fake-CLI argv assertions;
3. cover blank Architect, Developer, Reviewer, create, and exact resume where
   applicable;
4. keep hcom-owned config to the smallest exact leaf—never replace the whole
   native user config;
5. run targeted tests, then the full source gate;
6. do not automate a real Architect TUI by submitting its first prompt.
