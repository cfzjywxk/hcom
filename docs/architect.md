# Architect and in-session task workers

`hcom arch` starts one blank, foreground Codex or Claude architect. The
Architect can maintain project plans and coordination records. After the human
either directs the Architect to follow a named existing detailed plan or later
approves a newly drafted plan, the same foreground parent starts one fresh no-TUI
developer and reviewer per task. State exists only in memory and the whole run
stops when the parent terminal exits. `hcom architect` remains an equivalent
compatibility alias.

## Start

```bash
cd /path/to/project
hcom arch codex
# or
hcom arch claude
```

The exact current directory is the project directory. It must be an existing
canonical directory, but it does not need to be a Git repository. The
Architect and every native Codex/Claude task worker start in that same
directory. hcom does not change it to `/hcom/workspace`, create a hidden
project checkout, or require a repository argument.

The project documentation tells the Architect where its source repositories
live. Every task in the proposed plan contains an absolute
`repository_root`; it must resolve to the exact canonical Git top level, on an
attached branch, with a completely clean worktree. A repository may be outside
the project directory, such as `/home/user/src/hcom`, or nested inside it,
such as `/home/user/work/tidb_vulcan/src/component`. Different tasks may name
different repositories.

Repositories are validated and locked when the typed plan is proposed. After
execution authorization, the developer commits directly to the repository for
that task. There is no final apply, reset, rebase, merge, push, install, or
persistent recovery.

The architect starts with an empty input buffer. hcom does not provide a prompt
argument, write stdin, inject a key, or submit Enter. The human owns the first
and every later architect input.

## Profile configuration

Profiles live in `$HCOM_DIR/config.toml`; without `HCOM_DIR`, the path is
`~/.hcom/config.toml`. The file must be a current-user-owned regular file, must
not be a symlink or hard link, must not be writable by group/other, and must be
at most 1 MiB. Run `hcom config --edit` or edit that file directly;
`chmod 600 ~/.hcom/config.toml` is recommended.

Without profile configuration, the command selects these effective defaults:

| Command | Architect | Developer | Reviewer |
|---|---|---|---|
| `hcom arch codex` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, approvals `never` | Codex `gpt-5.6-sol`, `xhigh` | Claude `opus`, `xhigh`, skip permissions |
| `hcom arch claude` | Claude `opus`, `xhigh`, skip permissions | Codex `gpt-5.6-sol`, `xhigh` | Claude `opus`, `xhigh`, skip permissions |

The developer remains independently configurable and keeps its own built-in
profile. When `[architect.reviewer]` is absent, the reviewer always uses
Claude `opus` with `xhigh` effort and
`dangerously_skip_permissions = true`. The selected Architect,
`[architect.profile]`, and Architect command-line overrides do not change that
implicit reviewer. Supplying an explicit `[architect.reviewer]` table replaces
the built-in reviewer default.

The Codex Architect does not copy the parent `CODEX_HOME`: that would also
import unrelated native configuration and control surfaces. Its private
per-run config explicitly records the exact invocation directory as native
`untrusted`, so Codex does not repeat the unresolved folder-trust dialog and
does not load project-local `.codex` configuration, hooks, rules, or extra MCP
servers. This native project-config decision is separate from the Architect's
reviewed OS-level host read/write sandbox and does not override the explicit
command-line `danger-full-access`/`never` profile.

The Codex Architect's isolated configuration marks the one
`hcom_session_task_control` MCP server as approved for this invocation, so
status, plan, start, and cancel calls do not each show an additional native
tool-approval dialog. The Architect must display the complete repository
bindings plus exact plan version/hash before starting. A human message that
explicitly directs it to follow, implement, execute, proceed with, or complete
a named existing detailed plan, specification, or `current_todo` authorizes
same-turn plan derivation and start. A request only to read, analyze, discuss,
summarize, draft, or update a plan does not authorize execution and leaves the
run waiting for approval. The supervisor rechecks the exact version/hash and a
required confirmation bit. This is model-relayed authorization, not
independent OS-level proof of a human keystroke.

For a Codex architect, this complete Codex-developer/Claude-reviewer example
uses explicit profiles for all three roles:

```toml
[architect.profile]
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.developer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.reviewer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

It covers the native options represented by:

```bash
hcom codex --tag dev1 -- \
  --sandbox danger-full-access \
  --ask-for-approval never \
  --model gpt-5.6-sol \
  --config 'model_reasoning_effort="xhigh"'

hcom claude --tag dev2 -- \
  --dangerously-skip-permissions \
  --effort xhigh \
  --model opus
```

Architect session workers are not retained interactive hcom agents, so
`--tag dev1` and `--tag dev2` do not apply to this lane. Adapter, model,
reasoning/effort, sandbox/approval, and Claude permission mode do apply.

For a Claude architect, `[architect.profile]` has the Claude shape:

```toml
[architect.profile]
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

Developer and reviewer adapters are independent. For the reverse
Claude-developer/Codex-reviewer combination:

```toml
[architect.developer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true

[architect.reviewer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"
```

To use Codex for both roles, use the Codex table shape for both
`[architect.developer]` and `[architect.reviewer]`. To use Claude for both,
use the Claude table shape for both. There is no developer/reviewer adapter
pairing constraint.

For example, a Codex reviewer table is:

```toml
[architect.reviewer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"
```

All fields in a profile table are required. Supported values are:

- Codex `reasoning_effort`: `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`.
- Claude `effort`: `low`, `medium`, `high`, `xhigh`, or `max`.
- Codex `sandbox`: `read-only`, `workspace-write`, or `danger-full-access`;
  a Codex developer must use `workspace-write` or `danger-full-access` because
  a successful developer turn must write and commit.
- Codex `ask_for_approval`: `untrusted`, `on-request`, or `never`.
- Developer `adapter`: `claude` or `codex`.
- Reviewer `adapter`: `claude` or `codex`.

There is deliberately no arbitrary `args` field. This prevents a profile from
adding a prompt, resume/fork target, alternate working directory, MCP server,
result schema, output path, or delegation feature.

## Precedence and freezing

Precedence for the interactive architect is:

```text
built-in defaults < config.toml < explicit hcom arch options
```

The explicit options are `--model`, `--reasoning`, `--sandbox`, and
`--approval` (`--ask-for-approval` is an alias) for Codex, and `--model` plus
`--effort` for Claude. Developer and explicit reviewer profiles come from
TOML.

Task workers have no TTY or interactive approval channel. A Codex worker
approval policy other than `never`, or a Claude worker with
`dangerously_skip_permissions = false`, can therefore stop at `needs_human`
when the native CLI requires an approval; it never causes hcom to approve on
the human's behalf.

The parent reads and validates the file once before it starts the Architect.
It prints the sanitized effective profiles and a SHA-256 profile hash. The
configured developer/reviewer adapter and the profile hash are bound into the
plan hash. Editing `config.toml` later cannot change a running session; start a
new `hcom arch` invocation to pick up changes.

Maintainers changing Codex arguments, isolated configuration, JSONL parsing,
or size bounds must also follow the
[Codex adapter maintenance contract](codex-adapter-contract.md), including its
create/resume and developer/reviewer test matrix.

## Login and environment inheritance

Native login sources are derived from the terminal that starts the Architect,
for the selected Architect and whichever adapters the frozen
developer/reviewer profiles select:

- Codex: `$CODEX_HOME/auth.json` when `CODEX_HOME` is set, otherwise
  `$HOME/.codex/auth.json`.
- Claude: `$CLAUDE_CONFIG_DIR/.credentials.json` when
  `CLAUDE_CONFIG_DIR` is set, otherwise `$HOME/.claude/.credentials.json`.

The exact selected credential files are mounted read-only into fresh isolated
native config directories for each worker. An all-Claude session does not
require or inspect Codex credentials; an all-Codex session does not require or
inspect Claude credentials. hcom does not copy credentials into its database
or config, synthesize a login directory, or derive login from another window.

The process that starts `hcom arch` also supplies one complete,
session-frozen OS environment snapshot to the Architect and every developer
and reviewer. There is no inheritance allowlist: unknown names,
secret-shaped names, both cases of proxy variables, empty values, and
non-UTF-8 names or values are retained exactly. hcom does not normalize proxy
names or reconstruct values from another process.

After copying that snapshot, hcom applies only these role-local replacements:

- the Architect gets a private `HCOM_DIR`, private selected native config,
  private temp/runtime paths, and Claude-private XDG/control flags when Claude
  is selected;
- every worker gets its private HOME, selected native config, temp/runtime,
  generated cache paths, pinned Cargo/Rustup roots, and exact
  `HCOM_WORKER_ROLE`/`HCOM_RUN_ID`/`HCOM_TASK_ID`; Claude workers also get their
  private XDG paths and fixed noninteractive control flags;
- ordinary hcom work terminals replace hcom-owned launch/identity variables
  and terminal-pane identity while directly inheriting every other parent OS
  entry.

Those replacements take precedence over same-named parent entries. All other
entries remain byte-for-byte unchanged. An inherited path value does not grant
filesystem access by itself: the Architect and worker mount contracts below
remain authoritative. Hcom neither enumerates nor persists the complete
environment as a name/value inventory. Artifact redaction is derived only from
secret-shaped environment names, URI userinfo, adapter-declared secrets, and
the private prompt. Ordinary PWD, PATH, shell, locale, and workdir evidence
remains readable and admissible in structured results. `HCOM_DIR` changes hcom
state/config only; it does not redirect Codex or Claude login state.

Complete inheritance may carry marker-shaped values such as `HCOM_AGENT`,
terminal IDs, or stale outer-session names. They remain plain data: worker
namespaces expose no retained hcom state/control socket or interactive TTY, the
Architect receives a private `HCOM_DIR`, and supervisor-owned role/run/task
identity always wins. Values used to resolve HOME/native config/toolchain or
other mount paths must be valid UTF-8 and fail closed before spawn otherwise;
unrelated non-UTF-8 names and values remain byte-exact.

## Fixed safety boundaries

Profile configuration changes native CLI policy, not the outer containment:

- the exact invocation directory remains the native cwd for the Architect and
  every task worker;
- absolute host paths are preserved; no role sees a synthetic
  `/hcom/workspace`;
- the interactive Architect keeps a whole-host read-write filesystem view so
  it can create and maintain `current_todo`, technical plans, and discussion
  records at their real paths; `/tmp`, the host XDG runtime, all
  current/sibling architect session roots, and hcom control sockets remain
  masked, while the exact project and this Architect's private state are
  rebound writable as needed; live default/explicit hcom state, parent
  `.codex`/`.claude` configuration, the launching hcom executable, pinned
  Architect/MCP executables, and the exact credential source remain read-only;
- `HCOM_DIR` inside the Architect points to private per-run state, so invoking
  hcom there cannot message or wake retained interactive agents through the
  live v24 store;
- the developer receives only its task repository read-write at that
  repository's real absolute path;
- every reviewer sees the exact project and task repository HEAD read-only at
  their real absolute paths; this source/Git restriction does not make the
  whole reviewer environment read-only: its session-private home, temporary
  directory, and generated language caches remain writable;
- task-worker namespaces mount only the exact project/repository paths, pinned
  system/toolchain inputs, and private per-role state; they do not mount the
  host root or the user's unrelated HOME contents;
- no worker receives a TTY, hcom control socket, sibling session root, or host
  runtime contents; native credential/config directories remain read-only,
  while the complete parent environment may itself contain caller-provided
  token, SSH-agent, or other credential variables;
- hcom never invents a push credential or performs a push. Complete
  environment inheritance is deliberately not a credential-reduction
  boundary;
- Codex delegation, hooks, plugins, apps, and arbitrary MCP servers remain
  disabled for session workers;
- when a task repository differs from the project directory, hcom declares it
  to native Codex/Claude with `--add-dir`; Codex `workspace-write` therefore
  applies to both the project workspace and the task repository, while the
  outer mount still keeps the project read-only;
- one task gets fresh developer/reviewer sessions, while request-changes
  resumes only the exact sessions already bound to that task.

Whole-host read-write is intentionally broad Architect authority and is much
broader than the task-worker view. Do not start an Architect on untrusted
project instructions. Outside the protected hcom/Codex/Claude control surfaces,
same-user files including SSH keys, cloud credentials, shell configuration,
and unrelated application state remain writable. Before
`session_plan_replace`, the Architect may write and commit design artifacts.
Once a task repository is bound, the Architect must not modify it concurrently.
This is an instruction plus drift detection, not a dynamic filesystem
exclusion: an Architect write inside the task's allowed path scope during a
developer turn can be swept into the developer commit. Other drift may be
detected only after the developer has committed, moving the run to
`needs_human` and leaving that partial commit on the real branch; hcom never
resets or rebases it. Coordination files outside bound task repositories can
continue to be updated.

The Architect selects each `repository_root` from the human-named plan and
attests that the same message authorized execution. hcom validates canonical
Git identity, cleanliness, branch, HEAD, locks, and later drift, but there is
no configured repository-root allowlist. The exact binding display is
therefore visibility and TOCTOU protection, not independent proof that the
model selected the intended repository.

Reviewer source/Git paths remain read-only regardless of the configured native
permission mode. Developer write authority remains limited to the current task
repository by the outer worker namespace.
