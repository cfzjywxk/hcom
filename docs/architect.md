# Architect and in-session task workers

`hcom arch` starts one blank, foreground Codex or Claude architect. The
Architect can maintain project plans and coordination records. After the human
either directs the Architect to follow a named existing detailed plan or later
approves a newly drafted plan, the same foreground parent starts one fresh
no-TUI Developer and Reviewer per task. State exists only in memory and the
whole run stops when the parent terminal exits.

`hcom arch codex` is the Codex App Server lane: every task owns one fresh,
no-TTY Codex App Server 0.146 process with one fresh Developer thread and one
fresh Reviewer thread. Same-task correction, re-review, or bounded completion
recovery reuses the exact original role thread; a later task never reuses that
process or either thread. `hcom arch claude` retains the existing CLI-worker
lane.

## Start

```bash
cd /path/to/project
hcom arch codex
# or
hcom arch claude
```

The exact current directory is the project directory. It must be an existing
canonical directory, but it does not need to be a Git repository. The
Architect starts there. A Codex App Server role thread starts at its task's
exact canonical repository root; retained CLI workers keep the project cwd and
receive an external repository binding when needed. hcom does not change paths
to `/hcom/workspace`, create a hidden project checkout, or require a repository
argument.

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

The committed clean HEAD is the default handoff point to a reviewer, not a
requirement attributed to the human. If a developer process exits after
leaving only allowed-path uncommitted changes, hcom automatically exact-resumes
that same developer once to preserve the diff, finish the required checks, and
commit it. The reviewer is not started until the resulting HEAD is clean and
exactly bound. Out-of-scope changes, branch/HEAD rewrite, external drift, or an
unprovable native-session identity still stop at `needs_human`.

Worker monitoring does not require Architect model calls. After dispatching a
developer or reviewer, the Architect waits 3–5 minutes before the first
`session_status` call and between later calls unless the human explicitly asks
for status or immediate intervention is required. It does not poll every
30 seconds, and it stops polling as soon as a returned state is terminal or
`needs_human`. The in-process supervisor continues its lightweight lifecycle,
exit, cancellation, and repository-drift checks independently; this cadence
changes only model-facing status requests.

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
| `hcom arch codex` | Codex `gpt-5.6-sol`, `xhigh`, `danger-full-access`, approvals `never` | Codex App Server `gpt-5.6-sol`, `xhigh`, `danger-full-access`, approvals `never` | Codex App Server `gpt-5.6-sol`, `xhigh`, `danger-full-access`, approvals `never` |
| `hcom arch claude` | Claude `opus`, `xhigh`, skip permissions | Codex `gpt-5.6-sol`, `xhigh` | Claude `opus`, `xhigh`, skip permissions |

The two commands intentionally have different worker runtimes:

- In `hcom arch codex`, both worker tables must use `adapter = "codex"`,
  `sandbox = "danger-full-access"`, and `ask_for_approval = "never"`.
  Model and reasoning effort remain independently configurable. Claude,
  legacy-CLI, or weaker Codex worker profiles fail closed before the
  Architect starts; there is no implicit Claude Reviewer or CLI fallback.
- In `hcom arch claude`, the retained CLI-worker behavior remains available.
  The Developer and Reviewer adapters are independently configurable as Codex
  or Claude. With no explicit Reviewer table, that retained lane uses Claude
  `opus`, `xhigh`, and `dangerously_skip_permissions = true`.

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

For a Codex Architect, a complete explicit App Server profile is:

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
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"
```

Both worker profiles map to typed `thread/start` and `turn/start` fields whose
native semantics are equivalent to:

```bash
codex \
  --sandbox danger-full-access \
  --ask-for-approval never \
  --model gpt-5.6-sol \
  --config 'model_reasoning_effort="xhigh"'
```

App Server task workers are not retained interactive hcom agents, so tags and
interactive review commands do not apply to them. They receive no arbitrary
argv, delegation capability, project-local config, plugin, hook, or MCP
server.

For a Claude architect, `[architect.profile]` has the Claude shape:

```toml
[architect.profile]
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
```

The retained Claude-Architect lane still supports independent worker adapters.
For a Claude Developer and Codex Reviewer:

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

All fields in a profile table are required. Supported values are:

- Codex `reasoning_effort`: `none`, `minimal`, `low`, `medium`, `high`,
  `xhigh`, or `max`.
- Claude `effort`: `low`, `medium`, `high`, `xhigh`, or `max`.
- Codex `sandbox`: `read-only`, `workspace-write`, or `danger-full-access`;
  a retained CLI Developer must use `workspace-write` or
  `danger-full-access`; both App Server roles require
  `danger-full-access`.
- Codex `ask_for_approval`: `untrusted`, `on-request`, or `never`.
- Developer `adapter`: `claude` or `codex`.
- Reviewer `adapter`: `claude` or `codex`.

The last two adapter choices apply only to the retained Claude-Architect lane;
the Codex App Server lane requires Codex for both roles. App Server workers
also require `ask_for_approval = "never"` because they have no interactive
approval channel.

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
`--effort` for Claude. They change only the interactive Architect. Developer
and Reviewer profiles come from TOML or their lane-specific defaults.

Task workers have no TTY or interactive approval channel. A Codex worker
in the App Server lane must use `never`, and that lane rejects any other
policy before launch. In the retained CLI lane, a Codex policy other than
`never` or a Claude worker with `dangerously_skip_permissions = false` can stop
at `needs_human` when the native CLI requires approval; hcom never approves on
the human's behalf.

The parent reads and validates the file once before it starts the Architect.
It prints the sanitized effective profiles and a SHA-256 profile hash. The
configured developer/reviewer adapter and the profile hash are bound into the
plan hash. Editing `config.toml` later cannot change a running session; start a
new `hcom arch` invocation to pick up changes.

Maintainers changing the retained Codex CLI arguments, isolated configuration,
JSONL parsing, or size bounds must follow the
[Codex CLI adapter maintenance contract](codex-adapter-contract.md). Changes
to the App Server protocol, runtime profile mapping, or task lifecycle must
follow the [Codex App Server runtime contract](codex-app-server-runtime.md).

## Login and environment inheritance

Native login sources are derived from the terminal that starts the Architect,
for the selected Architect and whichever adapters the frozen
developer/reviewer profiles select:

- Codex: `$CODEX_HOME/auth.json` when `CODEX_HOME` is set, otherwise
  `$HOME/.codex/auth.json`.
- Claude: `$CLAUDE_CONFIG_DIR/.credentials.json` when
  `CLAUDE_CONFIG_DIR` is set, otherwise `$HOME/.claude/.credentials.json`.

The exact selected credential files are mounted read-only. In the App Server
lane, one task-private `CODEX_HOME` and one read-only Codex auth overlay are
shared by that task's two role threads. Retained CLI workers keep their fresh
role-private native config directories. An all-Claude retained session does
not require or inspect Codex credentials; an all-Codex session does not
require or inspect Claude credentials. hcom does not copy credentials into its
database or config, synthesize a login directory, or derive login from another
window.

The process that starts `hcom arch` also supplies one complete,
session-frozen OS environment snapshot to the Architect and every developer
and reviewer. There is no inheritance allowlist: unknown names,
secret-shaped names, both cases of proxy variables, empty values, and
non-UTF-8 names or values are retained exactly. hcom does not normalize proxy
names or reconstruct values from another process.

After copying that snapshot, hcom applies only these runtime-local
replacements:

- the Architect gets a private `HCOM_DIR`, private selected native config,
  private temp/runtime paths, and Claude-private XDG/control flags when Claude
  is selected;
- every App Server task process gets a task-private HOME, CODEX_HOME,
  HCOM_DIR, temp/runtime/XDG/cache paths, pinned Cargo/Rustup roots, and exact
  run/task identity; its OS role marker is `task-runtime`, while Developer and
  Reviewer identity is carried by the closed thread/turn request;
- every retained CLI worker gets its role-private HOME, selected native
  config, temp/runtime/generated cache paths, pinned Cargo/Rustup roots, and
  exact `HCOM_WORKER_ROLE`/`HCOM_RUN_ID`/`HCOM_TASK_ID`; Claude workers also
  get private XDG paths and fixed noninteractive control flags;
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

Profile configuration changes native policy, not the outer containment:

- the exact invocation directory remains the Architect cwd; an App Server
  role thread uses the exact task repository as its cwd, while retained CLI
  workers keep the project cwd;
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
- an App Server task process receives only its task repository read-write at
  repository's real absolute path;
- the App Server Reviewer deliberately has the same writable task repository,
  HOME, temporary paths, toolchain, and shell/test ability as its Developer.
  Its role contract forbids persistent source changes, and the Supervisor
  accepts a verdict only if local pre/post repository identity, branch, HEAD,
  tracked diff, index diff, and non-ignored-untracked evidence is exactly
  unchanged;
- retained CLI reviewers keep their existing read-only project/source/Git
  view, with writable session-private home, temporary directory, and generated
  language caches;
- task-worker namespaces mount only the lane-required exact
  project/repository paths, pinned system/toolchain inputs, and private
  task/role state; they do not mount the host root or the user's unrelated HOME
  contents;
- no worker receives a TTY, hcom control socket, sibling session root, or host
  runtime contents; native credential/config directories remain read-only,
  while the complete parent environment may itself contain caller-provided
  token, SSH-agent, or other credential variables;
- hcom never invents a push credential or performs a push. Complete
  environment inheritance is deliberately not a credential-reduction
  boundary;
- Codex delegation, hooks, plugins, apps, and arbitrary MCP servers remain
  disabled for session workers;
- in the retained CLI lane, an external task repository is declared to native
  Codex/Claude with `--add-dir` as needed. The App Server lane instead launches
  the task process directly in the exact repository and binds only that
  repository writable;
- one App Server task gets one fresh process and two fresh role threads;
  request-changes, re-review, and completion recovery resume only the exact
  threads already bound to that task, and task terminal cleanup completes
  before the next task process starts;
- a developer completion that leaves an in-scope dirty worktree or an invalid
  result on otherwise safe repository state gets one automatic exact-session
  recovery turn before `needs_human`; recovery never uses a fresh fallback,
  and reviewer startup still requires a committed clean HEAD.

Whole-host read-write is intentionally broad Architect authority and is much
broader than the task-worker view. Do not start an Architect on untrusted
project instructions. Outside the protected hcom/Codex/Claude control surfaces,
same-user files including SSH keys, cloud credentials, shell configuration,
and unrelated application state remain writable. Before
`session_plan_replace`, the Architect may write and commit design artifacts.
Once a task repository is bound, the Architect must not modify it concurrently.
This is an instruction plus drift detection, not a dynamic filesystem
exclusion: an Architect write inside the task's allowed path scope during a
developer turn is not attributable from Git state alone and can be swept into
the developer commit. hcom fingerprints a recoverable post-turn checkout and
revalidates it before exact resume; a later change is treated as external
concurrent drift and moves the run to `needs_human`. hcom never resets or
rebases the real branch. Coordination files outside bound task repositories
can continue to be updated.

The Architect selects each `repository_root` from the human-named plan and
attests that the same message authorized execution. hcom validates canonical
Git identity, cleanliness, branch, HEAD, locks, and later drift, but there is
no configured repository-root allowlist. The exact binding display is
therefore visibility and TOCTOU protection, not independent proof that the
model selected the intended repository.

App Server Developer and Reviewer write authority remains limited to the
current task repository by the outer worker namespace; Reviewer correctness is
the post-turn Git invariant described above, not filesystem read-only.
Retained CLI Reviewer source/Git paths remain read-only regardless of the
configured native permission mode.

## Terminal states and lifetime

The foreground parent is the only lifecycle owner. It keeps the typed plan,
task states, logical role-session bindings, review rounds, and runtime handles
in memory. It starts no daemon or global service, writes no Project Store, and
does not recover a run after the parent or terminal exits.

An explicit cancel interrupts the active turn and closes the task runtime.
Parent exit does the same. Runtime/protocol failure, repository drift,
out-of-scope changes, a second invalid Developer completion, Reviewer
mutation, or cleanup failure ends at a bounded `needs_human`/failure status;
hcom never resets or repairs the real repository. An LGTM closes the current
task runtime before the next task starts. If request changes reaches the
task's configured maximum, the visible task state is `review_exhausted` and
the ordered plan advances by policy.

Neither task LGTM nor a completed session authorizes push, install, dependency
installation, or modification of user hcom configuration. Those remain
separate human actions.

## Human-only Fibonacci acceptance

The real App Server/model journey is deliberately outside automated
development and review. After the source candidate has passed independent
review, a human may choose to validate it in a newly created disposable
terminal:

```bash
cd /home/ywxk/src/work/data/hcom-interactive/demo/fibonacci-background
HCOM_DIR="$PWD/.hcom" hcom arch codex
```

The target Codex terminal must open with an empty input buffer. The human, not
hcom or another agent, enters and submits:

```text
读取 TASKS.md，生成 exact 两任务技术方案并按照该方案执行。
不要增加第三个任务；完成 dev/review loop 后在这个 session 向我汇报最终结果。
```

The Architect should display the exact two-task repositories, branch/base
revisions, App Server profile, plan version, and plan hash; use the message's
explicit execution authority; then let the task-local Developer/Reviewer
loops advance. Task 2 must start from Task 1's terminal reviewed HEAD and use a
fresh App Server process and fresh role threads.

After both tasks finish, the human can run:

```bash
python3 -m unittest discover -s tests -v
python3 -m src.fib_cli 10
```

Acceptance requires all tests to pass, the CLI to print only `55`, the original
branch and clean worktree/index to remain, one Developer commit per task,
visible `lgtm` or `review_exhausted` outcomes, no Reviewer repository residue,
all task processes exited, and the final report in the same foreground
Architect session. This procedure does not authorize push or installation.
