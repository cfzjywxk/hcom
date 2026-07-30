# Architect and in-session task workers

`hcom architect` starts one blank, foreground Codex or Claude architect. After
the human types and approves an ordered plan, the same foreground parent starts
one fresh no-TUI developer and reviewer per task. State exists only in memory
and the whole run stops when the parent terminal exits.

## Start

```bash
cd /path/to/project
hcom architect codex
# or
hcom architect claude
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

Repositories are validated and locked when the plan is proposed. After human
approval, the developer commits directly to the repository for that task.
There is no final apply, reset, rebase, merge, push, install, or persistent
recovery.

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

| Command | Architect | Reviewer |
|---|---|---|
| `hcom architect codex` | Codex `gpt-5.6-sol`, `xhigh` | Codex `gpt-5.6-sol`, `xhigh` |
| `hcom architect claude` | Claude `opus`, `xhigh` | Claude `opus`, `xhigh` |

The developer remains independently configurable and keeps its own built-in
profile. When `[architect.reviewer]` is absent, the reviewer follows the
selected architect adapter and its effective model and reasoning/effort,
including `[architect.profile]` and command-line overrides. Supplying an
explicit `[architect.reviewer]` table disables that inheritance.

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
built-in defaults < config.toml < explicit hcom architect options
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
new `hcom architect` invocation to pick up changes.

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
Upper- and lower-case proxy variables are also captured from the starting
terminal. `HCOM_DIR` changes hcom state/config only; it does not redirect Codex
or Claude login state.

## Fixed safety boundaries

Profile configuration changes native CLI policy, not the outer containment:

- the exact invocation directory remains the native cwd for the Architect and
  every task worker;
- absolute host paths are preserved; no role sees a synthetic
  `/hcom/workspace`;
- the interactive Architect keeps a whole-host read-only filesystem view so it
  can follow project documentation to source repositories outside the project
  directory; `/tmp`, the host XDG runtime, all current/sibling architect
  session roots, and hcom control sockets are masked, while the exact project
  and this Architect's private state are rebound as needed;
- the developer receives only its task repository read-write at that
  repository's real absolute path;
- every reviewer sees the exact task repository HEAD read-only at its real
  absolute path;
- task-worker namespaces mount only the exact project/repository paths, pinned
  system/toolchain inputs, and private per-role state; they do not mount the
  host root or the user's unrelated HOME contents;
- no worker receives a TTY, hcom control socket, sibling session root, host
  runtime contents, or push credential;
- Codex delegation, hooks, plugins, apps, and arbitrary MCP servers remain
  disabled for session workers;
- when a task repository differs from the project directory, hcom declares it
  to native Codex/Claude with `--add-dir`; Codex `workspace-write` therefore
  applies to both the project workspace and the task repository, while the
  outer mount still keeps the project read-only;
- one task gets fresh developer/reviewer sessions, while request-changes
  resumes only the exact sessions already bound to that task.

Whole-host read-only means files outside those masks, including same-user
configuration such as `$HCOM_DIR` or `~/.ssh`, can be read by the interactive
Architect. This matches the authority needed to follow arbitrary absolute
source paths, but it is broader than the task-worker view. Do not start an
Architect on untrusted project instructions if that read authority is
unacceptable.

Consequently, configuring the Architect or reviewer with
`danger-full-access`/`dangerously_skip_permissions = true` does not make the
host checkout writable to those roles. The outer read-only mount remains the
enforced boundary.
