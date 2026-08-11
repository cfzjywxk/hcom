# hcom

[![CI](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/cfzjywxk/hcom/blob/master/LICENSE)

> **Hook your coding agents together**

`hcom` is a CLI that agents can use to message, watch, and spawn each other across terminals. It integrates with Claude Code, Gemini, Codex, OpenCode, Kilo Code, Pi, Oh My Pi, Antigravity, Cursor, Kimi and Copilot without changing how you use them.

Real Claude task-lane validation is always an explicit, serial,
Haiku/medium-only opt-in; see
[Claude task-lane test tooling](docs/claude-task-lane-testing.md).

Use it to coordinate pipelines, run different AI CLIs as each other's subagents, or just instead of copy-paste.

Single Rust binary, no background services. Start an agent with `hcom` in front, then prompt normally.

https://github.com/user-attachments/assets/1ce23ed9-f529-4be0-8124-816aa4c2fd43

---

## Install

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked
./target/release/hcom --version
```

This fork does not follow upstream releases. The retained `hcom update`,
`hcom update --check`, and `hcom update --go` forms report that upstream
updates are disabled and return a nonzero status; build the selected fork
revision instead.

---

## Quickstart

Terminal 1:

```bash
hcom claude   # codex / gemini / opencode / kilo / pi / omp / agy / cursor-agent / kimi / copilot
```

Terminal 2:

```bash
hcom codex
```

Prompt:

- `ask the other agent their favorite cake`
- `review what claude did and send it fixes`
- `ask codex to review, keep fixing and re-reviewing until LGTM, at most 3 rounds`
- `spawn 3x gemini, split work, collect results`
- `fork yourself to investigate the bug and report back`

Open the TUI:

```bash
hcom
```

---

## What agents can do

**Message** each other in real-time: intent, replies, bundled context for handoffs.

**Review loop** with deterministic `review → fix/rebut → re-review` state until LGTM or a configured round limit. Agents can start it from a natural-language request; only structured `hcom review` commands advance its state.

**Observe** each other: transcripts, file edits, terminal screens, command history.

**Subscribe** to each other: notify on status changes, file edits, specific events. React automatically.

**Spawn**, **fork**, **resume**, **kill** each other, in any terminal emulator or headless.

---

## Foreground architect

`hcom arch` runs one blank interactive Codex or Claude architect and an
in-memory, ordered task supervisor. Each approved task gets fresh no-TUI
Developer and active Reviewer sessions routed independently to native Codex or
Claude workers. The default built-in lanes are Codex Developer + Codex
Reviewer1 + Claude Reviewer2. Each review generation starts both default
Reviewers concurrently and waits for both responses. With
`hcom arch codex --single-review`, only Reviewer1 is active. Same-task
corrections resume the exact Developer session and re-review resumes each
active Reviewer's own exact native session.
In the default local-candidate lane, execution approval includes one signed-off
local candidate commit per task. Review corrections amend that same commit;
LGTM requires every active Reviewer to approve the same generation of the
final exact candidate range, so every amendment invalidates all earlier
verdicts and there is no extra post-LGTM commit. Push, install, and release
remain separately authorized in that lane.
If a developer exits with only allowed-path uncommitted changes, the
supervisor exact-resumes that developer once to finish checks and commit before
starting the reviewer; it does not terminate the whole run merely because the
first developer result forgot the commit.

```bash
cd /path/to/project
hcom arch codex
# single Reviewer1 lane:
hcom arch codex --single-review
# opt-in manual GitHub Pull Request delivery (also composes with --single-review):
hcom arch codex --github-pr
# opt in to the strict ruleset-attested exact-head merge path:
hcom arch codex --github-pr --protected-auto-merge
# or: hcom arch claude
# profiles from an exact file instead of $HCOM_DIR/config.toml:
hcom arch codex --config /absolute/path/to/profiles.toml
```

`--github-pr` explicitly selects manual Pull Request delivery. Without it, any
`[architect.github]` table is inert and the local lane opens no App key,
invokes no feature-owned Git command, and makes no GitHub request. With it,
hcom performs read-only private-repository/App/base preflight before the blank
Architect starts; manual mode neither calls nor requires repository rules APIs.
Writes begin only after an inspected typed plan is approved.
One approved run owns one append-only branch, linked worktree, and Pull
Request. Successful Developer and Reviewer finals are published byte-for-byte
without redaction or secret scanning, subject to a 60 KiB UTF-8 generated-body
cap. Every active Reviewer must publish same-head LGTM before the final Check
can succeed. Manual mode then completes as `review_complete_unmerged`, preserving
the open PR, remote/local run branch, linked worktree, and evidence for human
disposition; it cannot prove server-side protection for a private repository on
GitHub Free and never requests merge, branch deletion, or merged-run
finalization. `--protected-auto-merge` requires `--github-pr` and explicitly
selects the existing strict ruleset-attested exact-head squash-merge path.
Review exhaustion preserves the PR/branch/worktree unmerged. Install and release
are never implied. See the [GitHub Pull Request lane guide](docs/github-pr-lane.md).

Codex roles default to `gpt-5.6-sol` with `xhigh` reasoning,
`danger-full-access`, and approval policy `never`. Claude roles default to
`opus`/`xhigh` with `dangerously-skip-permissions`. These values are explicit
and therefore do not inherit model/effort defaults from native configuration.
`hcom arch codex` selects a Codex foreground Architect; `hcom arch claude`
selects a Claude foreground Architect. Both retain the Codex Developer + Codex
Reviewer1 + Claude Reviewer2 worker defaults unless their role tables override
them. `--single-review` is supported only with the Codex Architect and removes
Reviewer2 from the effective topology; an explicit `[architect.reviewer2]`
table is rejected in that mode. The
capability-bound session-control MCP server is additive, so all other native
MCP servers remain available. A human request that
explicitly says to follow or execute a named existing detailed plan,
specification, or `current_todo` authorizes the Architect to derive the typed
plan and start it in the same turn. So does a request to plan or define the
solution and then implement, proceed, finish, or drive the requested work: that
prospective authorization remains valid after the faithful detailed plan is
derived and displayed, even though it did not exist when the human spoke. The
Architect asks again only for a new unresolved material decision. A request
only to analyze, discuss, summarize, or draft does not authorize execution; an
explicit instruction not to start always wins. A bare generic
implement/proceed/finish/drive request selects the delegated workflow but does
not by itself authorize starting it. The supervisor validates the exact plan
version/hash and required confirmation bit, but does not independently attest
an OS-level human keystroke.

Codex Architects and workers use the launching terminal's real
HOME/CODEX_HOME and native Codex config, authentication, trust, AGENTS.md,
rules, hooks, skills, plugins, MCP servers, feature flags, custom providers,
caches, and session history. Claude Architects and workers likewise use the real
HOME/CLAUDE_CONFIG_DIR and native settings, authentication, instructions,
hooks, skills, MCP servers, provider, and managed policy. Both foreground
Architects are selected as bare programs from inherited `PATH`; hcom does not
pin their executable, version, or help output, and does not inspect or freeze
their native session stores.

Claude launch additionally requires all four inherited `http_proxy`,
`https_proxy`, `HTTP_PROXY`, and `HTTPS_PROXY` entries to equal
`http://127.0.0.1:7890`. It adds only
`CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` and
`CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`, then runs the native process through
the Linux per-invocation subreaper Guardian. Its cleanup guarantee covers owned
descendants while the Guardian remains live, not external service-manager
resources or unexpected Guardian death. Conflicting pin values fail closed.

The exact current directory is the Architect's project context and does not
need to be a Git repository. The Architect can read and write project plans,
`current_todo`, design notes, and discussion records, then binds each
authorized task to its actual source repository; that may be elsewhere or
nested under the project. Task workers keep the project cwd and receive the
task repository as `--add-dir` when distinct. Their prompts explicitly require
reading applicable project and source AGENTS.md/AGENTS.override.md plus nested
instructions; hcom passes paths and does not parse those files. Architect,
Developer, Reviewer1, and Reviewer2 adapter/model/effort/permission profiles
are typed TOML settings in `$HCOM_DIR/config.toml` (normally
`~/.hcom/config.toml`) and are merged independently onto the built-in role
defaults, then frozen when the command starts. `--config <absolute-file>`
swaps that profile source for one invocation without touching any other hcom
setting; it must be an existing canonical absolute regular file, so a mistyped
path fails closed rather than silently selecting the built-in defaults.
Canonical worker tables are
`[architect.developer]`, `[architect.reviewer1]`, and
`[architect.reviewer2]`; each may select `adapter = "codex"` or
`adapter = "claude"`, and omitted fields keep the selected role defaults. A
legacy-only `[architect.reviewer]` table is applied once to Reviewer1 in single
mode or copied completely to both Reviewer lanes in dual mode, with a
deprecation notice; mixing legacy and canonical Reviewer tables fails closed.
An unavailable selected adapter fails closed without fallback. Active Reviewers
have the same native host view as their directly launched providers; source
non-mutation is a role contract, not an OS
read-only mount. See [the Architect user guide](docs/architect.md) for the
profile schema, parent-terminal inheritance, and exact-session invariants.

The Architect and every session task worker inherit the complete environment
of the process that started `hcom arch`, captured once without a name
allowlist. Arbitrary and secret-shaped names, upper/lower-case pairs, empty
values, and non-UTF-8 OS strings are preserved. Codex receives that environment
without hcom additions or replacements, including the original `HCOM_DIR`.
Claude receives the same environment plus the two explicit pins above.
Ordinary hcom work terminals likewise inherit the parent OS environment
directly, then replace hcom-owned and new-terminal identity. Hcom neither
enumerates nor persists the complete environment as a name/value inventory.
Artifact containment is intentionally narrower than inheritance: it redacts
values carried by secret-shaped names, URI userinfo, and adapter-declared
secrets without hiding ordinary PWD, PATH, shell, or locale evidence.

Inherited marker-shaped values such as `HCOM_AGENT` do not grant hcom control
authority: workers receive no control socket or interactive TTY, and private
HCOM_DIR prevents retained-state access through hcom itself. Unrelated
non-UTF-8 entries remain byte-exact.

Before starting, the Architect must display every task's ordinal, key,
repository root, task/design document paths, selector, review and clarification
budgets, material assumptions, and the exact plan version and hash. An explicit
follow/execute/implement request for a named existing plan may authorize
same-turn start. An explicit request to first plan/define the solution and then
implement/proceed/finish/drive it does too, provided the derived plan is
faithful and adds no unresolved material decision; the human is not asked to
repeat that authorization merely because the exact binding was created later.
Unless the human explicitly assigns implementation to the current Architect
session, generic implementation requests mean planning and delegation through
the Developer/Reviewer loop; standing alone, they do not authorize starting
that loop. The local-candidate lane requires one signed-off local task commit
before review and same-commit amendments during correction. The GitHub lane
requires one new signed-off child commit on each Developer turn and never
rewrites a published commit. The Developer is instructed to use the matching
identity and `Signed-off-by` trailer, and every active Reviewer checks it.
An explicit no-commit requirement—or no-push in GitHub mode—is incompatible
and must be resolved before start; a general commit-authorization rule is
satisfied by exact run approval. If that explicit authority conflict
nevertheless reaches a Developer, the Developer must not modify or commit, and
the Architect must require a human decision regardless of remaining autonomous
clarification budget.
Repository identity is selected by the Architect from that plan; there is no
host-path allowlist. The Codex Architect has the native same-user host view;
hcom commands inside it use private per-run state so the live retained hcom
store is not addressed through the normal CLI.

After dispatch, the Codex Architect makes a blocking `session_wait` call bound
to the exact current run ID and a run-local progress cursor. The foreground
supervisor advances Developer and all active Reviewers without Architect model calls
and returns one retained review-request, per-Reviewer response, or
task-completion event, a latched Developer clarification/blocker action, or a
terminal state. Progress exposes the completed `review_round`, current
`review_generation`, Reviewer identity and response counts, exact durable
paths, and—on review request—the ordered Reviewer bindings. The Architect
displays each progress event without reading the response body. A Reviewer
response is partial progress while its received count is below the expected
count, so the Architect continues waiting rather than reporting the review
cycle complete or implying a Developer correction. It immediately re-arms the
wait with every event's sequence;
worker execution continues and events produced during the gap remain queued in
order. A defensible clarification is submitted through its exact artifact path
and the wait is likewise immediately re-armed. A material human decision ends
the Architect turn until the human answers. No timer or status polling is
involved. Esc cancels only the wait subscription, not the run. A pending action
takes priority over queued progress and records its `published_version`: an
older-version reconnect re-delivers it, while a same-version repeat is rejected
until the action is resolved. Queued progress, including every active Reviewer
response event, is delivered before a retained terminal result.
`session_status` is for an explicit human progress query only; it exposes the
bounded concurrent active-worker list, session-level Reviewer bindings,
current-generation Reviewer results, and clarification counts, not response
bodies or the accumulating clarification record list.
`session_clarifications_list` reads records for the exact run in pages of at
most eight. Only after terminal does the Architect read all active Reviewers'
current-generation evidence chains and report the original verdicts/findings.

A terminal run remains immutable but does not end the foreground Architect.
After delivering all Reviewer and clarification evidence, a later human
request can use `session_run_begin` with that terminal run ID and version to
create a fresh empty run in the same Architect process. The new run has a new
run ID, fresh Developer/Reviewer sessions and a separately bound and approved
plan; the cross-run session version remains monotonic so delayed old mutations
cannot match it. No new terminal, daemon, or cross-parent recovery is involved.
Once the first approved run acquires the project `hcom-tasks/.lock`, the
foreground supervisor retains that ownership lease across terminal handoff and
`session_run_begin`; only the per-run evidence handle changes. A later approve
claims its fresh run directory under the existing lease, so a newer hcom
session cannot displace the live foreground Architect between runs. The lease
is released when that foreground parent exits.

---

## How it works

Hooks record activity to a local SQLite database and deliver messages from it.

```bash
agent → hooks → db → hooks → other agent
```

Messages arrive mid-turn (injected between tool calls) or wake idle agents immediately.

Each agent gets a queryable identity:

- name
- status (active, blocked, listening)
- inbox
- live terminal screen
- transcript in structured chunks
- event log of every status change, file edit, tool call

Agents can subscribe to events and react instantly. Collision detection is on by default: if two agents edit the same file within 30 seconds, both get notified.

Hooks go additively into each tool's native config directory on first run.
`HCOM_DIR` stores hcom state only. Directly started Codex and Claude sessions
remain silent and unbound—even while other hcom agents are running. Their hooks
engage only for an hcom-launched session, an already-bound exact session, or
after you explicitly run `hcom start` inside that CLI.
If that command's output is deferred, binding may complete on a later hook only
when the same native session's transcript contains the exact marker for a
currently pending hcom identity. SessionStart does not advertise hcom.

Without hooks, any other AI tool can join by running `hcom start`. Any process can wake agents with `hcom send`.

---

## Terminal

Every agent runs in a real terminal you can see, scroll, and interrupt. Any emulator works for spawning; **kitty**, **wezterm**, **tmux**, **zellij**, **waveterm**, **cmux**, **herdr** also support closing panes from `hcom kill`.

To configure a custom terminal open/close setup, tell an agent to run:

```bash
hcom config terminal --info
```

---

## Cross-device

Connect agents across machines via MQTT relay.

```bash
hcom relay new               # get token
hcom relay connect <token>   # on each device
```

```bash
hcom relay status            # check connection
hcom relay off|on            # toggle
```

<details>
<summary>Relay Security</summary>

### Security

- Relay payloads are end-to-end encrypted. Brokers do not see data.
- Treat the join token like an SSH key or API key.
- If the token may have leaked, run `hcom relay off --all` to disconnect all devices.
- Use a private/custom/self-hosted broker with `--broker` and `--password` for better security.

### Security model

`hcom relay` is one trust domain for one operator's devices. Membership is all-or-nothing. There are no scoped roles, read-only peers, or per-device permissions.

Relay payloads use a shared PSK with XChaCha20-Poly1305. The encryption binds each payload to the relay, topic, and timestamp. A replay guard drops duplicate envelopes inside a freshness window.

Brokers and network observers cannot read or forge payloads without the PSK. They can still see metadata: topic names, timing, message sizes, and connection patterns.

### What the token means

The join token contains the relay ID, broker URL, and raw PSK. hcom does not ask a server to validate it. It has no expiry, no scope, and no revocation list.

On public brokers, a leaked token gives an attacker full control of the relay. They can decrypt captured traffic, publish authenticated relay traffic, send text to listening agents, launch agents on enrolled devices, kill running agents, and use remote relay RPCs. If those agents can run tools, treat that as shell access on every enrolled device in the relay.

On private brokers with `--password`, the token still leaks the PSK, so captured traffic is still exposed. But the token alone is not enough to publish unless the attacker also has the broker password. Use a private broker when broker-side access control matters, or when the metadata shape of your traffic is itself sensitive. `--password` is broker access control, not another layer of message encryption.

### Limits by design

- Forward secrecy. A leaked PSK can decrypt old captured traffic.
- Per-device attribution inside a relay. Sender identity is routing metadata, not authorization. Every enrolled device speaks with full authority.
- Prompt injection from an authenticated peer. Enrollment is total trust — a peer can launch, kill, and drive agents via RPC, not just send messages. Only enroll devices you would give shell access to.
- Local OS compromise. hcom trusts the local user account and `~/.hcom/config.toml`. It does not defend against another user on the same account or malware with filesystem access.

### Storage

The PSK is stored in `~/.hcom/config.toml`. On Unix, hcom writes that file with mode `0600`.

hcom keeps the PSK out of environment variables. Remote `config_get` and `config_set` refuse `relay_psk`, `relay_token`, `relay_id`, and the broker URL. `hcom relay status` shows only a short fingerprint so two devices can verify they share the same key without printing it.

Anyone who can read that file — another user on the same OS account, malware, or a backup written without preserving permissions — has the full PSK.

### Incident response

Run `hcom relay off --all`. It asks every reachable trusted peer to disable the relay, then disables it locally, so your agents stop acting on attacker messages. It is best-effort damage control, not containment: the attacker's device ignores the request.

The PSK cannot be revoked. There is no server to notify and no denylist to update. Anyone who has the PSK can keep using the old relay until you stop using it.

To keep using relay after a leak, create a new relay with `hcom relay new` and move every trusted device to the new token. Rotation also changes the `relay_id`, so retained state on the old broker topics is orphaned.

</details>

---

## Troubleshoot

```bash
hcom status                  # diagnostics
hcom reset all               # clear and archive: database + hooks + config
```

---

## Uninstall

```bash
hcom hooks remove            # safely remove all hcom hooks
brew uninstall hcom          # or: rm $(which hcom)
```

---

## Reference

<details>
<summary>Tools</summary>

### Supported tools

| Tool | Message delivery | Connect |
|---|---|---|
| Claude Code | automatic | `hcom claude` |
| Gemini CLI | automatic | `hcom gemini` |
| Codex CLI | automatic | `hcom codex` |
| Antigravity CLI | automatic | `hcom agy` |
| OpenCode | automatic | `hcom opencode` |
| Kilo Code | automatic | `hcom kilo` |
| Pi | automatic | `hcom pi` |
| Oh My Pi | automatic | `hcom omp` |
| Cursor CLI | automatic | `hcom cursor-agent` |
| Kimi | automatic | `hcom kimi` |
| Copilot CLI | automatic | `hcom copilot` |
| Anything else | manual via `hcom listen` | `hcom start` (run inside tool) |

```bash
hcom r <session_id>           # Resume a session started outside hcom
hcom f <session_id>           # Fork a session in hcom
```

#### Claude Code headless and subagents

Detached background processes in print mode stay alive. Manage through the TUI.

```bash
hcom claude -p 'say hi in hcom'   # print mode (separate Agent SDK credits)
hcom claude --headless            # Run normal claude in background pty (works for any tool)
```

For subagents, run `hcom claude`, then prompt:

> run 2x task tool and get them to talk to each other in hcom

</details>


<details>
<summary>CLI</summary>

### CLI commands

What you might type from a shell. Agents run their own commands that they learn from the hcom CLI primer (~700 tokens) at launch. `hcom <command> --help` for full flags.

### Spawn

```bash
hcom [N] claude|gemini|codex|agy|opencode|kilo|pi|omp|cursor-agent|kimi|copilot   # launch N agents
hcom r <name|session_id>                # resume agent
hcom f <name|session_id>                # fork session
hcom kill <name|tag:T|all>              # kill + close terminal pane
```

hcom launch flags:

| Flag | Purpose |
|---|---|
| `--tag <name>` | Group label — agents can be addressed as `@tag` |
| `--terminal <preset>` | Where windows open: `default` (auto-detect), `kitty`, `wezterm`, `tmux`, `cmux`, `iterm`, etc… |
| `--dir <path>` | Directory where the agent launches |
| `--headless` | Run in background pty with no terminal window |
| `--device <name>` | Spawn on a remote device (via relay) |
| `--hcom-prompt <text>` | Initial user prompt |
| `--hcom-system-prompt <text>` | Append to system prompt |

Anything else is forwarded to the tool: `--model sonnet`, `--yolo`, etc.

### Other commands

```bash
hcom                                # TUI dashboard
hcom send -b @luna -- hey           # one-off message to an agent
hcom list                           # show all active agents
hcom term [name]                    # view/inject into an agent's PTY screen
hcom events --wait <filters>         # Block until match for scripting
hcom update                         # report disabled upstream updates (nonzero)
```

`hcom run docs --cli` for all commands.

</details>

<details>
<summary>Config</summary>

### Configuration

Config lives in `~/.hcom/config.toml`. Precedence: defaults < `config.toml` < env vars.

```bash
hcom config                           # show all values with sources
hcom config <key>                     # get
hcom config <key> <value>             # set
hcom config <key> --info              # detailed help for a key
hcom config -i <name> <key> <value>   # per-agent override at runtime
```

### Keys

| Key | Purpose |
|---|---|
| `tag` | Group label — launched agents become `tag-name` |
| `hints` | Text appended to every message the agent receives |
| `notes` | Text appended to bootstrap (one-time, at launch) |
| `auto_approve` | Auto-approve safe hcom commands (send/list/events/…) |
| `auto_subscribe` | Event subscription presets: `collision`, `created`, `stopped`, `blocked` |
| `name_export` | Export instance name to a custom env var |
| `terminal` | Where new agent windows open (`hcom config terminal --info`) |
| `timeout` | Idle timeout for headless/vanilla Claude (seconds) |
| `subagent_timeout` | Keep-alive for Claude subagents (seconds) |
| `claude_args` / `gemini_args` / `codex_args` / `opencode_args` / `kilo_args` / `pi_args` / `omp_args` / `cursor_args` / `kimi_args` / `copilot_args` | Default args passed to the tool |

### Scope

```bash
hcom config tag mycrew                          # global
hcom config -i luna hints "respond in JSON"     # per-agent
HCOM_TAG=dev hcom 3 claude                      # per-launch env
```

### Per-project isolation

```bash
export HCOM_DIR="$PWD/.hcom"    # isolate hcom state only
rm -rf "$HCOM_DIR"              # removes only that hcom state
```

`HCOM_DIR` does not redirect any integrated tool's login, sessions, settings,
skills, plugins, or transcripts. Tool-native overrides such as
`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, and `GEMINI_CLI_HOME` remain authoritative.
Hooks are additive global integration entries; remove them separately with
`hcom hooks remove` only when you intend to disable hcom integration.

Run `hcom config <key> --info` or `hcom run docs --config` for the full per-key reference.

Edit `~/.hcom/env` to set external env vars passed to every launched agent.

</details>

<details>
<summary>Workflow Scripts</summary>

### Multi-agent workflows

Bundled and user scripts (`~/.hcom/scripts/`) for multi-agent patterns:

```bash
hcom run                   # list available scripts
hcom run debate "topic"    # run one
hcom run docs              # tell agent to run this to create any new workflow
```

### Included Scripts

Tell agent to run them:

**`hcom run confess`** — An agent (or background clone) writes an honesty self-eval. A spawned calibrator reads the target's transcript independently. A judge compares both reports and sends back a verdict via hcom message.

**`hcom run debate`** — A judge spawns and sets up a debate with existing agents. It coordinates rounds in a shared thread where all agents see each other's arguments, with shared context of workspace files and transcripts.

**`hcom run fatcow`** — headless agent reads every file in a path, subscribes to file edit events to stay current, and answers other agents on demand.

Custom scripts: drop `*.sh` or `*.py` into `~/.hcom/scripts/` — auto-discovered, override bundled scripts of the same name. Ask an agent to author one; `hcom run docs --scripts` is the authoring guide.

</details>

<details>
<summary>Build</summary>

### Building from Source

```bash
# Prerequisites: Rust 1.88+

git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build
cargo test
```

### Using local build

Two options:

**Symlink** — simple, dev build is global.

```bash
ln -sf $(pwd)/target/debug/hcom ~/.cargo/bin/hcom
```

**dev_root** — works regardless of how hcom was installed (brew, pip, etc.); picks the newer of debug/release automatically:

```bash
hcom config dev_root $(pwd)
hcom config dev_root --unset  # revert
hcom status    # run local build
```

For concurrent worktrees, scope each to its own DB:

```bash
HCOM_DIR=$PWD/.hcom HCOM_DEV_ROOT=$PWD hcom claude
```

</details>


---

## Contributing

Issues and PRs welcome. The codebase is Rust.

```bash
cargo build && cargo test
hcom config dev_root $(pwd)
hcom status
just ci  # run the CI gate locally

# On native Windows (PowerShell)
just ci-windows
```

---

## License

[MIT](LICENSE)
