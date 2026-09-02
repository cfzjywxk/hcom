# Everyday reference

The day-to-day surface inherited from [aannoo/hcom](https://github.com/aannoo/hcom),
plus the fork's `review` and `arch` entry points. `hcom <command> --help` is
always authoritative; `hcom run docs --cli` and `hcom run docs --config` print
the full command and config references.

## Install

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked      # Rust 1.88+
install -m 0755 target/release/hcom target/release/hcom-architect-mcp ~/.local/bin/
hcom --version
```

`hcom arch` looks for `hcom-architect-mcp` beside the `hcom` executable, then
in `~/.local/libexec`; install both from the same build.

This fork does not follow upstream releases. `hcom update`,
`hcom update --check`, and `hcom update --go` report that upstream updates are
disabled and return a nonzero status; build the fork revision you want instead.

## Quickstart

Terminal 1:

```bash
hcom claude   # codex / gemini / opencode / kilo / pi / omp / agy / cursor-agent / kimi / copilot
```

Terminal 2:

```bash
hcom codex
```

Prompt either agent:

- `ask the other agent their favorite cake`
- `review what claude did and send it fixes`
- `ask codex to review, keep fixing and re-reviewing until LGTM, at most 3 rounds`
- `spawn 3x gemini, split work, collect results`
- `fork yourself to investigate the bug and report back`

Open the TUI:

```bash
hcom
```

## What agents can do

**Message** each other in real time: intent, replies, bundled context for handoffs.

**Review loop** with deterministic `review → fix/rebut → re-review` state until
LGTM or a configured round limit. Agents start it from a natural-language
request; only structured `hcom review` commands advance its state, and it
supports local top-level Claude Code and Codex instances.

**Observe** each other: transcripts, file edits, terminal screens, command history.

**Subscribe** to each other: notify on status changes, file edits, specific events. React automatically.

**Spawn**, **fork**, **resume**, **kill** each other, in any terminal emulator or headless.

## How it works

Hooks record activity to a local SQLite database and deliver messages from it.

```text
agent → hooks → db → hooks → other agent
```

Messages arrive mid-turn (injected between tool calls) or wake idle agents immediately.

Each agent gets a queryable identity: name, status (active, blocked,
listening), inbox, live terminal screen, transcript in structured chunks, and
an event log of every status change, file edit, and tool call. Agents can
subscribe to events and react instantly. Collision detection is on by default:
if two agents edit the same file within 30 seconds, both get notified.

Hooks go additively into each tool's native config directory on first run.
`HCOM_DIR` stores hcom state only. Directly started Codex and Claude sessions
remain silent and unbound, even while other hcom agents are running. Their
hooks engage only for an hcom-launched session, an already-bound exact session,
or after you explicitly run `hcom start` inside that CLI. If that command's
output is deferred, binding may complete on a later hook only when the same
native session's transcript contains the exact marker for a currently pending
hcom identity. SessionStart does not advertise hcom.

Without hooks, any other AI tool can join by running `hcom start`. Any process
can wake agents with `hcom send`.

## Supported tools

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

### Claude Code headless and subagents

Detached background processes in print mode stay alive. Manage them through the TUI.

```bash
hcom claude -p 'say hi in hcom'   # print mode (separate Agent SDK credits)
hcom claude --headless            # run normal claude in a background pty (works for any tool)
```

For subagents, run `hcom claude`, then prompt:

> run 2x task tool and get them to talk to each other in hcom

## CLI commands

What you might type from a shell. Agents run their own commands that they
learn from the hcom CLI primer (~700 tokens) at launch.

### Spawn

```bash
hcom [N] claude|gemini|codex|agy|opencode|kilo|pi|omp|cursor-agent|kimi|copilot   # launch N agents
hcom r <name|session_id>                # resume agent
hcom f <name|session_id>                # fork session
hcom kill <name|tag:T|all>              # kill + close terminal pane
```

Launch flags:

| Flag | Purpose |
|---|---|
| `--tag <name>` | Group label — agents can be addressed as `@tag` |
| `--terminal <preset>` | Where windows open: `default` (auto-detect), `kitty`, `wezterm`, `tmux`, `cmux`, `iterm`, etc. |
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
hcom events --wait <filters>        # block until match, for scripting
hcom review --help                  # review loop: start / verdict / fixed / rebut / extend / cancel
hcom arch --help                    # foreground Architect and background task workers
hcom update                         # report disabled upstream updates (nonzero)
```

### Review loop

```text
review start @REVIEWER [--max-rounds N] -- TASK      start a persistent review loop (default: 3 rounds)
review verdict ID --round N --lgtm -- SUMMARY         approve as the bound reviewer
review verdict ID --round N --request-changes -- ...  request changes as the bound reviewer
review fixed ID --round N -- SUMMARY                  submit fixes and request the next review
review rebut ID --round N -- SUMMARY                  submit a rebuttal and request the next review
review status ID [--json] | review list [--json]      inspect workflows
review cancel ID -- REASON                            cancel as either participant
review extend ID --max-rounds NEW_TOTAL               raise the limit after max rounds
```

States: `awaiting_review` → `awaiting_developer` → … → `approved`,
`max_rounds`, or `canceled`. Only these structured commands change state;
ordinary message text never does.

## Configuration

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
| `[architect.profile]` / `[architect.developer]` / `[architect.reviewer1]` / `[architect.reviewer2]` / `[architect.github]` | Architect lane profiles; see the [Architect guide](architect.md) |

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

Edit `~/.hcom/env` to set external env vars passed to every launched agent.

## Terminal

Every agent runs in a real terminal you can see, scroll, and interrupt. Any
emulator works for spawning; **kitty**, **wezterm**, **tmux**, **zellij**,
**waveterm**, **cmux**, **herdr** also support closing panes from `hcom kill`.

To configure a custom terminal open/close setup, tell an agent to run:

```bash
hcom config terminal --info
```

## Cross-device

Connect agents across machines via MQTT relay.

```bash
hcom relay new               # get token
hcom relay connect <token>   # on each device
hcom relay status            # check connection
hcom relay off|on            # toggle
```

### Relay security

- Relay payloads are end-to-end encrypted. Brokers do not see data.
- Treat the join token like an SSH key or API key.
- If the token may have leaked, run `hcom relay off --all` to disconnect all devices.
- Use a private/custom/self-hosted broker with `--broker` and `--password` for better security.

**Security model.** `hcom relay` is one trust domain for one operator's
devices. Membership is all-or-nothing. There are no scoped roles, read-only
peers, or per-device permissions.

Relay payloads use a shared PSK with XChaCha20-Poly1305. The encryption binds
each payload to the relay, topic, and timestamp. A replay guard drops duplicate
envelopes inside a freshness window.

Brokers and network observers cannot read or forge payloads without the PSK.
They can still see metadata: topic names, timing, message sizes, and
connection patterns.

**What the token means.** The join token contains the relay ID, broker URL,
and raw PSK. hcom does not ask a server to validate it. It has no expiry, no
scope, and no revocation list.

On public brokers, a leaked token gives an attacker full control of the relay.
They can decrypt captured traffic, publish authenticated relay traffic, send
text to listening agents, launch agents on enrolled devices, kill running
agents, and use remote relay RPCs. If those agents can run tools, treat that as
shell access on every enrolled device in the relay.

On private brokers with `--password`, the token still leaks the PSK, so
captured traffic is still exposed. But the token alone is not enough to publish
unless the attacker also has the broker password. Use a private broker when
broker-side access control matters, or when the metadata shape of your traffic
is itself sensitive. `--password` is broker access control, not another layer
of message encryption.

**Limits by design.**

- Forward secrecy. A leaked PSK can decrypt old captured traffic.
- Per-device attribution inside a relay. Sender identity is routing metadata, not authorization. Every enrolled device speaks with full authority.
- Prompt injection from an authenticated peer. Enrollment is total trust — a peer can launch, kill, and drive agents via RPC, not just send messages. Only enroll devices you would give shell access to.
- Local OS compromise. hcom trusts the local user account and `~/.hcom/config.toml`. It does not defend against another user on the same account or malware with filesystem access.

**Storage.** The PSK is stored in `~/.hcom/config.toml`. On Unix, hcom writes
that file with mode `0600`. hcom keeps the PSK out of environment variables.
Remote `config_get` and `config_set` refuse `relay_psk`, `relay_token`,
`relay_id`, and the broker URL. `hcom relay status` shows only a short
fingerprint so two devices can verify they share the same key without printing
it. Anyone who can read that file — another user on the same OS account,
malware, or a backup written without preserving permissions — has the full PSK.

**Incident response.** Run `hcom relay off --all`. It asks every reachable
trusted peer to disable the relay, then disables it locally, so your agents
stop acting on attacker messages. It is best-effort damage control, not
containment: the attacker's device ignores the request. The PSK cannot be
revoked. To keep using relay after a leak, create a new relay with
`hcom relay new` and move every trusted device to the new token. Rotation also
changes the `relay_id`, so retained state on the old broker topics is orphaned.

## Workflow scripts

Bundled and user scripts (`~/.hcom/scripts/`) for multi-agent patterns:

```bash
hcom run                   # list available scripts
hcom run debate "topic"    # run one
hcom run docs              # tell an agent to run this to create any new workflow
```

**`hcom run confess`** — An agent (or background clone) writes an honesty
self-eval. A spawned calibrator reads the target's transcript independently. A
judge compares both reports and sends back a verdict via hcom message.

**`hcom run debate`** — A judge spawns and sets up a debate with existing
agents. It coordinates rounds in a shared thread where all agents see each
other's arguments, with shared context of workspace files and transcripts.

**`hcom run fatcow`** — A headless agent reads every file in a path,
subscribes to file edit events to stay current, and answers other agents on
demand.

Custom scripts: drop `*.sh` or `*.py` into `~/.hcom/scripts/`; they are
auto-discovered and override bundled scripts of the same name.
`hcom run docs --scripts` is the authoring guide.

## Building from source

```bash
# Prerequisites: Rust 1.88+ (the MSRV). rust-toolchain.toml pins CI and strict
# Clippy to an exact compiler release; clippy.toml keeps suggestions MSRV-aware.
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build
cargo test
just ci            # run the CI gate locally
just ci-windows    # on native Windows (PowerShell)
```

Using a local build, two options:

```bash
ln -sf $(pwd)/target/debug/hcom ~/.cargo/bin/hcom   # symlink: dev build becomes global

hcom config dev_root $(pwd)      # dev_root: works however hcom was installed; picks the newer of debug/release
hcom config dev_root --unset     # revert
hcom status                      # runs the local build
```

For concurrent worktrees, scope each to its own DB:

```bash
HCOM_DIR=$PWD/.hcom HCOM_DEV_ROOT=$PWD hcom claude
```

Real-tool integration tests use `--force` only for PTYs they create and own;
ordinary terminal injection keeps the guarded prompt/draft ownership checks.
Real Claude task-lane validation is always an explicit, serial,
Haiku/medium-only opt-in; see [Claude task-lane testing](claude-task-lane-testing.md).

The README diagrams are generated by `docs/assets/gen_diagrams.py`; rerun it
after editing rather than hand-editing the SVGs.

## Troubleshoot

```bash
hcom status                  # diagnostics
hcom reset all               # clear and archive: database + hooks + config
```

## Uninstall

```bash
hcom hooks remove            # safely remove all hcom hooks
rm ~/.local/bin/hcom ~/.local/bin/hcom-architect-mcp
```
