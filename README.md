# hcom

[![CI](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/cfzjywxk/hcom/blob/master/LICENSE)

## Upstream

This project builds on [aannoo/hcom](https://github.com/aannoo/hcom). Upstream
hcom connects coding-agent CLIs so they can message and observe one another
across terminals, and collaborate by spawning, forking, or resuming sessions.
It runs as a single CLI without a persistent background service.

## What this fork adds

### Deterministic review loops

A structured `review → fix/rebut → re-review` loop keeps the same workflow
moving until the reviewer returns LGTM or the configured round limit is
reached. Interactive hcom agents can start and advance the loop, and the
background workers launched by an Architect use the same deterministic review
cycle.

```bash
hcom review start @reviewer --max-rounds 3 -- \
  'Review the implementation and tests'
```

### Background Architect workers

Start a blank interactive Architect with `hcom arch codex` or
`hcom arch claude`. In that session, discuss and refine the work, inspect the
typed task plan, and approve it. Then ask the Architect in natural language to
“drive the approved plan.” The foreground supervisor processes approved tasks
in order, starting a fresh no-TUI Developer and fresh active Reviewer sessions
for each task. A same-task correction resumes the exact Developer session, and
re-review resumes each Reviewer's exact session.

`drive` is an instruction inside the Architect conversation, not an
`hcom arch drive` command. The public Architect entry points are
`hcom arch codex` and `hcom arch claude`; `hcom architect` remains an unknown
command.

See the [Architect guide](docs/architect.md) for task binding, profiles,
approval, worker lifecycle, and single/dual review modes. Pull Request delivery
is documented separately in the
[GitHub PR lane guide](docs/github-pr-lane.md).

## Install

Build this fork from the revision you intend to use:

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked
./target/release/hcom --version
```

This fork does not follow upstream releases. The retained `hcom update` forms
report that upstream updates are disabled; update from a selected fork revision
instead.

## Interactive quickstart

Launch two supported agents in separate terminals:

```bash
# Terminal 1
hcom claude

# Terminal 2
hcom codex
```

Then ask either agent to message, inspect, or delegate to the other. hcom also
supports Gemini CLI, OpenCode, Kilo Code, Pi, Oh My Pi, Antigravity, Cursor CLI,
Kimi, and Copilot CLI.

Useful starting points:

```bash
hcom                         # open the dashboard
hcom list                    # list active agents
hcom send @name -- hello     # send a message
hcom term name               # view an agent's terminal
hcom 3 gemini --tag research # launch a tagged group
hcom r name                  # resume a session
hcom f name                  # fork a session
```

Run `hcom <command> --help` for command-specific options or
`hcom run docs --cli` for the complete CLI reference.

## Architect quickstart

Run an Architect from the directory that holds the project context:

```bash
cd /path/to/project
hcom arch codex

# Codex Architect with Reviewer1 only
hcom arch codex --single-review

# Claude Architect
hcom arch claude
```

The Architect opens blank; the human supplies the first prompt. The built-in
worker topology is Codex Developer, Codex Reviewer1, and Claude Reviewer2.
`--single-review` is available only with the Codex Architect and activates only
Reviewer1. Each role can select a different provider through configuration.

GitHub Pull Request delivery is explicit and manual by default:

```bash
hcom arch codex --single-review --github-pr
```

It preserves a reviewed PR for human disposition; it does not imply install or
release. See the [GitHub PR lane guide](docs/github-pr-lane.md) before enabling
it. Real Claude task-lane validation is a separate opt-in described in the
[Claude task-lane testing guide](docs/claude-task-lane-testing.md).

## Core commands

| Command | Purpose |
|---|---|
| `hcom [N] <tool>` | Launch one or more coding-agent sessions |
| `hcom send ...` | Message agents or tagged groups |
| `hcom review ...` | Run the structured review workflow |
| `hcom r <name>` / `hcom f <name>` | Resume or fork a session |
| `hcom list` / `hcom events` | Inspect agents and activity |
| `hcom term [name]` | View or interact with an owned agent terminal |
| `hcom kill <name>` | Stop an agent and close its supported pane |
| `hcom config ...` | Read or change configuration |
| `hcom relay ...` | Connect trusted devices over an encrypted MQTT relay |
| `hcom run ...` | Run or inspect workflow scripts |
| `hcom status` | Show diagnostics |

hcom can deliver messages through native tool hooks. Other tools can join with
`hcom start` and receive work through `hcom listen`. Tags, bundles,
subscriptions, terminal observation, and relay remain available without a
daemon; use command help for their detailed contracts.

## Configuration

Configuration lives in `~/.hcom/config.toml` with precedence
`defaults < config.toml < environment`.

```bash
hcom config                         # values and their sources
hcom config <key>                   # read one value
hcom config <key> <value>           # set one value
hcom config <key> --info            # detailed key help
hcom config -i <name> <key> <value> # per-agent runtime override
```

Common keys include `tag`, `hints`, `terminal`, `auto_approve`,
`auto_subscribe`, and tool-specific argument settings. Architect role profiles
use the `architect.*` tables described in the
[Architect guide](docs/architect.md). Run `hcom run docs --config` for the full
reference.

Set `HCOM_DIR` to isolate hcom state for a project. It does not relocate or
replace an integrated tool's native login, configuration, skills, plugins, or
session history.

## Build and contribute

Rust 1.88 or newer is required. The repository pins the compiler used by CI.

```bash
cargo build --locked
cargo fmt --check
cargo test --locked --all-targets --all-features
just ci
```

Issues and Pull Requests are welcome. Keep behavior changes focused, add tests
for affected contracts, and run the relevant local checks before submitting.

## License

[MIT](LICENSE)
