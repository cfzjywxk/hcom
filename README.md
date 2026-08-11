# hcom

[![CI](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/cfzjywxk/hcom/blob/master/LICENSE)

## Upstream

This project builds on [aannoo/hcom](https://github.com/aannoo/hcom). Upstream
connects multiple coding-agent CLIs so they can exchange messages across
terminals, observe one another, and spawn, fork, or resume collaborators. It
does this without a resident background service.

## What this fork adds

### Deterministic review loops

A review follows an explicit `review → fix/rebut → re-review` state machine
until the Reviewer returns LGTM or the configured round limit is reached. The
same loop is available to tagged interactive hcom agents and to the no-TUI
background workers dispatched by an Architect.

Only structured `hcom review` commands advance the state; ordinary messages do
not. Run `hcom review --help` for the start, verdict, fix, rebuttal, status, and
extension commands.

### Background Architect worker lane

Start a blank interactive Architect with `hcom arch codex` or
`hcom arch claude`. In that session, the human discusses and refines the work,
reviews the proposed task plan, and approves it. The human then tells the
Architect in natural language to **drive the approved work**.

The foreground supervisor processes approved tasks in order. For each task it
starts a fresh no-TUI Developer and the active Reviewer or Reviewers. If a
review requests changes, the correction resumes that task's exact Developer
session and the re-review resumes each Reviewer's own exact session.

`drive` is an instruction in the Architect conversation, not a CLI subcommand.
The public entry points are `hcom arch codex` and `hcom arch claude`; there is
no `hcom arch drive` or `hcom architect` command. See the
[Architect guide](docs/architect.md) for approval, profiles, review modes, and
lifecycle details.

## Install

Build this fork from the selected source revision:

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked
./target/release/hcom --version
```

This fork does not follow upstream releases. The retained `hcom update` forms
report that upstream updates are disabled; update by building the desired fork
revision instead.

## Interactive quickstart

Launch agents in separate terminals:

```bash
# Terminal 1
hcom claude

# Terminal 2
hcom codex
```

Then ask either agent to message or collaborate with the other, for example:

```text
Ask the other agent to review this change. Fix or rebut every finding and
continue re-reviewing until LGTM, with at most three rounds.
```

Run `hcom` with no arguments to open the TUI dashboard. Common launch forms
include:

```bash
hcom 3 gemini                  # launch three agents
hcom r <name-or-session-id>    # resume
hcom f <name-or-session-id>    # fork
hcom codex --headless          # background PTY
```

Supported integrations include Claude Code, Gemini CLI, Codex CLI, OpenCode,
Kilo Code, Pi, Oh My Pi, Antigravity, Cursor CLI, Kimi, and Copilot CLI. Other
tools can join with `hcom start` and receive messages with `hcom listen`.

## Architect quickstart

Run an Architect from the project directory whose plans and instructions it
should use:

```bash
cd /path/to/project
hcom arch codex

# Alternatives and opt-in modes:
hcom arch claude
hcom arch codex --single-review
hcom arch codex --github-pr
```

The Architect opens blank: the human supplies and submits the first prompt.
Discuss the scope, inspect the typed plan, approve it, and then ask the
Architect to drive the approved work. `--single-review` is available only with
the Codex Architect. `--github-pr` selects manual Pull Request delivery; it
does not merge, install, or release. See the
[GitHub Pull Request lane guide](docs/github-pr-lane.md) for setup and terminal
states.

## Core commands

Use `hcom <command> --help` for complete flags.

| Command | Purpose |
|---|---|
| `hcom` | Open the TUI dashboard |
| `hcom [N] <tool>` | Launch one or more agents |
| `hcom r <target>` / `hcom f <target>` | Resume or fork an agent session |
| `hcom send ...` | Send a message |
| `hcom review ...` | Run or inspect a deterministic review loop |
| `hcom list` / `hcom events` | Inspect agents and activity |
| `hcom transcript` / `hcom bundle` | Read or package collaboration context |
| `hcom term [name]` | View or interact with an agent PTY |
| `hcom relay ...` | Connect trusted devices |
| `hcom config` / `hcom status` | Configure and diagnose hcom |
| `hcom hooks ...` | Add or remove tool integration hooks |
| `hcom run ...` | Run bundled or user workflow scripts |

Launch options include `--tag`, `--terminal`, `--dir`, `--headless`,
`--hcom-prompt`, and `--hcom-system-prompt`; remaining arguments are forwarded
to the selected coding-agent CLI.

## Configuration

Configuration lives in `~/.hcom/config.toml`, with defaults overridden by that
file and then by environment variables.

```bash
hcom config                         # show values and sources
hcom config terminal --info         # inspect one setting
hcom config tag mycrew              # set a global tag
hcom config -i luna hints "Use JSON" # per-agent override
```

Set `HCOM_DIR` to isolate hcom state for a project. It does not redirect the
integrated tools' own authentication, configuration, plugins, or session
stores. Architect role profiles also live in `$HCOM_DIR/config.toml`; their
schema and native-environment behavior are documented in the
[Architect guide](docs/architect.md).

## Cross-device relay

```bash
hcom relay new               # create a relay and print its join token
hcom relay connect <token>   # connect another trusted device
hcom relay status
```

Treat the join token as a secret with full authority over that relay. Run
`hcom relay --help` before enrolling devices or choosing a broker.

## Documentation

- [Architect sessions and worker lanes](docs/architect.md)
- [GitHub Pull Request delivery](docs/github-pr-lane.md)
- [Claude task-lane testing](docs/claude-task-lane-testing.md)
- [Codex adapter contract](docs/codex-adapter-contract.md)
- [Codex exec worker lane](docs/codex-exec-worker-lane.md)

## Build and contribute

The codebase is Rust and requires Rust 1.88 or newer. Before submitting a
change, run the relevant tests and the local CI gate:

```bash
cargo build --locked
cargo test --locked
cargo fmt --check
just ci
```

Issues and Pull Requests are welcome. Use `hcom config dev_root "$(pwd)"` to
exercise a local build through an existing hcom installation, and unset it
with `hcom config dev_root --unset` when finished.

## License

[MIT](LICENSE)
