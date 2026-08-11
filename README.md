# hcom

[![CI](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/cfzjywxk/hcom/blob/master/LICENSE)

> Hook your coding agents together.

## Upstream foundation

This repository is a fork of [aannoo/hcom](https://github.com/aannoo/hcom).
Upstream hcom connects coding-agent CLIs so they can message and observe one
another across terminals, and spawn, fork, or resume collaborators. It is a
single CLI with no resident background service.

## What this fork adds

### Deterministic review loops

Review work follows a deterministic
`review → fix/rebut → re-review` loop until LGTM or the configured round limit.
Both interactive hcom agents and no-TUI background workers launched by an
Architect can use the same loop.

### Background Architect worker lane

Start a blank interactive Architect session with `hcom arch codex` or
`hcom arch claude`. In that session, the human discusses and refines the work,
reviews the typed plan, and approves it. The human then asks the Architect to
**drive** the approved work. Here, “drive” is natural-language execution intent,
not an `hcom arch drive` command.

After approval, the foreground supervisor handles tasks in order. Each task
starts a fresh no-TUI Developer followed by the active Reviewer or Reviewers.
For corrections on that same task, the Developer resumes its exact session and
each Reviewer re-reviews in its own exact session.

The only Architect entry points are `hcom arch codex` and `hcom arch claude`;
`hcom architect` is not a command. See the [Architect guide](docs/architect.md)
for profiles, authorization, review topology, and lifecycle details. The
optional [GitHub Pull Request lane](docs/github-pr-lane.md) adds explicit manual
or protected-auto-merge delivery modes.

## Install

Build this fork from the selected source revision:

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked --bins
export PATH="$PWD/target/release:$PATH"
hcom --version
```

This fork does not follow upstream releases. The retained `hcom update` forms
report that upstream updates are disabled; update by building the intended fork
revision instead.

## Interactive quickstart

Launch agents in separate terminals:

```bash
# Terminal 1
hcom claude

# Terminal 2
hcom codex
```

Then prompt either agent to collaborate, for example:

- `ask the other agent to review this change`
- `keep fixing and re-reviewing until LGTM, at most 3 rounds`
- `fork yourself to investigate the failure and report back`

Run `hcom` with no arguments to open the dashboard. Supported integrations
include Claude Code, Codex, Gemini CLI, OpenCode, Kilo Code, Pi, Oh My Pi,
Antigravity, Cursor, Kimi, and Copilot. Other tools can join with `hcom start`
and receive work through `hcom listen`.

## Architect quickstart

```bash
cd /path/to/project

# Blank Codex or Claude Architect:
hcom arch codex
hcom arch claude

# Codex Architect with Reviewer1 only:
hcom arch codex --single-review

# Opt-in manual Pull Request delivery:
hcom arch codex --single-review --github-pr
```

The command starts a blank interactive session; the human supplies the first
prompt and retains input ownership. Pull Request delivery is opt-in and does
not imply install or release authority.

## Core commands

```bash
hcom [N] <tool>                   # launch one or more agents
hcom send @name -- message       # send a message
hcom list                         # list active agents
hcom term [name]                  # inspect an agent terminal
hcom r <name|session-id>          # resume a session
hcom f <name|session-id>          # fork a session
hcom events --wait <filters>      # wait for matching activity
hcom kill <name|tag:T|all>        # stop an agent
hcom status                       # show diagnostics
hcom run                          # list bundled workflows
```

Use `hcom <command> --help` for complete flags, or `hcom run docs --cli` for
the generated CLI reference. Launch flags can select a tag, directory,
terminal preset, remote device, or headless mode; unrecognized tool arguments
are forwarded to the underlying CLI.

For cross-device collaboration:

```bash
hcom relay new
hcom relay connect <token>
hcom relay status
```

Relay membership is a full-trust domain. Protect the join token like an SSH key
and use `hcom relay off --all` if it may have leaked.

## Configuration

Configuration lives in `~/.hcom/config.toml`:

```bash
hcom config                         # effective values and sources
hcom config <key>                   # get a value
hcom config <key> <value>           # set a value
hcom config <key> --info            # key-specific help
hcom config terminal --info         # terminal presets
```

Defaults are overridden by `config.toml`, then environment variables. Set
`HCOM_DIR="$PWD/.hcom"` to isolate hcom state for a project; this does not
relocate an integrated tool's native config, login, sessions, or plugins. Run
`hcom run docs --config` for the full configuration reference.

## Further reading

- [Architect and background worker guide](docs/architect.md)
- [GitHub Pull Request delivery lane](docs/github-pr-lane.md)
- [Claude task-lane testing](docs/claude-task-lane-testing.md)
- [Codex adapter contract](docs/codex-adapter-contract.md)
- [Codex no-TUI worker lane](docs/codex-exec-worker-lane.md)

Real provider/model tests and disposable TUI tests are explicit opt-ins. Normal
`cargo test` uses local fakes; the Claude testing guide documents the stricter
requirements for real Claude validation.

## Build and contribute

The codebase is Rust and requires Rust 1.88 or newer. The checked-in toolchain
pins the compiler used by CI.

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
just ci
```

Issues and pull requests are welcome. Real-tool integration tests act only on
the disposable terminals they create; ordinary terminal input remains
human-owned.

## License

[MIT](LICENSE)
