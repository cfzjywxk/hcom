# hcom

[![CI](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/hcom/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/cfzjywxk/hcom/blob/master/LICENSE)

> Hook your coding agents together.

## Upstream

This project is a fork of [aannoo/hcom](https://github.com/aannoo/hcom).
Upstream hcom connects coding-agent CLIs so they can message and observe one
another across terminals, and spawn, fork, or resume collaborators. It works
without a resident background service: start an agent through `hcom`, then use
the agent's normal interface.

## What this fork adds

This fork, currently version 1.0.34, retains hcom's interactive collaboration
features and adds two structured ways to drive reviewed work:

- **Deterministic review loops.** A task follows
  `review → fix/rebut → re-review` until every active reviewer returns LGTM or
  the configured round limit is reached. Interactive hcom agents can use the
  loop, and so can no-TUI background workers started by an Architect.
- **A background Architect worker lane.** Start a blank interactive Architect
  with `hcom arch codex` or `hcom arch claude`. Discuss and refine the work in
  that session, inspect and approve its task plan, then explicitly ask the
  Architect to “drive” the approved work. The foreground supervisor dispatches
  a fresh Developer followed by the active Reviewer or Reviewers. Same-task
  corrections resume the exact Developer session, and re-review resumes each
  Reviewer's own exact session.

`drive` is natural-language intent inside the Architect session; there is no
`hcom arch drive` command, and `hcom architect` is not an alias. See the
[Architect guide](docs/architect.md) for the full approval, worker, profile,
and lifecycle contract.

## Install

Rust 1.88 or newer is required.

```bash
git clone https://github.com/cfzjywxk/hcom.git
cd hcom
cargo build --release --locked
./target/release/hcom --version
```

This fork does not follow upstream releases. The retained `hcom update`,
`hcom update --check`, and `hcom update --go` commands report that upstream
updates are disabled and exit nonzero; build the selected fork revision
instead.

## Interactive quickstart

Start agents in separate terminals:

```bash
# Terminal 1
hcom claude

# Terminal 2
hcom codex
```

Then ask either agent to collaborate, for example:

```text
Ask codex to review this change, keep fixing or rebutting findings and
re-reviewing until LGTM, at most 3 rounds.
```

Open the dashboard from another terminal:

```bash
hcom
```

Other supported launch targets include `gemini`, `opencode`, `kilo`, `pi`,
`omp`, `agy`, `cursor-agent`, `kimi`, and `copilot`. Run multiple agents with a
count, such as `hcom 3 gemini`. A tool without native integration can join by
running `hcom start` inside its session.

## Architect quickstart

Start from the project whose work you want to plan:

```bash
cd /path/to/project

# Default dual-review lane
hcom arch codex

# Codex Architect with one Reviewer
hcom arch codex --single-review

# Claude Architect
hcom arch claude
```

The Architect opens as a blank interactive session. Describe the goal, discuss
tradeoffs, and inspect the typed plan before approving it. Once the task is
approved, tell the Architect in natural language to drive the approved work.
The foreground supervisor then runs the fresh no-TUI Developer/Reviewer loop
and returns progress to the same Architect session.

Local signed-off candidate commits are the default delivery. Manual GitHub Pull
Request delivery is an explicit opt-in and can be combined with single review:

```bash
hcom arch codex --single-review --github-pr
```

Manual delivery leaves the reviewed Pull Request, generated branch, and linked
worktree unmerged for human disposition. It does not imply install, release, or
deployment. Provisioning, permissions, checks, and the separately authorized
protected auto-merge policy are documented in the
[GitHub Pull Request lane guide](docs/github-pr-lane.md).

## Core commands

Use `hcom <command> --help` for all flags.

```bash
hcom list                              # list active agents
hcom send -b @luna -- "please review" # send a message
hcom term luna                         # view an agent's terminal
hcom r <name-or-session-id>            # resume a session
hcom f <name-or-session-id>            # fork a session
hcom kill <name|tag:T|all>             # stop agents and close their panes
hcom events --wait                     # wait for agent events
hcom status                            # show diagnostics
```

Agents can exchange messages and context bundles, observe transcripts and file
activity, subscribe to events, and coordinate review loops. Hooks record local
activity in hcom's SQLite state and deliver messages between connected agents;
the agents continue to use their native CLIs and terminals.

### Configuration

Configuration lives in `~/.hcom/config.toml`:

```bash
hcom config                            # show values and their sources
hcom config <key>                      # read a value
hcom config <key> <value>              # set a value
hcom config <key> --info               # explain a key
```

Common keys include `tag`, `hints`, `notes`, `auto_approve`, `auto_subscribe`,
`terminal`, and per-tool default arguments. Environment variables can override
configuration for a launch. To isolate hcom state for a project:

```bash
export HCOM_DIR="$PWD/.hcom"
```

`HCOM_DIR` changes hcom's state location only; it does not redirect a tool's
native login, settings, plugins, or session store.

### Terminals and relay

Hcom can open agents in common terminal emulators and multiplexers. Inspect the
available terminal presets with:

```bash
hcom config terminal --info
```

To connect trusted devices through the encrypted MQTT relay:

```bash
hcom relay new
hcom relay connect <token>
hcom relay status
```

Treat the relay token as a secret granting full participation in that relay.

## Further documentation

- [Architect sessions and worker profiles](docs/architect.md)
- [GitHub Pull Request delivery](docs/github-pr-lane.md)
- [Claude task-lane testing](docs/claude-task-lane-testing.md)
- [Codex adapter contract](docs/codex-adapter-contract.md)
- [Codex no-TUI worker lane](docs/codex-exec-worker-lane.md)

## Build and contribute

```bash
cargo build --locked
cargo test --locked
just ci
```

The checked-in toolchain and CI run strict formatting, Clippy, and tests. Issues
and Pull Requests are welcome. Real provider/model tests, interactive TUI
tests, installation, and release are separate opt-in operations; see the linked
testing documentation before running them.

## License

[MIT](LICENSE)
