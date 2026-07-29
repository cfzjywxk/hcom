# Phase 8 safety baseline: foreground session task workers

Status: active for the minimal Phase 8 product.

This document describes the safety boundary implemented by `hcom architect`.
One foreground invocation owns one in-memory ordered task run. It does not
create a project, persistent scheduler, daemon, recovery store, standalone
repository, object-transfer lane, or final-apply operation.

## Lifetime and input ownership

`hcom architect` launches one blank interactive architect and waits for the
human's first input. The launcher does not pass a prompt argument, write to the
architect's stdin, inject a PTY event, paste text, or submit Enter.

The architect has four capability-bound tools:

- replace an ordered task-plan draft;
- start the exact plan version and hash after explicit human approval;
- read the current in-memory session status;
- cancel the exact current session version.

Drafting never starts a worker. The start action requires an exact positive plan
version, an exact SHA-256 plan hash, and `approval_confirmed=true`. The bridge is
bound to the architect process tree, executable, PID/birth identities, native
session, nonce, capability secret, repository, and exact action inventory.

The architect parent owns the supervisor, bridge, architect child, and every
worker. A termination signal or parent/architect failure cancels the exact live
worker and returns failure. Worker processes use no controlling TTY, a fresh
session/process group, PID+birth binding, pidfd observation, `PDEATHSIG`, bounded
concurrent pipe drains, and exact descendant cleanup. Nothing continues or
recovers after the foreground invocation exits.

## Canonical checkout boundary

The developer writes and commits directly in the canonical checkout selected by
the human. The reviewer receives the same checkout read-only. This deliberately
has a larger blast radius than a disposable repository and is printed when the
run starts.

The supervisor:

1. requires an existing canonical Git top level, attached branch, and completely
   clean worktree before opening the session;
2. prints and records the run ID, repository, start branch, and start HEAD;
3. holds a nonblocking current-user runtime `flock` keyed by the checkout
   device/inode so renaming the live directory cannot create a second lock;
4. permits exactly one live worker;
5. accepts a developer result only when the checkout is clean, remains on the
   starting branch, names a committed HEAD, fast-forwards the exact task base,
   and does not rewrite the previous same-task turn HEAD;
6. recomputes the exact commit list and changed-path list, rejects paths outside
   the approved task allowlist, and requires every approved check to be reported
   passed by both a completed developer and an LGTM reviewer;
7. binds a reviewer turn to the exact observed clean HEAD and revalidates it
   while the reviewer runs and before applying the verdict;
8. uses the exact reviewed HEAD as the next task's base;
9. stops in `needs_human` on branch, HEAD, worktree, Git identity, replacement
   ref, graft, alternate, or result drift.

The supervisor never resets, rebases, merges, checks out, force-cleans, pushes,
installs, or applies a final patch. A reviewed developer commit is already in
the human's checkout.

## Task and native-session state

The approved plan, task states, current base/head, review round, exact logical
and native session IDs, used-session inventory, accepted completion tokens, and
terminal outcome live only in parent memory.

Every task receives fresh developer and reviewer logical sessions. A native
session ID may not cross a task or role. `request_changes` resumes the exact
same task-bound developer and reviewer sessions. Worker crashes have a fixed
small attempt limit; once a native session exists, every retry must be an exact
resume. Ambiguous or missing session evidence stops the queue.

A completion token is accepted at most once. Duplicate and late completions
cannot clear or advance another active turn. `review_exhausted` is distinct from
LGTM and still advances to the next explicitly approved task. Status carries a
validated, per-task outcome summary capped at 1 KiB so a human can see a worker
question or unresolved review without exposing unbounded model output.

## Empty-root worker sandbox

Production Codex and Claude worker adapters use a bubblewrap empty root. The
manifest exposes only:

- private `/proc`, `/dev`, `/tmp`, `/run`, and `/dev/shm`;
- exact read-only `/usr`, `/etc`, resolver, Rust toolchain, native executable,
  and minimal auth source;
- isolated writable HOME/native state and the exact artifact attempt;
- the canonical checkout read-write for the developer or read-only for the
  reviewer.

No host root, control socket, registration socket, relay socket, TTY, push
credential, interactive hcom identity, or sibling worker/session root is
mounted. The reviewer write probe is required to fail with `EROFS`.

Executable, toolchain, repository/Git administration, auth file, mount target,
runtime, and private-directory identities are captured and revalidated.
Artifacts and structured results use bounded schemas, create-once private
paths, atomic sealed files, hashes, redaction, and role/turn/session bindings.

## Environment and proxy inheritance

The parent captures a closed environment allowlist once for this invocation.
All present upper- and lower-case `https_proxy`, `http_proxy`, `all_proxy`, and
`no_proxy` names are inherited with their exact names and values. They are not
normalized, reconstructed, persisted, or recovered from another process.

Workers receive isolated generated HOME, native config, temp, runtime, XDG, and
Rust paths plus role/run/task markers. Unknown environment names are rejected.
Environment descriptors persist names and a hash, never raw proxy credentials;
artifact redaction covers raw values and proxy userinfo.

## Verification boundary

Phase 8 uses fake no-TTY workers and local Git repositories only. Its tests
cover:

- two tasks with fresh cross-task identities;
- same-task developer and reviewer exact resume;
- LGTM and explicit `review_exhausted` automatic advancement;
- duplicate/late completion rejection;
- reviewer HEAD drift;
- dirty start and second-session repository locking;
- bounded exact-resume crash retry;
- prompt absence from argv and zero worker TTY;
- parent-exit worker death;
- developer read-write and reviewer `EROFS` mounts;
- retained hcom tests.

Real model/network, disposable terminal acceptance, installation, and push are
Phase 9 actions and require separate human authorization.
