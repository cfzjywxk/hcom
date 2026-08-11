# GitHub Pull Request delivery lane

The GitHub lane is an explicit delivery mode for the foreground Architect:

```bash
hcom arch codex --github-pr
hcom arch codex --single-review --github-pr
hcom arch claude --github-pr
hcom arch codex --github-pr --protected-auto-merge
```

`--github-pr` selects manual delivery by default. The optional
`--protected-auto-merge` flag requires `--github-pr` and is the only way to
select the strict ruleset-attested exact-head merge policy.

Without `--github-pr`, `hcom arch` remains the local-candidate lane. A parsed
`[architect.github]` table is then semantically inert: hcom does not validate
its values, open App keys, invoke Git, inspect the repository, or contact
GitHub. Invalid TOML syntax is still an ordinary configuration error.

## Deployment configuration

GitHub mode requires a closed table in the selected profile file. The values
below are placeholders; private-key files must be separately provisioned,
absolute, canonical, current-user-owned regular files with the required
private mode. Do not store credentials in the source repository.

```toml
[architect.github]
owner = "example-owner"
repository = "private-repository"
local_repository_root = "/absolute/path/to/source"
base_branch = "master"
merge_method = "squash"
merge_wait_seconds = 21600
delete_remote_branch_after_merge = true
private_repository_required = true

[architect.github.apps.architect]
app_id = 1001
slug = "example-hcom-architect"
private_key_file = "/absolute/private/path/architect.pem"

[architect.github.apps.developer]
app_id = 1002
slug = "example-hcom-developer"
private_key_file = "/absolute/private/path/developer.pem"

[architect.github.apps.reviewer1]
app_id = 1003
slug = "example-hcom-reviewer1"
private_key_file = "/absolute/private/path/reviewer1.pem"

# Required only in dual-review mode; forbidden with --single-review.
[architect.github.apps.reviewer2]
app_id = 1004
slug = "example-hcom-reviewer2"
private_key_file = "/absolute/private/path/reviewer2.pem"
```

The four Apps must have distinct App, installation, slug, and bot identities.
In manual mode the Architect App minimum is Checks and Pull requests
read/write; it does not require Administration or Contents write for merge or
remote deletion. The Developer App needs Contents and Pull requests read/write
and is the sole commit/push/PR-comment identity. Each Reviewer App needs Pull
requests read/write and is the sole identity for its matching review lane.
Protected auto-merge additionally requires the Architect Administration read
and Contents write used by ruleset attestation, merge, and final cleanup. Hcom
requests per-operation down-scoped installation tokens and never exports tokens
or keys to Architect or worker processes, Git argv, Git configuration, errors,
status, or durable evidence.

Startup validates the effective provider topology and the Claude proxy gate
first. GitHub mode then opens and parses only the active App keys, validates
the canonical local repository root, and performs bounded read-only checks of
the Apps, installations, private repository, base ref, permissions, and actors.
Manual mode does not call, require, hash, freeze, or revalidate ruleset or
branch-protection APIs; a GitHub Free private-repository ruleset 403 is therefore
irrelevant. Protected auto-merge additionally attests the hcom-critical rules.
Hcom freezes and prints the resulting non-secret delivery binding, explicit
policy, and first inspection before launching the blank interactive Architect.
This preflight creates no ref, branch, worktree, Pull Request, Check, comment,
review, or merge.

## Authorization and one-run topology

`--github-pr` authorizes read-only preflight only. The human still owns the
Architect's first input. GitHub writes begin only after the Architect has
refreshed the inspection, displayed a complete typed plan with its exact
delivery policy, base SHA, policy-applicable rules evidence, generated branch,
external-publication disclosure, and terminal disposition, and received
execution authorization under the normal plan contract.

A manual plan explicitly discloses that hcom verifies its own exact base/head,
actors, append-only task chain, published reviews, and `hcom/review` Check, but
cannot prove server-side PR/direct-push enforcement for a private GitHub Free
repository or prevent an authorized external actor from direct-pushing or
merging early. Approval authorizes the bounded branch/worktree/push/PR/review/
Check workflow, not merge, remote deletion, or merged-run finalization.

One approved run is bound to one private repository, one base branch, one
generated `hcom/run-...` branch, one linked worktree below the run evidence
directory, and one Pull Request. Every task must name the frozen repository
root. Developer work appends one child commit per task or correction; hcom
rejects rewrites, force-pushes, unexpected identities, non-contiguous task
ranges, `hcom-tasks` content in history, external head/base/actor changes, and
hostile Git configuration. The user's primary checkout may be dirty and is
never reset, rebased, checked out, or used as the task worktree.

Successful Developer and Reviewer final messages are opaque publication
payloads. hcom preserves and publishes them byte-for-byte, without redaction,
secret-shaped scanning, truncation, or lossy conversion. Fixed wrappers and
idempotency markers are added by hcom; every generated GitHub body must remain
valid UTF-8 and no larger than 60 KiB. Workers must therefore keep credentials
out of their finals. An oversized final remains local evidence but cannot be
routed as a successful publication.

Reviewer1 and Reviewer2, when active, start concurrently and publish
independent App-authored reviews for the same exact head. A correction waits
for all active responses, appends a new commit, and resumes every Reviewer on
the new generation. Earlier approvals may become dismissed only because a
later hcom-owned commit was appended. The `hcom/review` Check succeeds only
when every task is LGTM and every active current-generation review is a
published same-head approval.

In manual mode the final exact-head Check is the last run mutation. All-task
LGTM completes as `review_complete_unmerged`: the PR remains open and the
generated remote/local branch, linked worktree, and evidence are preserved for
human disposition. This outcome means hcom review is complete; it is not a
claim that GitHub rules or CI consider the PR merge-ready. A later foreground
run never adopts those artifacts.

Only protected auto-merge revalidates rules/base/head identities, requests one
exact-head squash merge, and reconciles ambiguous responses before any retry.
Confirmed merge finalization and its evidence must complete before that policy
reports `delivered`. Hcom never retries a confirmed merge.

## Progress, terminal handoff, and failure behavior

Progress reports the exact PR URL, task/generation, head SHA, publication
identity and response counts. Terminal handoff includes the PR number/URL,
run base and final head, each task's exact range and outcome, ordered Reviewer
review URLs/verdicts, `hcom/review` Check URL/state, delivery policy,
policy-applicable approved/final ruleset attestations, delivery outcome, and
merge SHA only when delivered. It reports the preserved branch/worktree/PR for
`review_complete_unmerged`, review-exhausted, or human-action outcomes.

Review exhaustion advances through remaining tasks, leaves the aggregate
Check `action_required`, and completes as `unmerged_review_exhausted` without
requesting merge. Base/head/actor drift, conflicting remote state,
publication identity failure, lifecycle failure, or cleanup failure enters a
human-visible operational terminal. A confirmed merge followed by
finalization failure is possible only in protected mode and remains
distinguishable from an unmerged failure. Rules drift applies only to protected
auto-merge because manual delivery never reads or binds rules.

The foreground parent owns the workflow and linked worktree. Parent exit
stops workers; there is no daemon or restart recovery. A later foreground
Architect does not adopt a preserved branch, worktree, or PR as a new run.
The GitHub lane does not authorize install, release, deployment, or arbitrary
GitHub mutation beyond the bound workflow.

Retry-safe mutations use a 120-second phase window with a one-second initial
delay and a 30-second capped exponential backoff. A GitHub-provided retry delay
is treated as a lower bound. Every retry still follows a zero-effect
reconciliation readback; exhausting the window reports the final bounded HTTP
status, request ID, and sanitized GitHub reason when those fields were present.
It also reports the attempt count and elapsed retry time. Transport failures
distinguish request-send from response-body reads and retain a bounded category
(`timeout`, TLS/connect/I/O, connection-closed/canceled/protocol, or
body/decode), request/stage elapsed time, the configured timeout, and any
status, request ID, or rate-limit headers that arrived before a body failure.
No credential, request body, raw URL, or remote response body is copied into
this diagnostic.

The production REST client does not reuse idle TCP connections between API
operations. Architect sessions can span long model turns, and a proxy tunnel or
origin-side HTTP/1 connection can become stale without the client observing the
close until a later request starts. Each GitHub request therefore uses a fresh
bounded connection; mutation retries remain exclusively at the workflow layer
and still require endpoint-specific zero-effect reconciliation before replay.

## Local verification

Normal source tests use temporary real Git repositories/bare remotes and fake
HTTP providers. They cover local/GitHub and single/dual topology; clean and
dirty primary checkouts; append-only multi-task/correction history; all
Reviewer verdict matrices; synchronized reviews and Checks; exhaustion;
exact-head merge and finalization; drift, cancellation, cleanup, hostile Git
configuration, timeout-after-success reconciliation, schema projections, and
control/MCP size limits. They make no live GitHub, provider-model, network, or
interactive TUI call. An ignored, stateless transport probe can explicitly send
a fake credential and inert head name through the production reqwest/proxy
builder to `POST /repos/octocat/Hello-World/pulls`; success means GitHub returned
the expected HTTP 401 rather than a transport failure, and can never create a
PR.

Live private-repository canaries, installation, push of this source tree, and
release remain separate operations requiring explicit human authorization.
