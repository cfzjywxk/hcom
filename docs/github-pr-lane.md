# GitHub Pull Request delivery lane

The GitHub lane is an explicit delivery mode for the foreground Architect:

```bash
hcom arch codex --github-pr
hcom arch codex --single-review --github-pr
hcom arch claude --github-pr
```

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
The Architect App needs Administration read plus Contents, Pull requests, and
Checks read/write. The Developer App needs Contents and Pull requests
read/write and is the sole commit/push/PR-comment identity. Each Reviewer App
needs Pull requests read/write and is the sole identity for its matching review
lane. hcom requests down-scoped installation tokens and never exports tokens or
keys to Architect or worker processes, Git argv, Git configuration, errors,
status, or durable evidence.

Startup validates the effective provider topology and the Claude proxy gate
first. GitHub mode then opens and parses only the active App keys, validates
the canonical local repository root, and performs bounded read-only checks of
the Apps, installations, private repository, base ref, permissions, actors,
and hcom-critical rules. It freezes and prints the resulting non-secret
delivery binding and first inspection before launching the blank interactive
Architect. This preflight creates no ref, branch, worktree, Pull Request,
Check, comment, review, or merge.

## Authorization and one-run topology

`--github-pr` authorizes read-only preflight only. The human still owns the
Architect's first input. GitHub writes begin only after the Architect has
refreshed the inspection, displayed a complete typed plan with its exact base
SHA/rules attestation/generated branch, disclosed external publication, and
received execution authorization under the normal plan contract.

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

After the Check and rules/base/head identities are revalidated, hcom requests
one exact-head squash merge and reconciles ambiguous responses before any
retry. Confirmed merge finalization and its evidence must complete before the
run reports `delivered`. hcom never retries a confirmed merge.

## Progress, terminal handoff, and failure behavior

Progress reports the exact PR URL, task/generation, head SHA, publication
identity and response counts. Terminal handoff includes the PR number/URL,
run base and final head, each task's exact range and outcome, ordered Reviewer
review URLs/verdicts, `hcom/review` Check URL/state, approved and final ruleset
attestations, delivery outcome, and merge SHA when delivered. It also reports
the preserved branch/worktree/PR for unmerged or human-action outcomes.

Review exhaustion advances through remaining tasks, leaves the aggregate
Check `action_required`, and completes as `unmerged_review_exhausted` without
requesting merge. Base/rules/head/actor drift, conflicting remote state,
publication identity failure, lifecycle failure, or cleanup failure enters a
human-visible operational terminal. A confirmed merge followed by
finalization failure remains distinguishable from an unmerged failure.

The foreground parent owns the workflow and linked worktree. Parent exit
stops workers; there is no daemon or restart recovery. A later foreground
Architect does not adopt a preserved branch, worktree, or PR as a new run.
The GitHub lane does not authorize install, release, deployment, or arbitrary
GitHub mutation beyond the bound workflow.

## Local verification

Normal source tests use temporary real Git repositories/bare remotes and fake
HTTP providers. They cover local/GitHub and single/dual topology; clean and
dirty primary checkouts; append-only multi-task/correction history; all
Reviewer verdict matrices; synchronized reviews and Checks; exhaustion;
exact-head merge and finalization; drift, cancellation, cleanup, hostile Git
configuration, timeout-after-success reconciliation, schema projections, and
control/MCP size limits. They make no live GitHub, provider-model, network, or
interactive TUI call.

Live private-repository canaries, installation, push of this source tree, and
release remain separate operations requiring explicit human authorization.
