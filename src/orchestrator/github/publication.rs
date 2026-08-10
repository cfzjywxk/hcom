//! Canonical GitHub publication bodies and mutation reconciliation.
//!
//! Native finals are opaque: rendering adds a bounded hcom wrapper but never
//! rewrites, summarizes, scans, or redacts the final itself.

use super::auth::{InstallationOperation, InstallationToken};
use super::client::{GitHubApiError, GitHubAuthentication, GitHubRestClient, RestEndpoint};
use super::{validate_git_sha, validate_id, validate_sha256, validate_slug};
use crate::control_api::{GITHUB_REVIEW_CHECK_NAME, GitHubAppRole, ReviewerVerdict};
use crate::worker::profile::ReviewerId;
use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;

pub(crate) const MAX_GITHUB_BODY_BYTES: usize = 60 * 1024;
const MAX_TITLE_BYTES: usize = 256;
const MAX_URL_BYTES: usize = 2_048;
const MAX_TASKS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationKind {
    PullRequest,
    TaskStarted,
    Correction,
    Review,
    Check,
    Terminal,
    Merge,
}

impl PublicationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::PullRequest => "pull_request",
            Self::TaskStarted => "task_started",
            Self::Correction => "correction",
            Self::Review => "review",
            Self::Check => "check",
            Self::Terminal => "terminal",
            Self::Merge => "merge",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationMarker {
    pub(crate) kind: PublicationKind,
    pub(crate) run_id: String,
    pub(crate) task_key: String,
    pub(crate) generation: u32,
    pub(crate) lane: String,
    pub(crate) head_sha: String,
    pub(crate) artifact_sha256: String,
}

impl PublicationMarker {
    pub(crate) fn task(
        kind: PublicationKind,
        run_id: &str,
        task_key: &str,
        generation: u32,
        lane: &str,
        head_sha: &str,
        artifact_sha256: &str,
    ) -> Result<Self> {
        if task_key == "_run" || generation == 0 {
            bail!("task publication marker requires a task key and positive generation");
        }
        let marker = Self {
            kind,
            run_id: run_id.into(),
            task_key: task_key.into(),
            generation,
            lane: lane.into(),
            head_sha: head_sha.into(),
            artifact_sha256: artifact_sha256.into(),
        };
        marker.validate()?;
        Ok(marker)
    }

    pub(crate) fn run(
        kind: PublicationKind,
        run_id: &str,
        lane: &str,
        head_sha: &str,
        artifact_sha256: &str,
    ) -> Result<Self> {
        let marker = Self {
            kind,
            run_id: run_id.into(),
            task_key: "_run".into(),
            generation: 0,
            lane: lane.into(),
            head_sha: head_sha.into(),
            artifact_sha256: artifact_sha256.into(),
        };
        marker.validate()?;
        Ok(marker)
    }

    fn validate(&self) -> Result<()> {
        validate_id("GitHub publication run ID", &self.run_id)?;
        validate_id("GitHub publication task key", &self.task_key)?;
        validate_git_sha("GitHub publication head", &self.head_sha)?;
        validate_sha256("GitHub publication artifact", &self.artifact_sha256)?;
        if !matches!(
            self.lane.as_str(),
            "architect" | "developer" | "reviewer1" | "reviewer2"
        ) {
            bail!("GitHub publication marker lane is invalid");
        }
        if (self.task_key == "_run") != (self.generation == 0)
            || (self.task_key != "_run" && !(1..=20).contains(&self.generation))
        {
            bail!("GitHub publication marker run/task coordinates are inconsistent");
        }
        Ok(())
    }

    pub(crate) fn canonical_line(&self) -> String {
        format!(
            "<!-- hcom:v1 kind={} run={} task={} generation={} lane={} head={} artifact={} -->",
            self.kind.as_str(),
            self.run_id,
            self.task_key,
            self.generation,
            self.lane,
            self.head_sha,
            self.artifact_sha256
        )
    }

    pub(crate) fn parse_exact_line(line: &str) -> Result<Self> {
        if line.contains(['\r', '\n'])
            || !line.starts_with("<!-- hcom:v1 ")
            || !line.ends_with(" -->")
        {
            bail!("GitHub publication marker is not one canonical line");
        }
        let fields = line[13..line.len() - 4]
            .split(' ')
            .map(|field| {
                field
                    .split_once('=')
                    .ok_or_else(|| anyhow!("GitHub publication marker field is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.len() != 7
            || fields.iter().map(|(name, _)| *name).collect::<Vec<_>>()
                != [
                    "kind",
                    "run",
                    "task",
                    "generation",
                    "lane",
                    "head",
                    "artifact",
                ]
        {
            bail!("GitHub publication marker field set or order is invalid");
        }
        let kind = match fields[0].1 {
            "pull_request" => PublicationKind::PullRequest,
            "task_started" => PublicationKind::TaskStarted,
            "correction" => PublicationKind::Correction,
            "review" => PublicationKind::Review,
            "check" => PublicationKind::Check,
            "terminal" => PublicationKind::Terminal,
            "merge" => PublicationKind::Merge,
            _ => bail!("GitHub publication marker kind is invalid"),
        };
        let marker = Self {
            kind,
            run_id: fields[1].1.into(),
            task_key: fields[2].1.into(),
            generation: fields[3]
                .1
                .parse()
                .map_err(|_| anyhow!("GitHub publication generation is invalid"))?,
            lane: fields[4].1.into(),
            head_sha: fields[5].1.into(),
            artifact_sha256: fields[6].1.into(),
        };
        marker.validate()?;
        if marker.canonical_line() != line {
            bail!("GitHub publication marker is not canonical");
        }
        Ok(marker)
    }

    fn identifies_same_operation(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.run_id == other.run_id
            && self.task_key == other.task_key
            && self.generation == other.generation
            && self.lane == other.lane
            && self.head_sha == other.head_sha
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenderedPublication {
    pub(crate) body: String,
    pub(crate) marker: PublicationMarker,
    pub(crate) body_sha256: String,
}

impl RenderedPublication {
    fn new(mut wrapper: String, final_body: &str, marker: PublicationMarker) -> Result<Self> {
        marker.validate()?;
        if !wrapper.ends_with('\n') {
            wrapper.push('\n');
        }
        wrapper.push('\n');
        wrapper.push_str(final_body);
        if !final_body.ends_with('\n') {
            wrapper.push('\n');
        }
        wrapper.push('\n');
        wrapper.push_str(&marker.canonical_line());
        wrapper.push('\n');
        validate_body(&wrapper)?;
        let body_sha256 = sha256_hex(wrapper.as_bytes());
        Ok(Self {
            body: wrapper,
            marker,
            body_sha256,
        })
    }

    pub(crate) fn has_exact_marker(&self, remote_body: &str) -> bool {
        remote_body
            .lines()
            .filter(|line| *line == self.marker.canonical_line())
            .count()
            == 1
            && sha256_hex(remote_body.as_bytes()) == self.body_sha256
    }

    fn validate_for(&self, kinds: &[PublicationKind], lane: &str) -> Result<()> {
        self.marker.validate()?;
        validate_body(&self.body)?;
        if !kinds.contains(&self.marker.kind)
            || self.marker.lane != lane
            || !self.has_exact_marker(&self.body)
        {
            bail!("GitHub publication kind, lane, marker, or body hash is inconsistent");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationContext {
    pub(crate) run_id: String,
    pub(crate) plan_hash: String,
    pub(crate) owner: String,
    pub(crate) repository: String,
    pub(crate) repository_id: u64,
    pub(crate) branch: String,
    pub(crate) base_branch: String,
    pub(crate) base_sha: String,
}

impl PublicationContext {
    fn validate(&self) -> Result<()> {
        validate_id("GitHub publication run ID", &self.run_id)?;
        validate_sha256("GitHub publication plan hash", &self.plan_hash)?;
        validate_slug("GitHub publication owner", &self.owner)?;
        validate_slug("GitHub publication repository", &self.repository)?;
        if self.repository_id == 0 {
            bail!("GitHub publication repository ID must be positive");
        }
        super::validate_branch(&self.branch)?;
        super::validate_branch(&self.base_branch)?;
        validate_git_sha("GitHub publication base SHA", &self.base_sha)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskPublicationContext {
    pub(crate) ordinal: u32,
    pub(crate) count: u32,
    pub(crate) task_key: String,
    pub(crate) title: String,
    pub(crate) generation: u32,
    pub(crate) task_base_sha: String,
    pub(crate) previous_head_sha: String,
    pub(crate) head_sha: String,
}

impl TaskPublicationContext {
    fn validate(&self) -> Result<()> {
        if self.ordinal == 0
            || self.count == 0
            || self.ordinal > self.count
            || self.count as usize > MAX_TASKS
            || !(1..=20).contains(&self.generation)
            || self.task_key == "_run"
        {
            bail!("GitHub task publication coordinates are invalid");
        }
        validate_id("GitHub task key", &self.task_key)?;
        bounded_line("GitHub task title", &self.title, MAX_TITLE_BYTES)?;
        validate_git_sha("GitHub task base", &self.task_base_sha)?;
        validate_git_sha("GitHub previous head", &self.previous_head_sha)?;
        validate_git_sha("GitHub current head", &self.head_sha)
    }
}

pub(crate) fn pull_request_title(
    context: &PublicationContext,
    first_task: &TaskPublicationContext,
) -> Result<String> {
    context.validate()?;
    first_task.validate()?;
    let short_run = context
        .run_id
        .strip_prefix("run-")
        .unwrap_or(&context.run_id)
        .chars()
        .take(12)
        .collect::<String>();
    let suffix = if first_task.count > 1 {
        format!(" (+{} tasks)", first_task.count - 1)
    } else {
        String::new()
    };
    let title = format!(
        "[hcom {short_run}] {}{suffix}",
        markdown_escape(&first_task.title)
    );
    bounded_line("GitHub Pull Request title", &title, MAX_TITLE_BYTES)?;
    Ok(title)
}

pub(crate) fn render_pull_request_body(
    context: &PublicationContext,
    ordered_tasks: &[(String, String)],
    reviewer_actors: &[(ReviewerId, String)],
    initial: &TaskPublicationContext,
    developer_final: &str,
) -> Result<RenderedPublication> {
    context.validate()?;
    initial.validate()?;
    if ordered_tasks.is_empty()
        || ordered_tasks.len() > MAX_TASKS
        || ordered_tasks.len() != initial.count as usize
    {
        bail!("GitHub Pull Request task list is empty, oversized, or inconsistent");
    }
    validate_reviewers(reviewer_actors)?;
    let artifact = sha256_hex(developer_final.as_bytes());
    let marker = PublicationMarker::task(
        PublicationKind::PullRequest,
        &context.run_id,
        &initial.task_key,
        initial.generation,
        "developer",
        &initial.head_sha,
        &artifact,
    )?;
    let mut wrapper = format!(
        "## hcom run\n\n- Run: `{}`\n- Plan: `{}`\n- Base: `{}` at `{}`\n- Initial head: `{}`\n- Review mode: {}\n- Control: foreground hcom supervisor\n\n### Ordered tasks\n",
        markdown_escape(&context.run_id),
        context.plan_hash,
        markdown_escape(&context.base_branch),
        context.base_sha,
        initial.head_sha,
        if reviewer_actors.len() == 1 {
            "single"
        } else {
            "dual"
        },
    );
    for (index, (key, title)) in ordered_tasks.iter().enumerate() {
        validate_id("GitHub task key", key)?;
        bounded_line("GitHub task title", title, MAX_TITLE_BYTES)?;
        wrapper.push_str(&format!(
            "{}. `{}` — {}\n",
            index + 1,
            markdown_escape(key),
            markdown_escape(title)
        ));
    }
    wrapper.push_str("\n### Reviewer Apps\n");
    for (reviewer, actor) in reviewer_actors {
        validate_actor_login(actor)?;
        wrapper.push_str(&format!(
            "- {}: `{}`\n",
            reviewer.as_str(),
            markdown_escape(actor)
        ));
    }
    wrapper.push_str("\n### Task 1 Developer final\n");
    RenderedPublication::new(wrapper, developer_final, marker)
}

pub(crate) fn render_developer_comment(
    context: &PublicationContext,
    task: &TaskPublicationContext,
    correction: bool,
    developer_final: &str,
) -> Result<RenderedPublication> {
    context.validate()?;
    task.validate()?;
    let artifact = sha256_hex(developer_final.as_bytes());
    let marker = PublicationMarker::task(
        if correction {
            PublicationKind::Correction
        } else {
            PublicationKind::TaskStarted
        },
        &context.run_id,
        &task.task_key,
        task.generation,
        "developer",
        &task.head_sha,
        &artifact,
    )?;
    let wrapper = if correction {
        format!(
            "## Addressed hcom task `{}` review generation {}\n\n- Previous head: `{}`\n- New head: `{}`\n- All active hcom Reviewer lanes have been re-requested internally.\n\n### Developer final\n",
            markdown_escape(&task.task_key),
            task.generation.saturating_sub(1),
            task.previous_head_sha,
            task.head_sha,
        )
    } else {
        format!(
            "## Started hcom task {}/{}: `{}`\n\n- Task base: `{}`\n- New head: `{}`\n\n### Developer final\n",
            task.ordinal,
            task.count,
            markdown_escape(&task.task_key),
            task.task_base_sha,
            task.head_sha,
        )
    };
    RenderedPublication::new(wrapper, developer_final, marker)
}

pub(crate) fn render_reviewer_body(
    context: &PublicationContext,
    task: &TaskPublicationContext,
    reviewer_id: ReviewerId,
    reviewer_final: &str,
) -> Result<RenderedPublication> {
    context.validate()?;
    task.validate()?;
    let lane = reviewer_id.as_str();
    let artifact = sha256_hex(reviewer_final.as_bytes());
    let marker = PublicationMarker::task(
        PublicationKind::Review,
        &context.run_id,
        &task.task_key,
        task.generation,
        lane,
        &task.head_sha,
        &artifact,
    )?;
    let wrapper = format!(
        "## hcom review\n\n- Task: {}/{} `{}`\n- Reviewer lane: `{}`\n- Generation: {}\n- Exact range: `{}`..`{}`\n\n### Native Reviewer final\n",
        task.ordinal,
        task.count,
        markdown_escape(&task.task_key),
        lane,
        task.generation,
        task.task_base_sha,
        task.head_sha,
    );
    RenderedPublication::new(wrapper, reviewer_final, marker)
}

pub(crate) fn render_terminal_comment(
    context: &PublicationContext,
    head_sha: &str,
    outcome: &str,
    architect_message: &str,
) -> Result<RenderedPublication> {
    context.validate()?;
    validate_git_sha("GitHub terminal comment head", head_sha)?;
    if !matches!(
        outcome,
        "cancelled" | "needs_human" | "delivered" | "unmerged_review_exhausted"
    ) {
        bail!("GitHub terminal comment outcome is invalid");
    }
    let artifact = sha256_hex(architect_message.as_bytes());
    let marker = PublicationMarker::run(
        PublicationKind::Terminal,
        &context.run_id,
        "architect",
        head_sha,
        &artifact,
    )?;
    RenderedPublication::new(
        format!(
            "## hcom run terminal update\n\n- Outcome: `{outcome}`\n- Head: `{head_sha}`\n\n### Architect message\n"
        ),
        architect_message,
        marker,
    )
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct CheckOutput {
    title: String,
    summary: String,
    #[serde(skip)]
    conclusion: Option<CheckConclusion>,
}

pub(crate) fn render_check_output(
    context: &PublicationContext,
    task: &TaskPublicationContext,
    task_outcomes: &[(u32, String, String)],
    conclusion: Option<CheckConclusion>,
) -> Result<(CheckOutput, PublicationMarker)> {
    context.validate()?;
    task.validate()?;
    if task_outcomes.is_empty() || task_outcomes.len() > MAX_TASKS {
        bail!("GitHub Check task outcome list is empty or oversized");
    }
    let artifact = sha256_hex(
        task_outcomes
            .iter()
            .flat_map(|(ordinal, key, outcome)| format!("{ordinal}:{key}:{outcome}\n").into_bytes())
            .collect::<Vec<_>>()
            .as_slice(),
    );
    let marker = PublicationMarker::task(
        PublicationKind::Check,
        &context.run_id,
        &task.task_key,
        task.generation,
        "architect",
        &task.head_sha,
        &artifact,
    )?;
    let mut summary = format!(
        "Current task `{}` exact range `{}`..`{}`.\n\n",
        markdown_escape(&task.task_key),
        task.task_base_sha,
        task.head_sha
    );
    for (ordinal, key, outcome) in task_outcomes {
        if *ordinal == 0 || *ordinal > MAX_TASKS as u32 {
            bail!("GitHub Check task ordinal is invalid");
        }
        validate_id("GitHub Check task key", key)?;
        if !matches!(outcome.as_str(), "pending" | "lgtm" | "review_exhausted") {
            bail!("GitHub Check task outcome is invalid");
        }
        summary.push_str(&format!(
            "- Task {} `{}`: `{}`\n",
            ordinal,
            markdown_escape(key),
            outcome
        ));
    }
    if let Some(conclusion) = conclusion {
        summary.push_str(&format!("\nConclusion: `{}`\n", conclusion.as_str()));
    }
    summary.push('\n');
    summary.push_str(&marker.canonical_line());
    validate_body(&summary)?;
    Ok((
        CheckOutput {
            title: format!("hcom task {} generation {}", task.ordinal, task.generation),
            summary,
            conclusion,
        },
        marker,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckConclusion {
    Success,
    ActionRequired,
    Cancelled,
}

impl CheckConclusion {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ActionRequired => "action_required",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct GitHubActor {
    pub(crate) id: u64,
    pub(crate) login: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct GitHubRefObservation {
    #[serde(rename = "ref")]
    pub(crate) ref_name: String,
    pub(crate) sha: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct PullRequestObservation {
    pub(crate) id: u64,
    pub(crate) node_id: String,
    pub(crate) number: u64,
    pub(crate) title: String,
    pub(crate) html_url: String,
    pub(crate) state: String,
    pub(crate) draft: bool,
    pub(crate) body: Option<String>,
    pub(crate) user: GitHubActor,
    pub(crate) head: GitHubRefObservation,
    pub(crate) base: GitHubRefObservation,
    #[serde(default)]
    pub(crate) merged: bool,
    pub(crate) merge_commit_sha: Option<String>,
    pub(crate) merged_by: Option<GitHubActor>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct CommentObservation {
    pub(crate) id: u64,
    pub(crate) html_url: String,
    pub(crate) body: String,
    pub(crate) user: GitHubActor,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct ReviewObservation {
    pub(crate) id: u64,
    pub(crate) html_url: String,
    pub(crate) body: String,
    pub(crate) user: GitHubActor,
    pub(crate) state: String,
    pub(crate) commit_id: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct GitHubAppObservation {
    pub(crate) id: u64,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct CheckRunObservation {
    pub(crate) id: u64,
    pub(crate) html_url: String,
    pub(crate) name: String,
    pub(crate) head_sha: String,
    pub(crate) status: String,
    pub(crate) conclusion: Option<String>,
    pub(crate) external_id: Option<String>,
    pub(crate) app: GitHubAppObservation,
    pub(crate) output: Option<CheckOutputObservation>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct CheckOutputObservation {
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct MergeResponse {
    pub(crate) sha: String,
    pub(crate) merged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reconciliation<T> {
    Exactly(T),
    RetrySafe,
    Conflict(&'static str),
}

pub(crate) fn reconcile_pull_request(
    observations: Vec<PullRequestObservation>,
    expected_actor_id: u64,
    context: &PublicationContext,
    publication: &RenderedPublication,
    expected_title: &str,
) -> Reconciliation<PullRequestObservation> {
    let branch_conflict = observations.iter().any(|item| {
        item.head.ref_name == context.branch
            && item.base.ref_name == context.base_branch
            && !item.body.as_deref().is_some_and(|body| {
                body.lines().any(|line| {
                    PublicationMarker::parse_exact_line(line)
                        .is_ok_and(|marker| marker.identifies_same_operation(&publication.marker))
                })
            })
    });
    if branch_conflict {
        return Reconciliation::Conflict(
            "Pull Request already exists for the run branch without the exact operation marker",
        );
    }
    reconcile_marked(
        observations,
        |item| item.body.as_deref(),
        |item| item.user.id,
        expected_actor_id,
        publication,
        |item| {
            validate_pull_request_observation(item).is_ok()
                && item.state == "open"
                && !item.draft
                && item.title == expected_title
                && item.head.ref_name == context.branch
                && item.head.sha == publication.marker.head_sha
                && item.base.ref_name == context.base_branch
                && item.base.sha == context.base_sha
        },
        "Pull Request marker or binding conflicts with the expected operation",
    )
}

pub(crate) fn reconcile_comment(
    observations: Vec<CommentObservation>,
    expected_actor_id: u64,
    publication: &RenderedPublication,
) -> Reconciliation<CommentObservation> {
    reconcile_marked(
        observations,
        |item| Some(item.body.as_str()),
        |item| item.user.id,
        expected_actor_id,
        publication,
        |item| validate_comment_observation(item).is_ok(),
        "PR comment marker, actor, or body conflicts with the expected operation",
    )
}

pub(crate) fn reconcile_review(
    observations: Vec<ReviewObservation>,
    expected_actor_id: u64,
    expected_event: &str,
    publication: &RenderedPublication,
) -> Reconciliation<ReviewObservation> {
    reconcile_marked(
        observations,
        |item| Some(item.body.as_str()),
        |item| item.user.id,
        expected_actor_id,
        publication,
        |item| {
            validate_review_observation(item).is_ok()
                && item.commit_id == publication.marker.head_sha
                && review_state_matches_event(&item.state, expected_event)
        },
        "Reviewer review marker, actor, event, head, or body conflicts with the expected operation",
    )
}

fn review_state_matches_event(state: &str, event: &str) -> bool {
    matches!(
        (state, event),
        ("APPROVED", "APPROVE") | ("CHANGES_REQUESTED", "REQUEST_CHANGES")
    )
}

fn reconcile_marked<T, B, A, M>(
    observations: Vec<T>,
    body: B,
    actor: A,
    expected_actor_id: u64,
    publication: &RenderedPublication,
    exact_metadata: M,
    conflict: &'static str,
) -> Reconciliation<T>
where
    B: Fn(&T) -> Option<&str>,
    A: Fn(&T) -> u64,
    M: Fn(&T) -> bool,
{
    let mut exact = Vec::new();
    let mut conflicting_marker = false;
    for item in observations {
        let Some(remote_body) = body(&item) else {
            continue;
        };
        let identifies_operation = remote_body.lines().any(|line| {
            PublicationMarker::parse_exact_line(line)
                .is_ok_and(|marker| marker.identifies_same_operation(&publication.marker))
        });
        if !identifies_operation {
            continue;
        }
        if publication.has_exact_marker(remote_body)
            && actor(&item) == expected_actor_id
            && exact_metadata(&item)
        {
            exact.push(item);
        } else {
            conflicting_marker = true;
        }
    }
    match (exact.len(), conflicting_marker) {
        (1, false) => Reconciliation::Exactly(exact.remove(0)),
        (0, false) => Reconciliation::RetrySafe,
        _ => Reconciliation::Conflict(conflict),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn reconcile_check(
    observations: Vec<CheckRunObservation>,
    expected_app_id: u64,
    expected_check_run_id: Option<u64>,
    expected_external_id: &str,
    expected_status: &str,
    expected_conclusion: Option<CheckConclusion>,
    marker: &PublicationMarker,
    expected_output: &CheckOutput,
) -> Reconciliation<CheckRunObservation> {
    let mut exact = Vec::new();
    let mut conflict = false;
    for item in observations {
        if item.external_id.as_deref() != Some(expected_external_id) {
            continue;
        }
        if check_observation_matches(
            &item,
            expected_app_id,
            expected_check_run_id,
            expected_external_id,
            expected_status,
            expected_conclusion,
            marker,
            expected_output,
        ) {
            exact.push(item);
        } else {
            conflict = true;
        }
    }
    match (exact.len(), conflict) {
        (1, false) => Reconciliation::Exactly(exact.remove(0)),
        (0, false) => Reconciliation::RetrySafe,
        _ => Reconciliation::Conflict("Check external ID has conflicting actor/head/state/body"),
    }
}

#[allow(clippy::too_many_arguments)]
fn reconcile_check_update(
    observation: CheckRunObservation,
    expected_app_id: u64,
    expected_check_run_id: u64,
    expected_external_id: &str,
    previous_marker: &PublicationMarker,
    previous_output: &CheckOutput,
    desired_marker: &PublicationMarker,
    desired_output: &CheckOutput,
) -> Reconciliation<CheckRunObservation> {
    if check_observation_matches(
        &observation,
        expected_app_id,
        Some(expected_check_run_id),
        expected_external_id,
        "completed",
        desired_output.conclusion,
        desired_marker,
        desired_output,
    ) {
        return Reconciliation::Exactly(observation);
    }
    if check_observation_matches(
        &observation,
        expected_app_id,
        Some(expected_check_run_id),
        expected_external_id,
        "in_progress",
        None,
        previous_marker,
        previous_output,
    ) {
        return Reconciliation::RetrySafe;
    }
    Reconciliation::Conflict("Check update predecessor or completed state was externally mutated")
}

#[allow(clippy::too_many_arguments)]
fn check_observation_matches(
    item: &CheckRunObservation,
    expected_app_id: u64,
    expected_check_run_id: Option<u64>,
    expected_external_id: &str,
    expected_status: &str,
    expected_conclusion: Option<CheckConclusion>,
    marker: &PublicationMarker,
    expected_output: &CheckOutput,
) -> bool {
    let marker_line = marker.canonical_line();
    item.external_id.as_deref() == Some(expected_external_id)
        && item.app.id == expected_app_id
        && expected_check_run_id.is_none_or(|expected| item.id == expected)
        && validate_check_observation(item).is_ok()
        && item.name == GITHUB_REVIEW_CHECK_NAME
        && item.head_sha == marker.head_sha
        && item.status == expected_status
        && item.conclusion.as_deref() == expected_conclusion.map(CheckConclusion::as_str)
        && item.output.as_ref().is_some_and(|output| {
            output.title.as_deref() == Some(expected_output.title.as_str())
                && output.summary.as_deref() == Some(expected_output.summary.as_str())
                && output
                    .summary
                    .as_deref()
                    .is_some_and(|summary| summary.lines().any(|line| line == marker_line))
        })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn reconcile_merge(
    pull_request: PullRequestObservation,
    expected_developer_id: u64,
    expected_architect_id: u64,
    expected_head: &str,
    expected_title: &str,
    context: &PublicationContext,
    pull_request_publication: &RenderedPublication,
    expected_merge_sha: Option<&str>,
) -> Reconciliation<PullRequestObservation> {
    if context.validate().is_err()
        || pull_request_publication
            .validate_for(&[PublicationKind::PullRequest], "developer")
            .is_err()
        || validate_pull_request_observation(&pull_request).is_err()
        || pull_request.user.id != expected_developer_id
        || pull_request.draft
        || pull_request.title != expected_title
        || !pull_request
            .body
            .as_deref()
            .is_some_and(|body| pull_request_publication.has_exact_marker(body))
        || pull_request.head.ref_name != context.branch
        || pull_request.head.sha != expected_head
        || pull_request.base.ref_name != context.base_branch
        || pull_request.base.sha != context.base_sha
    {
        return Reconciliation::Conflict(
            "Pull Request actor/title/body/draft/head/base changed while merge was reconciled",
        );
    }
    if !pull_request.merged && pull_request.state == "open" && pull_request.merged_by.is_none() {
        return Reconciliation::RetrySafe;
    }
    if pull_request.merged
        && pull_request.state == "closed"
        && pull_request
            .merged_by
            .as_ref()
            .is_some_and(|actor| actor.id == expected_architect_id)
        && pull_request
            .merge_commit_sha
            .as_deref()
            .is_some_and(|sha| validate_git_sha("GitHub merge SHA", sha).is_ok())
        && expected_merge_sha
            .is_none_or(|expected| pull_request.merge_commit_sha.as_deref() == Some(expected))
    {
        Reconciliation::Exactly(pull_request)
    } else {
        Reconciliation::Conflict("Pull Request merge state or actor is externally mutated")
    }
}

#[derive(Debug, Error)]
pub(crate) enum PublicationError {
    #[error(transparent)]
    Api(#[from] GitHubApiError),
    #[error("GitHub mutation remains ambiguous after bounded reconciliation")]
    Ambiguous,
    #[error(
        "GitHub operation is confirmed to have no effect and may be retried (retry_after={retry_after_seconds:?}, rate_limit_reset={rate_limit_reset_unix:?}): {reason}"
    )]
    RetrySafe {
        reason: &'static str,
        retry_after_seconds: Option<u64>,
        rate_limit_reset_unix: Option<u64>,
    },
    #[error("GitHub remote publication conflicts with the frozen operation: {0}")]
    Conflict(&'static str),
    #[error("GitHub publication response violates the frozen operation: {0}")]
    Invalid(&'static str),
}

impl PublicationError {
    fn retry_safe(reason: &'static str, failure: Option<&GitHubApiError>) -> Self {
        let (retry_after_seconds, rate_limit_reset_unix) = failure
            .map(GitHubApiError::rate_limit_signals)
            .unwrap_or((None, None));
        Self::RetrySafe {
            reason,
            retry_after_seconds,
            rate_limit_reset_unix,
        }
    }
}

pub(crate) struct GitHubPublisher<'a> {
    client: &'a GitHubRestClient,
    context: &'a PublicationContext,
}

impl fmt::Debug for GitHubPublisher<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHubPublisher")
            .field(
                "repository",
                &format_args!("{}/{}", self.context.owner, self.context.repository),
            )
            .field("run_id", &self.context.run_id)
            .finish()
    }
}

impl<'a> GitHubPublisher<'a> {
    pub(crate) fn new(
        client: &'a GitHubRestClient,
        context: &'a PublicationContext,
    ) -> Result<Self> {
        context.validate()?;
        Ok(Self { client, context })
    }

    pub(crate) fn create_pull_request(
        &self,
        title: &str,
        publication: &RenderedPublication,
        expected_developer_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<PullRequestObservation, PublicationError> {
        bounded_line("GitHub Pull Request title", title, MAX_TITLE_BYTES)
            .map_err(|_| PublicationError::Invalid("Pull Request title is invalid"))?;
        if expected_developer_id == 0
            || publication
                .validate_for(&[PublicationKind::PullRequest], "developer")
                .is_err()
            || validate_operation_token(
                token,
                InstallationOperation::PullRequestCreate,
                GitHubAppRole::Developer,
                self.context.repository_id,
            )
            .is_err()
        {
            return Err(PublicationError::Invalid(
                "Pull Request publication binding is invalid",
            ));
        }
        let request = PullRequestCreateRequest {
            title,
            head: &self.context.branch,
            base: &self.context.base_branch,
            body: &publication.body,
            draft: false,
        };
        let endpoint = RestEndpoint::CreatePullRequest {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
        };
        match self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &request,
        ) {
            Ok(observation) => match reconcile_pull_request(
                vec![observation],
                expected_developer_id,
                self.context,
                publication,
                title,
            ) {
                Reconciliation::Exactly(value) => Ok(value),
                _ => Err(PublicationError::Invalid(
                    "created Pull Request readback differs from the request",
                )),
            },
            Err(error) if error.requires_mutation_reconciliation() => {
                self.reconcile_pr(title, expected_developer_id, publication, token, error)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn create_comment(
        &self,
        pr_number: u64,
        publication: &RenderedPublication,
        expected_actor_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<CommentObservation, PublicationError> {
        let (kinds, lane, operation, role): (&[_], _, _, _) = match publication.marker.kind {
            PublicationKind::TaskStarted | PublicationKind::Correction => (
                &[PublicationKind::TaskStarted, PublicationKind::Correction],
                "developer",
                InstallationOperation::DeveloperComment,
                GitHubAppRole::Developer,
            ),
            PublicationKind::Terminal => (
                &[PublicationKind::Terminal],
                "architect",
                InstallationOperation::TerminalComment,
                GitHubAppRole::Architect,
            ),
            _ => {
                return Err(PublicationError::Invalid(
                    "PR comment publication kind is invalid",
                ));
            }
        };
        if pr_number == 0
            || expected_actor_id == 0
            || publication.validate_for(kinds, lane).is_err()
            || validate_operation_token(token, operation, role, self.context.repository_id).is_err()
        {
            return Err(PublicationError::Invalid(
                "PR comment publication binding is invalid",
            ));
        }
        let endpoint = RestEndpoint::CreateIssueComment {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
            number: pr_number,
        };
        match self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &BodyRequest {
                body: &publication.body,
            },
        ) {
            Ok(observation) => {
                match reconcile_comment(vec![observation], expected_actor_id, publication) {
                    Reconciliation::Exactly(value) => Ok(value),
                    _ => Err(PublicationError::Invalid(
                        "created PR comment differs from the request",
                    )),
                }
            }
            Err(error) if error.requires_mutation_reconciliation() => {
                let values = self.client.paginated_values(
                    |page| RestEndpoint::ListIssueComments {
                        owner: self.context.owner.clone(),
                        repository: self.context.repository.clone(),
                        number: pr_number,
                        page,
                    },
                    GitHubAuthentication::Installation(token),
                    None,
                )?;
                let observations = decode_values(values, "Developer comment reconciliation")?;
                finish_reconciliation(
                    reconcile_comment(observations, expected_actor_id, publication),
                    Some(error),
                )
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn create_review(
        &self,
        pr_number: u64,
        verdict: ReviewerVerdict,
        publication: &RenderedPublication,
        expected_reviewer_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<ReviewObservation, PublicationError> {
        let reviewer_role = match publication.marker.lane.as_str() {
            "reviewer1" => GitHubAppRole::Reviewer1,
            "reviewer2" => GitHubAppRole::Reviewer2,
            _ => {
                return Err(PublicationError::Invalid(
                    "Reviewer publication lane is invalid",
                ));
            }
        };
        if pr_number == 0
            || expected_reviewer_id == 0
            || publication
                .validate_for(&[PublicationKind::Review], reviewer_role.as_str())
                .is_err()
            || validate_operation_token(
                token,
                InstallationOperation::ReviewPublish,
                reviewer_role,
                self.context.repository_id,
            )
            .is_err()
        {
            return Err(PublicationError::Invalid(
                "Reviewer publication binding is invalid",
            ));
        }
        let event = match verdict {
            ReviewerVerdict::Lgtm => "APPROVE",
            ReviewerVerdict::RequestChanges => "REQUEST_CHANGES",
        };
        let request = ReviewCreateRequest {
            body: &publication.body,
            event,
            commit_id: &publication.marker.head_sha,
        };
        let endpoint = RestEndpoint::CreateReview {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
            number: pr_number,
        };
        match self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &request,
        ) {
            Ok(observation) => {
                match reconcile_review(vec![observation], expected_reviewer_id, event, publication)
                {
                    Reconciliation::Exactly(value) => Ok(value),
                    _ => Err(PublicationError::Invalid(
                        "created Reviewer review differs from the request",
                    )),
                }
            }
            Err(error) if error.requires_mutation_reconciliation() => {
                let values = self.client.paginated_values(
                    |page| RestEndpoint::ListReviews {
                        owner: self.context.owner.clone(),
                        repository: self.context.repository.clone(),
                        number: pr_number,
                        page,
                    },
                    GitHubAuthentication::Installation(token),
                    None,
                )?;
                let observations = decode_values(values, "Reviewer review reconciliation")?;
                finish_reconciliation(
                    reconcile_review(observations, expected_reviewer_id, event, publication),
                    Some(error),
                )
            }
            Err(error) => Err(error.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_check(
        &self,
        external_id: &str,
        head_sha: &str,
        output: &CheckOutput,
        marker: &PublicationMarker,
        expected_architect_app_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<CheckRunObservation, PublicationError> {
        if expected_architect_app_id == 0
            || head_sha != marker.head_sha
            || validate_id("GitHub Check external ID", external_id).is_err()
            || validate_check_output_binding(output, marker, None).is_err()
            || validate_operation_token(
                token,
                InstallationOperation::CheckPublish,
                GitHubAppRole::Architect,
                self.context.repository_id,
            )
            .is_err()
        {
            return Err(PublicationError::Invalid(
                "Check creation binding is invalid",
            ));
        }
        let request = CheckCreateRequest {
            name: GITHUB_REVIEW_CHECK_NAME,
            head_sha,
            status: "in_progress",
            external_id,
            output,
        };
        let endpoint = RestEndpoint::CreateCheckRun {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
        };
        match self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &request,
        ) {
            Ok(observation) => match reconcile_check(
                vec![observation],
                expected_architect_app_id,
                None,
                external_id,
                "in_progress",
                None,
                marker,
                output,
            ) {
                Reconciliation::Exactly(value) => Ok(value),
                _ => Err(PublicationError::Invalid(
                    "created Check Run differs from the request",
                )),
            },
            Err(error) if error.requires_mutation_reconciliation() => self.reconcile_check_list(
                expected_architect_app_id,
                external_id,
                "in_progress",
                None,
                marker,
                output,
                token,
                error,
            ),
            Err(error) => Err(error.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn conclude_check(
        &self,
        check_run_id: u64,
        external_id: &str,
        conclusion: CheckConclusion,
        previous_output: &CheckOutput,
        previous_marker: &PublicationMarker,
        output: &CheckOutput,
        marker: &PublicationMarker,
        expected_architect_app_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<CheckRunObservation, PublicationError> {
        if check_run_id == 0
            || expected_architect_app_id == 0
            || validate_id("GitHub Check external ID", external_id).is_err()
            || validate_check_output_binding(previous_output, previous_marker, None).is_err()
            || validate_check_output_binding(output, marker, Some(conclusion)).is_err()
            || !previous_marker.identifies_same_operation(marker)
            || validate_operation_token(
                token,
                InstallationOperation::CheckPublish,
                GitHubAppRole::Architect,
                self.context.repository_id,
            )
            .is_err()
        {
            return Err(PublicationError::Invalid(
                "Check conclusion binding is invalid",
            ));
        }
        let read_endpoint = || RestEndpoint::CheckRun {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
            check_run_id,
            update: false,
        };
        let current = self
            .client
            .get(read_endpoint(), GitHubAuthentication::Installation(token))?;
        match reconcile_check_update(
            current,
            expected_architect_app_id,
            check_run_id,
            external_id,
            previous_marker,
            previous_output,
            marker,
            output,
        ) {
            Reconciliation::Exactly(completed) => return Ok(completed),
            Reconciliation::RetrySafe => {}
            Reconciliation::Conflict(reason) => {
                return Err(PublicationError::Conflict(reason));
            }
        }
        let request = CheckUpdateRequest {
            name: GITHUB_REVIEW_CHECK_NAME,
            status: "completed",
            conclusion: conclusion.as_str(),
            output,
        };
        let endpoint = RestEndpoint::CheckRun {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
            check_run_id,
            update: true,
        };
        match self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &request,
        ) {
            Ok(observation) => finish_reconciliation(
                reconcile_check_update(
                    observation,
                    expected_architect_app_id,
                    check_run_id,
                    external_id,
                    previous_marker,
                    previous_output,
                    marker,
                    output,
                ),
                None,
            ),
            Err(error) if error.requires_mutation_reconciliation() => {
                let observation = self
                    .client
                    .get(read_endpoint(), GitHubAuthentication::Installation(token))?;
                finish_reconciliation(
                    reconcile_check_update(
                        observation,
                        expected_architect_app_id,
                        check_run_id,
                        external_id,
                        previous_marker,
                        previous_output,
                        marker,
                        output,
                    ),
                    Some(error),
                )
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn read_pull_request(
        &self,
        pr_number: u64,
        token: &InstallationToken,
    ) -> std::result::Result<PullRequestObservation, PublicationError> {
        if pr_number == 0 {
            return Err(PublicationError::Invalid("Pull Request number is invalid"));
        }
        let observation = self.client.get(
            RestEndpoint::PullRequest {
                owner: self.context.owner.clone(),
                repository: self.context.repository.clone(),
                number: pr_number,
            },
            GitHubAuthentication::Installation(token),
        )?;
        validate_pull_request_observation(&observation)
            .map_err(|_| PublicationError::Invalid("Pull Request readback is invalid"))?;
        if observation.number != pr_number {
            return Err(PublicationError::Invalid(
                "Pull Request readback number differs from the request",
            ));
        }
        Ok(observation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn merge_exact_head(
        &self,
        pr_number: u64,
        final_head_sha: &str,
        expected_pull_request_title: &str,
        pull_request_publication: &RenderedPublication,
        expected_developer_id: u64,
        expected_architect_id: u64,
        token: &InstallationToken,
    ) -> std::result::Result<PullRequestObservation, PublicationError> {
        if pr_number == 0
            || expected_developer_id == 0
            || expected_architect_id == 0
            || validate_git_sha("GitHub exact merge head", final_head_sha).is_err()
            || bounded_line(
                "GitHub Pull Request title",
                expected_pull_request_title,
                MAX_TITLE_BYTES,
            )
            .is_err()
            || pull_request_publication
                .validate_for(&[PublicationKind::PullRequest], "developer")
                .is_err()
            || validate_operation_token(
                token,
                InstallationOperation::Merge,
                GitHubAppRole::Architect,
                self.context.repository_id,
            )
            .is_err()
        {
            return Err(PublicationError::Invalid(
                "exact-head merge binding is invalid",
            ));
        }
        match self.read_merge_reconciliation(
            pr_number,
            final_head_sha,
            expected_pull_request_title,
            pull_request_publication,
            expected_developer_id,
            expected_architect_id,
            None,
            token,
        )? {
            Reconciliation::Exactly(merged) => return Ok(merged),
            Reconciliation::RetrySafe => {}
            Reconciliation::Conflict(reason) => return Err(PublicationError::Conflict(reason)),
        }
        let request = MergeRequest {
            sha: final_head_sha,
            merge_method: "squash",
        };
        let endpoint = RestEndpoint::MergePullRequest {
            owner: self.context.owner.clone(),
            repository: self.context.repository.clone(),
            number: pr_number,
        };
        let result: std::result::Result<MergeResponse, GitHubApiError> = self.client.send_json(
            endpoint,
            GitHubAuthentication::Installation(token),
            &request,
        );
        match result {
            Ok(response) if response.merged => {
                validate_git_sha("GitHub merge response SHA", &response.sha)
                    .map_err(|_| PublicationError::Invalid("merge response SHA is invalid"))?;
                finish_reconciliation(
                    self.read_merge_reconciliation(
                        pr_number,
                        final_head_sha,
                        expected_pull_request_title,
                        pull_request_publication,
                        expected_developer_id,
                        expected_architect_id,
                        Some(&response.sha),
                        token,
                    )?,
                    None,
                )
            }
            Ok(_) => match self.read_merge_reconciliation(
                pr_number,
                final_head_sha,
                expected_pull_request_title,
                pull_request_publication,
                expected_developer_id,
                expected_architect_id,
                None,
                token,
            )? {
                Reconciliation::Exactly(merged) => Ok(merged),
                Reconciliation::RetrySafe => Err(PublicationError::retry_safe(
                    "repository merge gates are not ready",
                    None,
                )),
                Reconciliation::Conflict(reason) => Err(PublicationError::Conflict(reason)),
            },
            Err(error) if error.requires_mutation_reconciliation() => {
                let status = error.http_status();
                let reconciliation = self.read_merge_reconciliation(
                    pr_number,
                    final_head_sha,
                    expected_pull_request_title,
                    pull_request_publication,
                    expected_developer_id,
                    expected_architect_id,
                    None,
                    token,
                )?;
                if error.is_bound_exceeded() {
                    return Err(PublicationError::Api(error));
                }
                match reconciliation {
                    Reconciliation::Exactly(merged) => Ok(merged),
                    Reconciliation::RetrySafe if status == Some(405) => {
                        Err(PublicationError::retry_safe(
                            "repository merge gates are not ready",
                            Some(&error),
                        ))
                    }
                    Reconciliation::RetrySafe if matches!(status, Some(409 | 422)) => {
                        Err(PublicationError::Conflict(
                            "exact-head merge was rejected despite unchanged readback",
                        ))
                    }
                    Reconciliation::RetrySafe => Err(PublicationError::retry_safe(
                        "exact-head merge has no matching remote effect",
                        Some(&error),
                    )),
                    Reconciliation::Conflict(reason) => Err(PublicationError::Conflict(reason)),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn reconcile_pr(
        &self,
        title: &str,
        expected_developer_id: u64,
        publication: &RenderedPublication,
        token: &InstallationToken,
        failure: GitHubApiError,
    ) -> std::result::Result<PullRequestObservation, PublicationError> {
        let values = self.client.paginated_values(
            |page| RestEndpoint::ListPullRequests {
                owner: self.context.owner.clone(),
                repository: self.context.repository.clone(),
                head: format!("{}:{}", self.context.owner, self.context.branch),
                base: self.context.base_branch.clone(),
                page,
            },
            GitHubAuthentication::Installation(token),
            None,
        )?;
        let observations = decode_values(values, "Pull Request reconciliation")?;
        finish_reconciliation(
            reconcile_pull_request(
                observations,
                expected_developer_id,
                self.context,
                publication,
                title,
            ),
            Some(failure),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_check_list(
        &self,
        expected_architect_app_id: u64,
        external_id: &str,
        status: &str,
        conclusion: Option<CheckConclusion>,
        marker: &PublicationMarker,
        output: &CheckOutput,
        token: &InstallationToken,
        failure: GitHubApiError,
    ) -> std::result::Result<CheckRunObservation, PublicationError> {
        let values = self.client.paginated_values(
            |page| RestEndpoint::ListCheckRuns {
                owner: self.context.owner.clone(),
                repository: self.context.repository.clone(),
                head_sha: marker.head_sha.clone(),
                page,
            },
            GitHubAuthentication::Installation(token),
            Some("check_runs"),
        )?;
        let observations = decode_values(values, "Check Run reconciliation")?;
        finish_reconciliation(
            reconcile_check(
                observations,
                expected_architect_app_id,
                None,
                external_id,
                status,
                conclusion,
                marker,
                output,
            ),
            Some(failure),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn read_merge_reconciliation(
        &self,
        pr_number: u64,
        final_head_sha: &str,
        expected_pull_request_title: &str,
        pull_request_publication: &RenderedPublication,
        expected_developer_id: u64,
        expected_architect_id: u64,
        expected_merge_sha: Option<&str>,
        token: &InstallationToken,
    ) -> std::result::Result<Reconciliation<PullRequestObservation>, PublicationError> {
        let observation = self.read_pull_request(pr_number, token)?;
        Ok(reconcile_merge(
            observation,
            expected_developer_id,
            expected_architect_id,
            final_head_sha,
            expected_pull_request_title,
            self.context,
            pull_request_publication,
            expected_merge_sha,
        ))
    }
}

#[derive(Serialize)]
struct PullRequestCreateRequest<'a> {
    title: &'a str,
    head: &'a str,
    base: &'a str,
    body: &'a str,
    draft: bool,
}

#[derive(Serialize)]
struct BodyRequest<'a> {
    body: &'a str,
}

#[derive(Serialize)]
struct ReviewCreateRequest<'a> {
    body: &'a str,
    event: &'a str,
    commit_id: &'a str,
}

#[derive(Serialize)]
struct CheckCreateRequest<'a> {
    name: &'a str,
    head_sha: &'a str,
    status: &'a str,
    external_id: &'a str,
    output: &'a CheckOutput,
}

#[derive(Serialize)]
struct CheckUpdateRequest<'a> {
    name: &'a str,
    status: &'a str,
    conclusion: &'a str,
    output: &'a CheckOutput,
}

#[derive(Serialize)]
struct MergeRequest<'a> {
    sha: &'a str,
    merge_method: &'static str,
}

fn finish_reconciliation<T>(
    reconciliation: Reconciliation<T>,
    failure: Option<GitHubApiError>,
) -> std::result::Result<T, PublicationError> {
    if failure
        .as_ref()
        .is_some_and(GitHubApiError::is_bound_exceeded)
    {
        return Err(PublicationError::Api(
            failure.expect("checked mutation failure exists"),
        ));
    }
    match reconciliation {
        Reconciliation::Exactly(value) => Ok(value),
        Reconciliation::RetrySafe => Err(PublicationError::retry_safe(
            "remote readback confirms the mutation has no matching effect",
            failure.as_ref(),
        )),
        Reconciliation::Conflict(reason) => Err(PublicationError::Conflict(reason)),
    }
}

fn decode_values<T: for<'de> Deserialize<'de>>(
    values: Vec<serde_json::Value>,
    _label: &'static str,
) -> std::result::Result<Vec<T>, PublicationError> {
    values
        .into_iter()
        .map(|value| {
            serde_json::from_value(value).map_err(|_| {
                PublicationError::Invalid("reconciliation list has an invalid typed item")
            })
        })
        .collect()
}

fn validate_reviewers(reviewers: &[(ReviewerId, String)]) -> Result<()> {
    if !matches!(
        reviewers,
        [(ReviewerId::Reviewer1, _)] | [(ReviewerId::Reviewer1, _), (ReviewerId::Reviewer2, _)]
    ) {
        bail!("GitHub Reviewer actor list does not match canonical topology");
    }
    for (_, actor) in reviewers {
        validate_actor_login(actor)?;
    }
    Ok(())
}

fn validate_actor_login(value: &str) -> Result<()> {
    let slug = value
        .strip_suffix("[bot]")
        .ok_or_else(|| anyhow!("GitHub App actor must end in [bot]"))?;
    validate_slug("GitHub App actor slug", slug)
}

fn validate_operation_token(
    token: &InstallationToken,
    operation: InstallationOperation,
    role: GitHubAppRole,
    repository_id: u64,
) -> Result<()> {
    if token.operation() != operation
        || token.role() != role
        || token.repository_id() != repository_id
    {
        bail!("GitHub publication token does not match the exact role and operation");
    }
    Ok(())
}

fn validate_check_output_binding(
    output: &CheckOutput,
    marker: &PublicationMarker,
    conclusion: Option<CheckConclusion>,
) -> Result<()> {
    marker.validate()?;
    bounded_line("GitHub Check title", &output.title, MAX_TITLE_BYTES)?;
    validate_body(&output.summary)?;
    if marker.kind != PublicationKind::Check
        || marker.lane != "architect"
        || output.conclusion != conclusion
        || output
            .summary
            .lines()
            .filter(|line| *line == marker.canonical_line())
            .count()
            != 1
    {
        bail!("GitHub Check output does not match its marker and conclusion");
    }
    Ok(())
}

fn bounded_line(label: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max || value.contains(['\r', '\n']) {
        bail!("{label} is not one bounded line");
    }
    Ok(())
}

fn validate_body(body: &str) -> Result<()> {
    if body.is_empty() || body.len() > MAX_GITHUB_BODY_BYTES {
        bail!("GitHub publication body must contain 1..={MAX_GITHUB_BODY_BYTES} UTF-8 bytes");
    }
    Ok(())
}

fn validate_url(url: &str) -> Result<()> {
    if url.is_empty() || url.len() > MAX_URL_BYTES || url.contains(['\r', '\n']) {
        bail!("GitHub publication URL is not a bounded github.com URL");
    }
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| anyhow!("GitHub publication URL is not a bounded github.com URL"))?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("github.com")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
    {
        bail!("GitHub publication URL is not a bounded github.com URL");
    }
    Ok(())
}

pub(crate) fn validate_pull_request_observation(
    observation: &PullRequestObservation,
) -> Result<()> {
    if observation.id == 0 || observation.number == 0 || observation.user.id == 0 {
        bail!("GitHub Pull Request observation has a zero identifier");
    }
    validate_id("GitHub Pull Request node ID", &observation.node_id)?;
    bounded_line(
        "GitHub Pull Request title",
        &observation.title,
        MAX_TITLE_BYTES,
    )?;
    validate_url(&observation.html_url)?;
    if !matches!(observation.state.as_str(), "open" | "closed") {
        bail!("GitHub Pull Request state is invalid");
    }
    validate_git_sha("GitHub Pull Request head", &observation.head.sha)?;
    validate_git_sha("GitHub Pull Request base", &observation.base.sha)
}

fn validate_comment_observation(observation: &CommentObservation) -> Result<()> {
    if observation.id == 0 || observation.user.id == 0 {
        bail!("GitHub comment observation has a zero identifier");
    }
    validate_url(&observation.html_url)?;
    validate_body(&observation.body)
}

fn validate_review_observation(observation: &ReviewObservation) -> Result<()> {
    if observation.id == 0 || observation.user.id == 0 {
        bail!("GitHub review observation has a zero identifier");
    }
    validate_url(&observation.html_url)?;
    validate_body(&observation.body)?;
    validate_git_sha("GitHub review commit", &observation.commit_id)
}

fn validate_check_observation(observation: &CheckRunObservation) -> Result<()> {
    if observation.id == 0 || observation.app.id == 0 {
        bail!("GitHub Check observation has a zero identifier");
    }
    validate_url(&observation.html_url)?;
    validate_git_sha("GitHub Check observation head", &observation.head_sha)?;
    if observation.name != GITHUB_REVIEW_CHECK_NAME
        || !matches!(
            observation.status.as_str(),
            "queued" | "in_progress" | "completed"
        )
        || observation
            .external_id
            .as_deref()
            .is_none_or(|value| validate_id("GitHub Check external ID", value).is_err())
    {
        bail!("GitHub Check observation has an invalid name/state/external ID");
    }
    if let Some(output) = &observation.output
        && (output.title.as_deref().is_some_and(|value| {
            bounded_line("GitHub Check title", value, MAX_TITLE_BYTES).is_err()
        }) || output
            .summary
            .as_deref()
            .is_some_and(|value| validate_body(value).is_err()))
    {
        bail!("GitHub Check output is invalid or oversized");
    }
    Ok(())
}

fn markdown_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '\\' | '`' | '*' | '_' | '[' | ']' | '<' | '>') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::GitHubAppRole;
    use crate::orchestrator::github::auth::{InstallationOperation, InstallationToken};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    type FakeRequests = thread::JoinHandle<Vec<(String, Vec<u8>)>>;

    fn context() -> PublicationContext {
        PublicationContext {
            run_id: "run-fixture-1".into(),
            plan_hash: "b".repeat(64),
            owner: "owner".into(),
            repository: "repo".into(),
            repository_id: 99,
            branch: "hcom/run-fixture-plan".into(),
            base_branch: "master".into(),
            base_sha: "a".repeat(40),
        }
    }

    fn task() -> TaskPublicationContext {
        TaskPublicationContext {
            ordinal: 1,
            count: 2,
            task_key: "TASK-1".into(),
            title: "Implement [bounded] publication".into(),
            generation: 1,
            task_base_sha: "a".repeat(40),
            previous_head_sha: "a".repeat(40),
            head_sha: "c".repeat(40),
        }
    }

    fn pr_observation(body: &str, actor: u64) -> PullRequestObservation {
        PullRequestObservation {
            id: 10,
            node_id: "PR_fixture".into(),
            number: 7,
            title: pull_request_title(&context(), &task()).unwrap(),
            html_url: "https://github.com/owner/repo/pull/7".into(),
            state: "open".into(),
            draft: false,
            body: Some(body.into()),
            user: GitHubActor {
                id: actor,
                login: "dev[bot]".into(),
            },
            head: GitHubRefObservation {
                ref_name: context().branch,
                sha: "c".repeat(40),
            },
            base: GitHubRefObservation {
                ref_name: "master".into(),
                sha: "a".repeat(40),
            },
            merged: false,
            merge_commit_sha: None,
            merged_by: None,
        }
    }

    fn operation_token(operation: InstallationOperation) -> InstallationToken {
        let role = match operation {
            InstallationOperation::ReviewPublish | InstallationOperation::ReviewRead => {
                GitHubAppRole::Reviewer1
            }
            InstallationOperation::CheckPublish
            | InstallationOperation::CheckRead
            | InstallationOperation::Merge
            | InstallationOperation::TerminalComment => GitHubAppRole::Architect,
            _ => GitHubAppRole::Developer,
        };
        InstallationToken::from_github_response(
            "opaque-fake-publication-token".into(),
            "2099-01-01T00:00:00Z",
            99,
            role,
            operation,
            4_070_905_200,
        )
        .unwrap()
    }

    fn fake_client(
        responses: Vec<Option<(u16, serde_json::Value)>>,
    ) -> (GitHubRestClient, FakeRequests) {
        fake_raw_client(
            responses
                .into_iter()
                .map(|response| {
                    response.map(|(status, response)| json_http_response(status, response, ""))
                })
                .collect(),
        )
    }

    fn json_http_response(
        status: u16,
        response: serde_json::Value,
        extra_headers: &str,
    ) -> Vec<u8> {
        let body = serde_json::to_vec(&response).unwrap();
        let reason = match status {
            200 => "OK",
            201 => "Created",
            _ => "Fixture",
        };
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body)
        .collect()
    }

    fn fake_raw_client(responses: Vec<Option<Vec<u8>>>) -> (GitHubRestClient, FakeRequests) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut first_line = String::new();
                reader.read_line(&mut first_line).unwrap();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ")
                    {
                        content_length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; content_length];
                reader.read_exact(&mut body).unwrap();
                requests.push((first_line, body));
                if let Some(response) = response {
                    reader.get_mut().write_all(&response).unwrap();
                }
                // `None` deliberately closes after reading the mutation. The
                // caller must reconcile the timeout-after-success shape.
            }
            requests
        });
        let client = GitHubRestClient::new_for_test(
            reqwest::Url::parse(&format!("http://{address}/")).unwrap(),
        )
        .unwrap();
        (client, server)
    }

    #[test]
    fn canonical_marker_round_trips_only_as_one_exact_line() {
        let marker = PublicationMarker::task(
            PublicationKind::Review,
            "run-fixture-1",
            "TASK-1",
            2,
            "reviewer1",
            &"a".repeat(40),
            &"b".repeat(64),
        )
        .unwrap();
        let line = marker.canonical_line();
        assert_eq!(PublicationMarker::parse_exact_line(&line).unwrap(), marker);
        assert!(PublicationMarker::parse_exact_line(&format!("x{line}")).is_err());
        assert!(PublicationMarker::parse_exact_line(&line.replace(" kind=", "  kind=")).is_err());
    }

    #[test]
    fn bodies_preserve_native_final_exactly_and_enforce_sixty_kib() {
        let final_body = "STATUS: READY\nopaque [native] final\n";
        let rendered = render_developer_comment(&context(), &task(), true, final_body).unwrap();
        assert!(rendered.body.contains(final_body));
        assert_eq!(
            rendered.marker.artifact_sha256,
            sha256_hex(final_body.as_bytes())
        );
        assert!(rendered.has_exact_marker(&rendered.body));
        let mut edited = rendered.body.clone();
        edited.push('x');
        assert!(!rendered.has_exact_marker(&edited));

        let oversized = "x".repeat(MAX_GITHUB_BODY_BYTES);
        assert!(render_developer_comment(&context(), &task(), false, &oversized).is_err());
        let largest = (0..MAX_GITHUB_BODY_BYTES)
            .rev()
            .find_map(|size| {
                render_developer_comment(&context(), &task(), false, &"x".repeat(size)).ok()
            })
            .unwrap();
        assert!(largest.body.len() <= MAX_GITHUB_BODY_BYTES);
    }

    #[test]
    fn pull_request_wrapper_has_ordered_tasks_reviewers_and_no_local_path() {
        let rendered = render_pull_request_body(
            &context(),
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[
                (ReviewerId::Reviewer1, "reviewer-one[bot]".into()),
                (ReviewerId::Reviewer2, "reviewer-two[bot]".into()),
            ],
            &task(),
            "STATUS: READY",
        )
        .unwrap();
        assert!(rendered.body.contains("1. `TASK-1`"));
        assert!(rendered.body.contains("2. `TASK-2`"));
        assert!(!rendered.body.contains("/home/"));
        assert!(
            pull_request_title(&context(), &task())
                .unwrap()
                .contains("\\[bounded\\]")
        );
    }

    #[test]
    fn timeout_reconciliation_accepts_one_exact_match_and_rejects_conflicts() {
        let rendered = render_pull_request_body(
            &context(),
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[(ReviewerId::Reviewer1, "reviewer-one[bot]".into())],
            &task(),
            "STATUS: READY",
        )
        .unwrap();
        let title = pull_request_title(&context(), &task()).unwrap();
        assert!(matches!(
            reconcile_pull_request(Vec::new(), 20, &context(), &rendered, &title),
            Reconciliation::RetrySafe
        ));
        assert!(matches!(
            reconcile_pull_request(
                vec![pr_observation(&rendered.body, 20)],
                20,
                &context(),
                &rendered,
                &title,
            ),
            Reconciliation::Exactly(_)
        ));
        assert!(matches!(
            reconcile_pull_request(
                vec![pr_observation(&rendered.body, 21)],
                20,
                &context(),
                &rendered,
                &title,
            ),
            Reconciliation::Conflict(_)
        ));
        assert!(matches!(
            reconcile_pull_request(
                vec![pr_observation("body marker was removed", 20)],
                20,
                &context(),
                &rendered,
                &title,
            ),
            Reconciliation::Conflict(_)
        ));
        let conflicting_marker = rendered
            .body
            .replace(&rendered.marker.artifact_sha256, &"d".repeat(64));
        assert!(matches!(
            reconcile_pull_request(
                vec![pr_observation(&conflicting_marker, 20)],
                20,
                &context(),
                &rendered,
                &title,
            ),
            Reconciliation::Conflict(_)
        ));
        assert!(matches!(
            reconcile_pull_request(
                vec![
                    pr_observation(&rendered.body, 20),
                    pr_observation(&rendered.body, 20)
                ],
                20,
                &context(),
                &rendered,
                &title,
            ),
            Reconciliation::Conflict(_)
        ));
    }

    #[test]
    fn comment_review_check_and_merge_reconciliation_bind_actor_head_and_state() {
        let reviewer =
            render_reviewer_body(&context(), &task(), ReviewerId::Reviewer1, "VERDICT: LGTM")
                .unwrap();
        let review = ReviewObservation {
            id: 1,
            html_url: "https://github.com/owner/repo/pull/7#pullrequestreview-1".into(),
            body: reviewer.body.clone(),
            user: GitHubActor {
                id: 30,
                login: "reviewer[bot]".into(),
            },
            state: "APPROVED".into(),
            commit_id: task().head_sha,
        };
        assert!(matches!(
            reconcile_review(vec![review.clone()], 30, "APPROVE", &reviewer),
            Reconciliation::Exactly(_)
        ));
        let mut noncanonical_review = review;
        noncanonical_review.state = "APPROVE".into();
        assert!(matches!(
            reconcile_review(vec![noncanonical_review], 30, "APPROVE", &reviewer),
            Reconciliation::Conflict(_)
        ));

        let (output, marker) = render_check_output(
            &context(),
            &task(),
            &[(1, "TASK-1".into(), "lgtm".into())],
            Some(CheckConclusion::Success),
        )
        .unwrap();
        let check = CheckRunObservation {
            id: 2,
            html_url: "https://github.com/owner/repo/runs/2".into(),
            name: GITHUB_REVIEW_CHECK_NAME.into(),
            head_sha: task().head_sha,
            status: "completed".into(),
            conclusion: Some("success".into()),
            external_id: Some("check-fixture".into()),
            app: GitHubAppObservation { id: 40 },
            output: Some(CheckOutputObservation {
                title: Some(output.title.clone()),
                summary: Some(output.summary.clone()),
            }),
        };
        let mut edited_check = check.clone();
        edited_check
            .output
            .as_mut()
            .unwrap()
            .summary
            .as_mut()
            .unwrap()
            .push_str("\nedited");
        assert!(matches!(
            reconcile_check(
                vec![edited_check],
                40,
                Some(2),
                "check-fixture",
                "completed",
                Some(CheckConclusion::Success),
                &marker,
                &output,
            ),
            Reconciliation::Conflict(_)
        ));
        assert!(matches!(
            reconcile_check(
                vec![check],
                40,
                Some(2),
                "check-fixture",
                "completed",
                Some(CheckConclusion::Success),
                &marker,
                &output,
            ),
            Reconciliation::Exactly(_)
        ));

        let pr_publication = render_pull_request_body(
            &context(),
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[(ReviewerId::Reviewer1, "reviewer-one[bot]".into())],
            &task(),
            "STATUS: READY",
        )
        .unwrap();
        let mut mergeable = pr_observation(&pr_publication.body, 20);
        mergeable.merge_commit_sha = Some("b".repeat(40));
        assert!(matches!(
            reconcile_merge(
                mergeable,
                20,
                40,
                &"c".repeat(40),
                &pull_request_title(&context(), &task()).unwrap(),
                &context(),
                &pr_publication,
                None,
            ),
            Reconciliation::RetrySafe
        ));
        let mut merged = pr_observation(&pr_publication.body, 20);
        merged.merged = true;
        merged.state = "closed".into();
        merged.merged_by = Some(GitHubActor {
            id: 40,
            login: "arch[bot]".into(),
        });
        merged.merge_commit_sha = Some("d".repeat(40));
        let mut base_drifted = merged.clone();
        base_drifted.base.sha = "e".repeat(40);
        assert!(matches!(
            reconcile_merge(
                base_drifted,
                20,
                40,
                &"c".repeat(40),
                &pull_request_title(&context(), &task()).unwrap(),
                &context(),
                &pr_publication,
                Some(&"d".repeat(40)),
            ),
            Reconciliation::Conflict(_)
        ));
        assert!(matches!(
            reconcile_merge(
                merged,
                20,
                40,
                &"c".repeat(40),
                &pull_request_title(&context(), &task()).unwrap(),
                &context(),
                &pr_publication,
                Some(&"d".repeat(40)),
            ),
            Reconciliation::Exactly(_)
        ));
    }

    #[test]
    fn fake_http_timeout_after_success_reconciles_every_publication_mutation() {
        let context = context();
        let task = task();

        let pr_publication = render_pull_request_body(
            &context,
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[(ReviewerId::Reviewer1, "reviewer-one[bot]".into())],
            &task,
            "STATUS: READY",
        )
        .unwrap();
        let pr_json = serde_json::json!({
            "id": 10, "node_id": "PR_fixture", "number": 7,
            "title": pull_request_title(&context, &task).unwrap(), "draft": false,
            "html_url": "https://github.com/owner/repo/pull/7", "state": "open",
            "body": pr_publication.body, "user": {"id": 20, "login": "dev[bot]"},
            "head": {"ref": context.branch, "sha": task.head_sha},
            "base": {"ref": context.base_branch, "sha": context.base_sha},
            "merged": false, "merge_commit_sha": null, "merged_by": null
        });
        let (client, server) = fake_client(vec![None, Some((200, serde_json::json!([pr_json])))]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let created = publisher
            .create_pull_request(
                &pull_request_title(&context, &task).unwrap(),
                &pr_publication,
                20,
                &operation_token(InstallationOperation::PullRequestCreate),
            )
            .unwrap();
        assert_eq!(created.number, 7);
        let requests = server.join().unwrap();
        assert!(requests[0].0.starts_with("POST /repos/owner/repo/pulls "));
        assert!(requests[1].0.starts_with("GET /repos/owner/repo/pulls?"));

        let comment_publication =
            render_developer_comment(&context, &task, true, "STATUS: READY").unwrap();
        let comment_json = serde_json::json!({
            "id": 11,
            "html_url": "https://github.com/owner/repo/pull/7#issuecomment-11",
            "body": comment_publication.body,
            "user": {"id": 20, "login": "dev[bot]"}
        });
        let (client, server) =
            fake_client(vec![None, Some((200, serde_json::json!([comment_json])))]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .create_comment(
                    7,
                    &comment_publication,
                    20,
                    &operation_token(InstallationOperation::DeveloperComment),
                )
                .unwrap()
                .id,
            11
        );
        assert_eq!(server.join().unwrap().len(), 2);

        let review_publication =
            render_reviewer_body(&context, &task, ReviewerId::Reviewer1, "VERDICT: LGTM").unwrap();
        let review_json = serde_json::json!({
            "id": 12,
            "html_url": "https://github.com/owner/repo/pull/7#pullrequestreview-12",
            "body": review_publication.body,
            "user": {"id": 30, "login": "reviewer[bot]"},
            "state": "APPROVED", "commit_id": task.head_sha
        });
        let (client, server) =
            fake_client(vec![None, Some((200, serde_json::json!([review_json])))]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .create_review(
                    7,
                    ReviewerVerdict::Lgtm,
                    &review_publication,
                    30,
                    &operation_token(InstallationOperation::ReviewPublish),
                )
                .unwrap()
                .id,
            12
        );
        assert_eq!(server.join().unwrap().len(), 2);

        let (check_output, check_marker) = render_check_output(
            &context,
            &task,
            &[(1, "TASK-1".into(), "pending".into())],
            None,
        )
        .unwrap();
        let check_json = serde_json::json!({
            "id": 13, "html_url": "https://github.com/owner/repo/runs/13",
            "name": "hcom/review", "head_sha": task.head_sha,
            "status": "in_progress", "conclusion": null,
            "external_id": "check-fixture", "app": {"id": 40},
            "output": {"title": check_output.title, "summary": check_output.summary}
        });
        let (client, server) = fake_client(vec![
            None,
            Some((
                200,
                serde_json::json!({"total_count": 1, "check_runs": [check_json]}),
            )),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .create_check(
                    "check-fixture",
                    &task.head_sha,
                    &check_output,
                    &check_marker,
                    40,
                    &operation_token(InstallationOperation::CheckPublish),
                )
                .unwrap()
                .id,
            13
        );
        assert_eq!(server.join().unwrap().len(), 2);

        let (completed_output, completed_marker) = render_check_output(
            &context,
            &task,
            &[(1, "TASK-1".into(), "lgtm".into())],
            Some(CheckConclusion::Success),
        )
        .unwrap();
        let completed_check_json = serde_json::json!({
            "id": 13, "html_url": "https://github.com/owner/repo/runs/13",
            "name": "hcom/review", "head_sha": task.head_sha,
            "status": "completed", "conclusion": "success",
            "external_id": "check-fixture", "app": {"id": 40},
            "output": {"title": completed_output.title, "summary": completed_output.summary}
        });
        let (client, server) = fake_client(vec![
            Some((200, check_json.clone())),
            None,
            Some((200, completed_check_json)),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .conclude_check(
                    13,
                    "check-fixture",
                    CheckConclusion::Success,
                    &check_output,
                    &check_marker,
                    &completed_output,
                    &completed_marker,
                    40,
                    &operation_token(InstallationOperation::CheckPublish),
                )
                .unwrap()
                .conclusion,
            Some("success".into())
        );
        assert_eq!(server.join().unwrap().len(), 3);

        let (client, server) = fake_client(vec![
            Some((200, check_json.clone())),
            None,
            Some((200, check_json)),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .conclude_check(
                13,
                "check-fixture",
                CheckConclusion::Success,
                &check_output,
                &check_marker,
                &completed_output,
                &completed_marker,
                40,
                &operation_token(InstallationOperation::CheckPublish),
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::RetrySafe { .. }));
        assert_eq!(server.join().unwrap().len(), 3);

        let unmerged_json = serde_json::json!({
            "id": 10, "node_id": "PR_fixture", "number": 7,
            "title": pull_request_title(&context, &task).unwrap(), "draft": false,
            "html_url": "https://github.com/owner/repo/pull/7", "state": "open",
            "body": pr_publication.body, "user": {"id": 20, "login": "dev[bot]"},
            "head": {"ref": context.branch, "sha": task.head_sha},
            "base": {"ref": context.base_branch, "sha": context.base_sha},
            "merged": false, "merge_commit_sha": "b".repeat(40), "merged_by": null
        });
        let merged_json = serde_json::json!({
            "id": 10, "node_id": "PR_fixture", "number": 7,
            "title": pull_request_title(&context, &task).unwrap(), "draft": false,
            "html_url": "https://github.com/owner/repo/pull/7", "state": "closed",
            "body": pr_publication.body, "user": {"id": 20, "login": "dev[bot]"},
            "head": {"ref": context.branch, "sha": task.head_sha},
            "base": {"ref": context.base_branch, "sha": context.base_sha},
            "merged": true, "merge_commit_sha": "d".repeat(40),
            "merged_by": {"id": 40, "login": "arch[bot]"}
        });
        let (client, server) = fake_client(vec![
            Some((200, unmerged_json)),
            None,
            Some((200, merged_json)),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .merge_exact_head(
                    7,
                    &task.head_sha,
                    &pull_request_title(&context, &task).unwrap(),
                    &pr_publication,
                    20,
                    40,
                    &operation_token(InstallationOperation::Merge),
                )
                .unwrap()
                .merge_commit_sha,
            Some("d".repeat(40))
        );
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].0.starts_with("GET /repos/owner/repo/pulls/7 "));
        assert!(
            requests[1]
                .0
                .starts_with("PUT /repos/owner/repo/pulls/7/merge ")
        );
        assert!(requests[2].0.starts_with("GET /repos/owner/repo/pulls/7 "));
    }

    #[test]
    fn invalid_successful_mutation_response_reconciles_exact_or_retry_safe() {
        let context = context();
        let task = task();
        let publication = render_developer_comment(&context, &task, true, "STATUS: READY").unwrap();
        let exact = serde_json::json!({
            "id": 11,
            "html_url": "https://github.com/owner/repo/pull/7#issuecomment-11",
            "body": publication.body,
            "user": {"id": 20, "login": "dev[bot]"}
        });

        let (client, server) = fake_client(vec![
            Some((201, serde_json::json!({}))),
            Some((200, serde_json::json!([exact]))),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        assert_eq!(
            publisher
                .create_comment(
                    7,
                    &publication,
                    20,
                    &operation_token(InstallationOperation::DeveloperComment),
                )
                .unwrap()
                .id,
            11
        );
        assert_eq!(server.join().unwrap().len(), 2);

        let (client, server) = fake_client(vec![
            Some((201, serde_json::json!({}))),
            Some((200, serde_json::json!([]))),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .create_comment(
                7,
                &publication,
                20,
                &operation_token(InstallationOperation::DeveloperComment),
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::RetrySafe { .. }));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn mutation_reconciliation_preserves_hard_bounds_and_rate_limit_timing() {
        let context = context();
        let task = task();
        let publication = render_developer_comment(&context, &task, true, "STATUS: READY").unwrap();
        let oversized = format!(
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            super::super::client::MAX_RESPONSE_BODY_BYTES + 1
        )
        .into_bytes();
        let (client, server) = fake_raw_client(vec![
            Some(oversized),
            Some(json_http_response(200, serde_json::json!([]), "")),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .create_comment(
                7,
                &publication,
                20,
                &operation_token(InstallationOperation::DeveloperComment),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            PublicationError::Api(GitHubApiError::BoundExceeded { .. })
        ));
        assert_eq!(server.join().unwrap().len(), 2);

        let rate_limited = json_http_response(
            429,
            serde_json::json!({"message":"secondary rate limit"}),
            "Retry-After: 7\r\nX-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: 4070908800\r\n",
        );
        let (client, server) = fake_raw_client(vec![
            Some(rate_limited),
            Some(json_http_response(200, serde_json::json!([]), "")),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .create_comment(
                7,
                &publication,
                20,
                &operation_token(InstallationOperation::DeveloperComment),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            PublicationError::RetrySafe {
                retry_after_seconds: Some(7),
                rate_limit_reset_unix: Some(4_070_908_800),
                ..
            }
        ));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn merge_proves_pr_binding_before_mutation_and_treats_405_as_no_effect() {
        let context = context();
        let task = task();
        let publication = render_pull_request_body(
            &context,
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[(ReviewerId::Reviewer1, "reviewer-one[bot]".into())],
            &task,
            "STATUS: READY",
        )
        .unwrap();
        let observation = |actor_id| {
            serde_json::json!({
                "id": 10, "node_id": "PR_fixture", "number": 7,
                "title": pull_request_title(&context, &task).unwrap(), "draft": false,
                "html_url": "https://github.com/owner/repo/pull/7", "state": "open",
                "body": publication.body, "user": {"id": actor_id, "login": "dev[bot]"},
                "head": {"ref": context.branch, "sha": task.head_sha},
                "base": {"ref": context.base_branch, "sha": context.base_sha},
                "merged": false, "merge_commit_sha": null, "merged_by": null
            })
        };

        let (client, server) = fake_client(vec![Some((200, observation(21)))]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .merge_exact_head(
                7,
                &task.head_sha,
                &pull_request_title(&context, &task).unwrap(),
                &publication,
                20,
                40,
                &operation_token(InstallationOperation::Merge),
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::Conflict(_)));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].0.starts_with("GET /repos/owner/repo/pulls/7 "));

        let (client, server) = fake_client(vec![
            Some((200, observation(20))),
            Some((
                405,
                serde_json::json!({"message":"repository merge gates are not ready"}),
            )),
            Some((200, observation(20))),
        ]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let error = publisher
            .merge_exact_head(
                7,
                &task.head_sha,
                &pull_request_title(&context, &task).unwrap(),
                &publication,
                20,
                40,
                &operation_token(InstallationOperation::Merge),
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::RetrySafe { .. }));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
            requests[1]
                .0
                .starts_with("PUT /repos/owner/repo/pulls/7/merge ")
        );
    }

    #[test]
    fn publication_rejects_wrong_repository_and_operation_tokens_before_network() {
        let context = context();
        let task = task();
        let publication = render_pull_request_body(
            &context,
            &[
                ("TASK-1".into(), "First".into()),
                ("TASK-2".into(), "Second".into()),
            ],
            &[(ReviewerId::Reviewer1, "reviewer-one[bot]".into())],
            &task,
            "STATUS: READY",
        )
        .unwrap();
        let client =
            GitHubRestClient::new_for_test(reqwest::Url::parse("http://127.0.0.1:9/").unwrap())
                .unwrap();
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let wrong_repository = InstallationToken::from_github_response(
            "wrong-repository-fixture".into(),
            "2099-01-01T00:00:00Z",
            100,
            GitHubAppRole::Developer,
            InstallationOperation::PullRequestCreate,
            4_070_905_200,
        )
        .unwrap();
        let error = publisher
            .create_pull_request(
                &pull_request_title(&context, &task).unwrap(),
                &publication,
                20,
                &wrong_repository,
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::Invalid(_)));

        let wrong_operation = operation_token(InstallationOperation::DeveloperComment);
        let error = publisher
            .create_pull_request(
                &pull_request_title(&context, &task).unwrap(),
                &publication,
                20,
                &wrong_operation,
            )
            .unwrap_err();
        assert!(matches!(error, PublicationError::Invalid(_)));
    }

    #[test]
    fn terminal_comment_is_bound_to_the_architect_operation() {
        let context = context();
        let publication = render_terminal_comment(
            &context,
            &task().head_sha,
            "cancelled",
            "foreground run cancelled",
        )
        .unwrap();
        let comment = serde_json::json!({
            "id": 90,
            "html_url": "https://github.com/owner/repo/pull/7#issuecomment-90",
            "body": publication.body,
            "user": {"id": 40, "login": "arch[bot]"}
        });
        let (client, server) = fake_client(vec![Some((201, comment))]);
        let publisher = GitHubPublisher::new(&client, &context).unwrap();
        let observation = publisher
            .create_comment(
                7,
                &publication,
                40,
                &operation_token(InstallationOperation::TerminalComment),
            )
            .unwrap();
        assert_eq!(observation.id, 90);
        assert_eq!(server.join().unwrap().len(), 1);
    }
}
