//! Concrete Git/GitHub workflow composition for the opt-in PR delivery lane.
//!
//! This module is the only production composition of the reviewed managed-Git,
//! App-authentication, bounded REST, canonical-publication, reconciliation, and
//! durable-evidence components. Workers receive only the managed repository and
//! non-secret prompt bindings; every credential remains owned here.

use super::auth::{
    InstallationOperation, InstallationToken, InstallationTokenRequest, RsaAppSigner,
    mint_bootstrap_repository_token, mint_installation_token,
};
use super::client::{GitHubApiError, GitHubAuthentication, GitHubRestClient, RestEndpoint};
use super::evidence::{
    EvidenceActor, EvidenceKind, EvidencePath, GitHubEvidenceRecord, GitHubEvidenceWriter,
};
use super::git::{
    FinalizationAuthorization, GitFinalizationProgress, GitPreparationProgress,
    GitWorkspaceManager, PreparedGitWorkspace, RemoteRefFinalizationOutcome,
};
use super::publication::{
    CheckConclusion, CheckOutput, CheckRunObservation, CommentObservation, GitHubPublisher,
    PublicationContext, PublicationError, PublicationKind, PublicationMarker,
    PullRequestObservation, RenderedPublication, ReviewObservation, TaskPublicationContext,
    pull_request_title, reconcile_check, render_check_output, render_developer_comment,
    render_pull_request_body, render_reviewer_body, render_terminal_comment,
};
use super::{
    CandidatePublishedObservation, GitHubAppObservation, GitHubDeploymentConfig,
    GitHubInspectionProvider, GitHubInspectionRequest, GitHubInspectionResult,
    GitHubPreflightObservation, GitHubPreflightProvider, GitHubPreflightRequest,
    GitHubRunFinalizedObservation, GitHubWorkflowProvider, MergeGateObservation,
    PullRequestMergedObservation, RepositoryPreparedObservation, ReviewCheckPublishedObservation,
    ReviewerReviewPublishedObservation, freeze_preflight, validate_git_sha,
    validate_inspection_result,
};
use crate::control_api::{
    GitHubAppBinding, GitHubAppRole, GitHubFinalizationSnapshot, GitHubPermissionLevel,
    GitHubPullRequestBinding, ReviewerVerdict, TaskCompletionOutcome,
};
use crate::orchestrator::core::{
    FinalizeGitHubRunRequest, GitHubPartialCandidatePublication, GitHubPartialFinalization,
    GitHubPartialMerge, GitHubPartialOperation, GitHubPartialRepositoryPreparation,
    GitHubPartialReviewCheck, GitHubPartialReviewerReview, GitHubReviewCheckConclusion,
    GitHubTaskOutcomeEvidence, MergePullRequestRequest, PrepareGitHubRunRequest,
    PublishDeveloperCandidateRequest, PublishGitHubTerminalRequest, PublishReviewCheckRequest,
    PublishReviewerReviewRequest, WaitForMergeGateRequest,
};
use crate::orchestrator::workspace::TasksWorkspace;
use crate::worker::profile::ReviewerId;
use anyhow::{Context, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const MERGE_POLL_INITIAL: Duration = Duration::from_secs(1);
const MERGE_POLL_MAX: Duration = Duration::from_secs(30);
const CANCEL_POLL: Duration = Duration::from_millis(100);
const MUTATION_RETRY_INITIAL: Duration = Duration::from_secs(1);
const MUTATION_RETRY_MAX: Duration = Duration::from_secs(30);
const MUTATION_RETRY_WINDOW: Duration = Duration::from_secs(120);

fn ruleset_attestation_for_policy(
    policy: crate::control_api::GitHubDeliveryPolicy,
    attest: impl FnOnce() -> Result<String>,
) -> Result<Option<String>> {
    if policy.is_protected_auto_merge() {
        attest().map(Some)
    } else {
        Ok(None)
    }
}

fn validate_manual_terminal_audit_for_policy(
    policy: crate::control_api::GitHubDeliveryPolicy,
    task_ordinal: usize,
    task_count: usize,
    current_outcome: Option<TaskCompletionOutcome>,
    validate: impl FnOnce() -> Result<()>,
) -> Result<()> {
    if policy == crate::control_api::GitHubDeliveryPolicy::Manual
        && task_ordinal.checked_add(1) == Some(task_count)
        && current_outcome.is_some()
    {
        validate()
    } else {
        Ok(())
    }
}

fn remote_ref_finalization_outcome_name(outcome: RemoteRefFinalizationOutcome) -> &'static str {
    match outcome {
        RemoteRefFinalizationOutcome::Deleted => "deleted",
        RemoteRefFinalizationOutcome::AlreadyAbsent => "already_absent",
        RemoteRefFinalizationOutcome::PreservedByPolicy => "preserved_by_policy",
        RemoteRefFinalizationOutcome::PreservedAfterDeleteFailure => {
            "preserved_after_delete_failure"
        }
        RemoteRefFinalizationOutcome::PreservedAfterMutation => "preserved_after_mutation",
    }
}

fn partial_finalization_snapshot(progress: &GitFinalizationProgress) -> GitHubFinalizationSnapshot {
    GitHubFinalizationSnapshot {
        local_worktree_removed: progress.local_worktree_removed,
        local_ref_removed: progress.local_refs_removed,
        remote_ref_outcome: progress
            .remote_ref_outcome
            .map(remote_ref_finalization_outcome_name)
            .map(str::to_owned),
    }
}

fn partial_repository_preparation(
    request: &PrepareGitHubRunRequest,
    worktree_path: &str,
    progress: &GitPreparationProgress,
) -> GitHubPartialOperation {
    GitHubPartialOperation::RepositoryPreparation(GitHubPartialRepositoryPreparation {
        base_sha: request.run_binding.expected_base_sha.clone(),
        branch: request.run_binding.generated_run_branch.clone(),
        worktree_path: worktree_path.into(),
        local_base_ref_created: progress.local_base_ref_created,
        local_branch_created: progress.local_branch_created,
        local_worktree_created: progress.local_worktree_created,
    })
}

#[derive(Debug, thiserror::Error)]
#[error("GitHub merge operation was cancelled by the foreground supervisor")]
pub(crate) struct GitHubWorkflowCancelled;

#[derive(Debug, Deserialize)]
struct AccountObservation {
    id: u64,
    login: String,
}

#[derive(Debug, Deserialize)]
struct AppIdentityObservation {
    id: u64,
    slug: String,
    owner: AccountObservation,
}

#[derive(Debug, Deserialize)]
struct InstallationObservation {
    id: u64,
    app_id: u64,
    account: AccountObservation,
    repository_selection: String,
    permissions: BTreeMap<String, GitHubPermissionLevel>,
}

#[derive(Debug, Deserialize)]
struct RepositoryObservation {
    id: u64,
    name: String,
    private: bool,
    owner: AccountObservation,
}

#[derive(Debug, Deserialize)]
struct BotObservation {
    id: u64,
    login: String,
}

#[derive(Debug, Deserialize)]
struct RefObjectObservation {
    sha: String,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Deserialize)]
struct RefObservation {
    #[serde(rename = "ref")]
    ref_name: String,
    object: RefObjectObservation,
}

#[derive(Debug, Clone)]
struct StoredCheck {
    task: TaskPublicationContext,
    outcomes: Vec<(u32, String, String)>,
    external_id: String,
    output: CheckOutput,
    marker: PublicationMarker,
    observation: CheckRunObservation,
    completed: bool,
    conclusion: Option<CheckConclusion>,
}

#[derive(Debug, Clone)]
struct StoredComment {
    publication: RenderedPublication,
    observation: CommentObservation,
}

#[derive(Debug, Clone)]
struct StoredReview {
    task_ordinal: usize,
    reviewer_id: ReviewerId,
    generation: u32,
    head_sha: String,
    verdict: ReviewerVerdict,
    publication: RenderedPublication,
    observation: ReviewObservation,
}

#[derive(Debug, Clone)]
struct PublicationState {
    context: PublicationContext,
    pr_title: String,
    pr_publication: RenderedPublication,
    pr: PullRequestObservation,
    checks: BTreeMap<u64, StoredCheck>,
    comments: Vec<StoredComment>,
    reviews: Vec<StoredReview>,
    merge_wait_started: Option<Instant>,
    merge_sha: Option<String>,
}

#[derive(Default)]
struct WorkflowState {
    binding: Option<GitHubPullRequestBinding>,
    active_run_id: Option<String>,
    prepared: Option<PreparedGitWorkspace>,
    evidence: Option<GitHubEvidenceWriter>,
    publication: Option<PublicationState>,
    partial_operations: BTreeMap<String, GitHubPartialOperation>,
}

/// Production owner of one frozen GitHub delivery profile.
pub(crate) struct ProductionGitHubProvider {
    config: GitHubDeploymentConfig,
    reviewer_ids: Vec<ReviewerId>,
    client: GitHubRestClient,
    git: GitWorkspaceManager,
    signers: BTreeMap<GitHubAppRole, Arc<RsaAppSigner>>,
    state: Mutex<WorkflowState>,
}

impl ProductionGitHubProvider {
    pub(crate) fn open(
        config: GitHubDeploymentConfig,
        reviewer_ids: Vec<ReviewerId>,
    ) -> Result<Self> {
        let mut signers = BTreeMap::new();
        for role in GitHubAppRole::for_reviewers(&reviewer_ids) {
            let app = config
                .app(role)
                .ok_or_else(|| anyhow!("{} GitHub App configuration is missing", role.as_str()))?;
            let signer = RsaAppSigner::open_strict(&app.private_key_file).with_context(|| {
                format!(
                    "failed to open the configured {} GitHub App key",
                    role.as_str()
                )
            })?;
            signers.insert(role, Arc::new(signer));
        }
        Ok(Self {
            config,
            reviewer_ids,
            client: GitHubRestClient::new()?,
            git: GitWorkspaceManager::new(),
            signers,
            state: Mutex::new(WorkflowState::default()),
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, WorkflowState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("GitHub workflow state lock is poisoned"))
    }

    fn signer(&self, role: GitHubAppRole) -> Result<&RsaAppSigner> {
        self.signers
            .get(&role)
            .map(Arc::as_ref)
            .ok_or_else(|| anyhow!("{} GitHub App signer is unavailable", role.as_str()))
    }

    fn app_for_role(
        binding: &GitHubPullRequestBinding,
        role: GitHubAppRole,
    ) -> Result<&GitHubAppBinding> {
        match role {
            GitHubAppRole::Architect => Ok(&binding.architect_app),
            GitHubAppRole::Developer => Ok(&binding.developer_app),
            GitHubAppRole::Reviewer1 | GitHubAppRole::Reviewer2 => binding
                .reviewer_apps
                .iter()
                .find(|reviewer| {
                    reviewer.reviewer_id
                        == match role {
                            GitHubAppRole::Reviewer1 => ReviewerId::Reviewer1,
                            GitHubAppRole::Reviewer2 => ReviewerId::Reviewer2,
                            _ => unreachable!(),
                        }
                })
                .map(|reviewer| &reviewer.app)
                .ok_or_else(|| anyhow!("{} GitHub App is not active", role.as_str())),
        }
    }

    fn token(
        &self,
        binding: &GitHubPullRequestBinding,
        role: GitHubAppRole,
        operation: InstallationOperation,
    ) -> Result<InstallationToken> {
        if operation.requires_protected_auto_merge()
            && !binding.delivery_policy.is_protected_auto_merge()
        {
            bail!("manual GitHub delivery forbids protected auto-merge operations");
        }
        let app = Self::app_for_role(binding, role)?;
        let now = SystemTime::now();
        let jwt = self.signer(role)?.mint_jwt(app.app_id, now)?;
        let request =
            InstallationTokenRequest::for_operation(binding.repository_id, role, app, operation)?;
        mint_installation_token(&self.client, &jwt, app, &request, unix_time(now)?)
    }

    fn observe_preflight(&self) -> Result<GitHubPreflightObservation> {
        let roles = GitHubAppRole::for_reviewers(&self.reviewer_ids);
        let mut apps = Vec::new();
        let mut repository_ids = BTreeMap::new();
        let mut repository_identity: Option<(u64, bool)> = None;

        for role in roles.iter().copied() {
            let configured = self
                .config
                .app(role)
                .ok_or_else(|| anyhow!("{} GitHub App configuration is missing", role.as_str()))?;
            let now = SystemTime::now();
            let jwt = self.signer(role)?.mint_jwt(configured.app_id, now)?;
            let identity: AppIdentityObservation = self
                .client
                .get(RestEndpoint::AppIdentity, GitHubAuthentication::App(&jwt))?;
            if identity.id != configured.app_id
                || identity.slug != configured.slug
                || identity.owner.id == 0
                || identity.owner.login != self.config.owner
            {
                bail!(
                    "{} GitHub App identity differs from configuration",
                    role.as_str()
                );
            }
            let installation: InstallationObservation = self.client.get(
                RestEndpoint::RepositoryInstallation {
                    owner: self.config.owner.clone(),
                    repository: self.config.repository.clone(),
                },
                GitHubAuthentication::App(&jwt),
            )?;
            if installation.id == 0
                || installation.app_id != configured.app_id
                || installation.account.id == 0
                || installation.account.login != self.config.owner
                || installation.repository_selection != "selected"
            {
                bail!(
                    "{} GitHub App installation differs from configuration",
                    role.as_str()
                );
            }
            let (bootstrap, repository_id) = mint_bootstrap_repository_token(
                &self.client,
                &jwt,
                installation.id,
                &self.config.repository,
                role,
                unix_time(now)?,
            )
            .with_context(|| format!("{} GitHub App bootstrap preflight failed", role.as_str()))?;
            let repository: RepositoryObservation = self.client.get(
                RestEndpoint::Repository {
                    owner: self.config.owner.clone(),
                    repository: self.config.repository.clone(),
                },
                GitHubAuthentication::Installation(&bootstrap),
            )?;
            if repository.id != repository_id
                || repository.name != self.config.repository
                || !repository.private
                || repository.owner.id == 0
                || repository.owner.login != self.config.owner
            {
                bail!("{} GitHub App observed another repository", role.as_str());
            }
            if let Some(previous) = repository_identity {
                if previous != (repository.id, repository.private) {
                    bail!("active GitHub Apps observed different repository identities");
                }
            } else {
                repository_identity = Some((repository.id, repository.private));
            }
            let bot_login = format!("{}[bot]", configured.slug);
            let bot: BotObservation = self.client.get(
                RestEndpoint::BotUser {
                    login: bot_login.clone(),
                },
                GitHubAuthentication::Installation(&bootstrap),
            )?;
            if bot.id == 0 || bot.login != bot_login {
                bail!(
                    "{} GitHub App bot identity differs from configuration",
                    role.as_str()
                );
            }
            repository_ids.insert(role, vec![repository.id]);
            apps.push(GitHubAppObservation {
                role,
                app_id: identity.id,
                installation_id: installation.id,
                slug: identity.slug,
                bot_user_id: bot.id,
                effective_permissions: installation.permissions,
            });
        }

        let (repository_id, private) = repository_identity
            .ok_or_else(|| anyhow!("GitHub preflight observed no active App repository"))?;
        let provisional = freeze_preflight(
            &self.config,
            &self.reviewer_ids,
            GitHubPreflightObservation {
                owner: self.config.owner.clone(),
                repository: self.config.repository.clone(),
                repository_id,
                private,
                installation_repository_ids: repository_ids.clone(),
                apps: apps.clone(),
                expected_base_ref: format!("refs/heads/{}", self.config.base_branch),
                expected_base_sha: "0".repeat(40),
                ruleset_attestation_sha256: self
                    .config
                    .delivery_policy
                    .is_protected_auto_merge()
                    .then(|| "0".repeat(64)),
                inspection_id: "inspection-provisional".into(),
            },
        )?;
        let developer_read = self.token(
            &provisional.delivery_binding,
            GitHubAppRole::Developer,
            InstallationOperation::RepositoryAndRefRead,
        )?;
        let expected_ref = format!("refs/heads/{}", self.config.base_branch);
        let reference: RefObservation = self.client.get(
            RestEndpoint::Reference {
                owner: self.config.owner.clone(),
                repository: self.config.repository.clone(),
                qualified_ref: format!("heads/{}", self.config.base_branch),
            },
            GitHubAuthentication::Installation(&developer_read),
        )?;
        if reference.ref_name != expected_ref || reference.object.kind != "commit" {
            bail!("GitHub base ref observation differs from configuration");
        }
        validate_git_sha("GitHub preflight base SHA", &reference.object.sha)?;
        let ruleset_attestation_sha256 =
            ruleset_attestation_for_policy(self.config.delivery_policy, || {
                let architect_rules = self.token(
                    &provisional.delivery_binding,
                    GitHubAppRole::Architect,
                    InstallationOperation::RulesetAttestation,
                )?;
                self.attest_rules(&provisional.delivery_binding, &architect_rules)
            })?;

        Ok(GitHubPreflightObservation {
            owner: self.config.owner.clone(),
            repository: self.config.repository.clone(),
            repository_id,
            private,
            installation_repository_ids: repository_ids,
            apps,
            expected_base_ref: expected_ref,
            expected_base_sha: reference.object.sha,
            ruleset_attestation_sha256,
            inspection_id: format!("inspection-{}", Uuid::new_v4().simple()),
        })
    }

    fn attest_rules(
        &self,
        binding: &GitHubPullRequestBinding,
        token: &InstallationToken,
    ) -> Result<String> {
        let branch_rules: Vec<serde_json::Value> = self.client.get(
            RestEndpoint::RulesForBranch {
                owner: binding.owner.clone(),
                repository: binding.repository.clone(),
                branch: binding.base_branch.clone(),
            },
            GitHubAuthentication::Installation(token),
        )?;
        let ruleset_ids = branch_rules
            .iter()
            .map(|rule| required_u64(rule, "ruleset_id"))
            .collect::<Result<BTreeSet<_>>>()?;
        if ruleset_ids.is_empty() {
            bail!("GitHub base branch has no active repository rulesets");
        }
        let mut rulesets = Vec::new();
        for ruleset_id in ruleset_ids.iter().copied() {
            let ruleset: serde_json::Value = self.client.get(
                RestEndpoint::RepositoryRuleset {
                    owner: binding.owner.clone(),
                    repository: binding.repository.clone(),
                    ruleset_id,
                },
                GitHubAuthentication::Installation(token),
            )?;
            rulesets.push(ruleset);
        }
        canonical_rules_attestation(binding, &branch_rules, &rulesets)
    }

    fn binding(&self) -> Result<GitHubPullRequestBinding> {
        self.state()?
            .binding
            .clone()
            .ok_or_else(|| anyhow!("GitHub workflow has no frozen delivery binding"))
    }

    fn require_active_run(&self, run_id: &str) -> Result<()> {
        if self.state()?.active_run_id.as_deref() != Some(run_id) {
            bail!("GitHub workflow request belongs to another or unprepared run");
        }
        Ok(())
    }

    fn refresh_inspection(
        &self,
        binding: &GitHubPullRequestBinding,
    ) -> Result<GitHubInspectionResult> {
        let result = freeze_preflight(&self.config, &self.reviewer_ids, self.observe_preflight()?)?;
        validate_inspection_result(binding, &result)?;
        Ok(result)
    }

    fn publication_context(
        binding: &GitHubPullRequestBinding,
        request: &PublishDeveloperCandidateRequest,
        prepared: &PreparedGitWorkspace,
    ) -> PublicationContext {
        PublicationContext {
            run_id: request.run_id.clone(),
            plan_hash: request.plan_hash.clone(),
            owner: binding.owner.clone(),
            repository: binding.repository.clone(),
            repository_id: binding.repository_id,
            branch: prepared.branch().into(),
            base_branch: binding.base_branch.clone(),
            base_sha: prepared.base_sha().into(),
        }
    }

    fn task_context(
        request: &PublishDeveloperCandidateRequest,
        head_sha: &str,
    ) -> Result<TaskPublicationContext> {
        Ok(TaskPublicationContext {
            ordinal: u32::try_from(request.task_ordinal)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| anyhow!("GitHub task ordinal overflow"))?,
            count: u32::try_from(request.task_count)
                .map_err(|_| anyhow!("GitHub task count overflow"))?,
            task_key: request.task_key.clone(),
            title: request.task_title.clone(),
            generation: request.generation,
            task_base_sha: request.task_base_sha.clone(),
            previous_head_sha: request.previous_head_sha.clone(),
            head_sha: head_sha.into(),
        })
    }

    fn task_context_for_review(
        request: &PublishReviewerReviewRequest,
    ) -> Result<TaskPublicationContext> {
        Ok(TaskPublicationContext {
            ordinal: u32::try_from(request.task_ordinal)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| anyhow!("GitHub task ordinal overflow"))?,
            count: u32::try_from(request.task_count)
                .map_err(|_| anyhow!("GitHub task count overflow"))?,
            task_key: request.task_key.clone(),
            title: request.task_title.clone(),
            generation: request.generation,
            task_base_sha: request.task_base_sha.clone(),
            previous_head_sha: request.task_base_sha.clone(),
            head_sha: request.head_sha.clone(),
        })
    }

    fn outcomes(values: &[GitHubTaskOutcomeEvidence]) -> Result<Vec<(u32, String, String)>> {
        values
            .iter()
            .map(|task| {
                Ok((
                    u32::try_from(task.task_ordinal)
                        .ok()
                        .and_then(|value| value.checked_add(1))
                        .ok_or_else(|| anyhow!("GitHub task outcome ordinal overflow"))?,
                    task.task_key.clone(),
                    match task.outcome {
                        None => "pending",
                        Some(TaskCompletionOutcome::Lgtm) => "lgtm",
                        Some(TaskCompletionOutcome::ReviewExhausted) => "review_exhausted",
                    }
                    .into(),
                ))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        binding: &GitHubPullRequestBinding,
        kind: EvidenceKind,
        operation_id: &str,
        actor: Option<&GitHubAppBinding>,
        object_id: Option<u64>,
        url: Option<String>,
        base_sha: Option<String>,
        head_sha: Option<String>,
        merge_sha: Option<String>,
        artifact_sha256: Option<String>,
        outcome: &str,
        reconciled: bool,
    ) -> GitHubEvidenceRecord {
        GitHubEvidenceRecord {
            schema_version: 1,
            kind,
            operation_id: operation_id.into(),
            repository_id: binding.repository_id,
            actor: actor.map(|app| EvidenceActor {
                app_id: app.app_id,
                slug: app.slug.clone(),
                bot_user_id: app.bot_user_id,
            }),
            object_id,
            url,
            base_sha,
            head_sha,
            merge_sha,
            artifact_sha256,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            outcome: outcome.into(),
            reconciled_after_ambiguous_result: reconciled,
        }
    }

    fn validate_run_binding(
        binding: &GitHubPullRequestBinding,
        request: &PrepareGitHubRunRequest,
        refreshed: &GitHubInspectionResult,
    ) -> Result<()> {
        if request.run_binding.inspected_repository_id != binding.repository_id
            || request.run_binding.expected_base_ref
                != format!("refs/heads/{}", binding.base_branch)
            || refreshed.inspection.expected_base_sha != request.run_binding.expected_base_sha
            || refreshed.inspection.ruleset_attestation_sha256
                != request.run_binding.ruleset_attestation_sha256
        {
            bail!("approved GitHub run binding drifted before repository preparation");
        }
        Ok(())
    }

    fn gate_snapshot(
        &self,
    ) -> Result<(
        GitHubPullRequestBinding,
        PreparedGitWorkspace,
        PublicationState,
    )> {
        let state = self.state()?;
        Ok((
            state
                .binding
                .clone()
                .ok_or_else(|| anyhow!("GitHub merge gate lacks a delivery binding"))?,
            state
                .prepared
                .clone()
                .ok_or_else(|| anyhow!("GitHub merge gate lacks a prepared repository"))?,
            state
                .publication
                .clone()
                .ok_or_else(|| anyhow!("GitHub merge gate lacks Pull Request state"))?,
        ))
    }

    fn validate_manual_terminal_audit(
        &self,
        binding: &GitHubPullRequestBinding,
        request: &PublishReviewCheckRequest,
        require_all_checks_completed: bool,
    ) -> Result<()> {
        validate_manual_terminal_audit_for_policy(
            binding.delivery_policy,
            request.task_ordinal,
            request.task_count,
            request
                .task_outcomes
                .get(request.task_ordinal)
                .and_then(|task| task.outcome),
            || {
                let (_, prepared, publication) = self.gate_snapshot()?;
                if publication.context.run_id != request.run_id
                    || publication.context.branch != prepared.branch()
                    || request.task_outcomes.len() != request.task_count
                    || request
                        .task_outcomes
                        .first()
                        .is_none_or(|task| task.task_base_sha != prepared.base_sha())
                {
                    bail!("manual GitHub terminal audit differs from the frozen Pull Request");
                }
                let mut tasks = request.task_outcomes.clone();
                let current = tasks.get_mut(request.task_ordinal).ok_or_else(|| {
                    anyhow!("manual GitHub terminal audit lacks the current task")
                })?;
                if current.task_ordinal != request.task_ordinal
                    || current
                        .task_final_head_sha
                        .as_ref()
                        .is_some_and(|head| head != &request.head_sha)
                {
                    bail!("manual GitHub terminal audit differs from the exact final task");
                }
                current.task_final_head_sha = Some(request.head_sha.clone());
                validate_terminal_task_chain(&tasks, &request.head_sha)?;

                validate_remote_comments(self, binding, &publication, publication.pr.number)?;
                validate_remote_checks(self, binding, &publication, require_all_checks_completed)?;
                validate_remote_reviews(
                    self,
                    binding,
                    &publication,
                    publication.pr.number,
                    &tasks,
                )?;

                // Read the Pull Request last so an edited, closed, merged, or
                // rebound PR cannot be reported as the preserved terminal
                // disposition after the audit-history reads complete.
                let architect_pr = self.token(
                    binding,
                    GitHubAppRole::Architect,
                    InstallationOperation::PullRequestRead,
                )?;
                let publisher = GitHubPublisher::new(&self.client, &publication.context)?;
                let pr = publisher.read_pull_request(publication.pr.number, &architect_pr)?;
                validate_open_pr(binding, &publication, &pr, &request.head_sha)
            },
        )
    }

    fn probe_merge_gate(
        &self,
        request: &WaitForMergeGateRequest,
    ) -> Result<Option<MergeGateObservation>> {
        validate_all_lgtm_chain(&request.tasks, &request.final_head_sha)?;
        let (binding, prepared, publication) = self.gate_snapshot()?;
        if !binding.delivery_policy.is_protected_auto_merge() {
            bail!("manual GitHub delivery cannot enter the merge gate");
        }
        if publication.pr.number != request.pr_number
            || publication.context.run_id != request.run_id
            || publication.context.branch != prepared.branch()
            || request
                .tasks
                .first()
                .is_none_or(|task| task.task_base_sha != prepared.base_sha())
        {
            bail!("GitHub merge gate request differs from the frozen Pull Request");
        }
        let refreshed = self.refresh_inspection(&binding)?;
        if refreshed.inspection.expected_base_sha != prepared.base_sha()
            || refreshed.inspection.ruleset_attestation_sha256.as_deref()
                != Some(request.ruleset_attestation_sha256.as_str())
        {
            bail!("GitHub base or ruleset drifted before merge");
        }
        let read = self.token(
            &binding,
            GitHubAppRole::Developer,
            InstallationOperation::RepositoryAndRefRead,
        )?;
        let credential = read.git_credential()?;
        self.git.validate_published_checkout(
            &prepared,
            &binding,
            &request.final_head_sha,
            Some(&credential),
        )?;
        let architect_pr = self.token(
            &binding,
            GitHubAppRole::Architect,
            InstallationOperation::PullRequestRead,
        )?;
        let publisher = GitHubPublisher::new(&self.client, &publication.context)?;
        let pr = publisher.read_pull_request(request.pr_number, &architect_pr)?;
        validate_open_pr(&binding, &publication, &pr, &request.final_head_sha)?;
        validate_remote_comments(self, &binding, &publication, request.pr_number)?;

        let final_check = publication
            .checks
            .get(&request.check_run_id)
            .ok_or_else(|| anyhow!("GitHub merge gate Check was not published by this run"))?;
        if !final_check.completed || final_check.conclusion != Some(CheckConclusion::Success) {
            bail!("GitHub merge gate final Check is not successful");
        }
        validate_remote_checks(self, &binding, &publication, true)?;
        validate_remote_reviews(
            self,
            &binding,
            &publication,
            request.pr_number,
            &request.tasks,
        )?;

        if pr.mergeable_state.as_deref() == Some("dirty") {
            bail!("GitHub Pull Request has merge conflicts at the exact final head");
        }
        if pr.mergeable == Some(true)
            && matches!(
                pr.mergeable_state.as_deref(),
                Some("clean" | "has_hooks" | "unstable")
            )
        {
            return Ok(Some(MergeGateObservation {
                operation_id: request.operation_id.clone(),
                pr_number: request.pr_number,
                final_head_sha: request.final_head_sha.clone(),
                base_sha: prepared.base_sha().into(),
                ruleset_attestation_sha256: refreshed
                    .inspection
                    .ruleset_attestation_sha256
                    .expect("protected merge inspection has a ruleset attestation"),
                check_run_id: request.check_run_id,
            }));
        }
        Ok(None)
    }

    fn merge_deadline(&self, binding: &GitHubPullRequestBinding) -> Result<Instant> {
        let mut state = self.state()?;
        let publication = state
            .publication
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub merge wait lacks Pull Request state"))?;
        let started = *publication
            .merge_wait_started
            .get_or_insert_with(Instant::now);
        started
            .checked_add(Duration::from_secs(u64::from(binding.merge_wait_seconds)))
            .ok_or_else(|| anyhow!("GitHub merge wait deadline overflow"))
    }
}

impl GitHubPreflightProvider for ProductionGitHubProvider {
    fn preflight(
        &self,
        request: &GitHubPreflightRequest<'_>,
    ) -> Result<GitHubPreflightObservation> {
        if request.config != &self.config || request.reviewer_ids != self.reviewer_ids {
            bail!("GitHub preflight request differs from the provider's frozen configuration");
        }
        let observation = self.observe_preflight()?;
        let frozen = freeze_preflight(&self.config, &self.reviewer_ids, observation.clone())?;
        let mut state = self.state()?;
        if state.binding.is_some() {
            bail!("GitHub startup preflight was already completed");
        }
        state.binding = Some(frozen.delivery_binding);
        Ok(observation)
    }
}

impl GitHubInspectionProvider for ProductionGitHubProvider {
    fn inspect(&self, request: &GitHubInspectionRequest) -> Result<GitHubInspectionResult> {
        let binding = self.binding()?;
        if request.delivery_binding != binding {
            bail!("GitHub inspection request differs from the frozen delivery binding");
        }
        self.refresh_inspection(&binding)
    }
}

impl GitHubWorkflowProvider for ProductionGitHubProvider {
    fn begin_fresh_run(&self, terminal_run_id: &str, _fresh_run_id: &str) -> Result<()> {
        let mut state = self.state()?;
        if state
            .active_run_id
            .as_deref()
            .is_some_and(|active| active != terminal_run_id)
        {
            bail!("GitHub workflow state belongs to another run");
        }
        // Never clean up or adopt a terminal run here. Its durable evidence
        // and preserved repository/remote artifacts remain untouched; the new
        // run must perform a fresh inspection and preparation after approval.
        state.active_run_id = None;
        state.prepared = None;
        state.evidence = None;
        state.publication = None;
        state.partial_operations.clear();
        Ok(())
    }

    fn prepare_repository(
        &self,
        workspace: &TasksWorkspace,
        request: &PrepareGitHubRunRequest,
    ) -> Result<RepositoryPreparedObservation> {
        let binding = self.binding()?;
        let refreshed = self.refresh_inspection(&binding)?;
        Self::validate_run_binding(&binding, request, &refreshed)?;
        let worktree_path = workspace
            .repository_path()
            .to_str()
            .ok_or_else(|| anyhow!("prepared GitHub worktree path is not UTF-8"))?
            .to_owned();
        {
            let state = self.state()?;
            if state.active_run_id.is_some()
                || state.prepared.is_some()
                || state.publication.is_some()
                || state.evidence.is_some()
                || !state.partial_operations.is_empty()
            {
                bail!("GitHub repository preparation was already completed");
            }
        }
        let token = self.token(
            &binding,
            GitHubAppRole::Developer,
            InstallationOperation::GitFetch,
        )?;
        let credential = token.git_credential()?;
        let (prepared, progress) = self.git.prepare_with_progress(
            workspace,
            &binding,
            &request.run_binding,
            &request.plan_hash,
            Some(&credential),
        );
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if progress.has_managed_artifacts() {
                    let mut state = self.state()?;
                    if state
                        .partial_operations
                        .insert(
                            request.operation_id.clone(),
                            partial_repository_preparation(request, &worktree_path, &progress),
                        )
                        .is_some()
                    {
                        bail!("GitHub preparation operation already has partial state");
                    }
                }
                return Err(error);
            }
        };
        let complete_progress = GitPreparationProgress {
            local_base_ref_created: true,
            local_branch_created: true,
            local_worktree_created: true,
        };
        {
            let mut state = self.state()?;
            if state
                .partial_operations
                .insert(
                    request.operation_id.clone(),
                    partial_repository_preparation(request, &worktree_path, &complete_progress),
                )
                .is_some()
            {
                bail!("GitHub preparation operation already has partial state");
            }
        }
        let mut writer = GitHubEvidenceWriter::create(workspace)?;
        writer.write_binding(&binding, &request.run_binding)?;
        writer.write(
            EvidencePath::RepositoryPrepared,
            &Self::record(
                &binding,
                EvidenceKind::RepositoryPrepared,
                &request.operation_id,
                Some(&binding.developer_app),
                None,
                None,
                Some(prepared.base_sha().into()),
                Some(prepared.base_sha().into()),
                None,
                None,
                "prepared",
                false,
            ),
        )?;
        let observation = RepositoryPreparedObservation {
            operation_id: request.operation_id.clone(),
            base_sha: prepared.base_sha().into(),
            branch: prepared.branch().into(),
            worktree_path,
        };
        let mut state = self.state()?;
        if state.active_run_id.is_some()
            || state.prepared.is_some()
            || state.publication.is_some()
            || state.evidence.is_some()
        {
            bail!("GitHub repository preparation was already completed");
        }
        state.active_run_id = Some(request.run_id.clone());
        state.prepared = Some(prepared);
        state.evidence = Some(writer);
        state.partial_operations.remove(&request.operation_id);
        Ok(observation)
    }

    fn publish_candidate(
        &self,
        request: &PublishDeveloperCandidateRequest,
    ) -> Result<CandidatePublishedObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        let final_body = read_native_final(&request.developer_final_path)?;
        let (prepared, existing_publication) = {
            let state = self.state()?;
            (
                state
                    .prepared
                    .clone()
                    .ok_or_else(|| anyhow!("GitHub candidate has no prepared repository"))?,
                state.publication.clone(),
            )
        };
        let previous = (request.previous_head_sha != prepared.base_sha())
            .then_some(request.previous_head_sha.as_str());
        let push_token = self.token(
            &binding,
            GitHubAppRole::Developer,
            InstallationOperation::GitPush,
        )?;
        let push_credential = push_token.git_credential()?;
        let candidate = self.git.validate_developer_commit(
            &prepared,
            &binding,
            previous,
            Some(&push_credential),
        )?;
        if candidate.parent_sha != request.previous_head_sha {
            bail!("Developer candidate parent differs from the exact core request");
        }
        let context = existing_publication
            .as_ref()
            .map(|publication| publication.context.clone())
            .unwrap_or_else(|| Self::publication_context(&binding, request, &prepared));
        if context.run_id != request.run_id || context.plan_hash != request.plan_hash {
            bail!("GitHub candidate differs from the prepared run publication binding");
        }
        let task = Self::task_context(request, &candidate.head_sha)?;
        let publisher = GitHubPublisher::new(&self.client, &context)?;
        let push = self.git.push_candidate(
            &prepared,
            &binding,
            &candidate,
            previous,
            Some(&push_credential),
        )?;
        {
            let mut state = self.state()?;
            let partial = GitHubPartialOperation::Candidate(GitHubPartialCandidatePublication {
                task_ordinal: request.task_ordinal,
                generation: request.generation,
                previous_head_sha: request.previous_head_sha.clone(),
                head_sha: candidate.head_sha.clone(),
                pr_number: existing_publication
                    .as_ref()
                    .map(|publication| publication.pr.number),
                pr_node_id: existing_publication
                    .as_ref()
                    .map(|publication| publication.pr.node_id.clone()),
                pr_url: existing_publication
                    .as_ref()
                    .map(|publication| publication.pr.html_url.clone()),
                pr_actor_bot_user_id: existing_publication
                    .as_ref()
                    .map(|publication| publication.pr.user.id),
                check_run_id: None,
                check_url: None,
                check_actor_app_id: None,
            });
            if state
                .partial_operations
                .insert(request.operation_id.clone(), partial)
                .is_some()
            {
                bail!("GitHub candidate operation already has partial publication state");
            }
            state
                .evidence
                .as_mut()
                .ok_or_else(|| anyhow!("GitHub candidate evidence writer is missing"))?
                .write(
                    EvidencePath::Candidate {
                        ordinal: task.ordinal,
                        task_key: task.task_key.clone(),
                        generation: task.generation,
                    },
                    &Self::record(
                        &binding,
                        EvidenceKind::Candidate,
                        &request.operation_id,
                        Some(&binding.developer_app),
                        None,
                        None,
                        Some(task.task_base_sha.clone()),
                        Some(candidate.head_sha.clone()),
                        None,
                        Some(sha256_hex(final_body.as_bytes())),
                        "published",
                        push.reconciled_after_command_failure,
                    ),
                )?;
        }

        let (
            pr,
            pr_title_value,
            pr_publication,
            publication_kind,
            publication_object,
            developer_comment,
        ) = if let Some(existing) = existing_publication.as_ref() {
            if request.existing_pr_number != Some(existing.pr.number)
                || request.expected_check_run_id != existing.checks.keys().next_back().copied()
            {
                bail!("later GitHub candidate differs from the one-PR chain");
            }
            let publication =
                render_developer_comment(&context, &task, request.correction, &final_body)?;
            let token = self.token(
                &binding,
                GitHubAppRole::Developer,
                InstallationOperation::DeveloperComment,
            )?;
            let comment = retry_safe_mutation(|| {
                publisher.create_comment(
                    existing.pr.number,
                    &publication,
                    binding.developer_app.bot_user_id,
                    &token,
                )
            })?;
            (
                existing.pr.clone(),
                existing.pr_title.clone(),
                existing.pr_publication.clone(),
                EvidenceKind::DeveloperComment,
                Some((
                    comment.id,
                    comment.html_url.clone(),
                    publication.marker.artifact_sha256.clone(),
                )),
                Some(StoredComment {
                    publication,
                    observation: comment,
                }),
            )
        } else {
            if request.existing_pr_number.is_some()
                || request.expected_check_run_id.is_some()
                || request.task_ordinal != 0
            {
                bail!("initial GitHub candidate has stale Pull Request coordinates");
            }
            let ordered_tasks = request
                .task_outcomes
                .iter()
                .map(|item| (item.task_key.clone(), item.task_title.clone()))
                .collect::<Vec<_>>();
            let reviewers = binding
                .reviewer_apps
                .iter()
                .map(|reviewer| (reviewer.reviewer_id, format!("{}[bot]", reviewer.app.slug)))
                .collect::<Vec<_>>();
            let publication =
                render_pull_request_body(&context, &ordered_tasks, &reviewers, &task, &final_body)?;
            let title = pull_request_title(&context, &task)?;
            let token = self.token(
                &binding,
                GitHubAppRole::Developer,
                InstallationOperation::PullRequestCreate,
            )?;
            let pr = retry_safe_mutation(|| {
                publisher.create_pull_request(
                    &title,
                    &publication,
                    binding.developer_app.bot_user_id,
                    &token,
                )
            })?;
            (
                pr.clone(),
                title,
                publication.clone(),
                EvidenceKind::PullRequest,
                Some((
                    pr.id,
                    pr.html_url.clone(),
                    publication.marker.artifact_sha256,
                )),
                None,
            )
        };

        {
            let mut state = self.state()?;
            let partial = state
                .partial_operations
                .get_mut(&request.operation_id)
                .ok_or_else(|| anyhow!("GitHub candidate partial state disappeared"))?;
            let GitHubPartialOperation::Candidate(partial) = partial else {
                bail!("GitHub candidate partial state has another operation kind");
            };
            partial.pr_number = Some(pr.number);
            partial.pr_node_id = Some(pr.node_id.clone());
            partial.pr_url = Some(pr.html_url.clone());
            partial.pr_actor_bot_user_id = Some(pr.user.id);
            let publication = state.publication.get_or_insert_with(|| PublicationState {
                context: context.clone(),
                pr_title: pr_title_value.clone(),
                pr_publication: pr_publication.clone(),
                pr: pr.clone(),
                checks: BTreeMap::new(),
                comments: Vec::new(),
                reviews: Vec::new(),
                merge_wait_started: None,
                merge_sha: None,
            });
            if let Some(comment) = developer_comment {
                publication.comments.push(comment);
            }
            if let Some((object_id, url, artifact)) = publication_object {
                let path = if publication_kind == EvidenceKind::PullRequest {
                    EvidencePath::PullRequest
                } else {
                    EvidencePath::DeveloperComment {
                        ordinal: task.ordinal,
                        task_key: task.task_key.clone(),
                        generation: task.generation,
                    }
                };
                state
                    .evidence
                    .as_mut()
                    .ok_or_else(|| anyhow!("GitHub candidate evidence writer is missing"))?
                    .write(
                        path,
                        &Self::record(
                            &binding,
                            publication_kind,
                            &request.operation_id,
                            Some(&binding.developer_app),
                            Some(object_id),
                            Some(url),
                            Some(task.task_base_sha.clone()),
                            Some(candidate.head_sha.clone()),
                            None,
                            Some(artifact),
                            "published",
                            false,
                        ),
                    )?;
            }
        }

        let outcomes = Self::outcomes(&request.task_outcomes)?;
        let (check_output, check_marker) = render_check_output(&context, &task, &outcomes, None)?;
        let external_id = check_external_id(request, &candidate.head_sha);
        let check_token = self.token(
            &binding,
            GitHubAppRole::Architect,
            InstallationOperation::CheckPublish,
        )?;
        let check = retry_safe_mutation(|| {
            publisher.create_check(
                &external_id,
                &candidate.head_sha,
                &check_output,
                &check_marker,
                binding.architect_app.app_id,
                &check_token,
            )
        })?;
        let stored_check = StoredCheck {
            task: task.clone(),
            outcomes,
            external_id,
            output: check_output,
            marker: check_marker,
            observation: check.clone(),
            completed: false,
            conclusion: None,
        };

        let mut state = self.state()?;
        let partial = state
            .partial_operations
            .get_mut(&request.operation_id)
            .ok_or_else(|| anyhow!("GitHub candidate partial state disappeared"))?;
        let GitHubPartialOperation::Candidate(partial) = partial else {
            bail!("GitHub candidate partial state has another operation kind");
        };
        partial.check_run_id = Some(check.id);
        partial.check_url = Some(check.html_url.clone());
        partial.check_actor_app_id = Some(check.app.id);
        let publication = state
            .publication
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub candidate Pull Request state disappeared"))?;
        publication.checks.insert(check.id, stored_check);
        state.partial_operations.remove(&request.operation_id);

        Ok(CandidatePublishedObservation {
            operation_id: request.operation_id.clone(),
            task_ordinal: request.task_ordinal,
            generation: request.generation,
            previous_head_sha: request.previous_head_sha.clone(),
            head_sha: candidate.head_sha,
            pr_number: pr.number,
            pr_node_id: pr.node_id,
            pr_url: pr.html_url,
            pr_actor_bot_user_id: pr.user.id,
            check_run_id: check.id,
            check_url: check.html_url,
            check_actor_app_id: check.app.id,
        })
    }

    fn take_partial_operation(&self, operation_id: &str) -> Result<Option<GitHubPartialOperation>> {
        Ok(self.state()?.partial_operations.remove(operation_id))
    }

    fn publish_review(
        &self,
        request: &PublishReviewerReviewRequest,
    ) -> Result<ReviewerReviewPublishedObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        let final_body = read_native_final(&request.reviewer_final_path)?;
        let (prepared, publication) = {
            let state = self.state()?;
            (
                state
                    .prepared
                    .clone()
                    .ok_or_else(|| anyhow!("GitHub review has no prepared repository"))?,
                state
                    .publication
                    .clone()
                    .ok_or_else(|| anyhow!("GitHub review has no Pull Request"))?,
            )
        };
        if publication.context.run_id != request.run_id
            || publication.pr.number != request.pr_number
        {
            bail!("GitHub review targets another Pull Request");
        }
        let read = self.token(
            &binding,
            GitHubAppRole::Developer,
            InstallationOperation::RepositoryAndRefRead,
        )?;
        let credential = read.git_credential()?;
        self.git.validate_published_checkout(
            &prepared,
            &binding,
            &request.head_sha,
            Some(&credential),
        )?;
        let task = Self::task_context_for_review(request)?;
        let rendered = render_reviewer_body(
            &publication.context,
            &task,
            request.reviewer_id,
            &final_body,
        )?;
        let role = reviewer_role(request.reviewer_id);
        let app = Self::app_for_role(&binding, role)?;
        let token = self.token(&binding, role, InstallationOperation::ReviewPublish)?;
        let publisher = GitHubPublisher::new(&self.client, &publication.context)?;
        let review = retry_safe_mutation(|| {
            publisher.create_review(
                request.pr_number,
                request.verdict,
                &rendered,
                app.bot_user_id,
                &token,
            )
        })?;
        let mut state = self.state()?;
        let partial = GitHubPartialOperation::ReviewerReview(GitHubPartialReviewerReview {
            task_ordinal: request.task_ordinal,
            reviewer_id: request.reviewer_id,
            generation: request.generation,
            head_sha: request.head_sha.clone(),
            verdict: request.verdict,
            review_id: review.id,
            review_url: review.html_url.clone(),
            actor_bot_user_id: review.user.id,
            final_artifact_sha256: rendered.marker.artifact_sha256.clone(),
        });
        if state
            .partial_operations
            .insert(request.operation_id.clone(), partial)
            .is_some()
        {
            bail!("GitHub review operation already has partial publication state");
        }
        state
            .evidence
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub review evidence writer is missing"))?
            .write(
                EvidencePath::Review {
                    ordinal: task.ordinal,
                    task_key: task.task_key.clone(),
                    generation: task.generation,
                    reviewer: request.reviewer_id,
                },
                &Self::record(
                    &binding,
                    EvidenceKind::Review,
                    &request.operation_id,
                    Some(app),
                    Some(review.id),
                    Some(review.html_url.clone()),
                    Some(task.task_base_sha.clone()),
                    Some(task.head_sha.clone()),
                    None,
                    Some(rendered.marker.artifact_sha256.clone()),
                    match request.verdict {
                        ReviewerVerdict::Lgtm => "lgtm",
                        ReviewerVerdict::RequestChanges => "request_changes",
                    },
                    false,
                ),
            )?;
        state
            .publication
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub review lost Pull Request state"))?
            .reviews
            .push(StoredReview {
                task_ordinal: request.task_ordinal,
                reviewer_id: request.reviewer_id,
                generation: request.generation,
                head_sha: request.head_sha.clone(),
                verdict: request.verdict,
                publication: rendered.clone(),
                observation: review.clone(),
            });
        state.partial_operations.remove(&request.operation_id);
        Ok(ReviewerReviewPublishedObservation {
            operation_id: request.operation_id.clone(),
            task_ordinal: request.task_ordinal,
            reviewer_id: request.reviewer_id,
            generation: request.generation,
            head_sha: request.head_sha.clone(),
            verdict: request.verdict,
            review_id: review.id,
            review_url: review.html_url,
            actor_bot_user_id: review.user.id,
            final_artifact_sha256: rendered.marker.artifact_sha256,
        })
    }

    fn publish_check(
        &self,
        request: &PublishReviewCheckRequest,
    ) -> Result<ReviewCheckPublishedObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        let refreshed = self.refresh_inspection(&binding)?;
        let prepared = self
            .state()?
            .prepared
            .clone()
            .ok_or_else(|| anyhow!("GitHub Check has no prepared repository"))?;
        if refreshed.inspection.expected_base_sha != prepared.base_sha()
            || refreshed.inspection.ruleset_attestation_sha256 != request.ruleset_attestation_sha256
        {
            bail!("GitHub base or ruleset drifted before Check conclusion");
        }
        let read = self.token(
            &binding,
            GitHubAppRole::Developer,
            InstallationOperation::RepositoryAndRefRead,
        )?;
        let credential = read.git_credential()?;
        self.git.validate_published_checkout(
            &prepared,
            &binding,
            &request.head_sha,
            Some(&credential),
        )?;
        let (publication_context, previous) = {
            let state = self.state()?;
            let publication = state
                .publication
                .as_ref()
                .ok_or_else(|| anyhow!("GitHub Check has no Pull Request"))?;
            if publication.context.run_id != request.run_id {
                bail!("GitHub Check belongs to another run");
            }
            (
                publication.context.clone(),
                publication
                    .checks
                    .get(&request.check_run_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("GitHub Check ID was not opened by this run"))?,
            )
        };
        if previous.completed
            || previous.task.generation != request.generation
            || previous.task.head_sha != request.head_sha
            || previous.observation.html_url != request.check_url
        {
            bail!("GitHub Check conclusion differs from its exact predecessor");
        }
        self.validate_manual_terminal_audit(&binding, request, false)?;
        let outcomes = Self::outcomes(&request.task_outcomes)?;
        let conclusion = map_check_conclusion(request.conclusion);
        let (output, marker) = render_check_output(
            &publication_context,
            &previous.task,
            &outcomes,
            Some(conclusion),
        )?;
        let token = self.token(
            &binding,
            GitHubAppRole::Architect,
            InstallationOperation::CheckPublish,
        )?;
        let publisher = GitHubPublisher::new(&self.client, &publication_context)?;
        let check = retry_safe_mutation(|| {
            publisher.conclude_check(
                request.check_run_id,
                &previous.external_id,
                conclusion,
                &previous.output,
                &previous.marker,
                &output,
                &marker,
                binding.architect_app.app_id,
                &token,
            )
        })?;
        let mut state = self.state()?;
        let partial = GitHubPartialOperation::ReviewCheck(GitHubPartialReviewCheck {
            task_ordinal: request.task_ordinal,
            generation: request.generation,
            head_sha: request.head_sha.clone(),
            check_run_id: check.id,
            check_url: check.html_url.clone(),
            state: request.conclusion.as_str().into(),
            actor_app_id: check.app.id,
        });
        if state
            .partial_operations
            .insert(request.operation_id.clone(), partial)
            .is_some()
        {
            bail!("GitHub Check operation already has partial publication state");
        }
        state
            .evidence
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub Check evidence writer is missing"))?
            .write(
                EvidencePath::Check {
                    ordinal: previous.task.ordinal,
                    task_key: previous.task.task_key.clone(),
                    generation: previous.task.generation,
                },
                &Self::record(
                    &binding,
                    EvidenceKind::Check,
                    &request.operation_id,
                    Some(&binding.architect_app),
                    Some(check.id),
                    Some(check.html_url.clone()),
                    Some(previous.task.task_base_sha.clone()),
                    Some(previous.task.head_sha.clone()),
                    None,
                    Some(marker.artifact_sha256.clone()),
                    request.conclusion.as_str(),
                    false,
                ),
            )?;
        let stored = state
            .publication
            .as_mut()
            .and_then(|publication| publication.checks.get_mut(&request.check_run_id))
            .ok_or_else(|| anyhow!("GitHub Check state disappeared during conclusion"))?;
        stored.outcomes = outcomes;
        stored.output = output;
        stored.marker = marker;
        stored.observation = check.clone();
        stored.completed = true;
        stored.conclusion = Some(conclusion);
        drop(state);
        self.validate_manual_terminal_audit(&binding, request, true)?;
        self.state()?
            .partial_operations
            .remove(&request.operation_id);
        Ok(ReviewCheckPublishedObservation {
            operation_id: request.operation_id.clone(),
            task_ordinal: request.task_ordinal,
            generation: request.generation,
            head_sha: request.head_sha.clone(),
            check_run_id: check.id,
            check_url: check.html_url,
            state: request.conclusion.as_str().into(),
            actor_app_id: check.app.id,
        })
    }

    fn wait_for_merge_gate(
        &self,
        request: &WaitForMergeGateRequest,
        cancelled: &AtomicBool,
    ) -> Result<MergeGateObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        let deadline = self.merge_deadline(&binding)?;
        let mut delay = MERGE_POLL_INITIAL;
        loop {
            ensure_not_cancelled(cancelled)?;
            match self.probe_merge_gate(request) {
                Ok(Some(observation)) => return Ok(observation),
                Ok(None) => {}
                Err(error) => {
                    let Some(remote_delay) = retryable_read_delay(&error, delay)? else {
                        return Err(error);
                    };
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remote_delay > remaining {
                        bail!("GitHub merge-gate retry delay exceeds the approved wait deadline");
                    }
                    sleep_cancellable(cancelled, remote_delay)?;
                    delay = delay.saturating_mul(2).min(MERGE_POLL_MAX);
                    continue;
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "GitHub Pull Request did not satisfy merge gates before the approved deadline"
                );
            }
            sleep_cancellable(
                cancelled,
                delay.min(deadline.saturating_duration_since(Instant::now())),
            )?;
            delay = delay.saturating_mul(2).min(MERGE_POLL_MAX);
        }
    }

    fn merge_pull_request(
        &self,
        request: &MergePullRequestRequest,
        cancelled: &AtomicBool,
    ) -> Result<PullRequestMergedObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        validate_all_lgtm_chain(&request.tasks, &request.final_head_sha)?;
        let deadline = self.merge_deadline(&binding)?;
        let mut delay = MERGE_POLL_INITIAL;
        loop {
            ensure_not_cancelled(cancelled)?;
            let gate_request = WaitForMergeGateRequest {
                operation_id: request.operation_id.clone(),
                run_id: request.run_id.clone(),
                pr_number: request.pr_number,
                final_head_sha: request.final_head_sha.clone(),
                check_run_id: request.check_run_id,
                ruleset_attestation_sha256: request.ruleset_attestation_sha256.clone(),
                tasks: request.tasks.clone(),
            };
            if request.base_sha != self.gate_snapshot()?.1.base_sha() {
                bail!("GitHub merge request base differs from the prepared run");
            }
            let gate_ready = match self.probe_merge_gate(&gate_request) {
                Ok(observation) => observation.is_some(),
                Err(error) => {
                    let Some(remote_delay) = retryable_read_delay(&error, delay)? else {
                        return Err(error);
                    };
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remote_delay > remaining {
                        bail!("GitHub merge retry delay exceeds the approved wait deadline");
                    }
                    sleep_cancellable(cancelled, remote_delay)?;
                    delay = delay.saturating_mul(2).min(MERGE_POLL_MAX);
                    continue;
                }
            };
            if !gate_ready {
                if Instant::now() >= deadline {
                    bail!("GitHub Pull Request was not mergeable before the approved deadline");
                }
                sleep_cancellable(
                    cancelled,
                    delay.min(deadline.saturating_duration_since(Instant::now())),
                )?;
                delay = delay.saturating_mul(2).min(MERGE_POLL_MAX);
                continue;
            }
            let (_, _, publication) = self.gate_snapshot()?;
            let token = self.token(
                &binding,
                GitHubAppRole::Architect,
                InstallationOperation::Merge,
            )?;
            let publisher = GitHubPublisher::new(&self.client, &publication.context)?;
            ensure_not_cancelled(cancelled)?;
            match publisher.merge_exact_head(
                request.pr_number,
                &request.final_head_sha,
                &publication.pr_title,
                &publication.pr_publication,
                binding.developer_app.bot_user_id,
                binding.architect_app.bot_user_id,
                &token,
            ) {
                Ok(merged) => {
                    let merge_sha = merged
                        .merge_commit_sha
                        .clone()
                        .ok_or_else(|| anyhow!("confirmed GitHub merge omitted the merge SHA"))?;
                    let actor = merged
                        .merged_by
                        .as_ref()
                        .ok_or_else(|| anyhow!("confirmed GitHub merge omitted its actor"))?;
                    let actor_bot_user_id = actor.id;
                    let merge_url = merged.html_url.clone();
                    let mut state = self.state()?;
                    let partial = GitHubPartialOperation::Merge(GitHubPartialMerge {
                        pr_number: request.pr_number,
                        final_head_sha: request.final_head_sha.clone(),
                        merge_sha: merge_sha.clone(),
                        merge_url: merge_url.clone(),
                        actor_bot_user_id,
                    });
                    if state
                        .partial_operations
                        .insert(request.operation_id.clone(), partial)
                        .is_some()
                    {
                        bail!("GitHub merge operation already has partial publication state");
                    }
                    state
                        .evidence
                        .as_mut()
                        .ok_or_else(|| anyhow!("GitHub merge evidence writer is missing"))?
                        .write(
                            EvidencePath::Merge,
                            &Self::record(
                                &binding,
                                EvidenceKind::Merge,
                                &request.operation_id,
                                Some(&binding.architect_app),
                                Some(merged.id),
                                Some(merged.html_url.clone()),
                                Some(merged.base.sha.clone()),
                                Some(request.final_head_sha.clone()),
                                Some(merge_sha.clone()),
                                None,
                                "merged",
                                false,
                            ),
                        )?;
                    state
                        .publication
                        .as_mut()
                        .ok_or_else(|| anyhow!("GitHub merge lost Pull Request state"))?
                        .merge_sha = Some(merge_sha.clone());
                    state.partial_operations.remove(&request.operation_id);
                    return Ok(PullRequestMergedObservation {
                        operation_id: request.operation_id.clone(),
                        pr_number: request.pr_number,
                        final_head_sha: request.final_head_sha.clone(),
                        merge_sha,
                        merge_url,
                        actor_bot_user_id,
                        merge_evidence_durable: true,
                    });
                }
                Err(PublicationError::RetrySafe {
                    retry_after_seconds,
                    rate_limit_reset_unix,
                    ..
                }) => {
                    let remote_delay = retry_delay(retry_after_seconds, rate_limit_reset_unix)?;
                    let effective_delay = remote_delay.unwrap_or(delay);
                    if Instant::now() >= deadline {
                        bail!("GitHub Pull Request was not mergeable before the approved deadline");
                    }
                    sleep_cancellable(
                        cancelled,
                        effective_delay.min(deadline.saturating_duration_since(Instant::now())),
                    )?;
                    delay = delay.saturating_mul(2).min(MERGE_POLL_MAX);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn finalize_run(
        &self,
        request: &FinalizeGitHubRunRequest,
    ) -> Result<GitHubRunFinalizedObservation> {
        self.require_active_run(&request.run_id)?;
        let binding = self.binding()?;
        let (prepared, merge_sha) =
            {
                let state = self.state()?;
                let publication = state
                    .publication
                    .as_ref()
                    .ok_or_else(|| anyhow!("GitHub finalization lacks Pull Request state"))?;
                (
                    state.prepared.clone().ok_or_else(|| {
                        anyhow!("GitHub finalization lacks a prepared repository")
                    })?,
                    publication.merge_sha.clone().ok_or_else(|| {
                        anyhow!("GitHub finalization lacks durable merge evidence")
                    })?,
                )
            };
        if merge_sha != request.merge_sha {
            bail!("GitHub finalization merge SHA differs from durable evidence");
        }
        let authorization = FinalizationAuthorization::after_confirmed_merge(
            &request.final_head_sha,
            &request.final_head_sha,
            &request.merge_sha,
            true,
            true,
            0,
        )?;
        let token = self.token(
            &binding,
            GitHubAppRole::Architect,
            InstallationOperation::RemoteRefCleanup,
        )?;
        let credential = token.git_credential()?;
        let (outcome, progress) = self.git.finalize_success_with_progress(
            &prepared,
            &binding,
            &authorization,
            Some(&credential),
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if progress.has_confirmed_cleanup() {
                    let partial = GitHubPartialOperation::Finalization(GitHubPartialFinalization {
                        pr_number: request.pr_number,
                        final_head_sha: request.final_head_sha.clone(),
                        merge_sha: request.merge_sha.clone(),
                        finalization: partial_finalization_snapshot(&progress),
                    });
                    let mut state = self.state()?;
                    if state
                        .partial_operations
                        .insert(request.operation_id.clone(), partial)
                        .is_some()
                    {
                        bail!("GitHub finalization operation already has partial cleanup state");
                    }
                }
                return Err(error);
            }
        };
        let finalization = GitHubFinalizationSnapshot {
            local_worktree_removed: outcome.local_worktree_removed,
            local_ref_removed: outcome.local_refs_removed,
            remote_ref_outcome: Some(
                remote_ref_finalization_outcome_name(outcome.remote_ref_outcome).into(),
            ),
        };
        let mut state = self.state()?;
        let partial = GitHubPartialOperation::Finalization(GitHubPartialFinalization {
            pr_number: request.pr_number,
            final_head_sha: request.final_head_sha.clone(),
            merge_sha: request.merge_sha.clone(),
            finalization: finalization.clone(),
        });
        if state
            .partial_operations
            .insert(request.operation_id.clone(), partial)
            .is_some()
        {
            bail!("GitHub finalization operation already has partial cleanup state");
        }
        state
            .evidence
            .as_mut()
            .ok_or_else(|| anyhow!("GitHub finalization evidence writer is missing"))?
            .write(
                EvidencePath::Finalization,
                &Self::record(
                    &binding,
                    EvidenceKind::Finalization,
                    &request.operation_id,
                    Some(&binding.architect_app),
                    None,
                    None,
                    Some(prepared.base_sha().into()),
                    Some(request.final_head_sha.clone()),
                    Some(request.merge_sha.clone()),
                    None,
                    "finalized",
                    false,
                ),
            )?;
        state.partial_operations.remove(&request.operation_id);
        Ok(GitHubRunFinalizedObservation {
            operation_id: request.operation_id.clone(),
            pr_number: request.pr_number,
            final_head_sha: request.final_head_sha.clone(),
            merge_sha: request.merge_sha.clone(),
            finalization,
        })
    }

    fn publish_terminal_best_effort(&self, request: &PublishGitHubTerminalRequest) -> Result<()> {
        let binding = self.binding()?;
        let publication = self
            .state()?
            .publication
            .clone()
            .ok_or_else(|| anyhow!("GitHub terminal publication has no Pull Request"))?;
        self.require_active_run(&request.run_id)?;
        if publication.context.run_id != request.run_id
            || publication.pr.number != request.pr_number
        {
            bail!("GitHub terminal publication targets another Pull Request");
        }
        let publisher = GitHubPublisher::new(&self.client, &publication.context)?;
        if request.outcome == "cancelled"
            && let Some(check_id) = request.check_run_id
            && let Some(previous) = publication.checks.get(&check_id)
            && !previous.completed
        {
            let (output, marker) = render_check_output(
                &publication.context,
                &previous.task,
                &previous.outcomes,
                Some(CheckConclusion::Cancelled),
            )?;
            let token = self.token(
                &binding,
                GitHubAppRole::Architect,
                InstallationOperation::CheckPublish,
            )?;
            let check = retry_safe_mutation(|| {
                publisher.conclude_check(
                    check_id,
                    &previous.external_id,
                    CheckConclusion::Cancelled,
                    &previous.output,
                    &previous.marker,
                    &output,
                    &marker,
                    binding.architect_app.app_id,
                    &token,
                )
            })?;
            let operation_id = format!(
                "terminal-check-{}-{}",
                previous.task.ordinal, previous.task.generation
            );
            let mut state = self.state()?;
            state
                .evidence
                .as_mut()
                .ok_or_else(|| anyhow!("GitHub terminal Check evidence writer is missing"))?
                .write(
                    EvidencePath::Check {
                        ordinal: previous.task.ordinal,
                        task_key: previous.task.task_key.clone(),
                        generation: previous.task.generation,
                    },
                    &Self::record(
                        &binding,
                        EvidenceKind::Check,
                        &operation_id,
                        Some(&binding.architect_app),
                        Some(check.id),
                        Some(check.html_url.clone()),
                        Some(previous.task.task_base_sha.clone()),
                        Some(previous.task.head_sha.clone()),
                        None,
                        Some(marker.artifact_sha256.clone()),
                        "cancelled",
                        false,
                    ),
                )?;
            if let Some(stored) = state
                .publication
                .as_mut()
                .and_then(|publication| publication.checks.get_mut(&check_id))
            {
                stored.output = output;
                stored.marker = marker;
                stored.observation = check;
                stored.completed = true;
                stored.conclusion = Some(CheckConclusion::Cancelled);
            }
        }
        let terminal = render_terminal_comment(
            &publication.context,
            &request.head_sha,
            &request.outcome,
            &request.detail,
        )?;
        let token = self.token(
            &binding,
            GitHubAppRole::Architect,
            InstallationOperation::TerminalComment,
        )?;
        let _ = retry_safe_mutation(|| {
            publisher.create_comment(
                request.pr_number,
                &terminal,
                binding.architect_app.bot_user_id,
                &token,
            )
        })?;
        Ok(())
    }
}

fn unix_time(now: SystemTime) -> Result<i64> {
    i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
            .as_secs(),
    )
    .map_err(|_| anyhow!("system clock exceeds the supported Unix timestamp range"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("GitHub ruleset response omitted a positive {field}"))
}

fn canonical_rules_attestation(
    binding: &GitHubPullRequestBinding,
    branch_rules: &[serde_json::Value],
    rulesets: &[serde_json::Value],
) -> Result<String> {
    #[derive(Serialize)]
    struct CanonicalRule<'a> {
        ruleset_id: u64,
        kind: &'a str,
        strict_required_status_checks_policy: Option<bool>,
        required_check_context: Option<&'a str>,
        required_check_integration_id: Option<u64>,
    }
    #[derive(Serialize)]
    struct CanonicalRuleset<'a> {
        id: u64,
        source_type: &'a str,
        source: &'a str,
        target: &'a str,
        enforcement: &'a str,
    }
    #[derive(Serialize)]
    struct Attestation<'a> {
        repository_id: u64,
        branch: &'a str,
        architect_app_id: u64,
        rules: Vec<CanonicalRule<'a>>,
        rulesets: Vec<CanonicalRuleset<'a>>,
    }

    let active_app_ids = std::iter::once(binding.architect_app.app_id)
        .chain(std::iter::once(binding.developer_app.app_id))
        .chain(
            binding
                .reviewer_apps
                .iter()
                .map(|reviewer| reviewer.app.app_id),
        )
        .collect::<BTreeSet<_>>();
    let mut canonical_rulesets = Vec::new();
    for ruleset in rulesets {
        let id = required_u64(ruleset, "id")?;
        let source_type = ruleset
            .get("source_type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("GitHub ruleset omitted source_type"))?;
        let source = ruleset
            .get("source")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("GitHub ruleset omitted source"))?;
        let target = ruleset
            .get("target")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("GitHub ruleset omitted target"))?;
        let enforcement = ruleset
            .get("enforcement")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("GitHub ruleset omitted enforcement"))?;
        if target != "branch" || enforcement != "active" {
            bail!("GitHub critical ruleset is not an active branch ruleset");
        }
        let bypass = ruleset
            .get("bypass_actors")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow!("GitHub ruleset omitted its bypass actor set"))?;
        for actor in bypass {
            if actor.get("actor_type").and_then(serde_json::Value::as_str) == Some("Integration")
                && actor
                    .get("actor_id")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|id| active_app_ids.contains(&id))
            {
                bail!("an active hcom GitHub App has ruleset bypass authority");
            }
        }
        canonical_rulesets.push(CanonicalRuleset {
            id,
            source_type,
            source,
            target,
            enforcement,
        });
    }
    canonical_rulesets.sort_by_key(|ruleset| ruleset.id);

    let mut saw_pull_request = false;
    let mut saw_non_fast_forward = false;
    let mut saw_deletion = false;
    let mut saw_required_check = false;
    let mut critical_ruleset_ids = BTreeSet::new();
    let mut canonical_rules = Vec::new();
    for rule in branch_rules {
        let ruleset_id = required_u64(rule, "ruleset_id")?;
        if !canonical_rulesets
            .iter()
            .any(|ruleset| ruleset.id == ruleset_id)
        {
            bail!("GitHub branch rule references an unattested ruleset");
        }
        let kind = rule
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("GitHub branch rule omitted its type"))?;
        let parameters = rule.get("parameters");
        match kind {
            "pull_request" => {
                saw_pull_request = true;
                critical_ruleset_ids.insert(ruleset_id);
                canonical_rules.push(CanonicalRule {
                    ruleset_id,
                    kind,
                    strict_required_status_checks_policy: None,
                    required_check_context: None,
                    required_check_integration_id: None,
                });
            }
            "non_fast_forward" => {
                saw_non_fast_forward = true;
                critical_ruleset_ids.insert(ruleset_id);
                canonical_rules.push(CanonicalRule {
                    ruleset_id,
                    kind,
                    strict_required_status_checks_policy: None,
                    required_check_context: None,
                    required_check_integration_id: None,
                });
            }
            "deletion" => {
                saw_deletion = true;
                critical_ruleset_ids.insert(ruleset_id);
                canonical_rules.push(CanonicalRule {
                    ruleset_id,
                    kind,
                    strict_required_status_checks_policy: None,
                    required_check_context: None,
                    required_check_integration_id: None,
                });
            }
            "required_status_checks" => {
                let strict = parameters
                    .and_then(|parameters| parameters.get("strict_required_status_checks_policy"))
                    .and_then(serde_json::Value::as_bool)
                    == Some(true);
                let exact_check = parameters
                    .and_then(|parameters| parameters.get("required_status_checks"))
                    .and_then(serde_json::Value::as_array)
                    .and_then(|checks| {
                        checks.iter().find(|check| {
                            check.get("context").and_then(serde_json::Value::as_str)
                                == Some(binding.review_check_name.as_str())
                                && check
                                    .get("integration_id")
                                    .and_then(serde_json::Value::as_u64)
                                    == Some(binding.architect_app.app_id)
                        })
                    });
                if strict && exact_check.is_some() {
                    saw_required_check = true;
                    critical_ruleset_ids.insert(ruleset_id);
                    canonical_rules.push(CanonicalRule {
                        ruleset_id,
                        kind,
                        strict_required_status_checks_policy: Some(true),
                        required_check_context: Some(binding.review_check_name.as_str()),
                        required_check_integration_id: Some(binding.architect_app.app_id),
                    });
                }
            }
            _ => {}
        }
    }
    canonical_rules
        .sort_by(|left, right| (left.ruleset_id, left.kind).cmp(&(right.ruleset_id, right.kind)));
    if !saw_pull_request || !saw_non_fast_forward || !saw_deletion || !saw_required_check {
        bail!(
            "GitHub base rules lack PR-only, strict expected-source Check, force-push, or deletion protection"
        );
    }
    canonical_rulesets.retain(|ruleset| critical_ruleset_ids.contains(&ruleset.id));
    let bytes = serde_json::to_vec(&Attestation {
        repository_id: binding.repository_id,
        branch: &binding.base_branch,
        architect_app_id: binding.architect_app.app_id,
        rules: canonical_rules,
        rulesets: canonical_rulesets,
    })?;
    Ok(sha256_hex(&bytes))
}

fn read_native_final(path: &str) -> Result<String> {
    let path = Path::new(path);
    if !path.is_absolute() {
        bail!("GitHub native-final path is not absolute");
    }
    let metadata = std::fs::symlink_metadata(path)
        .context("failed to inspect the GitHub native-final artifact")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("GitHub native-final artifact is not a regular file");
    }
    let mut bytes = Vec::new();
    File::open(path)
        .context("failed to open the GitHub native-final artifact")?
        .take((super::publication::MAX_GITHUB_BODY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > super::publication::MAX_GITHUB_BODY_BYTES {
        bail!("GitHub native-final artifact is empty or exceeds the publication bound");
    }
    String::from_utf8(bytes).map_err(|_| anyhow!("GitHub native-final artifact is not UTF-8"))
}

fn check_external_id(request: &PublishDeveloperCandidateRequest, head_sha: &str) -> String {
    format!(
        "{}:task-{}:generation-{}:{}",
        request.run_id,
        request.task_ordinal + 1,
        request.generation,
        &head_sha[..12]
    )
}

fn reviewer_role(reviewer: ReviewerId) -> GitHubAppRole {
    match reviewer {
        ReviewerId::Reviewer1 => GitHubAppRole::Reviewer1,
        ReviewerId::Reviewer2 => GitHubAppRole::Reviewer2,
    }
}

fn map_check_conclusion(value: GitHubReviewCheckConclusion) -> CheckConclusion {
    match value {
        GitHubReviewCheckConclusion::Success => CheckConclusion::Success,
        GitHubReviewCheckConclusion::ActionRequired => CheckConclusion::ActionRequired,
        GitHubReviewCheckConclusion::Cancelled => CheckConclusion::Cancelled,
    }
}

fn validate_all_lgtm_chain(
    tasks: &[GitHubTaskOutcomeEvidence],
    final_head_sha: &str,
) -> Result<()> {
    validate_terminal_task_chain(tasks, final_head_sha)?;
    if tasks
        .iter()
        .any(|task| task.outcome != Some(TaskCompletionOutcome::Lgtm))
    {
        bail!("GitHub merge gate requires ordered all-LGTM task evidence");
    }
    Ok(())
}

fn validate_terminal_task_chain(
    tasks: &[GitHubTaskOutcomeEvidence],
    final_head_sha: &str,
) -> Result<()> {
    if tasks.is_empty() {
        bail!("GitHub terminal audit has no task evidence");
    }
    let mut previous_head: Option<&str> = None;
    for (ordinal, task) in tasks.iter().enumerate() {
        if task.task_ordinal != ordinal || task.outcome.is_none() {
            bail!("GitHub terminal audit requires ordered completed task evidence");
        }
        if let Some(previous) = previous_head
            && task.task_base_sha != previous
        {
            bail!("GitHub task commit ranges are not one contiguous chain");
        }
        let head = task
            .task_final_head_sha
            .as_deref()
            .ok_or_else(|| anyhow!("GitHub completed task lacks a final head"))?;
        validate_git_sha("GitHub task final head", head)?;
        previous_head = Some(head);
    }
    if previous_head != Some(final_head_sha) {
        bail!("GitHub terminal task chain does not end at the exact final head");
    }
    Ok(())
}

fn validate_open_pr(
    binding: &GitHubPullRequestBinding,
    stored: &PublicationState,
    remote: &PullRequestObservation,
    final_head_sha: &str,
) -> Result<()> {
    if remote.number != stored.pr.number
        || remote.user.id != binding.developer_app.bot_user_id
        || remote.title != stored.pr_title
        || remote.body.as_deref() != Some(stored.pr_publication.body.as_str())
        || remote.state != "open"
        || remote.draft
        || remote.merged
        || remote.head.ref_name != stored.context.branch
        || remote.head.sha != final_head_sha
        || remote.base.ref_name != binding.base_branch
        || remote.base.sha != stored.context.base_sha
    {
        bail!("GitHub Pull Request was closed, merged, edited, or remotely rebound");
    }
    Ok(())
}

fn validate_remote_comments(
    provider: &ProductionGitHubProvider,
    binding: &GitHubPullRequestBinding,
    publication: &PublicationState,
    pr_number: u64,
) -> Result<()> {
    let token = provider.token(
        binding,
        GitHubAppRole::Developer,
        InstallationOperation::DeveloperCommentRead,
    )?;
    let values = provider.client.paginated_values(
        |page| RestEndpoint::ListIssueComments {
            owner: binding.owner.clone(),
            repository: binding.repository.clone(),
            number: pr_number,
            page,
        },
        GitHubAuthentication::Installation(&token),
        None,
    )?;
    let remote = values
        .into_iter()
        .map(serde_json::from_value::<CommentObservation>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| anyhow!("GitHub comment history response is invalid"))?;

    for observed in &remote {
        for line in observed
            .body
            .lines()
            .filter(|line| line.starts_with("<!-- hcom:v1 "))
        {
            let marker = PublicationMarker::parse_exact_line(line)
                .map_err(|_| anyhow!("a GitHub comment contains a malformed hcom marker"))?;
            if marker.run_id != publication.context.run_id
                || marker.lane != "developer"
                || !matches!(
                    marker.kind,
                    PublicationKind::TaskStarted | PublicationKind::Correction
                )
            {
                continue;
            }
            let expected = publication
                .comments
                .iter()
                .find(|expected| expected.publication.marker == marker)
                .ok_or_else(|| anyhow!("an unrecognized hcom-owned comment appeared remotely"))?;
            if observed.id != expected.observation.id
                || observed.user.id != binding.developer_app.bot_user_id
                || observed.html_url != expected.observation.html_url
                || observed.body != expected.publication.body
            {
                bail!("an hcom-owned GitHub comment was duplicated, edited, or rebound");
            }
        }
    }
    for expected in &publication.comments {
        if remote
            .iter()
            .filter(|observed| {
                observed.id == expected.observation.id
                    && observed.user.id == binding.developer_app.bot_user_id
                    && observed.html_url == expected.observation.html_url
                    && observed.body == expected.publication.body
            })
            .count()
            != 1
        {
            bail!("an hcom-owned GitHub comment was deleted or edited");
        }
    }
    Ok(())
}

fn validate_remote_checks(
    provider: &ProductionGitHubProvider,
    binding: &GitHubPullRequestBinding,
    publication: &PublicationState,
    require_all_completed: bool,
) -> Result<()> {
    let check_token = provider.token(
        binding,
        GitHubAppRole::Architect,
        InstallationOperation::CheckRead,
    )?;
    for (check_run_id, stored_check) in &publication.checks {
        if require_all_completed && !stored_check.completed {
            bail!("an hcom-owned GitHub Check remained in progress");
        }
        let (status, conclusion) = if stored_check.completed {
            (
                "completed",
                Some(stored_check.conclusion.ok_or_else(|| {
                    anyhow!("a completed hcom-owned GitHub Check lacks a conclusion")
                })?),
            )
        } else {
            if stored_check.conclusion.is_some() {
                bail!("an in-progress hcom-owned GitHub Check has a conclusion");
            }
            ("in_progress", None)
        };
        let values = provider.client.paginated_values(
            |page| RestEndpoint::ListCheckRuns {
                owner: binding.owner.clone(),
                repository: binding.repository.clone(),
                head_sha: stored_check.task.head_sha.clone(),
                page,
            },
            GitHubAuthentication::Installation(&check_token),
            Some("check_runs"),
        )?;
        let remote_checks = values
            .into_iter()
            .map(serde_json::from_value::<CheckRunObservation>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| anyhow!("GitHub Check history response is invalid"))?;
        if !matches!(
            reconcile_check(
                remote_checks,
                binding.architect_app.app_id,
                Some(*check_run_id),
                &stored_check.external_id,
                status,
                conclusion,
                &stored_check.marker,
                &stored_check.output,
            ),
            super::publication::Reconciliation::Exactly(_)
        ) {
            bail!("an hcom-owned GitHub Check was edited, deleted, or rebound");
        }
    }
    Ok(())
}

fn validate_remote_reviews(
    provider: &ProductionGitHubProvider,
    binding: &GitHubPullRequestBinding,
    publication: &PublicationState,
    pr_number: u64,
    tasks: &[GitHubTaskOutcomeEvidence],
) -> Result<()> {
    for task in tasks {
        let Some(task_head) = task.task_final_head_sha.as_deref() else {
            bail!("GitHub final review evidence lacks a task head");
        };
        let verdicts_match_outcome = match task.outcome {
            Some(TaskCompletionOutcome::Lgtm) => task
                .reviews
                .iter()
                .all(|review| review.verdict == ReviewerVerdict::Lgtm),
            Some(TaskCompletionOutcome::ReviewExhausted) => task
                .reviews
                .iter()
                .any(|review| review.verdict == ReviewerVerdict::RequestChanges),
            None => false,
        };
        if !verdicts_match_outcome
            || task.reviews.len() != binding.reviewer_apps.len()
            || binding.reviewer_apps.iter().any(|reviewer| {
                !task.reviews.iter().any(|review| {
                    review.reviewer_id == reviewer.reviewer_id && review.head_sha == task_head
                })
            })
        {
            bail!("GitHub final review evidence does not contain every active Reviewer");
        }
        for expected in &task.reviews {
            if !publication.reviews.iter().any(|stored| {
                stored.task_ordinal == task.task_ordinal
                    && stored.reviewer_id == expected.reviewer_id
                    && stored.generation == expected.generation
                    && stored.head_sha == expected.head_sha
                    && stored.verdict == expected.verdict
                    && stored.observation.id == expected.review_id
                    && stored.observation.html_url == expected.review_url
                    && stored.publication.marker.artifact_sha256 == expected.final_artifact_sha256
            }) {
                bail!("GitHub final review evidence was not published by this run");
            }
        }
    }
    for reviewer in &binding.reviewer_apps {
        let role = reviewer_role(reviewer.reviewer_id);
        let token = provider.token(binding, role, InstallationOperation::ReviewRead)?;
        let values = provider.client.paginated_values(
            |page| RestEndpoint::ListReviews {
                owner: binding.owner.clone(),
                repository: binding.repository.clone(),
                number: pr_number,
                page,
            },
            GitHubAuthentication::Installation(&token),
            None,
        )?;
        let remote = values
            .into_iter()
            .map(serde_json::from_value::<ReviewObservation>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| anyhow!("GitHub review history response is invalid"))?;
        for observed in &remote {
            for line in observed
                .body
                .lines()
                .filter(|line| line.starts_with("<!-- hcom:v1 "))
            {
                let marker = PublicationMarker::parse_exact_line(line)
                    .map_err(|_| anyhow!("a GitHub review contains a malformed hcom marker"))?;
                if marker.run_id != publication.context.run_id
                    || marker.kind != PublicationKind::Review
                    || marker.lane != reviewer.reviewer_id.as_str()
                {
                    continue;
                }
                let expected = publication
                    .reviews
                    .iter()
                    .find(|expected| expected.publication.marker == marker)
                    .ok_or_else(|| {
                        anyhow!("an unrecognized hcom-owned review appeared remotely")
                    })?;
                if observed.id != expected.observation.id
                    || observed.user.id != reviewer.app.bot_user_id
                    || observed.html_url != expected.observation.html_url
                    || observed.body != expected.publication.body
                {
                    bail!("an hcom-owned GitHub review was duplicated, edited, or rebound");
                }
            }
        }
        for expected in publication
            .reviews
            .iter()
            .filter(|review| review.reviewer_id == reviewer.reviewer_id)
        {
            let observed = remote
                .iter()
                .find(|review| review.id == expected.observation.id)
                .ok_or_else(|| anyhow!("an hcom-owned GitHub review was deleted"))?;
            let retained_final = tasks
                .get(expected.task_ordinal)
                .and_then(|task| {
                    task.reviews.iter().find(|review| {
                        review.reviewer_id == expected.reviewer_id
                            && review.review_id == expected.observation.id
                            && review.generation == expected.generation
                            && review.head_sha == expected.head_sha
                            && review.verdict == expected.verdict
                    })
                })
                .is_some();
            let later_hcom_task_appended = expected.task_ordinal < tasks.len().saturating_sub(1);
            let state_allowed = review_state_allowed(
                retained_final,
                later_hcom_task_appended,
                expected.verdict,
                &observed.state,
            );
            if observed.user.id != reviewer.app.bot_user_id
                || observed.body != expected.publication.body
                || observed.commit_id != expected.observation.commit_id
                || !state_allowed
            {
                bail!("an hcom-owned GitHub review was edited or remotely rebound");
            }
        }
    }
    Ok(())
}

fn review_state_allowed(
    retained_final: bool,
    later_hcom_task_appended: bool,
    verdict: ReviewerVerdict,
    observed_state: &str,
) -> bool {
    if retained_final && !later_hcom_task_appended {
        return match verdict {
            ReviewerVerdict::Lgtm => observed_state == "APPROVED",
            ReviewerVerdict::RequestChanges => observed_state == "CHANGES_REQUESTED",
        };
    }
    match verdict {
        ReviewerVerdict::Lgtm => matches!(observed_state, "APPROVED" | "DISMISSED"),
        ReviewerVerdict::RequestChanges => {
            matches!(observed_state, "CHANGES_REQUESTED" | "DISMISSED")
        }
    }
}

fn ensure_not_cancelled(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Err(GitHubWorkflowCancelled.into());
    }
    Ok(())
}

fn sleep_cancellable(cancelled: &AtomicBool, duration: Duration) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(duration)
        .ok_or_else(|| anyhow!("GitHub retry delay overflow"))?;
    while Instant::now() < deadline {
        ensure_not_cancelled(cancelled)?;
        std::thread::sleep(CANCEL_POLL.min(deadline.saturating_duration_since(Instant::now())));
    }
    ensure_not_cancelled(cancelled)
}

fn retry_delay(
    retry_after_seconds: Option<u64>,
    rate_limit_reset_unix: Option<u64>,
) -> Result<Option<Duration>> {
    if let Some(seconds) = retry_after_seconds {
        return Ok(Some(Duration::from_secs(seconds)));
    }
    let Some(reset) = rate_limit_reset_unix else {
        return Ok(None);
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
        .as_secs();
    Ok(Some(Duration::from_secs(reset.saturating_sub(now))))
}

fn retryable_read_delay(error: &anyhow::Error, fallback: Duration) -> Result<Option<Duration>> {
    let Some(api) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<GitHubApiError>())
    else {
        return Ok(None);
    };
    if !api.is_retryable_read() {
        return Ok(None);
    }
    let (retry_after_seconds, rate_limit_reset_unix) = api.rate_limit_signals();
    Ok(Some(
        retry_delay(retry_after_seconds, rate_limit_reset_unix)?.unwrap_or(fallback),
    ))
}

fn retry_safe_mutation<T>(
    operation: impl FnMut() -> std::result::Result<T, PublicationError>,
) -> Result<T> {
    retry_safe_mutation_with_policy(
        operation,
        MUTATION_RETRY_WINDOW,
        MUTATION_RETRY_INITIAL,
        MUTATION_RETRY_MAX,
        Instant::now,
        std::thread::sleep,
    )
}

fn retry_safe_mutation_with_policy<T>(
    mut operation: impl FnMut() -> std::result::Result<T, PublicationError>,
    retry_window: Duration,
    initial_delay: Duration,
    max_delay: Duration,
    mut now: impl FnMut() -> Instant,
    mut sleep: impl FnMut(Duration),
) -> Result<T> {
    if initial_delay.is_zero() || max_delay < initial_delay {
        bail!("GitHub mutation retry policy is invalid");
    }
    let deadline = now()
        .checked_add(retry_window)
        .ok_or_else(|| anyhow!("GitHub mutation retry deadline overflow"))?;
    let mut fallback = initial_delay;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                let (retry_after_seconds, rate_limit_reset_unix) = match &error {
                    PublicationError::RetrySafe {
                        retry_after_seconds,
                        rate_limit_reset_unix,
                        ..
                    } => (*retry_after_seconds, *rate_limit_reset_unix),
                    _ => return Err(error.into()),
                };
                // Remote timing is a lower bound. Keeping the local exponential
                // floor prevents a zero-valued signal from turning this bounded
                // phase into a tight request loop.
                let delay = retry_delay(retry_after_seconds, rate_limit_reset_unix)?
                    .map_or(fallback, |remote| remote.max(fallback));
                let remaining = deadline.saturating_duration_since(now());
                if delay > remaining {
                    return Err(error.into());
                }
                sleep(delay);
                fallback = fallback.saturating_mul(2).min(max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::GitHubReviewerAppBinding;

    #[test]
    fn confirmed_zero_effect_mutations_use_the_window_beyond_three_attempts() {
        let clock = std::cell::Cell::new(Instant::now());
        let sleeps = std::cell::RefCell::new(Vec::new());
        let mut attempts = 0;
        let result = retry_safe_mutation_with_policy(
            || {
                attempts += 1;
                if attempts < 5 {
                    Err(PublicationError::RetrySafe {
                        reason: "fake confirmed zero effect",
                        retry_after_seconds: Some(0),
                        rate_limit_reset_unix: None,
                        failure: None,
                    })
                } else {
                    Ok("published")
                }
            },
            MUTATION_RETRY_WINDOW,
            MUTATION_RETRY_INITIAL,
            MUTATION_RETRY_MAX,
            || clock.get(),
            |delay| {
                sleeps.borrow_mut().push(delay);
                clock.set(clock.get() + delay);
            },
        )
        .unwrap();
        assert_eq!(result, "published");
        assert_eq!(attempts, 5);
        assert_eq!(
            *sleeps.borrow(),
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
            ]
        );
    }

    #[test]
    fn mutation_retry_window_returns_the_last_retry_safe_error() {
        let started = Instant::now();
        let clock = std::cell::Cell::new(started);
        let mut attempts = 0;
        let error = retry_safe_mutation_with_policy(
            || {
                attempts += 1;
                Err::<(), _>(PublicationError::RetrySafe {
                    reason: "last confirmed zero effect",
                    retry_after_seconds: None,
                    rate_limit_reset_unix: None,
                    failure: None,
                })
            },
            MUTATION_RETRY_WINDOW,
            MUTATION_RETRY_INITIAL,
            MUTATION_RETRY_MAX,
            || clock.get(),
            |delay| clock.set(clock.get() + delay),
        )
        .unwrap_err();
        assert_eq!(attempts, 8);
        assert_eq!(clock.get().duration_since(started), Duration::from_secs(91));
        assert!(error.to_string().contains("last confirmed zero effect"));
    }

    #[test]
    fn only_a_later_hcom_task_append_may_dismiss_a_retained_final_approval() {
        assert!(review_state_allowed(
            true,
            true,
            ReviewerVerdict::Lgtm,
            "DISMISSED"
        ));
        assert!(!review_state_allowed(
            true,
            false,
            ReviewerVerdict::Lgtm,
            "DISMISSED"
        ));
        assert!(review_state_allowed(
            false,
            false,
            ReviewerVerdict::RequestChanges,
            "DISMISSED"
        ));
        assert!(review_state_allowed(
            true,
            false,
            ReviewerVerdict::RequestChanges,
            "CHANGES_REQUESTED"
        ));
        assert!(!review_state_allowed(
            true,
            false,
            ReviewerVerdict::RequestChanges,
            "APPROVED"
        ));
    }

    #[test]
    fn terminal_manual_check_propagates_remote_mutation_but_correction_skips_audit() {
        use crate::control_api::GitHubDeliveryPolicy;

        let mut final_manual_called = false;
        let error = validate_manual_terminal_audit_for_policy(
            GitHubDeliveryPolicy::Manual,
            1,
            2,
            Some(TaskCompletionOutcome::Lgtm),
            || {
                final_manual_called = true;
                bail!("fake closed or edited Pull Request")
            },
        )
        .unwrap_err();
        assert!(final_manual_called);
        assert_eq!(error.to_string(), "fake closed or edited Pull Request");

        let mut nonterminal_manual_called = false;
        validate_manual_terminal_audit_for_policy(
            GitHubDeliveryPolicy::Manual,
            0,
            2,
            Some(TaskCompletionOutcome::Lgtm),
            || {
                nonterminal_manual_called = true;
                bail!("nonterminal audit must be skipped")
            },
        )
        .unwrap();
        assert!(!nonterminal_manual_called);

        let mut correction_called = false;
        validate_manual_terminal_audit_for_policy(GitHubDeliveryPolicy::Manual, 0, 1, None, || {
            correction_called = true;
            bail!("last-task correction audit must be skipped")
        })
        .unwrap();
        assert!(!correction_called);

        let mut protected_called = false;
        validate_manual_terminal_audit_for_policy(
            GitHubDeliveryPolicy::ProtectedAutoMerge,
            1,
            2,
            Some(TaskCompletionOutcome::Lgtm),
            || {
                protected_called = true;
                bail!("protected mode retains its merge-gate audit")
            },
        )
        .unwrap();
        assert!(!protected_called);
    }

    #[test]
    fn manual_terminal_chain_accepts_review_exhaustion_but_merge_chain_does_not() {
        let task = |task_ordinal: usize,
                    base: String,
                    head: String,
                    outcome: TaskCompletionOutcome| GitHubTaskOutcomeEvidence {
            task_ordinal,
            task_key: format!("task-{task_ordinal}"),
            task_title: format!("Task {task_ordinal}"),
            task_base_sha: base,
            task_final_head_sha: Some(head),
            outcome: Some(outcome),
            reviews: Vec::new(),
        };
        let first_head = "b".repeat(40);
        let final_head = "c".repeat(40);
        let tasks = vec![
            task(
                0,
                "a".repeat(40),
                first_head.clone(),
                TaskCompletionOutcome::Lgtm,
            ),
            task(
                1,
                first_head,
                final_head.clone(),
                TaskCompletionOutcome::ReviewExhausted,
            ),
        ];

        validate_terminal_task_chain(&tasks, &final_head).unwrap();
        assert!(validate_all_lgtm_chain(&tasks, &final_head).is_err());
    }

    #[test]
    fn canonical_rule_attestation_requires_every_hcom_critical_rule_and_no_app_bypass() {
        let app = |id, slug: &str| GitHubAppBinding {
            app_id: id,
            installation_id: id + 10,
            slug: slug.into(),
            bot_user_id: id + 20,
            effective_permissions: BTreeMap::from([
                ("administration".into(), GitHubPermissionLevel::Read),
                ("checks".into(), GitHubPermissionLevel::Write),
                ("contents".into(), GitHubPermissionLevel::Write),
                ("pull_requests".into(), GitHubPermissionLevel::Write),
            ]),
        };
        let binding = GitHubPullRequestBinding {
            delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
            owner: "owner".into(),
            repository: "repo".into(),
            repository_id: 99,
            visibility: "private".into(),
            local_repository_root: "/tmp/repo".into(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 60,
            delete_remote_branch_after_merge: true,
            architect_app: app(1, "arch"),
            developer_app: app(2, "dev"),
            reviewer_apps: vec![GitHubReviewerAppBinding {
                reviewer_id: ReviewerId::Reviewer1,
                app: app(3, "reviewer1"),
            }],
            review_check_name: "hcom/review".into(),
        };
        let rules = vec![
            serde_json::json!({"ruleset_id": 7, "type": "pull_request"}),
            serde_json::json!({"ruleset_id": 7, "type": "non_fast_forward"}),
            serde_json::json!({"ruleset_id": 7, "type": "deletion"}),
            serde_json::json!({
                "ruleset_id": 7,
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": true,
                    "required_status_checks": [{"context": "hcom/review", "integration_id": 1}]
                }
            }),
        ];
        let rulesets = vec![serde_json::json!({
            "id": 7,
            "source_type": "Repository",
            "source": "owner/repo",
            "target": "branch",
            "enforcement": "active",
            "bypass_actors": []
        })];
        let first = canonical_rules_attestation(&binding, &rules, &rulesets).unwrap();
        let mut reversed = rules.clone();
        reversed.reverse();
        assert_eq!(
            first,
            canonical_rules_attestation(&binding, &reversed, &rulesets).unwrap()
        );
        let mut unrelated = rules.clone();
        unrelated.push(serde_json::json!({
            "ruleset_id": 7,
            "type": "required_linear_history",
            "parameters": {"remote_ordering_noise": true}
        }));
        unrelated[3]["parameters"]["required_status_checks"] = serde_json::json!([
            {"context": "another/ci", "integration_id": 999},
            {"context": "hcom/review", "integration_id": 1}
        ]);
        assert_eq!(
            first,
            canonical_rules_attestation(&binding, &unrelated, &rulesets).unwrap()
        );
        let mut bypassed = rulesets;
        bypassed[0]["bypass_actors"] =
            serde_json::json!([{"actor_id": 2, "actor_type": "Integration"}]);
        assert!(canonical_rules_attestation(&binding, &rules, &bypassed).is_err());
        assert!(canonical_rules_attestation(&binding, &rules[..3], &bypassed).is_err());
        bypassed[0]["bypass_actors"] =
            serde_json::json!([{"actor_id": 2, "actor_type": "RepositoryRole"}]);
        assert!(canonical_rules_attestation(&binding, &rules, &bypassed).is_ok());
    }

    #[test]
    fn manual_policy_skips_a_ruleset_api_failure_while_protected_policy_requires_it() {
        use crate::control_api::GitHubDeliveryPolicy;
        let mut manual_called = false;
        let manual = ruleset_attestation_for_policy(GitHubDeliveryPolicy::Manual, || {
            manual_called = true;
            bail!("fake GitHub Free private ruleset 403")
        })
        .unwrap();
        assert_eq!(manual, None);
        assert!(!manual_called);

        let mut protected_called = false;
        let protected =
            ruleset_attestation_for_policy(GitHubDeliveryPolicy::ProtectedAutoMerge, || {
                protected_called = true;
                Ok("a".repeat(64))
            })
            .unwrap();
        assert_eq!(protected, Some("a".repeat(64)));
        assert!(protected_called);
        assert!(
            ruleset_attestation_for_policy(GitHubDeliveryPolicy::ProtectedAutoMerge, || bail!(
                "fake GitHub Free private ruleset 403"
            ))
            .is_err()
        );
    }
}
