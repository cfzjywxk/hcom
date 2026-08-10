//! Typed, I/O-free GitHub delivery contracts.
//!
//! Authentication, HTTP, and Git implementations deliberately sit behind the
//! traits in this module.  The profile/binding task can therefore validate and
//! freeze identity, permission, repository, and inspection observations
//! without giving the pure supervisor ambient filesystem or network access.

#![allow(
    dead_code,
    reason = "GITHUB-PR-01 defines seams consumed by later auth/API and driver tasks"
)]

pub(crate) mod auth;
pub(crate) mod client;
pub(crate) mod evidence;
pub(crate) mod git;
pub(crate) mod publication;
pub(crate) mod workflow;

pub(crate) use workflow::ProductionGitHubProvider;

use crate::control_api::{
    DeliveryBinding, GITHUB_REVIEW_CHECK_NAME, GitHubAppBinding, GitHubAppRole,
    GitHubInspectionBinding, GitHubPermissionLevel, GitHubPullRequestBinding,
    GitHubReviewerAppBinding,
};
use crate::orchestrator::core::{
    FinalizeGitHubRunRequest, MergePullRequestRequest, PrepareGitHubRunRequest,
    PublishDeveloperCandidateRequest, PublishGitHubTerminalRequest, PublishReviewCheckRequest,
    PublishReviewerReviewRequest, WaitForMergeGateRequest,
};
use crate::orchestrator::workspace::TasksWorkspace;
use crate::worker::profile::ReviewerId;
use anyhow::{Result, bail};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const MAX_GITHUB_SLUG_BYTES: usize = 100;
const MAX_GITHUB_BRANCH_BYTES: usize = 255;
const MAX_KEY_PATH_BYTES: usize = 4096;

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct GitHubAppConfig {
    pub(crate) app_id: u64,
    pub(crate) slug: String,
    pub(crate) private_key_file: PathBuf,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct GitHubAppsConfig {
    architect: GitHubAppConfig,
    developer: GitHubAppConfig,
    reviewer1: GitHubAppConfig,
    reviewer2: Option<GitHubAppConfig>,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct GitHubDeploymentConfig {
    pub(crate) owner: String,
    pub(crate) repository: String,
    pub(crate) local_repository_root: PathBuf,
    pub(crate) base_branch: String,
    pub(crate) merge_method: String,
    pub(crate) merge_wait_seconds: u32,
    pub(crate) delete_remote_branch_after_merge: bool,
    pub(crate) private_repository_required: bool,
    apps: GitHubAppsConfig,
}

impl GitHubDeploymentConfig {
    pub(crate) fn app(&self, role: GitHubAppRole) -> Option<&GitHubAppConfig> {
        match role {
            GitHubAppRole::Architect => Some(&self.apps.architect),
            GitHubAppRole::Developer => Some(&self.apps.developer),
            GitHubAppRole::Reviewer1 => Some(&self.apps.reviewer1),
            GitHubAppRole::Reviewer2 => self.apps.reviewer2.as_ref(),
        }
    }

    fn active_roles(&self, reviewer_ids: &[ReviewerId]) -> Vec<GitHubAppRole> {
        GitHubAppRole::for_reviewers(reviewer_ids)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GitHubAppObservation {
    pub(crate) role: GitHubAppRole,
    pub(crate) app_id: u64,
    pub(crate) installation_id: u64,
    pub(crate) slug: String,
    pub(crate) bot_user_id: u64,
    pub(crate) effective_permissions: BTreeMap<String, GitHubPermissionLevel>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GitHubPreflightObservation {
    pub(crate) owner: String,
    pub(crate) repository: String,
    pub(crate) repository_id: u64,
    pub(crate) private: bool,
    /// Complete selected-repository set returned for each role installation.
    /// V1 requires every set to contain only `repository_id`.
    pub(crate) installation_repository_ids: BTreeMap<GitHubAppRole, Vec<u64>>,
    pub(crate) apps: Vec<GitHubAppObservation>,
    pub(crate) expected_base_ref: String,
    pub(crate) expected_base_sha: String,
    /// SHA-256 of the provider-validated canonical hcom-critical rule subset
    /// and integration IDs. Raw response ordering must not affect this value;
    /// missing PR-only/strict/check-source/force-push/deletion/no-bypass rules
    /// are provider errors and must never produce an observation.
    pub(crate) ruleset_attestation_sha256: String,
    pub(crate) inspection_id: String,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GitHubInspectionResult {
    pub(crate) delivery_binding: GitHubPullRequestBinding,
    pub(crate) inspection: GitHubInspectionBinding,
}

pub(crate) struct GitHubPreflightRequest<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) session_version: u64,
    pub(crate) config: &'a GitHubDeploymentConfig,
    pub(crate) reviewer_ids: &'a [ReviewerId],
}

pub(crate) trait GitHubPreflightProvider: Send + Sync {
    /// Authenticate/open only the active configured Apps and return a complete,
    /// read-only observation. Implementations own key metadata/open/parse,
    /// token lifetime, repository scoping, and canonical rules validation.
    fn preflight(&self, request: &GitHubPreflightRequest<'_>)
    -> Result<GitHubPreflightObservation>;
}

#[derive(Clone)]
pub(crate) struct GitHubInspectionRequest {
    pub(crate) run_id: String,
    pub(crate) session_version: u64,
    pub(crate) delivery_binding: GitHubPullRequestBinding,
}

pub(crate) trait GitHubInspectionProvider: Send + Sync {
    /// Refresh the same bounded read-only repository/App/rules observation;
    /// implementations must not create refs, worktrees, PRs, Checks, comments,
    /// or any other external state.
    fn inspect(&self, request: &GitHubInspectionRequest) -> Result<GitHubInspectionResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryPreparedObservation {
    pub(crate) operation_id: String,
    pub(crate) base_sha: String,
    pub(crate) branch: String,
    pub(crate) worktree_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidatePublishedObservation {
    pub(crate) operation_id: String,
    pub(crate) task_ordinal: usize,
    pub(crate) generation: u32,
    pub(crate) previous_head_sha: String,
    pub(crate) head_sha: String,
    pub(crate) pr_number: u64,
    pub(crate) pr_node_id: String,
    pub(crate) pr_url: String,
    pub(crate) pr_actor_bot_user_id: u64,
    pub(crate) check_run_id: u64,
    pub(crate) check_url: String,
    pub(crate) check_actor_app_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReviewerReviewPublishedObservation {
    pub(crate) operation_id: String,
    pub(crate) task_ordinal: usize,
    pub(crate) reviewer_id: ReviewerId,
    pub(crate) generation: u32,
    pub(crate) head_sha: String,
    pub(crate) verdict: crate::control_api::ReviewerVerdict,
    pub(crate) review_id: u64,
    pub(crate) review_url: String,
    pub(crate) actor_bot_user_id: u64,
    pub(crate) final_artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReviewCheckPublishedObservation {
    pub(crate) operation_id: String,
    pub(crate) task_ordinal: usize,
    pub(crate) generation: u32,
    pub(crate) head_sha: String,
    pub(crate) check_run_id: u64,
    pub(crate) check_url: String,
    pub(crate) state: String,
    pub(crate) actor_app_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeGateObservation {
    pub(crate) operation_id: String,
    pub(crate) pr_number: u64,
    pub(crate) final_head_sha: String,
    pub(crate) base_sha: String,
    pub(crate) ruleset_attestation_sha256: String,
    pub(crate) check_run_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRequestMergedObservation {
    pub(crate) operation_id: String,
    pub(crate) pr_number: u64,
    pub(crate) final_head_sha: String,
    pub(crate) merge_sha: String,
    pub(crate) merge_url: String,
    pub(crate) actor_bot_user_id: u64,
    pub(crate) merge_evidence_durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GitHubRunFinalizedObservation {
    pub(crate) operation_id: String,
    pub(crate) pr_number: u64,
    pub(crate) final_head_sha: String,
    pub(crate) merge_sha: String,
    pub(crate) finalization: crate::control_api::GitHubFinalizationSnapshot,
}

/// Executes the ordered Git/GitHub effects emitted by `SupervisorCore`.
///
/// Implementations reconcile every ambiguous external result before returning
/// one normalized observation. Returning `Err` means the intended operation
/// could not be proved and must fail closed; the task lane never retries a
/// model turn to repeat publication.
pub(crate) trait GitHubWorkflowProvider: Send + Sync {
    /// Drops only run-local in-memory composition state before an explicitly
    /// requested fresh run. Durable evidence and preserved repository/remote
    /// artifacts from the terminal run are intentionally left untouched.
    fn begin_fresh_run(&self, _terminal_run_id: &str, _fresh_run_id: &str) -> Result<()> {
        Ok(())
    }

    fn prepare_repository(
        &self,
        workspace: &TasksWorkspace,
        request: &PrepareGitHubRunRequest,
    ) -> Result<RepositoryPreparedObservation>;

    fn publish_candidate(
        &self,
        request: &PublishDeveloperCandidateRequest,
    ) -> Result<CandidatePublishedObservation>;

    fn take_partial_operation(
        &self,
        _operation_id: &str,
    ) -> Result<Option<crate::orchestrator::core::GitHubPartialOperation>> {
        Ok(None)
    }

    fn publish_review(
        &self,
        request: &PublishReviewerReviewRequest,
    ) -> Result<ReviewerReviewPublishedObservation>;

    fn publish_check(
        &self,
        request: &PublishReviewCheckRequest,
    ) -> Result<ReviewCheckPublishedObservation>;

    fn wait_for_merge_gate(
        &self,
        request: &WaitForMergeGateRequest,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<MergeGateObservation>;

    fn merge_pull_request(
        &self,
        request: &MergePullRequestRequest,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<PullRequestMergedObservation>;

    fn finalize_run(
        &self,
        request: &FinalizeGitHubRunRequest,
    ) -> Result<GitHubRunFinalizedObservation>;

    fn publish_terminal_best_effort(&self, request: &PublishGitHubTerminalRequest) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct GitHubRuntimeBinding {
    pub(crate) binding: GitHubPullRequestBinding,
    pub(crate) initial_inspection: GitHubInspectionBinding,
    pub(crate) inspector: Arc<dyn GitHubInspectionProvider>,
    pub(crate) workflow: Arc<dyn GitHubWorkflowProvider>,
}

pub(crate) fn preflight_runtime<P>(
    provider: Arc<P>,
    run_id: &str,
    session_version: u64,
    config: &GitHubDeploymentConfig,
    reviewer_ids: &[ReviewerId],
) -> Result<GitHubRuntimeBinding>
where
    P: GitHubPreflightProvider + GitHubInspectionProvider + GitHubWorkflowProvider + 'static,
{
    let observation = provider.preflight(&GitHubPreflightRequest {
        run_id,
        session_version,
        config,
        reviewer_ids,
    })?;
    let frozen = freeze_preflight(config, reviewer_ids, observation)?;
    let inspector: Arc<dyn GitHubInspectionProvider> = provider.clone();
    let workflow: Arc<dyn GitHubWorkflowProvider> = provider;
    Ok(GitHubRuntimeBinding {
        binding: frozen.delivery_binding,
        initial_inspection: frozen.inspection,
        inspector,
        workflow,
    })
}

pub(crate) fn parse_github_deployment_config(
    value: toml::Value,
    reviewer_ids: &[ReviewerId],
    project_root: &Path,
) -> Result<GitHubDeploymentConfig> {
    let config: GitHubDeploymentConfig = value
        .try_into()
        .map_err(|error| anyhow::anyhow!("invalid [architect.github] configuration: {error}"))?;
    validate_github_deployment_config(&config, reviewer_ids, project_root)?;
    Ok(config)
}

pub(crate) fn validate_github_deployment_config(
    config: &GitHubDeploymentConfig,
    reviewer_ids: &[ReviewerId],
    project_root: &Path,
) -> Result<()> {
    validate_slug("GitHub owner", &config.owner)?;
    validate_slug("GitHub repository", &config.repository)?;
    if config.owner.contains('/') || config.repository.contains('/') {
        bail!("GitHub owner and repository must be slugs, not URLs or owner/repository pairs");
    }
    validate_branch(&config.base_branch)?;
    if config.merge_method != "squash" {
        bail!("GitHub merge_method must be squash");
    }
    if !config.private_repository_required {
        bail!("GitHub private_repository_required must be true");
    }
    if !(60..=86_400).contains(&config.merge_wait_seconds) {
        bail!("GitHub merge_wait_seconds must be between 60 and 86400");
    }
    validate_canonical_directory(
        "GitHub local_repository_root",
        &config.local_repository_root,
    )?;

    let expected_roles = GitHubAppRole::for_reviewers(reviewer_ids);
    if reviewer_ids == [ReviewerId::Reviewer1] && config.apps.reviewer2.is_some() {
        bail!("[architect.github.apps.reviewer2] is not allowed in single-review mode");
    }
    if reviewer_ids == [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
        && config.apps.reviewer2.is_none()
    {
        bail!("[architect.github.apps.reviewer2] is required in dual-review mode");
    }
    if !matches!(
        reviewer_ids,
        [ReviewerId::Reviewer1] | [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
    ) {
        bail!("GitHub App configuration requires the canonical active Reviewer topology");
    }

    let mut app_ids = BTreeSet::new();
    let mut slugs = BTreeSet::new();
    let mut key_paths = BTreeSet::new();
    for role in expected_roles {
        let app = config.app(role).ok_or_else(|| {
            anyhow::anyhow!("{} GitHub App configuration is missing", role.as_str())
        })?;
        if app.app_id == 0 {
            bail!("{} GitHub App ID must be positive", role.as_str());
        }
        validate_slug(&format!("{} GitHub App slug", role.as_str()), &app.slug)?;
        validate_key_path(
            &app.private_key_file,
            project_root,
            &config.local_repository_root,
        )?;
        if !app_ids.insert(app.app_id) {
            bail!("active GitHub App IDs must be unique");
        }
        if !slugs.insert(app.slug.as_str()) {
            bail!("active GitHub App slugs must be unique");
        }
        if !key_paths.insert(app.private_key_file.as_path()) {
            bail!("active GitHub Apps must use distinct private-key files");
        }
    }
    Ok(())
}

pub(crate) fn freeze_preflight(
    config: &GitHubDeploymentConfig,
    reviewer_ids: &[ReviewerId],
    observation: GitHubPreflightObservation,
) -> Result<GitHubInspectionResult> {
    if observation.owner != config.owner || observation.repository != config.repository {
        bail!("GitHub preflight repository identity differs from configuration");
    }
    if observation.repository_id == 0 || !observation.private {
        bail!("GitHub preflight requires the exact private repository");
    }
    let expected_ref = format!("refs/heads/{}", config.base_branch);
    if observation.expected_base_ref != expected_ref {
        bail!("GitHub preflight base ref differs from configuration");
    }
    validate_git_sha("GitHub preflight base SHA", &observation.expected_base_sha)?;
    validate_sha256(
        "GitHub ruleset attestation",
        &observation.ruleset_attestation_sha256,
    )?;
    validate_id("GitHub inspection ID", &observation.inspection_id)?;

    let expected_roles = config.active_roles(reviewer_ids);
    let observed_roles = observation
        .apps
        .iter()
        .map(|app| app.role)
        .collect::<Vec<_>>();
    if observed_roles != expected_roles {
        bail!("GitHub preflight App observations differ from the exact active role set");
    }
    let repository_roles = observation
        .installation_repository_ids
        .keys()
        .copied()
        .collect::<Vec<_>>();
    if repository_roles != expected_roles {
        bail!("GitHub installation repository observations omit or add an active role");
    }

    let mut app_ids = BTreeSet::new();
    let mut bot_ids = BTreeSet::new();
    let mut slugs = BTreeSet::new();
    let mut frozen_apps = BTreeMap::new();
    for app in observation.apps {
        let configured = config
            .app(app.role)
            .expect("the exact active role set was validated above");
        if app.app_id != configured.app_id || app.slug != configured.slug {
            bail!(
                "{} GitHub App identity differs from configuration",
                app.role.as_str()
            );
        }
        if app.installation_id == 0 || app.bot_user_id == 0 {
            bail!(
                "{} GitHub App installation and bot IDs must be positive",
                app.role.as_str()
            );
        }
        if observation
            .installation_repository_ids
            .get(&app.role)
            .map(Vec::as_slice)
            != Some(&[observation.repository_id])
        {
            bail!(
                "{} GitHub App installation must select only the exact repository",
                app.role.as_str()
            );
        }
        validate_effective_permissions(app.role, &app.effective_permissions)?;
        if !app_ids.insert(app.app_id)
            || !bot_ids.insert(app.bot_user_id)
            || !slugs.insert(app.slug.clone())
        {
            bail!("active GitHub App IDs, slugs, and bot user IDs must each be unique");
        }
        frozen_apps.insert(
            app.role,
            GitHubAppBinding {
                app_id: app.app_id,
                installation_id: app.installation_id,
                slug: app.slug,
                bot_user_id: app.bot_user_id,
                effective_permissions: app.effective_permissions,
            },
        );
    }

    let architect_app = frozen_apps
        .remove(&GitHubAppRole::Architect)
        .expect("Architect observation was validated");
    let developer_app = frozen_apps
        .remove(&GitHubAppRole::Developer)
        .expect("Developer observation was validated");
    let reviewer_apps = reviewer_ids
        .iter()
        .copied()
        .map(|reviewer_id| {
            let role = match reviewer_id {
                ReviewerId::Reviewer1 => GitHubAppRole::Reviewer1,
                ReviewerId::Reviewer2 => GitHubAppRole::Reviewer2,
            };
            GitHubReviewerAppBinding {
                reviewer_id,
                app: frozen_apps
                    .remove(&role)
                    .expect("Reviewer observation was validated"),
            }
        })
        .collect();
    let delivery_binding = GitHubPullRequestBinding {
        owner: config.owner.clone(),
        repository: config.repository.clone(),
        repository_id: observation.repository_id,
        visibility: "private".into(),
        local_repository_root: config.local_repository_root.to_string_lossy().into_owned(),
        base_branch: config.base_branch.clone(),
        merge_method: config.merge_method.clone(),
        merge_wait_seconds: config.merge_wait_seconds,
        delete_remote_branch_after_merge: config.delete_remote_branch_after_merge,
        architect_app,
        developer_app,
        reviewer_apps,
        review_check_name: GITHUB_REVIEW_CHECK_NAME.into(),
    };
    let inspection = GitHubInspectionBinding {
        inspected_repository_id: observation.repository_id,
        expected_base_ref: observation.expected_base_ref,
        expected_base_sha: observation.expected_base_sha,
        ruleset_attestation_sha256: observation.ruleset_attestation_sha256,
        inspection_id: observation.inspection_id,
    };
    validate_frozen_delivery_binding(&delivery_binding, reviewer_ids)?;
    validate_inspection_against_delivery(&delivery_binding, &inspection)?;
    Ok(GitHubInspectionResult {
        delivery_binding,
        inspection,
    })
}

pub(crate) fn validate_inspection_result(
    frozen: &GitHubPullRequestBinding,
    result: &GitHubInspectionResult,
) -> Result<()> {
    let reviewer_ids = frozen
        .reviewer_apps
        .iter()
        .map(|reviewer| reviewer.reviewer_id)
        .collect::<Vec<_>>();
    validate_frozen_delivery_binding(frozen, &reviewer_ids)?;
    if &result.delivery_binding != frozen {
        bail!("GitHub App identity or complete effective permission map drifted after startup");
    }
    validate_inspection_against_delivery(frozen, &result.inspection)
}

fn validate_frozen_delivery_binding(
    binding: &GitHubPullRequestBinding,
    reviewer_ids: &[ReviewerId],
) -> Result<()> {
    validate_slug("frozen GitHub owner", &binding.owner)?;
    validate_slug("frozen GitHub repository", &binding.repository)?;
    if binding.owner.contains('/')
        || binding.repository.contains('/')
        || binding.repository_id == 0
        || binding.visibility != "private"
    {
        bail!("frozen GitHub repository binding is invalid");
    }
    validate_canonical_directory(
        "frozen GitHub local_repository_root",
        Path::new(&binding.local_repository_root),
    )?;
    validate_branch(&binding.base_branch)?;
    if binding.merge_method != "squash"
        || !(60..=86_400).contains(&binding.merge_wait_seconds)
        || binding.review_check_name != GITHUB_REVIEW_CHECK_NAME
    {
        bail!("frozen GitHub delivery policy is invalid");
    }
    if !matches!(
        reviewer_ids,
        [ReviewerId::Reviewer1] | [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
    ) || binding
        .reviewer_apps
        .iter()
        .map(|reviewer| reviewer.reviewer_id)
        .ne(reviewer_ids.iter().copied())
    {
        bail!("frozen GitHub Reviewer App topology is invalid");
    }

    let apps = std::iter::once((GitHubAppRole::Architect, &binding.architect_app))
        .chain(std::iter::once((
            GitHubAppRole::Developer,
            &binding.developer_app,
        )))
        .chain(binding.reviewer_apps.iter().map(|reviewer| {
            (
                match reviewer.reviewer_id {
                    ReviewerId::Reviewer1 => GitHubAppRole::Reviewer1,
                    ReviewerId::Reviewer2 => GitHubAppRole::Reviewer2,
                },
                &reviewer.app,
            )
        }))
        .collect::<Vec<_>>();
    let expected_roles = GitHubAppRole::for_reviewers(reviewer_ids);
    if apps.iter().map(|(role, _)| *role).ne(expected_roles) {
        bail!("frozen GitHub App role set is invalid");
    }
    let mut app_ids = BTreeSet::new();
    let mut bot_ids = BTreeSet::new();
    let mut slugs = BTreeSet::new();
    for (role, app) in apps {
        if app.app_id == 0 || app.installation_id == 0 || app.bot_user_id == 0 {
            bail!(
                "frozen {} GitHub App identifiers must be positive",
                role.as_str()
            );
        }
        validate_slug(
            &format!("frozen {} GitHub App slug", role.as_str()),
            &app.slug,
        )?;
        validate_effective_permissions(role, &app.effective_permissions)?;
        if !app_ids.insert(app.app_id)
            || !bot_ids.insert(app.bot_user_id)
            || !slugs.insert(app.slug.as_str())
        {
            bail!("frozen GitHub App IDs, slugs, and bot user IDs must each be unique");
        }
    }
    Ok(())
}

pub(crate) fn delivery_binding(result: &GitHubInspectionResult) -> DeliveryBinding {
    DeliveryBinding::GitHubPullRequest {
        binding: Box::new(result.delivery_binding.clone()),
    }
}

fn validate_inspection_against_delivery(
    delivery: &GitHubPullRequestBinding,
    inspection: &GitHubInspectionBinding,
) -> Result<()> {
    if inspection.inspected_repository_id != delivery.repository_id
        || inspection.expected_base_ref != format!("refs/heads/{}", delivery.base_branch)
    {
        bail!("GitHub inspection differs from the frozen repository/base binding");
    }
    validate_git_sha("GitHub inspected base SHA", &inspection.expected_base_sha)?;
    validate_sha256(
        "GitHub inspected ruleset attestation",
        &inspection.ruleset_attestation_sha256,
    )?;
    validate_id("GitHub inspection ID", &inspection.inspection_id)
}

fn validate_effective_permissions(
    role: GitHubAppRole,
    permissions: &BTreeMap<String, GitHubPermissionLevel>,
) -> Result<()> {
    if permissions.is_empty() || permissions.len() > 64 {
        bail!(
            "{} GitHub App effective permission map is empty or unbounded",
            role.as_str()
        );
    }
    for name in permissions.keys() {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        {
            bail!(
                "{} GitHub App effective permission name is invalid",
                role.as_str()
            );
        }
    }
    for &(name, required) in required_permissions(role) {
        if !permissions
            .get(name)
            .is_some_and(|actual| actual.satisfies(required))
        {
            bail!(
                "{} GitHub App is missing required {name} permission",
                role.as_str()
            );
        }
    }
    Ok(())
}

pub(crate) fn required_permissions(
    role: GitHubAppRole,
) -> &'static [(&'static str, GitHubPermissionLevel)] {
    const ARCHITECT: &[(&str, GitHubPermissionLevel)] = &[
        ("administration", GitHubPermissionLevel::Read),
        ("checks", GitHubPermissionLevel::Write),
        ("contents", GitHubPermissionLevel::Write),
        ("pull_requests", GitHubPermissionLevel::Write),
    ];
    const DEVELOPER: &[(&str, GitHubPermissionLevel)] = &[
        ("contents", GitHubPermissionLevel::Write),
        ("pull_requests", GitHubPermissionLevel::Write),
    ];
    const REVIEWER: &[(&str, GitHubPermissionLevel)] =
        &[("pull_requests", GitHubPermissionLevel::Write)];
    match role {
        GitHubAppRole::Architect => ARCHITECT,
        GitHubAppRole::Developer => DEVELOPER,
        GitHubAppRole::Reviewer1 | GitHubAppRole::Reviewer2 => REVIEWER,
    }
}

pub(crate) fn validate_slug(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_GITHUB_SLUG_BYTES
        || value.starts_with(['-', '.'])
        || value.ends_with(['-', '.'])
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{label} is not a bounded GitHub slug");
    }
    Ok(())
}

pub(crate) fn validate_branch(value: &str) -> Result<()> {
    // Match the literal branch-name rules enforced by
    // `git check-ref-format --branch`. `@{-n}` is deliberately rejected by the
    // `@{` rule: Git expands that shorthand using local checkout history, while
    // this config must identify the same literal ref through the GitHub API.
    if value.is_empty()
        || value.len() > MAX_GITHUB_BRANCH_BYTES
        || value.starts_with('-')
        || value.starts_with('/')
        || value.ends_with('/')
        || value.ends_with('.')
        || value == "HEAD"
        || value.contains("..")
        || value.contains("@{")
        || value.bytes().any(|byte| {
            byte.is_ascii_control()
                || byte == b' '
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
        || value.split('/').any(|component| {
            component.is_empty() || component.starts_with('.') || component.ends_with(".lock")
        })
    {
        bail!("GitHub base_branch is not a valid bounded branch name");
    }
    Ok(())
}

fn validate_canonical_directory(label: &str, value: &Path) -> Result<()> {
    if !value.is_absolute()
        || !value
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
        || value.as_os_str().as_encoded_bytes().len() > MAX_KEY_PATH_BYTES
    {
        bail!("{label} must be an existing canonical absolute directory");
    }
    let canonical = std::fs::canonicalize(value)
        .map_err(|_| anyhow::anyhow!("{label} must be an existing canonical absolute directory"))?;
    let metadata = std::fs::symlink_metadata(value)?;
    if canonical != value || metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} must be an existing canonical absolute directory");
    }
    Ok(())
}

fn validate_key_path(path: &Path, project_root: &Path, repository_root: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.as_os_str().as_encoded_bytes().len() > MAX_KEY_PATH_BYTES
        || path.file_name().is_none()
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        bail!("GitHub App private_key_file must be a normalized absolute path");
    }
    if path.starts_with(project_root) || path.starts_with(repository_root) {
        bail!("GitHub App private_key_file must be outside project and repository workspaces");
    }
    Ok(())
}

pub(crate) fn validate_id(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("{label} is not a bounded opaque identifier");
    }
    Ok(())
}

pub(crate) fn validate_git_sha(label: &str, value: &str) -> Result<()> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase 40-hex Git object ID");
    }
    Ok(())
}

pub(crate) fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase SHA-256 digest");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn config(root: &Path, dual: bool) -> GitHubDeploymentConfig {
        let app = |id, slug: &str| GitHubAppConfig {
            app_id: id,
            slug: slug.into(),
            private_key_file: PathBuf::from(format!("/var/lib/hcom-secrets/{slug}.pem")),
        };
        GitHubDeploymentConfig {
            owner: "owner".into(),
            repository: "repository".into(),
            local_repository_root: root.into(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 21_600,
            delete_remote_branch_after_merge: true,
            private_repository_required: true,
            apps: GitHubAppsConfig {
                architect: app(1, "hcom-arch"),
                developer: app(2, "hcom-dev"),
                reviewer1: app(3, "hcom-reviewer1"),
                reviewer2: dual.then(|| app(4, "hcom-reviewer2")),
            },
        }
    }

    fn permissions(role: GitHubAppRole) -> BTreeMap<String, GitHubPermissionLevel> {
        let mut permissions = BTreeMap::from([
            ("pull_requests".into(), GitHubPermissionLevel::Write),
            ("issues".into(), GitHubPermissionLevel::Write),
        ]);
        if role == GitHubAppRole::Developer || role == GitHubAppRole::Architect {
            permissions.insert("contents".into(), GitHubPermissionLevel::Write);
        }
        if role == GitHubAppRole::Architect {
            permissions.insert("administration".into(), GitHubPermissionLevel::Read);
            permissions.insert("checks".into(), GitHubPermissionLevel::Write);
        }
        permissions
    }

    fn observation(
        config: &GitHubDeploymentConfig,
        reviewers: &[ReviewerId],
    ) -> GitHubPreflightObservation {
        let roles = GitHubAppRole::for_reviewers(reviewers);
        GitHubPreflightObservation {
            owner: config.owner.clone(),
            repository: config.repository.clone(),
            repository_id: 99,
            private: true,
            installation_repository_ids: roles
                .iter()
                .copied()
                .map(|role| (role, vec![99]))
                .collect(),
            apps: roles
                .iter()
                .copied()
                .enumerate()
                .map(|(index, role)| GitHubAppObservation {
                    role,
                    app_id: config.app(role).unwrap().app_id,
                    installation_id: 100 + index as u64,
                    slug: config.app(role).unwrap().slug.clone(),
                    bot_user_id: 200 + index as u64,
                    effective_permissions: permissions(role),
                })
                .collect(),
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: "b".repeat(64),
            inspection_id: "inspection-one".into(),
        }
    }

    struct MockProvider {
        preflight_result: Mutex<Option<GitHubPreflightObservation>>,
        inspection_result: GitHubInspectionResult,
        requests: Mutex<Vec<(String, u64)>>,
    }

    impl GitHubPreflightProvider for MockProvider {
        fn preflight(
            &self,
            request: &GitHubPreflightRequest<'_>,
        ) -> Result<GitHubPreflightObservation> {
            self.requests
                .lock()
                .unwrap()
                .push((request.run_id.into(), request.session_version));
            self.preflight_result
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| anyhow::anyhow!("mock preflight result was consumed"))
        }
    }

    impl GitHubInspectionProvider for MockProvider {
        fn inspect(&self, request: &GitHubInspectionRequest) -> Result<GitHubInspectionResult> {
            self.requests
                .lock()
                .unwrap()
                .push((request.run_id.clone(), request.session_version));
            Ok(self.inspection_result.clone())
        }
    }

    impl GitHubWorkflowProvider for MockProvider {
        fn prepare_repository(
            &self,
            _workspace: &TasksWorkspace,
            _request: &PrepareGitHubRunRequest,
        ) -> Result<RepositoryPreparedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn publish_candidate(
            &self,
            _request: &PublishDeveloperCandidateRequest,
        ) -> Result<CandidatePublishedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn publish_review(
            &self,
            _request: &PublishReviewerReviewRequest,
        ) -> Result<ReviewerReviewPublishedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn publish_check(
            &self,
            _request: &PublishReviewCheckRequest,
        ) -> Result<ReviewCheckPublishedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn wait_for_merge_gate(
            &self,
            _request: &WaitForMergeGateRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<MergeGateObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn merge_pull_request(
            &self,
            _request: &MergePullRequestRequest,
            _cancelled: &std::sync::atomic::AtomicBool,
        ) -> Result<PullRequestMergedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn finalize_run(
            &self,
            _request: &FinalizeGitHubRunRequest,
        ) -> Result<GitHubRunFinalizedObservation> {
            bail!("workflow not used by preflight seam test")
        }

        fn publish_terminal_best_effort(
            &self,
            _request: &PublishGitHubTerminalRequest,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn complete_permission_supersets_are_frozen_but_required_minimums_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let reviewers = [ReviewerId::Reviewer1, ReviewerId::Reviewer2];
        let config = config(&root, true);
        validate_github_deployment_config(&config, &reviewers, Path::new("/project")).unwrap();
        let frozen =
            freeze_preflight(&config, &reviewers, observation(&config, &reviewers)).unwrap();
        assert_eq!(
            frozen.delivery_binding.architect_app.effective_permissions["issues"],
            GitHubPermissionLevel::Write
        );
        assert_eq!(frozen.delivery_binding.reviewer_apps.len(), 2);
        let encoded = serde_json::to_value(delivery_binding(&frozen)).unwrap();
        assert_eq!(encoded["mode"], "github_pull_request");
        assert_eq!(encoded["owner"], "owner");
        assert!(encoded.get("binding").is_none());
        assert!(!encoded.to_string().contains(".pem"));
        assert!(!format!("{:?}", frozen.delivery_binding).contains("hcom-secrets"));

        let mut missing = observation(&config, &reviewers);
        missing.apps[0].effective_permissions.remove("checks");
        assert!(freeze_preflight(&config, &reviewers, missing).is_err());

        let mut drifted = frozen.clone();
        drifted
            .delivery_binding
            .architect_app
            .effective_permissions
            .insert("workflows".into(), GitHubPermissionLevel::Write);
        assert!(validate_inspection_result(&frozen.delivery_binding, &drifted).is_err());
    }

    #[test]
    fn mockable_preflight_seam_freezes_runtime_and_exact_request_identity() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let reviewers = [ReviewerId::Reviewer1];
        let config = config(&root, false);
        let initial_observation = observation(&config, &reviewers);
        let frozen = freeze_preflight(&config, &reviewers, initial_observation.clone()).unwrap();
        let provider = Arc::new(MockProvider {
            preflight_result: Mutex::new(Some(initial_observation)),
            inspection_result: frozen.clone(),
            requests: Mutex::new(Vec::new()),
        });

        let runtime =
            preflight_runtime(provider.clone(), "run-one", 0, &config, &reviewers).unwrap();
        assert_eq!(runtime.binding, frozen.delivery_binding);
        assert_eq!(runtime.initial_inspection, frozen.inspection);
        assert_eq!(
            runtime.binding.developer_commit_identity(),
            crate::control_api::GitHubCommitIdentity {
                name: "hcom-dev[bot]".into(),
                email: "201+hcom-dev[bot]@users.noreply.github.com".into(),
            }
        );
        assert_eq!(
            provider.requests.lock().unwrap().as_slice(),
            [("run-one".into(), 0)]
        );
    }

    #[test]
    fn single_mode_omits_reviewer2_everywhere_and_rejects_an_inactive_profile() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let reviewers = [ReviewerId::Reviewer1];
        let single = config(&root, false);
        validate_github_deployment_config(&single, &reviewers, Path::new("/project")).unwrap();
        let frozen =
            freeze_preflight(&single, &reviewers, observation(&single, &reviewers)).unwrap();
        assert_eq!(frozen.delivery_binding.reviewer_apps.len(), 1);
        assert_eq!(
            frozen.delivery_binding.reviewer_apps[0].reviewer_id,
            ReviewerId::Reviewer1
        );

        let inactive = config(&root, true);
        assert!(
            validate_github_deployment_config(&inactive, &reviewers, Path::new("/project"))
                .is_err()
        );
    }

    #[test]
    fn config_is_closed_and_rejects_unsafe_or_non_v1_values() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let reviewers = [ReviewerId::Reviewer1];
        let mut candidate = config(&root, false);
        candidate.apps.developer.private_key_file = PathBuf::from("relative.pem");
        assert!(
            validate_github_deployment_config(&candidate, &reviewers, Path::new("/project"))
                .is_err()
        );

        let mut candidate = config(&root, false);
        candidate.private_repository_required = false;
        assert!(
            validate_github_deployment_config(&candidate, &reviewers, Path::new("/project"))
                .is_err()
        );

        let dual_reviewers = [ReviewerId::Reviewer1, ReviewerId::Reviewer2];
        let mut duplicate_app = config(&root, true);
        duplicate_app.apps.developer.app_id = duplicate_app.apps.architect.app_id;
        assert!(
            validate_github_deployment_config(
                &duplicate_app,
                &dual_reviewers,
                Path::new("/project")
            )
            .is_err()
        );

        let valid = config(&root, true);
        let mut duplicate_bot = observation(&valid, &dual_reviewers);
        duplicate_bot.apps[1].bot_user_id = duplicate_bot.apps[0].bot_user_id;
        assert!(freeze_preflight(&valid, &dual_reviewers, duplicate_bot).is_err());
        let mut public = observation(&valid, &dual_reviewers);
        public.private = false;
        assert!(freeze_preflight(&valid, &dual_reviewers, public).is_err());

        let value: toml::Value = toml::from_str(&format!(
            "owner='owner'\nrepository='repo'\nlocal_repository_root='{}'\nbase_branch='master'\nmerge_method='squash'\nmerge_wait_seconds=60\ndelete_remote_branch_after_merge=true\nprivate_repository_required=true\nunknown=true\n",
            root.display()
        )).unwrap();
        assert!(parse_github_deployment_config(value, &reviewers, Path::new("/project")).is_err());
    }

    #[test]
    fn base_branch_validation_matches_git_branch_ref_rules() {
        for valid in ["master", "feature/github-pr", "Head", "@"] {
            validate_branch(valid).unwrap();
        }
        for invalid in [
            "HEAD",
            "-topic",
            ".topic",
            "topic.",
            "topic.lock",
            "topic//nested",
            "topic..nested",
            "topic@{nested",
            "topic nested",
            "topic~nested",
            "topic^nested",
            "topic:nested",
            "topic?nested",
            "topic*nested",
            "topic[nested",
            "topic\\nested",
        ] {
            assert!(
                validate_branch(invalid).is_err(),
                "invalid Git branch name was accepted: {invalid:?}"
            );
        }
    }
}
