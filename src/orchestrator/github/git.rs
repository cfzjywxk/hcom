//! Managed Git workspace for the opt-in GitHub Pull Request lane.
//!
//! This module owns only repository mechanics. GitHub REST observations and
//! state-machine scheduling remain in the adjacent client/driver layers. The
//! production remote is always the fixed github.com HTTPS URL derived from the
//! frozen binding; a local-bare transport exists only for deterministic tests.

#![allow(
    dead_code,
    reason = "GITHUB-PR-02 is consumed by the later GitHub workflow driver task"
)]

use super::{validate_git_sha, validate_sha256};
use crate::control_api::{GitHubCommitIdentity, GitHubPullRequestBinding, GitHubRunBinding};
use crate::orchestrator::workspace::TasksWorkspace;
use anyhow::{Context, Result, anyhow, bail};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const GITHUB_HTTPS_ORIGIN: &str = "https://github.com";
const LOCAL_BASE_REF_PREFIX: &str = "refs/hcom/runs";
const MAX_GIT_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_GIT_TOKEN_BYTES: usize = 64 * 1024;
const GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const ASKPASS_FILE_NAME: &str = "hcom-github-askpass";

const ASKPASS_HELPER: &[u8] = br#"#!/bin/sh
set -eu
prompt=${1-}
url=${HCOM_GITHUB_ASKPASS_URL-}
nonce=${HCOM_GITHUB_ASKPASS_NONCE-}
fd=${HCOM_GITHUB_CREDENTIAL_FD-}
[ -n "$url" ] && [ -n "$nonce" ] || exit 1
case "$fd" in ''|*[!0-9]*) exit 1 ;; esac
plain=${url%.git}
host=https://github.com
auth="https://x-access-token@${url#https://}"
auth_plain=${auth%.git}
auth_host=https://x-access-token@github.com
case "$prompt" in
  "Username for '$url': "|"Username for '$plain': "|"Username for '$host': ")
    printf '%s\n' x-access-token
    ;;
  "Password for '$auth': "|"Password for '$auth_plain': "|"Password for '$auth_host': ")
    exec /bin/cat "/proc/self/fd/$fd"
    ;;
  *)
    exit 1
    ;;
esac
"#;

/// One owned installation token. Debug output and errors never contain its
/// bytes, and every owned copy is overwritten at drop.
pub(crate) struct GitCredential {
    bytes: SecretBytes,
}

impl GitCredential {
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty()
            || bytes.len() > MAX_GIT_TOKEN_BYTES
            || bytes.contains(&0)
            || bytes.contains(&b'\n')
            || bytes.contains(&b'\r')
        {
            bail!("GitHub Git credential is empty, unbounded, or not a single opaque value");
        }
        Ok(Self {
            bytes: SecretBytes(bytes),
        })
    }
}

impl fmt::Debug for GitCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GitCredential([redacted])")
    }
}

struct SecretBytes(Vec<u8>);

impl SecretBytes {
    fn duplicate(&self) -> Self {
        Self(self.0.clone())
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // SAFETY: `byte` is a valid, uniquely borrowed byte in this Vec.
            // Volatile writes are a best-effort lifetime control for the
            // owned buffer; they do not claim an adversarial same-user
            // isolation boundary.
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedGitWorkspace {
    repository_root: PathBuf,
    worktree_path: PathBuf,
    branch: String,
    branch_ref: String,
    namespaced_base_ref: String,
    base_sha: String,
}

impl PreparedGitWorkspace {
    pub(crate) fn repository_root(&self) -> &Path {
        &self.repository_root
    }

    pub(crate) fn worktree_path(&self) -> &Path {
        &self.worktree_path
    }

    pub(crate) fn branch(&self) -> &str {
        &self.branch
    }

    pub(crate) fn base_sha(&self) -> &str {
        &self.base_sha
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedDeveloperCommit {
    pub(crate) head_sha: String,
    pub(crate) parent_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidatePushOutcome {
    pub(crate) previous_remote_head: Option<String>,
    pub(crate) published_head: String,
    pub(crate) reconciled_after_command_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizationAuthorization {
    final_head_sha: String,
    merge_sha: String,
}

impl FinalizationAuthorization {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn after_confirmed_merge(
        final_head_sha: &str,
        recorded_pre_merge_head: &str,
        merge_sha: &str,
        pull_request_merged: bool,
        merge_evidence_durable: bool,
        live_task_workers: usize,
    ) -> Result<Self> {
        validate_git_sha("GitHub final head", final_head_sha)?;
        validate_git_sha("GitHub recorded pre-merge head", recorded_pre_merge_head)?;
        validate_git_sha("GitHub merge SHA", merge_sha)?;
        if !pull_request_merged
            || !merge_evidence_durable
            || live_task_workers != 0
            || recorded_pre_merge_head != final_head_sha
        {
            bail!(
                "GitHub finalization requires durable exact-head merge evidence and no live task worker"
            );
        }
        Ok(Self {
            final_head_sha: final_head_sha.into(),
            merge_sha: merge_sha.into(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteRefFinalizationOutcome {
    Deleted,
    AlreadyAbsent,
    PreservedByPolicy,
    PreservedAfterDeleteFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GitFinalizationOutcome {
    pub(crate) merge_sha: String,
    pub(crate) local_worktree_removed: bool,
    pub(crate) local_refs_removed: bool,
    pub(crate) remote_ref_outcome: RemoteRefFinalizationOutcome,
}

#[derive(Clone)]
enum RemoteEndpoint {
    GitHub,
    #[cfg(test)]
    LocalBare(PathBuf),
}

impl RemoteEndpoint {
    fn value(&self, binding: &GitHubPullRequestBinding) -> OsString {
        match self {
            Self::GitHub => fixed_github_https_url(binding).into(),
            #[cfg(test)]
            Self::LocalBare(path) => path.as_os_str().to_owned(),
        }
    }

    fn protocol(&self) -> &'static str {
        match self {
            Self::GitHub => "https",
            #[cfg(test)]
            Self::LocalBare(_) => "file",
        }
    }

    fn needs_credential(&self) -> bool {
        matches!(self, Self::GitHub)
    }
}

/// Filesystem/Git executor for one frozen GitHub repository binding.
///
/// The value has no Drop cleanup on purpose: pre-merge errors, cancellation,
/// and parent death preserve the generated refs/worktree for human evidence.
pub(crate) struct GitWorkspaceManager {
    git_program: PathBuf,
    remote: RemoteEndpoint,
    operation_timeout: Duration,
}

impl GitWorkspaceManager {
    pub(crate) fn new() -> Self {
        Self {
            git_program: PathBuf::from("git"),
            remote: RemoteEndpoint::GitHub,
            operation_timeout: GIT_OPERATION_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn for_local_bare(remote: PathBuf) -> Self {
        Self::for_local_bare_with_git(remote, PathBuf::from("git"))
    }

    #[cfg(test)]
    fn for_local_bare_with_git(remote: PathBuf, git_program: PathBuf) -> Self {
        Self {
            git_program,
            remote: RemoteEndpoint::LocalBare(remote),
            operation_timeout: GIT_OPERATION_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_git_program(git_program: PathBuf) -> Self {
        Self {
            git_program,
            remote: RemoteEndpoint::GitHub,
            operation_timeout: GIT_OPERATION_TIMEOUT,
        }
    }

    pub(crate) fn prepare(
        &self,
        workspace: &TasksWorkspace,
        delivery: &GitHubPullRequestBinding,
        run: &GitHubRunBinding,
        plan_hash: &str,
        credential: Option<&GitCredential>,
    ) -> Result<PreparedGitWorkspace> {
        validate_sha256("GitHub plan hash", plan_hash)?;
        validate_git_sha("GitHub expected base SHA", &run.expected_base_sha)?;
        if run.inspected_repository_id != delivery.repository_id
            || run.expected_base_ref != format!("refs/heads/{}", delivery.base_branch)
        {
            bail!("GitHub run base binding differs from the frozen repository binding");
        }
        let expected_branch = format!("hcom/{}-{}", workspace.run_id(), &plan_hash[..12]);
        if run.generated_run_branch != expected_branch {
            bail!("GitHub run branch is not the deterministic run/plan branch");
        }
        self.validate_ref_name(&format!("refs/heads/{expected_branch}"))?;

        let repository_root = PathBuf::from(&delivery.local_repository_root);
        let canonical_repository = fs::canonicalize(&repository_root)
            .context("failed to canonicalize the configured GitHub repository root")?;
        if canonical_repository != repository_root {
            bail!("configured GitHub repository root is not canonical");
        }
        let top_level = PathBuf::from(self.one_line(
            &repository_root,
            [
                OsString::from("rev-parse"),
                OsString::from("--show-toplevel"),
            ],
        )?);
        if fs::canonicalize(&top_level).ok().as_ref() != Some(&repository_root) {
            bail!("configured GitHub repository root is not the exact Git top level");
        }

        let worktree_path = workspace.repository_path();
        match fs::symlink_metadata(&worktree_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => bail!("managed GitHub worktree path already exists"),
            Err(error) => return Err(error).context("failed to inspect managed worktree path"),
        }
        let namespaced_base_ref = format!("{LOCAL_BASE_REF_PREFIX}/{}/base", workspace.run_id());
        self.validate_ref_name(&namespaced_base_ref)?;
        let branch_ref = format!("refs/heads/{expected_branch}");
        if self
            .local_ref(&repository_root, &namespaced_base_ref)?
            .is_some()
        {
            bail!("run-namespaced GitHub base ref already exists");
        }
        if self.local_ref(&repository_root, &branch_ref)?.is_some() {
            bail!("generated GitHub run branch already exists locally");
        }

        self.reject_unsafe_remote_config(&repository_root)?;
        let remote_base = self.remote_ref(
            &repository_root,
            delivery,
            &run.expected_base_ref,
            credential,
        )?;
        if remote_base.as_deref() != Some(run.expected_base_sha.as_str()) {
            bail!("remote GitHub base ref drifted from the approved exact base SHA");
        }
        if self
            .remote_ref(&repository_root, delivery, &branch_ref, credential)?
            .is_some()
        {
            bail!("generated GitHub run branch already exists remotely");
        }

        let refspec = format!("{}:{namespaced_base_ref}", run.expected_base_ref);
        let remote = self.remote.value(delivery);
        let _fetch = self.run_remote_git(
            &repository_root,
            delivery,
            credential,
            [
                OsString::from("fetch"),
                OsString::from("--quiet"),
                OsString::from("--no-tags"),
                OsString::from("--no-recurse-submodules"),
                OsString::from("--no-write-fetch-head"),
                remote,
                OsString::from(refspec),
            ],
        );
        let fetched_exact_base = self
            .local_ref(&repository_root, &namespaced_base_ref)?
            .as_deref()
            == Some(run.expected_base_sha.as_str());
        let remote_still_exact = self
            .remote_ref(
                &repository_root,
                delivery,
                &run.expected_base_ref,
                credential,
            )?
            .as_deref()
            == Some(run.expected_base_sha.as_str());
        if !fetched_exact_base || !remote_still_exact {
            bail!("fetched run-namespaced base ref differs from the approved exact base SHA");
        }
        // A timeout/connection loss after the ref update is reconciled by the
        // exact local+remote proof above. No model turn or second fetch is
        // needed.

        let add = self.run_git(
            &repository_root,
            [
                OsString::from("worktree"),
                OsString::from("add"),
                OsString::from("--no-track"),
                OsString::from("-b"),
                OsString::from(&expected_branch),
                worktree_path.as_os_str().to_owned(),
                OsString::from(&run.expected_base_sha),
            ],
        )?;
        if !add.status.success() {
            bail!("managed linked-worktree creation failed; partial artifacts were preserved");
        }
        let canonical_worktree = fs::canonicalize(&worktree_path)
            .context("failed to canonicalize the prepared GitHub worktree")?;
        if canonical_worktree != worktree_path {
            bail!("prepared GitHub worktree path is not canonical");
        }
        let prepared = PreparedGitWorkspace {
            repository_root,
            worktree_path,
            branch: expected_branch,
            branch_ref,
            namespaced_base_ref,
            base_sha: run.expected_base_sha.clone(),
        };
        self.validate_local_checkout(&prepared, &prepared.base_sha)?;
        Ok(prepared)
    }

    pub(crate) fn validate_remote_base(
        &self,
        prepared: &PreparedGitWorkspace,
        delivery: &GitHubPullRequestBinding,
        api_observed_base_sha: &str,
        credential: Option<&GitCredential>,
    ) -> Result<()> {
        validate_git_sha("GitHub API base SHA", api_observed_base_sha)?;
        if api_observed_base_sha != prepared.base_sha {
            bail!("GitHub API base ref drifted from the run's frozen base SHA");
        }
        let expected_ref = format!("refs/heads/{}", delivery.base_branch);
        if self
            .remote_ref(
                &prepared.repository_root,
                delivery,
                &expected_ref,
                credential,
            )?
            .as_deref()
            != Some(prepared.base_sha.as_str())
        {
            bail!("remote Git base ref drifted from the run's frozen base SHA");
        }
        Ok(())
    }

    pub(crate) fn validate_developer_commit(
        &self,
        prepared: &PreparedGitWorkspace,
        delivery: &GitHubPullRequestBinding,
        previously_published_head: Option<&str>,
        credential: Option<&GitCredential>,
    ) -> Result<ValidatedDeveloperCommit> {
        let expected_parent = previously_published_head.unwrap_or(&prepared.base_sha);
        validate_git_sha("expected Developer commit parent", expected_parent)?;
        self.validate_namespaced_base(prepared)?;
        self.validate_local_checkout_branch(prepared)?;
        self.validate_clean_checkout(prepared)?;

        let head = self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from("HEAD^{commit}"),
            ],
        )?;
        validate_git_sha("Developer commit HEAD", &head)?;
        let parents = self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("rev-list"),
                OsString::from("--parents"),
                OsString::from("-n"),
                OsString::from("1"),
                OsString::from(&head),
            ],
        )?;
        let fields = parents.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.as_slice() != [head.as_str(), expected_parent] {
            bail!(
                "Developer turn must append exactly one non-merge child of the previously published head"
            );
        }

        self.validate_commit_identity(
            &prepared.worktree_path,
            &head,
            &delivery.developer_commit_identity(),
        )?;
        self.reject_hcom_tasks_commit(&prepared.worktree_path, &head)?;

        let expected_remote = previously_published_head.map(str::to_owned);
        let actual_remote = self.remote_ref(
            &prepared.repository_root,
            delivery,
            &prepared.branch_ref,
            credential,
        )?;
        if actual_remote != expected_remote {
            bail!("remote GitHub run branch changed from its previously published head");
        }
        self.validate_clean_checkout(prepared)?;
        if self.current_head(prepared)? != head {
            bail!("Developer Git state drifted during append-only validation");
        }
        Ok(ValidatedDeveloperCommit {
            head_sha: head,
            parent_sha: expected_parent.into(),
        })
    }

    pub(crate) fn push_candidate(
        &self,
        prepared: &PreparedGitWorkspace,
        delivery: &GitHubPullRequestBinding,
        candidate: &ValidatedDeveloperCommit,
        previously_published_head: Option<&str>,
        credential: Option<&GitCredential>,
    ) -> Result<CandidatePushOutcome> {
        let revalidated = self.validate_developer_commit(
            prepared,
            delivery,
            previously_published_head,
            credential,
        )?;
        if &revalidated != candidate {
            bail!("Developer candidate changed after its repository validation");
        }
        let refspec = format!("{}:{}", candidate.head_sha, prepared.branch_ref);
        let push = self.run_remote_git(
            &prepared.repository_root,
            delivery,
            credential,
            [
                OsString::from("push"),
                OsString::from("--porcelain"),
                self.remote.value(delivery),
                OsString::from(refspec),
            ],
        );
        let remote_after = self.remote_ref(
            &prepared.repository_root,
            delivery,
            &prepared.branch_ref,
            credential,
        )?;
        if remote_after.as_deref() != Some(candidate.head_sha.as_str()) {
            bail!("candidate push did not produce the exact expected remote run head");
        }
        self.validate_local_checkout(prepared, &candidate.head_sha)?;
        let push_failed = match &push {
            Ok(output) => !output.status.success(),
            Err(_) => true,
        };
        Ok(CandidatePushOutcome {
            previous_remote_head: previously_published_head.map(str::to_owned),
            published_head: candidate.head_sha.clone(),
            reconciled_after_command_failure: push_failed,
        })
    }

    pub(crate) fn validate_published_checkout(
        &self,
        prepared: &PreparedGitWorkspace,
        delivery: &GitHubPullRequestBinding,
        published_head: &str,
        credential: Option<&GitCredential>,
    ) -> Result<()> {
        validate_git_sha("published GitHub head", published_head)?;
        self.validate_namespaced_base(prepared)?;
        self.validate_local_checkout(prepared, published_head)?;
        if self
            .remote_ref(
                &prepared.repository_root,
                delivery,
                &prepared.branch_ref,
                credential,
            )?
            .as_deref()
            != Some(published_head)
        {
            bail!("remote GitHub run branch differs from the published candidate head");
        }
        Ok(())
    }

    pub(crate) fn finalize_success(
        &self,
        prepared: &PreparedGitWorkspace,
        delivery: &GitHubPullRequestBinding,
        authorization: &FinalizationAuthorization,
        architect_credential: Option<&GitCredential>,
    ) -> Result<GitFinalizationOutcome> {
        self.validate_namespaced_base(prepared)?;
        if prepared.worktree_path.exists() {
            self.validate_local_checkout(prepared, &authorization.final_head_sha)?;
        } else if self.worktree_registered(prepared)? {
            bail!("managed GitHub worktree is missing but remains registered");
        }

        let remote_before = self.remote_ref(
            &prepared.repository_root,
            delivery,
            &prepared.branch_ref,
            architect_credential,
        )?;
        if remote_before
            .as_deref()
            .is_some_and(|head| head != authorization.final_head_sha)
        {
            bail!("merged GitHub run branch was remotely mutated and will not be deleted");
        }

        if self.branch_is_checked_out_outside_managed_worktree(prepared)? {
            bail!(
                "generated local GitHub branch is checked out outside the managed worktree and will not be deleted"
            );
        }

        if prepared.worktree_path.exists() {
            let remove = self.run_git(
                &prepared.repository_root,
                [
                    OsString::from("worktree"),
                    OsString::from("remove"),
                    prepared.worktree_path.as_os_str().to_owned(),
                ],
            )?;
            if !remove.status.success() {
                bail!("required managed GitHub worktree removal failed");
            }
        }
        if prepared.worktree_path.exists() || self.worktree_registered(prepared)? {
            bail!("managed GitHub worktree was not completely removed");
        }

        self.delete_local_branch_if_exact(prepared, &authorization.final_head_sha)?;
        self.delete_local_ref_if_exact(
            &prepared.repository_root,
            &prepared.namespaced_base_ref,
            &prepared.base_sha,
        )?;

        let remote_ref_outcome = if !delivery.delete_remote_branch_after_merge {
            if remote_before.is_some() {
                RemoteRefFinalizationOutcome::PreservedByPolicy
            } else {
                RemoteRefFinalizationOutcome::AlreadyAbsent
            }
        } else if remote_before.is_none() {
            RemoteRefFinalizationOutcome::AlreadyAbsent
        } else {
            let lease = format!(
                "--force-with-lease={}:{}",
                prepared.branch_ref, authorization.final_head_sha
            );
            let deletion_refspec = format!(":{}", prepared.branch_ref);
            let deletion = self.run_remote_git(
                &prepared.repository_root,
                delivery,
                architect_credential,
                [
                    OsString::from("push"),
                    OsString::from("--porcelain"),
                    OsString::from(lease),
                    self.remote.value(delivery),
                    OsString::from(deletion_refspec),
                ],
            );
            let remote_after = self.remote_ref(
                &prepared.repository_root,
                delivery,
                &prepared.branch_ref,
                architect_credential,
            )?;
            let deletion_failed = match &deletion {
                Ok(output) => !output.status.success(),
                Err(_) => true,
            };
            match remote_after.as_deref() {
                None => RemoteRefFinalizationOutcome::Deleted,
                Some(head) if head == authorization.final_head_sha && deletion_failed => {
                    RemoteRefFinalizationOutcome::PreservedAfterDeleteFailure
                }
                Some(head) if head == authorization.final_head_sha => {
                    RemoteRefFinalizationOutcome::PreservedAfterDeleteFailure
                }
                Some(_) => {
                    bail!(
                        "merged GitHub run branch changed during final deletion and was preserved"
                    )
                }
            }
        };

        Ok(GitFinalizationOutcome {
            merge_sha: authorization.merge_sha.clone(),
            local_worktree_removed: true,
            local_refs_removed: true,
            remote_ref_outcome,
        })
    }

    fn validate_local_checkout(
        &self,
        prepared: &PreparedGitWorkspace,
        expected_head: &str,
    ) -> Result<()> {
        self.validate_local_checkout_branch(prepared)?;
        self.validate_clean_checkout(prepared)?;
        if self.current_head(prepared)? != expected_head {
            bail!("managed GitHub worktree HEAD differs from the exact expected head");
        }
        Ok(())
    }

    fn validate_local_checkout_branch(&self, prepared: &PreparedGitWorkspace) -> Result<()> {
        let top_level = self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("rev-parse"),
                OsString::from("--show-toplevel"),
            ],
        )?;
        if Path::new(&top_level) != prepared.worktree_path {
            bail!("managed GitHub worktree is not its exact Git top level");
        }
        let branch = self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("symbolic-ref"),
                OsString::from("--quiet"),
                OsString::from("HEAD"),
            ],
        )?;
        if branch != prepared.branch_ref {
            bail!("managed GitHub worktree is not on its frozen run branch");
        }
        let common = PathBuf::from(self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("rev-parse"),
                OsString::from("--path-format=absolute"),
                OsString::from("--git-common-dir"),
            ],
        )?);
        let source_common = PathBuf::from(self.one_line(
            &prepared.repository_root,
            [
                OsString::from("rev-parse"),
                OsString::from("--path-format=absolute"),
                OsString::from("--git-common-dir"),
            ],
        )?);
        if fs::canonicalize(common).ok() != fs::canonicalize(source_common).ok() {
            bail!("managed GitHub worktree does not belong to the configured source repository");
        }
        Ok(())
    }

    fn validate_clean_checkout(&self, prepared: &PreparedGitWorkspace) -> Result<()> {
        let output = self.run_git(
            &prepared.worktree_path,
            [
                OsString::from("status"),
                OsString::from("--porcelain=v1"),
                OsString::from("-z"),
                OsString::from("--untracked-files=all"),
            ],
        )?;
        if !output.status.success() || !output.stdout.is_empty() {
            bail!("managed GitHub worktree/index is not clean");
        }
        Ok(())
    }

    fn validate_namespaced_base(&self, prepared: &PreparedGitWorkspace) -> Result<()> {
        if self
            .local_ref(&prepared.repository_root, &prepared.namespaced_base_ref)?
            .as_deref()
            != Some(prepared.base_sha.as_str())
        {
            bail!("run-namespaced GitHub base ref changed after preparation");
        }
        Ok(())
    }

    fn current_head(&self, prepared: &PreparedGitWorkspace) -> Result<String> {
        let head = self.one_line(
            &prepared.worktree_path,
            [
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from("HEAD^{commit}"),
            ],
        )?;
        validate_git_sha("managed GitHub worktree HEAD", &head)?;
        Ok(head)
    }

    fn validate_commit_identity(
        &self,
        worktree: &Path,
        head: &str,
        expected: &GitHubCommitIdentity,
    ) -> Result<()> {
        let output = self.run_git(
            worktree,
            [
                OsString::from("show"),
                OsString::from("--no-patch"),
                OsString::from("--no-show-signature"),
                OsString::from(
                    "--format=%an%x00%ae%x00%cn%x00%ce%x00%(trailers:key=Signed-off-by,valueonly,unfold)%x00",
                ),
                OsString::from(head),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to inspect Developer commit identity");
        }
        let identity_bytes = output
            .stdout
            .strip_suffix(b"\n")
            .unwrap_or(output.stdout.as_slice());
        let mut fields = identity_bytes.split(|byte| *byte == 0);
        let author_name = fields.next().unwrap_or_default();
        let author_email = fields.next().unwrap_or_default();
        let committer_name = fields.next().unwrap_or_default();
        let committer_email = fields.next().unwrap_or_default();
        let signoff_trailers = fields.next().unwrap_or_default();
        if fields.any(|field| !field.is_empty())
            || author_name != expected.name.as_bytes()
            || author_email != expected.email.as_bytes()
            || committer_name != expected.name.as_bytes()
            || committer_email != expected.email.as_bytes()
        {
            bail!("Developer commit author and committer must match the frozen bot identity");
        }
        let signoff_trailers = std::str::from_utf8(signoff_trailers)
            .context("Developer commit Signed-off-by trailer is not valid UTF-8")?;
        let expected_signoff = format!("{} <{}>", expected.name, expected.email);
        let signoffs = signoff_trailers.lines().collect::<Vec<_>>();
        if signoffs.as_slice() != [expected_signoff.as_str()] {
            bail!("Developer commit requires exactly one matching Signed-off-by trailer");
        }
        Ok(())
    }

    fn reject_hcom_tasks_commit(&self, worktree: &Path, head: &str) -> Result<()> {
        let output = self.run_git(
            worktree,
            [
                OsString::from("diff-tree"),
                OsString::from("--no-commit-id"),
                OsString::from("--name-only"),
                OsString::from("-r"),
                OsString::from("-z"),
                OsString::from(head),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to inspect Developer commit paths");
        }
        for path in output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
        {
            let path = Path::new(OsStr::from_bytes(path));
            if path
                .components()
                .any(|component| component == Component::Normal(OsStr::new("hcom-tasks")))
            {
                bail!("Developer commit contains a forbidden hcom-tasks artifact");
            }
        }
        Ok(())
    }

    fn delete_local_branch_if_exact(
        &self,
        prepared: &PreparedGitWorkspace,
        expected: &str,
    ) -> Result<()> {
        if self.branch_is_checked_out_outside_managed_worktree(prepared)? {
            bail!(
                "generated local GitHub branch is checked out outside the managed worktree and will not be deleted"
            );
        }
        self.delete_local_ref_if_exact(&prepared.repository_root, &prepared.branch_ref, expected)
    }

    fn delete_local_ref_if_exact(
        &self,
        repository: &Path,
        reference: &str,
        expected: &str,
    ) -> Result<()> {
        match self.local_ref(repository, reference)? {
            None => Ok(()),
            Some(actual) if actual == expected => {
                let output = self.run_git(
                    repository,
                    [
                        OsString::from("update-ref"),
                        OsString::from("-d"),
                        OsString::from(reference),
                        OsString::from(expected),
                    ],
                )?;
                match self.local_ref(repository, reference)? {
                    None => Ok(()),
                    Some(after) if after != expected => {
                        bail!("generated local GitHub ref changed and was preserved")
                    }
                    Some(_) if !output.status.success() => {
                        bail!("exact generated local GitHub ref removal command failed")
                    }
                    Some(_) => bail!("exact generated local GitHub ref removal failed"),
                }
            }
            Some(_) => bail!("generated local GitHub ref changed and was preserved"),
        }
    }

    fn worktree_registered(&self, prepared: &PreparedGitWorkspace) -> Result<bool> {
        let output = self.run_git(
            &prepared.repository_root,
            [
                OsString::from("worktree"),
                OsString::from("list"),
                OsString::from("--porcelain"),
                OsString::from("-z"),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to inspect registered Git worktrees");
        }
        let expected = prepared.worktree_path.as_os_str().as_bytes();
        Ok(output.stdout.split(|byte| *byte == 0).any(|field| {
            field
                .strip_prefix(b"worktree ")
                .is_some_and(|path| path == expected)
        }))
    }

    fn branch_is_checked_out_outside_managed_worktree(
        &self,
        prepared: &PreparedGitWorkspace,
    ) -> Result<bool> {
        let output = self.run_git(
            &prepared.repository_root,
            [
                OsString::from("worktree"),
                OsString::from("list"),
                OsString::from("--porcelain"),
                OsString::from("-z"),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to inspect registered Git worktrees");
        }

        let managed_path = prepared.worktree_path.as_os_str().as_bytes();
        let mut current_path = None;
        for field in output.stdout.split(|byte| *byte == 0) {
            if field.is_empty() {
                current_path = None;
            } else if let Some(path) = field.strip_prefix(b"worktree ") {
                current_path = Some(path);
            } else if field.strip_prefix(b"branch ") == Some(prepared.branch_ref.as_bytes()) {
                let path = current_path
                    .ok_or_else(|| anyhow!("registered Git worktree listing is malformed"))?;
                if path != managed_path {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn local_ref(&self, repository: &Path, reference: &str) -> Result<Option<String>> {
        let output = self.run_git(
            repository,
            [
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from("--quiet"),
                OsString::from(format!("{reference}^{{commit}}")),
            ],
        )?;
        match output.status.code() {
            Some(0) => {
                let value = exactly_one_line(&output.stdout, "local Git ref")?;
                validate_git_sha("local Git ref", &value)?;
                Ok(Some(value))
            }
            Some(1) => Ok(None),
            _ => bail!("failed to inspect exact local Git ref"),
        }
    }

    fn remote_ref(
        &self,
        repository: &Path,
        delivery: &GitHubPullRequestBinding,
        reference: &str,
        credential: Option<&GitCredential>,
    ) -> Result<Option<String>> {
        self.validate_ref_name(reference)?;
        let output = self.run_remote_git(
            repository,
            delivery,
            credential,
            [
                OsString::from("ls-remote"),
                OsString::from("--refs"),
                self.remote.value(delivery),
                OsString::from(reference),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to read exact remote Git ref; diagnostic output was suppressed");
        }
        if output.stdout.is_empty() {
            return Ok(None);
        }
        let text = std::str::from_utf8(&output.stdout).context("remote Git ref is not UTF-8")?;
        let lines = text.lines().collect::<Vec<_>>();
        if lines.len() != 1 {
            bail!("remote Git ref lookup returned an ambiguous result");
        }
        let Some((sha, observed_ref)) = lines[0].split_once('\t') else {
            bail!("remote Git ref lookup returned a malformed result");
        };
        validate_git_sha("remote Git ref SHA", sha)?;
        if observed_ref != reference {
            bail!("remote Git ref lookup returned a different ref");
        }
        Ok(Some(sha.into()))
    }

    fn validate_ref_name(&self, reference: &str) -> Result<()> {
        let output = self.run_git(
            Path::new("/"),
            [
                OsString::from("check-ref-format"),
                OsString::from(reference),
            ],
        )?;
        if !output.status.success() {
            bail!("generated GitHub ref is not a valid Git ref name");
        }
        Ok(())
    }

    fn reject_unsafe_remote_config(&self, repository: &Path) -> Result<()> {
        let output = self.run_git(
            repository,
            [
                OsString::from("config"),
                OsString::from("--null"),
                OsString::from("--list"),
            ],
        )?;
        if !output.status.success() {
            bail!("failed to inspect effective Git configuration before authenticated operation");
        }
        for entry in output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            let name = entry
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap_or_default();
            let lower = name.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>();
            let url_rewrite = lower.starts_with(b"url.")
                && (lower.ends_with(b".insteadof") || lower.ends_with(b".pushinsteadof"));
            let extra_header = lower == b"http.extraheader"
                || (lower.starts_with(b"http.") && lower.ends_with(b".extraheader"));
            if url_rewrite || extra_header {
                bail!(
                    "effective Git configuration may rewrite the fixed GitHub URL or inject an HTTP header"
                );
            }
        }
        Ok(())
    }

    fn one_line<I>(&self, repository: &Path, args: I) -> Result<String>
    where
        I: IntoIterator<Item = OsString>,
    {
        let output = self.run_git(repository, args)?;
        if !output.status.success() {
            bail!("bounded Git evidence command failed");
        }
        exactly_one_line(&output.stdout, "Git evidence")
    }

    fn run_remote_git<I>(
        &self,
        repository: &Path,
        delivery: &GitHubPullRequestBinding,
        credential: Option<&GitCredential>,
        args: I,
    ) -> Result<BoundedOutput>
    where
        I: IntoIterator<Item = OsString>,
    {
        self.reject_unsafe_remote_config(repository)?;
        if self.remote.needs_credential() && credential.is_none() {
            bail!("fixed GitHub HTTPS operation requires an ephemeral App credential");
        }
        if !self.remote.needs_credential() && credential.is_some() {
            bail!("local-bare test transport must not receive a GitHub credential");
        }
        self.run_git_inner(repository, args, Some((delivery, credential)))
    }

    fn run_git<I>(&self, repository: &Path, args: I) -> Result<BoundedOutput>
    where
        I: IntoIterator<Item = OsString>,
    {
        self.run_git_inner(repository, args, None)
    }

    fn run_git_inner<I>(
        &self,
        repository: &Path,
        args: I,
        remote: Option<(&GitHubPullRequestBinding, Option<&GitCredential>)>,
    ) -> Result<BoundedOutput>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut command = Command::new(&self.git_program);
        command
            .arg("--no-replace-objects")
            .args(["-c", "core.fsmonitor=false"])
            .args(["-c", "core.untrackedCache=false"])
            .current_dir(repository)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // SAFETY: the closure calls only async-signal-safe descriptor/process
        // operations. Marking the entire non-stdio range close-on-exec makes
        // the credential pipe the sole deliberate exception (the later
        // channel closure clears CLOEXEC only on that exact descriptor). A
        // private process group lets timeout/overflow cleanup include Git's
        // remote and askpass descendants instead of killing only the leader.
        unsafe {
            command.pre_exec(|| {
                if libc::syscall(
                    libc::SYS_close_range,
                    3_u32,
                    u32::MAX,
                    libc::CLOSE_RANGE_CLOEXEC,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        sanitize_git_environment(&mut command);
        command.env("LC_ALL", "C");

        let mut channel = None;
        if let Some((binding, credential)) = remote {
            command
                .args(["-c", "core.hooksPath=/dev/null"])
                .args(["-c", "credential.helper="])
                .args(["-c", "credential.username=x-access-token"])
                .args(["-c", "credential.useHttpPath=true"])
                .args(["-c", "credential.interactive=never"])
                .args(["-c", "http.followRedirects=false"])
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_ALLOW_PROTOCOL", self.remote.protocol());
            if let Some(credential) = credential {
                let prepared_channel =
                    CredentialChannel::new(&fixed_github_https_url(binding), &credential.bytes)?;
                prepared_channel.configure(&mut command)?;
                channel = Some(prepared_channel);
            }
        }
        command.args(args);
        run_bounded(command, channel, self.operation_timeout)
    }
}

pub(crate) fn fixed_github_https_url(binding: &GitHubPullRequestBinding) -> String {
    format!(
        "{GITHUB_HTTPS_ORIGIN}/{}/{}.git",
        binding.owner, binding.repository
    )
}

fn sanitize_git_environment(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if name.as_os_str().as_bytes().starts_with(b"GIT_") {
            command.env_remove(name);
        }
    }
    command
        .env_remove("SSH_ASKPASS")
        .env_remove("SSH_ASKPASS_REQUIRE")
        .env_remove("GCM_INTERACTIVE");
}

struct CredentialChannel {
    _helper_directory: TempDir,
    url: String,
    read_end: OwnedFd,
    write_end: OwnedFd,
    token: SecretBytes,
}

impl CredentialChannel {
    fn new(url: &str, token: &SecretBytes) -> Result<Self> {
        if !url.starts_with("https://github.com/") || !url.ends_with(".git") {
            bail!("askpass credential channel requires the fixed GitHub HTTPS URL");
        }
        let helper_directory = tempfile::Builder::new()
            .prefix("hcom-github-askpass.")
            .tempdir()
            .context("failed to create private GitHub askpass directory")?;
        fs::set_permissions(helper_directory.path(), fs::Permissions::from_mode(0o700))?;
        let helper = helper_directory.path().join(ASKPASS_FILE_NAME);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o700)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&helper)
            .context("failed to create private GitHub askpass helper")?;
        file.write_all(ASKPASS_HELPER)
            .context("failed to write private GitHub askpass helper")?;
        file.flush()?;
        drop(file);

        let mut descriptors = [-1; 2];
        // SAFETY: `descriptors` points to two writable integers and pipe2 has
        // no aliasing requirements.
        if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to create GitHub credential pipe");
        }
        // SAFETY: pipe2 returned two newly owned descriptors on success.
        let read_end = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
        // SAFETY: pipe2 returned two newly owned descriptors on success.
        let write_end = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
        Ok(Self {
            _helper_directory: helper_directory,
            url: url.into(),
            read_end,
            write_end,
            token: token.duplicate(),
        })
    }

    fn configure(&self, command: &mut Command) -> Result<()> {
        let helper = self._helper_directory.path().join(ASKPASS_FILE_NAME);
        let helper_text = helper
            .to_str()
            .ok_or_else(|| anyhow!("GitHub askpass helper path is not UTF-8"))?;
        let fd = self.read_end.as_raw_fd();
        let nonce = Uuid::new_v4().simple().to_string();
        command
            .env("GIT_ASKPASS", helper_text)
            .env("SSH_ASKPASS", helper_text)
            .env("HCOM_GITHUB_ASKPASS_URL", &self.url)
            .env("HCOM_GITHUB_ASKPASS_NONCE", nonce)
            .env("HCOM_GITHUB_CREDENTIAL_FD", fd.to_string());
        // SAFETY: the closure calls only async-signal-safe fcntl operations on
        // a descriptor owned by this channel and alive through spawn.
        unsafe {
            command.pre_exec(move || {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }
}

struct BoundedOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    #[allow(dead_code)]
    stderr: Vec<u8>,
}

fn run_bounded(
    mut command: Command,
    channel: Option<CredentialChannel>,
    timeout: Duration,
) -> Result<BoundedOutput> {
    let mut child = command
        .spawn()
        .context("failed to spawn bounded Git subprocess")?;
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            terminate_git_process_group(&mut child);
            bail!("Git stdout pipe is unavailable");
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            terminate_git_process_group(&mut child);
            bail!("Git stderr pipe is unavailable");
        }
    };

    let overflow = Arc::new(AtomicBool::new(false));
    let stdout_overflow = overflow.clone();
    let stderr_overflow = overflow.clone();
    let stdout_thread = thread::spawn(move || read_bounded(stdout, stdout_overflow));
    let stderr_thread = thread::spawn(move || read_bounded(stderr, stderr_overflow));

    let (helper_directory, writer_thread) = match channel {
        Some(channel) => {
            let CredentialChannel {
                _helper_directory,
                url: _,
                read_end,
                write_end,
                token,
            } = channel;
            drop(read_end);
            let writer = thread::spawn(move || {
                // SAFETY: `write_end` is uniquely owned by this thread.
                let mut pipe = File::from(write_end);
                pipe.write_all(token.as_slice())
            });
            (Some(_helper_directory), Some(writer))
        }
        None => (None, None),
    };

    let status = wait_bounded_child(&mut child, &overflow, timeout)?;
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("Git stdout drain thread panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("Git stderr drain thread panicked"))??;
    if let Some(writer) = writer_thread {
        let written = writer
            .join()
            .map_err(|_| anyhow!("Git credential writer thread panicked"))?;
        if status.success() && written.is_err() {
            bail!("Git credential channel failed");
        }
    }
    drop(helper_directory);
    if overflow.load(Ordering::Acquire) {
        bail!("Git subprocess output exceeded its bounded cap");
    }
    Ok(BoundedOutput {
        status,
        stdout,
        stderr,
    })
}

fn wait_bounded_child(
    child: &mut Child,
    overflow: &AtomicBool,
    timeout: Duration,
) -> Result<ExitStatus> {
    let started = Instant::now();
    loop {
        if overflow.load(Ordering::Acquire) || started.elapsed() >= timeout {
            terminate_git_process_group(child);
            if overflow.load(Ordering::Acquire) {
                bail!("Git subprocess output exceeded its bounded cap");
            }
            bail!("Git subprocess exceeded its bounded operation timeout");
        }
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(error) => {
                terminate_git_process_group(child);
                return Err(error).context("failed to poll bounded Git subprocess");
            }
        }
    }
}

fn terminate_git_process_group(child: &mut Child) {
    if let Ok(pid) = i32::try_from(child.id()) {
        // SAFETY: a negative PID addresses the private process group created
        // for this child. ESRCH is harmless when the process already exited.
        let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn read_bounded(mut pipe: impl Read, overflow: Arc<AtomicBool>) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = pipe.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if bytes.len().saturating_add(read) > MAX_GIT_OUTPUT_BYTES {
            overflow.store(true, Ordering::Release);
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

fn exactly_one_line(bytes: &[u8], label: &str) -> Result<String> {
    let text = std::str::from_utf8(bytes).with_context(|| format!("{label} is not UTF-8"))?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    if text.is_empty() || text.contains(['\n', '\r']) {
        bail!("{label} did not contain exactly one bounded line");
    }
    Ok(text.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::{
        GITHUB_REVIEW_CHECK_NAME, GitHubAppBinding, GitHubPermissionLevel, GitHubReviewerAppBinding,
    };
    use crate::orchestrator::workspace::ProjectTasksWorkspace;
    use crate::worker::profile::ReviewerId;
    use std::collections::BTreeMap;

    struct Fixture {
        workspace: TasksWorkspace,
        _workspace_owner: ProjectTasksWorkspace,
        manager: GitWorkspaceManager,
        binding: GitHubPullRequestBinding,
        run: GitHubRunBinding,
        plan_hash: String,
        primary: PathBuf,
        bare: PathBuf,
        seed: PathBuf,
        base_sha: String,
        primary_branch_before: String,
        primary_head_before: String,
        primary_status_before: Vec<u8>,
        primary_origin_before: String,
        _temp: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let bare = temp.path().join("remote.git");
            let seed = temp.path().join("seed");
            let primary = temp.path().join("primary");
            let project = temp.path().join("project");
            fs::create_dir(&project).unwrap();

            git_ok(
                temp.path(),
                ["init", "--bare", "--initial-branch=master", path(&bare)],
            );
            git_ok(
                temp.path(),
                ["init", "--initial-branch=master", path(&seed)],
            );
            fs::write(seed.join("README.md"), "base\n").unwrap();
            git_ok(&seed, ["add", "README.md"]);
            git_ok(
                &seed,
                [
                    "-c",
                    "user.name=seed",
                    "-c",
                    "user.email=seed@example.com",
                    "commit",
                    "-m",
                    "base",
                ],
            );
            git_ok(&seed, ["remote", "add", "origin", path(&bare)]);
            git_ok(&seed, ["push", "origin", "master"]);
            git_ok(temp.path(), ["clone", path(&bare), path(&primary)]);
            let primary = fs::canonicalize(primary).unwrap();
            let bare = fs::canonicalize(bare).unwrap();
            let seed = fs::canonicalize(seed).unwrap();
            let base_sha = git_line(&primary, ["rev-parse", "HEAD"]);

            fs::write(primary.join("README.md"), "base\nuser dirty change\n").unwrap();
            fs::write(primary.join("local-only.txt"), "primary checkout only\n").unwrap();
            fs::write(primary.join("staged-user.txt"), "staged primary index\n").unwrap();
            git_ok(&primary, ["add", "staged-user.txt"]);
            git_ok(
                &primary,
                [
                    "remote",
                    "set-url",
                    "origin",
                    "/tmp/hcom-do-not-trust-origin",
                ],
            );
            let primary_branch_before = git_line(&primary, ["symbolic-ref", "--short", "HEAD"]);
            let primary_head_before = git_line(&primary, ["rev-parse", "HEAD"]);
            let primary_status_before = git_bytes(
                &primary,
                ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            );
            let primary_origin_before = git_line(&primary, ["remote", "get-url", "origin"]);

            let workspace_owner = ProjectTasksWorkspace::open(&project).unwrap();
            let workspace = workspace_owner.claim_run("run-pr02").unwrap();
            let plan_hash = "a".repeat(64);
            let branch = format!("hcom/run-pr02-{}", &plan_hash[..12]);
            let binding = binding(&primary);
            let run = GitHubRunBinding {
                inspected_repository_id: binding.repository_id,
                expected_base_ref: "refs/heads/master".into(),
                expected_base_sha: base_sha.clone(),
                ruleset_attestation_sha256: "b".repeat(64),
                inspection_id: "inspection-pr02".into(),
                generated_run_branch: branch,
            };
            Self {
                workspace,
                _workspace_owner: workspace_owner,
                manager: GitWorkspaceManager::for_local_bare(bare.clone()),
                binding,
                run,
                plan_hash,
                primary,
                bare,
                seed,
                base_sha,
                primary_branch_before,
                primary_head_before,
                primary_status_before,
                primary_origin_before,
                _temp: temp,
            }
        }

        fn prepare(&self) -> PreparedGitWorkspace {
            self.manager
                .prepare(
                    &self.workspace,
                    &self.binding,
                    &self.run,
                    &self.plan_hash,
                    None,
                )
                .unwrap()
        }

        fn assert_primary_unchanged(&self) {
            assert_eq!(
                git_line(&self.primary, ["symbolic-ref", "--short", "HEAD"]),
                self.primary_branch_before
            );
            assert_eq!(
                git_line(&self.primary, ["rev-parse", "HEAD"]),
                self.primary_head_before
            );
            assert_eq!(
                git_bytes(
                    &self.primary,
                    ["status", "--porcelain=v1", "-z", "--untracked-files=all"]
                ),
                self.primary_status_before
            );
            assert_eq!(
                git_line(&self.primary, ["remote", "get-url", "origin"]),
                self.primary_origin_before
            );
        }
    }

    #[test]
    fn exact_base_append_only_push_and_finalization_leave_primary_checkout_untouched() {
        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        assert_eq!(
            prepared.worktree_path(),
            fixture.workspace.repository_path()
        );
        assert_eq!(prepared.base_sha(), fixture.base_sha);
        assert_eq!(
            git_line(prepared.worktree_path(), ["rev-parse", "HEAD"]),
            fixture.base_sha
        );
        fixture.assert_primary_unchanged();

        let first_head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task-one.txt",
            "first\n",
            "task one",
            true,
        );
        let first = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        assert_eq!(first.head_sha, first_head);
        assert_eq!(first.parent_sha, fixture.base_sha);
        let pushed = fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &first, None, None)
            .unwrap();
        assert_eq!(pushed.published_head, first_head);
        assert!(!pushed.reconciled_after_command_failure);
        assert_eq!(
            bare_ref(&fixture.bare, &prepared.branch_ref),
            Some(first_head.clone())
        );

        let second_head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task-one.txt",
            "first\ncorrection\n",
            "address review",
            true,
        );
        let second = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, Some(&first_head), None)
            .unwrap();
        assert_eq!(second.head_sha, second_head);
        assert_eq!(second.parent_sha, first_head);
        fixture
            .manager
            .push_candidate(
                &prepared,
                &fixture.binding,
                &second,
                Some(&first_head),
                None,
            )
            .unwrap();
        assert_eq!(
            git_line(
                prepared.worktree_path(),
                [
                    "rev-list",
                    "--count",
                    &format!("{}..{second_head}", fixture.base_sha)
                ]
            ),
            "2"
        );
        fixture
            .manager
            .validate_published_checkout(&prepared, &fixture.binding, &second_head, None)
            .unwrap();
        fixture.assert_primary_unchanged();

        let authorization = FinalizationAuthorization::after_confirmed_merge(
            &second_head,
            &second_head,
            &"f".repeat(40),
            true,
            true,
            0,
        )
        .unwrap();
        let finalized = fixture
            .manager
            .finalize_success(&prepared, &fixture.binding, &authorization, None)
            .unwrap();
        assert!(finalized.local_worktree_removed);
        assert!(finalized.local_refs_removed);
        assert_eq!(
            finalized.remote_ref_outcome,
            RemoteRefFinalizationOutcome::Deleted
        );
        assert!(!prepared.worktree_path().exists());
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), None);
        assert_eq!(local_ref(&fixture.primary, &prepared.branch_ref), None);
        assert_eq!(
            local_ref(&fixture.primary, &prepared.namespaced_base_ref),
            None
        );
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn finalization_preserves_artifacts_when_primary_checkout_adopts_run_branch() {
        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "candidate\n",
            "candidate",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();

        git_ok(
            &fixture.primary,
            ["symbolic-ref", "HEAD", &prepared.branch_ref],
        );
        let primary_status_before = git_bytes(
            &fixture.primary,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        );
        let authorization = FinalizationAuthorization::after_confirmed_merge(
            &head,
            &head,
            &"f".repeat(40),
            true,
            true,
            0,
        )
        .unwrap();
        let error = fixture
            .manager
            .finalize_success(&prepared, &fixture.binding, &authorization, None)
            .unwrap_err();

        assert!(error.to_string().contains("checked out outside"));
        assert_eq!(
            git_line(&fixture.primary, ["symbolic-ref", "HEAD"]),
            prepared.branch_ref
        );
        assert_eq!(
            git_bytes(
                &fixture.primary,
                ["status", "--porcelain=v1", "-z", "--untracked-files=all"]
            ),
            primary_status_before
        );
        assert!(prepared.worktree_path().is_dir());
        assert_eq!(
            local_ref(&fixture.primary, &prepared.branch_ref),
            Some(head.clone())
        );
        assert_eq!(
            local_ref(&fixture.primary, &prepared.namespaced_base_ref),
            Some(fixture.base_sha.clone())
        );
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), Some(head));
    }

    #[test]
    fn finalization_preserves_local_branch_when_it_moves_during_exact_deletion() {
        let mut fixture = Fixture::new();
        let prepared = fixture.prepare();
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "candidate\n",
            "candidate",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();

        let wrapper = fixture._temp.path().join("git-move-ref-before-delete");
        fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nmove=false\nprevious=\nfor arg in \"$@\"; do\n  [ \"$previous\" != -d ] || [ \"$arg\" != '{}' ] || move=true\n  previous=$arg\ndone\nif [ \"$move\" = true ]; then\n  /usr/bin/git update-ref '{}' '{}' '{}' || exit $?\nfi\nexec /usr/bin/git \"$@\"\n",
                prepared.branch_ref,
                prepared.branch_ref,
                fixture.base_sha,
                head,
            ),
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        fixture.manager =
            GitWorkspaceManager::for_local_bare_with_git(fixture.bare.clone(), wrapper);

        let authorization = FinalizationAuthorization::after_confirmed_merge(
            &head,
            &head,
            &"f".repeat(40),
            true,
            true,
            0,
        )
        .unwrap();
        let error = fixture
            .manager
            .finalize_success(&prepared, &fixture.binding, &authorization, None)
            .unwrap_err();

        assert!(error.to_string().contains("changed and was preserved"));
        assert!(!prepared.worktree_path().exists());
        assert_eq!(
            local_ref(&fixture.primary, &prepared.branch_ref),
            Some(fixture.base_sha.clone())
        );
        assert_eq!(
            local_ref(&fixture.primary, &prepared.namespaced_base_ref),
            Some(fixture.base_sha.clone())
        );
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), Some(head));
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn fetch_and_push_failure_after_effect_are_reconciled_without_repeating_the_effect() {
        let mut fixture = Fixture::new();
        let wrapper = fixture._temp.path().join("git-fail-after-fetch-push");
        fs::write(
            &wrapper,
            "#!/bin/sh\nmutation=false\nfor arg in \"$@\"; do\n  case \"$arg\" in fetch|push) mutation=true ;; esac\ndone\n/usr/bin/git \"$@\"\nstatus=$?\n[ \"$status\" -eq 0 ] || exit \"$status\"\n[ \"$mutation\" = false ] || exit 1\n",
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        fixture.manager =
            GitWorkspaceManager::for_local_bare_with_git(fixture.bare.clone(), wrapper);

        let prepared = fixture.prepare();
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "candidate\n",
            "candidate",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        let pushed = fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();
        assert!(pushed.reconciled_after_command_failure);
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), Some(head));
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn preparation_stops_on_base_drift_before_creating_refs_or_worktree() {
        let fixture = Fixture::new();
        fs::write(fixture.seed.join("base-moved.txt"), "moved\n").unwrap();
        git_ok(&fixture.seed, ["add", "base-moved.txt"]);
        git_ok(
            &fixture.seed,
            [
                "-c",
                "user.name=seed",
                "-c",
                "user.email=seed@example.com",
                "commit",
                "-m",
                "move base",
            ],
        );
        git_ok(&fixture.seed, ["push", "origin", "master"]);

        let error = fixture
            .manager
            .prepare(
                &fixture.workspace,
                &fixture.binding,
                &fixture.run,
                &fixture.plan_hash,
                None,
            )
            .unwrap_err();
        assert!(error.to_string().contains("base ref drifted"));
        assert!(!fixture.workspace.repository_path().exists());
        assert_eq!(
            local_ref(
                &fixture.primary,
                &format!("refs/heads/{}", fixture.run.generated_run_branch)
            ),
            None
        );
        assert_eq!(
            local_ref(
                &fixture.primary,
                &format!("refs/hcom/runs/{}/base", fixture.workspace.run_id())
            ),
            None
        );
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn in_run_base_drift_stops_and_preserves_the_existing_run_workspace() {
        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        fs::write(fixture.seed.join("later-base.txt"), "moved later\n").unwrap();
        git_ok(&fixture.seed, ["add", "later-base.txt"]);
        git_ok(
            &fixture.seed,
            [
                "-c",
                "user.name=seed",
                "-c",
                "user.email=seed@example.com",
                "commit",
                "-m",
                "later base move",
            ],
        );
        git_ok(&fixture.seed, ["push", "origin", "master"]);
        let error = fixture
            .manager
            .validate_remote_base(&prepared, &fixture.binding, &fixture.base_sha, None)
            .unwrap_err();
        assert!(error.to_string().contains("base ref drifted"));
        assert!(prepared.worktree_path().is_dir());
        assert_eq!(
            local_ref(&fixture.primary, &prepared.namespaced_base_ref),
            Some(fixture.base_sha.clone())
        );
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn commit_gate_rejects_wrong_topology_identity_signoff_dirty_state_and_hcom_artifacts() {
        let wrong_identity = Fixture::new();
        let prepared = wrong_identity.prepare();
        commit_file(
            prepared.worktree_path(),
            &GitHubCommitIdentity {
                name: "wrong[bot]".into(),
                email: "9+wrong[bot]@users.noreply.github.com".into(),
            },
            "wrong.txt",
            "wrong\n",
            "wrong identity",
            true,
        );
        assert!(
            wrong_identity
                .manager
                .validate_developer_commit(&prepared, &wrong_identity.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("author and committer")
        );

        let no_signoff = Fixture::new();
        let prepared = no_signoff.prepare();
        commit_file(
            prepared.worktree_path(),
            &no_signoff.binding.developer_commit_identity(),
            "unsigned.txt",
            "unsigned\n",
            "missing signoff",
            false,
        );
        assert!(
            no_signoff
                .manager
                .validate_developer_commit(&prepared, &no_signoff.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("Signed-off-by")
        );

        let body_after_signoff = Fixture::new();
        let prepared = body_after_signoff.prepare();
        let identity = body_after_signoff.binding.developer_commit_identity();
        let false_trailer = format!(
            "false trailer\nSigned-off-by: {} <{}>\nordinary body text",
            identity.name, identity.email
        );
        commit_file(
            prepared.worktree_path(),
            &identity,
            "false-trailer.txt",
            "false trailer\n",
            &false_trailer,
            false,
        );
        assert!(
            body_after_signoff
                .manager
                .validate_developer_commit(&prepared, &body_after_signoff.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("Signed-off-by")
        );

        let two_commits = Fixture::new();
        let prepared = two_commits.prepare();
        commit_file(
            prepared.worktree_path(),
            &two_commits.binding.developer_commit_identity(),
            "one.txt",
            "one\n",
            "one",
            true,
        );
        commit_file(
            prepared.worktree_path(),
            &two_commits.binding.developer_commit_identity(),
            "two.txt",
            "two\n",
            "two",
            true,
        );
        assert!(
            two_commits
                .manager
                .validate_developer_commit(&prepared, &two_commits.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );

        let dirty = Fixture::new();
        let prepared = dirty.prepare();
        commit_file(
            prepared.worktree_path(),
            &dirty.binding.developer_commit_identity(),
            "clean.txt",
            "clean\n",
            "clean",
            true,
        );
        fs::write(prepared.worktree_path().join("untracked.txt"), "dirty\n").unwrap();
        assert!(
            dirty
                .manager
                .validate_developer_commit(&prepared, &dirty.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("not clean")
        );

        let artifact = Fixture::new();
        let prepared = artifact.prepare();
        fs::create_dir(prepared.worktree_path().join("hcom-tasks")).unwrap();
        commit_file(
            prepared.worktree_path(),
            &artifact.binding.developer_commit_identity(),
            "hcom-tasks/leak.txt",
            "leak\n",
            "leak artifact",
            true,
        );
        assert!(
            artifact
                .manager
                .validate_developer_commit(&prepared, &artifact.binding, None, None)
                .unwrap_err()
                .to_string()
                .contains("hcom-tasks")
        );
    }

    #[test]
    fn remote_mutation_and_validation_failure_preserve_managed_artifacts() {
        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "initial\n",
            "initial",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();
        let correction = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "initial\ncorrection\n",
            "correction",
            true,
        );
        git_ok(
            &fixture.bare,
            ["update-ref", &prepared.branch_ref, &fixture.base_sha, &head],
        );
        let error = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, Some(&head), None)
            .unwrap_err();
        assert!(error.to_string().contains("remotely") || error.to_string().contains("remote"));
        assert!(prepared.worktree_path().is_dir());
        assert_eq!(
            local_ref(&fixture.primary, &prepared.branch_ref),
            Some(correction)
        );
        assert_eq!(
            local_ref(&fixture.primary, &prepared.namespaced_base_ref),
            Some(fixture.base_sha.clone())
        );
        assert_eq!(
            bare_ref(&fixture.bare, &prepared.branch_ref),
            Some(fixture.base_sha.clone())
        );
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn supervisor_push_disables_hooks_helpers_and_rejects_rewrite_or_extra_header_config() {
        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        let hook_ran = fixture._temp.path().join("hook-ran");
        let helper_ran = fixture._temp.path().join("helper-ran");
        let hook = fixture.primary.join(".git/hooks/pre-push");
        fs::write(
            &hook,
            format!("#!/bin/sh\nprintf ran >'{}'\n", hook_ran.display()),
        )
        .unwrap();
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        git_ok(
            &fixture.primary,
            [
                "config",
                "credential.helper",
                &format!("!printf ran >'{}'", helper_ran.display()),
            ],
        );
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "candidate\n",
            "candidate",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), Some(head));
        assert!(!hook_ran.exists());
        assert!(!helper_ran.exists());

        git_ok(
            &fixture.primary,
            [
                "config",
                "url.file:///tmp/hostile.insteadOf",
                "https://github.com/",
            ],
        );
        let error = fixture
            .manager
            .validate_published_checkout(&prepared, &fixture.binding, &candidate.head_sha, None)
            .unwrap_err();
        assert!(error.to_string().contains("rewrite"));
        git_ok(
            &fixture.primary,
            ["config", "--unset-all", "url.file:///tmp/hostile.insteadOf"],
        );
        git_ok(
            &fixture.primary,
            ["config", "http.extraHeader", "Authorization: hostile"],
        );
        let error = fixture
            .manager
            .validate_published_checkout(&prepared, &fixture.binding, &candidate.head_sha, None)
            .unwrap_err();
        assert!(error.to_string().contains("HTTP header"));
        assert!(prepared.worktree_path().is_dir());
        fixture.assert_primary_unchanged();
    }

    #[test]
    fn fixed_https_askpass_keeps_token_out_of_argv_environment_and_errors() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repository");
        fs::create_dir(&repository).unwrap();
        let binding = binding(&repository);
        assert_eq!(
            fixed_github_https_url(&binding),
            "https://github.com/owner/repository.git"
        );
        let capture_args = temp.path().join("args");
        let capture_env = temp.path().join("environment");
        let capture_user = temp.path().join("username");
        let capture_password = temp.path().join("password");
        let inherited_fd = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temp.path().join("unrelated-descriptor"))
            .unwrap();
        let inherited_fd_number = inherited_fd.as_raw_fd();
        // SAFETY: this test owns the descriptor and intentionally makes it
        // inheritable to prove the Git child closes every unrelated FD.
        unsafe {
            let flags = libc::fcntl(inherited_fd_number, libc::F_GETFD);
            assert!(flags >= 0);
            assert_eq!(
                libc::fcntl(
                    inherited_fd_number,
                    libc::F_SETFD,
                    flags & !libc::FD_CLOEXEC,
                ),
                0
            );
        }
        let inherited_fd_leaked = temp.path().join("unrelated-fd-leaked");
        let fake = temp.path().join("git-fake");
        let sha = "a".repeat(40);
        fs::write(
            &fake,
            format!(
                "#!/bin/sh\nfor arg in \"$@\"; do [ \"$arg\" = config ] && exit 0; done\n[ ! -e '/proc/self/fd/{}' ] || printf leaked >'{}'\nprintf '%s\\0' \"$@\" >'{}'\n/usr/bin/env -0 >'{}'\n\"$GIT_ASKPASS\" \"Username for 'https://github.com/owner/repository.git': \" >'{}'\n\"$GIT_ASKPASS\" \"Password for 'https://x-access-token@github.com/owner/repository.git': \" >'{}'\nprintf '%s\\t%s\\n' '{}' 'refs/heads/master'\n",
                inherited_fd_number,
                inherited_fd_leaked.display(),
                capture_args.display(),
                capture_env.display(),
                capture_user.display(),
                capture_password.display(),
                sha,
            ),
        )
        .unwrap();
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
        let manager = GitWorkspaceManager::with_git_program(fake);
        let token_text = "fixture-token-with-no-real-prefix";
        let credential = GitCredential::new(token_text.as_bytes().to_vec()).unwrap();
        let observed = manager
            .remote_ref(
                &repository,
                &binding,
                "refs/heads/master",
                Some(&credential),
            )
            .unwrap();
        assert_eq!(observed, Some(sha));
        assert!(!inherited_fd_leaked.exists());
        assert_eq!(
            fs::read_to_string(capture_user).unwrap(),
            "x-access-token\n"
        );
        assert_eq!(fs::read_to_string(capture_password).unwrap(), token_text);
        let args = fs::read(capture_args).unwrap();
        let environment = fs::read(capture_env).unwrap();
        assert!(
            !args
                .windows(token_text.len())
                .any(|window| window == token_text.as_bytes())
        );
        assert!(
            !environment
                .windows(token_text.len())
                .any(|window| window == token_text.as_bytes())
        );
        assert!(
            environment
                .split(|byte| *byte == 0)
                .any(|entry| entry.starts_with(b"HCOM_GITHUB_CREDENTIAL_FD="))
        );
        assert!(
            args.split(|byte| *byte == 0)
                .any(|arg| arg == b"core.hooksPath=/dev/null")
        );
        assert!(
            args.split(|byte| *byte == 0)
                .all(|arg| arg != b"--force" && !arg.starts_with(b"--force-with-lease"))
        );

        let leaking_fake = temp.path().join("git-leaking-fake");
        fs::write(
            &leaking_fake,
            "#!/bin/sh\nfor arg in \"$@\"; do [ \"$arg\" = config ] && exit 0; done\nsecret=$(\"$GIT_ASKPASS\" \"Password for 'https://x-access-token@github.com/owner/repository.git': \")\nprintf '%s\\n' \"$secret\" >&2\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&leaking_fake, fs::Permissions::from_mode(0o755)).unwrap();
        let leaking_manager = GitWorkspaceManager::with_git_program(leaking_fake);
        let error = leaking_manager
            .remote_ref(
                &repository,
                &binding,
                "refs/heads/master",
                Some(&credential),
            )
            .unwrap_err();
        assert!(!format!("{error:#}").contains(token_text));
    }

    #[test]
    fn finalization_requires_merge_proof_and_preserves_unchanged_remote_on_optional_delete_failure()
    {
        assert!(
            FinalizationAuthorization::after_confirmed_merge(
                &"a".repeat(40),
                &"b".repeat(40),
                &"c".repeat(40),
                true,
                true,
                0,
            )
            .is_err()
        );
        assert!(
            FinalizationAuthorization::after_confirmed_merge(
                &"a".repeat(40),
                &"a".repeat(40),
                &"c".repeat(40),
                true,
                true,
                1,
            )
            .is_err()
        );

        let fixture = Fixture::new();
        let prepared = fixture.prepare();
        let head = commit_file(
            prepared.worktree_path(),
            &fixture.binding.developer_commit_identity(),
            "task.txt",
            "candidate\n",
            "candidate",
            true,
        );
        let candidate = fixture
            .manager
            .validate_developer_commit(&prepared, &fixture.binding, None, None)
            .unwrap();
        fixture
            .manager
            .push_candidate(&prepared, &fixture.binding, &candidate, None, None)
            .unwrap();
        let hook = fixture.bare.join("hooks/pre-receive");
        fs::write(
            &hook,
            "#!/bin/sh\nwhile read old new ref; do [ \"$new\" = 0000000000000000000000000000000000000000 ] && exit 1; done\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        let authorization = FinalizationAuthorization::after_confirmed_merge(
            &head,
            &head,
            &"e".repeat(40),
            true,
            true,
            0,
        )
        .unwrap();
        let outcome = fixture
            .manager
            .finalize_success(&prepared, &fixture.binding, &authorization, None)
            .unwrap();
        assert_eq!(
            outcome.remote_ref_outcome,
            RemoteRefFinalizationOutcome::PreservedAfterDeleteFailure
        );
        assert_eq!(bare_ref(&fixture.bare, &prepared.branch_ref), Some(head));
        assert!(!prepared.worktree_path().exists());
        fixture.assert_primary_unchanged();
    }

    fn binding(repository_root: &Path) -> GitHubPullRequestBinding {
        let app = |id: u64, slug: &str, bot_user_id: u64| GitHubAppBinding {
            app_id: id,
            installation_id: id + 100,
            slug: slug.into(),
            bot_user_id,
            effective_permissions: BTreeMap::from([
                ("contents".into(), GitHubPermissionLevel::Write),
                ("pull_requests".into(), GitHubPermissionLevel::Write),
            ]),
        };
        GitHubPullRequestBinding {
            owner: "owner".into(),
            repository: "repository".into(),
            repository_id: 42,
            visibility: "private".into(),
            local_repository_root: repository_root.to_string_lossy().into_owned(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 21_600,
            delete_remote_branch_after_merge: true,
            architect_app: app(1, "architect-app", 1001),
            developer_app: app(2, "developer-app", 1002),
            reviewer_apps: vec![GitHubReviewerAppBinding {
                reviewer_id: ReviewerId::Reviewer1,
                app: app(3, "reviewer-app", 1003),
            }],
            review_check_name: GITHUB_REVIEW_CHECK_NAME.into(),
        }
    }

    fn commit_file(
        repository: &Path,
        identity: &GitHubCommitIdentity,
        relative_path: &str,
        contents: &str,
        subject: &str,
        signoff: bool,
    ) -> String {
        let destination = repository.join(relative_path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&destination, contents).unwrap();
        git_ok(repository, ["add", "--", relative_path]);
        let name = format!("user.name={}", identity.name);
        let email = format!("user.email={}", identity.email);
        let mut args = vec![
            "-c".to_string(),
            name,
            "-c".to_string(),
            email,
            "commit".to_string(),
        ];
        if signoff {
            args.push("--signoff".into());
        }
        args.extend(["-m".into(), subject.into()]);
        git_ok_owned(repository, args);
        git_line(repository, ["rev-parse", "HEAD"])
    }

    fn bare_ref(bare: &Path, reference: &str) -> Option<String> {
        local_ref(bare, reference)
    }

    fn local_ref(repository: &Path, reference: &str) -> Option<String> {
        let commit_reference = format!("{reference}^{{commit}}");
        let output = Command::new("git")
            .arg("--no-replace-objects")
            .args(["rev-parse", "--verify", "--quiet", &commit_reference])
            .current_dir(repository)
            .output()
            .unwrap();
        match output.status.code() {
            Some(0) => Some(String::from_utf8(output.stdout).unwrap().trim_end().into()),
            Some(1) => None,
            code => panic!("show-ref failed with {code:?}"),
        }
    }

    fn git_ok<'a>(repository: &Path, args: impl IntoIterator<Item = &'a str>) {
        let status = Command::new("git")
            .args(args)
            .current_dir(repository)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn git_ok_owned(repository: &Path, args: Vec<String>) {
        let status = Command::new("git")
            .args(args)
            .current_dir(repository)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn git_line<'a>(repository: &Path, args: impl IntoIterator<Item = &'a str>) -> String {
        String::from_utf8(git_bytes(repository, args))
            .unwrap()
            .trim_end()
            .into()
    }

    fn git_bytes<'a>(repository: &Path, args: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
        let output = Command::new("git")
            .args(args)
            .current_dir(repository)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    fn path(path: &Path) -> &str {
        path.to_str().unwrap()
    }
}
