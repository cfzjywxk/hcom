//! Bounded, non-secret GitHub evidence under one existing run workspace.
//!
//! Evidence is an immutable handoff record, not a restart/adoption store. Each
//! destination is published atomically and exclusively through a same-directory
//! temporary inode plus `link(2)`; an existing record is never overwritten.

use super::{validate_git_sha, validate_id, validate_sha256, validate_slug};
use crate::control_api::{GitHubPullRequestBinding, GitHubRunBinding};
use crate::orchestrator::workspace::TasksWorkspace;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const MAX_EVIDENCE_FILE_BYTES: usize = 64 * 1024;
const MAX_EVIDENCE_RUN_BYTES: usize = 64 * 1024 * 1024;
const MAX_EVIDENCE_URL_BYTES: usize = 2_048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EvidencePath {
    Binding,
    RepositoryPrepared,
    PullRequest,
    Candidate {
        ordinal: u32,
        task_key: String,
        generation: u32,
    },
    DeveloperComment {
        ordinal: u32,
        task_key: String,
        generation: u32,
    },
    Review {
        ordinal: u32,
        task_key: String,
        generation: u32,
        reviewer: crate::worker::profile::ReviewerId,
    },
    Check {
        ordinal: u32,
        task_key: String,
        generation: u32,
    },
    Merge,
    Finalization,
}

impl EvidencePath {
    fn expected_kind(&self) -> Option<EvidenceKind> {
        match self {
            Self::Binding => None,
            Self::RepositoryPrepared => Some(EvidenceKind::RepositoryPrepared),
            Self::PullRequest => Some(EvidenceKind::PullRequest),
            Self::Candidate { .. } => Some(EvidenceKind::Candidate),
            Self::DeveloperComment { .. } => Some(EvidenceKind::DeveloperComment),
            Self::Review { .. } => Some(EvidenceKind::Review),
            Self::Check { .. } => Some(EvidenceKind::Check),
            Self::Merge => Some(EvidenceKind::Merge),
            Self::Finalization => Some(EvidenceKind::Finalization),
        }
    }

    fn relative_path(&self) -> Result<PathBuf> {
        let task_root = |ordinal: u32, task_key: &str| -> Result<PathBuf> {
            if !(1..=64).contains(&ordinal) {
                bail!("GitHub evidence task ordinal is outside 1..=64");
            }
            validate_plain_component("GitHub evidence task key", task_key)?;
            Ok(PathBuf::from(format!("tasks/{ordinal:02}-{task_key}")))
        };
        let generation_name = |generation: u32| -> Result<String> {
            if !(1..=20).contains(&generation) {
                bail!("GitHub evidence generation is outside 1..=20");
            }
            Ok(format!("generation-{generation:02}"))
        };
        Ok(match self {
            Self::Binding => PathBuf::from("binding.json"),
            Self::RepositoryPrepared => PathBuf::from("repository-prepared.json"),
            Self::PullRequest => PathBuf::from("pull-request.json"),
            Self::Candidate {
                ordinal,
                task_key,
                generation,
            } => task_root(*ordinal, task_key)?
                .join(format!("candidate-{}.json", generation_name(*generation)?)),
            Self::DeveloperComment {
                ordinal,
                task_key,
                generation,
            } => task_root(*ordinal, task_key)?.join(format!(
                "developer-{}-comment.json",
                generation_name(*generation)?
            )),
            Self::Review {
                ordinal,
                task_key,
                generation,
                reviewer,
            } => task_root(*ordinal, task_key)?.join(format!(
                "reviews/{}-{}.json",
                generation_name(*generation)?,
                reviewer.as_str()
            )),
            Self::Check {
                ordinal,
                task_key,
                generation,
            } => task_root(*ordinal, task_key)?
                .join(format!("checks/{}.json", generation_name(*generation)?)),
            Self::Merge => PathBuf::from("merge.json"),
            Self::Finalization => PathBuf::from("finalization.json"),
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct EvidenceActor {
    pub(crate) app_id: u64,
    pub(crate) slug: String,
    pub(crate) bot_user_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceKind {
    RepositoryPrepared,
    PullRequest,
    Candidate,
    DeveloperComment,
    Review,
    Check,
    Merge,
    Finalization,
}

/// Fixed non-secret evidence schema. It intentionally cannot hold request
/// headers, response bodies, PEM, JWT, token, prompt, or native-final bytes.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct GitHubEvidenceRecord {
    pub(crate) schema_version: u8,
    pub(crate) kind: EvidenceKind,
    pub(crate) operation_id: String,
    pub(crate) repository_id: u64,
    pub(crate) actor: Option<EvidenceActor>,
    pub(crate) object_id: Option<u64>,
    pub(crate) url: Option<String>,
    pub(crate) base_sha: Option<String>,
    pub(crate) head_sha: Option<String>,
    pub(crate) merge_sha: Option<String>,
    pub(crate) artifact_sha256: Option<String>,
    pub(crate) timestamp: String,
    pub(crate) outcome: String,
    pub(crate) reconciled_after_ambiguous_result: bool,
}

impl GitHubEvidenceRecord {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || self.repository_id == 0 {
            bail!("GitHub evidence schema/repository identifier is invalid");
        }
        validate_id("GitHub evidence operation ID", &self.operation_id)?;
        if let Some(actor) = &self.actor {
            if actor.app_id == 0 || actor.bot_user_id == 0 {
                bail!("GitHub evidence actor identifiers must be positive");
            }
            validate_slug("GitHub evidence App slug", &actor.slug)?;
        }
        if self.object_id == Some(0) {
            bail!("GitHub evidence object ID must be positive when present");
        }
        if let Some(url) = &self.url
            && (url.len() > MAX_EVIDENCE_URL_BYTES
                || url.contains(['\r', '\n'])
                || reqwest::Url::parse(url).map_or(true, |parsed| {
                    parsed.scheme() != "https"
                        || parsed.host_str() != Some("github.com")
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.port().is_some()
                }))
        {
            bail!("GitHub evidence URL is not a bounded github.com URL");
        }
        for (label, sha) in [
            ("GitHub evidence base SHA", self.base_sha.as_deref()),
            ("GitHub evidence head SHA", self.head_sha.as_deref()),
            ("GitHub evidence merge SHA", self.merge_sha.as_deref()),
        ] {
            if let Some(sha) = sha {
                validate_git_sha(label, sha)?;
            }
        }
        if let Some(hash) = &self.artifact_sha256 {
            validate_sha256("GitHub evidence artifact SHA-256", hash)?;
        }
        if self.timestamp.len() > 64
            || chrono::DateTime::parse_from_rfc3339(&self.timestamp).is_err()
        {
            bail!("GitHub evidence timestamp is not bounded RFC 3339");
        }
        if self.outcome.is_empty()
            || self.outcome.len() > 128
            || !self
                .outcome
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            bail!("GitHub evidence outcome is not a bounded outcome class");
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct BindingEvidence<'a> {
    schema_version: u8,
    delivery: &'a GitHubPullRequestBinding,
    run: &'a GitHubRunBinding,
}

#[derive(Debug)]
pub(crate) struct GitHubEvidenceWriter {
    root: PathBuf,
    bytes_written: usize,
    repository_id: Option<u64>,
}

impl GitHubEvidenceWriter {
    pub(crate) fn create(workspace: &TasksWorkspace) -> Result<Self> {
        let root = workspace.run_dir().join("github");
        fs::create_dir(&root).with_context(|| {
            format!(
                "failed to create GitHub evidence directory {}",
                root.display()
            )
        })?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        validate_private_directory(&root)?;
        Ok(Self {
            root,
            bytes_written: 0,
            repository_id: None,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write_binding(
        &mut self,
        delivery: &GitHubPullRequestBinding,
        run: &GitHubRunBinding,
    ) -> Result<PathBuf> {
        if self.repository_id.is_some() {
            bail!("GitHub binding evidence was already published");
        }
        if delivery.repository_id == 0
            || run.inspected_repository_id != delivery.repository_id
            || run.expected_base_ref != format!("refs/heads/{}", delivery.base_branch)
        {
            bail!("GitHub evidence binding is inconsistent");
        }
        validate_git_sha("GitHub evidence expected base", &run.expected_base_sha)?;
        match (
            delivery.delivery_policy,
            run.ruleset_attestation_sha256.as_deref(),
        ) {
            (crate::control_api::GitHubDeliveryPolicy::Manual, None) => {}
            (crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge, Some(attestation)) => {
                validate_sha256("GitHub evidence ruleset attestation", attestation)?
            }
            _ => bail!("GitHub evidence ruleset binding differs from delivery policy"),
        }
        let path = self.write_serialized(
            EvidencePath::Binding,
            &BindingEvidence {
                schema_version: 1,
                delivery,
                run,
            },
        )?;
        self.repository_id = Some(delivery.repository_id);
        Ok(path)
    }

    pub(crate) fn write(
        &mut self,
        path: EvidencePath,
        record: &GitHubEvidenceRecord,
    ) -> Result<PathBuf> {
        record.validate()?;
        let repository_id = self
            .repository_id
            .ok_or_else(|| anyhow::anyhow!("GitHub binding evidence must be published first"))?;
        if record.repository_id != repository_id {
            bail!("GitHub evidence repository differs from the frozen binding");
        }
        if path.expected_kind().is_none() {
            bail!("GitHub binding evidence requires the typed binding writer");
        }
        if path.expected_kind() != Some(record.kind) {
            bail!("GitHub evidence kind does not match its immutable path");
        }
        self.write_serialized(path, record)
    }

    fn write_serialized<T: Serialize>(
        &mut self,
        evidence_path: EvidencePath,
        value: &T,
    ) -> Result<PathBuf> {
        let relative = evidence_path.relative_path()?;
        let mut bytes = serde_json::to_vec_pretty(value)
            .context("failed to serialize bounded GitHub evidence")?;
        bytes.push(b'\n');
        if bytes.len() > MAX_EVIDENCE_FILE_BYTES {
            bail!("GitHub evidence file exceeds the 64-KiB bound");
        }
        let new_total = self
            .bytes_written
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("GitHub evidence byte accounting overflow"))?;
        if new_total > MAX_EVIDENCE_RUN_BYTES {
            bail!("GitHub evidence exceeds the 64-MiB per-run bound");
        }
        let destination = self.root.join(relative);
        let parent = destination
            .parent()
            .ok_or_else(|| anyhow::anyhow!("GitHub evidence path has no parent"))?;
        ensure_private_tree(&self.root, parent)?;
        atomic_exclusive_write(&destination, &bytes)?;
        self.bytes_written = new_total;
        Ok(destination)
    }
}

fn validate_plain_component(label: &str, value: &str) -> Result<()> {
    validate_id(label, value)?;
    if !matches!(
        Path::new(value).components().collect::<Vec<_>>().as_slice(),
        [Component::Normal(_)]
    ) {
        bail!("{label} is not one plain path component");
    }
    Ok(())
}

fn ensure_private_tree(root: &Path, target: &Path) -> Result<()> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| anyhow::anyhow!("GitHub evidence path escaped its run root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("GitHub evidence path contains an unsafe component");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(_) => validate_private_directory(&current)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("failed to create {}", current.display()))?;
                fs::set_permissions(&current, fs::Permissions::from_mode(0o700))?;
                validate_private_directory(&current)?;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect {}", current.display()));
            }
        }
    }
    Ok(())
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    let euid = unsafe { libc::geteuid() };
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != euid
        || metadata.permissions().mode() & 0o077 != 0
    {
        bail!("GitHub evidence directory is not a private current-user-owned plain directory");
    }
    Ok(())
}

fn atomic_exclusive_write(destination: &Path, contents: &[u8]) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| anyhow::anyhow!("GitHub evidence destination has no parent"))?;
    let temporary = parent.join(format!(".hcom-evidence-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&temporary)
        .with_context(|| format!("failed to stage GitHub evidence in {}", parent.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != 0o600
    {
        bail!("staged GitHub evidence is not a private current-user-owned regular file");
    }
    let result = (|| -> Result<()> {
        file.write_all(contents)
            .context("failed to write staged GitHub evidence")?;
        file.sync_all()
            .context("failed to sync staged GitHub evidence")?;
        // `link` is atomic and fails with EEXIST, giving rename-no-replace
        // semantics without adopting or overwriting an existing record.
        fs::hard_link(&temporary, destination).with_context(|| {
            format!(
                "failed to publish exclusive GitHub evidence {}",
                destination.display()
            )
        })?;
        let directory = File::open(parent)?;
        // Ensure this descriptor never crosses a worker exec in the tiny
        // interval before it is dropped.
        let flags = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_GETFD) };
        if flags >= 0 {
            unsafe {
                libc::fcntl(
                    directory.as_raw_fd(),
                    libc::F_SETFD,
                    flags | libc::FD_CLOEXEC,
                )
            };
        }
        directory
            .sync_all()
            .context("failed to sync GitHub evidence directory")
    })();
    drop(file);
    let _ = fs::remove_file(&temporary);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::{
        GitHubAppBinding, GitHubInspectionBinding, GitHubPermissionLevel, GitHubReviewerAppBinding,
    };
    use crate::orchestrator::workspace::ProjectTasksWorkspace;
    use crate::worker::profile::ReviewerId;
    use std::collections::BTreeMap;

    fn record() -> GitHubEvidenceRecord {
        GitHubEvidenceRecord {
            schema_version: 1,
            kind: EvidenceKind::Review,
            operation_id: "review-task-1-generation-1-reviewer1".into(),
            repository_id: 99,
            actor: Some(EvidenceActor {
                app_id: 3,
                slug: "reviewer-one".into(),
                bot_user_id: 30,
            }),
            object_id: Some(7),
            url: Some("https://github.com/owner/repo/pull/1#review-7".into()),
            base_sha: Some("a".repeat(40)),
            head_sha: Some("b".repeat(40)),
            merge_sha: None,
            artifact_sha256: Some("c".repeat(64)),
            timestamp: "2026-08-10T12:00:00+08:00".into(),
            outcome: "lgtm".into(),
            reconciled_after_ambiguous_result: false,
        }
    }

    fn app(id: u64, slug: &str) -> GitHubAppBinding {
        GitHubAppBinding {
            app_id: id,
            installation_id: id + 10,
            slug: slug.into(),
            bot_user_id: id + 20,
            effective_permissions: BTreeMap::from([(
                "pull_requests".into(),
                GitHubPermissionLevel::Write,
            )]),
        }
    }

    fn binding() -> (GitHubPullRequestBinding, GitHubRunBinding) {
        let mut architect = app(1, "arch");
        architect.effective_permissions.extend([
            ("administration".into(), GitHubPermissionLevel::Read),
            ("checks".into(), GitHubPermissionLevel::Write),
            ("contents".into(), GitHubPermissionLevel::Write),
        ]);
        let mut developer = app(2, "dev");
        developer
            .effective_permissions
            .insert("contents".into(), GitHubPermissionLevel::Write);
        let delivery = GitHubPullRequestBinding {
            delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
            owner: "owner".into(),
            repository: "repo".into(),
            repository_id: 99,
            visibility: "private".into(),
            local_repository_root: "/repository".into(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 21_600,
            delete_remote_branch_after_merge: true,
            architect_app: architect,
            developer_app: developer,
            reviewer_apps: vec![GitHubReviewerAppBinding {
                reviewer_id: ReviewerId::Reviewer1,
                app: app(3, "reviewer-one"),
            }],
            review_check_name: "hcom/review".into(),
        };
        let inspection = GitHubInspectionBinding {
            inspected_repository_id: 99,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: Some("b".repeat(64)),
            inspection_id: "inspection-fixture".into(),
        };
        let run = GitHubRunBinding {
            inspected_repository_id: inspection.inspected_repository_id,
            expected_base_ref: inspection.expected_base_ref,
            expected_base_sha: inspection.expected_base_sha,
            ruleset_attestation_sha256: inspection.ruleset_attestation_sha256,
            inspection_id: inspection.inspection_id,
            generated_run_branch: "hcom/run-fixture-plan".into(),
        };
        (delivery, run)
    }

    #[test]
    fn evidence_is_private_atomic_exclusive_bounded_and_nonsecret() {
        let project = tempfile::tempdir().unwrap();
        let owner = ProjectTasksWorkspace::open(project.path()).unwrap();
        let workspace = owner.claim_run("run-fixture").unwrap();
        let mut writer = GitHubEvidenceWriter::create(&workspace).unwrap();
        assert!(writer.write(EvidencePath::Merge, &record()).is_err());
        let (delivery, run) = binding();
        writer.write_binding(&delivery, &run).unwrap();
        let path = EvidencePath::Review {
            ordinal: 1,
            task_key: "TASK-1".into(),
            generation: 1,
            reviewer: ReviewerId::Reviewer1,
        };
        let mut wrong_repository = record();
        wrong_repository.repository_id = 100;
        assert!(writer.write(path.clone(), &wrong_repository).is_err());
        assert!(
            writer
                .write(
                    EvidencePath::Candidate {
                        ordinal: 1,
                        task_key: "TASK-1".into(),
                        generation: 1,
                    },
                    &record(),
                )
                .is_err()
        );
        let destination = writer.write(path.clone(), &record()).unwrap();
        assert_eq!(
            destination,
            workspace
                .run_dir()
                .join("github/tasks/01-TASK-1/reviews/generation-01-reviewer1.json")
        );
        let bytes = fs::read(&destination).unwrap();
        assert!(bytes.len() <= MAX_EVIDENCE_FILE_BYTES);
        assert!(!bytes.windows(5).any(|window| window == b"token"));
        assert_eq!(
            fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(writer.write(path, &record()).is_err());

        let mut invalid_url = record();
        invalid_url.url = Some("https://github.com:8443/owner/repo/pull/1".into());
        assert!(invalid_url.validate().is_err());
    }

    #[test]
    fn binding_evidence_contains_only_frozen_nonsecret_contract() {
        let project = tempfile::tempdir().unwrap();
        let owner = ProjectTasksWorkspace::open(project.path()).unwrap();
        let workspace = owner.claim_run("run-fixture").unwrap();
        let mut writer = GitHubEvidenceWriter::create(&workspace).unwrap();
        let (delivery, run) = binding();
        let path = writer.write_binding(&delivery, &run).unwrap();
        let text = fs::read_to_string(path).unwrap();
        assert!(!text.contains("\"issues\""));
        assert!(!text.contains("private_key"));
        assert!(!text.contains("authorization"));
        assert!(!text.contains("jwt"));
    }
}
