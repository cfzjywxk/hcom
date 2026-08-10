//! Foreground, in-memory task supervisor for one `hcom arch` invocation.

pub mod core;
pub(crate) mod github;
#[cfg(target_os = "linux")]
pub(crate) mod task_lane;
#[cfg(target_os = "linux")]
pub mod workspace;

use crate::control_api::DeliveryBinding;
use crate::worker::environment::ParentEnvironment;
use crate::worker::profile::{ArchitectAdapter, SessionInvocationProfiles};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionStartup {
    pub(crate) run_id: String,
    pub(crate) project_root: PathBuf,
    /// Exact frozen Architect/worker profile, runtime-contract, and
    /// additional-directory binding used by every plan in this session.
    pub(crate) session_binding_hash: String,
}

#[derive(Clone)]
pub(crate) struct SessionRuntimeSources {
    parent_environment: ParentEnvironment,
    profiles: Option<SessionInvocationProfiles>,
    architect_additional_directories: Vec<PathBuf>,
    guardian_executable: PathBuf,
    delivery_binding: DeliveryBinding,
    github_runtime: Option<github::GitHubRuntimeBinding>,
}

impl SessionRuntimeSources {
    pub(crate) fn capture(
        parent_environment: impl Into<ParentEnvironment>,
        profiles: SessionInvocationProfiles,
        architect_additional_directories: Vec<PathBuf>,
    ) -> Result<Self> {
        profiles.validate()?;
        validate_architect_additional_directories(
            profiles.architect.adapter(),
            &architect_additional_directories,
        )?;
        Ok(Self {
            parent_environment: parent_environment.into(),
            profiles: Some(profiles),
            architect_additional_directories,
            guardian_executable: std::env::current_exe()
                .context("failed to resolve the current hcom Guardian executable")?,
            delivery_binding: DeliveryBinding::LocalCandidate,
            github_runtime: None,
        })
    }

    #[allow(
        dead_code,
        reason = "constructed by the later production GitHub preflight driver"
    )]
    pub(crate) fn capture_with_github(
        parent_environment: impl Into<ParentEnvironment>,
        profiles: SessionInvocationProfiles,
        architect_additional_directories: Vec<PathBuf>,
        github_runtime: github::GitHubRuntimeBinding,
    ) -> Result<Self> {
        let mut sources = Self::capture(
            parent_environment,
            profiles,
            architect_additional_directories,
        )?;
        github::validate_inspection_result(
            &github_runtime.binding,
            &github::GitHubInspectionResult {
                delivery_binding: github_runtime.binding.clone(),
                inspection: github_runtime.initial_inspection.clone(),
            },
        )?;
        sources.delivery_binding = DeliveryBinding::GitHubPullRequest {
            binding: Box::new(github_runtime.binding.clone()),
        };
        sources.github_runtime = Some(github_runtime);
        Ok(sources)
    }

    #[cfg(test)]
    pub(crate) fn fake(_path: &Path) -> Self {
        Self {
            parent_environment: std::collections::BTreeMap::from([(
                "PATH".into(),
                "/usr/bin:/bin".into(),
            )])
            .into(),
            profiles: None,
            architect_additional_directories: Vec::new(),
            guardian_executable: std::env::current_exe()
                .expect("test process executable must be available"),
            delivery_binding: DeliveryBinding::LocalCandidate,
            github_runtime: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_profiles_for_test(&mut self, profiles: SessionInvocationProfiles) {
        self.profiles = Some(profiles);
    }

    #[cfg(test)]
    pub(crate) fn set_guardian_executable_for_test(&mut self, executable: PathBuf) {
        self.guardian_executable = executable;
    }
}

fn validate_architect_additional_directories(
    adapter: ArchitectAdapter,
    directories: &[PathBuf],
) -> Result<()> {
    if adapter == ArchitectAdapter::Codex && !directories.is_empty() {
        bail!("Codex Architect cannot bind Claude --add-dir roots");
    }
    if directories.len() > 64 {
        bail!("Claude Architect accepts at most 64 --add-dir roots");
    }
    let mut unique = BTreeSet::new();
    for directory in directories {
        if !directory.is_absolute() || directory.as_os_str().as_encoded_bytes().len() > 4096 {
            bail!("Claude Architect --add-dir must be an existing canonical absolute directory");
        }
        let canonical = fs::canonicalize(directory).map_err(|_| {
            anyhow::anyhow!(
                "Claude Architect --add-dir must be an existing canonical absolute directory"
            )
        })?;
        let metadata = fs::symlink_metadata(directory)?;
        if canonical != *directory || metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("Claude Architect --add-dir must be an existing canonical absolute directory");
        }
        if !unique.insert(directory.clone()) {
            bail!("Claude Architect --add-dir roots must be unique");
        }
    }
    Ok(())
}

fn canonical_project_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("architect project directory must be absolute");
    }
    let canonical =
        fs::canonicalize(path).context("failed to resolve architect project directory")?;
    let metadata = fs::symlink_metadata(path)?;
    if canonical != path || metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("architect project directory must be an existing canonical directory");
    }
    Ok(canonical)
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| {
                format!("failed to create private directory {}", path.display())
            })?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Err(error) => return Err(error.into()),
    }
    let _ = canonical_private_directory(path, "private session directory")?;
    Ok(())
}

fn canonical_private_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).with_context(|| format!("failed to resolve {label}"))?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("{label} must be canonical, current-user owned, and mode 0700");
    }
    Ok(canonical)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
