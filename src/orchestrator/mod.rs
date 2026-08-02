//! Foreground, in-memory task supervisor for one `hcom arch` invocation.

pub mod core;
#[cfg(target_os = "linux")]
pub(crate) mod task_lane;
#[cfg(target_os = "linux")]
pub mod workspace;

use crate::worker::environment::ParentEnvironment;
use crate::worker::profile::SessionInvocationProfiles;
use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionStartup {
    pub(crate) run_id: String,
    pub(crate) project_root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct SessionRuntimeSources {
    parent_environment: ParentEnvironment,
    codex_auth_source: Option<PathBuf>,
    profiles: Option<SessionInvocationProfiles>,
}

impl SessionRuntimeSources {
    pub(crate) fn capture(
        parent_environment: impl Into<ParentEnvironment>,
        host_runtime_dir: PathBuf,
        profiles: SessionInvocationProfiles,
    ) -> Result<Self> {
        profiles.validate()?;
        let parent_environment = parent_environment.into();
        let home = parent_environment
            .unicode("HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("session worker environment requires non-empty parent HOME"))?;
        let _host_runtime_dir =
            canonical_private_directory(&host_runtime_dir, "host XDG runtime directory")?;
        let codex_home = parent_environment
            .unicode("CODEX_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        let uses_codex =
            profiles.developer.codex().is_some() || profiles.reviewer.codex().is_some();
        let codex_auth = codex_home.join("auth.json");
        let codex_auth_source = if uses_codex && codex_auth.exists() {
            Some(canonical_private_file(&codex_auth, "Codex auth source")?)
        } else {
            None
        };
        Ok(Self {
            parent_environment,
            codex_auth_source,
            profiles: Some(profiles),
        })
    }

    #[cfg(test)]
    pub(crate) fn fake(_path: &Path) -> Self {
        Self {
            parent_environment: std::collections::BTreeMap::from([(
                "PATH".into(),
                "/usr/bin:/bin".into(),
            )])
            .into(),
            codex_auth_source: None,
            profiles: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_profiles_for_test(&mut self, profiles: SessionInvocationProfiles) {
        self.profiles = Some(profiles);
    }
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

fn path_value(label: &str, path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{label} is not valid UTF-8"))
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

fn canonical_private_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o600 != 0o600
        || metadata.nlink() != 1
    {
        bail!("{label} has an unsafe identity");
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
