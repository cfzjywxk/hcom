use crate::worker::profile::{
    CodexApprovalPolicy, CodexInvocationProfile, CodexSandbox, ReviewerInvocationProfile,
    SessionInvocationProfiles,
};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MAX_PROFILE_CONFIG_BYTES: usize = 1024 * 1024;

pub(super) struct LoadedInvocationProfiles {
    pub profiles: SessionInvocationProfiles,
    pub config_path: PathBuf,
    pub loaded_from_file: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchitectToml {
    profile: Option<CodexInvocationProfile>,
    developer: Option<CodexDeveloperToml>,
    reviewer: Option<ReviewerInvocationProfile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CodexDeveloperToml {
    adapter: String,
    model: String,
    reasoning_effort: String,
    sandbox: CodexSandbox,
    ask_for_approval: CodexApprovalPolicy,
}

impl CodexDeveloperToml {
    fn into_profile(self) -> Result<CodexInvocationProfile> {
        if self.adapter != "codex" {
            bail!("architect.developer.adapter enables only codex");
        }
        Ok(CodexInvocationProfile {
            model: self.model,
            reasoning_effort: self.reasoning_effort,
            sandbox: self.sandbox,
            approval_policy: self.ask_for_approval,
        })
    }
}

pub(super) fn load_invocation_profiles(path: &Path) -> Result<LoadedInvocationProfiles> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let profiles = SessionInvocationProfiles::default();
            profiles.validate()?;
            return Ok(LoadedInvocationProfiles {
                profiles,
                config_path: path.to_owned(),
                loaded_from_file: false,
            });
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect architect profile configuration {}",
                    path.display()
                )
            });
        }
    };
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != uid
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.len() > MAX_PROFILE_CONFIG_BYTES as u64
    {
        bail!(
            "architect profile configuration must be one current-user-owned, non-writable-by-others regular file no larger than 1 MiB"
        );
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .with_context(|| {
            format!(
                "failed to open architect profile configuration {}",
                path.display()
            )
        })?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev()
        || opened.ino() != metadata.ino()
        || !opened.is_file()
        || opened.nlink() != 1
        || opened.uid() != uid
        || opened.permissions().mode() & 0o022 != 0
        || opened.len() > MAX_PROFILE_CONFIG_BYTES as u64
    {
        bail!("architect profile configuration changed or became unsafe before it was opened");
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take((MAX_PROFILE_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_PROFILE_CONFIG_BYTES {
        bail!("architect profile configuration exceeds 1 MiB");
    }
    let text = std::str::from_utf8(&bytes)
        .context("architect profile configuration is not valid UTF-8")?;
    let document: toml::Table = text
        .parse()
        .context("architect profile configuration is malformed TOML")?;
    let mut profiles = SessionInvocationProfiles::default();
    if let Some(value) = document.get("architect") {
        let configured: ArchitectToml = value
            .clone()
            .try_into()
            .context("invalid [architect] profile configuration")?;
        if let Some(profile) = configured.profile {
            profiles.architect = profile;
        }
        if let Some(developer) = configured.developer {
            profiles.developer = developer.into_profile()?;
        }
        if let Some(reviewer) = configured.reviewer {
            profiles.reviewer = reviewer;
        }
    }
    profiles.validate()?;
    Ok(LoadedInvocationProfiles {
        profiles,
        config_path: path.to_owned(),
        loaded_from_file: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::profile::{CLAUDE_REVIEWER_ADAPTER, CODEX_REVIEWER_ADAPTER, CodexSandbox};
    use std::os::unix::fs::PermissionsExt;

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        fs::write(&path, contents).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        (temp, path)
    }

    #[test]
    fn missing_file_uses_reviewed_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let loaded = load_invocation_profiles(&temp.path().join("missing.toml")).unwrap();
        assert!(!loaded.loaded_from_file);
        assert_eq!(loaded.profiles.architect.sandbox, CodexSandbox::ReadOnly);
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CLAUDE_REVIEWER_ADAPTER
        );
    }

    #[test]
    fn existing_hcom_toml_can_select_all_three_typed_profiles() {
        let (_temp, path) = write_config(
            r#"
[terminal]
active = "default"

[architect.profile]
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.developer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.reviewer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
"#,
        );
        let loaded = load_invocation_profiles(&path).unwrap();
        assert!(loaded.loaded_from_file);
        assert_eq!(loaded.profiles.architect.reasoning_effort, "max");
        assert_eq!(loaded.profiles.developer.reasoning_effort, "max");
        let reviewer = loaded.profiles.reviewer.claude().unwrap();
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
    }

    #[test]
    fn config_can_select_the_closed_codex_reviewer_profile() {
        let (_temp, path) = write_config(
            r#"
[architect.reviewer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"
"#,
        );
        let loaded = load_invocation_profiles(&path).unwrap();
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
        let reviewer = loaded.profiles.reviewer.codex().unwrap();
        assert_eq!(reviewer.reasoning_effort, "max");
        assert_eq!(reviewer.sandbox, CodexSandbox::DangerFullAccess);
    }

    #[test]
    fn profile_tables_are_closed_and_config_file_is_not_trusted_by_path_alone() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"
args = ["--resume", "foreign-session"]
"#,
        );
        assert!(load_invocation_profiles(&path).is_err());

        fs::write(
            &path,
            r#"
[architect]
unknown = true
"#,
        )
        .unwrap();
        assert!(load_invocation_profiles(&path).is_err());

        fs::write(
            &path,
            r#"
[architect.reviewer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
args = ["--resume", "foreign-session"]
"#,
        )
        .unwrap();
        assert!(load_invocation_profiles(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o622)).unwrap();
        assert!(load_invocation_profiles(&path).is_err());
    }
}
