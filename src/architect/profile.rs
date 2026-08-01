use crate::worker::profile::{
    ArchitectAdapter, ArchitectInvocationProfile, ClaudeInvocationProfile, CodexInvocationProfile,
    DeveloperInvocationProfile, ReviewerInvocationProfile, SessionInvocationProfiles,
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
    profile: Option<toml::Value>,
    developer: Option<DeveloperInvocationProfile>,
    reviewer: Option<ReviewerInvocationProfile>,
}

/// Resolver for the production Codex App Server task-runtime lane.
pub(super) fn load_codex_app_server_profiles(
    path: &Path,
    architect_adapter: ArchitectAdapter,
) -> Result<LoadedInvocationProfiles> {
    load_invocation_profiles_with_defaults(path, architect_adapter, true)
}

fn load_invocation_profiles_with_defaults(
    path: &Path,
    architect_adapter: ArchitectAdapter,
    codex_app_server_lane: bool,
) -> Result<LoadedInvocationProfiles> {
    let defaults = || {
        if codex_app_server_lane {
            SessionInvocationProfiles::for_codex_app_server_lane(architect_adapter)
        } else {
            Ok(SessionInvocationProfiles::for_architect(architect_adapter))
        }
    };
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let profiles = defaults()?;
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
    let mut profiles = defaults()?;
    if let Some(value) = document.get("architect") {
        let configured: ArchitectToml = value
            .clone()
            .try_into()
            .context("invalid [architect] profile configuration")?;
        if let Some(value) = configured.profile {
            profiles.architect = match architect_adapter {
                ArchitectAdapter::Codex => ArchitectInvocationProfile::Codex {
                    profile: value
                        .try_into::<CodexInvocationProfile>()
                        .context("invalid Codex [architect.profile] configuration")?,
                },
                ArchitectAdapter::Claude => ArchitectInvocationProfile::Claude {
                    profile: value
                        .try_into::<ClaudeInvocationProfile>()
                        .context("invalid Claude [architect.profile] configuration")?,
                },
            };
        }
        if let Some(developer) = configured.developer {
            profiles.developer = developer;
        }
        if let Some(reviewer) = configured.reviewer {
            profiles.reviewer = reviewer;
        }
    }
    profiles.validate()?;
    if codex_app_server_lane {
        crate::worker::runtime::AppServerWorkerProfiles::from_session_profiles(&profiles)
            .map_err(|error| anyhow::anyhow!(error.detail))?;
    }
    Ok(LoadedInvocationProfiles {
        profiles,
        config_path: path.to_owned(),
        loaded_from_file: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::profile::{CODEX_DEVELOPER_ADAPTER, CODEX_REVIEWER_ADAPTER, CodexSandbox};
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
        let loaded = load_codex_app_server_profiles(
            &temp.path().join("missing.toml"),
            ArchitectAdapter::Codex,
        )
        .unwrap();
        assert!(!loaded.loaded_from_file);
        assert_eq!(
            loaded.profiles.architect.codex().unwrap().sandbox,
            CodexSandbox::DangerFullAccess
        );
        // Workers are Codex-only in this lane, for both roles.
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
    }

    #[test]
    fn claude_architect_keeps_its_adapter_but_gets_codex_workers() {
        let temp = tempfile::tempdir().unwrap();
        let loaded = load_codex_app_server_profiles(
            &temp.path().join("missing.toml"),
            ArchitectAdapter::Claude,
        )
        .unwrap();
        let architect = loaded.profiles.architect.claude().unwrap();
        assert_eq!(architect.model, "opus");
        assert_eq!(architect.effort, "xhigh");
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
    }

    #[test]
    fn app_server_resolver_uses_codex_reviewer_when_config_is_absent() {
        let temp = tempfile::tempdir().unwrap();
        let loaded = load_codex_app_server_profiles(
            &temp.path().join("missing.toml"),
            ArchitectAdapter::Codex,
        )
        .unwrap();
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.developer.codex().unwrap(),
            loaded.profiles.reviewer.codex().unwrap()
        );
    }

    #[test]
    fn app_server_resolver_rejects_explicit_claude_without_fallback() {
        let (_temp, path) = write_config(
            r#"
[architect.reviewer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
"#,
        );
        let error = match load_codex_app_server_profiles(&path, ArchitectAdapter::Codex) {
            Ok(_) => panic!("explicit Claude reviewer was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "Claude reviewer is unsupported in the Codex App Server runtime lane"
        );
    }

    #[test]
    fn app_server_resolver_applies_each_complete_role_override_independently() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
adapter = "codex"
model = "developer-override"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.reviewer]
adapter = "codex"
model = "reviewer-override"
reasoning_effort = "high"
sandbox = "danger-full-access"
ask_for_approval = "never"
"#,
        );
        let loaded = load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).unwrap();
        let developer = loaded.profiles.developer.codex().unwrap();
        let reviewer = loaded.profiles.reviewer.codex().unwrap();
        assert_eq!(developer.model, "developer-override");
        assert_eq!(developer.reasoning_effort, "max");
        assert_eq!(reviewer.model, "reviewer-override");
        assert_eq!(reviewer.reasoning_effort, "high");
    }

    #[test]
    fn app_server_role_sections_default_independently_and_partial_profiles_fail_closed() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
adapter = "codex"
model = "developer-override"
reasoning_effort = "high"
sandbox = "danger-full-access"
ask_for_approval = "never"
"#,
        );
        let loaded = load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).unwrap();
        assert_eq!(
            loaded.profiles.developer.codex().unwrap().model,
            "developer-override"
        );
        assert_eq!(
            loaded.profiles.reviewer.codex().unwrap(),
            &CodexInvocationProfile::reviewer_default()
        );

        let (_temp, path) = write_config(
            r#"
[architect.reviewer]
adapter = "codex"
model = "partial"
"#,
        );
        let error = match load_codex_app_server_profiles(&path, ArchitectAdapter::Codex) {
            Ok(_) => panic!("partial role profile was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "invalid [architect] profile configuration"
        );
    }
    #[test]
    fn config_can_select_codex_for_both_roles() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"

[architect.reviewer]
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "danger-full-access"
ask_for_approval = "never"
"#,
        );
        for architect_adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            let loaded = load_codex_app_server_profiles(&path, architect_adapter).unwrap();
            assert_eq!(
                loaded.profiles.developer_adapter_name(),
                CODEX_DEVELOPER_ADAPTER
            );
            assert_eq!(
                loaded.profiles.reviewer_adapter_name(),
                CODEX_REVIEWER_ADAPTER
            );
        }
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
        for architect_adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            let loaded = load_codex_app_server_profiles(&path, architect_adapter).unwrap();
            assert_eq!(
                loaded.profiles.reviewer_adapter_name(),
                CODEX_REVIEWER_ADAPTER
            );
            let reviewer = loaded.profiles.reviewer.codex().unwrap();
            assert_eq!(reviewer.reasoning_effort, "max");
            assert_eq!(reviewer.sandbox, CodexSandbox::DangerFullAccess);
        }
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
        assert!(load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).is_err());

        fs::write(
            &path,
            r#"
[architect]
unknown = true
"#,
        )
        .unwrap();
        assert!(load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).is_err());

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
        assert!(load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o622)).unwrap();
        assert!(load_codex_app_server_profiles(&path, ArchitectAdapter::Codex).is_err());
    }
}
