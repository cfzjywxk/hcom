use crate::worker::profile::{
    ArchitectAdapter, ArchitectInvocationProfile, ClaudeInvocationProfile, CodexApprovalPolicy,
    CodexInvocationProfile, CodexSandbox, DeveloperInvocationProfile, ReviewerId,
    ReviewerInvocationProfile, SessionInvocationProfiles,
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
    pub legacy_reviewer_migrated: bool,
    /// Retained but deliberately not interpreted unless the explicit
    /// `--github-pr` delivery selector is present.
    pub github: Option<toml::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchitectToml {
    profile: Option<toml::Value>,
    developer: Option<toml::Value>,
    reviewer: Option<toml::Value>,
    reviewer1: Option<toml::Value>,
    reviewer2: Option<toml::Value>,
    github: Option<toml::Value>,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ConfiguredAdapter {
    Codex,
    Claude,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileOverride {
    adapter: Option<ConfiguredAdapter>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    effort: Option<String>,
    sandbox: Option<CodexSandbox>,
    #[serde(rename = "ask_for_approval")]
    approval_policy: Option<CodexApprovalPolicy>,
    dangerously_skip_permissions: Option<bool>,
}

fn apply_codex_override(
    profile: &mut CodexInvocationProfile,
    configured: ProfileOverride,
    label: &str,
) -> Result<()> {
    let ProfileOverride {
        adapter,
        model,
        reasoning_effort,
        effort,
        sandbox,
        approval_policy,
        dangerously_skip_permissions,
    } = configured;
    if adapter.is_some() {
        bail!("{label} cannot select its adapter in this table");
    }
    if dangerously_skip_permissions.is_some() {
        bail!("{label} contains a Claude-only field");
    }
    if reasoning_effort.is_some() && effort.is_some() {
        bail!("{label} cannot set both reasoning_effort and its effort alias");
    }
    if let Some(model) = model {
        profile.model = model;
    }
    if let Some(effort) = reasoning_effort.or(effort) {
        profile.reasoning_effort = effort;
    }
    if let Some(sandbox) = sandbox {
        profile.sandbox = sandbox;
    }
    if let Some(approval_policy) = approval_policy {
        profile.approval_policy = approval_policy;
    }
    Ok(())
}

fn apply_claude_override(
    profile: &mut ClaudeInvocationProfile,
    configured: ProfileOverride,
    label: &str,
) -> Result<()> {
    let ProfileOverride {
        adapter,
        model,
        reasoning_effort,
        effort,
        sandbox,
        approval_policy,
        dangerously_skip_permissions,
    } = configured;
    if adapter.is_some() {
        bail!("{label} cannot select its adapter in this table");
    }
    if reasoning_effort.is_some() || sandbox.is_some() || approval_policy.is_some() {
        bail!("{label} contains a Codex-only field");
    }
    if let Some(model) = model {
        profile.model = model;
    }
    if let Some(effort) = effort {
        profile.effort = effort;
    }
    if let Some(dangerously_skip_permissions) = dangerously_skip_permissions {
        profile.dangerously_skip_permissions = dangerously_skip_permissions;
    }
    Ok(())
}

fn apply_architect_override(
    profile: &mut ArchitectInvocationProfile,
    value: toml::Value,
) -> Result<()> {
    let configured: ProfileOverride = value
        .try_into()
        .context("invalid foreground Architect profile fields")?;
    match profile {
        ArchitectInvocationProfile::Codex { profile } => {
            apply_codex_override(profile, configured, "Codex [architect.profile]")
        }
        ArchitectInvocationProfile::Claude { profile } => {
            apply_claude_override(profile, configured, "Claude [architect.profile]")
        }
    }
}

fn apply_developer_override(
    profile: &mut DeveloperInvocationProfile,
    value: toml::Value,
) -> Result<()> {
    let mut configured: ProfileOverride = value
        .try_into()
        .context("invalid [architect.developer] profile fields")?;
    let adapter = configured.adapter.take().unwrap_or(match profile {
        DeveloperInvocationProfile::Codex { .. } => ConfiguredAdapter::Codex,
        DeveloperInvocationProfile::Claude { .. } => ConfiguredAdapter::Claude,
    });
    *profile = match adapter {
        ConfiguredAdapter::Codex => {
            let mut merged = match profile {
                DeveloperInvocationProfile::Codex { profile } => profile.clone(),
                DeveloperInvocationProfile::Claude { .. } => {
                    CodexInvocationProfile::developer_default()
                }
            };
            apply_codex_override(&mut merged, configured, "Codex [architect.developer]")?;
            DeveloperInvocationProfile::Codex { profile: merged }
        }
        ConfiguredAdapter::Claude => {
            let mut merged = match profile {
                DeveloperInvocationProfile::Claude { profile } => profile.clone(),
                DeveloperInvocationProfile::Codex { .. } => {
                    ClaudeInvocationProfile::developer_default()
                }
            };
            apply_claude_override(&mut merged, configured, "Claude [architect.developer]")?;
            DeveloperInvocationProfile::Claude { profile: merged }
        }
    };
    Ok(())
}

fn apply_reviewer_override(
    profile: &mut ReviewerInvocationProfile,
    value: toml::Value,
    table: &str,
) -> Result<()> {
    let mut configured: ProfileOverride = value
        .try_into()
        .with_context(|| format!("invalid {table} profile fields"))?;
    let adapter = configured.adapter.take().unwrap_or(match profile {
        ReviewerInvocationProfile::Codex { .. } => ConfiguredAdapter::Codex,
        ReviewerInvocationProfile::Claude { .. } => ConfiguredAdapter::Claude,
    });
    *profile = match adapter {
        ConfiguredAdapter::Codex => {
            let mut merged = match profile {
                ReviewerInvocationProfile::Codex { profile } => profile.clone(),
                ReviewerInvocationProfile::Claude { .. } => {
                    CodexInvocationProfile::reviewer_default()
                }
            };
            apply_codex_override(&mut merged, configured, &format!("Codex {table}"))?;
            ReviewerInvocationProfile::Codex { profile: merged }
        }
        ConfiguredAdapter::Claude => {
            let mut merged = match profile {
                ReviewerInvocationProfile::Claude { profile } => profile.clone(),
                ReviewerInvocationProfile::Codex { .. } => {
                    ClaudeInvocationProfile::reviewer_default()
                }
            };
            apply_claude_override(&mut merged, configured, &format!("Claude {table}"))?;
            ReviewerInvocationProfile::Claude { profile: merged }
        }
    };
    Ok(())
}

/// Test helper for the explicit dual provider-routed task-runtime lane.
#[cfg(test)]
pub(super) fn load_task_lane_profiles(
    path: &Path,
    architect_adapter: ArchitectAdapter,
) -> Result<LoadedInvocationProfiles> {
    load_task_lane_profiles_for_mode(path, architect_adapter, true)
}

pub(super) fn load_task_lane_profiles_for_mode(
    path: &Path,
    architect_adapter: ArchitectAdapter,
    include_reviewer2: bool,
) -> Result<LoadedInvocationProfiles> {
    load_invocation_profiles_with_defaults(path, architect_adapter, true, include_reviewer2)
}

fn load_invocation_profiles_with_defaults(
    path: &Path,
    architect_adapter: ArchitectAdapter,
    provider_routed_worker_lane: bool,
    include_reviewer2: bool,
) -> Result<LoadedInvocationProfiles> {
    let defaults = || {
        if provider_routed_worker_lane {
            if include_reviewer2 {
                SessionInvocationProfiles::for_task_lane(architect_adapter)
            } else {
                SessionInvocationProfiles::for_single_review_task_lane(architect_adapter)
            }
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
                legacy_reviewer_migrated: false,
                github: None,
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
    let mut legacy_reviewer_migrated = false;
    let mut github = None;
    if let Some(value) = document.get("architect") {
        let configured: ArchitectToml = value
            .clone()
            .try_into()
            .context("invalid [architect] profile configuration")?;
        github = configured.github;
        if let Some(value) = configured.profile {
            apply_architect_override(&mut profiles.architect, value)
                .context("invalid [architect.profile] configuration")?;
        }
        if let Some(value) = configured.developer {
            apply_developer_override(&mut profiles.developer, value)
                .context("invalid [architect.developer] configuration")?;
        }
        if configured.reviewer.is_some()
            && (configured.reviewer1.is_some() || configured.reviewer2.is_some())
        {
            if include_reviewer2 {
                bail!(
                    "legacy [architect.reviewer] cannot be combined with [architect.reviewer1] or [architect.reviewer2]; remove the legacy table and declare both canonical Reviewer lanes explicitly"
                );
            } else {
                bail!(
                    "legacy [architect.reviewer] cannot be combined with [architect.reviewer1] or [architect.reviewer2]; remove the legacy table and declare [architect.reviewer1] explicitly"
                );
            }
        }
        if !include_reviewer2 && configured.reviewer2.is_some() {
            bail!(
                "[architect.reviewer2] is not allowed in the default single-review mode; remove that table or use --double-review"
            );
        }
        if let Some(value) = configured.reviewer {
            let mut resolved = ReviewerInvocationProfile::default();
            apply_reviewer_override(&mut resolved, value, "[architect.reviewer]")
                .context("invalid [architect.reviewer] configuration")?;
            profiles.reviewers = if include_reviewer2 {
                SessionInvocationProfiles::legacy_reviewer_pair(resolved)
            } else {
                vec![crate::worker::profile::ReviewerInvocationBinding::new(
                    ReviewerId::Reviewer1,
                    resolved,
                )]
            };
            legacy_reviewer_migrated = true;
        } else {
            if let Some(value) = configured.reviewer1 {
                apply_reviewer_override(
                    profiles.reviewer_mut(ReviewerId::Reviewer1),
                    value,
                    "[architect.reviewer1]",
                )
                .context("invalid [architect.reviewer1] configuration")?;
            }
            if let Some(value) = configured.reviewer2 {
                apply_reviewer_override(
                    profiles.reviewer_mut(ReviewerId::Reviewer2),
                    value,
                    "[architect.reviewer2]",
                )
                .context("invalid [architect.reviewer2] configuration")?;
            }
        }
    }
    profiles.validate()?;
    if provider_routed_worker_lane {
        crate::worker::runtime::TaskWorkerProfiles::from_session_profiles(&profiles)
            .map_err(|error| anyhow::anyhow!(error.detail))?;
    }
    Ok(LoadedInvocationProfiles {
        profiles,
        config_path: path.to_owned(),
        loaded_from_file: true,
        legacy_reviewer_migrated,
        github,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::profile::{
        CLAUDE_DEVELOPER_ADAPTER, CLAUDE_REVIEWER_ADAPTER, CODEX_DEVELOPER_ADAPTER,
        CODEX_REVIEWER_ADAPTER,
    };
    use std::os::unix::fs::PermissionsExt;

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        fs::write(&path, contents).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        (temp, path)
    }

    #[test]
    fn missing_file_uses_explicit_dual_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let loaded =
            load_task_lane_profiles(&temp.path().join("missing.toml"), ArchitectAdapter::Codex)
                .unwrap();
        assert!(!loaded.loaded_from_file);
        assert_eq!(
            loaded.profiles.architect.codex().unwrap().sandbox,
            CodexSandbox::DangerFullAccess
        );
        for profile in [
            loaded.profiles.architect.codex().unwrap(),
            loaded.profiles.developer.codex().unwrap(),
        ] {
            assert_eq!(profile.model, "gpt-5.6-sol");
            assert_eq!(profile.reasoning_effort, "xhigh");
        }
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer1().codex().unwrap().model,
            "gpt-5.6-sol"
        );
        let reviewer2 = loaded.profiles.reviewer2().claude().unwrap();
        assert_eq!(reviewer2.model, "opus");
        assert_eq!(reviewer2.effort, "xhigh");
        assert!(reviewer2.dangerously_skip_permissions);
    }

    #[test]
    fn github_subtable_is_recognized_but_semantically_inert_for_local_mode() {
        let (_temp, path) = write_config(
            r#"
[architect.github]
stale_unknown_field = "ignored unless --github-pr is selected"

[architect.github.apps.reviewer2]
incomplete = true
"#,
        );
        let loaded =
            load_task_lane_profiles_for_mode(&path, ArchitectAdapter::Codex, false).unwrap();
        assert!(loaded.github.is_some());
        assert_eq!(loaded.profiles.reviewers.len(), 1);
    }

    #[test]
    fn claude_architect_changes_only_the_foreground_default() {
        let temp = tempfile::tempdir().unwrap();
        let loaded =
            load_task_lane_profiles(&temp.path().join("missing.toml"), ArchitectAdapter::Claude)
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
        assert_eq!(loaded.profiles.reviewer2().claude().unwrap().model, "opus");
    }

    #[test]
    fn explicit_dual_resolver_locks_the_mixed_default_when_config_is_absent() {
        let temp = tempfile::tempdir().unwrap();
        let loaded =
            load_task_lane_profiles(&temp.path().join("missing.toml"), ArchitectAdapter::Codex)
                .unwrap();
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert_eq!(
            loaded.profiles.reviewer_adapter_name(),
            CODEX_REVIEWER_ADAPTER
        );
        assert!(loaded.profiles.reviewer1().codex().is_some());
        assert_eq!(loaded.profiles.reviewer2().claude().unwrap().model, "opus");
    }

    #[test]
    fn codex_default_single_review_has_only_reviewer1_without_claude_gate() {
        let temp = tempfile::tempdir().unwrap();
        let loaded = load_task_lane_profiles_for_mode(
            &temp.path().join("missing.toml"),
            ArchitectAdapter::Codex,
            false,
        )
        .unwrap();
        assert_eq!(loaded.profiles.reviewers.len(), 1);
        assert!(loaded.profiles.reviewer1().codex().is_some());
        assert!(!loaded.profiles.uses_claude());
        assert_eq!(loaded.profiles.review_mode_name(), "single");
    }

    #[test]
    fn codex_default_single_applies_reviewer1_configuration_and_legacy_once() {
        let (_temp, canonical) = write_config(
            r#"
[architect.reviewer1]
adapter = "claude"
model = "reviewer-one"
"#,
        );
        let loaded =
            load_task_lane_profiles_for_mode(&canonical, ArchitectAdapter::Codex, false).unwrap();
        assert_eq!(loaded.profiles.reviewers.len(), 1);
        assert_eq!(
            loaded.profiles.reviewer1().claude().unwrap().model,
            "reviewer-one"
        );
        assert!(loaded.profiles.uses_claude());

        let (_temp, legacy) = write_config(
            r#"
[architect.reviewer]
adapter = "codex"
model = "legacy-one"
"#,
        );
        let loaded =
            load_task_lane_profiles_for_mode(&legacy, ArchitectAdapter::Codex, false).unwrap();
        assert!(loaded.legacy_reviewer_migrated);
        assert_eq!(loaded.profiles.reviewers.len(), 1);
        assert_eq!(
            loaded.profiles.reviewer1().codex().unwrap().model,
            "legacy-one"
        );

        let (_temp, mixed) = write_config(
            r#"
[architect.reviewer]
model = "legacy"
[architect.reviewer1]
model = "canonical"
"#,
        );
        let error = load_task_lane_profiles_for_mode(&mixed, ArchitectAdapter::Codex, false)
            .err()
            .expect("single review must reject mixed legacy and canonical Reviewer tables");
        assert!(
            format!("{error:#}").contains("declare [architect.reviewer1] explicitly"),
            "unexpected single-review legacy mix error: {error:#}"
        );
    }

    #[test]
    fn codex_default_single_rejects_explicit_reviewer2_configuration() {
        let (_temp, path) = write_config(
            r#"
[architect.reviewer2]
adapter = "codex"
"#,
        );
        let error = load_task_lane_profiles_for_mode(&path, ArchitectAdapter::Codex, false)
            .err()
            .expect("single review must reject Reviewer2 configuration");
        assert!(
            format!("{error:#}").contains("[architect.reviewer2] is not allowed"),
            "unexpected single-review Reviewer2 error: {error:#}"
        );
    }

    #[test]
    fn task_lane_resolver_accepts_explicit_claude_without_changing_other_role() {
        let (_temp, path) = write_config(
            r#"
[architect.reviewer]
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
"#,
        );
        let loaded = load_task_lane_profiles(&path, ArchitectAdapter::Codex).unwrap();
        assert!(loaded.legacy_reviewer_migrated);
        assert_eq!(
            loaded.profiles.developer_adapter_name(),
            CODEX_DEVELOPER_ADAPTER
        );
        assert!(loaded.profiles.reviewer1().claude().is_some());
        assert_eq!(
            loaded.profiles.reviewer1(),
            loaded.profiles.reviewer2(),
            "legacy Reviewer profile must be copied completely to both lanes"
        );
        let runtime =
            crate::worker::runtime::TaskWorkerProfiles::from_session_profiles(&loaded.profiles)
                .unwrap();
        assert_eq!(
            runtime.developer.provider,
            crate::worker::runtime::RuntimeProvider::CodexExec
        );
        assert_eq!(
            runtime.reviewer1().provider,
            crate::worker::runtime::RuntimeProvider::ClaudeExec
        );
    }

    #[test]
    fn task_lane_resolver_applies_each_complete_role_override_independently() {
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
        let loaded = load_task_lane_profiles(&path, ArchitectAdapter::Codex).unwrap();
        let developer = loaded.profiles.developer.codex().unwrap();
        let reviewer = loaded.profiles.reviewer1().codex().unwrap();
        assert_eq!(developer.model, "developer-override");
        assert_eq!(developer.reasoning_effort, "max");
        assert_eq!(reviewer.model, "reviewer-override");
        assert_eq!(reviewer.reasoning_effort, "high");
        assert_eq!(loaded.profiles.reviewer1(), loaded.profiles.reviewer2());
    }

    #[test]
    fn profile_sections_merge_partial_overrides_onto_role_defaults() {
        let (_temp, path) = write_config(
            r#"
[architect.profile]
model = "architect-override"

[architect.developer]
model = "developer-override"

[architect.reviewer]
effort = "medium"
"#,
        );
        let loaded = load_task_lane_profiles(&path, ArchitectAdapter::Codex).unwrap();
        let architect = loaded.profiles.architect.codex().unwrap();
        assert_eq!(architect.model, "architect-override");
        assert_eq!(architect.reasoning_effort, "xhigh");
        assert_eq!(architect.sandbox, CodexSandbox::DangerFullAccess);
        assert_eq!(
            loaded.profiles.developer.codex().unwrap().model,
            "developer-override"
        );
        assert_eq!(
            loaded.profiles.developer.codex().unwrap().reasoning_effort,
            "xhigh"
        );
        let reviewer = loaded.profiles.reviewer1().claude().unwrap();
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "medium");
        assert!(reviewer.dangerously_skip_permissions);
        assert_eq!(loaded.profiles.reviewer1(), loaded.profiles.reviewer2());
    }

    #[test]
    fn adapter_switches_merge_partial_overrides_onto_the_selected_role_defaults() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
adapter = "claude"
effort = "medium"

[architect.reviewer]
adapter = "codex"
reasoning_effort = "high"
"#,
        );
        let loaded = load_task_lane_profiles(&path, ArchitectAdapter::Codex).unwrap();
        let developer = loaded.profiles.developer.claude().unwrap();
        assert_eq!(developer.model, "opus");
        assert_eq!(developer.effort, "medium");
        assert!(developer.dangerously_skip_permissions);
        let reviewer = loaded.profiles.reviewer1().codex().unwrap();
        assert_eq!(reviewer.model, "gpt-5.6-sol");
        assert_eq!(reviewer.reasoning_effort, "high");
        assert_eq!(reviewer.sandbox, CodexSandbox::DangerFullAccess);
        assert_eq!(reviewer.approval_policy, CodexApprovalPolicy::Never);
        assert_eq!(loaded.profiles.reviewer1(), loaded.profiles.reviewer2());
    }

    #[test]
    fn all_sixteen_architect_and_worker_provider_combinations_bind_exactly() {
        use crate::worker::runtime::{RuntimeProvider, TaskWorkerProfiles};

        for architect in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            for developer in [ConfiguredAdapter::Codex, ConfiguredAdapter::Claude] {
                for reviewer1 in [ConfiguredAdapter::Codex, ConfiguredAdapter::Claude] {
                    for reviewer2 in [ConfiguredAdapter::Codex, ConfiguredAdapter::Claude] {
                        let developer_name = match developer {
                            ConfiguredAdapter::Codex => "codex",
                            ConfiguredAdapter::Claude => "claude",
                        };
                        let reviewer1_name = match reviewer1 {
                            ConfiguredAdapter::Codex => "codex",
                            ConfiguredAdapter::Claude => "claude",
                        };
                        let reviewer2_name = match reviewer2 {
                            ConfiguredAdapter::Codex => "codex",
                            ConfiguredAdapter::Claude => "claude",
                        };
                        let config = format!(
                            "[architect.developer]\nadapter = \"{developer_name}\"\n\n\
                         [architect.reviewer1]\nadapter = \"{reviewer1_name}\"\n\n\
                         [architect.reviewer2]\nadapter = \"{reviewer2_name}\"\n"
                        );
                        let (_temp, path) = write_config(&config);
                        let loaded = load_task_lane_profiles(&path, architect).unwrap();
                        assert_eq!(loaded.profiles.architect.adapter(), architect);
                        assert_eq!(
                            loaded.profiles.developer_adapter_name(),
                            match developer {
                                ConfiguredAdapter::Codex => CODEX_DEVELOPER_ADAPTER,
                                ConfiguredAdapter::Claude => CLAUDE_DEVELOPER_ADAPTER,
                            }
                        );
                        assert_eq!(
                            loaded.profiles.reviewer_adapter_name(),
                            match reviewer1 {
                                ConfiguredAdapter::Codex => CODEX_REVIEWER_ADAPTER,
                                ConfiguredAdapter::Claude => CLAUDE_REVIEWER_ADAPTER,
                            }
                        );
                        let runtime =
                            TaskWorkerProfiles::from_session_profiles(&loaded.profiles).unwrap();
                        assert_eq!(
                            runtime.developer.provider,
                            match developer {
                                ConfiguredAdapter::Codex => RuntimeProvider::CodexExec,
                                ConfiguredAdapter::Claude => RuntimeProvider::ClaudeExec,
                            }
                        );
                        assert_eq!(
                            runtime.reviewer1().provider,
                            match reviewer1 {
                                ConfiguredAdapter::Codex => RuntimeProvider::CodexExec,
                                ConfiguredAdapter::Claude => RuntimeProvider::ClaudeExec,
                            }
                        );
                        assert_eq!(
                            runtime.reviewer2().provider,
                            match reviewer2 {
                                ConfiguredAdapter::Codex => RuntimeProvider::CodexExec,
                                ConfiguredAdapter::Claude => RuntimeProvider::ClaudeExec,
                            }
                        );
                        assert_eq!(loaded.profiles.canonical_hash().len(), 64);
                        assert_eq!(runtime.canonical_hash().len(), 64);
                    }
                }
            }
        }
    }

    #[test]
    fn codex_effort_alias_is_unambiguous() {
        let (_temp, path) = write_config(
            r#"
[architect.developer]
reasoning_effort = "high"
effort = "medium"
"#,
        );
        let error = load_task_lane_profiles(&path, ArchitectAdapter::Codex)
            .err()
            .expect("duplicate effort spelling must be rejected");
        assert!(
            format!("{error:#}").contains("cannot set both reasoning_effort and its effort alias")
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
            let loaded = load_task_lane_profiles(&path, architect_adapter).unwrap();
            assert!(loaded.legacy_reviewer_migrated);
            assert_eq!(
                loaded.profiles.developer_adapter_name(),
                CODEX_DEVELOPER_ADAPTER
            );
            assert_eq!(
                loaded.profiles.reviewer_adapter_name(),
                CODEX_REVIEWER_ADAPTER
            );
            if architect_adapter == ArchitectAdapter::Codex {
                assert!(
                    !loaded.profiles.uses_claude(),
                    "pure-Codex legacy migration must not trigger the Claude gate"
                );
            }
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
            let loaded = load_task_lane_profiles(&path, architect_adapter).unwrap();
            assert_eq!(
                loaded.profiles.reviewer_adapter_name(),
                CODEX_REVIEWER_ADAPTER
            );
            let reviewer = loaded.profiles.reviewer1().codex().unwrap();
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
        assert!(load_task_lane_profiles(&path, ArchitectAdapter::Codex).is_err());

        fs::write(
            &path,
            r#"
[architect]
unknown = true
"#,
        )
        .unwrap();
        assert!(load_task_lane_profiles(&path, ArchitectAdapter::Codex).is_err());

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
        assert!(load_task_lane_profiles(&path, ArchitectAdapter::Codex).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o622)).unwrap();
        assert!(load_task_lane_profiles(&path, ArchitectAdapter::Codex).is_err());
    }

    #[test]
    fn canonical_reviewer_tables_are_independent_and_cannot_mix_with_legacy() {
        let (_temp, path) = write_config(
            r#"
[architect.reviewer1]
adapter = "claude"
model = "reviewer-one"

[architect.reviewer2]
adapter = "codex"
model = "reviewer-two"
"#,
        );
        let loaded = load_task_lane_profiles(&path, ArchitectAdapter::Codex).unwrap();
        assert!(!loaded.legacy_reviewer_migrated);
        assert_eq!(
            loaded.profiles.reviewer1().claude().unwrap().model,
            "reviewer-one"
        );
        assert_eq!(
            loaded.profiles.reviewer2().codex().unwrap().model,
            "reviewer-two"
        );

        for canonical in ["reviewer1", "reviewer2"] {
            let (_temp, path) = write_config(&format!(
                "[architect.reviewer]\nadapter = \"codex\"\n\n\
                 [architect.{canonical}]\nadapter = \"claude\"\n"
            ));
            let error = load_task_lane_profiles(&path, ArchitectAdapter::Codex)
                .err()
                .expect("legacy and canonical Reviewer tables must fail closed");
            assert!(
                format!("{error:#}").contains(
                    "legacy [architect.reviewer] cannot be combined with [architect.reviewer1] or [architect.reviewer2]"
                ),
                "unexpected mixed-table error: {error:#}"
            );
        }
    }
}
