//! Exact-version Codex and Claude no-TUI reviewer adapters.

use super::codex::{DISABLED_CODEX_FEATURES, observe_codex_record, parse_codex_turn};
use super::contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeOutputKind, NativeResult, OuterLaunchEnvelope, OutputDeclaration,
    ResultTransport, SchemaTransport, TurnControl, WorkerAdapter, WorkerProfile,
    validate_native_session_id,
};
use super::environment::{EnvironmentPolicy, ExactEnvironmentRequirement};
use super::result::{CheckStatus, MAX_RESULT_BYTES, ReviewerResult};
use super::sandbox::{
    EmptyRootContract, EmptyRootMounts, INSIDE_ARTIFACTS, INSIDE_CARGO_HOME, INSIDE_CLAUDE,
    INSIDE_CODEX, INSIDE_HOME, INSIDE_NATIVE_CONFIG, INSIDE_PATH, INSIDE_RUNTIME,
    INSIDE_RUSTUP_HOME, INSIDE_TEMP, INSIDE_WORKSPACE,
};
use super::validation::{MAX_PATH_BYTES, validate_git_oid, validate_text};
use crate::control_api::{CapabilitySnapshot, NativeSessionMode, WorkerRole};
use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const CODEX_REVIEWER_EXECUTABLE: &str =
    "/home/ywxk/.codex/packages/standalone/releases/0.145.0-x86_64-unknown-linux-musl/bin/codex";
pub const CODEX_REVIEWER_CLI_VERSION: &str = "codex-cli 0.145.0";
pub const CODEX_REVIEWER_MODEL: &str = "gpt-5.6-sol";
pub const CODEX_REVIEWER_REASONING: &str = "high";
pub const CLAUDE_REVIEWER_EXECUTABLE: &str = "/home/ywxk/.local/share/claude/versions/2.1.220";
pub const CLAUDE_REVIEWER_CLI_VERSION: &str = "2.1.220 (Claude Code)";
pub const CLAUDE_REVIEWER_MODEL: &str = "claude-opus-5";
pub const CLAUDE_REVIEWER_REASONING: &str = "high";

const BWRAP_EXECUTABLE: &str = "/usr/bin/bwrap";
const BWRAP_VERSION: &str = "bubblewrap 0.9.0";
const GIT_EXECUTABLE: &str = "/usr/bin/git";
const GIT_VERSION: &str = "git version 2.43.0";
const CODEX_ADAPTER_NAME: &str = "codex-reviewer-0.145.0";
const CLAUDE_ADAPTER_NAME: &str = "claude-reviewer-2.1.220";
const ADAPTER_CONTRACT_VERSION: u32 = 2;
const CODEX_EFFECTIVE_POLICY: &str =
    "native=danger-full-access;outer=bubblewrap-0.9.0-empty-root-reviewer-ro-v2;approval=never";
const CLAUDE_EFFECTIVE_POLICY: &str =
    "native=bypassPermissions;outer=bubblewrap-0.9.0-empty-root-reviewer-ro-v2";
const CODEX_RESULT_SCHEMA_FILE: &str = "codex-reviewer-result-schema.json";
const CODEX_FINAL_FILE: &str = "native-final.partial";
const JSON_SCHEMA_DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const CODEX_AUTH_FILE: &str = "auth.json";
const CLAUDE_AUTH_FILE: &str = ".credentials.json";
const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_DURATION: Duration = Duration::from_secs(30);

const CLAUDE_EXACT_ENVIRONMENT: &[(&str, &str)] = &[
    ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS", "1"),
    ("CLAUDE_CODE_DISABLE_FAST_MODE", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION", "false"),
];

#[derive(Clone, PartialEq, Eq)]
pub struct CodexReviewerConfig {
    pub run_id: String,
    pub workspace_cwd: PathBuf,
    pub artifact_root: PathBuf,
    pub isolated_home: PathBuf,
    pub codex_home: PathBuf,
    pub temp_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub host_runtime_dir: PathBuf,
    pub auth_source: PathBuf,
    pub cargo_bin_source: PathBuf,
    pub rustup_home_source: PathBuf,
}

pub struct CodexReviewerAdapter {
    descriptor: AdapterDescriptor,
    executable: ExecutableIdentity,
    outer_executable: ExecutableIdentity,
    git_executable: ExecutableIdentity,
    sandbox: ReviewerSandbox,
}

impl CodexReviewerAdapter {
    pub fn discover(config: CodexReviewerConfig) -> Result<Self> {
        validate_production_runtime_contract(&config.host_runtime_dir, &config.run_id)?;
        Self::discover_with_paths(
            config,
            Path::new(CODEX_REVIEWER_EXECUTABLE),
            Path::new(BWRAP_EXECUTABLE),
            Path::new(GIT_EXECUTABLE),
        )
    }

    pub fn environment_policy() -> Result<EnvironmentPolicy> {
        policy_with_required(&[
            "CARGO_HOME",
            "CODEX_HOME",
            "HOME",
            "PATH",
            "RUSTUP_HOME",
            "TMPDIR",
            "XDG_RUNTIME_DIR",
        ])
    }

    pub fn profile(&self) -> WorkerProfile {
        profile(&self.descriptor, &self.executable)
    }

    fn discover_with_paths(
        config: CodexReviewerConfig,
        codex_path: &Path,
        bwrap_path: &Path,
        git_path: &Path,
    ) -> Result<Self> {
        let executable = capture_exact_tool(codex_path, CODEX_REVIEWER_CLI_VERSION)?;
        let outer_executable = capture_exact_tool(bwrap_path, BWRAP_VERSION)?;
        let git_executable = capture_exact_tool(git_path, GIT_VERSION)?;
        let sandbox = ReviewerSandbox::capture(
            ReviewerSandboxConfig {
                run_id: config.run_id,
                workspace_cwd: config.workspace_cwd,
                artifact_root: config.artifact_root,
                isolated_home: config.isolated_home,
                native_config_dir: config.codex_home,
                temp_dir: config.temp_dir,
                runtime_dir: config.runtime_dir,
                host_runtime_dir: config.host_runtime_dir,
                auth_source: config.auth_source,
                auth_target_name: CODEX_AUTH_FILE.into(),
                cargo_bin_source: config.cargo_bin_source,
                rustup_home_source: config.rustup_home_source,
                extra_private_dirs: vec![],
            },
            &executable,
            &outer_executable,
            &git_executable,
        )?;
        let descriptor = codex_reviewer_descriptor()?;
        Ok(Self {
            descriptor,
            executable,
            outer_executable,
            git_executable,
            sandbox,
        })
    }

    fn command(
        &self,
        control: &TurnControl,
        resume_session_id: Option<&str>,
    ) -> Result<CommandSpec> {
        validate_reviewer_control(control)?;
        revalidate_exact_tool(&self.executable, CODEX_REVIEWER_CLI_VERSION)?;
        revalidate_exact_tool(&self.outer_executable, BWRAP_VERSION)?;
        revalidate_exact_tool(&self.git_executable, GIT_VERSION)?;
        self.sandbox.revalidate(
            &self.executable,
            &self.outer_executable,
            &self.git_executable,
        )?;
        self.sandbox
            .validate_revision(control, &self.git_executable)?;

        let mut fixed_argv = vec!["exec".into()];
        if let Some(session_id) = resume_session_id {
            validate_native_session_id(session_id)?;
            fixed_argv.extend(["resume".into(), session_id.into()]);
        }
        fixed_argv.extend([
            "--json".into(),
            "--model".into(),
            CODEX_REVIEWER_MODEL.into(),
            "--config".into(),
            "model_reasoning_effort=\"high\"".into(),
            "--config".into(),
            "approval_policy=\"never\"".into(),
            "--config".into(),
            "mcp_servers={}".into(),
            "--dangerously-bypass-approvals-and-sandbox".into(),
            "--ignore-user-config".into(),
            "--ignore-rules".into(),
        ]);
        for feature in DISABLED_CODEX_FEATURES {
            fixed_argv.extend(["--disable".into(), (*feature).into()]);
        }
        if resume_session_id.is_none() {
            fixed_argv.extend(["--cd".into(), INSIDE_WORKSPACE.into()]);
        }
        let expected_artifact_dir = self
            .sandbox
            .artifact_root
            .path()
            .join(&control.artifact_dir);
        let production_outer = self.outer_executable.canonical_path == Path::new(BWRAP_EXECUTABLE);
        Ok(CommandSpec {
            executable: self.executable.clone(),
            fixed_argv,
            schema_transport: SchemaTransport::File {
                argument: "--output-schema".into(),
                relative_path: CODEX_RESULT_SCHEMA_FILE.into(),
                contents: codex_reviewer_result_schema(),
            },
            expected_outputs: vec![
                OutputDeclaration {
                    kind: NativeOutputKind::StdoutEnvelope,
                    relative_path: "native.stdout.partial".into(),
                    max_bytes: 1024 * 1024,
                    output_argument: None,
                },
                OutputDeclaration {
                    kind: NativeOutputKind::FinalFile,
                    relative_path: CODEX_FINAL_FILE.into(),
                    max_bytes: MAX_RESULT_BYTES,
                    output_argument: Some("--output-last-message".into()),
                },
            ],
            stdin_prompt_argument: Some("-".into()),
            workspace_cwd: self.sandbox.workspace.path().to_owned(),
            outer_launch: Some(OuterLaunchEnvelope {
                executable: self.outer_executable.clone(),
                fixed_argv: self.sandbox.outer_argv(
                    &expected_artifact_dir,
                    &self.executable,
                    INSIDE_CODEX,
                    "/hcom/native/auth.json",
                )?,
                expected_artifact_dir,
                inside_executable: if production_outer {
                    INSIDE_CODEX.into()
                } else {
                    self.executable.canonical_path.clone()
                },
                inside_artifact_dir: if production_outer {
                    INSIDE_ARTIFACTS.into()
                } else {
                    self.sandbox
                        .artifact_root
                        .path()
                        .join(&control.artifact_dir)
                },
            }),
            exact_environment: vec![
                ExactEnvironmentRequirement::new("CARGO_HOME", INSIDE_CARGO_HOME)?,
                ExactEnvironmentRequirement::new("CODEX_HOME", INSIDE_NATIVE_CONFIG)?,
                ExactEnvironmentRequirement::new("HOME", INSIDE_HOME)?,
                ExactEnvironmentRequirement::new("PATH", INSIDE_PATH)?,
                ExactEnvironmentRequirement::new("RUSTUP_HOME", INSIDE_RUSTUP_HOME)?,
                ExactEnvironmentRequirement::new("TMPDIR", INSIDE_TEMP)?,
                ExactEnvironmentRequirement::new("XDG_RUNTIME_DIR", INSIDE_RUNTIME)?,
            ],
        })
    }
}

impl WorkerAdapter for CodexReviewerAdapter {
    fn descriptor(&self) -> &AdapterDescriptor {
        &self.descriptor
    }

    fn executable_contract(&self) -> &ExecutableIdentity {
        &self.executable
    }

    fn build_create(&self, control: &TurnControl) -> Result<CommandSpec> {
        if control.native_session_id.is_some() {
            bail!("Codex reviewer create cannot pre-bind a discovered native session");
        }
        self.command(control, None)
    }

    fn build_resume(&self, native_session_id: &str, control: &TurnControl) -> Result<CommandSpec> {
        if control.native_session_id.as_deref() != Some(native_session_id) {
            bail!("Codex reviewer resume must use the exact session-bound native session");
        }
        self.command(control, Some(native_session_id))
    }

    fn observe_native_record(&self, record: &[u8]) -> Result<Vec<NativeObservation>> {
        observe_codex_record(record)
    }

    fn extract_result(
        &self,
        control: &TurnControl,
        artifacts: &NativeArtifacts,
    ) -> Result<NativeResult> {
        validate_reviewer_artifacts(control, artifacts)?;
        if !artifacts.stderr().iter().all(u8::is_ascii_whitespace) {
            bail!("Codex reviewer emitted unexpected stderr");
        }
        let evidence = parse_codex_turn(control, artifacts.stdout())?;
        let output = artifacts
            .final_output()
            .ok_or_else(|| anyhow!("Codex reviewer final result is missing"))?;
        let result = ReviewerResult::parse(output)
            .context("Codex reviewer final result is not strict ReviewerResult JSON")?;
        validate_reported_checks(
            &result,
            &evidence.completed_commands,
            &evidence.failed_commands,
        )?;
        self.sandbox
            .validate_revision(control, &self.git_executable)?;
        Ok(NativeResult::Reviewer {
            native_session_id: evidence.native_session_id,
            result,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ClaudeReviewerConfig {
    pub run_id: String,
    pub workspace_cwd: PathBuf,
    pub artifact_root: PathBuf,
    pub isolated_home: PathBuf,
    pub claude_config_dir: PathBuf,
    pub xdg_config_home: PathBuf,
    pub xdg_state_home: PathBuf,
    pub xdg_cache_home: PathBuf,
    pub xdg_data_home: PathBuf,
    pub temp_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub host_runtime_dir: PathBuf,
    pub auth_source: PathBuf,
    pub cargo_bin_source: PathBuf,
    pub rustup_home_source: PathBuf,
}

pub struct ClaudeReviewerAdapter {
    descriptor: AdapterDescriptor,
    executable: ExecutableIdentity,
    outer_executable: ExecutableIdentity,
    git_executable: ExecutableIdentity,
    sandbox: ReviewerSandbox,
    xdg_config_home: DirectoryIdentity,
    xdg_state_home: DirectoryIdentity,
    xdg_cache_home: DirectoryIdentity,
    xdg_data_home: DirectoryIdentity,
}

impl ClaudeReviewerAdapter {
    pub fn discover(config: ClaudeReviewerConfig) -> Result<Self> {
        validate_production_runtime_contract(&config.host_runtime_dir, &config.run_id)?;
        Self::discover_with_paths(
            config,
            Path::new(CLAUDE_REVIEWER_EXECUTABLE),
            Path::new(BWRAP_EXECUTABLE),
            Path::new(GIT_EXECUTABLE),
        )
    }

    pub fn environment_policy() -> Result<EnvironmentPolicy> {
        policy_with_required(&[
            "CARGO_HOME",
            "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS",
            "CLAUDE_CODE_DISABLE_FAST_MODE",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
            "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION",
            "CLAUDE_CONFIG_DIR",
            "HOME",
            "PATH",
            "RUSTUP_HOME",
            "TMPDIR",
            "XDG_CACHE_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_RUNTIME_DIR",
            "XDG_STATE_HOME",
        ])
    }

    pub fn profile(&self) -> WorkerProfile {
        profile(&self.descriptor, &self.executable)
    }

    fn discover_with_paths(
        config: ClaudeReviewerConfig,
        claude_path: &Path,
        bwrap_path: &Path,
        git_path: &Path,
    ) -> Result<Self> {
        let executable = capture_exact_tool(claude_path, CLAUDE_REVIEWER_CLI_VERSION)?;
        let outer_executable = capture_exact_tool(bwrap_path, BWRAP_VERSION)?;
        let git_executable = capture_exact_tool(git_path, GIT_VERSION)?;
        let xdg_config_home = DirectoryIdentity::capture(&config.xdg_config_home, true)?;
        let xdg_state_home = DirectoryIdentity::capture(&config.xdg_state_home, true)?;
        let xdg_cache_home = DirectoryIdentity::capture(&config.xdg_cache_home, true)?;
        let xdg_data_home = DirectoryIdentity::capture(&config.xdg_data_home, true)?;
        let sandbox = ReviewerSandbox::capture(
            ReviewerSandboxConfig {
                run_id: config.run_id,
                workspace_cwd: config.workspace_cwd,
                artifact_root: config.artifact_root,
                isolated_home: config.isolated_home,
                native_config_dir: config.claude_config_dir,
                temp_dir: config.temp_dir,
                runtime_dir: config.runtime_dir,
                host_runtime_dir: config.host_runtime_dir,
                auth_source: config.auth_source,
                auth_target_name: CLAUDE_AUTH_FILE.into(),
                cargo_bin_source: config.cargo_bin_source,
                rustup_home_source: config.rustup_home_source,
                extra_private_dirs: vec![
                    xdg_config_home.clone(),
                    xdg_state_home.clone(),
                    xdg_cache_home.clone(),
                    xdg_data_home.clone(),
                ],
            },
            &executable,
            &outer_executable,
            &git_executable,
        )?;
        let descriptor = claude_reviewer_descriptor()?;
        Ok(Self {
            descriptor,
            executable,
            outer_executable,
            git_executable,
            sandbox,
            xdg_config_home,
            xdg_state_home,
            xdg_cache_home,
            xdg_data_home,
        })
    }

    fn command(
        &self,
        control: &TurnControl,
        resume_session_id: Option<&str>,
    ) -> Result<CommandSpec> {
        validate_reviewer_control(control)?;
        let exact_session = control
            .native_session_id
            .as_deref()
            .ok_or_else(|| anyhow!("Claude reviewer requires a preassigned native session"))?;
        validate_claude_session_id(exact_session)?;
        if resume_session_id.is_some_and(|session| session != exact_session) {
            bail!("Claude reviewer resume must use the exact session-bound native session");
        }
        revalidate_exact_tool(&self.executable, CLAUDE_REVIEWER_CLI_VERSION)?;
        revalidate_exact_tool(&self.outer_executable, BWRAP_VERSION)?;
        revalidate_exact_tool(&self.git_executable, GIT_VERSION)?;
        self.sandbox.revalidate(
            &self.executable,
            &self.outer_executable,
            &self.git_executable,
        )?;
        for directory in [
            &self.xdg_config_home,
            &self.xdg_state_home,
            &self.xdg_cache_home,
            &self.xdg_data_home,
        ] {
            directory.revalidate(true)?;
        }
        self.sandbox
            .validate_revision(control, &self.git_executable)?;

        let mut fixed_argv = vec!["-p".into(), "--output-format".into(), "json".into()];
        if resume_session_id.is_some() {
            fixed_argv.extend(["--resume".into(), exact_session.into()]);
        } else {
            fixed_argv.extend(["--session-id".into(), exact_session.into()]);
        }
        fixed_argv.extend([
            "--name".into(),
            "hcom-session-reviewer".into(),
            "--model".into(),
            CLAUDE_REVIEWER_MODEL.into(),
            "--effort".into(),
            CLAUDE_REVIEWER_REASONING.into(),
            "--permission-mode".into(),
            "bypassPermissions".into(),
            "--tools".into(),
            "Bash,Read".into(),
            "--setting-sources".into(),
            "project".into(),
            "--strict-mcp-config".into(),
            "--mcp-config".into(),
            r#"{"mcpServers":{}}"#.into(),
            "--disable-slash-commands".into(),
            "--prompt-suggestions".into(),
            "false".into(),
            "--no-chrome".into(),
        ]);
        let expected_artifact_dir = self
            .sandbox
            .artifact_root
            .path()
            .join(&control.artifact_dir);
        let production_outer = self.outer_executable.canonical_path == Path::new(BWRAP_EXECUTABLE);
        Ok(CommandSpec {
            executable: self.executable.clone(),
            fixed_argv,
            schema_transport: SchemaTransport::InlineArgument {
                flag: "--json-schema".into(),
                json: String::from_utf8(claude_reviewer_result_schema())
                    .expect("static reviewer schema is UTF-8"),
            },
            expected_outputs: vec![OutputDeclaration {
                kind: NativeOutputKind::StdoutEnvelope,
                relative_path: "native.stdout.partial".into(),
                max_bytes: 1024 * 1024,
                output_argument: None,
            }],
            stdin_prompt_argument: None,
            workspace_cwd: self.sandbox.workspace.path().to_owned(),
            outer_launch: Some(OuterLaunchEnvelope {
                executable: self.outer_executable.clone(),
                fixed_argv: self.sandbox.outer_argv(
                    &expected_artifact_dir,
                    &self.executable,
                    INSIDE_CLAUDE,
                    "/hcom/native/.credentials.json",
                )?,
                expected_artifact_dir,
                inside_executable: if production_outer {
                    INSIDE_CLAUDE.into()
                } else {
                    self.executable.canonical_path.clone()
                },
                inside_artifact_dir: if production_outer {
                    INSIDE_ARTIFACTS.into()
                } else {
                    self.sandbox
                        .artifact_root
                        .path()
                        .join(&control.artifact_dir)
                },
            }),
            exact_environment: self.exact_environment()?,
        })
    }

    fn exact_environment(&self) -> Result<Vec<ExactEnvironmentRequirement>> {
        let mut exact = CLAUDE_EXACT_ENVIRONMENT
            .iter()
            .map(|(name, value)| ExactEnvironmentRequirement::new(*name, *value))
            .collect::<Result<Vec<_>>>()?;
        exact.extend([
            ExactEnvironmentRequirement::new("CARGO_HOME", INSIDE_CARGO_HOME)?,
            ExactEnvironmentRequirement::new("CLAUDE_CONFIG_DIR", INSIDE_NATIVE_CONFIG)?,
            ExactEnvironmentRequirement::new("HOME", INSIDE_HOME)?,
            ExactEnvironmentRequirement::new("PATH", INSIDE_PATH)?,
            ExactEnvironmentRequirement::new("RUSTUP_HOME", INSIDE_RUSTUP_HOME)?,
            ExactEnvironmentRequirement::new("TMPDIR", INSIDE_TEMP)?,
            ExactEnvironmentRequirement::new("XDG_CACHE_HOME", "/hcom/home/.cache")?,
            ExactEnvironmentRequirement::new("XDG_CONFIG_HOME", "/hcom/home/.config")?,
            ExactEnvironmentRequirement::new("XDG_DATA_HOME", "/hcom/home/.data")?,
            ExactEnvironmentRequirement::new("XDG_RUNTIME_DIR", INSIDE_RUNTIME)?,
            ExactEnvironmentRequirement::new("XDG_STATE_HOME", "/hcom/home/.state")?,
        ]);
        exact.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(exact)
    }
}

impl WorkerAdapter for ClaudeReviewerAdapter {
    fn descriptor(&self) -> &AdapterDescriptor {
        &self.descriptor
    }

    fn executable_contract(&self) -> &ExecutableIdentity {
        &self.executable
    }

    fn build_create(&self, control: &TurnControl) -> Result<CommandSpec> {
        self.command(control, None)
    }

    fn build_resume(&self, native_session_id: &str, control: &TurnControl) -> Result<CommandSpec> {
        validate_claude_session_id(native_session_id)?;
        self.command(control, Some(native_session_id))
    }

    fn observe_native_record(&self, record: &[u8]) -> Result<Vec<NativeObservation>> {
        let envelope = parse_claude_envelope(record, None)?;
        Ok(vec![
            NativeObservation::SessionStarted {
                native_session_id: envelope.session_id,
            },
            NativeObservation::Activity {
                kind: "turn".into(),
                message: "completed".into(),
            },
        ])
    }

    fn extract_result(
        &self,
        control: &TurnControl,
        artifacts: &NativeArtifacts,
    ) -> Result<NativeResult> {
        validate_reviewer_artifacts(control, artifacts)?;
        if artifacts.final_output().is_some() {
            bail!("Claude reviewer unexpectedly emitted a final-file result");
        }
        if !artifacts.stderr().iter().all(u8::is_ascii_whitespace) {
            bail!("Claude reviewer emitted unexpected stderr");
        }
        let envelope =
            parse_claude_envelope(artifacts.stdout(), control.native_session_id.as_deref())?;
        let encoded = serde_json::to_vec(&envelope.structured_output)?;
        let result = ReviewerResult::parse(&encoded)
            .context("Claude structured output is not strict ReviewerResult JSON")?;
        self.sandbox
            .validate_revision(control, &self.git_executable)?;
        Ok(NativeResult::Reviewer {
            native_session_id: envelope.session_id,
            result,
        })
    }
}

fn profile(descriptor: &AdapterDescriptor, executable: &ExecutableIdentity) -> WorkerProfile {
    WorkerProfile {
        role: WorkerRole::Reviewer,
        adapter: descriptor.name.clone(),
        model: descriptor.model.clone(),
        reasoning: descriptor.reasoning.clone(),
        policy: descriptor.policy.clone(),
        executable: executable.clone(),
        cli_version: descriptor.cli_version.clone(),
        adapter_contract_version: descriptor.contract_version,
        native_session_mode: descriptor.capabilities.native_session_mode,
        capability: CapabilitySnapshot {
            contract_hash: descriptor.capability_contract_hash.clone(),
            features: descriptor.capabilities.features.clone(),
        },
    }
}

fn reviewer_capabilities(
    native_session_mode: NativeSessionMode,
    result_transport: ResultTransport,
) -> AdapterCapabilities {
    AdapterCapabilities {
        roles: vec![WorkerRole::Reviewer],
        native_session_mode,
        result_transport,
        features: vec![
            "exact-resume".into(),
            "outer-bwrap-empty-root-reviewer-ro-v2".into(),
            "workspace-attestation".into(),
            "structured-major-minor".into(),
        ],
    }
}

fn codex_reviewer_descriptor() -> Result<AdapterDescriptor> {
    AdapterDescriptor::new(
        CODEX_ADAPTER_NAME,
        ADAPTER_CONTRACT_VERSION,
        CODEX_REVIEWER_CLI_VERSION,
        CODEX_REVIEWER_MODEL,
        CODEX_REVIEWER_REASONING,
        CODEX_EFFECTIVE_POLICY,
        reviewer_capabilities(NativeSessionMode::Discovered, ResultTransport::FinalFile),
    )
}

fn claude_reviewer_descriptor() -> Result<AdapterDescriptor> {
    AdapterDescriptor::new(
        CLAUDE_ADAPTER_NAME,
        ADAPTER_CONTRACT_VERSION,
        CLAUDE_REVIEWER_CLI_VERSION,
        CLAUDE_REVIEWER_MODEL,
        CLAUDE_REVIEWER_REASONING,
        CLAUDE_EFFECTIVE_POLICY,
        reviewer_capabilities(NativeSessionMode::Preassigned, ResultTransport::Envelope),
    )
}

fn policy_with_required(extra: &[&str]) -> Result<EnvironmentPolicy> {
    let mut inherited = EnvironmentPolicy::baseline().inherited_names;
    inherited.extend(extra.iter().map(|name| (*name).to_owned()));
    inherited.sort();
    inherited.dedup();
    let mut required = vec!["PATH".into()];
    required.extend(extra.iter().map(|name| (*name).to_owned()));
    required.sort();
    required.dedup();
    EnvironmentPolicy::new(inherited, required)
}

fn validate_reviewer_control(control: &TurnControl) -> Result<()> {
    control.validate()?;
    if control.role != WorkerRole::Reviewer {
        bail!("reviewer adapter cannot build a developer turn");
    }
    if control.head_revision.is_none() {
        bail!("reviewer turn lost its exact head revision");
    }
    Ok(())
}

fn validate_reviewer_artifacts(control: &TurnControl, artifacts: &NativeArtifacts) -> Result<()> {
    validate_reviewer_control(control)?;
    if artifacts.role() != WorkerRole::Reviewer {
        bail!("reviewer native artifacts do not match their exact turn");
    }
    Ok(())
}

fn validate_reported_checks(
    result: &ReviewerResult,
    completed: &std::collections::BTreeSet<String>,
    failed: &std::collections::BTreeSet<String>,
) -> Result<()> {
    for check in &result.checks {
        if check.status == CheckStatus::Passed && !completed.contains(&check.command) {
            bail!("reviewer reported a passed check without current-turn evidence");
        }
        if check.status == CheckStatus::Passed && failed.contains(&check.command) {
            bail!("reviewer reported a passed check with conflicting turn evidence");
        }
    }
    Ok(())
}

fn reviewer_result_schema_shape() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "summary", "findings", "checks"],
        "properties": {
            "decision": {"enum": ["lgtm", "request_changes"]},
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["severity", "title", "body", "file", "line"],
                    "properties": {
                        "severity": {"enum": ["major", "minor"]},
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                        "file": {"type": ["string", "null"]},
                        "line": {"type": ["integer", "null"], "minimum": 1}
                    }
                }
            },
            "checks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["command", "status", "summary"],
                    "properties": {
                        "command": {"type": "string"},
                        "status": {"enum": ["passed", "failed", "not_run"]},
                        "summary": {"type": "string"}
                    }
                }
            }
        }
    })
}

fn codex_reviewer_result_schema() -> Vec<u8> {
    let mut schema = reviewer_result_schema_shape();
    schema
        .as_object_mut()
        .expect("static reviewer schema is an object")
        .insert(
            "$schema".into(),
            serde_json::Value::String(JSON_SCHEMA_DRAFT_2020_12.into()),
        );
    serialize_reviewer_result_schema(&schema)
}

fn claude_reviewer_result_schema() -> Vec<u8> {
    serialize_reviewer_result_schema(&reviewer_result_schema_shape())
}

fn serialize_reviewer_result_schema(schema: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(schema).expect("static reviewer result schema is valid JSON")
}

#[derive(Deserialize)]
struct ClaudeResultEnvelope {
    #[serde(rename = "type")]
    kind: String,
    subtype: String,
    is_error: bool,
    session_id: String,
    structured_output: serde_json::Value,
    #[serde(rename = "modelUsage")]
    model_usage: BTreeMap<String, serde_json::Value>,
}

fn parse_claude_envelope(
    bytes: &[u8],
    expected_session_id: Option<&str>,
) -> Result<ClaudeResultEnvelope> {
    if bytes.is_empty() || bytes.len() > 1024 * 1024 {
        bail!("Claude result envelope exceeds its bound");
    }
    let text = std::str::from_utf8(bytes).context("Claude result envelope is not UTF-8")?;
    validate_text("Claude result envelope", text.trim(), 1024 * 1024, true)?;
    let envelope: ClaudeResultEnvelope =
        serde_json::from_slice(bytes).context("Claude result envelope is malformed")?;
    if envelope.kind != "result" || envelope.subtype != "success" || envelope.is_error {
        bail!("Claude reviewer did not report a successful terminal result");
    }
    validate_claude_session_id(&envelope.session_id)?;
    if expected_session_id.is_some_and(|expected| expected != envelope.session_id) {
        bail!("Claude reviewer returned a different native session");
    }
    if envelope.model_usage.len() != 1 || !envelope.model_usage.contains_key(CLAUDE_REVIEWER_MODEL)
    {
        bail!("Claude reviewer model usage drifted from its exact pinned model");
    }
    if !envelope.structured_output.is_object() {
        bail!("Claude reviewer omitted its structured result object");
    }
    Ok(envelope)
}

fn validate_claude_session_id(value: &str) -> Result<()> {
    validate_native_session_id(value)?;
    let parsed = Uuid::parse_str(value).context("Claude native session is not a UUID")?;
    if parsed.hyphenated().to_string() != value {
        bail!("Claude native session must use canonical lowercase UUID form");
    }
    Ok(())
}

struct ReviewerSandboxConfig {
    run_id: String,
    workspace_cwd: PathBuf,
    artifact_root: PathBuf,
    isolated_home: PathBuf,
    native_config_dir: PathBuf,
    temp_dir: PathBuf,
    runtime_dir: PathBuf,
    host_runtime_dir: PathBuf,
    auth_source: PathBuf,
    auth_target_name: String,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
    extra_private_dirs: Vec<DirectoryIdentity>,
}

struct ReviewerSandbox {
    run_id: String,
    workspace: DirectoryIdentity,
    artifact_root: DirectoryIdentity,
    isolated_home: DirectoryIdentity,
    native_config: DirectoryIdentity,
    temp_dir: DirectoryIdentity,
    runtime_dir: DirectoryIdentity,
    host_runtime_dir: DirectoryIdentity,
    auth_source: FileIdentity,
    auth_target: FileIdentity,
    extra_private_dirs: Vec<DirectoryIdentity>,
    git_workspace: GitWorkspaceIdentity,
    empty_root: EmptyRootContract,
}

impl ReviewerSandbox {
    fn capture(
        config: ReviewerSandboxConfig,
        native: &ExecutableIdentity,
        bwrap: &ExecutableIdentity,
        git: &ExecutableIdentity,
    ) -> Result<Self> {
        validate_text(
            "reviewer auth target filename",
            &config.auth_target_name,
            128,
            false,
        )?;
        if Path::new(&config.auth_target_name).components().count() != 1 {
            bail!("reviewer auth target filename must be one normal component");
        }
        let workspace = DirectoryIdentity::capture(&config.workspace_cwd, false)
            .context("invalid reviewer checkout root")?;
        let artifact_root = DirectoryIdentity::capture(&config.artifact_root, true)
            .context("invalid reviewer artifact root")?;
        let isolated_home = DirectoryIdentity::capture(&config.isolated_home, true)
            .context("invalid reviewer isolated HOME")?;
        let native_config = DirectoryIdentity::capture(&config.native_config_dir, true)
            .context("invalid reviewer native config root")?;
        let temp_dir = DirectoryIdentity::capture(&config.temp_dir, true)
            .context("invalid reviewer temporary root")?;
        let runtime_dir = DirectoryIdentity::capture(&config.runtime_dir, true)
            .context("invalid reviewer private runtime root")?;
        let host_runtime_dir = DirectoryIdentity::capture(&config.host_runtime_dir, true)
            .context("invalid reviewer host runtime mask")?;
        if !native_config.path().starts_with(isolated_home.path())
            || native_config.path() == isolated_home.path()
        {
            bail!("reviewer native config root must be a strict child of isolated HOME");
        }
        for extra in &config.extra_private_dirs {
            extra.revalidate(true)?;
            if !extra.path().starts_with(isolated_home.path())
                || extra.path() == isolated_home.path()
                || extra.path() == native_config.path()
            {
                bail!("reviewer extra state roots must be distinct children of isolated HOME");
            }
        }
        let mut disjoint = vec![
            ("workspace", workspace.path()),
            ("artifact root", artifact_root.path()),
            ("isolated HOME", isolated_home.path()),
            ("isolated temp", temp_dir.path()),
            ("private runtime", runtime_dir.path()),
            ("host runtime mask", host_runtime_dir.path()),
        ];
        for left in 0..disjoint.len() {
            for right in left + 1..disjoint.len() {
                if paths_overlap(disjoint[left].1, disjoint[right].1) {
                    bail!(
                        "reviewer {} and {} must not overlap",
                        disjoint[left].0,
                        disjoint[right].0
                    );
                }
            }
        }
        disjoint.clear();

        let auth_source = FileIdentity::capture(&config.auth_source)?;
        let auth_target =
            FileIdentity::capture(&native_config.path().join(&config.auth_target_name))?;
        if auth_source.path() == auth_target.path() {
            bail!("reviewer auth source must be distinct from its isolated target");
        }
        let writable_roots = [artifact_root.path(), isolated_home.path(), temp_dir.path()];
        for protected in [
            native.canonical_path.as_path(),
            bwrap.canonical_path.as_path(),
            git.canonical_path.as_path(),
            auth_source.path(),
            workspace.path(),
        ] {
            if protected.starts_with(host_runtime_dir.path()) {
                bail!("host runtime mask hides a required reviewer sandbox path");
            }
            if writable_roots
                .iter()
                .any(|root| protected.starts_with(root))
            {
                bail!("reviewer writable roots contain a protected host path");
            }
        }
        let git_workspace = GitWorkspaceIdentity::capture(workspace.path(), git)?;
        let empty_root =
            EmptyRootContract::capture(&config.cargo_bin_source, &config.rustup_home_source)?;
        Ok(Self {
            run_id: config.run_id,
            workspace,
            artifact_root,
            isolated_home,
            native_config,
            temp_dir,
            runtime_dir,
            host_runtime_dir,
            auth_source,
            auth_target,
            extra_private_dirs: config.extra_private_dirs,
            git_workspace,
            empty_root,
        })
    }

    fn revalidate(
        &self,
        native: &ExecutableIdentity,
        bwrap: &ExecutableIdentity,
        git: &ExecutableIdentity,
    ) -> Result<()> {
        native.revalidate()?;
        bwrap.revalidate()?;
        git.revalidate()?;
        self.workspace.revalidate(false)?;
        self.artifact_root.revalidate(true)?;
        self.isolated_home.revalidate(true)?;
        self.native_config.revalidate(true)?;
        self.temp_dir.revalidate(true)?;
        self.runtime_dir.revalidate(true)?;
        self.host_runtime_dir.revalidate(true)?;
        for directory in &self.extra_private_dirs {
            directory.revalidate(true)?;
        }
        super::validation::validate_opaque_id("reviewer run id", &self.run_id)?;
        self.auth_source.revalidate()?;
        self.auth_target.revalidate()?;
        self.empty_root.revalidate()?;
        self.git_workspace.revalidate(self.workspace.path(), git)
    }

    fn outer_argv(
        &self,
        artifact_dir: &Path,
        native: &ExecutableIdentity,
        inside_native: &'static str,
        auth_target: &'static str,
    ) -> Result<Vec<String>> {
        if !artifact_dir.starts_with(self.artifact_root.path()) {
            bail!("reviewer artifact attempt escaped its pinned artifact root");
        }
        let argv = self.empty_root.outer_argv(EmptyRootMounts {
            native,
            inside_native,
            isolated_home: self.isolated_home.path(),
            native_config: self.native_config.path(),
            workspace: self.workspace.path(),
            workspace_writable: false,
            artifact_dir,
            auth_source: self.auth_source.path(),
            auth_target,
        })?;
        if argv.iter().any(|argument| {
            argument == "--"
                || argument.contains("control.sock")
                || argument == "--new-session"
                || argument == "/"
        }) {
            bail!("reviewer outer sandbox manifest contains forbidden launch authority");
        }
        Ok(argv)
    }

    fn validate_revision(&self, control: &TurnControl, git: &ExecutableIdentity) -> Result<()> {
        validate_reviewer_control(control)?;
        revalidate_exact_tool(git, GIT_VERSION)?;
        self.git_workspace.revalidate(self.workspace.path(), git)?;
        let runner = GitRunner {
            executable: git,
            workspace: self.workspace.path(),
        };
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
        {
            bail!("review checkout is not clean");
        }
        if !runner
            .success(&["for-each-ref", "--format=%(refname)", "refs/replace/"])?
            .is_empty()
        {
            bail!("review checkout contains replacement refs");
        }
        let head = runner.one_line(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        validate_git_oid("actual review checkout HEAD", &head)?;
        if control.head_revision.as_deref() != Some(head.as_str()) {
            bail!("review checkout HEAD differs from the exact reviewer turn");
        }
        let ancestor =
            runner.run(&["merge-base", "--is-ancestor", &control.base_revision, &head])?;
        if ancestor.status.code() != Some(0) || !ancestor.stderr.is_empty() {
            bail!("review base revision is not an ancestor of the exact workspace HEAD");
        }
        revalidate_exact_tool(git, GIT_VERSION)?;
        self.git_workspace.revalidate(self.workspace.path(), git)?;
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
            || runner.one_line(&["rev-parse", "--verify", "HEAD^{commit}"])? != head
        {
            bail!("review checkout drifted during its revision gate");
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DirectoryIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
}

impl DirectoryIdentity {
    fn capture(path: &Path, private: bool) -> Result<Self> {
        let link = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect reviewer directory {}", path.display()))?;
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!("reviewer sandbox directory must be a real directory");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("reviewer sandbox directory must already be canonical");
        }
        let metadata = fs::metadata(path)?;
        let identity = Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
        };
        identity.validate_metadata(private)?;
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self, private: bool) -> Result<()> {
        if Self::capture(&self.path, private)? != *self {
            bail!("reviewer sandbox directory identity drifted");
        }
        Ok(())
    }

    fn validate_metadata(&self, private: bool) -> Result<()> {
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if self.uid != uid {
            bail!("reviewer sandbox directory is not owned by the current user");
        }
        if private && (self.mode & 0o077 != 0 || self.mode & 0o700 != 0o700) {
            bail!("reviewer private directory must be mode 0700");
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct FileIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    links: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanos: i64,
    changed_seconds: i64,
    changed_nanos: i64,
}

impl FileIdentity {
    fn capture(path: &Path) -> Result<Self> {
        let link = fs::symlink_metadata(path).with_context(|| {
            format!("failed to inspect reviewer private file {}", path.display())
        })?;
        if link.file_type().is_symlink() || !link.is_file() {
            bail!("reviewer private file must be a regular non-symlink file");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("reviewer private file must already be canonical");
        }
        let metadata = fs::metadata(path)?;
        let identity = Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
            links: metadata.nlink(),
            size: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanos: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanos: metadata.ctime_nsec(),
        };
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if identity.uid != uid
            || identity.links != 1
            || identity.mode & 0o077 != 0
            || identity.mode & 0o600 != 0o600
        {
            bail!("reviewer private file has unsafe ownership, links, or permissions");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self) -> Result<()> {
        if Self::capture(&self.path)? != *self {
            bail!("reviewer private file identity drifted");
        }
        Ok(())
    }
}

struct GitWorkspaceIdentity {
    top_level: PathBuf,
    git_dir: DirectoryIdentity,
    common_dir: DirectoryIdentity,
    object_dir: DirectoryIdentity,
}

impl GitWorkspaceIdentity {
    fn capture(workspace: &Path, git: &ExecutableIdentity) -> Result<Self> {
        let runner = GitRunner {
            executable: git,
            workspace,
        };
        let top_level = canonical_git_path(&runner.one_line(&["rev-parse", "--show-toplevel"])?)?;
        if top_level != workspace {
            bail!("review checkout is not its exact Git top level");
        }
        let git_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
        ])?)?;
        let common_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        ])?)?;
        let object_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "objects",
        ])?)?;
        if !git_dir.starts_with(workspace)
            || !common_dir.starts_with(workspace)
            || !object_dir.starts_with(workspace)
        {
            bail!("review checkout Git administration must stay inside the read-only workspace");
        }
        let identity = Self {
            top_level,
            git_dir: DirectoryIdentity::capture(&git_dir, false)
                .context("invalid review checkout Git directory")?,
            common_dir: DirectoryIdentity::capture(&common_dir, false)
                .context("invalid review checkout common Git directory")?,
            object_dir: DirectoryIdentity::capture(&object_dir, false)
                .context("invalid review checkout object directory")?,
        };
        identity.reject_admin_indirections()?;
        Ok(identity)
    }

    fn revalidate(&self, workspace: &Path, git: &ExecutableIdentity) -> Result<()> {
        let current = Self::capture(workspace, git)?;
        if current.top_level != self.top_level
            || current.git_dir != self.git_dir
            || current.common_dir != self.common_dir
            || current.object_dir != self.object_dir
        {
            bail!("review checkout Git identity drifted");
        }
        Ok(())
    }

    fn reject_admin_indirections(&self) -> Result<()> {
        for path in [
            self.common_dir.path().join("info/grafts"),
            self.object_dir.path().join("info/alternates"),
            self.object_dir.path().join("info/http-alternates"),
        ] {
            match fs::symlink_metadata(&path) {
                Ok(_) => bail!("review checkout uses forbidden Git object indirection"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).context("failed to inspect review checkout indirection");
                }
            }
        }
        Ok(())
    }
}

struct GitRunner<'a> {
    executable: &'a ExecutableIdentity,
    workspace: &'a Path,
}

impl GitRunner<'_> {
    fn run(&self, args: &[&str]) -> Result<BoundedCommandOutput> {
        let mut command = Command::new(&self.executable.canonical_path);
        command
            .arg("--no-replace-objects")
            .args(["-c", "core.fsmonitor=false"])
            .args(["-c", "core.untrackedCache=false"])
            .args(args)
            .current_dir(self.workspace)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_PAGER", "/bin/cat")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C");
        run_bounded_command(command, MAX_TOOL_OUTPUT_BYTES)
    }

    fn success(&self, args: &[&str]) -> Result<Vec<u8>> {
        let output = self.run(args)?;
        if !output.status.success() || !output.stderr.is_empty() {
            bail!("bounded review Git evidence command failed");
        }
        Ok(output.stdout)
    }

    fn one_line(&self, args: &[&str]) -> Result<String> {
        let output = self.success(args)?;
        let text = std::str::from_utf8(&output).context("review Git evidence is not UTF-8")?;
        let text = text.strip_suffix('\n').unwrap_or(text);
        if text.is_empty() || text.contains('\n') || text.contains('\r') {
            bail!("review Git evidence did not contain exactly one bounded line");
        }
        Ok(text.to_owned())
    }
}

struct BoundedCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn capture_exact_tool(path: &Path, expected: &str) -> Result<ExecutableIdentity> {
    let before = ExecutableIdentity::capture(path)?;
    let mut command = Command::new(path);
    command.arg("--version").env_clear();
    let output = run_bounded_command(command, 4096)?;
    let mut expected_output = expected.as_bytes().to_vec();
    expected_output.push(b'\n');
    if !output.status.success() || !output.stderr.is_empty() || output.stdout != expected_output {
        bail!("reviewer tool version does not match its exact enabled contract");
    }
    let after = ExecutableIdentity::capture(path)?;
    if before != after {
        bail!("reviewer tool identity changed during version validation");
    }
    Ok(after)
}

fn revalidate_exact_tool(identity: &ExecutableIdentity, expected: &str) -> Result<()> {
    let current = capture_exact_tool(&identity.canonical_path, expected)?;
    if current != *identity {
        bail!("reviewer tool identity or version drifted");
    }
    Ok(())
}

fn run_bounded_command(mut command: Command, cap: usize) -> Result<BoundedCommandOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .context("failed to spawn bounded reviewer helper")?;
    let Some(stdout) = child.stdout.take() else {
        terminate_and_reap(&mut child);
        bail!("bounded reviewer helper stdout pipe is unavailable");
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_and_reap(&mut child);
        bail!("bounded reviewer helper stderr pipe is unavailable");
    };
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout_overflow = overflow.clone();
    let stderr_overflow = overflow.clone();
    let stdout_thread = thread::spawn(move || read_bounded_pipe(stdout, cap, stdout_overflow));
    let stderr_thread = thread::spawn(move || read_bounded_pipe(stderr, cap, stderr_overflow));
    let started = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if overflow.load(Ordering::Acquire) {
            let _ = child.kill();
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                terminate_and_reap(&mut child);
                return Err(error).context("failed to inspect bounded reviewer helper");
            }
        }
        if started.elapsed() >= MAX_TOOL_DURATION {
            timed_out = true;
            let _ = child.kill();
            break child
                .wait()
                .context("failed to reap timed-out reviewer helper")?;
        }
        thread::sleep(Duration::from_millis(2));
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("bounded reviewer stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("bounded reviewer stderr reader panicked"))??;
    if overflow.load(Ordering::Acquire) {
        bail!("bounded reviewer helper output exceeded its hard cap");
    }
    if timed_out {
        bail!("bounded reviewer helper exceeded its hard deadline");
    }
    Ok(BoundedCommandOutput {
        status,
        stdout,
        stderr,
    })
}

fn terminate_and_reap(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn read_bounded_pipe<R: Read>(
    mut pipe: R,
    cap: usize,
    overflow: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(cap.min(32 * 1024));
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let count = pipe.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = cap.saturating_sub(output.len());
        let accepted = remaining.min(count);
        output.extend_from_slice(&buffer[..accepted]);
        if accepted != count {
            overflow.store(true, Ordering::Release);
        }
    }
    Ok(output)
}

fn canonical_git_path(value: &str) -> Result<PathBuf> {
    if value.len() > MAX_PATH_BYTES {
        bail!("review Git path exceeds its bound");
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("review Git path must be absolute");
    }
    let canonical = fs::canonicalize(&path)?;
    if canonical != path {
        bail!("review Git path must already be canonical");
    }
    Ok(canonical)
}

fn validate_production_runtime_contract(host_runtime_dir: &Path, run_id: &str) -> Result<()> {
    super::validation::validate_opaque_id("reviewer run id", run_id)?;
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR is required for the reviewer sandbox"))?;
    let canonical = fs::canonicalize(&runtime).context("failed to resolve host XDG_RUNTIME_DIR")?;
    if runtime != canonical || host_runtime_dir != canonical {
        bail!("reviewer host runtime mask does not match canonical XDG_RUNTIME_DIR");
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{ArtifactAttempt, ArtifactRoot, ArtifactScope};
    use crate::worker::environment::{ExecutionEnvironmentLease, WorkerEnvironmentIdentity};
    use crate::worker::{
        HeartbeatControl, NativeSessionBinding, ProcessRunner, WorkerAdapter, prepare_create_turn,
        prepare_resume_turn,
    };
    use std::os::unix::net::{UnixListener, UnixStream};

    const CLAUDE_SESSION: &str = "b174295a-e7a8-4bb6-ac78-a96d34b2ab21";
    const OTHER_CLAUDE_SESSION: &str = "7b8a787a-6303-4eb6-b03f-6c3e22042c8b";

    struct Fixture {
        _temp: tempfile::TempDir,
        workspace: PathBuf,
        artifact_root: PathBuf,
        isolated_home: PathBuf,
        codex_home: PathBuf,
        claude_config_dir: PathBuf,
        xdg_config_home: PathBuf,
        xdg_state_home: PathBuf,
        xdg_cache_home: PathBuf,
        xdg_data_home: PathBuf,
        temp_dir: PathBuf,
        runtime_dir: PathBuf,
        host_runtime_dir: PathBuf,
        codex_auth_source: PathBuf,
        claude_auth_source: PathBuf,
        cargo_bin_source: PathBuf,
        rustup_home_source: PathBuf,
        codex: PathBuf,
        claude: PathBuf,
        bwrap: PathBuf,
        git: PathBuf,
        base_revision: String,
        first_head: String,
        second_head: String,
        developer_worktree: PathBuf,
        global_sentinel: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let workspace = temp.path().join("review-workspace");
            let artifact_root = temp.path().join("artifacts");
            let isolated_home = temp.path().join("isolated-home");
            let codex_home = isolated_home.join("codex");
            let claude_config_dir = isolated_home.join("claude");
            let xdg_config_home = isolated_home.join("xdg-config");
            let xdg_state_home = isolated_home.join("xdg-state");
            let xdg_cache_home = isolated_home.join("xdg-cache");
            let xdg_data_home = isolated_home.join("xdg-data");
            let temp_dir = temp.path().join("isolated-tmp");
            let runtime_dir = temp.path().join("isolated-runtime");
            let host_runtime_dir = temp.path().join("host-runtime");
            let developer_worktree = temp.path().join("developer-worktree");
            let cargo_bin_source = temp.path().join("cargo-bin");
            let rustup_home_source = temp.path().join("rustup-home");
            let tools = temp.path().join("tools");
            for directory in [
                &workspace,
                &artifact_root,
                &isolated_home,
                &codex_home,
                &claude_config_dir,
                &xdg_config_home,
                &xdg_state_home,
                &xdg_cache_home,
                &xdg_data_home,
                &temp_dir,
                &runtime_dir,
                &host_runtime_dir,
                &developer_worktree,
                &cargo_bin_source,
                &rustup_home_source,
                &tools,
            ] {
                fs::create_dir(directory).unwrap();
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
            }

            let codex_auth_source = temp.path().join("codex-native-auth.json");
            write_private_file(&codex_auth_source, b"codex-private-auth-sentinel");
            write_private_file(&codex_home.join(CODEX_AUTH_FILE), b"isolated-placeholder");
            let claude_auth_source = temp.path().join("claude-native-auth.json");
            write_private_file(&claude_auth_source, b"claude-private-auth-sentinel");
            write_private_file(
                &claude_config_dir.join(CLAUDE_AUTH_FILE),
                b"isolated-placeholder",
            );
            let global_sentinel = temp.path().join("global-config-sentinel");
            write_private_file(&global_sentinel, b"global-config-must-not-change");

            let codex = tools.join("codex");
            write_executable(&codex, fake_codex_script());
            let claude = tools.join("claude");
            write_executable(&claude, fake_claude_script());
            let bwrap = tools.join("bwrap");
            write_executable(&bwrap, fake_bwrap_script());
            let git = fs::canonicalize(GIT_EXECUTABLE).unwrap();

            git_ok(
                &git,
                &workspace,
                &["init", "--quiet", "--initial-branch=master"],
            );
            git_ok(&git, &workspace, &["config", "user.name", "Phase Six"]);
            git_ok(
                &git,
                &workspace,
                &["config", "user.email", "phase6@example.invalid"],
            );
            fs::write(workspace.join("base.txt"), b"base\n").unwrap();
            git_ok(&git, &workspace, &["add", "base.txt"]);
            git_ok(
                &git,
                &workspace,
                &["commit", "--quiet", "-m", "Base commit"],
            );
            let base_revision = git_line(&git, &workspace, &["rev-parse", "HEAD"]);

            fs::write(workspace.join("tracked.txt"), b"revision one\n").unwrap();
            git_ok(&git, &workspace, &["add", "tracked.txt"]);
            git_ok(
                &git,
                &workspace,
                &["commit", "--quiet", "-m", "First review head"],
            );
            let first_head = git_line(&git, &workspace, &["rev-parse", "HEAD"]);

            fs::write(workspace.join("tracked.txt"), b"revision two\n").unwrap();
            git_ok(&git, &workspace, &["add", "tracked.txt"]);
            git_ok(
                &git,
                &workspace,
                &["commit", "--quiet", "-m", "Second review head"],
            );
            let second_head = git_line(&git, &workspace, &["rev-parse", "HEAD"]);
            git_ok(
                &git,
                &workspace,
                &["checkout", "--quiet", "--detach", &first_head],
            );
            fs::set_permissions(workspace.join(".git"), fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(
                workspace.join(".git/objects"),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();

            Self {
                _temp: temp,
                workspace: fs::canonicalize(workspace).unwrap(),
                artifact_root: fs::canonicalize(artifact_root).unwrap(),
                isolated_home: fs::canonicalize(isolated_home).unwrap(),
                codex_home: fs::canonicalize(codex_home).unwrap(),
                claude_config_dir: fs::canonicalize(claude_config_dir).unwrap(),
                xdg_config_home: fs::canonicalize(xdg_config_home).unwrap(),
                xdg_state_home: fs::canonicalize(xdg_state_home).unwrap(),
                xdg_cache_home: fs::canonicalize(xdg_cache_home).unwrap(),
                xdg_data_home: fs::canonicalize(xdg_data_home).unwrap(),
                temp_dir: fs::canonicalize(temp_dir).unwrap(),
                runtime_dir: fs::canonicalize(runtime_dir).unwrap(),
                host_runtime_dir: fs::canonicalize(host_runtime_dir).unwrap(),
                codex_auth_source: fs::canonicalize(codex_auth_source).unwrap(),
                claude_auth_source: fs::canonicalize(claude_auth_source).unwrap(),
                cargo_bin_source: fs::canonicalize(cargo_bin_source).unwrap(),
                rustup_home_source: fs::canonicalize(rustup_home_source).unwrap(),
                codex: fs::canonicalize(codex).unwrap(),
                claude: fs::canonicalize(claude).unwrap(),
                bwrap: fs::canonicalize(bwrap).unwrap(),
                git,
                base_revision,
                first_head,
                second_head,
                developer_worktree: fs::canonicalize(developer_worktree).unwrap(),
                global_sentinel,
            }
        }

        fn codex_config(&self) -> CodexReviewerConfig {
            CodexReviewerConfig {
                run_id: "run-reviewer-fixture".into(),
                workspace_cwd: self.workspace.clone(),
                artifact_root: self.artifact_root.clone(),
                isolated_home: self.isolated_home.clone(),
                codex_home: self.codex_home.clone(),
                temp_dir: self.temp_dir.clone(),
                runtime_dir: self.runtime_dir.clone(),
                host_runtime_dir: self.host_runtime_dir.clone(),
                auth_source: self.codex_auth_source.clone(),
                cargo_bin_source: self.cargo_bin_source.clone(),
                rustup_home_source: self.rustup_home_source.clone(),
            }
        }

        fn claude_config(&self) -> ClaudeReviewerConfig {
            ClaudeReviewerConfig {
                run_id: "run-reviewer-fixture".into(),
                workspace_cwd: self.workspace.clone(),
                artifact_root: self.artifact_root.clone(),
                isolated_home: self.isolated_home.clone(),
                claude_config_dir: self.claude_config_dir.clone(),
                xdg_config_home: self.xdg_config_home.clone(),
                xdg_state_home: self.xdg_state_home.clone(),
                xdg_cache_home: self.xdg_cache_home.clone(),
                xdg_data_home: self.xdg_data_home.clone(),
                temp_dir: self.temp_dir.clone(),
                runtime_dir: self.runtime_dir.clone(),
                host_runtime_dir: self.host_runtime_dir.clone(),
                auth_source: self.claude_auth_source.clone(),
                cargo_bin_source: self.cargo_bin_source.clone(),
                rustup_home_source: self.rustup_home_source.clone(),
            }
        }

        fn codex_adapter(&self) -> CodexReviewerAdapter {
            CodexReviewerAdapter::discover_with_paths(
                self.codex_config(),
                &self.codex,
                &self.bwrap,
                &self.git,
            )
            .unwrap()
        }

        fn claude_adapter(&self) -> ClaudeReviewerAdapter {
            ClaudeReviewerAdapter::discover_with_paths(
                self.claude_config(),
                &self.claude,
                &self.bwrap,
                &self.git,
            )
            .unwrap()
        }

        fn refresh(&self, head: &str) {
            git_ok(
                &self.git,
                &self.workspace,
                &["checkout", "--quiet", "--force", "--detach", head],
            );
        }

        fn control(
            &self,
            task_id: &str,
            logical_session_id: &str,
            native_session_id: Option<&str>,
            sequence: u32,
            round: u32,
            head: &str,
        ) -> TurnControl {
            TurnControl {
                run_id: "run-reviewers".into(),
                task_id: task_id.into(),
                role: WorkerRole::Reviewer,
                logical_session_id: logical_session_id.into(),
                native_session_id: native_session_id.map(str::to_owned),
                turn_sequence: sequence,
                attempt: 1,
                task_version: u64::from(sequence) + 10,
                review_round: round,
                base_revision: self.base_revision.clone(),
                head_revision: Some(head.into()),
                artifact_dir: format!(
                    "run-reviewers/{task_id}/reviewer/{logical_session_id}/turn-{sequence}/attempt-1"
                ),
            }
        }

        fn codex_lease(&self, id: &str) -> ExecutionEnvironmentLease {
            ExecutionEnvironmentLease::capture(
                id,
                "epoch-reviewers",
                &CodexReviewerAdapter::environment_policy().unwrap(),
                vec![
                    ("CARGO_HOME".into(), INSIDE_CARGO_HOME.into()),
                    ("CODEX_HOME".into(), INSIDE_NATIVE_CONFIG.into()),
                    ("HOME".into(), INSIDE_HOME.into()),
                    ("PATH".into(), INSIDE_PATH.into()),
                    ("RUSTUP_HOME".into(), INSIDE_RUSTUP_HOME.into()),
                    ("TMPDIR".into(), INSIDE_TEMP.into()),
                    ("XDG_RUNTIME_DIR".into(), INSIDE_RUNTIME.into()),
                ],
            )
            .unwrap()
        }

        fn claude_lease(&self, id: &str) -> ExecutionEnvironmentLease {
            let mut values = vec![
                ("CARGO_HOME".into(), INSIDE_CARGO_HOME.into()),
                ("CLAUDE_CONFIG_DIR".into(), INSIDE_NATIVE_CONFIG.into()),
                ("HOME".into(), INSIDE_HOME.into()),
                ("PATH".into(), INSIDE_PATH.into()),
                ("RUSTUP_HOME".into(), INSIDE_RUSTUP_HOME.into()),
                ("TMPDIR".into(), INSIDE_TEMP.into()),
                ("XDG_CACHE_HOME".into(), "/hcom/home/.cache".into()),
                ("XDG_CONFIG_HOME".into(), "/hcom/home/.config".into()),
                ("XDG_DATA_HOME".into(), "/hcom/home/.data".into()),
                ("XDG_RUNTIME_DIR".into(), INSIDE_RUNTIME.into()),
                ("XDG_STATE_HOME".into(), "/hcom/home/.state".into()),
            ];
            values.extend(
                CLAUDE_EXACT_ENVIRONMENT
                    .iter()
                    .map(|(name, value)| ((*name).into(), (*value).into())),
            );
            ExecutionEnvironmentLease::capture(
                id,
                "epoch-reviewers",
                &ClaudeReviewerAdapter::environment_policy().unwrap(),
                values,
            )
            .unwrap()
        }

        fn run(
            &self,
            adapter: &dyn WorkerAdapter,
            profile: &WorkerProfile,
            control: &TurnControl,
            lease: ExecutionEnvironmentLease,
            prompt: &[u8],
        ) -> crate::worker::ProcessCompletion {
            let prepared = match &control.native_session_id {
                Some(session) if control.turn_sequence > 1 => {
                    prepare_resume_turn(adapter, profile, control, session, prompt.to_vec())
                        .unwrap()
                }
                _ => prepare_create_turn(adapter, profile, control, prompt.to_vec()).unwrap(),
            };
            let root = ArtifactRoot::open(&self.artifact_root).unwrap();
            let attempt = ArtifactAttempt::create(
                &root,
                ArtifactScope {
                    run_id: control.run_id.clone(),
                    task_id: control.task_id.clone(),
                    role: WorkerRole::Reviewer,
                    logical_session_id: control.logical_session_id.clone(),
                    turn_sequence: control.turn_sequence,
                    attempt: control.attempt,
                },
                &lease,
                prompt,
            )
            .unwrap();
            let environment = lease
                .materialize(
                    "epoch-reviewers",
                    &WorkerEnvironmentIdentity {
                        role: WorkerRole::Reviewer,
                        run_id: control.run_id.clone(),
                        task_id: control.task_id.clone(),
                    },
                )
                .unwrap();
            ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(50))
                .unwrap()
                .spawn(WorkerRole::Reviewer, prepared, &environment, attempt)
                .unwrap()
                .wait(|_| Ok(HeartbeatControl::Continue))
                .unwrap()
        }
    }

    #[test]
    fn adapter_schema_declarations_share_one_strict_result_shape() {
        let mut codex: serde_json::Value =
            serde_json::from_slice(&codex_reviewer_result_schema()).unwrap();
        let claude: serde_json::Value =
            serde_json::from_slice(&claude_reviewer_result_schema()).unwrap();

        assert_eq!(
            codex.get("$schema"),
            Some(&serde_json::Value::String(JSON_SCHEMA_DRAFT_2020_12.into()))
        );
        assert!(claude.get("$schema").is_none());
        codex.as_object_mut().unwrap().remove("$schema");
        assert_eq!(codex, claude);

        assert_eq!(
            claude.get("required"),
            Some(&serde_json::json!([
                "decision", "summary", "findings", "checks"
            ]))
        );
        assert_eq!(
            claude.get("additionalProperties"),
            Some(&serde_json::Value::Bool(false))
        );
        assert_eq!(
            claude.pointer("/properties/findings/items/additionalProperties"),
            Some(&serde_json::Value::Bool(false))
        );
        assert_eq!(
            claude.pointer("/properties/checks/items/additionalProperties"),
            Some(&serde_json::Value::Bool(false))
        );

        let valid = serde_json::json!({
            "decision": "lgtm",
            "summary": "no blocking issue",
            "findings": [],
            "checks": []
        });
        assert!(ReviewerResult::parse(&serde_json::to_vec(&valid).unwrap()).is_ok());

        let mut unknown = valid.clone();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), true.into());
        assert!(ReviewerResult::parse(&serde_json::to_vec(&unknown).unwrap()).is_err());

        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove("checks");
        assert!(ReviewerResult::parse(&serde_json::to_vec(&missing).unwrap()).is_err());

        let nested_unknown = serde_json::json!({
            "decision": "lgtm",
            "summary": "no blocking issue",
            "findings": [],
            "checks": [{
                "command": "git diff --check",
                "status": "passed",
                "summary": "clean",
                "unexpected": true
            }]
        });
        assert!(ReviewerResult::parse(&serde_json::to_vec(&nested_unknown).unwrap()).is_err());
    }

    #[test]
    fn exact_profiles_fake_create_and_same_task_workspace_refresh_resume_are_closed() {
        let fixture = Fixture::new();
        let prompt = b"review only the exact approved base and head";

        let codex = fixture.codex_adapter();
        let codex_profile = codex.profile();
        codex_profile.validate_for(&codex).unwrap();
        assert_eq!(codex_profile.role, WorkerRole::Reviewer);
        assert_eq!(codex_profile.model, CODEX_REVIEWER_MODEL);
        assert_eq!(codex_profile.reasoning, CODEX_REVIEWER_REASONING);
        assert_eq!(codex_profile.policy, CODEX_EFFECTIVE_POLICY);
        assert_eq!(
            codex_profile.native_session_mode,
            NativeSessionMode::Discovered
        );
        let codex_create = fixture.control(
            "task-codex",
            "logical-codex",
            None,
            1,
            1,
            &fixture.first_head,
        );
        let prepared =
            prepare_create_turn(&codex, &codex_profile, &codex_create, prompt.to_vec()).unwrap();
        assert_closed_codex_argv(
            &prepared.command().materialized_control_argv(),
            &fixture,
            prompt,
        );
        let completion = fixture.run(
            &codex,
            &codex_profile,
            &codex_create,
            fixture.codex_lease("lease-codex-create"),
            prompt,
        );
        assert_eq!(completion.exit.code, Some(0));
        let result = codex
            .extract_result(&codex_create, &completion.artifacts)
            .unwrap();
        assert_eq!(result.native_session_id(), "native-codex-reviewer-1");
        let mut codex_binding =
            NativeSessionBinding::new(WorkerRole::Reviewer, NativeSessionMode::Discovered, None)
                .unwrap();
        codex_binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: result.native_session_id().into(),
            })
            .unwrap();
        codex_binding.seal_result(&result).unwrap();

        fixture.refresh(&fixture.second_head);
        let codex_resume = fixture.control(
            "task-codex",
            "logical-codex",
            Some("native-codex-reviewer-1"),
            2,
            2,
            &fixture.second_head,
        );
        codex_binding
            .begin_resume("native-codex-reviewer-1")
            .unwrap();
        let prepared = prepare_resume_turn(
            &codex,
            &codex_profile,
            &codex_resume,
            "native-codex-reviewer-1",
            prompt.to_vec(),
        )
        .unwrap();
        let argv = prepared.command().materialized_control_argv();
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["resume", "native-codex-reviewer-1"])
        );
        assert!(!argv.iter().any(|argument| argument == "--cd"));
        assert!(
            prepare_resume_turn(
                &codex,
                &codex_profile,
                &codex_resume,
                "native-other",
                prompt.to_vec(),
            )
            .is_err()
        );
        let completion = fixture.run(
            &codex,
            &codex_profile,
            &codex_resume,
            fixture.codex_lease("lease-codex-resume"),
            prompt,
        );
        let result = codex
            .extract_result(&codex_resume, &completion.artifacts)
            .unwrap();
        assert_eq!(result.native_session_id(), "native-codex-reviewer-1");
        codex_binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: result.native_session_id().into(),
            })
            .unwrap();
        codex_binding.seal_result(&result).unwrap();

        fixture.refresh(&fixture.first_head);
        let claude = fixture.claude_adapter();
        let claude_profile = claude.profile();
        claude_profile.validate_for(&claude).unwrap();
        assert_eq!(claude_profile.role, WorkerRole::Reviewer);
        assert_eq!(claude_profile.model, CLAUDE_REVIEWER_MODEL);
        assert_eq!(claude_profile.reasoning, CLAUDE_REVIEWER_REASONING);
        assert_eq!(claude_profile.policy, CLAUDE_EFFECTIVE_POLICY);
        assert_eq!(
            claude_profile.native_session_mode,
            NativeSessionMode::Preassigned
        );
        let claude_create = fixture.control(
            "task-claude",
            "logical-claude",
            Some(CLAUDE_SESSION),
            1,
            1,
            &fixture.first_head,
        );
        let prepared =
            prepare_create_turn(&claude, &claude_profile, &claude_create, prompt.to_vec()).unwrap();
        assert_closed_claude_argv(
            &prepared.command().materialized_control_argv(),
            &fixture,
            prompt,
            "--session-id",
        );
        let completion = fixture.run(
            &claude,
            &claude_profile,
            &claude_create,
            fixture.claude_lease("lease-claude-create"),
            prompt,
        );
        assert_eq!(completion.exit.code, Some(0));
        let result = claude
            .extract_result(&claude_create, &completion.artifacts)
            .unwrap();
        assert_eq!(result.native_session_id(), CLAUDE_SESSION);
        let mut claude_binding = NativeSessionBinding::new(
            WorkerRole::Reviewer,
            NativeSessionMode::Preassigned,
            Some(CLAUDE_SESSION.into()),
        )
        .unwrap();
        claude_binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: result.native_session_id().into(),
            })
            .unwrap();
        claude_binding.seal_result(&result).unwrap();

        fixture.refresh(&fixture.second_head);
        let claude_resume = fixture.control(
            "task-claude",
            "logical-claude",
            Some(CLAUDE_SESSION),
            2,
            2,
            &fixture.second_head,
        );
        claude_binding.begin_resume(CLAUDE_SESSION).unwrap();
        let prepared = prepare_resume_turn(
            &claude,
            &claude_profile,
            &claude_resume,
            CLAUDE_SESSION,
            prompt.to_vec(),
        )
        .unwrap();
        assert_closed_claude_argv(
            &prepared.command().materialized_control_argv(),
            &fixture,
            prompt,
            "--resume",
        );
        let completion = fixture.run(
            &claude,
            &claude_profile,
            &claude_resume,
            fixture.claude_lease("lease-claude-resume"),
            prompt,
        );
        let result = claude
            .extract_result(&claude_resume, &completion.artifacts)
            .unwrap();
        assert_eq!(result.native_session_id(), CLAUDE_SESSION);
        claude_binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: result.native_session_id().into(),
            })
            .unwrap();
        claude_binding.seal_result(&result).unwrap();

        let next_task = fixture.control(
            "task-claude-next",
            "logical-claude-next",
            Some(OTHER_CLAUDE_SESSION),
            1,
            1,
            &fixture.second_head,
        );
        let next =
            prepare_create_turn(&claude, &claude_profile, &next_task, prompt.to_vec()).unwrap();
        assert!(
            next.command()
                .materialized_control_argv()
                .windows(2)
                .any(|pair| pair == ["--session-id", OTHER_CLAUDE_SESSION])
        );
        assert_ne!(CLAUDE_SESSION, OTHER_CLAUDE_SESSION);
        assert_eq!(
            fs::read(&fixture.global_sentinel).unwrap(),
            b"global-config-must-not-change"
        );
    }

    #[test]
    fn revision_git_identity_tool_auth_and_environment_drift_fail_closed() {
        let fixture = Fixture::new();
        let adapter = fixture.codex_adapter();
        let profile = adapter.profile();
        let prompt = b"review exact revision";

        let wrong_head = fixture.control(
            "task-wrong-head",
            "logical-wrong-head",
            None,
            1,
            1,
            &fixture.second_head,
        );
        assert!(prepare_create_turn(&adapter, &profile, &wrong_head, prompt.to_vec()).is_err());

        let mut wrong_base = fixture.control(
            "task-wrong-base",
            "logical-wrong-base",
            None,
            1,
            1,
            &fixture.first_head,
        );
        wrong_base.base_revision = "f".repeat(40);
        assert!(prepare_create_turn(&adapter, &profile, &wrong_base, prompt.to_vec()).is_err());

        fs::write(fixture.workspace.join("tracked.txt"), b"dirty\n").unwrap();
        let clean_control = fixture.control(
            "task-dirty",
            "logical-dirty",
            None,
            1,
            1,
            &fixture.first_head,
        );
        assert!(prepare_create_turn(&adapter, &profile, &clean_control, prompt.to_vec()).is_err());
        fixture.refresh(&fixture.first_head);

        git_ok(
            &fixture.git,
            &fixture.workspace,
            &["replace", &fixture.first_head, &fixture.base_revision],
        );
        assert!(prepare_create_turn(&adapter, &profile, &clean_control, prompt.to_vec()).is_err());
        git_ok(
            &fixture.git,
            &fixture.workspace,
            &["replace", "-d", &fixture.first_head],
        );

        let alternates = fixture.workspace.join(".git/objects/info/alternates");
        fs::write(&alternates, b"/forbidden/object/store\n").unwrap();
        assert!(prepare_create_turn(&adapter, &profile, &clean_control, prompt.to_vec()).is_err());
        fs::remove_file(alternates).unwrap();

        let wrong_environment = ExecutionEnvironmentLease::capture(
            "lease-wrong-reviewer-environment",
            "epoch-reviewers",
            &ClaudeReviewerAdapter::environment_policy().unwrap(),
            vec![
                ("CARGO_HOME".into(), INSIDE_CARGO_HOME.into()),
                ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS".into(), "1".into()),
                ("CLAUDE_CODE_DISABLE_FAST_MODE".into(), "0".into()),
                (
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC".into(),
                    "1".into(),
                ),
                (
                    "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION".into(),
                    "false".into(),
                ),
                ("CLAUDE_CONFIG_DIR".into(), INSIDE_NATIVE_CONFIG.into()),
                ("HOME".into(), INSIDE_HOME.into()),
                ("PATH".into(), INSIDE_PATH.into()),
                ("RUSTUP_HOME".into(), INSIDE_RUSTUP_HOME.into()),
                ("TMPDIR".into(), INSIDE_TEMP.into()),
                ("XDG_CACHE_HOME".into(), "/hcom/home/.cache".into()),
                ("XDG_CONFIG_HOME".into(), "/hcom/home/.config".into()),
                ("XDG_DATA_HOME".into(), "/hcom/home/.data".into()),
                ("XDG_RUNTIME_DIR".into(), INSIDE_RUNTIME.into()),
                ("XDG_STATE_HOME".into(), "/hcom/home/.state".into()),
            ],
        )
        .unwrap();
        let claude = fixture.claude_adapter();
        let claude_control = fixture.control(
            "task-env-drift",
            "logical-env-drift",
            Some(CLAUDE_SESSION),
            1,
            1,
            &fixture.first_head,
        );
        let prepared =
            prepare_create_turn(&claude, &claude.profile(), &claude_control, prompt.to_vec())
                .unwrap();
        let root = ArtifactRoot::open(&fixture.artifact_root).unwrap();
        let attempt = ArtifactAttempt::create(
            &root,
            ArtifactScope {
                run_id: claude_control.run_id.clone(),
                task_id: claude_control.task_id.clone(),
                role: WorkerRole::Reviewer,
                logical_session_id: claude_control.logical_session_id.clone(),
                turn_sequence: 1,
                attempt: 1,
            },
            &wrong_environment,
            prompt,
        )
        .unwrap();
        let materialized = wrong_environment
            .materialize(
                "epoch-reviewers",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Reviewer,
                    run_id: claude_control.run_id.clone(),
                    task_id: claude_control.task_id.clone(),
                },
            )
            .unwrap();
        assert!(
            ProcessRunner::default()
                .spawn(WorkerRole::Reviewer, prepared, &materialized, attempt)
                .is_err()
        );

        fs::write(&fixture.codex_auth_source, b"drifted-auth").unwrap();
        assert!(prepare_create_turn(&adapter, &profile, &clean_control, prompt.to_vec()).is_err());

        let version_fixture = Fixture::new();
        let version_adapter = version_fixture.codex_adapter();
        write_executable(
            &version_fixture.codex,
            &format!("{}\n# executable identity drift\n", fake_codex_script()),
        );
        assert!(
            prepare_create_turn(
                &version_adapter,
                &version_adapter.profile(),
                &version_fixture.control(
                    "task-version-drift",
                    "logical-version-drift",
                    None,
                    1,
                    1,
                    &version_fixture.first_head,
                ),
                prompt.to_vec(),
            )
            .is_err()
        );
    }

    #[test]
    fn strict_native_results_reject_wrong_model_session_semantics_and_check_claims() {
        let fixture = Fixture::new();
        let claude = fixture.claude_adapter();
        let control = fixture.control(
            "task-result",
            "logical-result",
            Some(CLAUDE_SESSION),
            1,
            1,
            &fixture.first_head,
        );
        let lgtm = serde_json::json!({
            "decision": "lgtm",
            "summary": "no blocking issue",
            "findings": [],
            "checks": []
        });
        let wrong_model = claude_artifacts(
            CLAUDE_SESSION,
            lgtm.clone(),
            serde_json::json!({"claude-haiku-4-5": {}}),
        );
        assert!(claude.extract_result(&control, &wrong_model).is_err());
        let extra_model = claude_artifacts(
            CLAUDE_SESSION,
            lgtm.clone(),
            serde_json::json!({
                "claude-opus-5": {},
                "claude-haiku-4-5": {}
            }),
        );
        assert!(claude.extract_result(&control, &extra_model).is_err());
        let wrong_session = claude_artifacts(
            OTHER_CLAUDE_SESSION,
            lgtm.clone(),
            serde_json::json!({"claude-opus-5": {}}),
        );
        assert!(claude.extract_result(&control, &wrong_session).is_err());
        let request_without_major = claude_artifacts(
            CLAUDE_SESSION,
            serde_json::json!({
                "decision": "request_changes",
                "summary": "only a minor",
                "findings": [{
                    "severity": "minor",
                    "title": "Non-blocking",
                    "body": "This must not drive request_changes.",
                    "file": null,
                    "line": null
                }],
                "checks": []
            }),
            serde_json::json!({"claude-opus-5": {}}),
        );
        assert!(
            claude
                .extract_result(&control, &request_without_major)
                .is_err()
        );
        let lgtm_with_major = claude_artifacts(
            CLAUDE_SESSION,
            serde_json::json!({
                "decision": "lgtm",
                "summary": "contradictory",
                "findings": [{
                    "severity": "major",
                    "title": "Blocking",
                    "body": "LGTM cannot carry this finding.",
                    "file": "tracked.txt",
                    "line": 1
                }],
                "checks": []
            }),
            serde_json::json!({"claude-opus-5": {}}),
        );
        assert!(claude.extract_result(&control, &lgtm_with_major).is_err());

        let codex = fixture.codex_adapter();
        let codex_control = fixture.control(
            "task-codex-result",
            "logical-codex-result",
            None,
            1,
            1,
            &fixture.first_head,
        );
        let stdout = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-reviewer-1\"}\n",
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"turn.completed\"}\n"
        );
        let result = serde_json::json!({
            "decision": "lgtm",
            "summary": "claims an unobserved check",
            "findings": [],
            "checks": [{
                "command": "cargo test",
                "status": "passed",
                "summary": "claimed passed"
            }]
        });
        let artifacts = NativeArtifacts::new(
            WorkerRole::Reviewer,
            stdout.as_bytes().to_vec(),
            vec![],
            Some(serde_json::to_vec(&result).unwrap()),
        )
        .unwrap();
        assert!(codex.extract_result(&codex_control, &artifacts).is_err());
    }

    #[test]
    fn real_bwrap_enforces_erofs_and_masks_live_host_control_socket() {
        let fixture = Fixture::new();
        let control_root = fixture.host_runtime_dir.join("hcom-architect-session");
        fs::create_dir(&control_root).unwrap();
        fs::set_permissions(&control_root, fs::Permissions::from_mode(0o700)).unwrap();
        let socket_path = control_root.join("control.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).unwrap();
        let outside = UnixStream::connect(&socket_path).unwrap();
        drop(outside);

        let probe_path = fixture.temp_dir.join("sandbox-probe.py");
        write_executable(&probe_path, &sandbox_probe_script(&socket_path));
        write_executable(
            &fixture.codex,
            &fake_codex_script_with_sandbox_probe(&probe_path),
        );
        let real_bwrap = fs::canonicalize(BWRAP_EXECUTABLE).unwrap();
        let adapter = CodexReviewerAdapter::discover_with_paths(
            fixture.codex_config(),
            &fixture.codex,
            &real_bwrap,
            &fixture.git,
        )
        .unwrap();
        let control = fixture.control(
            "task-real-bwrap",
            "logical-real-bwrap",
            None,
            1,
            1,
            &fixture.first_head,
        );
        let completion = fixture.run(
            &adapter,
            &adapter.profile(),
            &control,
            fixture.codex_lease("lease-real-bwrap"),
            b"real bwrap reviewer isolation probe",
        );
        assert_eq!(completion.exit.code, Some(0));
        assert!(completion.exit.signal.is_none());
        adapter
            .extract_result(&control, &completion.artifacts)
            .unwrap();
        assert!(!fixture.workspace.join("reviewer-write-probe").exists());
        drop(listener);
    }

    #[test]
    fn claude_auth_mount_target_must_exist_with_exact_private_permissions() {
        let fixture = Fixture::new();
        let target = fixture.claude_config_dir.join(CLAUDE_AUTH_FILE);
        fs::set_permissions(&target, fs::Permissions::from_mode(0o664)).unwrap();
        let error = ClaudeReviewerAdapter::discover_with_paths(
            fixture.claude_config(),
            &fixture.claude,
            &fixture.bwrap,
            &fixture.git,
        )
        .err()
        .expect("group-readable Claude auth target must fail before spawn");
        assert!(
            format!("{error:#}").contains("unsafe ownership, links, or permissions"),
            "unexpected error: {error:#}"
        );
    }

    fn assert_closed_codex_argv(argv: &[String], fixture: &Fixture, prompt: &[u8]) {
        assert!(argv.iter().any(|argument| argument == "--die-with-parent"));
        assert!(argv.iter().any(|argument| argument == "--unshare-pid"));
        assert!(
            argv.iter()
                .any(|argument| argument == "--dangerously-bypass-approvals-and-sandbox")
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", CODEX_REVIEWER_MODEL])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--disable", "hooks"]));
        assert!(!argv.iter().any(|argument| argument == "--new-session"));
        assert!(!argv.iter().any(|argument| argument == "--last"));
        assert!(!argv.iter().any(|argument| argument == "--ephemeral"));
        assert_ro_workspace_and_no_authority(argv, fixture, prompt);
    }

    fn assert_closed_claude_argv(
        argv: &[String],
        fixture: &Fixture,
        prompt: &[u8],
        session_flag: &str,
    ) {
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", CLAUDE_REVIEWER_MODEL])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--effort", "high"]));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--permission-mode", "bypassPermissions"])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--tools", "Bash,Read"]));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--setting-sources", "project"])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--mcp-config", r#"{"mcpServers":{}}"#])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair[0] == session_flag && pair[1] == CLAUDE_SESSION)
        );
        assert!(
            argv.iter()
                .any(|argument| argument == "--strict-mcp-config")
        );
        assert!(
            argv.iter()
                .any(|argument| argument == "--disable-slash-commands")
        );
        assert!(!argv.iter().any(|argument| argument == "--new-session"));
        assert!(!argv.iter().any(|argument| argument == "--last"));
        assert_ro_workspace_and_no_authority(argv, fixture, prompt);
    }

    fn assert_ro_workspace_and_no_authority(argv: &[String], fixture: &Fixture, prompt: &[u8]) {
        let workspace = fixture.workspace.to_str().unwrap();
        assert!(
            argv.windows(3)
                .any(|part| part == ["--ro-bind", workspace, INSIDE_WORKSPACE])
        );
        assert!(
            !argv
                .windows(3)
                .any(|part| part == ["--bind", workspace, INSIDE_WORKSPACE])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--tmpfs", INSIDE_RUNTIME])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--tmpfs", INSIDE_TEMP]));
        assert!(!argv.windows(3).any(|part| part == ["--ro-bind", "/", "/"]));
        let joined = argv.join("\0");
        assert!(!joined.contains(&String::from_utf8_lossy(prompt).to_string()));
        assert!(!joined.contains(fixture.developer_worktree.to_str().unwrap()));
        assert!(!joined.contains("hcom-architect-session/control.sock"));
        assert!(!joined.contains("HCOM_AGENT"));
        assert!(!joined.contains("CHAIN"));
        assert!(!joined.contains("HANDOFF"));
    }

    fn claude_artifacts(
        session_id: &str,
        structured_output: serde_json::Value,
        model_usage: serde_json::Value,
    ) -> NativeArtifacts {
        let envelope = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "session_id": session_id,
            "structured_output": structured_output,
            "modelUsage": model_usage
        });
        NativeArtifacts::new(
            WorkerRole::Reviewer,
            serde_json::to_vec(&envelope).unwrap(),
            vec![],
            None,
        )
        .unwrap()
    }

    fn write_private_file(path: &Path, contents: &[u8]) {
        fs::write(path, contents).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn fake_bwrap_script() -> &'static str {
        r#"#!/bin/sh
set -eu
if [ "${1-}" = "--version" ]; then
    printf '%s\n' 'bubblewrap 0.9.0'
    exit 0
fi
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    shift
done
[ "$#" -gt 0 ]
shift
exec "$@"
"#
    }

    fn fake_codex_script() -> &'static str {
        r#"#!/bin/sh
set -eu
if [ "${1-}" = "--version" ]; then
    printf '%s\n' 'codex-cli 0.145.0'
    exit 0
fi
# SANDBOX_PROBE
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
[ "${HCOM_WORKER_ROLE-}" = reviewer ]
[ -n "${HCOM_RUN_ID-}" ] && [ -n "${HCOM_TASK_ID-}" ]
[ -z "${HCOM_AGENT-}" ] && [ -z "${TERM-}" ] && [ -z "${STY-}" ]
[ -n "${HOME-}" ] && [ -n "${CODEX_HOME-}" ] && [ -n "${TMPDIR-}" ]
[ "${HOME-}" = /hcom/home ] && [ "${CODEX_HOME-}" = /hcom/native ]
[ "${TMPDIR-}" = /tmp ] && [ "${XDG_RUNTIME_DIR-}" = /hcom/run ]
[ "${CARGO_HOME-}" = /hcom/home/.cargo ]
[ "${RUSTUP_HOME-}" = /hcom/toolchains/rust/rustup ]
[ "$1" = exec ]
shift
session=native-codex-reviewer-1
if [ "${1-}" = resume ]; then
    shift
    session="$1"
    shift
fi
output=
schema=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message)
            shift
            output="$1"
            ;;
        --output-schema)
            shift
            schema="$1"
            ;;
        --model|--config|--disable|--cd)
            shift
            ;;
        -)
            ;;
    esac
    shift
done
[ -n "$output" ] && [ -s "$schema" ]
grep -q '"\$schema":"https://json-schema.org/draft/2020-12/schema"' "$schema"
prompt=$(sed -n '1,$p')
[ -n "$prompt" ]
printf '{"type":"thread.started","thread_id":"%s"}\n' "$session"
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"check-1","type":"command_execution","command":"git diff --check","exit_code":0,"status":"completed"}}'
printf '%s' '{"decision":"lgtm","summary":"fake exact review passed","findings":[],"checks":[{"command":"git diff --check","status":"passed","summary":"clean"}]}' >"$output"
printf '%s\n' '{"type":"turn.completed"}'
"#
    }

    fn fake_claude_script() -> &'static str {
        r#"#!/bin/sh
set -eu
if [ "${1-}" = "--version" ]; then
    printf '%s\n' '2.1.220 (Claude Code)'
    exit 0
fi
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
[ "${HCOM_WORKER_ROLE-}" = reviewer ]
[ -n "${HCOM_RUN_ID-}" ] && [ -n "${HCOM_TASK_ID-}" ]
[ -z "${HCOM_AGENT-}" ] && [ -z "${TERM-}" ] && [ -z "${STY-}" ]
[ -n "${HOME-}" ] && [ -n "${CLAUDE_CONFIG_DIR-}" ] && [ -n "${TMPDIR-}" ]
[ "${HOME-}" = /hcom/home ] && [ "${CLAUDE_CONFIG_DIR-}" = /hcom/native ]
[ "${TMPDIR-}" = /tmp ] && [ "${XDG_RUNTIME_DIR-}" = /hcom/run ]
[ "${CARGO_HOME-}" = /hcom/home/.cargo ]
[ "${RUSTUP_HOME-}" = /hcom/toolchains/rust/rustup ]
[ "${CLAUDE_CODE_DISABLE_BACKGROUND_TASKS-}" = 1 ]
[ "${CLAUDE_CODE_DISABLE_FAST_MODE-}" = 1 ]
[ "${CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC-}" = 1 ]
[ "${CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION-}" = false ]
session=
schema=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --session-id|--resume)
            shift
            session="$1"
            ;;
        --json-schema)
            shift
            schema="$1"
            ;;
    esac
    shift
done
[ -n "$session" ] && [ -n "$schema" ]
if printf '%s' "$schema" | grep -q '"\$schema"'; then
    exit 91
fi
printf '%s' "$schema" | grep -q '"additionalProperties":false'
printf '%s' "$schema" | grep -q '"required":\["decision","summary","findings","checks"\]'
prompt=$(sed -n '1,$p')
[ -n "$prompt" ]
printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","structured_output":{"decision":"lgtm","summary":"fake exact Claude review passed","findings":[],"checks":[]},"modelUsage":{"claude-opus-5":{}}}\n' "$session"
"#
    }

    fn fake_codex_script_with_sandbox_probe(probe_path: &Path) -> String {
        let source = fs::read_to_string(probe_path).unwrap();
        let body = source
            .strip_prefix("#!/usr/bin/python3\n")
            .expect("sandbox probe must use the exact Python interpreter");
        assert!(!body.lines().any(|line| line == "HCOM_PROBE_EOF"));
        fake_codex_script().replace(
            "# SANDBOX_PROBE",
            &format!("/usr/bin/python3 - <<'HCOM_PROBE_EOF'\n{body}\nHCOM_PROBE_EOF"),
        )
    }

    fn sandbox_probe_script(socket_path: &Path) -> String {
        let socket_path = serde_json::to_string(socket_path.to_str().unwrap()).unwrap();
        format!(
            r#"#!/usr/bin/python3
import errno
import os
import socket

try:
    open("reviewer-write-probe", "wb").close()
except OSError as error:
    if error.errno != errno.EROFS:
        raise SystemExit(41)
else:
    raise SystemExit(42)

path = {socket_path}
if os.path.exists(path):
    raise SystemExit(43)
client = socket.socket(socket.AF_UNIX)
try:
    client.connect(path)
except FileNotFoundError:
    pass
else:
    raise SystemExit(44)
"#
        )
    }

    fn git_ok(git: &Path, workspace: &Path, args: &[&str]) {
        let output = Command::new(git)
            .args(args)
            .current_dir(workspace)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git fixture command failed: {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_line(git: &Path, workspace: &Path, args: &[&str]) -> String {
        let output = Command::new(git)
            .args(args)
            .current_dir(workspace)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().into()
    }
}
