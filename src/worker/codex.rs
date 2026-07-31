//! Exact-version Codex no-TUI developer adapter.

use super::contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeOutputKind, NativeResult, OuterLaunchEnvelope, OutputDeclaration,
    ResultTransport, SchemaTransport, TurnControl, WorkerAdapter, WorkerProfile,
    validate_native_session_id,
};
use super::environment::{EnvironmentPolicy, ExactEnvironmentRequirement};
use super::profile::{CodexInvocationProfile, CodexSandbox, validate_cli_help_contract};
use super::result::{
    CheckStatus, CommitSummary, DeveloperDecision, DeveloperResult, MAX_RESULT_BYTES,
};
use super::sandbox::{HostRootAccess, HostRootContract, HostRootMounts};
use super::validation::{
    MAX_ITEMS, MAX_PATH_BYTES, validate_git_oid, validate_relative_path, validate_text,
};
use crate::control_api::{CapabilitySnapshot, NativeSessionMode, WorkerRole};
use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use std::collections::BTreeSet;
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

pub const CODEX_DEVELOPER_EXECUTABLE: &str =
    "/home/ywxk/.codex/packages/standalone/releases/0.145.0-x86_64-unknown-linux-musl/bin/codex";
pub const CODEX_DEVELOPER_CLI_VERSION: &str = "codex-cli 0.145.0";
pub const CODEX_DEVELOPER_MODEL: &str = "gpt-5.6-sol";
pub const CODEX_DEVELOPER_REASONING: &str = "xhigh";
pub const BWRAP_EXECUTABLE: &str = "/usr/bin/bwrap";
pub const BWRAP_VERSION: &str = "bubblewrap 0.9.0";
pub const GIT_EXECUTABLE: &str = "/usr/bin/git";
pub const GIT_VERSION: &str = "git version 2.43.0";

const ADAPTER_NAME: &str = "codex-developer-0.145.0";
const ADAPTER_CONTRACT_VERSION: u32 = 7;
const OUTER_POLICY: &str = "bubblewrap-0.9.0-host-path-developer-repo-rw-v1";
const MAX_CODEX_EVENTS: usize = 4096;
pub(super) const MAX_CODEX_EVENT_BYTES: usize = 128 * 1024;
pub(super) const MAX_CODEX_JSONL_BYTES: usize = 1024 * 1024;
const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_DURATION: Duration = Duration::from_secs(30);
// Keep these inventories and their runtime-option coverage tests in sync with
// docs/codex-adapter-contract.md.
pub(super) const CODEX_EXEC_HELP_REQUIREMENTS: &[&str] = &[
    "resume",
    "--config",
    "--disable",
    "--strict-config",
    "--model",
    "--sandbox",
    "--skip-git-repo-check",
    "--cd",
    "--add-dir",
    "--ignore-user-config",
    "--ignore-rules",
    "--output-schema",
    "--json",
    "--output-last-message",
];
pub(super) const CODEX_RESUME_HELP_REQUIREMENTS: &[&str] = &[
    "--config",
    "--disable",
    "--strict-config",
    "--model",
    "--ignore-user-config",
    "--ignore-rules",
    "--output-schema",
    "--json",
    "--output-last-message",
];
const CODEX_RESULT_SCHEMA_FILE: &str = "codex-developer-result-schema.json";
const CODEX_FINAL_FILE: &str = "native-final.partial";
const CODEX_AUTH_FILE: &str = "auth.json";
pub(super) const CODEX_JSONL_EVENT_BOUND_CAPABILITY: &str = "codex-jsonl-large-command-output-v1";

pub(crate) const DISABLED_CODEX_FEATURES: &[&str] = &[
    "apps",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode_host",
    "collaboration_modes",
    "computer_use",
    "enable_fanout",
    "goals",
    "guardian_approval",
    "hooks",
    "image_generation",
    "in_app_browser",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "plugins",
    "plugin_sharing",
    "remote_plugin",
    "shell_snapshot",
    "skill_mcp_dependency_install",
    "skill_search",
    "tool_call_mcp_elicitation",
    "workspace_dependencies",
];

#[derive(Clone, PartialEq, Eq)]
pub struct CodexDeveloperConfig {
    pub run_id: String,
    pub launch_cwd: PathBuf,
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
    pub invocation: CodexInvocationProfile,
}

pub struct CodexDeveloperAdapter {
    descriptor: AdapterDescriptor,
    executable: ExecutableIdentity,
    outer_executable: ExecutableIdentity,
    git_executable: ExecutableIdentity,
    sandbox: SandboxContract,
    invocation: CodexInvocationProfile,
}

impl CodexDeveloperAdapter {
    pub fn discover(config: CodexDeveloperConfig) -> Result<Self> {
        validate_production_runtime_contract(&config)?;
        validate_codex_exec_cli(Path::new(CODEX_DEVELOPER_EXECUTABLE))?;
        Self::discover_with_paths(
            config,
            Path::new(CODEX_DEVELOPER_EXECUTABLE),
            Path::new(BWRAP_EXECUTABLE),
            Path::new(GIT_EXECUTABLE),
        )
    }

    pub fn environment_policy() -> Result<EnvironmentPolicy> {
        let overrides = [
            "CARGO_HOME",
            "CODEX_HOME",
            "HOME",
            "PYTHONPYCACHEPREFIX",
            "RUSTUP_HOME",
            "TMPDIR",
            "XDG_RUNTIME_DIR",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        EnvironmentPolicy::new(
            overrides,
            vec![
                "CARGO_HOME".into(),
                "CODEX_HOME".into(),
                "HOME".into(),
                "PATH".into(),
                "PYTHONPYCACHEPREFIX".into(),
                "RUSTUP_HOME".into(),
                "TMPDIR".into(),
                "XDG_RUNTIME_DIR".into(),
            ],
        )
    }

    pub fn profile(&self) -> WorkerProfile {
        WorkerProfile {
            role: WorkerRole::Developer,
            adapter: self.descriptor.name.clone(),
            model: self.descriptor.model.clone(),
            reasoning: self.descriptor.reasoning.clone(),
            policy: self.descriptor.policy.clone(),
            executable: self.executable.clone(),
            cli_version: self.descriptor.cli_version.clone(),
            adapter_contract_version: self.descriptor.contract_version,
            native_session_mode: self.descriptor.capabilities.native_session_mode,
            capability: CapabilitySnapshot {
                contract_hash: self.descriptor.capability_contract_hash.clone(),
                features: self.descriptor.capabilities.features.clone(),
            },
        }
    }

    fn discover_with_paths(
        config: CodexDeveloperConfig,
        codex_path: &Path,
        bwrap_path: &Path,
        git_path: &Path,
    ) -> Result<Self> {
        validate_codex_developer_invocation(&config.invocation)?;
        let invocation = config.invocation.clone();
        let executable = capture_exact_tool(codex_path, CODEX_DEVELOPER_CLI_VERSION)?;
        let outer_executable = capture_exact_tool(bwrap_path, BWRAP_VERSION)?;
        let git_executable = capture_exact_tool(git_path, GIT_VERSION)?;
        let sandbox =
            SandboxContract::capture(config, &executable, &outer_executable, &git_executable)?;
        let descriptor = developer_descriptor(&invocation)?;
        Ok(Self {
            descriptor,
            executable,
            outer_executable,
            git_executable,
            sandbox,
            invocation,
        })
    }

    fn command(
        &self,
        control: &TurnControl,
        resume_session_id: Option<&str>,
    ) -> Result<CommandSpec> {
        control.validate()?;
        if control.role != WorkerRole::Developer {
            bail!("Codex developer adapter cannot build a reviewer turn");
        }
        revalidate_exact_tool(&self.executable, CODEX_DEVELOPER_CLI_VERSION)?;
        revalidate_exact_tool(&self.outer_executable, BWRAP_VERSION)?;
        revalidate_exact_tool(&self.git_executable, GIT_VERSION)?;
        self.sandbox.revalidate(
            &self.executable,
            &self.outer_executable,
            &self.git_executable,
        )?;
        validate_codex_developer_invocation(&self.invocation)?;

        let mut fixed_argv = vec![
            "exec".into(),
            "--sandbox".into(),
            self.invocation.sandbox.as_str().into(),
            "--skip-git-repo-check".into(),
        ];
        if self.invocation.sandbox == CodexSandbox::WorkspaceWrite
            && self.sandbox.workspace.path() != self.sandbox.launch_cwd.path()
        {
            fixed_argv.extend([
                "--add-dir".into(),
                path_string(
                    "Codex developer task repository",
                    self.sandbox.workspace.path(),
                )?,
            ]);
        }
        if let Some(session_id) = resume_session_id {
            validate_native_session_id(session_id)?;
            fixed_argv.extend(["resume".into(), session_id.into()]);
        }
        fixed_argv.extend([
            "--json".into(),
            "--strict-config".into(),
            "--model".into(),
            self.invocation.model.clone(),
            "--config".into(),
            self.invocation.reasoning_config_argument(),
            "--config".into(),
            self.invocation.approval_config_argument(),
            "--config".into(),
            "mcp_servers={}".into(),
            "--ignore-user-config".into(),
            "--ignore-rules".into(),
        ]);
        for feature in DISABLED_CODEX_FEATURES {
            fixed_argv.extend(["--disable".into(), (*feature).into()]);
        }
        if resume_session_id.is_none() {
            fixed_argv.extend([
                "--cd".into(),
                path_string("Codex launch cwd", self.sandbox.launch_cwd.path())?,
            ]);
        }
        let expected_artifact_dir = self
            .sandbox
            .artifact_root
            .path()
            .join(&control.artifact_dir);
        let outer_launch = OuterLaunchEnvelope {
            executable: self.outer_executable.clone(),
            fixed_argv: self
                .sandbox
                .outer_argv(&expected_artifact_dir, &self.executable)?,
            expected_artifact_dir,
            inside_executable: self.executable.canonical_path.clone(),
            inside_artifact_dir: self
                .sandbox
                .artifact_root
                .path()
                .join(&control.artifact_dir),
        };
        Ok(CommandSpec {
            executable: self.executable.clone(),
            fixed_argv,
            schema_transport: SchemaTransport::File {
                argument: "--output-schema".into(),
                relative_path: CODEX_RESULT_SCHEMA_FILE.into(),
                contents: developer_result_schema(),
            },
            expected_outputs: vec![
                OutputDeclaration {
                    kind: NativeOutputKind::StdoutEnvelope,
                    relative_path: "native.stdout.partial".into(),
                    max_bytes: MAX_CODEX_JSONL_BYTES,
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
            workspace_cwd: self.sandbox.launch_cwd.path().to_owned(),
            outer_launch: Some(outer_launch),
            exact_environment: self.sandbox.exact_environment()?,
        })
    }
}

fn validate_codex_developer_invocation(invocation: &CodexInvocationProfile) -> Result<()> {
    invocation.validate("Codex developer")?;
    if invocation.sandbox == CodexSandbox::ReadOnly {
        bail!(
            "Codex developer sandbox must be workspace-write or danger-full-access because a completed developer turn must commit"
        );
    }
    Ok(())
}

fn validate_codex_exec_cli(path: &Path) -> Result<()> {
    validate_codex_worker_cli(path, "Codex developer CLI")
}

pub(super) fn validate_codex_worker_cli(path: &Path, label: &str) -> Result<()> {
    let mut command = Command::new(path);
    command.args(["exec", "--help"]).env_clear();
    let output = run_bounded_command(command, 128 * 1024)?;
    if !output.status.success() || !output.stderr.is_empty() {
        bail!("{label} capability probe failed");
    }
    validate_cli_help_contract(label, &output.stdout, CODEX_EXEC_HELP_REQUIREMENTS)?;

    // `--sandbox` belongs to the `exec` parent in Codex 0.145. Keep it before
    // `resume`; the resume subcommand does not declare that option itself.
    let mut resume = Command::new(path);
    resume
        .args(["exec", "--sandbox", "read-only", "resume", "--help"])
        .env_clear();
    let output = run_bounded_command(resume, 128 * 1024)?;
    if !output.status.success() || !output.stderr.is_empty() {
        bail!("{label} resume capability probe failed");
    }
    validate_cli_help_contract(
        &format!("{label} resume"),
        &output.stdout,
        CODEX_RESUME_HELP_REQUIREMENTS,
    )
}

fn path_string(label: &str, path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{label} is not valid UTF-8"))
}

impl WorkerAdapter for CodexDeveloperAdapter {
    fn descriptor(&self) -> &AdapterDescriptor {
        &self.descriptor
    }

    fn executable_contract(&self) -> &ExecutableIdentity {
        &self.executable
    }

    fn build_create(&self, control: &TurnControl) -> Result<CommandSpec> {
        if control.native_session_id.is_some() {
            bail!("Codex discovered-session create cannot pre-bind a native session");
        }
        self.command(control, None)
    }

    fn build_resume(&self, native_session_id: &str, control: &TurnControl) -> Result<CommandSpec> {
        if control.native_session_id.as_deref() != Some(native_session_id) {
            bail!("Codex resume must use the exact session-bound native session");
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
        control.validate()?;
        if control.role != WorkerRole::Developer || artifacts.role() != WorkerRole::Developer {
            bail!("Codex developer result role does not match its exact turn");
        }
        validate_codex_worker_stderr(artifacts.stderr())
            .context("Codex developer emitted unexpected stderr")?;
        let evidence = parse_codex_turn(control, artifacts.stdout())?;
        let final_output = artifacts
            .final_output()
            .ok_or_else(|| anyhow!("Codex developer final result is missing"))?;
        let result = DeveloperResult::parse(final_output)
            .context("Codex developer final result is not strict DeveloperResult JSON")?;
        for check in &result.checks {
            if check.status == CheckStatus::Passed
                && !evidence.completed_commands.contains(&check.command)
            {
                bail!("Codex developer reported a passed check without current-turn evidence");
            }
            if check.status == CheckStatus::Passed
                && evidence.failed_commands.contains(&check.command)
            {
                bail!("Codex developer reported a passed check with conflicting turn evidence");
            }
        }
        if result.decision == DeveloperDecision::Completed {
            self.sandbox
                .validate_completed_result(control, &result, &self.git_executable)?;
        }
        Ok(NativeResult::Developer {
            native_session_id: evidence.native_session_id,
            result,
        })
    }
}

pub(crate) fn validate_codex_worker_stderr(stderr: &[u8]) -> Result<()> {
    if stderr.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let text = std::str::from_utf8(stderr).context("Codex worker stderr is not UTF-8")?;
    validate_text("Codex worker stderr", text, 4096, true)?;
    let mut lines = 0usize;
    for line in text.lines() {
        lines += 1;
        if lines > 4 {
            bail!("Codex worker emitted too many recoverable router diagnostics");
        }
        let (timestamp, detail) = line
            .split_once(" ERROR codex_core::tools::router: error=exec_command failed for ")
            .ok_or_else(|| anyhow!("Codex worker stderr is not a recoverable router diagnostic"))?;
        if !is_codex_log_timestamp(timestamp)
            || !detail.contains("`: CreateProcess { message: \"Rejected(\\\"`")
            || !detail.ends_with(
                "rejected: rm -f style commands are not permitted. \
Use a safer approach\\\")\" }",
            )
        {
            bail!("Codex worker stderr is not the pinned safe-delete rejection");
        }
    }
    if lines == 0 {
        bail!("Codex worker stderr did not contain a diagnostic");
    }
    Ok(())
}

fn is_codex_log_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 22
        || bytes.last() != Some(&b'Z')
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || bytes.get(19) != Some(&b'.')
    {
        return false;
    }
    bytes[..19]
        .iter()
        .enumerate()
        .all(|(index, byte)| matches!(index, 4 | 7 | 10 | 13 | 16) || byte.is_ascii_digit())
        && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit)
}

fn developer_descriptor(invocation: &CodexInvocationProfile) -> Result<AdapterDescriptor> {
    let policy = invocation.effective_policy(OUTER_POLICY);
    AdapterDescriptor::new(
        ADAPTER_NAME,
        ADAPTER_CONTRACT_VERSION,
        CODEX_DEVELOPER_CLI_VERSION,
        &invocation.model,
        &invocation.reasoning_effort,
        &policy,
        AdapterCapabilities {
            roles: vec![WorkerRole::Developer],
            native_session_mode: NativeSessionMode::Discovered,
            result_transport: ResultTransport::FinalFile,
            features: vec![
                "complete-parent-environment-v1".into(),
                CODEX_JSONL_EVENT_BOUND_CAPABILITY.into(),
                "exact-resume".into(),
                "host-git-evidence".into(),
                "native-add-dir-task-repository".into(),
                "outer-bwrap-host-path-developer-repo-rw-v1".into(),
                "structured-result".into(),
            ],
        },
    )
}

struct SandboxContract {
    run_id: String,
    launch_cwd: DirectoryIdentity,
    workspace: DirectoryIdentity,
    artifact_root: DirectoryIdentity,
    isolated_home: DirectoryIdentity,
    codex_home: DirectoryIdentity,
    temp_dir: DirectoryIdentity,
    runtime_dir: DirectoryIdentity,
    host_runtime_dir: DirectoryIdentity,
    auth_source: FileIdentity,
    auth_target: FileIdentity,
    git_workspace: GitWorkspaceIdentity,
    host_root: HostRootContract,
}

impl SandboxContract {
    fn capture(
        config: CodexDeveloperConfig,
        codex: &ExecutableIdentity,
        bwrap: &ExecutableIdentity,
        git: &ExecutableIdentity,
    ) -> Result<Self> {
        let launch_cwd = DirectoryIdentity::capture(&config.launch_cwd, false)?;
        let workspace = DirectoryIdentity::capture(&config.workspace_cwd, false)?;
        let artifact_root = DirectoryIdentity::capture(&config.artifact_root, true)?;
        let isolated_home = DirectoryIdentity::capture(&config.isolated_home, true)?;
        let codex_home = DirectoryIdentity::capture(&config.codex_home, true)?;
        let temp_dir = DirectoryIdentity::capture(&config.temp_dir, true)?;
        let runtime_dir = DirectoryIdentity::capture(&config.runtime_dir, true)?;
        let host_runtime_dir = DirectoryIdentity::capture(&config.host_runtime_dir, true)?;
        let auth_source = FileIdentity::capture(&config.auth_source)?;
        let auth_target_path = codex_home.path().join(CODEX_AUTH_FILE);
        let auth_target = FileIdentity::capture(&auth_target_path)?;
        if auth_source.path() == auth_target.path() {
            bail!("Codex auth source must be distinct from the isolated mount target");
        }
        if paths_overlap(auth_source.path(), host_runtime_dir.path()) {
            bail!("Codex auth source must not overlap the masked host runtime directory");
        }

        for (left_label, left, right_label, right) in [
            (
                "workspace",
                workspace.path(),
                "artifact root",
                artifact_root.path(),
            ),
            (
                "workspace",
                workspace.path(),
                "isolated HOME",
                isolated_home.path(),
            ),
            (
                "workspace",
                workspace.path(),
                "isolated CODEX_HOME",
                codex_home.path(),
            ),
            (
                "artifact root",
                artifact_root.path(),
                "isolated HOME",
                isolated_home.path(),
            ),
            (
                "artifact root",
                artifact_root.path(),
                "isolated CODEX_HOME",
                codex_home.path(),
            ),
        ] {
            if paths_overlap(left, right) {
                bail!("{left_label} and {right_label} must not overlap");
            }
        }
        if !codex_home.path().starts_with(isolated_home.path())
            || codex_home.path() == isolated_home.path()
        {
            bail!("isolated CODEX_HOME must be a strict child of isolated HOME");
        }

        let writable_roots = [workspace.path(), isolated_home.path(), artifact_root.path()];
        for protected in [
            codex.canonical_path.as_path(),
            bwrap.canonical_path.as_path(),
            git.canonical_path.as_path(),
            auth_source.path(),
        ] {
            if writable_roots
                .iter()
                .any(|root| protected.starts_with(root))
            {
                bail!("sandbox writable roots must not contain a protected host file");
            }
        }
        let git_workspace = GitWorkspaceIdentity::capture(workspace.path(), git)?;
        let host_root =
            HostRootContract::capture(&config.cargo_bin_source, &config.rustup_home_source)?;
        Ok(Self {
            run_id: config.run_id,
            launch_cwd,
            workspace,
            artifact_root,
            isolated_home,
            codex_home,
            temp_dir,
            runtime_dir,
            host_runtime_dir,
            auth_source,
            auth_target,
            git_workspace,
            host_root,
        })
    }

    fn revalidate(
        &self,
        codex: &ExecutableIdentity,
        bwrap: &ExecutableIdentity,
        git: &ExecutableIdentity,
    ) -> Result<()> {
        codex.revalidate()?;
        bwrap.revalidate()?;
        git.revalidate()?;
        self.launch_cwd.revalidate(false)?;
        self.workspace.revalidate(false)?;
        self.artifact_root.revalidate(true)?;
        self.isolated_home.revalidate(true)?;
        self.codex_home.revalidate(true)?;
        self.temp_dir.revalidate(true)?;
        self.runtime_dir.revalidate(true)?;
        self.host_runtime_dir.revalidate(true)?;
        super::validation::validate_opaque_id("Codex worker run id", &self.run_id)?;
        self.auth_source.revalidate()?;
        self.auth_target.revalidate()?;
        self.host_root.revalidate()?;
        self.git_workspace.revalidate(self.workspace.path(), git)
    }

    fn exact_environment(&self) -> Result<Vec<ExactEnvironmentRequirement>> {
        let mut exact = vec![
            ExactEnvironmentRequirement::new(
                "HOME",
                path_string("isolated HOME", self.isolated_home.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "CODEX_HOME",
                path_string("isolated CODEX_HOME", self.codex_home.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "PYTHONPYCACHEPREFIX",
                path_string(
                    "worker Python bytecode cache",
                    &self.temp_dir.path().join("python-pycache"),
                )?,
            )?,
            ExactEnvironmentRequirement::new(
                "TMPDIR",
                path_string("worker temp", self.temp_dir.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "XDG_RUNTIME_DIR",
                path_string("worker runtime", self.runtime_dir.path())?,
            )?,
        ];
        exact.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(exact)
    }

    fn outer_argv(&self, artifact_dir: &Path, native: &ExecutableIdentity) -> Result<Vec<String>> {
        if !artifact_dir.starts_with(self.artifact_root.path()) {
            bail!("Codex artifact attempt escaped its pinned artifact root");
        }
        let auth_target = self.codex_home.path().join(CODEX_AUTH_FILE);
        let argv = self.host_root.host_root_argv(HostRootMounts {
            isolated_home: self.isolated_home.path(),
            native_config: self.codex_home.path(),
            launch_cwd: self.launch_cwd.path(),
            artifact_dir,
            auth_source: self.auth_source.path(),
            auth_target: &auth_target,
            readable_roots: &[self.launch_cwd.path()],
            writable_roots: &[self.workspace.path()],
            read_only_files: &[&native.canonical_path],
            extra_writable_dirs: &[self.temp_dir.path(), self.runtime_dir.path()],
            host_root_access: HostRootAccess::Hidden,
            masked_dirs: &[],
        })?;
        if argv
            .iter()
            .any(|argument| argument == "--" || argument.contains("control.sock"))
        {
            bail!("Codex outer sandbox manifest contains forbidden launch authority");
        }
        Ok(argv)
    }

    fn validate_completed_result(
        &self,
        control: &TurnControl,
        result: &DeveloperResult,
        git: &ExecutableIdentity,
    ) -> Result<()> {
        revalidate_exact_tool(git, GIT_VERSION)?;
        self.git_workspace.revalidate(self.workspace.path(), git)?;
        let runner = GitRunner {
            executable: git,
            workspace: self.workspace.path(),
        };
        let status =
            runner.success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?;
        if !status.is_empty() {
            bail!("completed Codex developer worktree is not clean");
        }
        if !runner
            .success(&["for-each-ref", "--format=%(refname)", "refs/replace/"])?
            .is_empty()
        {
            bail!("completed Codex developer repository contains replacement refs");
        }
        let head = runner.one_line(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        validate_git_oid("actual developer HEAD", &head)?;
        if result.head_revision.as_deref() != Some(head.as_str()) {
            bail!("Codex developer result HEAD does not match the worktree");
        }
        let ancestor =
            runner.run(&["merge-base", "--is-ancestor", &control.base_revision, &head])?;
        if ancestor.status.code() != Some(0) || !ancestor.stderr.is_empty() {
            bail!("task base revision is not an ancestor of the completed developer HEAD");
        }

        let range = format!("{}..{head}", control.base_revision);
        let commit_output = runner.success(&[
            "log",
            "-z",
            "--reverse",
            "--topo-order",
            "--max-count=257",
            "--no-show-signature",
            "--format=%H%x00%s",
            &range,
            "--",
        ])?;
        let commits = parse_git_commits(&commit_output)?;
        if commits != result.commits {
            bail!("Codex developer reported commits do not match the exact Git range");
        }

        let changed_output = runner.success(&[
            "diff",
            "--name-only",
            "-z",
            "--no-ext-diff",
            "--no-textconv",
            &range,
            "--",
        ])?;
        let mut changed_paths = parse_nul_paths(&changed_output)?;
        let mut reported_paths = result.changed_paths.clone();
        changed_paths.sort();
        reported_paths.sort();
        if changed_paths != reported_paths {
            bail!("Codex developer changed paths do not match the exact Git range");
        }
        revalidate_exact_tool(git, GIT_VERSION)?;
        self.git_workspace.revalidate(self.workspace.path(), git)?;
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
            || runner.one_line(&["rev-parse", "--verify", "HEAD^{commit}"])? != head
        {
            bail!("Codex developer Git state drifted during its completed-result gate");
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
            .with_context(|| format!("failed to inspect directory {}", path.display()))?;
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!("Codex sandbox directory must be a real directory");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("Codex sandbox directory must already be canonical");
        }
        let metadata = fs::metadata(path)?;
        let identity = Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
        };
        identity
            .validate_metadata(private)
            .with_context(|| format!("unsafe Codex sandbox directory {}", path.display()))?;
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self, private: bool) -> Result<()> {
        if Self::capture(&self.path, private)? != *self {
            bail!("Codex sandbox directory identity drifted");
        }
        Ok(())
    }

    fn validate_metadata(&self, private: bool) -> Result<()> {
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if self.uid != uid {
            bail!("Codex sandbox directory is not owned by the current user");
        }
        if private && (self.mode & 0o077 != 0 || self.mode & 0o700 != 0o700) {
            bail!("Codex private directory must be mode 0700");
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
        let link = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect private file {}", path.display()))?;
        if link.file_type().is_symlink() || !link.is_file() {
            bail!("Codex private file must be a regular non-symlink file");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("Codex private file must already be canonical");
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
            bail!("Codex private file has unsafe ownership, links, or permissions");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self) -> Result<()> {
        if Self::capture(&self.path)? != *self {
            bail!("Codex private file identity drifted");
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
            bail!("Codex developer workspace is not the exact Git top level");
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
            bail!(
                "Codex developer Git administrative and object paths must stay inside the writable worktree"
            );
        }
        let identity = Self {
            top_level,
            git_dir: DirectoryIdentity::capture(&git_dir, false)?,
            common_dir: DirectoryIdentity::capture(&common_dir, false)?,
            object_dir: DirectoryIdentity::capture(&object_dir, false)?,
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
            bail!("Codex developer Git workspace identity drifted");
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
                Ok(_) => bail!("Codex developer Git repository uses forbidden object indirection"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .context("failed to inspect Codex developer Git object indirection");
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
            bail!("bounded host Git evidence command failed");
        }
        Ok(output.stdout)
    }

    fn one_line(&self, args: &[&str]) -> Result<String> {
        let output = self.success(args)?;
        let text = std::str::from_utf8(&output).context("Git evidence is not UTF-8")?;
        let text = text.strip_suffix('\n').unwrap_or(text);
        if text.is_empty() || text.contains('\n') || text.contains('\r') {
            bail!("Git evidence did not contain exactly one bounded line");
        }
        Ok(text.to_owned())
    }
}

struct BoundedCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_bounded_command(mut command: Command, cap: usize) -> Result<BoundedCommandOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(test)]
    let child = super::spawn_test_command_with_etxtbsy_retry(&mut command);
    #[cfg(not(test))]
    let child = command.spawn();
    let mut child = child.context("failed to spawn bounded helper")?;
    let Some(stdout) = child.stdout.take() else {
        terminate_and_reap(&mut child);
        bail!("bounded helper stdout pipe is unavailable");
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_and_reap(&mut child);
        bail!("bounded helper stderr pipe is unavailable");
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
                return Err(error).context("failed to inspect bounded helper");
            }
        }
        if started.elapsed() >= MAX_TOOL_DURATION {
            timed_out = true;
            let _ = child.kill();
            break child
                .wait()
                .context("failed to reap timed-out bounded helper")?;
        }
        thread::sleep(Duration::from_millis(2));
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("bounded stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("bounded stderr reader panicked"))??;
    if overflow.load(Ordering::Acquire) {
        bail!("bounded helper output exceeded its hard cap");
    }
    if timed_out {
        bail!("bounded helper exceeded its hard deadline");
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

fn capture_exact_tool(path: &Path, expected: &str) -> Result<ExecutableIdentity> {
    let before = ExecutableIdentity::capture(path)?;
    let mut command = Command::new(path);
    command.arg("--version").env_clear();
    let output = run_bounded_command(command, 4096)?;
    let mut expected_output = expected.as_bytes().to_vec();
    expected_output.push(b'\n');
    if !output.status.success() || !output.stderr.is_empty() || output.stdout != expected_output {
        bail!("session worker tool version does not match its exact enabled contract");
    }
    let after = ExecutableIdentity::capture(path)?;
    if before != after {
        bail!("session worker tool identity changed during version validation");
    }
    Ok(after)
}

fn revalidate_exact_tool(identity: &ExecutableIdentity, expected: &str) -> Result<()> {
    let current = capture_exact_tool(&identity.canonical_path, expected)?;
    if current != *identity {
        bail!("session Codex tool identity or version drifted");
    }
    Ok(())
}

pub(super) fn developer_result_schema() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "decision", "summary", "head_revision", "commits", "checks",
            "questions", "risks", "changed_paths"
        ],
        "properties": {
            "decision": {"enum": ["completed", "needs_input", "blocked"]},
            "summary": {"type": "string"},
            "head_revision": {
                "type": ["string", "null"],
                "description": "Exact full committed HEAD revision after this turn"
            },
            "commits": {
                "type": "array",
                "description": "Every commit in chronological base_revision..HEAD order for the whole approved task, including commits from earlier resumed turns; never only the current-turn delta",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["sha", "subject"],
                    "properties": {
                        "sha": {"type": "string"},
                        "subject": {"type": "string"}
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
                        "command": {
                            "type": "string",
                            "description": "Exact command string from this turn's command_execution event"
                        },
                        "status": {"enum": ["passed", "failed", "not_run"]},
                        "summary": {"type": "string"}
                    }
                }
            },
            "questions": {"type": "array", "items": {"type": "string"}},
            "risks": {"type": "array", "items": {"type": "string"}},
            "changed_paths": {
                "type": "array",
                "description": "Complete union of paths changed anywhere in base_revision..HEAD for the whole approved task, including paths changed in earlier resumed turns",
                "items": {"type": "string"}
            }
        }
    }))
    .expect("static Codex developer result schema is valid JSON")
}

pub(super) struct CodexTurnEvidence {
    pub(super) native_session_id: String,
    pub(super) completed_commands: BTreeSet<String>,
    pub(super) failed_commands: BTreeSet<String>,
}

#[derive(Deserialize)]
struct EventHeader {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct ThreadStartedEvent {
    #[serde(rename = "type")]
    _kind: String,
    thread_id: String,
}

#[derive(Deserialize)]
struct ItemEvent<'a> {
    #[serde(rename = "type")]
    _kind: String,
    #[serde(borrow)]
    item: CodexItem<'a>,
}

#[derive(Deserialize)]
struct CodexItem<'a> {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    command: Option<String>,
    #[serde(borrow)]
    aggregated_output: Option<&'a serde_json::value::RawValue>,
    exit_code: Option<i32>,
    status: Option<String>,
}

fn large_command_output_is_only_oversized_value(line: &str, item: &CodexItem<'_>) -> bool {
    let Some(output) = item.aggregated_output else {
        return false;
    };
    let raw = output.get();
    let trimmed = raw.trim();
    trimmed.starts_with('"')
        && trimmed.ends_with('"')
        && line.len().saturating_sub(raw.len()) <= MAX_CODEX_EVENT_BYTES
}

pub(super) fn parse_codex_turn(control: &TurnControl, stdout: &[u8]) -> Result<CodexTurnEvidence> {
    control.validate()?;
    if stdout.is_empty() {
        bail!("Codex JSONL does not match a session worker turn");
    }
    if stdout.len() > MAX_CODEX_JSONL_BYTES {
        bail!("Codex JSONL aggregate output exceeds its hard bound");
    }
    let text = std::str::from_utf8(stdout).context("Codex JSONL is not UTF-8")?;
    validate_text("Codex JSONL", text, MAX_CODEX_JSONL_BYTES, true)?;
    let mut session = None;
    let mut turn_started = false;
    let mut turn_completed = false;
    let mut event_count = 0usize;
    let mut completed_commands = BTreeSet::new();
    let mut failed_commands = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        event_count += 1;
        if event_count > MAX_CODEX_EVENTS {
            bail!("Codex JSONL event count exceeds its hard bound");
        }
        if turn_completed {
            bail!("Codex JSONL contains events after its terminal event");
        }
        let header: EventHeader =
            serde_json::from_str(line).context("Codex JSONL event header is malformed")?;
        validate_text("Codex event type", &header.kind, 128, false)?;
        let oversized_event = line.len() > MAX_CODEX_EVENT_BYTES;
        // Codex 0.145 places a command's aggregate output in the same
        // item.completed object as the bounded command/status evidence. Borrow
        // its raw range so only that known string may account for bytes above
        // the event bound; the aggregate stream cap still bounds parsing.
        if oversized_event && header.kind != "item.completed" {
            bail!("Codex JSONL event exceeds its per-event shape bound");
        }
        match header.kind.as_str() {
            "thread.started" => {
                if event_count != 1 || session.is_some() || turn_started {
                    bail!("Codex JSONL must start with exactly one thread.started event");
                }
                let event: ThreadStartedEvent = serde_json::from_str(line)
                    .context("Codex thread.started event is malformed")?;
                validate_native_session_id(&event.thread_id)?;
                if control
                    .native_session_id
                    .as_deref()
                    .is_some_and(|expected| expected != event.thread_id)
                {
                    bail!("Codex resume returned a different native session");
                }
                session = Some(event.thread_id);
            }
            "turn.started" => {
                if session.is_none() || turn_started {
                    bail!("Codex JSONL contains an invalid turn.started transition");
                }
                turn_started = true;
            }
            "turn.completed" => {
                if !turn_started {
                    bail!("Codex JSONL completed before its turn started");
                }
                turn_completed = true;
            }
            "turn.failed" | "error" => bail!("Codex JSONL reported a failed turn"),
            "item.started" | "item.completed" => {
                if !turn_started {
                    bail!("Codex JSONL item arrived outside a started turn");
                }
                let event: ItemEvent =
                    serde_json::from_str(line).context("Codex item event is malformed")?;
                validate_text("Codex item id", &event.item.id, 256, false)?;
                validate_text("Codex item type", &event.item.kind, 128, false)?;
                if oversized_event
                    && (header.kind != "item.completed"
                        || event.item.kind != "command_execution"
                        || !large_command_output_is_only_oversized_value(line, &event.item))
                {
                    bail!("Codex JSONL event exceeds its per-event shape bound");
                }
                if matches!(
                    event.item.kind.as_str(),
                    "mcp_tool_call" | "collab_tool_call"
                ) {
                    bail!("Codex worker emitted forbidden delegated activity");
                }
                if let Some(status) = &event.item.status {
                    validate_text("Codex item status", status, 128, false)?;
                }
                if header.kind == "item.completed" && event.item.kind == "command_execution" {
                    let command = event
                        .item
                        .command
                        .ok_or_else(|| anyhow!("Codex command event omitted its command"))?;
                    // Codex 0.145 preserves a shell script's embedded newlines
                    // in the JSONL display command for `/bin/bash -lc`. Keep the
                    // bounded, no-escape/no-CR terminal-safety checks while
                    // admitting that native multiline event shape.
                    validate_text("Codex command event", &command, 4096, true)?;
                    let exit_code = event
                        .item
                        .exit_code
                        .ok_or_else(|| anyhow!("Codex command event omitted its exit code"))?;
                    match (exit_code, event.item.status.as_deref()) {
                        (0, Some("completed")) => {
                            for evidence in exact_command_evidence(&command) {
                                failed_commands.remove(&evidence);
                                completed_commands.insert(evidence);
                            }
                        }
                        (code, Some("failed")) if code != 0 => {
                            for evidence in exact_command_evidence(&command) {
                                completed_commands.remove(&evidence);
                                failed_commands.insert(evidence);
                            }
                        }
                        (0, _) => {
                            bail!("successful Codex command event has an invalid status");
                        }
                        _ => {
                            bail!("failed Codex command event has an invalid status");
                        }
                    }
                }
            }
            kind if kind.contains("failed") || kind.contains("error") => {
                bail!("Codex JSONL reported an error-shaped event")
            }
            _ => {}
        }
    }
    if !turn_started || !turn_completed {
        bail!("Codex JSONL omitted its successful terminal transition");
    }
    Ok(CodexTurnEvidence {
        native_session_id: session
            .ok_or_else(|| anyhow!("Codex JSONL omitted its native session"))?,
        completed_commands,
        failed_commands,
    })
}

fn exact_command_evidence(command: &str) -> BTreeSet<String> {
    let mut evidence = BTreeSet::from([command.to_owned()]);
    if let Ok(argv) = shell_words::split(command)
        && let [shell, flag, payload] = argv.as_slice()
        && shell == "/bin/bash"
        && matches!(flag.as_str(), "-c" | "-lc")
        && !payload.is_empty()
        && validate_text("Codex bash -lc payload", payload, 4096, true).is_ok()
    {
        // Codex 0.145 reports shell tool executions as `/bin/bash -c
        // '<payload>'` or `/bin/bash -lc '<payload>'`, depending on the tool
        // path. The approved check is the exact payload, so expose that one
        // payload, including its exact embedded newlines, as evidence too. No
        // prefix, substring, or multi-command normalization is accepted.
        evidence.insert(payload.clone());
    }
    evidence
}

pub(super) fn observe_codex_record(record: &[u8]) -> Result<Vec<NativeObservation>> {
    if record.is_empty() || record.len() > 128 * 1024 {
        bail!("Codex native record exceeds its bound");
    }
    let text = std::str::from_utf8(record).context("Codex native record is not UTF-8")?;
    validate_text("Codex native record", text.trim_end(), 128 * 1024, false)?;
    let header: EventHeader =
        serde_json::from_slice(record).context("Codex native record is malformed")?;
    validate_text("Codex event type", &header.kind, 128, false)?;
    Ok(match header.kind.as_str() {
        "thread.started" => {
            let event: ThreadStartedEvent = serde_json::from_slice(record)
                .context("Codex thread.started record is malformed")?;
            validate_native_session_id(&event.thread_id)?;
            vec![NativeObservation::SessionStarted {
                native_session_id: event.thread_id,
            }]
        }
        "turn.started" => vec![NativeObservation::Activity {
            kind: "turn".into(),
            message: "started".into(),
        }],
        "turn.completed" => vec![NativeObservation::Activity {
            kind: "turn".into(),
            message: "completed".into(),
        }],
        "turn.failed" | "error" => bail!("Codex native record reported a failed turn"),
        "item.started" | "item.completed" => {
            let event: ItemEvent =
                serde_json::from_slice(record).context("Codex item record is malformed")?;
            validate_text("Codex item id", &event.item.id, 256, false)?;
            let item = match event.item.kind.as_str() {
                "command_execution" => "command",
                "mcp_tool_call" | "collab_tool_call" => {
                    bail!("Codex native record reported forbidden delegated activity")
                }
                "agent_message" => "message",
                _ => "item",
            };
            vec![NativeObservation::Activity {
                kind: "item".into(),
                message: format!(
                    "{item} {}",
                    if header.kind == "item.started" {
                        "started"
                    } else {
                        "completed"
                    }
                ),
            }]
        }
        kind if kind.contains("failed") || kind.contains("error") => {
            bail!("Codex native record reported an error-shaped event")
        }
        _ => vec![],
    })
}

fn canonical_git_path(value: &str) -> Result<PathBuf> {
    if value.len() > MAX_PATH_BYTES {
        bail!("Git administrative path exceeds its bound");
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("Git administrative path must be absolute");
    }
    let canonical = fs::canonicalize(&path)?;
    if canonical != path {
        bail!("Git administrative path must already be canonical");
    }
    Ok(canonical)
}

pub(super) fn parse_git_commits(bytes: &[u8]) -> Result<Vec<CommitSummary>> {
    if bytes.is_empty() {
        return Ok(vec![]);
    }
    let fields = bytes
        .strip_suffix(&[0])
        .ok_or_else(|| anyhow!("Git commit evidence omitted its terminal delimiter"))?
        .split(|byte| *byte == 0)
        .collect::<Vec<_>>();
    if fields.len() % 2 != 0 || fields.len() / 2 > MAX_ITEMS {
        bail!("Git commit evidence has an invalid bounded field count");
    }
    fields
        .chunks_exact(2)
        .map(|fields| {
            let sha = std::str::from_utf8(fields[0])
                .context("Git commit ID is not UTF-8")?
                .to_owned();
            let subject = std::str::from_utf8(fields[1])
                .context("Git commit subject is not UTF-8")?
                .to_owned();
            validate_git_oid("actual developer commit", &sha)?;
            validate_text("actual developer commit subject", &subject, 512, false)?;
            Ok(CommitSummary { sha, subject })
        })
        .collect()
}

pub(super) fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for component in bytes.split(|byte| *byte == 0) {
        if component.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(component).context("Git changed path is not UTF-8")?;
        validate_relative_path("Git changed path", path)?;
        paths.push(path.to_owned());
        if paths.len() > MAX_ITEMS {
            bail!("Git changed paths exceed their bounded count");
        }
    }
    Ok(paths)
}

fn validate_production_runtime_contract(config: &CodexDeveloperConfig) -> Result<()> {
    super::validation::validate_opaque_id("Codex worker run id", &config.run_id)?;
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR is required for the Codex worker sandbox"))?;
    let canonical_runtime =
        fs::canonicalize(&runtime).context("failed to resolve host XDG_RUNTIME_DIR")?;
    if runtime != canonical_runtime || config.host_runtime_dir != canonical_runtime {
        bail!("Codex worker host runtime root does not match canonical XDG_RUNTIME_DIR");
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
    use crate::control_api::WorkerRole;
    use crate::worker::environment::{ExecutionEnvironmentLease, WorkerEnvironmentIdentity};
    use crate::worker::{
        HeartbeatControl, NativeSessionBinding, ProcessRunner, WorkerAdapter, prepare_create_turn,
        prepare_resume_turn,
    };
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::{UnixListener, UnixStream};

    struct Fixture {
        _temp: tempfile::TempDir,
        config: CodexDeveloperConfig,
        codex: PathBuf,
        bwrap: PathBuf,
        git: PathBuf,
        base_revision: String,
        global_sentinel: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let workspace = temp.path().join("workspace");
            let artifact_root = temp.path().join("artifacts");
            let isolated_home = temp.path().join("isolated-home");
            let codex_home = isolated_home.join("codex");
            let temp_dir = temp.path().join("isolated-tmp");
            let runtime_dir = temp.path().join("isolated-runtime");
            let host_runtime_dir = temp.path().join("host-runtime");
            let cargo_bin_source = temp.path().join("cargo-bin");
            let rustup_home_source = temp.path().join("rustup-home");
            let tools = temp.path().join("tools");
            for directory in [
                &workspace,
                &artifact_root,
                &isolated_home,
                &codex_home,
                &temp_dir,
                &runtime_dir,
                &host_runtime_dir,
                &cargo_bin_source,
                &rustup_home_source,
                &tools,
            ] {
                fs::create_dir(directory).unwrap();
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
            }

            let auth_source = temp.path().join("native-auth.json");
            fs::write(&auth_source, b"private-auth-sentinel").unwrap();
            fs::set_permissions(&auth_source, fs::Permissions::from_mode(0o600)).unwrap();
            let auth_target = codex_home.join(CODEX_AUTH_FILE);
            fs::write(&auth_target, b"isolated-placeholder").unwrap();
            fs::set_permissions(&auth_target, fs::Permissions::from_mode(0o600)).unwrap();
            let global_sentinel = temp.path().join("global-config-sentinel.toml");
            fs::write(&global_sentinel, b"global-config-must-not-change").unwrap();
            fs::set_permissions(&global_sentinel, fs::Permissions::from_mode(0o600)).unwrap();

            let codex = tools.join("codex");
            write_executable(&codex, fake_codex_script());
            let bwrap = tools.join("bwrap");
            write_executable(&bwrap, fake_bwrap_script());
            let git = fs::canonicalize(GIT_EXECUTABLE).unwrap();

            git_ok(
                &git,
                &workspace,
                &["init", "--quiet", "--initial-branch=master"],
            );
            git_ok(&git, &workspace, &["config", "user.name", "Phase Five"]);
            git_ok(
                &git,
                &workspace,
                &["config", "user.email", "phase5@example.invalid"],
            );
            fs::write(workspace.join("base.txt"), b"base\n").unwrap();
            git_ok(&git, &workspace, &["add", "base.txt"]);
            git_ok(
                &git,
                &workspace,
                &["commit", "--quiet", "-m", "Base commit"],
            );
            let base_revision = git_line(&git, &workspace, &["rev-parse", "HEAD"]);
            fs::set_permissions(workspace.join(".git"), fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(
                workspace.join(".git/objects"),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();

            Self {
                _temp: temp,
                config: CodexDeveloperConfig {
                    run_id: "run-codex-fixture".into(),
                    launch_cwd: fs::canonicalize(&workspace).unwrap(),
                    workspace_cwd: fs::canonicalize(&workspace).unwrap(),
                    artifact_root: fs::canonicalize(artifact_root).unwrap(),
                    isolated_home: fs::canonicalize(isolated_home).unwrap(),
                    codex_home: fs::canonicalize(codex_home).unwrap(),
                    temp_dir: fs::canonicalize(temp_dir).unwrap(),
                    runtime_dir: fs::canonicalize(runtime_dir).unwrap(),
                    host_runtime_dir: fs::canonicalize(host_runtime_dir).unwrap(),
                    auth_source: fs::canonicalize(auth_source).unwrap(),
                    cargo_bin_source: fs::canonicalize(cargo_bin_source).unwrap(),
                    rustup_home_source: fs::canonicalize(rustup_home_source).unwrap(),
                    invocation: CodexInvocationProfile::developer_default(),
                },
                codex: fs::canonicalize(codex).unwrap(),
                bwrap: fs::canonicalize(bwrap).unwrap(),
                git,
                base_revision,
                global_sentinel,
            }
        }

        fn adapter(&self) -> CodexDeveloperAdapter {
            CodexDeveloperAdapter::discover_with_paths(
                self.config.clone(),
                &self.codex,
                &self.bwrap,
                &self.git,
            )
            .unwrap()
        }

        fn control(&self, native_session_id: Option<&str>) -> TurnControl {
            let mut control = control(native_session_id);
            control.base_revision = self.base_revision.clone();
            control
        }

        fn lease(&self, epoch: &str) -> ExecutionEnvironmentLease {
            ExecutionEnvironmentLease::capture(
                "lease-codex",
                epoch,
                &CodexDeveloperAdapter::environment_policy().unwrap(),
                vec![
                    (
                        "CARGO_HOME".into(),
                        self.config
                            .cargo_bin_source
                            .parent()
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    (
                        "CODEX_HOME".into(),
                        self.config.codex_home.to_string_lossy().into_owned(),
                    ),
                    (
                        "HOME".into(),
                        self.config.isolated_home.to_string_lossy().into_owned(),
                    ),
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    (
                        "PYTHONPYCACHEPREFIX".into(),
                        self.config
                            .temp_dir
                            .join("python-pycache")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    (
                        "RUSTUP_HOME".into(),
                        self.config
                            .rustup_home_source
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    (
                        "TMPDIR".into(),
                        self.config.temp_dir.to_string_lossy().into_owned(),
                    ),
                    (
                        "XDG_RUNTIME_DIR".into(),
                        self.config.runtime_dir.to_string_lossy().into_owned(),
                    ),
                ],
            )
            .unwrap()
        }

        fn run(
            &self,
            adapter: &CodexDeveloperAdapter,
            control: &TurnControl,
            prompt: &[u8],
        ) -> crate::worker::ProcessCompletion {
            let profile = adapter.profile();
            let prepared = match &control.native_session_id {
                Some(session) => {
                    prepare_resume_turn(adapter, &profile, control, session, prompt.to_vec())
                        .unwrap()
                }
                None => prepare_create_turn(adapter, &profile, control, prompt.to_vec()).unwrap(),
            };
            let root = ArtifactRoot::open(&self.config.artifact_root).unwrap();
            let lease = self.lease("epoch-codex");
            let attempt = ArtifactAttempt::create(
                &root,
                ArtifactScope {
                    run_id: control.run_id.clone(),
                    task_id: control.task_id.clone(),
                    role: WorkerRole::Developer,
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
                    "epoch-codex",
                    &WorkerEnvironmentIdentity {
                        role: WorkerRole::Developer,
                        run_id: control.run_id.clone(),
                        task_id: control.task_id.clone(),
                    },
                )
                .unwrap();
            ProcessRunner::new(Duration::from_millis(10), Duration::from_millis(50))
                .unwrap()
                .spawn(WorkerRole::Developer, prepared, &environment, attempt)
                .unwrap()
                .wait(|_| Ok(HeartbeatControl::Continue))
                .unwrap()
        }

        fn commit_change(&self) -> String {
            fs::write(
                self.config.workspace_cwd.join("implemented.txt"),
                b"implemented\n",
            )
            .unwrap();
            git_ok(
                &self.git,
                &self.config.workspace_cwd,
                &["add", "implemented.txt"],
            );
            git_ok(
                &self.git,
                &self.config.workspace_cwd,
                &["commit", "--quiet", "-m", "Implement exact task"],
            );
            git_line(
                &self.git,
                &self.config.workspace_cwd,
                &["rev-parse", "HEAD"],
            )
        }
    }

    fn control(native_session_id: Option<&str>) -> TurnControl {
        TurnControl {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            role: WorkerRole::Developer,
            logical_session_id: "logical-1".into(),
            native_session_id: native_session_id.map(str::to_owned),
            turn_sequence: if native_session_id.is_some() { 2 } else { 1 },
            attempt: 1,
            task_version: 1,
            review_round: 0,
            base_revision: "a".repeat(40),
            head_revision: None,
            artifact_dir: format!(
                "run-1/task-1/developer/logical-1/turn-{}/attempt-1",
                if native_session_id.is_some() { 2 } else { 1 }
            ),
        }
    }

    fn codex_transcript(session: &str, event: &serde_json::Value) -> Vec<u8> {
        let mut stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{session}\"}}\n\
             {{\"type\":\"turn.started\"}}\n"
        )
        .into_bytes();
        serde_json::to_writer(&mut stdout, event).unwrap();
        stdout.extend_from_slice(b"\n{\"type\":\"turn.completed\"}\n");
        stdout
    }

    fn codex_command_transcript(session: &str, command: &str, output: &str) -> Vec<u8> {
        codex_transcript(
            session,
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "command-1",
                    "type": "command_execution",
                    "command": command,
                    "aggregated_output": output,
                    "exit_code": 0,
                    "status": "completed"
                }
            }),
        )
    }

    fn codex_parse_error(control: &TurnControl, stdout: &[u8]) -> anyhow::Error {
        match parse_codex_turn(control, stdout) {
            Ok(_) => panic!("Codex JSONL was unexpectedly accepted"),
            Err(error) => error,
        }
    }

    fn assert_closed_codex_worker_config_argv(argv: &[String]) {
        for option in [
            "--json",
            "--strict-config",
            "--skip-git-repo-check",
            "--ignore-user-config",
            "--ignore-rules",
            "--output-schema",
            "--output-last-message",
        ] {
            assert!(
                argv.iter().any(|argument| argument == option),
                "missing closed Codex worker option {option}"
            );
        }
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--config", "mcp_servers={}"])
        );
        for feature in DISABLED_CODEX_FEATURES {
            assert!(
                argv.windows(2).any(|pair| pair == ["--disable", *feature]),
                "missing closed Codex feature {feature}"
            );
        }
    }

    #[test]
    fn jsonl_requires_one_exact_session_and_successful_terminal_event() {
        let stdout = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"turn.completed\"}\n"
        );
        let evidence = parse_codex_turn(&control(None), stdout.as_bytes()).unwrap();
        assert_eq!(evidence.native_session_id, "native-codex-1");

        let resume = parse_codex_turn(&control(Some("native-codex-1")), stdout.as_bytes()).unwrap();
        assert_eq!(resume.native_session_id, "native-codex-1");

        for invalid in [
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n",
                "{\"type\":\"turn.completed\"}\n"
            ),
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n",
                "{\"type\":\"turn.failed\"}\n"
            ),
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n"
            ),
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n",
                "{\"type\":\"turn.completed\"}\n",
                "{\"type\":\"item.completed\",\"item\":{\"id\":\"late\",\"type\":\"agent_message\"}}\n"
            ),
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n",
                "{\"type\":\"item.started\",\"item\":",
                "{\"id\":\"mcp\",\"type\":\"mcp_tool_call\"}}\n",
                "{\"type\":\"turn.completed\"}\n"
            ),
            concat!(
                "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
                "{\"type\":\"turn.started\"}\n",
                "{\"type\":\"item.completed\",\"item\":",
                "{\"id\":\"delegate\",\"type\":\"collab_tool_call\"}}\n",
                "{\"type\":\"turn.completed\"}\n"
            ),
        ] {
            assert!(parse_codex_turn(&control(None), invalid.as_bytes()).is_err());
        }
        assert!(parse_codex_turn(&control(Some("native-other")), stdout.as_bytes()).is_err());
    }

    #[test]
    fn jsonl_accepts_large_ignored_command_output_for_create_and_exact_resume() {
        let command = "cargo test --quiet --all-targets";
        let ignored_output = "x".repeat(MAX_CODEX_EVENT_BYTES + 64 * 1024);
        let stdout = codex_command_transcript("native-codex-large", command, &ignored_output);
        let command_event_len = stdout.split(|byte| *byte == b'\n').nth(2).unwrap().len();
        assert!(command_event_len > MAX_CODEX_EVENT_BYTES);
        assert!(stdout.len() < MAX_CODEX_JSONL_BYTES);

        for turn in [control(None), control(Some("native-codex-large"))] {
            let evidence = parse_codex_turn(&turn, &stdout).unwrap();
            assert_eq!(evidence.native_session_id, "native-codex-large");
            assert_eq!(
                evidence.completed_commands,
                BTreeSet::from([command.to_owned()])
            );
            assert!(evidence.failed_commands.is_empty());
        }
    }

    #[test]
    fn jsonl_reports_sanitized_distinct_aggregate_count_and_event_shape_bounds() {
        let aggregate = codex_command_transcript(
            "native-codex-aggregate",
            "cargo test --quiet",
            &"a".repeat(MAX_CODEX_JSONL_BYTES),
        );
        assert!(aggregate.len() > MAX_CODEX_JSONL_BYTES);
        let error = codex_parse_error(&control(None), &aggregate);
        assert_eq!(
            error.to_string(),
            "Codex JSONL aggregate output exceeds its hard bound"
        );

        let mut too_many = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-count\"}\n",
            "{\"type\":\"turn.started\"}\n"
        )
        .to_owned();
        for _ in 2..MAX_CODEX_EVENTS {
            too_many.push_str("{\"type\":\"heartbeat\"}\n");
        }
        let private_payload = "count-bound-private-payload";
        too_many.push_str(&format!(
            "{{\"type\":\"heartbeat\",\"payload\":\"{private_payload}\"}}\n"
        ));
        let error = codex_parse_error(&control(None), too_many.as_bytes());
        assert_eq!(
            error.to_string(),
            "Codex JSONL event count exceeds its hard bound"
        );
        assert!(!format!("{error:#}").contains(private_payload));

        let private_payload = "event-bound-private-payload";
        let oversized_non_command = codex_transcript(
            "native-codex-event",
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "message-1",
                    "type": "agent_message",
                    "text": format!(
                        "{private_payload}{}",
                        "b".repeat(MAX_CODEX_EVENT_BYTES)
                    )
                }
            }),
        );
        assert!(
            oversized_non_command
                .split(|byte| *byte == b'\n')
                .nth(2)
                .unwrap()
                .len()
                > MAX_CODEX_EVENT_BYTES
        );
        let error = codex_parse_error(&control(None), &oversized_non_command);
        assert_eq!(
            error.to_string(),
            "Codex JSONL event exceeds its per-event shape bound"
        );
        assert!(!format!("{error:#}").contains(private_payload));

        let private_payload = "unknown-field-private-payload";
        let oversized_unknown_command = codex_transcript(
            "native-codex-unknown",
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "command-1",
                    "type": "command_execution",
                    "command": "cargo test --quiet",
                    "padding": format!(
                        "{private_payload}{}",
                        "c".repeat(MAX_CODEX_EVENT_BYTES)
                    ),
                    "exit_code": 0,
                    "status": "completed"
                }
            }),
        );
        let error = codex_parse_error(&control(None), &oversized_unknown_command);
        assert_eq!(
            error.to_string(),
            "Codex JSONL event exceeds its per-event shape bound"
        );
        assert!(!format!("{error:#}").contains(private_payload));

        let private_payload = "misattributed-field-private-payload";
        let oversized_misattributed_command = codex_transcript(
            "native-codex-misattributed",
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "command-1",
                    "type": "command_execution",
                    "command": "cargo test --quiet",
                    "aggregated_output": "short",
                    "padding": format!(
                        "{private_payload}{}",
                        "d".repeat(MAX_CODEX_EVENT_BYTES)
                    ),
                    "exit_code": 0,
                    "status": "completed"
                }
            }),
        );
        let error = codex_parse_error(&control(None), &oversized_misattributed_command);
        assert_eq!(
            error.to_string(),
            "Codex JSONL event exceeds its per-event shape bound"
        );
        assert!(!format!("{error:#}").contains(private_payload));

        let oversized_non_string_output = codex_transcript(
            "native-codex-non-string-output",
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "command-1",
                    "type": "command_execution",
                    "command": "cargo test --quiet",
                    "aggregated_output": ["e".repeat(MAX_CODEX_EVENT_BYTES)],
                    "exit_code": 0,
                    "status": "completed"
                }
            }),
        );
        let error = codex_parse_error(&control(None), &oversized_non_string_output);
        assert_eq!(
            error.to_string(),
            "Codex JSONL event exceeds its per-event shape bound"
        );
    }

    #[test]
    fn jsonl_large_event_exception_does_not_relax_semantic_field_bounds() {
        let valid_command = serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "command-1",
                "type": "command_execution",
                "command": "cargo test --quiet",
                "exit_code": 0,
                "status": "completed"
            }
        });
        let cases = [
            (
                serde_json::json!({"type": "t".repeat(129)}),
                "Codex event type",
            ),
            (
                {
                    let mut event = valid_command.clone();
                    event["item"]["id"] = serde_json::Value::String("i".repeat(257));
                    event
                },
                "Codex item id",
            ),
            (
                {
                    let mut event = valid_command.clone();
                    event["item"]["type"] = serde_json::Value::String("t".repeat(129));
                    event
                },
                "Codex item type",
            ),
            (
                {
                    let mut event = valid_command.clone();
                    event["item"]["status"] = serde_json::Value::String("s".repeat(129));
                    event
                },
                "Codex item status",
            ),
            (
                {
                    let mut event = valid_command;
                    event["item"]["command"] = serde_json::Value::String("c".repeat(4097));
                    event
                },
                "Codex command event",
            ),
        ];

        for (event, expected_label) in cases {
            let stdout = codex_transcript("native-codex-semantic", &event);
            let error = codex_parse_error(&control(None), &stdout);
            assert!(
                format!("{error:#}").contains(expected_label),
                "missing sanitized semantic label {expected_label}: {error:#}"
            );
        }
    }

    #[test]
    fn codex_0145_adjacent_quoted_bash_payload_is_exact_command_evidence() {
        let display = "/bin/bash -lc '/usr/bin/git diff --check HEAD''^ HEAD'";
        let evidence = exact_command_evidence(display);
        assert!(evidence.contains(display));
        assert!(evidence.contains("/usr/bin/git diff --check HEAD^ HEAD"));

        let plain_display = "/bin/bash -c 'python3 -m py_compile src/__init__.py src/fibonacci.py'";
        let plain_evidence = exact_command_evidence(plain_display);
        assert!(plain_evidence.contains("python3 -m py_compile src/__init__.py src/fibonacci.py"));

        let multiline_payload = "set -e\ngit status --porcelain=v1\ngit rev-parse HEAD";
        let multiline_display = format!("/bin/bash -lc {}", shell_words::quote(multiline_payload));
        let stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}}\n\
             {{\"type\":\"turn.started\"}}\n\
             {{\"type\":\"item.completed\",\"item\":{{\"id\":\"command-1\",\
             \"type\":\"command_execution\",\"command\":{},\"exit_code\":0,\
             \"status\":\"completed\"}}}}\n\
             {{\"type\":\"turn.completed\"}}\n",
            serde_json::to_string(&multiline_display).unwrap()
        );
        let parsed = parse_codex_turn(&control(None), stdout.as_bytes()).unwrap();
        assert!(parsed.completed_commands.contains(&multiline_display));
        assert!(parsed.completed_commands.contains(multiline_payload));

        let unsafe_display = "/bin/bash -lc 'printf unsafe\u{1b}'";
        let unsafe_stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}}\n\
             {{\"type\":\"turn.started\"}}\n\
             {{\"type\":\"item.completed\",\"item\":{{\"id\":\"command-2\",\
             \"type\":\"command_execution\",\"command\":{},\"exit_code\":0,\
             \"status\":\"completed\"}}}}\n\
             {{\"type\":\"turn.completed\"}}\n",
            serde_json::to_string(unsafe_display).unwrap()
        );
        assert!(parse_codex_turn(&control(None), unsafe_stdout.as_bytes()).is_err());
    }

    #[test]
    fn native_observations_never_forward_model_or_provider_text() {
        let secret = "provider-secret-must-not-leak";
        let message = format!(
            "{{\"type\":\"item.completed\",\"item\":{{\"id\":\"item-1\",\
             \"type\":\"agent_message\",\"text\":\"{secret}\"}}}}"
        );
        let observations = observe_codex_record(message.as_bytes()).unwrap();
        assert_eq!(observations.len(), 1);
        match &observations[0] {
            NativeObservation::Activity { kind, message } => {
                assert_eq!(kind, "item");
                assert_eq!(message, "message completed");
                assert!(!message.contains(secret));
            }
            NativeObservation::SessionStarted { .. } => panic!("unexpected session observation"),
        }

        let provider_error = format!("{{\"type\":\"error\",\"message\":\"{secret}\"}}");
        let error = match observe_codex_record(provider_error.as_bytes()) {
            Ok(_) => panic!("provider error was accepted"),
            Err(error) => error,
        };
        assert!(!format!("{error:#}").contains(secret));

        let mcp = br#"{"type":"item.started","item":{"id":"item-2","type":"mcp_tool_call"}}"#;
        assert!(observe_codex_record(mcp).is_err());
        let delegate =
            br#"{"type":"item.completed","item":{"id":"item-3","type":"collab_tool_call"}}"#;
        assert!(observe_codex_record(delegate).is_err());
    }

    #[test]
    fn pinned_codex_exec_help_matches_configurable_command_contract_when_installed() {
        let path = Path::new(CODEX_DEVELOPER_EXECUTABLE);
        if path.exists() {
            validate_codex_exec_cli(path).unwrap();
        }
    }

    #[test]
    fn worker_cli_help_requirements_cover_every_runtime_option() {
        fn declared_output_options(command: &CommandSpec) -> BTreeSet<String> {
            let mut options = BTreeSet::new();
            match &command.schema_transport {
                SchemaTransport::None => {}
                SchemaTransport::InlineArgument { flag, .. } => {
                    options.insert(flag.clone());
                }
                SchemaTransport::File { argument, .. } => {
                    options.insert(argument.clone());
                }
            }
            for output in &command.expected_outputs {
                if let Some(argument) = &output.output_argument {
                    options.insert(argument.clone());
                }
            }
            options
        }

        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let create = adapter.build_create(&control(None)).unwrap();
        let resume = adapter
            .build_resume("native-codex-1", &control(Some("native-codex-1")))
            .unwrap();
        for command in [&create, &resume] {
            let mut options = command
                .fixed_argv
                .iter()
                .filter(|argument| argument.starts_with("--"))
                .cloned()
                .collect::<BTreeSet<_>>();
            options.extend(declared_output_options(command));
            for option in options {
                assert!(
                    CODEX_EXEC_HELP_REQUIREMENTS.contains(&option.as_str()),
                    "runtime option {option} is absent from the exec capability probe"
                );
            }
        }

        let resume_index = resume
            .fixed_argv
            .iter()
            .position(|argument| argument == "resume")
            .unwrap();
        let mut resume_options = resume.fixed_argv[resume_index + 2..]
            .iter()
            .filter(|argument| argument.starts_with("--"))
            .cloned()
            .collect::<BTreeSet<_>>();
        resume_options.extend(declared_output_options(&resume));
        for option in resume_options {
            assert!(
                CODEX_RESUME_HELP_REQUIREMENTS.contains(&option.as_str()),
                "runtime resume option {option} is absent from the resume capability probe"
            );
        }
        assert!(CODEX_EXEC_HELP_REQUIREMENTS.contains(&"resume"));
        assert!(CODEX_EXEC_HELP_REQUIREMENTS.contains(&"--add-dir"));
    }

    #[test]
    fn configured_model_and_max_reasoning_reach_exact_codex_argv() {
        let fixture = Fixture::new();
        let mut config = fixture.config.clone();
        config.invocation.model = "gpt-5.6-sol-configured".into();
        config.invocation.reasoning_effort = "max".into();
        let adapter = CodexDeveloperAdapter::discover_with_paths(
            config,
            &fixture.codex,
            &fixture.bwrap,
            &fixture.git,
        )
        .unwrap();
        let profile = adapter.profile();
        assert_eq!(profile.model, "gpt-5.6-sol-configured");
        assert_eq!(profile.reasoning, "max");
        let prepared = prepare_create_turn(
            &adapter,
            &profile,
            &fixture.control(None),
            b"typed prompt remains stdin-only".to_vec(),
        )
        .unwrap();
        let argv = prepared.command().materialized_control_argv();
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", "gpt-5.6-sol-configured"])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--config", "model_reasoning_effort=\"max\""])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--sandbox", "danger-full-access"])
        );
        assert!(
            argv.iter()
                .any(|argument| argument == "--skip-git-repo-check")
        );
        assert!(argv.iter().any(|argument| argument == "--strict-config"));
        assert_eq!(argv.last().map(String::as_str), Some("-"));
    }

    #[test]
    fn codex_stderr_accepts_only_the_pinned_recovered_safe_delete_rejection() {
        let accepted = concat!(
            "2026-07-30T12:44:08.667685Z ERROR codex_core::tools::router: ",
            "error=exec_command failed for `/bin/bash -c 'rm -rf /tmp/review.abc'`: ",
            "CreateProcess { message: \"Rejected(\\\"`/bin/bash -c 'rm -rf ",
            "/tmp/review.abc'` rejected: rm -f style commands are not permitted. ",
            "Use a safer approach\\\")\" }\n"
        );
        validate_codex_worker_stderr(accepted.as_bytes()).unwrap();
        assert!(validate_codex_worker_stderr(b"\n\t").is_ok());
        assert!(
            validate_codex_worker_stderr(accepted.replace(" ERROR ", " WARN ").as_bytes()).is_err()
        );
        assert!(
            validate_codex_worker_stderr(
                format!("{accepted}provider authentication failed\n").as_bytes()
            )
            .is_err()
        );
        assert!(
            validate_codex_worker_stderr(
                accepted
                    .replace("rm -f style commands", "network request")
                    .as_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn exact_profile_outer_envelope_and_fake_create_resume_are_closed() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let profile = adapter.profile();
        profile.validate_for(&adapter).unwrap();
        assert_eq!(profile.role, WorkerRole::Developer);
        assert_eq!(profile.model, CODEX_DEVELOPER_MODEL);
        assert_eq!(profile.reasoning, CODEX_DEVELOPER_REASONING);
        assert_eq!(
            profile.policy,
            CodexInvocationProfile::developer_default().effective_policy(OUTER_POLICY)
        );
        assert_eq!(profile.adapter_contract_version, ADAPTER_CONTRACT_VERSION);
        assert_eq!(profile.native_session_mode, NativeSessionMode::Discovered);
        assert!(
            profile
                .capability
                .features
                .iter()
                .any(|feature| feature == CODEX_JSONL_EVENT_BOUND_CAPABILITY)
        );

        let prompt = b"private task body sentinel phase-five";
        let create_control = fixture.control(None);
        let create =
            prepare_create_turn(&adapter, &profile, &create_control, prompt.to_vec()).unwrap();
        let argv = create.command().materialized_control_argv();
        assert_closed_codex_worker_config_argv(&argv);
        assert_eq!(argv.last().map(String::as_str), Some("-"));
        assert!(argv.iter().any(|argument| argument == "--die-with-parent"));
        assert!(argv.iter().any(|argument| argument == "--unshare-pid"));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--sandbox", "danger-full-access"])
        );
        assert!(
            argv.iter()
                .any(|argument| argument == "--skip-git-repo-check")
        );
        assert!(!argv.iter().any(|argument| argument == "--new-session"));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", CODEX_DEVELOPER_MODEL])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--disable", "hooks"]));
        for feature in [
            "collaboration_modes",
            "enable_fanout",
            "multi_agent",
            "multi_agent_v2",
        ] {
            assert!(
                argv.windows(2).any(|pair| pair == ["--disable", feature]),
                "missing closed Codex delegation feature: {feature}"
            );
        }
        assert!(argv.windows(3).any(|part| {
            part == [
                "--ro-bind",
                fixture.config.launch_cwd.to_str().unwrap(),
                fixture.config.launch_cwd.to_str().unwrap(),
            ]
        }));
        assert!(argv.windows(3).any(|part| {
            part == [
                "--bind",
                fixture.config.workspace_cwd.to_str().unwrap(),
                fixture.config.workspace_cwd.to_str().unwrap(),
            ]
        }));
        assert!(
            argv.windows(2)
                .any(|part| { part == ["--chdir", fixture.config.launch_cwd.to_str().unwrap(),] })
        );
        assert!(
            !argv
                .join("\0")
                .contains("hcom-architect-session/control.sock")
        );
        assert!(!argv.iter().any(|argument| argument == "--last"));
        assert!(!argv.iter().any(|argument| argument == "--ephemeral"));
        assert!(!argv.join("\0").contains("private task body"));
        assert!(!argv.join("\0").contains("HCOM_AGENT"));
        assert!(!argv.join("\0").contains("CHAIN"));
        assert!(!argv.join("\0").contains("HANDOFF"));

        let create_completion = fixture.run(&adapter, &create_control, prompt);
        assert_eq!(create_completion.exit.code, Some(0));
        let create_native = adapter
            .extract_result(&create_control, &create_completion.artifacts)
            .unwrap();
        assert_eq!(create_native.native_session_id(), "native-codex-1");
        let mut binding =
            NativeSessionBinding::new(WorkerRole::Developer, NativeSessionMode::Discovered, None)
                .unwrap();
        binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: create_native.native_session_id().into(),
            })
            .unwrap();
        binding.seal_result(&create_native).unwrap();

        let resume_control = fixture.control(Some("native-codex-1"));
        binding.begin_resume("native-codex-1").unwrap();
        let resume = prepare_resume_turn(
            &adapter,
            &profile,
            &resume_control,
            "native-codex-1",
            prompt.to_vec(),
        )
        .unwrap();
        let resume_argv = resume.command().materialized_control_argv();
        assert_closed_codex_worker_config_argv(&resume_argv);
        assert_eq!(resume_argv.last().map(String::as_str), Some("-"));
        assert!(
            resume_argv
                .windows(2)
                .any(|pair| pair == ["resume", "native-codex-1"])
        );
        let sandbox = resume_argv
            .iter()
            .position(|argument| argument == "--sandbox")
            .unwrap();
        let resume = resume_argv
            .iter()
            .position(|argument| argument == "resume")
            .unwrap();
        assert!(sandbox < resume);
        let skip_trust = resume_argv
            .iter()
            .position(|argument| argument == "--skip-git-repo-check")
            .unwrap();
        assert!(skip_trust < resume);
        assert!(!resume_argv.iter().any(|argument| argument == "--cd"));
        assert!(
            prepare_resume_turn(
                &adapter,
                &profile,
                &resume_control,
                "native-other",
                prompt.to_vec()
            )
            .is_err()
        );
        let resume_completion = fixture.run(&adapter, &resume_control, prompt);
        let resume_native = adapter
            .extract_result(&resume_control, &resume_completion.artifacts)
            .unwrap();
        assert_eq!(resume_native.native_session_id(), "native-codex-1");
        binding
            .observe(&NativeObservation::SessionStarted {
                native_session_id: resume_native.native_session_id().into(),
            })
            .unwrap();
        binding.seal_result(&resume_native).unwrap();
        assert_eq!(
            fs::read(&fixture.global_sentinel).unwrap(),
            b"global-config-must-not-change"
        );
    }

    #[test]
    fn project_cwd_is_distinct_from_the_writable_task_repository() {
        let fixture = Fixture::new();
        let project = fixture._temp.path().join("project-context");
        fs::create_dir(&project).unwrap();
        fs::set_permissions(&project, fs::Permissions::from_mode(0o700)).unwrap();
        let project = fs::canonicalize(project).unwrap();
        let mut config = fixture.config.clone();
        config.launch_cwd = project.clone();
        config.invocation.sandbox = CodexSandbox::WorkspaceWrite;
        let adapter = CodexDeveloperAdapter::discover_with_paths(
            config,
            &fixture.codex,
            &fixture.bwrap,
            &fixture.git,
        )
        .unwrap();
        let control = fixture.control(None);
        let prepared = prepare_create_turn(
            &adapter,
            &adapter.profile(),
            &control,
            b"read project context and edit only the task repository".to_vec(),
        )
        .unwrap();
        let command = prepared.command();
        assert_eq!(command.workspace_cwd, project);
        assert!(
            command.fixed_argv.windows(2).any(|pair| {
                pair == ["--add-dir", fixture.config.workspace_cwd.to_str().unwrap()]
            })
        );
        assert!(
            command
                .fixed_argv
                .iter()
                .any(|argument| argument == "--skip-git-repo-check")
        );
        assert!(
            command
                .fixed_argv
                .windows(2)
                .any(|pair| pair == ["--cd", command.workspace_cwd.to_str().unwrap()])
        );
        let outer = &command.outer_launch.as_ref().unwrap().fixed_argv;
        assert!(
            outer
                .windows(2)
                .any(|pair| { pair == ["--chdir", command.workspace_cwd.to_str().unwrap()] })
        );
        assert!(outer.windows(3).any(|part| {
            part == [
                "--bind",
                fixture.config.workspace_cwd.to_str().unwrap(),
                fixture.config.workspace_cwd.to_str().unwrap(),
            ]
        }));
        assert!(!outer.windows(3).any(|part| part == ["--ro-bind", "/", "/"]));
        assert!(!outer.iter().any(|argument| argument == "/hcom/workspace"));

        let resume_control = fixture.control(Some("native-codex-workspace-write"));
        let resumed = prepare_resume_turn(
            &adapter,
            &adapter.profile(),
            &resume_control,
            "native-codex-workspace-write",
            b"resume in the same external task repository".to_vec(),
        )
        .unwrap();
        let resumed_argv = &resumed.command().fixed_argv;
        let add_dir = resumed_argv
            .iter()
            .position(|argument| argument == "--add-dir")
            .unwrap();
        let resume = resumed_argv
            .iter()
            .position(|argument| argument == "resume")
            .unwrap();
        assert!(add_dir < resume);
        let skip_trust = resumed_argv
            .iter()
            .position(|argument| argument == "--skip-git-repo-check")
            .unwrap();
        assert!(skip_trust < resume);
        assert_eq!(
            resumed_argv[add_dir + 1],
            fixture.config.workspace_cwd.to_string_lossy()
        );
    }

    #[test]
    fn read_only_native_sandbox_is_rejected_for_a_developer() {
        let fixture = Fixture::new();
        let mut config = fixture.config.clone();
        config.invocation.sandbox = CodexSandbox::ReadOnly;
        assert!(
            CodexDeveloperAdapter::discover_with_paths(
                config,
                &fixture.codex,
                &fixture.bwrap,
                &fixture.git,
            )
            .is_err()
        );
    }

    #[test]
    fn real_bwrap_masks_all_live_host_control_and_architect_sockets() {
        let fixture = Fixture::new();
        let control_root = fixture
            .config
            .host_runtime_dir
            .join("hcom-architect-session");
        let architect_root = control_root.join("architect");
        let launch_root = architect_root.join("launch-mask-proof");
        fs::create_dir(&control_root).unwrap();
        fs::create_dir(&architect_root).unwrap();
        fs::create_dir(&launch_root).unwrap();
        for directory in [&control_root, &architect_root, &launch_root] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let socket_paths = vec![
            control_root.join("control.sock"),
            control_root.join("registration.sock"),
            launch_root.join("relay.sock"),
        ];
        let listeners: Vec<_> = socket_paths
            .iter()
            .map(|socket_path| {
                let listener = UnixListener::bind(socket_path).unwrap();
                fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600)).unwrap();
                let outside = UnixStream::connect(socket_path).unwrap();
                drop(outside);
                listener
            })
            .collect();

        write_executable(
            &fixture.codex,
            &fake_codex_script_with_socket_probes(&socket_paths),
        );
        let real_bwrap = fs::canonicalize(BWRAP_EXECUTABLE).unwrap();
        let adapter = CodexDeveloperAdapter::discover_with_paths(
            fixture.config.clone(),
            &fixture.codex,
            &real_bwrap,
            &fixture.git,
        )
        .unwrap();
        let control = fixture.control(None);
        let completion = fixture.run(&adapter, &control, b"runtime-mask-probe");
        assert_eq!(completion.exit.code, Some(0));
        assert!(completion.exit.signal.is_none());
        adapter
            .extract_result(&control, &completion.artifacts)
            .unwrap();
        drop(listeners);
    }

    #[test]
    fn real_bwrap_preserves_project_cwd_but_only_task_repository_is_writable() {
        let fixture = Fixture::new();
        let project = fixture._temp.path().join("real-project-context");
        fs::create_dir(&project).unwrap();
        fs::set_permissions(&project, fs::Permissions::from_mode(0o700)).unwrap();
        let project = fs::canonicalize(project).unwrap();
        write_executable(
            &fixture.codex,
            &fake_codex_script_with_path_probe(
                &project,
                &fixture.config.workspace_cwd,
                &fixture.global_sentinel,
            ),
        );
        let mut config = fixture.config.clone();
        config.launch_cwd = project.clone();
        let real_bwrap = fs::canonicalize(BWRAP_EXECUTABLE).unwrap();
        let adapter = CodexDeveloperAdapter::discover_with_paths(
            config,
            &fixture.codex,
            &real_bwrap,
            &fixture.git,
        )
        .unwrap();
        let control = fixture.control(None);
        let completion = fixture.run(
            &adapter,
            &control,
            b"real path-preserving write-scope probe",
        );
        assert_eq!(completion.exit.code, Some(0));
        adapter
            .extract_result(&control, &completion.artifacts)
            .unwrap();
        assert!(!project.join("project-write-probe").exists());
        assert!(
            !fixture
                .config
                .workspace_cwd
                .join("repository-write-probe")
                .exists()
        );
    }

    #[test]
    fn real_bwrap_keeps_nested_task_repository_writable_under_read_only_project() {
        let fixture = Fixture::new();
        let project = fixture._temp.path().join("nested-project-context");
        let repository = project.join("src/task-repository");
        fs::create_dir_all(&repository).unwrap();
        fs::set_permissions(&project, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            repository.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::set_permissions(&repository, fs::Permissions::from_mode(0o700)).unwrap();
        git_ok(
            &fixture.git,
            &repository,
            &["init", "--quiet", "--initial-branch=master"],
        );
        git_ok(
            &fixture.git,
            &repository,
            &["config", "user.name", "Nested Layout"],
        );
        git_ok(
            &fixture.git,
            &repository,
            &["config", "user.email", "nested@example.invalid"],
        );
        fs::write(repository.join("base.txt"), b"nested base\n").unwrap();
        git_ok(&fixture.git, &repository, &["add", "base.txt"]);
        git_ok(
            &fixture.git,
            &repository,
            &["commit", "--quiet", "-m", "Nested base"],
        );
        fs::set_permissions(repository.join(".git"), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            repository.join(".git/objects"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let project = fs::canonicalize(project).unwrap();
        let repository = fs::canonicalize(repository).unwrap();
        write_executable(
            &fixture.codex,
            &fake_codex_script_with_path_probe(&project, &repository, &fixture.global_sentinel),
        );
        let mut config = fixture.config.clone();
        config.launch_cwd = project.clone();
        config.workspace_cwd = repository.clone();
        let real_bwrap = fs::canonicalize(BWRAP_EXECUTABLE).unwrap();
        let adapter = CodexDeveloperAdapter::discover_with_paths(
            config,
            &fixture.codex,
            &real_bwrap,
            &fixture.git,
        )
        .unwrap();
        let control = fixture.control(None);
        let completion = fixture.run(&adapter, &control, b"nested mount-order probe");
        assert_eq!(completion.exit.code, Some(0));
        adapter
            .extract_result(&control, &completion.artifacts)
            .unwrap();
        assert!(!project.join("project-write-probe").exists());
        assert!(!repository.join("repository-write-probe").exists());
    }

    #[test]
    fn completed_result_requires_exact_git_and_current_turn_check_evidence() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let head = fixture.commit_change();
        let result = DeveloperResult {
            decision: DeveloperDecision::Completed,
            summary: "implemented the exact task".into(),
            head_revision: Some(head.clone()),
            commits: vec![CommitSummary {
                sha: head,
                subject: "Implement exact task".into(),
            }],
            checks: vec![super::super::result::CheckResult {
                command: "cargo test --lib".into(),
                status: CheckStatus::Passed,
                summary: "passed".into(),
            }],
            questions: vec![],
            risks: vec![],
            changed_paths: vec!["implemented.txt".into()],
        };
        let artifacts = completed_artifacts("native-codex-1", &result, "cargo test --lib");
        adapter
            .extract_result(&fixture.control(None), &artifacts)
            .unwrap();

        let wrapped = completed_artifacts(
            "native-codex-1",
            &result,
            "/bin/bash -lc 'cargo test --lib'",
        );
        adapter
            .extract_result(&fixture.control(None), &wrapped)
            .unwrap();

        let mut shell_escaped_result = result.clone();
        shell_escaped_result.checks[0].command = "/usr/bin/git diff --check HEAD^ HEAD".into();
        let shell_escaped = completed_artifacts(
            "native-codex-1",
            &shell_escaped_result,
            "/bin/bash -lc '/usr/bin/git diff --check HEAD''^ HEAD'",
        );
        adapter
            .extract_result(&fixture.control(None), &shell_escaped)
            .unwrap();

        let recovered_after_unrelated_failure = completed_artifacts_with_commands(
            "native-codex-1",
            &result,
            &[
                ("git commit -m 'first attempt without identity'", 128),
                ("cargo test --lib", 0),
            ],
        );
        adapter
            .extract_result(&fixture.control(None), &recovered_after_unrelated_failure)
            .unwrap();

        let recovered_required_check = completed_artifacts_with_commands(
            "native-codex-1",
            &result,
            &[("cargo test --lib", 1), ("cargo test --lib", 0)],
        );
        adapter
            .extract_result(&fixture.control(None), &recovered_required_check)
            .unwrap();

        let regressed_required_check = completed_artifacts_with_commands(
            "native-codex-1",
            &result,
            &[("cargo test --lib", 0), ("cargo test --lib", 1)],
        );
        assert!(
            adapter
                .extract_result(&fixture.control(None), &regressed_required_check)
                .is_err()
        );

        let combined = completed_artifacts(
            "native-codex-1",
            &result,
            "/bin/bash -lc 'cargo test --lib && echo not-the-exact-check'",
        );
        assert!(
            adapter
                .extract_result(&fixture.control(None), &combined)
                .is_err()
        );

        let mut wrong_head = result.clone();
        wrong_head.head_revision = Some("f".repeat(40));
        wrong_head.commits[0].sha = "f".repeat(40);
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts("native-codex-1", &wrong_head, "cargo test --lib")
                )
                .is_err()
        );

        let mut fake_check = result.clone();
        fake_check.checks[0].command = "cargo test --all-targets".into();
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts("native-codex-1", &fake_check, "cargo test --lib")
                )
                .is_err()
        );

        let conflicting = completed_artifacts_with_commands(
            "native-codex-1",
            &result,
            &[("cargo test --lib", 0), ("cargo test --lib", 1)],
        );
        assert!(
            adapter
                .extract_result(&fixture.control(None), &conflicting)
                .is_err()
        );

        let mut wrong_subject = result.clone();
        wrong_subject.commits[0].subject = "Invented subject".into();
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts("native-codex-1", &wrong_subject, "cargo test --lib")
                )
                .is_err()
        );

        let mut wrong_paths = result.clone();
        wrong_paths.changed_paths = vec!["base.txt".into()];
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts("native-codex-1", &wrong_paths, "cargo test --lib")
                )
                .is_err()
        );

        fs::write(
            fixture.config.workspace_cwd.join("uncommitted.txt"),
            b"dirty\n",
        )
        .unwrap();
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts("native-codex-1", &result, "cargo test --lib")
                )
                .is_err()
        );
    }

    #[test]
    fn resumed_completed_result_requires_the_full_task_range_not_only_the_turn_delta() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let first_head = fixture.commit_change();

        fs::write(
            fixture.config.workspace_cwd.join("review-fix.txt"),
            b"review fix\n",
        )
        .unwrap();
        git_ok(
            &fixture.git,
            &fixture.config.workspace_cwd,
            &["add", "review-fix.txt"],
        );
        git_ok(
            &fixture.git,
            &fixture.config.workspace_cwd,
            &["commit", "--quiet", "-m", "Fix reviewed task"],
        );
        let final_head = git_line(
            &fixture.git,
            &fixture.config.workspace_cwd,
            &["rev-parse", "HEAD"],
        );

        let full_range = DeveloperResult {
            decision: DeveloperDecision::Completed,
            summary: "completed the reviewed task".into(),
            head_revision: Some(final_head.clone()),
            commits: vec![
                CommitSummary {
                    sha: first_head,
                    subject: "Implement exact task".into(),
                },
                CommitSummary {
                    sha: final_head.clone(),
                    subject: "Fix reviewed task".into(),
                },
            ],
            checks: vec![],
            questions: vec![],
            risks: vec![],
            changed_paths: vec!["implemented.txt".into(), "review-fix.txt".into()],
        };
        let resumed_control = fixture.control(Some("native-codex-1"));
        adapter
            .extract_result(
                &resumed_control,
                &completed_artifacts_with_commands("native-codex-1", &full_range, &[]),
            )
            .unwrap();

        let mut current_turn_only = full_range.clone();
        current_turn_only.commits.remove(0);
        current_turn_only.changed_paths.remove(0);
        assert!(
            adapter
                .extract_result(
                    &resumed_control,
                    &completed_artifacts_with_commands("native-codex-1", &current_turn_only, &[])
                )
                .is_err()
        );
    }

    #[test]
    fn developer_result_schema_explains_full_range_resume_semantics() {
        let schema: serde_json::Value = serde_json::from_slice(&developer_result_schema()).unwrap();
        assert!(
            schema["properties"]["commits"]["description"]
                .as_str()
                .unwrap()
                .contains("base_revision..HEAD")
        );
        assert!(
            schema["properties"]["commits"]["description"]
                .as_str()
                .unwrap()
                .contains("earlier resumed turns")
        );
        assert!(
            schema["properties"]["changed_paths"]["description"]
                .as_str()
                .unwrap()
                .contains("whole approved task")
        );
    }

    #[test]
    fn command_completion_status_must_match_its_exit_code() {
        let control = control(None);
        for (exit_code, status) in [(0, "failed"), (1, "completed"), (1, "in_progress")] {
            let stdout = format!(
                "{{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}}\n\
                 {{\"type\":\"turn.started\"}}\n\
                 {{\"type\":\"item.completed\",\"item\":\
                 {{\"id\":\"item-1\",\"type\":\"command_execution\",\
                 \"command\":\"cargo test --lib\",\"exit_code\":{exit_code},\
                 \"status\":\"{status}\"}}}}\n\
                 {{\"type\":\"turn.completed\"}}\n"
            );
            assert!(parse_codex_turn(&control, stdout.as_bytes()).is_err());
        }
    }

    #[test]
    fn exact_discovery_rejects_version_mismatch_and_external_git_admin_paths() {
        let version_fixture = Fixture::new();
        write_executable(
            &version_fixture.codex,
            "#!/bin/sh\nprintf '%s\\n' 'codex-cli 0.146.0'\n",
        );
        assert!(
            CodexDeveloperAdapter::discover_with_paths(
                version_fixture.config.clone(),
                &version_fixture.codex,
                &version_fixture.bwrap,
                &version_fixture.git,
            )
            .is_err()
        );

        let linked_fixture = Fixture::new();
        let source = linked_fixture._temp.path().join("shared-source");
        fs::create_dir(&source).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        git_ok(
            &linked_fixture.git,
            &source,
            &["init", "--quiet", "--initial-branch=master"],
        );
        git_ok(
            &linked_fixture.git,
            &source,
            &["config", "user.name", "Phase Five"],
        );
        git_ok(
            &linked_fixture.git,
            &source,
            &["config", "user.email", "phase5@example.invalid"],
        );
        fs::write(source.join("source.txt"), b"shared\n").unwrap();
        git_ok(&linked_fixture.git, &source, &["add", "source.txt"]);
        git_ok(
            &linked_fixture.git,
            &source,
            &["commit", "--quiet", "-m", "Shared base"],
        );
        let linked = linked_fixture._temp.path().join("linked-worktree");
        git_ok(
            &linked_fixture.git,
            &source,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                linked.to_str().unwrap(),
                "HEAD",
            ],
        );
        let mut config = linked_fixture.config.clone();
        config.workspace_cwd = fs::canonicalize(linked).unwrap();
        assert!(
            CodexDeveloperAdapter::discover_with_paths(
                config,
                &linked_fixture.codex,
                &linked_fixture.bwrap,
                &linked_fixture.git,
            )
            .is_err()
        );

        let alternate_fixture = Fixture::new();
        let external_objects = linked_fixture.config.workspace_cwd.join(".git/objects");
        fs::write(
            alternate_fixture
                .config
                .workspace_cwd
                .join(".git/objects/info/alternates"),
            format!("{}\n", external_objects.display()),
        )
        .unwrap();
        assert!(
            CodexDeveloperAdapter::discover_with_paths(
                alternate_fixture.config.clone(),
                &alternate_fixture.codex,
                &alternate_fixture.bwrap,
                &alternate_fixture.git,
            )
            .is_err()
        );

        let masked_auth_fixture = Fixture::new();
        let masked_auth = masked_auth_fixture
            .config
            .host_runtime_dir
            .join("masked-auth.json");
        fs::write(&masked_auth, b"masked-auth").unwrap();
        fs::set_permissions(&masked_auth, fs::Permissions::from_mode(0o600)).unwrap();
        let mut masked_auth_config = masked_auth_fixture.config.clone();
        masked_auth_config.auth_source = fs::canonicalize(masked_auth).unwrap();
        assert!(
            CodexDeveloperAdapter::discover_with_paths(
                masked_auth_config,
                &masked_auth_fixture.codex,
                &masked_auth_fixture.bwrap,
                &masked_auth_fixture.git,
            )
            .is_err()
        );
    }

    #[test]
    fn completed_result_rejects_git_replacement_refs() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let head = fixture.commit_change();
        git_ok(
            &fixture.git,
            &fixture.config.workspace_cwd,
            &["replace", &head, &fixture.base_revision],
        );
        let result = DeveloperResult {
            decision: DeveloperDecision::Completed,
            summary: "implemented the exact task".into(),
            head_revision: Some(head.clone()),
            commits: vec![CommitSummary {
                sha: head,
                subject: "Implement exact task".into(),
            }],
            checks: vec![],
            questions: vec![],
            risks: vec![],
            changed_paths: vec!["implemented.txt".into()],
        };
        assert!(
            adapter
                .extract_result(
                    &fixture.control(None),
                    &completed_artifacts_with_commands("native-codex-1", &result, &[])
                )
                .is_err()
        );
    }

    #[test]
    fn auth_quota_session_result_and_identity_drift_fail_closed() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let blocked = DeveloperResult {
            decision: DeveloperDecision::Blocked,
            summary: "bounded failure".into(),
            head_revision: None,
            commits: vec![],
            checks: vec![],
            questions: vec![],
            risks: vec![],
            changed_paths: vec![],
        };
        let error_stdout = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"error\",\"message\":\"authentication failed\"}\n"
        );
        let error_artifacts = NativeArtifacts::new(
            WorkerRole::Developer,
            error_stdout.as_bytes().to_vec(),
            vec![],
            Some(blocked.canonical_json().unwrap()),
        )
        .unwrap();
        assert!(
            adapter
                .extract_result(&fixture.control(None), &error_artifacts)
                .is_err()
        );

        let valid_stdout = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"native-codex-1\"}\n",
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"turn.completed\"}\n"
        );
        let malformed = NativeArtifacts::new(
            WorkerRole::Developer,
            valid_stdout.as_bytes().to_vec(),
            vec![],
            Some(br#"{"decision":"blocked","unknown":true}"#.to_vec()),
        )
        .unwrap();
        assert!(
            adapter
                .extract_result(&fixture.control(None), &malformed)
                .is_err()
        );
        let stderr = NativeArtifacts::new(
            WorkerRole::Developer,
            valid_stdout.as_bytes().to_vec(),
            b"provider warning".to_vec(),
            Some(blocked.canonical_json().unwrap()),
        )
        .unwrap();
        assert!(
            adapter
                .extract_result(&fixture.control(None), &stderr)
                .is_err()
        );

        fs::write(&fixture.codex, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&fixture.codex, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(adapter.profile().validate_for(&adapter).is_err());
        assert!(adapter.build_create(&fixture.control(None)).is_err());

        let wrapper_fixture = Fixture::new();
        let wrapper_adapter = wrapper_fixture.adapter();
        fs::write(&wrapper_fixture.bwrap, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&wrapper_fixture.bwrap, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            wrapper_adapter
                .build_create(&wrapper_fixture.control(None))
                .is_err()
        );

        let auth_fixture = Fixture::new();
        let auth_adapter = auth_fixture.adapter();
        fs::write(&auth_fixture.config.auth_source, b"changed-auth-sentinel").unwrap();
        fs::set_permissions(
            &auth_fixture.config.auth_source,
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(
            auth_adapter
                .build_create(&auth_fixture.control(None))
                .is_err()
        );
    }

    #[test]
    fn nonzero_fake_codex_exit_is_not_an_admissible_result() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let control = fixture.control(None);
        let completion = fixture.run(&adapter, &control, b"exit-nonzero");
        assert_eq!(completion.exit.code, Some(17));
        assert!(completion.exit.signal.is_none());

        let incomplete_fixture = Fixture::new();
        let incomplete_adapter = incomplete_fixture.adapter();
        let incomplete_control = incomplete_fixture.control(None);
        let incomplete = incomplete_fixture.run(
            &incomplete_adapter,
            &incomplete_control,
            b"missing-terminal",
        );
        assert_eq!(incomplete.exit.code, Some(0));
        assert!(
            incomplete_adapter
                .extract_result(&incomplete_control, &incomplete.artifacts)
                .is_err()
        );
    }

    #[test]
    fn exact_materialized_environment_is_checked_before_outer_process_spawn() {
        let fixture = Fixture::new();
        let adapter = fixture.adapter();
        let control = fixture.control(None);
        let prompt = b"bounded prompt";
        let prepared =
            prepare_create_turn(&adapter, &adapter.profile(), &control, prompt.to_vec()).unwrap();
        let root = ArtifactRoot::open(&fixture.config.artifact_root).unwrap();
        let policy = CodexDeveloperAdapter::environment_policy().unwrap();
        let lease = ExecutionEnvironmentLease::capture(
            "lease-drift",
            "epoch-drift",
            &policy,
            vec![
                ("CARGO_HOME".into(), "/wrong/cargo-home".into()),
                ("CODEX_HOME".into(), "/wrong/codex-home".into()),
                ("HOME".into(), "/wrong/isolated-home".into()),
                ("PATH".into(), "/wrong/bin".into()),
                ("PYTHONPYCACHEPREFIX".into(), "/wrong/python-cache".into()),
                ("RUSTUP_HOME".into(), "/wrong/rustup-home".into()),
                ("TMPDIR".into(), "/wrong/temp".into()),
                ("XDG_RUNTIME_DIR".into(), "/wrong/runtime".into()),
            ],
        )
        .unwrap();
        let attempt = ArtifactAttempt::create(
            &root,
            ArtifactScope {
                run_id: control.run_id.clone(),
                task_id: control.task_id.clone(),
                role: WorkerRole::Developer,
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
                "epoch-drift",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: control.run_id,
                    task_id: control.task_id,
                },
            )
            .unwrap();
        assert!(
            ProcessRunner::default()
                .spawn(WorkerRole::Developer, prepared, &environment, attempt)
                .is_err()
        );
    }

    fn completed_artifacts(
        session: &str,
        result: &DeveloperResult,
        command: &str,
    ) -> NativeArtifacts {
        completed_artifacts_with_commands(session, result, &[(command, 0)])
    }

    fn completed_artifacts_with_commands(
        session: &str,
        result: &DeveloperResult,
        commands: &[(&str, i32)],
    ) -> NativeArtifacts {
        let mut stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{session}\"}}\n\
             {{\"type\":\"turn.started\"}}\n"
        );
        for (index, (command, exit_code)) in commands.iter().enumerate() {
            let status = if *exit_code == 0 {
                "completed"
            } else {
                "failed"
            };
            stdout.push_str(&format!(
                "{{\"type\":\"item.completed\",\"item\":\
                 {{\"id\":\"item-{index}\",\"type\":\"command_execution\",\
                 \"command\":{},\"exit_code\":{exit_code},\"status\":\"{status}\"}}}}\n",
                serde_json::to_string(command).unwrap(),
            ));
        }
        stdout.push_str("{\"type\":\"turn.completed\"}\n");
        NativeArtifacts::new(
            WorkerRole::Developer,
            stdout.into_bytes(),
            vec![],
            Some(result.canonical_json().unwrap()),
        )
        .unwrap()
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
# SOCKET_REACHABILITY_PROBE
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
[ "${HCOM_WORKER_ROLE-}" = developer ]
[ -n "${HCOM_RUN_ID-}" ] && [ -n "${HCOM_TASK_ID-}" ]
[ -n "${HOME-}" ] && [ -n "${CODEX_HOME-}" ] && [ -n "${TMPDIR-}" ]
case "${HOME-}" in /*) ;; *) exit 91 ;; esac
case "${CODEX_HOME-}" in /*) ;; *) exit 92 ;; esac
case "${TMPDIR-}" in /*) ;; *) exit 93 ;; esac
case "${XDG_RUNTIME_DIR-}" in /*) ;; *) exit 94 ;; esac
case "${CARGO_HOME-}" in /*) ;; *) exit 95 ;; esac
case "${RUSTUP_HOME-}" in /*) ;; *) exit 96 ;; esac
[ "$1" = exec ]
shift
session=native-codex-1
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
prompt=$(sed -n '1,$p')
[ -n "$prompt" ]
printf '{"type":"thread.started","thread_id":"%s"}\n' "$session"
printf '%s\n' '{"type":"turn.started"}'
printf '%s' '{"decision":"blocked","summary":"bounded fake result","head_revision":null,"commits":[],"checks":[],"questions":[],"risks":[],"changed_paths":[]}' >"$output"
if [ "$prompt" = exit-nonzero ]; then
    exit 17
fi
if [ "$prompt" = auth-quota ]; then
    printf '%s\n' '{"type":"error","message":"quota unavailable"}'
    exit 0
fi
if [ "$prompt" = missing-terminal ]; then
    exit 0
fi
printf '%s\n' '{"type":"turn.completed"}'
"#
    }

    fn fake_codex_script_with_socket_probes(socket_paths: &[PathBuf]) -> String {
        let socket_paths: Vec<_> = socket_paths
            .iter()
            .map(|path| {
                let path = path.to_str().unwrap();
                assert!(!path.contains('\''));
                path
            })
            .collect();
        let socket_paths = serde_json::to_string(&socket_paths).unwrap();
        let probe = format!(
            "/usr/bin/python3 -c 'import os,socket\nfor p in {socket_paths}:\n    if os.path.exists(p):\n        raise SystemExit(41)\n    client=socket.socket(socket.AF_UNIX)\n    try:\n        client.connect(p)\n    except FileNotFoundError:\n        pass\n    else:\n        raise SystemExit(42)\n    finally:\n        client.close()'"
        );
        fake_codex_script().replace("# SOCKET_REACHABILITY_PROBE", &probe)
    }

    fn fake_codex_script_with_path_probe(
        project: &Path,
        workspace: &Path,
        hidden_file: &Path,
    ) -> String {
        let project = project.to_str().unwrap();
        let workspace = workspace.to_str().unwrap();
        let hidden_file = hidden_file.to_str().unwrap();
        assert!(!project.contains('\''));
        assert!(!workspace.contains('\''));
        assert!(!hidden_file.contains('\''));
        let probe = format!(
            "[ ! -e '{hidden_file}' ]\n\
             if /usr/bin/touch '{project}/project-write-probe' 2>/dev/null; then \
                 /usr/bin/rm '{project}/project-write-probe'; exit 81; \
             fi\n\
             /usr/bin/touch '{workspace}/repository-write-probe'\n\
             /usr/bin/rm '{workspace}/repository-write-probe'"
        );
        fake_codex_script().replace("# SOCKET_REACHABILITY_PROBE", &probe)
    }

    fn git_ok(git: &Path, workspace: &Path, args: &[&str]) {
        let status = Command::new(git)
            .args(args)
            .current_dir(workspace)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .status()
            .unwrap();
        assert!(status.success(), "git fixture command failed: {args:?}");
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
