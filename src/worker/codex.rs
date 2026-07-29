//! Exact-version Codex no-TUI developer adapter.

use super::contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeOutputKind, NativeResult, OuterLaunchEnvelope, OutputDeclaration,
    ResultTransport, SchemaTransport, TurnControl, WorkerAdapter, WorkerProfile,
    validate_native_session_id,
};
use super::environment::{EnvironmentPolicy, ExactEnvironmentRequirement};
use super::result::{
    CheckStatus, CommitSummary, DeveloperDecision, DeveloperResult, MAX_RESULT_BYTES,
};
use super::validation::{
    MAX_ITEMS, MAX_PATH_BYTES, validate_git_oid, validate_relative_path, validate_text,
};
use crate::control_api::daemon::ControlPaths;
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
pub const CODEX_DEVELOPER_REASONING: &str = "high";
pub const BWRAP_EXECUTABLE: &str = "/usr/bin/bwrap";
pub const BWRAP_VERSION: &str = "bubblewrap 0.9.0";
pub const GIT_EXECUTABLE: &str = "/usr/bin/git";
pub const GIT_VERSION: &str = "git version 2.43.0";

const ADAPTER_NAME: &str = "codex-developer-0.145.0";
const ADAPTER_CONTRACT_VERSION: u32 = 1;
const EFFECTIVE_POLICY: &str =
    "native=danger-full-access;outer=bubblewrap-0.9.0-developer-v1;approval=never";
const MAX_CODEX_EVENTS: usize = 4096;
const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_DURATION: Duration = Duration::from_secs(30);
const CODEX_RESULT_SCHEMA_FILE: &str = "codex-developer-result-schema.json";
const CODEX_FINAL_FILE: &str = "native-final.partial";
const CODEX_AUTH_FILE: &str = "auth.json";

const DISABLED_CODEX_FEATURES: &[&str] = &[
    "apps",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode_host",
    "computer_use",
    "goals",
    "guardian_approval",
    "hooks",
    "image_generation",
    "in_app_browser",
    "memories",
    "multi_agent",
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
    pub workspace_cwd: PathBuf,
    pub artifact_root: PathBuf,
    pub isolated_home: PathBuf,
    pub codex_home: PathBuf,
    pub temp_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub host_runtime_dir: PathBuf,
    pub auth_source: PathBuf,
}

pub struct CodexDeveloperAdapter {
    descriptor: AdapterDescriptor,
    executable: ExecutableIdentity,
    outer_executable: ExecutableIdentity,
    git_executable: ExecutableIdentity,
    sandbox: SandboxContract,
}

impl CodexDeveloperAdapter {
    pub fn discover(config: CodexDeveloperConfig) -> Result<Self> {
        validate_production_runtime_contract(&config)?;
        Self::discover_with_paths(
            config,
            Path::new(CODEX_DEVELOPER_EXECUTABLE),
            Path::new(BWRAP_EXECUTABLE),
            Path::new(GIT_EXECUTABLE),
        )
    }

    pub fn environment_policy() -> Result<EnvironmentPolicy> {
        let mut inherited = EnvironmentPolicy::baseline().inherited_names;
        inherited.extend(
            ["CODEX_HOME", "HOME", "TMPDIR", "XDG_RUNTIME_DIR"]
                .into_iter()
                .map(str::to_owned),
        );
        inherited.sort();
        inherited.dedup();
        EnvironmentPolicy::new(
            inherited,
            vec![
                "CODEX_HOME".into(),
                "HOME".into(),
                "PATH".into(),
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
        let executable = capture_exact_tool(codex_path, CODEX_DEVELOPER_CLI_VERSION)?;
        let outer_executable = capture_exact_tool(bwrap_path, BWRAP_VERSION)?;
        let git_executable = capture_exact_tool(git_path, GIT_VERSION)?;
        let sandbox =
            SandboxContract::capture(config, &executable, &outer_executable, &git_executable)?;
        let descriptor = AdapterDescriptor::new(
            ADAPTER_NAME,
            ADAPTER_CONTRACT_VERSION,
            CODEX_DEVELOPER_CLI_VERSION,
            CODEX_DEVELOPER_MODEL,
            CODEX_DEVELOPER_REASONING,
            EFFECTIVE_POLICY,
            AdapterCapabilities {
                roles: vec![WorkerRole::Developer],
                native_session_mode: NativeSessionMode::Discovered,
                result_transport: ResultTransport::FinalFile,
                features: vec![
                    "exact-resume".into(),
                    "host-git-evidence".into(),
                    "outer-bwrap-v1".into(),
                    "structured-result".into(),
                ],
            },
        )?;
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

        let mut fixed_argv = vec!["exec".into()];
        if let Some(session_id) = resume_session_id {
            validate_native_session_id(session_id)?;
            fixed_argv.extend(["resume".into(), session_id.into()]);
        }
        fixed_argv.extend([
            "--json".into(),
            "--model".into(),
            CODEX_DEVELOPER_MODEL.into(),
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
            fixed_argv.extend([
                "--cd".into(),
                path_text("Codex developer workspace", self.sandbox.workspace.path())?.into(),
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
            outer_launch: Some(outer_launch),
            exact_environment: self.sandbox.exact_environment()?,
        })
    }
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
            bail!("Codex resume must use the exact durable native session");
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
        if !artifacts.stderr().iter().all(u8::is_ascii_whitespace) {
            bail!("Codex developer emitted unexpected stderr");
        }
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

struct SandboxContract {
    workspace: DirectoryIdentity,
    artifact_root: DirectoryIdentity,
    isolated_home: DirectoryIdentity,
    codex_home: DirectoryIdentity,
    temp_dir: DirectoryIdentity,
    runtime_dir: DirectoryIdentity,
    host_runtime_dir: DirectoryIdentity,
    control_socket_path: PathBuf,
    auth_source: FileIdentity,
    auth_target: FileIdentity,
    git_workspace: GitWorkspaceIdentity,
}

impl SandboxContract {
    fn capture(
        config: CodexDeveloperConfig,
        codex: &ExecutableIdentity,
        bwrap: &ExecutableIdentity,
        git: &ExecutableIdentity,
    ) -> Result<Self> {
        let workspace = DirectoryIdentity::capture(&config.workspace_cwd, false)?;
        let artifact_root = DirectoryIdentity::capture(&config.artifact_root, true)?;
        let isolated_home = DirectoryIdentity::capture(&config.isolated_home, true)?;
        let codex_home = DirectoryIdentity::capture(&config.codex_home, true)?;
        let temp_dir = DirectoryIdentity::capture(&config.temp_dir, true)?;
        let runtime_dir = DirectoryIdentity::capture(&config.runtime_dir, true)?;
        let host_runtime_dir = DirectoryIdentity::capture(&config.host_runtime_dir, true)?;
        let control_socket_path = host_runtime_dir
            .path()
            .join("hcom-project-control/control.sock");
        let auth_source = FileIdentity::capture(&config.auth_source)?;
        let auth_target_path = codex_home.path().join(CODEX_AUTH_FILE);
        let auth_target = FileIdentity::capture(&auth_target_path)?;
        if auth_source.path() == auth_target.path() {
            bail!("Codex auth source must be distinct from the isolated mount target");
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
                "workspace",
                workspace.path(),
                "isolated temp",
                temp_dir.path(),
            ),
            (
                "workspace",
                workspace.path(),
                "private runtime",
                runtime_dir.path(),
            ),
            (
                "workspace",
                workspace.path(),
                "host runtime mask",
                host_runtime_dir.path(),
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
            (
                "artifact root",
                artifact_root.path(),
                "isolated temp",
                temp_dir.path(),
            ),
            (
                "artifact root",
                artifact_root.path(),
                "private runtime",
                runtime_dir.path(),
            ),
            (
                "artifact root",
                artifact_root.path(),
                "host runtime mask",
                host_runtime_dir.path(),
            ),
            (
                "isolated CODEX_HOME",
                codex_home.path(),
                "isolated temp",
                temp_dir.path(),
            ),
            (
                "isolated CODEX_HOME",
                codex_home.path(),
                "private runtime",
                runtime_dir.path(),
            ),
            (
                "isolated HOME",
                isolated_home.path(),
                "host runtime mask",
                host_runtime_dir.path(),
            ),
            (
                "isolated CODEX_HOME",
                codex_home.path(),
                "host runtime mask",
                host_runtime_dir.path(),
            ),
            (
                "isolated temp",
                temp_dir.path(),
                "private runtime",
                runtime_dir.path(),
            ),
            (
                "isolated temp",
                temp_dir.path(),
                "host runtime mask",
                host_runtime_dir.path(),
            ),
            (
                "private runtime",
                runtime_dir.path(),
                "host runtime mask",
                host_runtime_dir.path(),
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

        let writable_roots = [
            workspace.path(),
            isolated_home.path(),
            temp_dir.path(),
            artifact_root.path(),
        ];
        for protected in [
            codex.canonical_path.as_path(),
            bwrap.canonical_path.as_path(),
            git.canonical_path.as_path(),
            auth_source.path(),
        ] {
            if protected.starts_with(host_runtime_dir.path()) {
                bail!("host runtime mask must not hide a required Codex sandbox file");
            }
            if writable_roots
                .iter()
                .any(|root| protected.starts_with(root))
            {
                bail!("sandbox writable roots must not contain a protected host file");
            }
        }
        let git_workspace = GitWorkspaceIdentity::capture(workspace.path(), git)?;
        Ok(Self {
            workspace,
            artifact_root,
            isolated_home,
            codex_home,
            temp_dir,
            runtime_dir,
            host_runtime_dir,
            control_socket_path,
            auth_source,
            auth_target,
            git_workspace,
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
        self.workspace.revalidate(false)?;
        self.artifact_root.revalidate(true)?;
        self.isolated_home.revalidate(true)?;
        self.codex_home.revalidate(true)?;
        self.temp_dir.revalidate(true)?;
        self.runtime_dir.revalidate(true)?;
        self.host_runtime_dir.revalidate(true)?;
        if self.control_socket_path
            != self
                .host_runtime_dir
                .path()
                .join("hcom-project-control/control.sock")
        {
            bail!("Codex durable control socket mask contract drifted");
        }
        self.auth_source.revalidate()?;
        self.auth_target.revalidate()?;
        self.git_workspace.revalidate(self.workspace.path(), git)
    }

    fn exact_environment(&self) -> Result<Vec<ExactEnvironmentRequirement>> {
        Ok(vec![
            ExactEnvironmentRequirement::new(
                "CODEX_HOME",
                path_text("isolated CODEX_HOME", self.codex_home.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "HOME",
                path_text("isolated HOME", self.isolated_home.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "TMPDIR",
                path_text("isolated temp", self.temp_dir.path())?,
            )?,
            ExactEnvironmentRequirement::new(
                "XDG_RUNTIME_DIR",
                path_text("private runtime", self.runtime_dir.path())?,
            )?,
        ])
    }

    fn outer_argv(&self, artifact_dir: &Path, codex: &ExecutableIdentity) -> Result<Vec<String>> {
        if !artifact_dir.starts_with(self.artifact_root.path()) {
            bail!("Codex artifact attempt escaped its pinned artifact root");
        }
        let mut argv: Vec<String> = vec![
            "--die-with-parent".into(),
            "--unshare-pid".into(),
            "--unshare-ipc".into(),
            "--unshare-uts".into(),
            "--ro-bind".into(),
            "/".into(),
            "/".into(),
            "--proc".into(),
            "/proc".into(),
            "--dev".into(),
            "/dev".into(),
            "--tmpfs".into(),
            path_text("host XDG runtime mask", self.host_runtime_dir.path())?.into(),
        ];
        for path in [
            self.isolated_home.path(),
            self.codex_home.path(),
            self.temp_dir.path(),
            self.workspace.path(),
            artifact_dir,
        ] {
            let path = path_text("Codex writable sandbox mount", path)?;
            argv.extend(["--bind".into(), path.into(), path.into()]);
        }
        argv.extend([
            "--tmpfs".into(),
            path_text("Codex private runtime mount", self.runtime_dir.path())?.into(),
            "--ro-bind".into(),
            path_text("Codex auth source", self.auth_source.path())?.into(),
            path_text("Codex auth target", self.auth_target.path())?.into(),
            "--chdir".into(),
            path_text("Codex developer workspace", self.workspace.path())?.into(),
        ]);
        let host_runtime = path_text("host XDG runtime mask", self.host_runtime_dir.path())?;
        let private_runtime = path_text("Codex private runtime mount", self.runtime_dir.path())?;
        let tmpfs_targets: BTreeSet<_> = argv
            .windows(2)
            .filter(|pair| pair[0] == "--tmpfs")
            .map(|pair| pair[1].as_str())
            .collect();
        if !self
            .control_socket_path
            .starts_with(self.host_runtime_dir.path())
            || !tmpfs_targets.contains(host_runtime)
            || !tmpfs_targets.contains(private_runtime)
        {
            bail!("Codex outer sandbox does not mask its resolved durable runtime endpoints");
        }
        if argv.iter().any(|argument| {
            argument == "--"
                || argument == &codex.canonical_path.to_string_lossy()
                || argument.contains("hcom-project-control/control.sock")
        }) {
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
        if self.uid != uid || self.mode & 0o022 != 0 {
            bail!("Codex sandbox directory has unsafe ownership or write permissions");
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
    let mut child = command.spawn().context("failed to spawn bounded helper")?;
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
        bail!("durable worker tool version does not match its exact enabled contract");
    }
    let after = ExecutableIdentity::capture(path)?;
    if before != after {
        bail!("durable worker tool identity changed during version validation");
    }
    Ok(after)
}

fn revalidate_exact_tool(identity: &ExecutableIdentity, expected: &str) -> Result<()> {
    let current = capture_exact_tool(&identity.canonical_path, expected)?;
    if current != *identity {
        bail!("durable Codex tool identity or version drifted");
    }
    Ok(())
}

fn developer_result_schema() -> Vec<u8> {
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
            "head_revision": {"type": ["string", "null"]},
            "commits": {
                "type": "array",
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
            "changed_paths": {"type": "array", "items": {"type": "string"}}
        }
    }))
    .expect("static Codex developer result schema is valid JSON")
}

struct CodexTurnEvidence {
    native_session_id: String,
    completed_commands: BTreeSet<String>,
    failed_commands: BTreeSet<String>,
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
struct ItemEvent {
    #[serde(rename = "type")]
    _kind: String,
    item: CodexItem,
}

#[derive(Deserialize)]
struct CodexItem {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    command: Option<String>,
    exit_code: Option<i32>,
    status: Option<String>,
}

fn parse_codex_turn(control: &TurnControl, stdout: &[u8]) -> Result<CodexTurnEvidence> {
    control.validate()?;
    if control.role != WorkerRole::Developer || stdout.is_empty() {
        bail!("Codex JSONL does not match a developer turn");
    }
    let text = std::str::from_utf8(stdout).context("Codex JSONL is not UTF-8")?;
    validate_text("Codex JSONL", text, 1024 * 1024, true)?;
    let mut session = None;
    let mut turn_started = false;
    let mut turn_completed = false;
    let mut event_count = 0usize;
    let mut completed_commands = BTreeSet::new();
    let mut failed_commands = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        event_count += 1;
        if event_count > MAX_CODEX_EVENTS || line.len() > 128 * 1024 {
            bail!("Codex JSONL exceeds its bounded event shape");
        }
        if turn_completed {
            bail!("Codex JSONL contains events after its terminal event");
        }
        let header: EventHeader =
            serde_json::from_str(line).context("Codex JSONL event header is malformed")?;
        validate_text("Codex event type", &header.kind, 128, false)?;
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
                if event.item.kind == "mcp_tool_call" {
                    bail!("Codex developer emitted forbidden MCP activity");
                }
                if let Some(status) = &event.item.status {
                    validate_text("Codex item status", status, 128, false)?;
                }
                if header.kind == "item.completed" && event.item.kind == "command_execution" {
                    let command = event
                        .item
                        .command
                        .ok_or_else(|| anyhow!("Codex command event omitted its command"))?;
                    validate_text("Codex command event", &command, 4096, false)?;
                    let exit_code = event
                        .item
                        .exit_code
                        .ok_or_else(|| anyhow!("Codex command event omitted its exit code"))?;
                    if event.item.status.as_deref() != Some("completed") {
                        bail!("Codex command event did not reach completed status");
                    }
                    if exit_code == 0 {
                        completed_commands.insert(command);
                    } else {
                        failed_commands.insert(command);
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

fn observe_codex_record(record: &[u8]) -> Result<Vec<NativeObservation>> {
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
                "mcp_tool_call" => bail!("Codex native record reported forbidden MCP activity"),
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

fn parse_git_commits(bytes: &[u8]) -> Result<Vec<CommitSummary>> {
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

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<String>> {
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
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR is required for the Codex worker sandbox"))?;
    let canonical_runtime =
        fs::canonicalize(&runtime).context("failed to resolve host XDG_RUNTIME_DIR")?;
    if runtime != canonical_runtime || config.host_runtime_dir != canonical_runtime {
        bail!("Codex worker host runtime mask does not match canonical XDG_RUNTIME_DIR");
    }
    let resolved_control_socket = ControlPaths::discover()?.socket_path();
    if resolved_control_socket != canonical_runtime.join("hcom-project-control/control.sock") {
        bail!("Codex worker could not resolve the exact durable control socket under its mask");
    }
    Ok(())
}

fn path_text<'a>(label: &str, path: &'a Path) -> Result<&'a str> {
    let text = path
        .to_str()
        .ok_or_else(|| anyhow!("{label} must be valid UTF-8"))?;
    validate_text(label, text, MAX_PATH_BYTES, false)?;
    Ok(text)
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
            let tools = temp.path().join("tools");
            for directory in [
                &workspace,
                &artifact_root,
                &isolated_home,
                &codex_home,
                &temp_dir,
                &runtime_dir,
                &host_runtime_dir,
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
                    workspace_cwd: fs::canonicalize(workspace).unwrap(),
                    artifact_root: fs::canonicalize(artifact_root).unwrap(),
                    isolated_home: fs::canonicalize(isolated_home).unwrap(),
                    codex_home: fs::canonicalize(codex_home).unwrap(),
                    temp_dir: fs::canonicalize(temp_dir).unwrap(),
                    runtime_dir: fs::canonicalize(runtime_dir).unwrap(),
                    host_runtime_dir: fs::canonicalize(host_runtime_dir).unwrap(),
                    auth_source: fs::canonicalize(auth_source).unwrap(),
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
                        "CODEX_HOME".into(),
                        self.config.codex_home.to_string_lossy().into_owned(),
                    ),
                    (
                        "HOME".into(),
                        self.config.isolated_home.to_string_lossy().into_owned(),
                    ),
                    ("PATH".into(), "/usr/bin:/bin".into()),
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
                    project_id: control.project_id.clone(),
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
                        project_id: control.project_id.clone(),
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
            project_id: "project-1".into(),
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
                "project-1/task-1/developer/logical-1/turn-{}/attempt-1",
                if native_session_id.is_some() { 2 } else { 1 }
            ),
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
        ] {
            assert!(parse_codex_turn(&control(None), invalid.as_bytes()).is_err());
        }
        assert!(parse_codex_turn(&control(Some("native-other")), stdout.as_bytes()).is_err());
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
        assert_eq!(profile.policy, EFFECTIVE_POLICY);
        assert_eq!(profile.native_session_mode, NativeSessionMode::Discovered);

        let prompt = b"private task body sentinel phase-five";
        let create_control = fixture.control(None);
        let create =
            prepare_create_turn(&adapter, &profile, &create_control, prompt.to_vec()).unwrap();
        let argv = create.command().materialized_control_argv();
        assert_eq!(argv.last().map(String::as_str), Some("-"));
        assert!(argv.iter().any(|argument| argument == "--die-with-parent"));
        assert!(argv.iter().any(|argument| argument == "--unshare-pid"));
        assert!(
            argv.iter()
                .any(|argument| argument == "--dangerously-bypass-approvals-and-sandbox")
        );
        assert!(!argv.iter().any(|argument| argument == "--new-session"));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", CODEX_DEVELOPER_MODEL])
        );
        assert!(argv.windows(2).any(|pair| pair == ["--disable", "hooks"]));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--tmpfs", fixture.config.runtime_dir.to_str().unwrap()])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--tmpfs", fixture.config.host_runtime_dir.to_str().unwrap()])
        );
        assert!(
            !argv
                .join("\0")
                .contains("hcom-project-control/control.sock")
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
        assert_eq!(resume_argv.last().map(String::as_str), Some("-"));
        assert!(
            resume_argv
                .windows(2)
                .any(|pair| pair == ["resume", "native-codex-1"])
        );
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
    fn real_bwrap_masks_a_live_host_control_socket() {
        let fixture = Fixture::new();
        let control_root = fixture.config.host_runtime_dir.join("hcom-project-control");
        fs::create_dir(&control_root).unwrap();
        fs::set_permissions(&control_root, fs::Permissions::from_mode(0o700)).unwrap();
        let socket_path = control_root.join("control.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).unwrap();
        let outside = UnixStream::connect(&socket_path).unwrap();
        drop(outside);

        write_executable(
            &fixture.codex,
            &fake_codex_script_with_socket_probe(&socket_path),
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
        drop(listener);
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
    fn auth_quota_session_result_environment_and_identity_drift_fail_closed() {
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

        let policy = CodexDeveloperAdapter::environment_policy().unwrap();
        assert!(
            ExecutionEnvironmentLease::capture(
                "lease-bad",
                "epoch-bad",
                &policy,
                vec![
                    (
                        "CODEX_HOME".into(),
                        fixture.config.codex_home.to_string_lossy().into_owned()
                    ),
                    (
                        "HOME".into(),
                        fixture.config.isolated_home.to_string_lossy().into_owned()
                    ),
                    ("PATH".into(), "/usr/bin".into()),
                    (
                        "TMPDIR".into(),
                        fixture.config.temp_dir.to_string_lossy().into_owned()
                    ),
                    (
                        "XDG_RUNTIME_DIR".into(),
                        fixture.config.runtime_dir.to_string_lossy().into_owned()
                    ),
                    ("HCOM_AGENT".into(), "forbidden".into()),
                ]
            )
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
                (
                    "CODEX_HOME".into(),
                    fixture.config.codex_home.to_string_lossy().into_owned(),
                ),
                ("HOME".into(), "/wrong/isolated-home".into()),
                ("PATH".into(), "/usr/bin:/bin".into()),
                (
                    "TMPDIR".into(),
                    fixture.config.temp_dir.to_string_lossy().into_owned(),
                ),
                (
                    "XDG_RUNTIME_DIR".into(),
                    fixture.config.runtime_dir.to_string_lossy().into_owned(),
                ),
            ],
        )
        .unwrap();
        let attempt = ArtifactAttempt::create(
            &root,
            ArtifactScope {
                project_id: control.project_id.clone(),
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
                    project_id: control.project_id,
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
            stdout.push_str(&format!(
                "{{\"type\":\"item.completed\",\"item\":\
                 {{\"id\":\"item-{index}\",\"type\":\"command_execution\",\
                 \"command\":{},\"exit_code\":{exit_code},\"status\":\"completed\"}}}}\n",
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
[ -n "${HCOM_PROJECT_ID-}" ] && [ -n "${HCOM_TASK_ID-}" ]
[ -z "${HCOM_AGENT-}" ] && [ -z "${TERM-}" ] && [ -z "${STY-}" ]
[ -n "${HOME-}" ] && [ -n "${CODEX_HOME-}" ] && [ -n "${TMPDIR-}" ]
[ -n "${XDG_RUNTIME_DIR-}" ] && [ -f "$CODEX_HOME/auth.json" ]
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

    fn fake_codex_script_with_socket_probe(socket_path: &Path) -> String {
        let socket_path = socket_path.to_str().unwrap();
        assert!(!socket_path.contains('\''));
        let socket_path = serde_json::to_string(socket_path).unwrap();
        let probe = format!(
            "/usr/bin/python3 -c 'import os,socket\np={socket_path}\nif os.path.exists(p):\n    raise SystemExit(41)\nclient=socket.socket(socket.AF_UNIX)\ntry:\n    client.connect(p)\nexcept FileNotFoundError:\n    pass\nelse:\n    raise SystemExit(42)'"
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
