//! Pinned Codex 0.146 App Server implementation of the task-worker runtime.
//!
//! This module owns provider-specific process, JSON-RPC, thread, and turn
//! identities. The session supervisor sees only the provider-neutral runtime
//! contract from `worker::runtime`.

use super::codex::{BWRAP_EXECUTABLE, BWRAP_VERSION, DISABLED_CODEX_FEATURES};
use super::codex_rpc::{CodexRpc, OPT_OUT_NOTIFICATION_METHODS, RpcInbound};
use super::contract::ExecutableIdentity;
use super::process::{ProcessGroupBinding, configure_worker_child};
use super::runtime::{
    CODEX_APP_SERVER_EXECUTABLE_SHA256, CODEX_APP_SERVER_SCHEMA_CANONICAL_SHA256,
    CODEX_APP_SERVER_SCHEMA_EXEMPLAR_SHA256, OutcomeContract, RoleSessionSpec,
    RuntimeContractIdentity, RuntimeError, RuntimeFailureClass, RuntimeOutcome, RuntimeSessionKey,
    RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnSpec, SanitizedRuntimeFailure, TaskWorkerRuntime,
};
use super::sandbox::{HostRootAccess, HostRootContract, HostRootMounts};
use crate::control_api::WorkerRole;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const CODEX_APP_SERVER_EXECUTABLE: &str =
    "/home/ywxk/.codex/packages/standalone/releases/0.146.0-x86_64-unknown-linux-musl/bin/codex";
pub const CODEX_APP_SERVER_CLI_VERSION: &str = "codex-cli 0.146.0";

const CLIENT_NAME: &str = "hcom_arch";
const CLIENT_TITLE: &str = "hcom Architect task workers";
const MAX_PREFLIGHT_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_NATIVE_ID_BYTES: usize = 256;
const MAX_TASK_NATIVE_TURNS: usize = 64;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_TERMINATE_GRACE: Duration = Duration::from_secs(2);
const INTERRUPT_RESPONSE_GRACE: Duration = Duration::from_millis(500);
const STABLE_FIXTURE: &str =
    include_str!("../../tests/fixtures/codex-app-server-0.146-stable-contract.json");

#[derive(Clone)]
pub struct CodexAppServerPreflight {
    executable: ExecutableIdentity,
    contract: RuntimeContractIdentity,
}

impl CodexAppServerPreflight {
    /// Verify the exact local binary and its generated stable protocol without
    /// starting an App Server or a model turn.
    pub fn verify_pinned() -> Result<Self, RuntimeError> {
        let expected_path = Path::new(CODEX_APP_SERVER_EXECUTABLE);
        let executable = ExecutableIdentity::capture(expected_path)
            .map_err(|_| RuntimeError::invalid_identity("failed to capture pinned Codex binary"))?;
        if executable.canonical_path != expected_path
            || executable.sha256 != CODEX_APP_SERVER_EXECUTABLE_SHA256
        {
            return Err(RuntimeError::invalid_identity(
                "pinned Codex executable path or SHA-256 mismatch",
            ));
        }

        let fixture: StableSchemaFixture = serde_json::from_str(STABLE_FIXTURE)
            .map_err(|_| RuntimeError::invalid_contract("stable schema fixture is invalid"))?;
        fixture.validate_constants()?;
        let temporary = tempfile::Builder::new()
            .prefix("hcom-codex-preflight-")
            .tempdir()
            .map_err(|_| RuntimeError::internal("failed to create Codex preflight directory"))?;
        let home = temporary.path().join("home");
        let codex_home = home.join(".codex");
        let schema_dir = temporary.path().join("schema");
        fs::create_dir_all(&codex_home)
            .and_then(|()| fs::create_dir(&schema_dir))
            .map_err(|_| RuntimeError::internal("failed to prepare Codex preflight directory"))?;
        let config_path = codex_home.join("config.toml");
        write_private_file(&config_path, b"mcp_servers = {}\n")?;

        let version = run_exact_command(
            "version",
            &executable,
            &home,
            &codex_home,
            temporary.path(),
            ["--version"],
        )?;
        if trim_single_line(&version) != Some(CODEX_APP_SERVER_CLI_VERSION) {
            return Err(RuntimeError::invalid_contract(
                "pinned Codex version output mismatch",
            ));
        }

        let help = run_exact_command(
            "app-server help",
            &executable,
            &home,
            &codex_home,
            temporary.path(),
            ["app-server", "--help"],
        )?;
        let help = std::str::from_utf8(&help)
            .map_err(|_| RuntimeError::invalid_contract("Codex app-server help was not UTF-8"))?;
        for required in [
            "--config",
            "--disable",
            "--strict-config",
            "--stdio",
            "generate-json-schema",
        ] {
            if !help.contains(required) {
                return Err(RuntimeError::invalid_contract(
                    "Codex app-server help omitted a required capability",
                ));
            }
        }
        let mut launch_probe = native_app_server_arguments();
        launch_probe.push("--help".into());
        run_exact_command(
            "runtime argument",
            &executable,
            &home,
            &codex_home,
            temporary.path(),
            launch_probe.iter(),
        )?;

        let features = run_exact_command(
            "feature inventory",
            &executable,
            &home,
            &codex_home,
            temporary.path(),
            ["features", "list"],
        )?;
        let features = std::str::from_utf8(&features)
            .map_err(|_| RuntimeError::invalid_contract("Codex feature list was not UTF-8"))?;
        let available: BTreeSet<_> = features
            .lines()
            .filter_map(|line| line.split_ascii_whitespace().next())
            .collect();
        if DISABLED_CODEX_FEATURES
            .iter()
            .any(|feature| !available.contains(feature))
        {
            return Err(RuntimeError::invalid_contract(
                "Codex disabled-feature inventory drifted",
            ));
        }

        let schema_path = schema_dir
            .to_str()
            .ok_or_else(|| RuntimeError::internal("schema path was not UTF-8"))?
            .to_owned();
        run_exact_command(
            "stable schema generation",
            &executable,
            &home,
            &codex_home,
            temporary.path(),
            ["app-server", "generate-json-schema", "--out", &schema_path],
        )?;
        verify_generated_stable_schema(&schema_dir, &fixture)?;

        let contract = RuntimeContractIdentity::codex_app_server_0_146();
        contract.validate()?;
        Ok(Self {
            executable,
            contract,
        })
    }

    pub fn executable(&self) -> &ExecutableIdentity {
        &self.executable
    }

    pub fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    pub fn revalidate(&self) -> Result<(), RuntimeError> {
        self.executable
            .revalidate()
            .map_err(|_| RuntimeError::invalid_identity("pinned Codex executable drifted"))?;
        if self.executable.sha256 != CODEX_APP_SERVER_EXECUTABLE_SHA256
            || self.contract != RuntimeContractIdentity::codex_app_server_0_146()
        {
            return Err(RuntimeError::invalid_contract(
                "Codex App Server preflight identity drifted",
            ));
        }
        Ok(())
    }
}

struct CodexAppServerLaunch {
    authority: LaunchAuthority,
    program: PathBuf,
    arguments: Vec<OsString>,
    cwd: PathBuf,
    environment: Vec<(OsString, OsString)>,
    sandbox: Option<Box<CodexAppServerSandbox>>,
    request_timeout: Duration,
    terminate_grace: Duration,
}

enum LaunchAuthority {
    Pinned(Box<CodexAppServerPreflight>),
    #[cfg(test)]
    Fixture,
}

impl CodexAppServerLaunch {
    pub(crate) fn direct(
        preflight: CodexAppServerPreflight,
        cwd: PathBuf,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        preflight.revalidate()?;
        validate_launch_cwd(&cwd)?;
        Ok(Self {
            program: preflight.executable.canonical_path.clone(),
            authority: LaunchAuthority::Pinned(Box::new(preflight)),
            arguments: native_app_server_arguments(),
            cwd,
            environment,
            sandbox: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            terminate_grace: DEFAULT_TERMINATE_GRACE,
        })
    }

    pub(crate) fn sandboxed(
        preflight: CodexAppServerPreflight,
        sandbox_config: CodexAppServerSandboxConfig,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        preflight.revalidate()?;
        let sandbox = CodexAppServerSandbox::capture(sandbox_config, preflight.executable())?;
        let cwd = sandbox.repository.path.clone();
        let program = sandbox.outer.canonical_path.clone();
        let arguments = sandbox.launch_arguments()?;
        Ok(Self {
            authority: LaunchAuthority::Pinned(Box::new(preflight)),
            program,
            arguments,
            cwd,
            environment,
            sandbox: Some(Box::new(sandbox)),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            terminate_grace: DEFAULT_TERMINATE_GRACE,
        })
    }

    #[cfg(test)]
    fn fixture(
        program: PathBuf,
        arguments: Vec<OsString>,
        cwd: PathBuf,
        environment: Vec<(OsString, OsString)>,
    ) -> Self {
        Self {
            authority: LaunchAuthority::Fixture,
            program,
            arguments,
            cwd,
            environment,
            sandbox: None,
            request_timeout: Duration::from_secs(3),
            terminate_grace: Duration::from_millis(250),
        }
    }

    #[cfg(test)]
    fn sandboxed_fixture(
        program: PathBuf,
        sandbox_config: CodexAppServerSandboxConfig,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        let native = ExecutableIdentity::capture(&program)
            .map_err(|_| RuntimeError::invalid_identity("fixture executable was unsafe"))?;
        let sandbox = CodexAppServerSandbox::capture(sandbox_config, &native)?;
        let cwd = sandbox.repository.path.clone();
        let outer_program = sandbox.outer.canonical_path.clone();
        let arguments = sandbox.launch_arguments()?;
        Ok(Self {
            authority: LaunchAuthority::Fixture,
            program: outer_program,
            arguments,
            cwd,
            environment,
            sandbox: Some(Box::new(sandbox)),
            request_timeout: Duration::from_secs(3),
            terminate_grace: Duration::from_millis(250),
        })
    }

    fn contract(&self) -> RuntimeContractIdentity {
        match &self.authority {
            LaunchAuthority::Pinned(preflight) => preflight.contract.clone(),
            #[cfg(test)]
            LaunchAuthority::Fixture => RuntimeContractIdentity::codex_app_server_0_146(),
        }
    }

    fn revalidate(&self) -> Result<(), RuntimeError> {
        validate_launch_cwd(&self.cwd)?;
        if let Some(sandbox) = &self.sandbox {
            sandbox.revalidate()?;
        }
        let codex_home = required_environment_path(&self.environment, "CODEX_HOME")?;
        validate_private_runtime_directory(&codex_home, "Codex App Server CODEX_HOME")?;
        let temporary = required_environment_path(&self.environment, "TMPDIR")?;
        validate_private_runtime_directory(&temporary, "Codex App Server TMPDIR")?;
        if self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(30)
            || self.terminate_grace.is_zero()
            || self.terminate_grace > Duration::from_secs(30)
        {
            return Err(RuntimeError::invalid_contract(
                "Codex App Server process timing is outside its bound",
            ));
        }
        match &self.authority {
            LaunchAuthority::Pinned(preflight) => preflight.revalidate(),
            #[cfg(test)]
            LaunchAuthority::Fixture => Ok(()),
        }
    }
}

pub(crate) struct CodexAppServerSandboxConfig {
    pub repository_root: PathBuf,
    pub isolated_home: PathBuf,
    pub codex_home: PathBuf,
    pub temp_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub artifact_dir: PathBuf,
    pub hcom_dir: PathBuf,
    pub xdg_config_home: PathBuf,
    pub xdg_state_home: PathBuf,
    pub xdg_cache_home: PathBuf,
    pub xdg_data_home: PathBuf,
    pub auth_source: PathBuf,
    pub cargo_bin_source: PathBuf,
    pub rustup_home_source: PathBuf,
}

#[derive(Clone, PartialEq, Eq)]
struct SandboxDirectoryIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    private: bool,
}

impl SandboxDirectoryIdentity {
    fn capture(path: &Path, private: bool) -> Result<Self, RuntimeError> {
        let link = fs::symlink_metadata(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox directory was unavailable")
        })?;
        let canonical = fs::canonicalize(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox directory was unavailable")
        })?;
        let metadata = fs::metadata(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox directory was unavailable")
        })?;
        // SAFETY: geteuid has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        let mode = metadata.permissions().mode() & 0o777;
        if canonical != path
            || link.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.uid() != current_uid
            || metadata.permissions().mode() & 0o002 != 0
            || (private && mode != 0o700)
        {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server sandbox directory identity was unsafe",
            ));
        }
        Ok(Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode,
            private,
        })
    }

    fn revalidate(&self) -> Result<(), RuntimeError> {
        if Self::capture(&self.path, self.private)? != *self {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server sandbox directory identity drifted",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SandboxFileIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    size: u64,
    sha256: String,
}

impl SandboxFileIdentity {
    fn capture(path: &Path) -> Result<Self, RuntimeError> {
        let link = fs::symlink_metadata(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox file was unavailable")
        })?;
        let canonical = fs::canonicalize(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox file was unavailable")
        })?;
        let metadata = fs::metadata(path).map_err(|_| {
            RuntimeError::invalid_identity("Codex App Server sandbox file was unavailable")
        })?;
        // SAFETY: geteuid has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        let mode = metadata.permissions().mode() & 0o777;
        if canonical != path
            || link.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.uid() != current_uid
            || mode & 0o077 != 0
            || metadata.len() > 16 * 1024 * 1024
        {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server sandbox file identity was unsafe",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| {
                RuntimeError::invalid_identity("Codex App Server sandbox file was unavailable")
            })?;
        let mut bytes = Vec::new();
        file.take(16 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| RuntimeError::internal("failed to hash sandbox file identity"))?;
        if bytes.len() as u64 != metadata.len() {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server sandbox file changed while captured",
            ));
        }
        Ok(Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode,
            size: metadata.len(),
            sha256: sha256_hex(&bytes),
        })
    }

    fn revalidate(&self) -> Result<(), RuntimeError> {
        if Self::capture(&self.path)? != *self {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server sandbox file identity drifted",
            ));
        }
        Ok(())
    }
}

struct CodexAppServerSandbox {
    outer: ExecutableIdentity,
    native: ExecutableIdentity,
    repository: SandboxDirectoryIdentity,
    isolated_home: SandboxDirectoryIdentity,
    codex_home: SandboxDirectoryIdentity,
    temp_dir: SandboxDirectoryIdentity,
    runtime_dir: SandboxDirectoryIdentity,
    artifact_dir: SandboxDirectoryIdentity,
    hcom_dir: SandboxDirectoryIdentity,
    xdg_config_home: SandboxDirectoryIdentity,
    xdg_state_home: SandboxDirectoryIdentity,
    xdg_cache_home: SandboxDirectoryIdentity,
    xdg_data_home: SandboxDirectoryIdentity,
    auth_source: SandboxFileIdentity,
    auth_target: SandboxFileIdentity,
    config_file: SandboxFileIdentity,
    host_root: HostRootContract,
}

impl CodexAppServerSandbox {
    fn capture(
        config: CodexAppServerSandboxConfig,
        native: &ExecutableIdentity,
    ) -> Result<Self, RuntimeError> {
        native
            .revalidate()
            .map_err(|_| RuntimeError::invalid_identity("pinned Codex executable drifted"))?;
        let outer = capture_bwrap()?;
        let repository = SandboxDirectoryIdentity::capture(&config.repository_root, false)?;
        let isolated_home = SandboxDirectoryIdentity::capture(&config.isolated_home, true)?;
        let codex_home = SandboxDirectoryIdentity::capture(&config.codex_home, true)?;
        if !codex_home.path.starts_with(&isolated_home.path)
            || codex_home.path == isolated_home.path
        {
            return Err(RuntimeError::invalid_contract(
                "Codex App Server CODEX_HOME must be a strict child of isolated HOME",
            ));
        }
        let temp_dir = SandboxDirectoryIdentity::capture(&config.temp_dir, true)?;
        let runtime_dir = SandboxDirectoryIdentity::capture(&config.runtime_dir, true)?;
        let artifact_dir = SandboxDirectoryIdentity::capture(&config.artifact_dir, true)?;
        let hcom_dir = SandboxDirectoryIdentity::capture(&config.hcom_dir, true)?;
        let xdg_config_home = SandboxDirectoryIdentity::capture(&config.xdg_config_home, true)?;
        let xdg_state_home = SandboxDirectoryIdentity::capture(&config.xdg_state_home, true)?;
        let xdg_cache_home = SandboxDirectoryIdentity::capture(&config.xdg_cache_home, true)?;
        let xdg_data_home = SandboxDirectoryIdentity::capture(&config.xdg_data_home, true)?;
        let auth_source = SandboxFileIdentity::capture(&config.auth_source)?;
        let auth_target = SandboxFileIdentity::capture(&codex_home.path.join("auth.json"))?;
        let config_file = SandboxFileIdentity::capture(&codex_home.path.join("config.toml"))?;
        if auth_source.path == auth_target.path {
            return Err(RuntimeError::invalid_contract(
                "Codex App Server auth source must differ from its private mount target",
            ));
        }
        let writable = [
            &repository.path,
            &isolated_home.path,
            &temp_dir.path,
            &runtime_dir.path,
            &artifact_dir.path,
            &hcom_dir.path,
            &xdg_config_home.path,
            &xdg_state_home.path,
            &xdg_cache_home.path,
            &xdg_data_home.path,
        ];
        for protected in [
            native.canonical_path.as_path(),
            outer.canonical_path.as_path(),
            auth_source.path.as_path(),
        ] {
            if writable.iter().any(|root| protected.starts_with(root)) {
                return Err(RuntimeError::invalid_contract(
                    "Codex App Server writable roots contain a protected host file",
                ));
            }
        }
        let host_root =
            HostRootContract::capture(&config.cargo_bin_source, &config.rustup_home_source)
                .map_err(|_| {
                    RuntimeError::invalid_identity("outer sandbox host roots were unsafe")
                })?;
        Ok(Self {
            outer,
            native: native.clone(),
            repository,
            isolated_home,
            codex_home,
            temp_dir,
            runtime_dir,
            artifact_dir,
            hcom_dir,
            xdg_config_home,
            xdg_state_home,
            xdg_cache_home,
            xdg_data_home,
            auth_source,
            auth_target,
            config_file,
            host_root,
        })
    }

    fn revalidate(&self) -> Result<(), RuntimeError> {
        self.outer
            .revalidate()
            .and_then(|()| self.native.revalidate())
            .map_err(|_| RuntimeError::invalid_identity("sandbox executable identity drifted"))?;
        validate_bwrap_version(&self.outer)?;
        for directory in [
            &self.repository,
            &self.isolated_home,
            &self.codex_home,
            &self.temp_dir,
            &self.runtime_dir,
            &self.artifact_dir,
            &self.hcom_dir,
            &self.xdg_config_home,
            &self.xdg_state_home,
            &self.xdg_cache_home,
            &self.xdg_data_home,
        ] {
            directory.revalidate()?;
        }
        self.auth_source.revalidate()?;
        self.auth_target.revalidate()?;
        self.config_file.revalidate()?;
        self.host_root
            .revalidate()
            .map_err(|_| RuntimeError::invalid_identity("outer sandbox host roots drifted"))
    }

    fn launch_arguments(&self) -> Result<Vec<OsString>, RuntimeError> {
        self.revalidate()?;
        let extra_writable_dirs = [
            self.temp_dir.path.as_path(),
            self.runtime_dir.path.as_path(),
            self.hcom_dir.path.as_path(),
            self.xdg_config_home.path.as_path(),
            self.xdg_state_home.path.as_path(),
            self.xdg_cache_home.path.as_path(),
            self.xdg_data_home.path.as_path(),
        ];
        let argv = self
            .host_root
            .host_root_argv(HostRootMounts {
                isolated_home: &self.isolated_home.path,
                native_config: &self.codex_home.path,
                launch_cwd: &self.repository.path,
                artifact_dir: &self.artifact_dir.path,
                auth_source: &self.auth_source.path,
                auth_target: &self.auth_target.path,
                readable_roots: &[&self.repository.path],
                writable_roots: &[&self.repository.path],
                read_only_files: &[&self.native.canonical_path],
                extra_writable_dirs: &extra_writable_dirs,
                host_root_access: HostRootAccess::Hidden,
                masked_dirs: &[],
            })
            .map_err(|_| RuntimeError::invalid_contract("outer sandbox manifest was invalid"))?;
        let mut arguments: Vec<OsString> = argv.into_iter().map(OsString::from).collect();
        arguments.push("--".into());
        arguments.push(self.native.canonical_path.as_os_str().to_owned());
        arguments.extend(native_app_server_arguments());
        Ok(arguments)
    }
}

fn capture_bwrap() -> Result<ExecutableIdentity, RuntimeError> {
    let executable = ExecutableIdentity::capture(Path::new(BWRAP_EXECUTABLE))
        .map_err(|_| RuntimeError::invalid_identity("bubblewrap executable was unavailable"))?;
    validate_bwrap_version(&executable)?;
    Ok(executable)
}

fn validate_bwrap_version(executable: &ExecutableIdentity) -> Result<(), RuntimeError> {
    executable
        .revalidate()
        .map_err(|_| RuntimeError::invalid_identity("bubblewrap executable drifted"))?;
    let output = Command::new(&executable.canonical_path)
        .arg("--version")
        .env_clear()
        .stdin(Stdio::null())
        .output()
        .map_err(|_| RuntimeError::internal("failed to run bubblewrap version probe"))?;
    if !output.status.success()
        || output.stdout.len() > 4096
        || output.stderr.len() > 4096
        || trim_single_line(&output.stdout) != Some(BWRAP_VERSION)
    {
        return Err(RuntimeError::invalid_contract(
            "bubblewrap version contract mismatch",
        ));
    }
    Ok(())
}

struct NativeSession {
    spec: RoleSessionSpec,
    thread_id: String,
}

struct ActiveTurn {
    key: RuntimeTurnKey,
    thread_id: String,
    turn_id: String,
    spec: RuntimeTurnSpec,
    started: Instant,
}

pub struct CodexAppServerRuntime {
    contract: RuntimeContractIdentity,
    rpc: CodexRpc,
    child: Option<Child>,
    process: Option<ProcessGroupBinding>,
    sessions: BTreeMap<RuntimeSessionKey, NativeSession>,
    native_turn_ids: BTreeSet<String>,
    active: Option<ActiveTurn>,
    next_session: u64,
    next_turn: u64,
    request_timeout: Duration,
    terminate_grace: Duration,
    expected_codex_home: PathBuf,
    shutdown: bool,
}

impl CodexAppServerRuntime {
    fn launch(launch: CodexAppServerLaunch) -> Result<Self, RuntimeError> {
        launch.revalidate()?;
        let contract = launch.contract();
        let expected_codex_home = required_environment_path(&launch.environment, "CODEX_HOME")?;
        let expected_parent = std::process::id();
        let mut command = Command::new(&launch.program);
        command
            .args(&launch.arguments)
            .current_dir(&launch.cwd)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in &launch.environment {
            command.env(name, value);
        }
        // SAFETY: the closure calls only the async-signal-safe child setup
        // shared with existing no-TTY worker processes.
        unsafe {
            command.pre_exec(move || configure_worker_child(expected_parent));
        }
        let mut child = command
            .spawn()
            .map_err(|_| RuntimeError::internal("failed to spawn Codex App Server"))?;
        let process = ProcessGroupBinding::capture(&mut child)
            .map_err(|_| RuntimeError::internal("failed to bind Codex App Server process group"))?;
        let Some(stdin) = child.stdin.take() else {
            let _ = process.terminate_and_reap(&mut child, launch.terminate_grace);
            return Err(RuntimeError::internal(
                "Codex App Server stdin pipe was unavailable",
            ));
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = process.terminate_and_reap(&mut child, launch.terminate_grace);
            return Err(RuntimeError::internal(
                "Codex App Server stdout pipe was unavailable",
            ));
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = process.terminate_and_reap(&mut child, launch.terminate_grace);
            return Err(RuntimeError::internal(
                "Codex App Server stderr pipe was unavailable",
            ));
        };
        let rpc = CodexRpc::new(stdin, stdout, stderr);
        let mut runtime = Self {
            contract,
            rpc,
            child: Some(child),
            process: Some(process),
            sessions: BTreeMap::new(),
            native_turn_ids: BTreeSet::new(),
            active: None,
            next_session: 1,
            next_turn: 1,
            request_timeout: launch.request_timeout,
            terminate_grace: launch.terminate_grace,
            expected_codex_home,
            shutdown: false,
        };
        if let Err(error) = runtime.initialize() {
            let _ = runtime.shutdown();
            return Err(error);
        }
        Ok(runtime)
    }

    pub fn launch_direct(
        preflight: CodexAppServerPreflight,
        cwd: PathBuf,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        Self::launch(CodexAppServerLaunch::direct(preflight, cwd, environment)?)
    }

    pub(crate) fn launch_sandboxed(
        preflight: CodexAppServerPreflight,
        sandbox: CodexAppServerSandboxConfig,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        Self::launch(CodexAppServerLaunch::sandboxed(
            preflight,
            sandbox,
            environment,
        )?)
    }

    #[cfg(test)]
    fn launch_sandboxed_fixture(
        program: PathBuf,
        sandbox: CodexAppServerSandboxConfig,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self, RuntimeError> {
        Self::launch(CodexAppServerLaunch::sandboxed_fixture(
            program,
            sandbox,
            environment,
        )?)
    }

    fn initialize(&mut self) -> Result<(), RuntimeError> {
        let request = self.rpc.send_request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": CLIENT_NAME,
                    "title": CLIENT_TITLE,
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "optOutNotificationMethods": OPT_OUT_NOTIFICATION_METHODS
                }
            }),
        )?;
        let result = self.await_response(request, self.request_timeout)?;
        validate_initialize_response(&result, &self.expected_codex_home)?;
        self.rpc.send_notification("initialized", json!({}))
    }

    fn await_response(
        &mut self,
        expected_id: u64,
        timeout: Duration,
    ) -> Result<Value, RuntimeError> {
        let result = match self.rpc.receive(timeout) {
            Err(failure) => Err(runtime_error_from_failure(failure)),
            Ok(Some(RpcInbound::Response { id, result })) if id == expected_id => Ok(result),
            Ok(Some(RpcInbound::Response { .. })) => Err(RuntimeError::invalid_contract(
                "Codex App Server returned a mismatched response",
            )),
            Ok(Some(RpcInbound::ResponseError { id })) if id == expected_id => Err(
                RuntimeError::invalid_contract("Codex App Server rejected a client request"),
            ),
            Ok(Some(RpcInbound::ResponseError { .. })) => Err(RuntimeError::invalid_contract(
                "Codex App Server returned a mismatched error response",
            )),
            Ok(Some(RpcInbound::ServerRequest { method })) => Err(RuntimeError::invalid_contract(
                format!("Codex App Server issued forbidden server request {method}"),
            )),
            Ok(Some(RpcInbound::TurnCompleted { .. })) => Err(RuntimeError::invalid_contract(
                "Codex App Server completed a turn before its start response",
            )),
            Ok(Some(RpcInbound::Eof)) => Err(RuntimeError::internal(
                "Codex App Server exited before responding",
            )),
            Ok(None) => Err(RuntimeError::internal("Codex App Server request timed out")),
        };
        if result.is_err() {
            self.active.take();
            self.sessions.clear();
            self.rpc.end_turn_window();
            let _ = self.terminate_process();
            self.shutdown = true;
        }
        result
    }

    fn allocate_session_key(&mut self) -> Result<RuntimeSessionKey, RuntimeError> {
        let counter = self.next_session;
        self.next_session = self
            .next_session
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("runtime session key overflow"))?;
        RuntimeSessionKey::from_counter(counter)
    }

    fn allocate_turn_key(&mut self) -> Result<RuntimeTurnKey, RuntimeError> {
        let counter = self.next_turn;
        self.next_turn = self
            .next_turn
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("runtime turn key overflow"))?;
        RuntimeTurnKey::from_counter(counter)
    }

    fn require_open(&self) -> Result<(), RuntimeError> {
        if self.shutdown {
            return Err(RuntimeError::invalid_transition(
                "Codex App Server runtime is shut down",
            ));
        }
        Ok(())
    }

    fn terminate_process(&mut self) -> Result<(), RuntimeError> {
        self.rpc.close_writer();
        let process_result = match (&self.process, &mut self.child) {
            (Some(process), Some(child)) => process
                .terminate_and_reap(child, self.terminate_grace)
                .map_err(|_| RuntimeError::internal("failed to terminate Codex App Server group")),
            _ => Ok(()),
        };
        if process_result.is_err()
            && let Some(child) = self.child.as_mut()
        {
            // The exact unreaped Child handle is still safe to target even if
            // process-group identity validation failed.
            let _ = child.kill();
        }
        self.process.take();
        let reader_result = self.rpc.join_readers(self.terminate_grace);
        if process_result.is_err()
            && let Some(mut child) = self.child.take()
        {
            // Do not turn an uninterruptible child into an unbounded shutdown
            // wait. A detached reaper retains the exact Child handle and
            // collects it if/when the kernel makes it waitable.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        } else {
            self.child.take();
        }
        process_result.and(reader_result)
    }

    fn fail_transport(&mut self, failure: SanitizedRuntimeFailure) -> RuntimeTurnPoll {
        let telemetry = self.rpc.telemetry();
        self.active.take();
        self.sessions.clear();
        let _ = self.terminate_process();
        self.shutdown = true;
        RuntimeTurnPoll::Failed { failure, telemetry }
    }
}

impl TaskWorkerRuntime for CodexAppServerRuntime {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        if self.active.is_some() {
            return Err(RuntimeError::invalid_transition(
                "cannot open a role session while a turn is active",
            ));
        }
        if self
            .sessions
            .values()
            .any(|session| session.spec.role == spec.role)
        {
            return Err(RuntimeError::invalid_transition(
                "task runtime already has a session for this role",
            ));
        }
        if let Some(bound) = self.sessions.values().next()
            && (bound.spec.task_key != spec.task_key || bound.spec.cwd != spec.cwd)
        {
            return Err(RuntimeError::invalid_identity(
                "role sessions in one task runtime must share task and repository identity",
            ));
        }
        let cwd = path_text(&spec.cwd)?;
        let fields = spec.profile.thread_fields();
        let request = match self.rpc.send_request(
            "thread/start",
            json!({
                "cwd": cwd,
                "model": fields.model,
                "approvalPolicy": fields.approval_policy,
                "sandbox": fields.sandbox,
                "ephemeral": true,
                "developerInstructions": spec.developer_instructions,
                "config": {"mcp_servers": {}}
            }),
        ) {
            Ok(request) => request,
            Err(error) => {
                let _ = self.shutdown();
                return Err(error);
            }
        };
        let response = self.await_response(request, self.request_timeout)?;
        let thread_id = match validate_thread_start_response(&response, &spec) {
            Ok(thread_id) => thread_id,
            Err(error) => {
                let _ = self.shutdown();
                return Err(error);
            }
        };
        if self
            .sessions
            .values()
            .any(|session| session.thread_id == thread_id)
        {
            return Err(RuntimeError::invalid_identity(
                "Codex App Server reused a native role thread id",
            ));
        }
        let key = self.allocate_session_key()?;
        self.sessions.insert(key, NativeSession { spec, thread_id });
        Ok(key)
    }

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        if self.active.is_some() {
            return Err(RuntimeError::invalid_transition(
                "task runtime already has an active turn",
            ));
        }
        let role_session = self
            .sessions
            .get(&session)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime session key"))?;
        if role_session.spec.role != spec.role
            || role_session.spec.task_key != spec.task_key
            || role_session.spec.cwd != spec.cwd
            || role_session.spec.profile != spec.profile
        {
            return Err(RuntimeError::invalid_identity(
                "runtime turn does not match its bound role session",
            ));
        }
        let thread_id = role_session.thread_id.clone();
        let cwd = path_text(&spec.cwd)?;
        let fields = spec.profile.turn_fields();
        if let Err(error) = self.rpc.begin_turn_window() {
            let _ = self.shutdown();
            return Err(error);
        }
        let request = match self.rpc.send_request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{"type": "text", "text": spec.prompt}],
                "cwd": cwd,
                "model": fields.model,
                "effort": fields.reasoning_effort,
                "approvalPolicy": fields.approval_policy,
                "sandboxPolicy": {"type": fields.sandbox_policy_type},
                "outputSchema": spec.outcome_contract.json_schema()
            }),
        ) {
            Ok(request) => request,
            Err(error) => {
                let _ = self.shutdown();
                return Err(error);
            }
        };
        let response = self.await_response(request, self.request_timeout)?;
        let turn_id = match validate_turn_start_response(&response) {
            Ok(turn_id) => turn_id,
            Err(error) => {
                let _ = self.shutdown();
                return Err(error);
            }
        };
        if self.native_turn_ids.len() >= MAX_TASK_NATIVE_TURNS {
            let error =
                RuntimeError::invalid_transition("Codex App Server task exceeded 64 native turns");
            let _ = self.shutdown();
            return Err(error);
        }
        if !self.native_turn_ids.insert(turn_id.clone()) {
            let error = RuntimeError::invalid_identity("Codex App Server reused a native turn id");
            let _ = self.shutdown();
            return Err(error);
        }
        let key = self.allocate_turn_key()?;
        self.active = Some(ActiveTurn {
            key,
            thread_id,
            turn_id,
            spec,
            started: Instant::now(),
        });
        Ok(key)
    }

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
        self.require_open()?;
        let Some(active) = &self.active else {
            return Err(RuntimeError::invalid_identity(
                "unknown or terminal runtime turn",
            ));
        };
        if active.key != turn {
            return Err(RuntimeError::invalid_identity(
                "runtime turn key does not identify the active turn",
            ));
        }
        if active.started.elapsed() >= active.spec.timeout {
            let failure = sanitized_failure(
                RuntimeFailureClass::Timeout,
                "Codex App Server turn exceeded its wall-clock timeout",
                false,
            );
            return Ok(self.fail_transport(failure));
        }

        let event = match self.rpc.try_receive() {
            Ok(Some(event)) => event,
            Ok(None) => {
                return Ok(RuntimeTurnPoll::Pending {
                    telemetry: self.rpc.telemetry(),
                });
            }
            Err(failure) => return Ok(self.fail_transport(failure)),
        };
        match event {
            RpcInbound::TurnCompleted { params } => {
                let active = self.active.take().expect("active turn was checked");
                let telemetry = self.rpc.telemetry();
                self.rpc.end_turn_window();
                match extract_terminal_outcome(
                    &params,
                    &active.thread_id,
                    &active.turn_id,
                    active.spec.role,
                    active.spec.outcome_contract,
                ) {
                    Ok(outcome) => Ok(RuntimeTurnPoll::Completed { outcome, telemetry }),
                    Err(failure) if failure.class == RuntimeFailureClass::Protocol => {
                        Ok(self.fail_transport(failure))
                    }
                    Err(failure) => Ok(RuntimeTurnPoll::Failed { failure, telemetry }),
                }
            }
            RpcInbound::ServerRequest { method } => {
                let failure = sanitized_failure(
                    RuntimeFailureClass::Contract,
                    format!("Codex App Server issued forbidden server request {method}"),
                    false,
                );
                Ok(self.fail_transport(failure))
            }
            RpcInbound::Eof => {
                let failure = sanitized_failure(
                    RuntimeFailureClass::Process,
                    "Codex App Server exited while a turn was active",
                    false,
                );
                Ok(self.fail_transport(failure))
            }
            RpcInbound::Response { .. } | RpcInbound::ResponseError { .. } => {
                let failure = sanitized_failure(
                    RuntimeFailureClass::Protocol,
                    "Codex App Server emitted an unexpected response",
                    false,
                );
                Ok(self.fail_transport(failure))
            }
        }
    }

    fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
        self.require_open()?;
        let active = self
            .active
            .take()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown or terminal runtime turn"))?;
        if active.key != turn {
            self.active = Some(active);
            return Err(RuntimeError::invalid_identity(
                "runtime turn key does not identify the active turn",
            ));
        }
        let request_result = self.rpc.send_request(
            "turn/interrupt",
            json!({"threadId": active.thread_id, "turnId": active.turn_id}),
        );
        if let Ok(request) = request_result {
            let _ = self.await_response(request, INTERRUPT_RESPONSE_GRACE);
        }
        self.rpc.end_turn_window();
        self.sessions.clear();
        let cleanup = self.terminate_process();
        self.shutdown = true;
        cleanup
    }

    fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.shutdown {
            return Ok(());
        }
        self.active.take();
        self.sessions.clear();
        let result = self.terminate_process();
        self.shutdown = true;
        result
    }
}

impl Drop for CodexAppServerRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn validate_initialize_response(
    value: &Value,
    expected_codex_home: &Path,
) -> Result<(), RuntimeError> {
    let object = value.as_object().ok_or_else(|| {
        RuntimeError::invalid_contract("Codex initialize result was not an object")
    })?;
    for field in ["codexHome", "platformFamily", "platformOs", "userAgent"] {
        let Some(value) = object.get(field).and_then(Value::as_str) else {
            return Err(RuntimeError::invalid_contract(
                "Codex initialize result omitted a required string field",
            ));
        };
        validate_native_text(value, "Codex initialize field")?;
    }
    if object.get("codexHome").and_then(Value::as_str) != expected_codex_home.to_str() {
        return Err(RuntimeError::invalid_identity(
            "Codex initialize result reported the wrong private CODEX_HOME",
        ));
    }
    Ok(())
}

fn validate_thread_start_response(
    value: &Value,
    spec: &RoleSessionSpec,
) -> Result<String, RuntimeError> {
    let object = value.as_object().ok_or_else(|| {
        RuntimeError::invalid_contract("Codex thread/start result was not an object")
    })?;
    let fields = spec.profile.thread_fields();
    if object.get("approvalPolicy").and_then(Value::as_str) != Some(fields.approval_policy)
        || object.get("cwd").and_then(Value::as_str) != spec.cwd.to_str()
        || object.get("model").and_then(Value::as_str) != Some(fields.model)
        || object
            .get("sandbox")
            .and_then(Value::as_object)
            .and_then(|sandbox| sandbox.get("type"))
            .and_then(Value::as_str)
            != Some("dangerFullAccess")
    {
        return Err(RuntimeError::invalid_contract(
            "Codex thread/start result did not preserve the exact profile",
        ));
    }
    let thread = object
        .get("thread")
        .and_then(Value::as_object)
        .ok_or_else(|| RuntimeError::invalid_contract("Codex thread/start omitted thread"))?;
    if thread.get("cwd").and_then(Value::as_str) != spec.cwd.to_str()
        || thread.get("ephemeral").and_then(Value::as_bool) != Some(true)
    {
        return Err(RuntimeError::invalid_contract(
            "Codex thread/start returned the wrong cwd or persistence mode",
        ));
    }
    let id = thread
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::invalid_contract("Codex thread/start omitted thread id"))?;
    validate_native_id(id)?;
    Ok(id.to_owned())
}

fn validate_turn_start_response(value: &Value) -> Result<String, RuntimeError> {
    let turn = value
        .as_object()
        .and_then(|object| object.get("turn"))
        .and_then(Value::as_object)
        .ok_or_else(|| RuntimeError::invalid_contract("Codex turn/start omitted turn"))?;
    let id = turn
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::invalid_contract("Codex turn/start omitted turn id"))?;
    validate_native_id(id)?;
    if turn.get("status").and_then(Value::as_str) != Some("inProgress")
        || !turn.get("items").is_some_and(Value::is_array)
    {
        return Err(RuntimeError::invalid_contract(
            "Codex turn/start returned an invalid initial turn",
        ));
    }
    Ok(id.to_owned())
}

fn extract_terminal_outcome(
    params: &Value,
    expected_thread: &str,
    expected_turn: &str,
    role: WorkerRole,
    contract: OutcomeContract,
) -> Result<RuntimeOutcome, SanitizedRuntimeFailure> {
    let object = params.as_object().ok_or_else(|| {
        sanitized_failure(
            RuntimeFailureClass::Protocol,
            "Codex turn/completed params were not an object",
            false,
        )
    })?;
    if object.get("threadId").and_then(Value::as_str) != Some(expected_thread) {
        return Err(sanitized_failure(
            RuntimeFailureClass::Protocol,
            "Codex turn/completed thread id mismatched the active turn",
            false,
        ));
    }
    let turn = object
        .get("turn")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            sanitized_failure(
                RuntimeFailureClass::Protocol,
                "Codex turn/completed omitted its turn object",
                false,
            )
        })?;
    if turn.get("id").and_then(Value::as_str) != Some(expected_turn) {
        return Err(sanitized_failure(
            RuntimeFailureClass::Protocol,
            "Codex turn/completed turn id mismatched the active turn",
            false,
        ));
    }
    match turn.get("status").and_then(Value::as_str) {
        Some("failed") => {
            return Err(sanitized_failure(
                RuntimeFailureClass::Process,
                "Codex App Server reported a failed turn",
                false,
            ));
        }
        Some("interrupted") => {
            return Err(sanitized_failure(
                RuntimeFailureClass::Canceled,
                "Codex App Server reported an interrupted turn",
                false,
            ));
        }
        Some("completed") => {}
        _ => {
            return Err(sanitized_failure(
                RuntimeFailureClass::Protocol,
                "Codex turn/completed carried an invalid terminal status",
                false,
            ));
        }
    }
    if let Some(items_view) = turn.get("itemsView")
        && items_view.as_str() != Some("full")
    {
        return Err(sanitized_failure(
            RuntimeFailureClass::Contract,
            "Codex terminal turn items were not a full view",
            role == WorkerRole::Developer,
        ));
    }
    let items = turn.get("items").and_then(Value::as_array).ok_or_else(|| {
        sanitized_failure(
            RuntimeFailureClass::Protocol,
            "Codex terminal turn omitted its item list",
            false,
        )
    })?;
    let mut terminal_text = None;
    for item in items {
        let Some(item) = item.as_object() else {
            continue;
        };
        if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
            continue;
        }
        if item.get("id").and_then(Value::as_str).is_none()
            || item.get("text").and_then(Value::as_str).is_none()
        {
            return Err(sanitized_failure(
                RuntimeFailureClass::Contract,
                "Codex terminal agent message omitted a required field",
                role == WorkerRole::Developer,
            ));
        }
        match item.get("phase") {
            None | Some(Value::Null) => {}
            Some(Value::String(phase)) if phase == "final_answer" => {}
            Some(Value::String(phase)) if phase == "commentary" => continue,
            _ => {
                return Err(sanitized_failure(
                    RuntimeFailureClass::Contract,
                    "Codex terminal agent message had an invalid phase",
                    role == WorkerRole::Developer,
                ));
            }
        }
        if terminal_text
            .replace(item.get("text").and_then(Value::as_str).unwrap())
            .is_some()
        {
            return Err(sanitized_failure(
                RuntimeFailureClass::Contract,
                "Codex terminal turn had ambiguous final agent messages",
                role == WorkerRole::Developer,
            ));
        }
    }
    let terminal_text = terminal_text.ok_or_else(|| {
        sanitized_failure(
            RuntimeFailureClass::Contract,
            "Codex terminal turn omitted a final agent message",
            role == WorkerRole::Developer,
        )
    })?;
    contract
        .parse(terminal_text.as_bytes())
        .map_err(|_| {
            sanitized_failure(
                RuntimeFailureClass::Contract,
                "Codex terminal agent message violated its outcome contract",
                role == WorkerRole::Developer,
            )
        })
        .and_then(|outcome| {
            if outcome.role() == role {
                Ok(outcome)
            } else {
                Err(sanitized_failure(
                    RuntimeFailureClass::Contract,
                    "Codex terminal outcome role mismatched the active session",
                    false,
                ))
            }
        })
}

fn sanitized_failure(
    class: RuntimeFailureClass,
    detail: impl Into<String>,
    retryable: bool,
) -> SanitizedRuntimeFailure {
    SanitizedRuntimeFailure::new(class, detail, retryable).unwrap_or(SanitizedRuntimeFailure {
        class,
        detail: "Codex App Server runtime failure".into(),
        retryable: false,
    })
}

fn runtime_error_from_failure(failure: SanitizedRuntimeFailure) -> RuntimeError {
    match failure.class {
        RuntimeFailureClass::Contract => RuntimeError::invalid_contract(failure.detail),
        RuntimeFailureClass::Protocol => RuntimeError::invalid_contract(failure.detail),
        RuntimeFailureClass::Process
        | RuntimeFailureClass::Timeout
        | RuntimeFailureClass::Canceled => RuntimeError::internal(failure.detail),
    }
}

fn validate_native_id(value: &str) -> Result<(), RuntimeError> {
    validate_native_text(value, "Codex native id")?;
    if value.len() > MAX_NATIVE_ID_BYTES {
        return Err(RuntimeError::invalid_identity(
            "Codex native id exceeded its bound",
        ));
    }
    Ok(())
}

fn validate_native_text(value: &str, label: &str) -> Result<(), RuntimeError> {
    if value.is_empty() || value.contains(['\r', '\n', '\0']) {
        return Err(RuntimeError::invalid_contract(format!(
            "{label} was empty or contained control delimiters"
        )));
    }
    Ok(())
}

fn validate_launch_cwd(cwd: &Path) -> Result<(), RuntimeError> {
    if !cwd.is_absolute() || cwd.to_str().is_none() {
        return Err(RuntimeError::invalid_contract(
            "Codex App Server cwd must be an absolute UTF-8 path",
        ));
    }
    Ok(())
}

fn required_environment_path(
    environment: &[(OsString, OsString)],
    name: &str,
) -> Result<PathBuf, RuntimeError> {
    let mut matches = environment
        .iter()
        .filter(|(candidate, _)| candidate == OsStr::new(name));
    let value = matches.next().map(|(_, value)| value).ok_or_else(|| {
        RuntimeError::invalid_contract(format!("Codex App Server environment omitted {name}"))
    })?;
    if matches.next().is_some() {
        return Err(RuntimeError::invalid_contract(format!(
            "Codex App Server environment duplicated {name}"
        )));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() || path.to_str().is_none() {
        return Err(RuntimeError::invalid_contract(format!(
            "Codex App Server {name} must be an absolute UTF-8 path"
        )));
    }
    Ok(path)
}

fn validate_private_runtime_directory(path: &Path, label: &str) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| RuntimeError::invalid_identity(format!("{label} was unavailable")))?;
    // SAFETY: geteuid has no preconditions.
    let current_uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != current_uid
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(RuntimeError::invalid_identity(format!(
            "{label} was not a current-user private directory"
        )));
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<&str, RuntimeError> {
    path.to_str()
        .ok_or_else(|| RuntimeError::invalid_contract("runtime path was not UTF-8"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StableSchemaFixture {
    cli_version: String,
    executable_sha256: String,
    stable_bundle_exemplar_sha256: String,
    stable_bundle_canonical_sha256: String,
    client_request_methods: Vec<String>,
    client_notification_methods: Vec<String>,
    server_notification_methods: Vec<String>,
    server_request_methods: Vec<String>,
    required_definition_fields: BTreeMap<String, Vec<String>>,
    turn_status_values: Vec<String>,
    message_phase_values: Vec<String>,
    initialize_response_fields: Vec<String>,
}

impl StableSchemaFixture {
    fn validate_constants(&self) -> Result<(), RuntimeError> {
        if self.cli_version != CODEX_APP_SERVER_CLI_VERSION
            || self.executable_sha256 != CODEX_APP_SERVER_EXECUTABLE_SHA256
            || self.stable_bundle_exemplar_sha256 != CODEX_APP_SERVER_SCHEMA_EXEMPLAR_SHA256
            || self.stable_bundle_canonical_sha256 != CODEX_APP_SERVER_SCHEMA_CANONICAL_SHA256
        {
            return Err(RuntimeError::invalid_contract(
                "stable schema fixture identity does not match the compiled pin",
            ));
        }
        Ok(())
    }
}

fn verify_generated_stable_schema(
    schema_dir: &Path,
    fixture: &StableSchemaFixture,
) -> Result<(), RuntimeError> {
    let combined_bytes =
        read_bounded_file(&schema_dir.join("codex_app_server_protocol.v2.schemas.json"))?;
    let combined: Value = serde_json::from_slice(&combined_bytes)
        .map_err(|_| RuntimeError::invalid_contract("generated stable schema was invalid JSON"))?;
    let mut canonical = Vec::new();
    write_canonical_json(&combined, &mut canonical)?;
    canonical.push(b'\n');
    if sha256_hex(&canonical) != CODEX_APP_SERVER_SCHEMA_CANONICAL_SHA256 {
        return Err(RuntimeError::invalid_contract(
            "generated stable schema canonical SHA-256 mismatch",
        ));
    }

    let definitions = combined
        .get("definitions")
        .and_then(Value::as_object)
        .ok_or_else(|| RuntimeError::invalid_contract("stable schema omitted definitions"))?;
    require_methods(
        definitions.get("ClientRequest"),
        &fixture.client_request_methods,
        false,
    )?;
    require_methods(
        definitions.get("ServerNotification"),
        &fixture.server_notification_methods,
        false,
    )?;
    for (definition, required) in &fixture.required_definition_fields {
        let actual = definitions
            .get(definition)
            .and_then(|value| value.get("required"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                RuntimeError::invalid_contract(
                    "stable schema omitted a selected required-field contract",
                )
            })?;
        let actual: Vec<_> = actual.iter().filter_map(Value::as_str).collect();
        let required: Vec<_> = required.iter().map(String::as_str).collect();
        if actual != required {
            return Err(RuntimeError::invalid_contract(
                "stable schema selected required fields drifted",
            ));
        }
    }
    let turn_status = definitions
        .get("TurnStatus")
        .and_then(|value| value.get("enum"))
        .and_then(Value::as_array)
        .ok_or_else(|| RuntimeError::invalid_contract("stable schema omitted TurnStatus"))?;
    if string_array(turn_status) != fixture.turn_status_values {
        return Err(RuntimeError::invalid_contract(
            "stable schema TurnStatus values drifted",
        ));
    }
    let message_phases = collect_enum_strings(
        definitions
            .get("MessagePhase")
            .ok_or_else(|| RuntimeError::invalid_contract("stable schema omitted MessagePhase"))?,
    );
    if message_phases != fixture.message_phase_values {
        return Err(RuntimeError::invalid_contract(
            "stable schema MessagePhase values drifted",
        ));
    }

    let client_notification = parse_bounded_json(&schema_dir.join("ClientNotification.json"))?;
    require_methods(
        Some(&client_notification),
        &fixture.client_notification_methods,
        true,
    )?;
    let server_request = parse_bounded_json(&schema_dir.join("ServerRequest.json"))?;
    require_methods(Some(&server_request), &fixture.server_request_methods, true)?;
    let initialize_response = parse_bounded_json(&schema_dir.join("v1/InitializeResponse.json"))?;
    let actual = initialize_response
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            RuntimeError::invalid_contract("stable schema omitted initialize response fields")
        })?;
    if string_array(actual) != fixture.initialize_response_fields {
        return Err(RuntimeError::invalid_contract(
            "stable schema initialize response fields drifted",
        ));
    }
    Ok(())
}

fn require_methods(
    schema: Option<&Value>,
    expected: &[String],
    exact: bool,
) -> Result<(), RuntimeError> {
    let schema = schema
        .ok_or_else(|| RuntimeError::invalid_contract("stable schema omitted a method union"))?;
    let methods = collect_method_strings(schema);
    if (exact && methods != expected)
        || (!exact && expected.iter().any(|item| !methods.contains(item)))
    {
        return Err(RuntimeError::invalid_contract(
            "stable schema method inventory drifted",
        ));
    }
    Ok(())
}

fn collect_method_strings(value: &Value) -> Vec<String> {
    let mut methods = Vec::new();
    collect_named_enum(value, "method", &mut methods);
    methods
}

fn collect_named_enum(value: &Value, property: &str, output: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(values) = object
                .get("properties")
                .and_then(|properties| properties.get(property))
                .and_then(|property| property.get("enum"))
                .and_then(Value::as_array)
            {
                output.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
            }
            for child in object.values() {
                collect_named_enum(child, property, output);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_named_enum(child, property, output);
            }
        }
        _ => {}
    }
}

fn collect_enum_strings(value: &Value) -> Vec<String> {
    let mut output = Vec::new();
    match value {
        Value::Object(object) => {
            if let Some(values) = object.get("enum").and_then(Value::as_array) {
                output.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
            }
            for child in object.values() {
                output.extend(collect_enum_strings(child));
            }
        }
        Value::Array(values) => {
            for child in values {
                output.extend(collect_enum_strings(child));
            }
        }
        _ => {}
    }
    output
}

fn string_array(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn run_exact_command<I, S>(
    label: &'static str,
    executable: &ExecutableIdentity,
    home: &Path,
    codex_home: &Path,
    temporary: &Path,
    arguments: I,
) -> Result<Vec<u8>, RuntimeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    executable
        .revalidate()
        .map_err(|_| RuntimeError::invalid_identity("pinned Codex executable drifted"))?;
    let output = Command::new(&executable.canonical_path)
        .args(arguments)
        .env_clear()
        .env("HOME", home)
        .env("CODEX_HOME", codex_home)
        .env("TMPDIR", temporary)
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::null())
        .output()
        .map_err(|_| RuntimeError::internal("failed to run Codex contract preflight"))?;
    if !output.status.success()
        || output.stdout.len() > MAX_PREFLIGHT_OUTPUT_BYTES
        || output.stderr.len() > MAX_PREFLIGHT_OUTPUT_BYTES
    {
        return Err(RuntimeError::invalid_contract(format!(
            "Codex {label} preflight failed or exceeded its bound"
        )));
    }
    Ok(output.stdout)
}

fn trim_single_line(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    if text.is_empty() || text.contains(['\r', '\n', '\0']) {
        return None;
    }
    Some(text)
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<(), RuntimeError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| RuntimeError::internal("failed to create private Codex config"))?;
    file.write_all(contents)
        .and_then(|()| file.flush())
        .map_err(|_| RuntimeError::internal("failed to write private Codex config"))
}

fn read_bounded_file(path: &Path) -> Result<Vec<u8>, RuntimeError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| RuntimeError::invalid_contract("generated schema file was missing"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_PREFLIGHT_OUTPUT_BYTES as u64
    {
        return Err(RuntimeError::invalid_contract(
            "generated schema file identity or size was invalid",
        ));
    }
    fs::read(path).map_err(|_| RuntimeError::internal("failed to read generated schema"))
}

fn parse_bounded_json(path: &Path) -> Result<Value, RuntimeError> {
    serde_json::from_slice(&read_bounded_file(path)?)
        .map_err(|_| RuntimeError::invalid_contract("generated schema file was invalid JSON"))
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<(), RuntimeError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => serde_json::to_writer(output, value)
            .map_err(|_| RuntimeError::internal("failed to canonicalize stable schema"))?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let sorted: BTreeMap<_, _> = values.iter().collect();
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)
                    .map_err(|_| RuntimeError::internal("failed to canonicalize stable schema"))?;
                output.push(b':');
                write_canonical_json(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
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

fn native_app_server_arguments() -> Vec<OsString> {
    let mut arguments: Vec<OsString> = [
        "app-server",
        "--stdio",
        "--strict-config",
        "--config",
        "mcp_servers={}",
        "--config",
        "shell_environment_policy.inherit=\"all\"",
        "--config",
        "shell_environment_policy.ignore_default_excludes=true",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    for feature in DISABLED_CODEX_FEATURES {
        arguments.extend([OsString::from("--disable"), OsString::from(feature)]);
    }
    arguments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::codex_rpc::{
        MAX_RPC_LINE_BYTES, MAX_STDERR_TAIL_BYTES, MAX_UNKNOWN_NOTIFICATIONS,
    };
    use crate::worker::runtime::{
        DeveloperOutcomeStatus, ReviewerVerdict, RuntimeErrorCode, RuntimeProfile,
        RuntimeTurnPurpose,
    };
    use std::os::unix::ffi::OsStringExt;
    use std::process::Command;
    use std::thread;

    struct FixtureRuntime {
        runtime: CodexAppServerRuntime,
        root: tempfile::TempDir,
        report: PathBuf,
        descendant_pid: PathBuf,
    }

    impl FixtureRuntime {
        fn launch(scenario: &str, request_method: Option<&str>) -> Self {
            Self::try_launch(scenario, request_method).unwrap()
        }

        fn try_launch(scenario: &str, request_method: Option<&str>) -> Result<Self, RuntimeError> {
            let root = tempfile::tempdir().unwrap();
            let cwd = fs::canonicalize(root.path()).unwrap();
            let report = cwd.join("wire-report.jsonl");
            let descendant_pid = cwd.join("descendant.pid");
            let codex_home = cwd.join("codex-home");
            let private_tmp = cwd.join("private-tmp");
            fs::create_dir(&codex_home).unwrap();
            fs::create_dir(&private_tmp).unwrap();
            fs::set_permissions(&codex_home, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&private_tmp, fs::Permissions::from_mode(0o700)).unwrap();
            let script = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/fake_codex_app_server.py");
            let mut environment = vec![
                (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
                (
                    OsString::from("CODEX_HOME"),
                    codex_home.as_os_str().to_owned(),
                ),
                (OsString::from("TMPDIR"), private_tmp.as_os_str().to_owned()),
                (
                    OsString::from("PYTHONDONTWRITEBYTECODE"),
                    OsString::from("1"),
                ),
                (
                    OsString::from("HCOM_FAKE_CODEX_SCENARIO"),
                    OsString::from(scenario),
                ),
                (
                    OsString::from("HCOM_FAKE_CODEX_REPORT"),
                    report.as_os_str().to_owned(),
                ),
                (
                    OsString::from("HCOM_FAKE_CODEX_DESCENDANT_PID"),
                    descendant_pid.as_os_str().to_owned(),
                ),
            ];
            if let Some(method) = request_method {
                environment.push((
                    OsString::from("HCOM_FAKE_CODEX_REQUEST_METHOD"),
                    OsString::from(method),
                ));
            }
            if scenario == "environment" {
                environment.extend([
                    (
                        OsString::from("UNKNOWN_PARENT_VALUE"),
                        OsString::from("unknown-value"),
                    ),
                    (
                        OsString::from("SERVICE_ACCESS_TOKEN"),
                        OsString::from("environment-secret-sentinel"),
                    ),
                    (OsString::from("EMPTY_PARENT_VALUE"), OsString::new()),
                    (OsString::from("CASE_PAIR"), OsString::from("upper")),
                    (OsString::from("case_pair"), OsString::from("lower")),
                    (
                        OsString::from("HTTP_PROXY"),
                        OsString::from("http://proxy.example"),
                    ),
                    (
                        OsString::from("https_proxy"),
                        OsString::from("http://lower-proxy.example"),
                    ),
                    (
                        OsString::from_vec(b"RAW_\xff_NAME".to_vec()),
                        OsString::from_vec(b"value-\xfe".to_vec()),
                    ),
                ]);
            }
            if let Some(pid_file) = std::env::var_os("HCOM_P2_PARENT_EXIT_PID_FILE") {
                environment.push((OsString::from("HCOM_FAKE_CODEX_SERVER_PID"), pid_file));
            }
            let launch = CodexAppServerLaunch::fixture(
                PathBuf::from("/usr/bin/python3"),
                vec![script.into_os_string()],
                cwd,
                environment,
            );
            let runtime = CodexAppServerRuntime::launch(launch)?;
            Ok(Self {
                runtime,
                root,
                report,
                descendant_pid,
            })
        }

        fn cwd(&self) -> PathBuf {
            fs::canonicalize(self.root.path()).unwrap()
        }

        fn session_spec(&self, role: WorkerRole) -> RoleSessionSpec {
            RoleSessionSpec {
                role,
                task_key: "task-1".into(),
                cwd: self.cwd(),
                profile: RuntimeProfile::codex_app_server_default(),
                developer_instructions: match role {
                    WorkerRole::Developer => "closed developer instructions",
                    WorkerRole::Reviewer => "closed reviewer instructions",
                }
                .into(),
            }
        }

        fn turn_spec(&self, role: WorkerRole, purpose: RuntimeTurnPurpose) -> RuntimeTurnSpec {
            RuntimeTurnSpec {
                role,
                task_key: "task-1".into(),
                purpose,
                cwd: self.cwd(),
                prompt: "PROMPT_SECRET_SENTINEL".into(),
                profile: RuntimeProfile::codex_app_server_default(),
                outcome_contract: match role {
                    WorkerRole::Developer => OutcomeContract::DeveloperV1,
                    WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
                },
                timeout: Duration::from_secs(5),
            }
        }

        fn open(&mut self, role: WorkerRole) -> RuntimeSessionKey {
            let spec = self.session_spec(role);
            self.runtime.open_session(spec).unwrap()
        }

        fn start(
            &mut self,
            session: RuntimeSessionKey,
            role: WorkerRole,
            purpose: RuntimeTurnPurpose,
        ) -> RuntimeTurnKey {
            let spec = self.turn_spec(role, purpose);
            self.runtime.start_turn(session, spec).unwrap()
        }

        fn poll_terminal(&mut self, turn: RuntimeTurnKey) -> RuntimeTurnPoll {
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                let poll = self.runtime.poll_turn(turn).unwrap();
                if poll.is_terminal() {
                    return poll;
                }
                assert!(Instant::now() < deadline, "fake App Server turn timed out");
                thread::sleep(Duration::from_millis(1));
            }
        }

        fn report(&self) -> Vec<Value> {
            fs::read_to_string(&self.report)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
    }

    #[test]
    fn exact_binary_preflight_uses_the_checked_in_stable_schema_fixture() {
        let preflight = CodexAppServerPreflight::verify_pinned().unwrap();
        assert_eq!(
            preflight.executable().canonical_path,
            Path::new(CODEX_APP_SERVER_EXECUTABLE)
        );
        assert_eq!(
            preflight.executable().sha256,
            CODEX_APP_SERVER_EXECUTABLE_SHA256
        );
        assert_eq!(
            preflight.contract().schema_canonical_sha256,
            CODEX_APP_SERVER_SCHEMA_CANONICAL_SHA256
        );
        preflight.revalidate().unwrap();
    }

    #[test]
    fn fresh_role_threads_same_thread_followup_and_exact_wire_fields() {
        let mut fixture = FixtureRuntime::launch("happy", None);
        let developer = fixture.open(WorkerRole::Developer);
        let reviewer = fixture.open(WorkerRole::Reviewer);
        assert_ne!(developer, reviewer);

        let first = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        match fixture.poll_terminal(first) {
            RuntimeTurnPoll::Completed {
                outcome: RuntimeOutcome::Developer(outcome),
                ..
            } => {
                assert_eq!(outcome.status, DeveloperOutcomeStatus::Ready);
                assert!(outcome.questions.is_empty());
            }
            _ => panic!("developer fixture did not complete"),
        }
        let correction = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
        );
        assert!(fixture.poll_terminal(correction).is_terminal());
        let review = fixture.start(
            reviewer,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        match fixture.poll_terminal(review) {
            RuntimeTurnPoll::Completed {
                outcome: RuntimeOutcome::Reviewer(outcome),
                ..
            } => assert_eq!(outcome.verdict, ReviewerVerdict::Lgtm),
            _ => panic!("reviewer fixture did not complete"),
        }

        let report = fixture.report();
        let initialize = report
            .iter()
            .find(|entry| entry["method"] == "initialize")
            .unwrap();
        assert_eq!(initialize["clientInfo"]["name"], CLIENT_NAME);
        assert_eq!(
            initialize["capabilities"]["optOutNotificationMethods"],
            json!(OPT_OUT_NOTIFICATION_METHODS)
        );
        assert!(initialize["capabilities"].get("experimentalApi").is_none());
        let threads: Vec<_> = report
            .iter()
            .filter(|entry| entry["method"] == "thread/start")
            .collect();
        assert_eq!(threads.len(), 2);
        for entry in threads {
            assert_eq!(entry["model"], "gpt-5.6-sol");
            assert_eq!(entry["approvalPolicy"], "never");
            assert_eq!(entry["sandbox"], "danger-full-access");
            assert_eq!(entry["ephemeral"], true);
            assert_eq!(entry["config"], json!({"mcp_servers": {}}));
        }
        let turns: Vec<_> = report
            .iter()
            .filter(|entry| entry["method"] == "turn/start")
            .collect();
        assert_eq!(turns.len(), 3);
        assert_eq!(turns[0]["threadId"], turns[1]["threadId"]);
        assert_ne!(turns[0]["threadId"], turns[2]["threadId"]);
        for entry in turns {
            assert_eq!(entry["model"], "gpt-5.6-sol");
            assert_eq!(entry["effort"], "xhigh");
            assert_eq!(entry["approvalPolicy"], "never");
            assert_eq!(entry["sandboxPolicy"], json!({"type":"dangerFullAccess"}));
            assert_eq!(entry["inputTypes"], json!(["text"]));
            assert_eq!(entry["promptBytes"], "PROMPT_SECRET_SENTINEL".len());
            assert!(entry["outputSchema"].is_object());
        }
        let report_text = fs::read_to_string(&fixture.report).unwrap();
        assert!(!report_text.contains("PROMPT_SECRET_SENTINEL"));
        fixture.runtime.shutdown().unwrap();
    }

    #[test]
    fn native_environment_preserves_unknown_empty_case_paired_proxy_and_non_utf8_entries() {
        let mut fixture = FixtureRuntime::launch("environment", None);
        let developer = fixture.open(WorkerRole::Developer);
        let turn = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        assert!(matches!(
            fixture.poll_terminal(turn),
            RuntimeTurnPoll::Completed { .. }
        ));
        let report = fixture.report();
        let environment = report
            .iter()
            .find(|entry| entry["method"] == "fixture/environment")
            .unwrap();
        for field in [
            "unknown",
            "secretShaped",
            "empty",
            "casePair",
            "nonUtf8",
            "proxyPair",
        ] {
            assert_eq!(environment[field], true, "environment field {field}");
        }
        let report = fs::read_to_string(&fixture.report).unwrap();
        assert!(!report.contains("environment-secret-sentinel"));
        assert!(!report.contains("proxy.example"));
        fixture.runtime.shutdown().unwrap();
    }

    #[test]
    fn real_outer_sandbox_is_writable_for_both_roles_and_hides_unbound_host_state() {
        let root = tempfile::tempdir().unwrap();
        let root_path = fs::canonicalize(root.path()).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let repository = root_path.join("repository");
        let home = root_path.join("private-home");
        let codex_home = home.join(".codex");
        let temp = root_path.join("private-tmp");
        let runtime_dir = root_path.join("private-run");
        let artifacts = root_path.join("artifacts");
        let hcom_dir = root_path.join("private-hcom");
        let xdg_config = home.join(".config");
        let xdg_state = home.join(".state");
        let xdg_cache = home.join(".cache");
        let xdg_data = home.join(".data");
        let cargo_bin = root_path.join("cargo-bin");
        let rustup_home = root_path.join("rustup-home");
        let protected = root_path.join("unbound-protected");
        for directory in [
            &repository,
            &home,
            &codex_home,
            &temp,
            &runtime_dir,
            &artifacts,
            &hcom_dir,
            &xdg_config,
            &xdg_state,
            &xdg_cache,
            &xdg_data,
            &cargo_bin,
            &rustup_home,
            &protected,
        ] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let git = |arguments: &[&str]| {
            let output = Command::new("/usr/bin/git")
                .args(arguments)
                .current_dir(&repository)
                .env_clear()
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("HOME", "/nonexistent")
                .env("LC_ALL", "C")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {:?} failed: {}",
                arguments,
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-b", "master"]);
        fs::write(repository.join(".gitignore"), "target/\n").unwrap();
        fs::write(repository.join("seed.txt"), "seed\n").unwrap();
        git(&["add", "--", ".gitignore", "seed.txt"]);
        let commit = Command::new("/usr/bin/git")
            .args([
                "-c",
                "user.name=Sandbox Fixture",
                "-c",
                "user.email=sandbox@example.invalid",
                "commit",
                "-m",
                "Initial fixture",
            ])
            .current_dir(&repository)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(commit.status.success());

        let auth_source = root_path.join("native-auth.json");
        let auth_target = codex_home.join("auth.json");
        let config = codex_home.join("config.toml");
        let protected_file = protected.join("live-control-secret");
        for (path, contents) in [
            (&auth_source, b"{\"token\":\"fixture-secret\"}".as_slice()),
            (&auth_target, b"placeholder".as_slice()),
            (
                &config,
                b"mcp_servers = {}\n[shell_environment_policy]\ninherit = \"all\"\nignore_default_excludes = true\n"
                    .as_slice(),
            ),
            (&protected_file, b"must remain hidden".as_slice()),
        ] {
            fs::write(path, contents).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let fixture_program = root_path.join("fake-app-server.py");
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_codex_app_server.py"),
            &fixture_program,
        )
        .unwrap();
        fs::set_permissions(&fixture_program, fs::Permissions::from_mode(0o700)).unwrap();
        let report = artifacts.join("sandbox-report.jsonl");
        let environment = vec![
            (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
            (OsString::from("HOME"), home.as_os_str().to_owned()),
            (
                OsString::from("CODEX_HOME"),
                codex_home.as_os_str().to_owned(),
            ),
            (OsString::from("TMPDIR"), temp.as_os_str().to_owned()),
            (
                OsString::from("XDG_RUNTIME_DIR"),
                runtime_dir.as_os_str().to_owned(),
            ),
            (
                OsString::from("XDG_CONFIG_HOME"),
                xdg_config.as_os_str().to_owned(),
            ),
            (
                OsString::from("XDG_STATE_HOME"),
                xdg_state.as_os_str().to_owned(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                xdg_cache.as_os_str().to_owned(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                xdg_data.as_os_str().to_owned(),
            ),
            (OsString::from("HCOM_DIR"), hcom_dir.as_os_str().to_owned()),
            (
                OsString::from("HCOM_FAKE_CODEX_SCENARIO"),
                OsString::from("sandbox_writable"),
            ),
            (
                OsString::from("HCOM_FAKE_CODEX_REPORT"),
                report.as_os_str().to_owned(),
            ),
            (
                OsString::from("HCOM_FAKE_PROTECTED_PATH"),
                protected_file.as_os_str().to_owned(),
            ),
            (
                OsString::from("PYTHONDONTWRITEBYTECODE"),
                OsString::from("1"),
            ),
        ];
        let mut runtime = CodexAppServerRuntime::launch_sandboxed_fixture(
            fs::canonicalize(&fixture_program).unwrap(),
            CodexAppServerSandboxConfig {
                repository_root: fs::canonicalize(&repository).unwrap(),
                isolated_home: fs::canonicalize(&home).unwrap(),
                codex_home: fs::canonicalize(&codex_home).unwrap(),
                temp_dir: fs::canonicalize(&temp).unwrap(),
                runtime_dir: fs::canonicalize(&runtime_dir).unwrap(),
                artifact_dir: fs::canonicalize(&artifacts).unwrap(),
                hcom_dir: fs::canonicalize(&hcom_dir).unwrap(),
                xdg_config_home: fs::canonicalize(&xdg_config).unwrap(),
                xdg_state_home: fs::canonicalize(&xdg_state).unwrap(),
                xdg_cache_home: fs::canonicalize(&xdg_cache).unwrap(),
                xdg_data_home: fs::canonicalize(&xdg_data).unwrap(),
                auth_source: fs::canonicalize(&auth_source).unwrap(),
                cargo_bin_source: fs::canonicalize(&cargo_bin).unwrap(),
                rustup_home_source: fs::canonicalize(&rustup_home).unwrap(),
            },
            environment,
        )
        .unwrap();
        let session_spec = |role| RoleSessionSpec {
            role,
            task_key: "sandbox-task".into(),
            cwd: fs::canonicalize(&repository).unwrap(),
            profile: RuntimeProfile::codex_app_server_default(),
            developer_instructions: "closed role contract".into(),
        };
        let developer = runtime
            .open_session(session_spec(WorkerRole::Developer))
            .unwrap();
        let reviewer = runtime
            .open_session(session_spec(WorkerRole::Reviewer))
            .unwrap();
        for (session, role, purpose) in [
            (
                developer,
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            ),
            (
                reviewer,
                WorkerRole::Reviewer,
                RuntimeTurnPurpose::InitialReview,
            ),
        ] {
            let turn = runtime
                .start_turn(
                    session,
                    RuntimeTurnSpec {
                        role,
                        task_key: "sandbox-task".into(),
                        purpose,
                        cwd: fs::canonicalize(&repository).unwrap(),
                        prompt: "bounded sandbox probe".into(),
                        profile: RuntimeProfile::codex_app_server_default(),
                        outcome_contract: match role {
                            WorkerRole::Developer => OutcomeContract::DeveloperV1,
                            WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
                        },
                        timeout: Duration::from_secs(5),
                    },
                )
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                match runtime.poll_turn(turn).unwrap() {
                    RuntimeTurnPoll::Pending { .. } if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    RuntimeTurnPoll::Pending { .. } => panic!("sandboxed fixture turn timed out"),
                    RuntimeTurnPoll::Completed { .. } => break,
                    RuntimeTurnPoll::Failed { failure, .. } => {
                        panic!("sandboxed fixture turn failed: {}", failure.detail)
                    }
                }
            }
        }
        runtime.shutdown().unwrap();
        assert_eq!(
            fs::read_to_string(repository.join("target/sandbox-turn-1")).unwrap(),
            "writable\n"
        );
        assert_eq!(
            fs::read_to_string(repository.join("target/sandbox-turn-2")).unwrap(),
            "writable\n"
        );
        let report = fs::read_to_string(report).unwrap();
        let sandbox_records: Vec<Value> = report
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|record: &Value| record["method"] == "fixture/sandbox")
            .collect();
        assert_eq!(sandbox_records.len(), 2);
        assert!(
            sandbox_records
                .iter()
                .all(|record| record["protectedVisible"] == false)
        );
        assert!(
            sandbox_records
                .iter()
                .all(|record| record["stdioIsTty"] == false)
        );
        assert!(
            sandbox_records
                .iter()
                .all(|record| record["controllingTty"] == false)
        );
    }

    #[test]
    fn launch_requires_unique_current_user_private_codex_home_and_tmpdir() {
        let root = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(root.path()).unwrap();
        let codex_home = cwd.join("codex-home");
        let private_tmp = cwd.join("private-tmp");
        for directory in [&codex_home, &private_tmp] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let valid = vec![
            (
                OsString::from("CODEX_HOME"),
                codex_home.as_os_str().to_owned(),
            ),
            (OsString::from("TMPDIR"), private_tmp.as_os_str().to_owned()),
        ];
        CodexAppServerLaunch::fixture(
            PathBuf::from("/usr/bin/true"),
            Vec::new(),
            cwd.clone(),
            valid.clone(),
        )
        .revalidate()
        .unwrap();

        let mut duplicate = valid.clone();
        duplicate.push((
            OsString::from("CODEX_HOME"),
            codex_home.as_os_str().to_owned(),
        ));
        assert!(
            CodexAppServerLaunch::fixture(
                PathBuf::from("/usr/bin/true"),
                Vec::new(),
                cwd.clone(),
                duplicate,
            )
            .revalidate()
            .is_err()
        );

        fs::set_permissions(&private_tmp, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            CodexAppServerLaunch::fixture(PathBuf::from("/usr/bin/true"), Vec::new(), cwd, valid,)
                .revalidate()
                .is_err()
        );
    }

    #[test]
    fn partial_reads_and_bounded_high_volume_pipes_cannot_deadlock() {
        for scenario in [
            "partial",
            "interleaved",
            "stdout_backpressure",
            "stderr_saturation",
            "unknown_notifications_exact",
            "line_below",
            "line_exact",
        ] {
            let mut fixture = FixtureRuntime::launch(scenario, None);
            let developer = fixture.open(WorkerRole::Developer);
            let turn = fixture.start(
                developer,
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            );
            let poll = fixture.poll_terminal(turn);
            match poll {
                RuntimeTurnPoll::Completed { outcome, telemetry } => {
                    if scenario == "stdout_backpressure" {
                        assert!(matches!(
                            outcome,
                            RuntimeOutcome::Developer(ref developer)
                                if developer.status == DeveloperOutcomeStatus::Ready
                        ));
                    }
                    if scenario == "stderr_saturation" {
                        assert!(telemetry.stderr_bytes > MAX_STDERR_TAIL_BYTES as u64);
                    }
                    if scenario == "unknown_notifications_exact" {
                        assert_eq!(telemetry.notification_count, MAX_UNKNOWN_NOTIFICATIONS + 1);
                    }
                    if matches!(scenario, "line_below" | "line_exact") {
                        assert!(telemetry.protocol_bytes > MAX_RPC_LINE_BYTES as u64);
                    }
                }
                _ => panic!("bounded scenario {scenario} did not complete"),
            }
            fixture.runtime.shutdown().unwrap();
        }
    }

    #[test]
    fn malformed_ids_eof_and_bound_violations_fail_closed_without_raw_output() {
        for scenario in [
            "duplicate_response",
            "malformed",
            "malformed_envelope",
            "eof",
            "exit_nonzero",
            "unknown_notifications_over",
            "line_oversized",
        ] {
            let mut fixture = FixtureRuntime::launch(scenario, None);
            let developer = fixture.open(WorkerRole::Developer);
            let spec = fixture.turn_spec(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            );
            match fixture.runtime.start_turn(developer, spec) {
                Ok(turn) => match fixture.poll_terminal(turn) {
                    RuntimeTurnPoll::Failed { failure, .. } => {
                        assert!(!failure.detail.contains("SECRET_SENTINEL"));
                    }
                    _ => panic!("protocol scenario {scenario} did not fail"),
                },
                Err(error) => assert!(!error.detail.contains("SECRET_SENTINEL")),
            }
        }

        let mut fixture = FixtureRuntime::launch("stale_response", None);
        let developer = fixture.open(WorkerRole::Developer);
        let spec = fixture.turn_spec(
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let error = fixture.runtime.start_turn(developer, spec).unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::InvalidContract);
        assert!(!error.detail.contains("SECRET_SENTINEL"));
    }

    #[test]
    fn initialize_thread_and_turn_error_shapes_fail_closed() {
        for scenario in [
            "initialize_error",
            "initialize_timeout",
            "initialize_server_request",
            "wrong_codex_home",
        ] {
            let error = match FixtureRuntime::try_launch(scenario, None) {
                Ok(_) => panic!("initialize scenario {scenario} unexpectedly launched"),
                Err(error) => error,
            };
            assert!(!error.detail.contains("SECRET_SENTINEL"));
        }

        for scenario in ["thread_error", "thread_missing_id"] {
            let mut fixture = FixtureRuntime::launch(scenario, None);
            let spec = fixture.session_spec(WorkerRole::Developer);
            let error = fixture.runtime.open_session(spec).unwrap_err();
            assert!(!error.detail.contains("SECRET_SENTINEL"));
            assert!(fixture.runtime.shutdown);
        }

        let mut fixture = FixtureRuntime::launch("thread_duplicate_id", None);
        fixture.open(WorkerRole::Developer);
        let reviewer = fixture.session_spec(WorkerRole::Reviewer);
        let error = fixture.runtime.open_session(reviewer).unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::InvalidIdentity);

        for scenario in ["turn_error", "turn_missing_id"] {
            let mut fixture = FixtureRuntime::launch(scenario, None);
            let developer = fixture.open(WorkerRole::Developer);
            let spec = fixture.turn_spec(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            );
            let error = fixture.runtime.start_turn(developer, spec).unwrap_err();
            assert!(!error.detail.contains("SECRET_SENTINEL"));
            assert!(fixture.runtime.shutdown);
        }

        let mut fixture = FixtureRuntime::launch("turn_duplicate_id", None);
        let developer = fixture.open(WorkerRole::Developer);
        let first = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        assert!(fixture.poll_terminal(first).is_terminal());
        let follow_up = fixture.turn_spec(
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
        );
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, follow_up)
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
    }

    #[test]
    fn role_task_repository_profile_and_in_flight_bindings_cannot_cross() {
        let mut fixture = FixtureRuntime::launch("pending", None);
        let developer = fixture.open(WorkerRole::Developer);

        let mut wrong_role =
            fixture.turn_spec(WorkerRole::Reviewer, RuntimeTurnPurpose::InitialReview);
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, wrong_role.clone())
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
        wrong_role.role = WorkerRole::Developer;
        wrong_role.purpose = RuntimeTurnPurpose::InitialDevelopment;
        wrong_role.outcome_contract = OutcomeContract::DeveloperV1;
        wrong_role.task_key = "task-2".into();
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, wrong_role.clone())
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
        wrong_role.task_key = "task-1".into();
        wrong_role.cwd = fixture.cwd().join("other-repository");
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, wrong_role.clone())
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
        wrong_role.cwd = fixture.cwd();
        wrong_role.profile.model = "different-model".into();
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, wrong_role)
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );

        let turn = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let concurrent = fixture.turn_spec(
            WorkerRole::Developer,
            RuntimeTurnPurpose::DeveloperCorrection,
        );
        assert_eq!(
            fixture
                .runtime
                .start_turn(developer, concurrent)
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidTransition
        );
        fixture.runtime.cancel_turn(turn).unwrap();

        let mut fixture = FixtureRuntime::launch("happy", None);
        fixture.open(WorkerRole::Developer);
        let duplicate = fixture.session_spec(WorkerRole::Developer);
        assert_eq!(
            fixture.runtime.open_session(duplicate).unwrap_err().code,
            RuntimeErrorCode::InvalidTransition
        );
        let mut cross_task = fixture.session_spec(WorkerRole::Reviewer);
        cross_task.task_key = "task-2".into();
        assert_eq!(
            fixture.runtime.open_session(cross_task).unwrap_err().code,
            RuntimeErrorCode::InvalidIdentity
        );
    }

    #[test]
    fn every_server_request_class_and_unknown_request_terminate_the_runtime() {
        for method in [
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/tool/requestUserInput",
            "mcpServer/elicitation/request",
            "item/permissions/requestApproval",
            "item/tool/call",
            "account/chatgptAuthTokens/refresh",
            "attestation/generate",
            "applyPatchApproval",
            "execCommandApproval",
            "future/unknownRequest",
            "unsafe method\nSECRET_SENTINEL",
        ] {
            let mut fixture = FixtureRuntime::launch("server_request", Some(method));
            let developer = fixture.open(WorkerRole::Developer);
            let turn = fixture.start(
                developer,
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            );
            match fixture.poll_terminal(turn) {
                RuntimeTurnPoll::Failed { failure, .. } => {
                    assert_eq!(failure.class, RuntimeFailureClass::Contract);
                    assert!(!failure.retryable);
                    assert!(!failure.detail.contains("SECRET_SENTINEL"));
                }
                _ => panic!("server request {method:?} was not rejected"),
            }
            let reviewer = fixture.session_spec(WorkerRole::Reviewer);
            assert!(fixture.runtime.open_session(reviewer).is_err());
        }
    }

    #[test]
    fn terminal_identity_status_and_outcome_failures_are_typed() {
        for (scenario, class, retryable) in [
            ("wrong_thread", RuntimeFailureClass::Protocol, false),
            ("wrong_turn", RuntimeFailureClass::Protocol, false),
            ("missing_final", RuntimeFailureClass::Contract, true),
            ("ambiguous_final", RuntimeFailureClass::Contract, true),
            ("invalid_outcome", RuntimeFailureClass::Contract, true),
            ("failed", RuntimeFailureClass::Process, false),
            ("interrupted", RuntimeFailureClass::Canceled, false),
        ] {
            let mut fixture = FixtureRuntime::launch(scenario, None);
            let developer = fixture.open(WorkerRole::Developer);
            let turn = fixture.start(
                developer,
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
            );
            match fixture.poll_terminal(turn) {
                RuntimeTurnPoll::Failed { failure, .. } => {
                    assert_eq!(failure.class, class, "scenario {scenario}");
                    assert_eq!(failure.retryable, retryable, "scenario {scenario}");
                }
                _ => panic!("terminal scenario {scenario} did not fail"),
            }
            if retryable {
                let recovery = fixture.start(
                    developer,
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCompletionRecovery,
                );
                assert!(fixture.poll_terminal(recovery).is_terminal());
            }
        }

        let mut fixture = FixtureRuntime::launch("missing_final", None);
        let reviewer = fixture.open(WorkerRole::Reviewer);
        let turn = fixture.start(
            reviewer,
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
        );
        match fixture.poll_terminal(turn) {
            RuntimeTurnPoll::Failed { failure, .. } => assert!(!failure.retryable),
            _ => panic!("missing reviewer result did not fail"),
        }
    }

    #[test]
    fn interrupt_timeout_kills_the_entire_owned_process_group() {
        let mut fixture = FixtureRuntime::launch("descendant", None);
        let developer = fixture.open(WorkerRole::Developer);
        let turn = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while !fixture.descendant_pid.exists() {
            assert!(Instant::now() < deadline, "descendant pid was not reported");
            thread::sleep(Duration::from_millis(5));
        }
        let descendant: u32 = fs::read_to_string(&fixture.descendant_pid)
            .unwrap()
            .parse()
            .unwrap();
        assert!(Path::new(&format!("/proc/{descendant}")).exists());
        fixture.runtime.cancel_turn(turn).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Path::new(&format!("/proc/{descendant}")).exists() {
            assert!(
                Instant::now() < deadline,
                "owned App Server descendant survived cancellation"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn parent_exit_helper_process() {
        if std::env::var_os("HCOM_P2_PARENT_EXIT_HELPER").is_none() {
            return;
        }
        let mut fixture = FixtureRuntime::launch("pending", None);
        let developer = fixture.open(WorkerRole::Developer);
        let _turn = fixture.start(
            developer,
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
        );
        // SAFETY: this exact-filtered disposable helper intentionally models
        // an abrupt supervisor exit; no destructor or test-harness cleanup may
        // run before the kernel delivers the configured parent-death signal.
        unsafe {
            libc::_exit(0);
        }
    }

    #[test]
    fn abrupt_parent_exit_kills_the_app_server_leader() {
        let root = tempfile::tempdir().unwrap();
        let pid_file = root.path().join("server.pid");
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "worker::codex_app_server::tests::parent_exit_helper_process",
                "--nocapture",
            ])
            .env("HCOM_P2_PARENT_EXIT_HELPER", "1")
            .env("HCOM_P2_PARENT_EXIT_PID_FILE", &pid_file)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "parent-exit helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let pid: u32 = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
        let process = PathBuf::from(format!("/proc/{pid}"));
        let deadline = Instant::now() + Duration::from_secs(3);
        while process.exists() {
            assert!(
                Instant::now() < deadline,
                "App Server leader survived its hcom parent"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}
