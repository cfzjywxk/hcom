use super::bridge::{
    BridgeActivation, BridgeConfiguration, activate_bridge, configure_bridge,
    relay_runtime_scope_hash, sha256_hex,
};
use crate::control_api::ActionName;
use crate::control_api::peer::{process_birth_identity, process_owns_foreground_tty};
use crate::control_api::protocol::PROTOCOL_VERSION;
use crate::control_api::registration::{
    RegistrationAction, RegistrationCaller, RegistrationClient,
};
use crate::control_api::supervisor::{ControlPaths, SessionSupervisorEndpoint};
use crate::orchestrator::SessionRuntimeSources;
use crate::worker::ExecutableIdentity;
use crate::worker::codex::{
    BWRAP_EXECUTABLE, BWRAP_VERSION, CODEX_DEVELOPER_CLI_VERSION, CODEX_DEVELOPER_EXECUTABLE,
    CODEX_DEVELOPER_MODEL, CODEX_DEVELOPER_REASONING, DISABLED_CODEX_FEATURES,
};
use crate::worker::sandbox::{
    EmptyRootContract, INSIDE_CARGO_HOME, INSIDE_CODEX, INSIDE_HOME, INSIDE_NATIVE_CONFIG,
    INSIDE_PATH, INSIDE_RUNTIME, INSIDE_RUSTUP_HOME, INSIDE_TEMP, INSIDE_WORKSPACE,
};
use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const MAX_BWRAP_INFO_BYTES: usize = 4096;
const INSIDE_ARCHITECT_RUNTIME: &str = "/hcom/architect";
const INSIDE_ARCHITECT_RELAY: &str = "/hcom/architect/relay.sock";
const INSIDE_ARCHITECT_COMPONENT: &str = "/hcom/bin/hcom-architect-mcp";

#[derive(Parser)]
#[command(
    name = "hcom architect",
    disable_help_flag = true,
    disable_version_flag = true
)]
struct ArchitectArgs {
    /// Exact enabled architect adapter.
    adapter: String,

    /// Existing canonical Git repository.
    #[arg(long)]
    repo: PathBuf,

    #[arg(long, default_value = CODEX_DEVELOPER_MODEL)]
    model: String,

    #[arg(long, default_value = CODEX_DEVELOPER_REASONING)]
    reasoning: String,

    #[arg(long, default_value = "read-only")]
    sandbox: String,

    #[arg(long, default_value = "never")]
    approval: String,
}

fn create_private_session_runtime() -> Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix("hcom-architect-session.")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .context("failed to create private architect-session runtime")
}

pub(super) fn run_cli(argv: &[String]) -> Result<i32> {
    let args = ArchitectArgs::try_parse_from(
        std::iter::once("hcom architect".to_owned()).chain(argv.iter().skip(1).cloned()),
    )?;
    validate_requested_profile(&args)?;
    validate_foreground_terminal()?;

    let repository = canonical_repository(&args.repo)?;
    let native_environment = ArchitectEnvironment::capture()?;
    let session_root = create_private_session_runtime()?;
    let run_root = fs::canonicalize(session_root.path())?;
    let lock_root = native_environment
        .runtime_home
        .join("hcom-architect-repository-locks");
    ensure_private_directory(&lock_root, true)?;
    let control_paths = ControlPaths::new(&run_root, &lock_root)?;
    let run_id = format!("run-{}", random_hex(16)?);
    let runtime_sources = SessionRuntimeSources::capture(
        native_environment.control_environment.clone(),
        native_environment.runtime_home.clone(),
    )?;
    let supervisor_endpoint = SessionSupervisorEndpoint::bind(
        control_paths.clone(),
        run_id.clone(),
        repository.clone(),
        runtime_sources,
    )?;
    let startup = supervisor_endpoint.startup().clone();
    let supervisor_stop = Arc::new(AtomicBool::new(false));
    for signal in [
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ] {
        signal_hook::flag::register(signal, Arc::clone(&supervisor_stop))?;
    }
    let mut supervisor =
        SessionSupervisorThread::start(supervisor_endpoint, Arc::clone(&supervisor_stop))?;
    {
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "hcom session run: {}", startup.run_id)?;
        writeln!(
            stdout,
            "canonical repository: {}",
            startup.repo_root.display()
        )?;
        writeln!(stdout, "start branch: {}", startup.start_branch)?;
        writeln!(stdout, "start HEAD: {}", startup.start_head)?;
        writeln!(
            stdout,
            "canonical-checkout risk: approved developer tasks commit directly in this checkout; drift stops the run without reset, rebase, merge, or final apply"
        )?;
        stdout.flush()?;
    }
    let registration_client = RegistrationClient::new(control_paths.registration_socket_path());
    validate_supervisor_sockets(&control_paths)?;
    let tools = ExactTools::discover()?;
    let launch_id = random_hex(16)?;
    let binding_id = format!("architect-{launch_id}");
    let architect_name = format!("architect-{launch_id}");
    let launch_nonce = random_hex(32)?;
    let capability = random_hex(32)?;
    let paths = ArchitectLaunchPaths::create(&control_paths, &launch_id)?;
    validate_path_isolation(&repository, &paths, &tools)?;
    let auth_source = PrivateFileIdentity::capture(&discover_codex_auth_source()?)?;
    if paths_overlap(auth_source.path(), &paths.state)
        || paths_overlap(auth_source.path(), &paths.runtime)
    {
        bail!("Codex auth source overlaps architect writable state");
    }
    create_empty_private_file(&paths.auth_target)?;
    write_isolated_codex_config(&paths)?;

    let process_birth = process_birth_identity(std::process::id())?;
    let relay_contract_hash = sha256_hex(&serde_json::to_vec(&tools.component)?);
    let relay_scope_hash = relay_runtime_scope_hash(&paths.runtime)?;
    let pending_version = match registration(
        &registration_client,
        &process_birth,
        RegistrationAction::CreateBinding {
            binding_id: binding_id.clone(),
            repo_root: path_text("architect repository", &repository)?.into(),
            architect_name,
            architect_adapter: "codex-0.145.0".into(),
            launch_nonce: launch_nonce.clone(),
            capability: capability.clone(),
            actions: ActionName::ARCHITECT.into_iter().collect(),
        },
    ) {
        Ok(version) => version,
        Err(error) => {
            best_effort_close_binding(&registration_client, &process_birth, &binding_id, &[0]);
            return Err(error);
        }
    };
    if pending_version != 0 {
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[pending_version, 0],
        );
        bail!("architect pending binding returned an invalid version");
    }

    let mut bridge = match spawn_bridge(
        &tools.component.canonical_path,
        &native_environment.control_environment,
    ) {
        Ok(bridge) => bridge,
        Err(error) => {
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[pending_version],
            );
            return Err(error);
        }
    };
    let bridge_pid = bridge.child.id();
    let bridge_birth = match process_birth_identity(bridge_pid) {
        Ok(birth) => birth,
        Err(error) => {
            terminate_child(&mut bridge.child);
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[pending_version],
            );
            return Err(error).context("architect bridge disappeared during registration");
        }
    };
    let configuration = BridgeConfiguration {
        binding_id: binding_id.clone(),
        launch_nonce: launch_nonce.clone(),
        capability: capability.clone(),
        repo_root: repository.clone(),
        run_root: control_paths.run_root().to_owned(),
        lock_root: control_paths.lock_root().to_owned(),
        relay_socket_path: paths.relay_socket.clone(),
        registration_socket_path: control_paths.registration_socket_path(),
        control_socket_path: control_paths.socket_path(),
        codex_home: paths.codex_home.clone(),
        relay_executable: tools.component.clone(),
        relay_runtime_scope_hash: relay_scope_hash.clone(),
    };
    if let Err(error) = configure_bridge(&mut bridge.bootstrap, configuration) {
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[pending_version],
        );
        return Err(error).context("architect bridge configuration failed");
    }

    let sandbox = ArchitectSandbox {
        repository: repository.clone(),
        paths: paths.clone(),
        auth_source,
        host_runtime: native_environment.runtime_home.clone(),
        empty_root: EmptyRootContract::capture(
            &native_environment.cargo_bin_source,
            &native_environment.rustup_home_source,
        )?,
    };
    let (mut architect, gate_write, info_read) =
        match spawn_blocked_architect(&tools, &sandbox, &native_environment) {
            Ok(spawned) => spawned,
            Err(error) => {
                terminate_child(&mut bridge.child);
                best_effort_close_binding(
                    &registration_client,
                    &process_birth,
                    &binding_id,
                    &[pending_version],
                );
                return Err(error);
            }
        };
    let info = match read_bwrap_info(&info_read) {
        Ok(info) => info,
        Err(error) => {
            terminate_child(&mut architect);
            terminate_child(&mut bridge.child);
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[pending_version.saturating_add(1), pending_version],
            );
            return Err(error);
        }
    };
    let architect_birth = match process_birth_identity(info.child_pid) {
        Ok(birth) => birth,
        Err(error) => {
            terminate_child(&mut architect);
            terminate_child(&mut bridge.child);
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[pending_version.saturating_add(1), pending_version],
            );
            return Err(error).context("architect launch gate process disappeared");
        }
    };
    let process_bound_version = pending_version
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))?;
    let binding_version = match registration(
        &registration_client,
        &process_birth,
        RegistrationAction::BindProcess {
            binding_id: binding_id.clone(),
            expected_version: pending_version,
            architect_pid: info.child_pid,
            architect_process_birth: architect_birth.clone(),
            bridge_pid,
            bridge_process_birth: bridge_birth.clone(),
            relay_executable_contract_hash: relay_contract_hash,
            relay_runtime_scope_hash: relay_scope_hash,
        },
    ) {
        Ok(version) if version == process_bound_version => version,
        Ok(_) => {
            terminate_child(&mut architect);
            terminate_child(&mut bridge.child);
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[process_bound_version, pending_version],
            );
            bail!("architect process registration returned an invalid version");
        }
        Err(error) => {
            terminate_child(&mut architect);
            terminate_child(&mut bridge.child);
            best_effort_close_binding(
                &registration_client,
                &process_birth,
                &binding_id,
                &[pending_version.saturating_add(1), pending_version],
            );
            return Err(error);
        }
    };
    if let Err(error) = activate_bridge(
        &mut bridge.bootstrap,
        BridgeActivation {
            architect_pid: info.child_pid,
            architect_process_birth: architect_birth.clone(),
            bridge_pid,
            bridge_process_birth: bridge_birth,
            binding_version,
        },
    ) {
        terminate_child(&mut architect);
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[binding_version],
        );
        return Err(error).context("architect bridge activation failed");
    }
    let live_architect_birth = process_birth_identity(info.child_pid);
    if !matches!(
        live_architect_birth.as_deref(),
        Ok(birth) if birth == architect_birth
    ) {
        terminate_child(&mut architect);
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[binding_version],
        );
        bail!("architect launch identity changed before gate release");
    }
    if let Err(error) = release_gate(gate_write) {
        terminate_child(&mut architect);
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[binding_version],
        );
        return Err(error).context("failed to release architect launch gate");
    }

    let outcome =
        match wait_for_architect_and_bridge(&mut architect, &mut bridge.child, &supervisor_stop) {
            Ok(outcome) => outcome,
            Err(error) => {
                best_effort_close_binding(
                    &registration_client,
                    &process_birth,
                    &binding_id,
                    &[binding_version.saturating_add(1), binding_version],
                );
                return Err(error);
            }
        };
    drop(bridge.bootstrap);
    supervisor.stop_and_join()?;
    let _ = fs::remove_dir(&paths.runtime);
    Ok(outcome)
}

struct SessionSupervisorThread {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<()>>>,
}

impl SessionSupervisorThread {
    fn start(mut endpoint: SessionSupervisorEndpoint, stop: Arc<AtomicBool>) -> Result<Self> {
        let thread_stop = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("hcom-session-supervisor".into())
            .spawn(move || endpoint.run_until_stopped(&thread_stop))
            .context("failed to start session supervisor thread")?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    fn stop_and_join(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        match self.handle.take() {
            Some(handle) => handle
                .join()
                .map_err(|_| anyhow::anyhow!("session supervisor thread panicked"))?,
            None => Ok(()),
        }
    }
}

impl Drop for SessionSupervisorThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn validate_requested_profile(args: &ArchitectArgs) -> Result<()> {
    if args.adapter != "codex"
        || args.model != CODEX_DEVELOPER_MODEL
        || args.reasoning != CODEX_DEVELOPER_REASONING
        || args.sandbox != "read-only"
        || args.approval != "never"
    {
        bail!("session architect enables only the codex gpt-5.6-sol/high/read-only/never profile");
    }
    Ok(())
}

fn validate_foreground_terminal() -> Result<()> {
    for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // SAFETY: isatty only inspects an integer descriptor.
        if unsafe { libc::isatty(fd) } != 1 {
            bail!("hcom architect requires stdin/stdout/stderr on a real terminal");
        }
    }
    let birth = process_birth_identity(std::process::id())?;
    if !process_owns_foreground_tty(std::process::id(), &birth)? {
        bail!("hcom architect must be launched by the foreground terminal process group");
    }
    let stdin = fs::metadata("/proc/self/fd/0")?;
    let stdout = fs::metadata("/proc/self/fd/1")?;
    let stderr = fs::metadata("/proc/self/fd/2")?;
    if stdin.rdev() != stdout.rdev() || stdin.rdev() != stderr.rdev() {
        bail!("hcom architect requires one exact foreground terminal");
    }
    Ok(())
}

fn canonical_repository(repo: &Path) -> Result<PathBuf> {
    if !repo.is_absolute() {
        bail!("architect repository must be absolute");
    }
    let canonical =
        fs::canonicalize(repo).context("failed to canonicalize architect repository")?;
    if canonical != repo || !canonical.is_dir() {
        bail!("architect repository must already be an existing canonical directory");
    }
    let output = Command::new("/usr/bin/git")
        .args([
            "-C",
            path_text("architect repository", &canonical)?,
            "rev-parse",
            "--show-toplevel",
        ])
        .env_clear()
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .context("failed to validate architect Git repository")?;
    if !output.status.success() || !output.stderr.is_empty() || output.stdout.len() > 4096 {
        bail!("architect repository is not a canonical Git worktree");
    }
    let top = std::str::from_utf8(&output.stdout)?.trim_end();
    if top != path_text("architect repository", &canonical)? {
        bail!("architect --repo must name the exact Git top level");
    }
    Ok(canonical)
}

fn validate_supervisor_sockets(paths: &ControlPaths) -> Result<()> {
    for path in [paths.socket_path(), paths.registration_socket_path()] {
        let metadata = fs::symlink_metadata(&path).with_context(|| {
            format!(
                "session supervisor socket is unavailable: {}",
                path.display()
            )
        })?;
        // SAFETY: geteuid has no preconditions.
        if !metadata.file_type().is_socket()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            bail!("session supervisor socket is not private and current-user owned");
        }
    }
    Ok(())
}

struct ExactTools {
    codex: ExecutableIdentity,
    bwrap: ExecutableIdentity,
    component: ExecutableIdentity,
}

impl ExactTools {
    fn discover() -> Result<Self> {
        let codex = capture_exact_tool(
            Path::new(CODEX_DEVELOPER_EXECUTABLE),
            CODEX_DEVELOPER_CLI_VERSION,
        )?;
        let bwrap = capture_exact_tool(Path::new(BWRAP_EXECUTABLE), BWRAP_VERSION)?;
        let component_path = resolve_component_path()?;
        let component = ExecutableIdentity::capture(component_path)?;
        Ok(Self {
            codex,
            bwrap,
            component,
        })
    }

    fn revalidate(&self) -> Result<()> {
        revalidate_exact_tool(&self.codex, CODEX_DEVELOPER_CLI_VERSION)?;
        revalidate_exact_tool(&self.bwrap, BWRAP_VERSION)?;
        self.component.revalidate()
    }
}

fn resolve_component_path() -> Result<PathBuf> {
    let current = fs::canonicalize(std::env::current_exe()?)?;
    let sibling = current
        .parent()
        .ok_or_else(|| anyhow::anyhow!("hcom executable has no parent"))?
        .join("hcom-architect-mcp");
    if sibling.exists() {
        return fs::canonicalize(sibling).context("failed to canonicalize architect component");
    }
    let installed = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("home directory is unavailable"))?
        .join(".local/libexec/hcom-architect-mcp");
    if installed.exists() {
        return fs::canonicalize(installed)
            .context("failed to canonicalize installed architect component");
    }
    bail!("hcom-architect-mcp is not installed beside hcom or in ~/.local/libexec")
}

fn capture_exact_tool(path: &Path, expected_version: &str) -> Result<ExecutableIdentity> {
    let before = ExecutableIdentity::capture(path)?;
    let output = Command::new(&before.canonical_path)
        .arg("--version")
        .env_clear()
        .output()
        .with_context(|| format!("failed to query exact tool {}", path.display()))?;
    if !output.status.success()
        || !output.stderr.is_empty()
        || output.stdout.len() > 4096
        || std::str::from_utf8(&output.stdout)?.trim_end() != expected_version
    {
        bail!("exact architect tool version mismatch");
    }
    let after = ExecutableIdentity::capture(path)?;
    if before != after {
        bail!("architect tool changed during version discovery");
    }
    Ok(before)
}

fn revalidate_exact_tool(identity: &ExecutableIdentity, expected_version: &str) -> Result<()> {
    identity.revalidate()?;
    let output = Command::new(&identity.canonical_path)
        .arg("--version")
        .env_clear()
        .output()?;
    if !output.status.success()
        || !output.stderr.is_empty()
        || output.stdout.len() > 4096
        || std::str::from_utf8(&output.stdout)?.trim_end() != expected_version
    {
        bail!("exact architect tool version drifted");
    }
    identity.revalidate()
}

#[derive(Clone)]
struct ArchitectLaunchPaths {
    state: PathBuf,
    home: PathBuf,
    codex_home: PathBuf,
    runtime: PathBuf,
    relay_socket: PathBuf,
    auth_target: PathBuf,
    config_file: PathBuf,
}

impl ArchitectLaunchPaths {
    fn create(control: &ControlPaths, launch_id: &str) -> Result<Self> {
        let state_parent = control.architect_state_root_path();
        let runtime_parent = control.architect_runtime_root_path();
        ensure_private_directory(&state_parent, true)?;
        ensure_private_directory(&runtime_parent, true)?;
        let state = state_parent.join(launch_id);
        let runtime = runtime_parent.join(launch_id);
        ensure_private_directory(&state, false)?;
        ensure_private_directory(&runtime, false)?;
        let home = state.join("home");
        let codex_home = home.join(".codex");
        ensure_private_directory(&home, false)?;
        ensure_private_directory(&codex_home, false)?;
        Ok(Self {
            state,
            home,
            codex_home: codex_home.clone(),
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: codex_home.join("auth.json"),
            config_file: codex_home.join("config.toml"),
        })
    }
}

fn ensure_private_directory(path: &Path, allow_existing: bool) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) if !allow_existing => bail!("architect launch directory already exists"),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| {
                format!("failed to create private directory {}", path.display())
            })?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
        || fs::canonicalize(path)? != path
    {
        bail!("architect directory is not canonical, private, and current-user owned");
    }
    Ok(())
}

fn discover_codex_auth_source() -> Result<PathBuf> {
    let base = match std::env::var_os("CODEX_HOME") {
        Some(path) => PathBuf::from(path),
        None => dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("home directory is unavailable"))?
            .join(".codex"),
    };
    let source = base.join("auth.json");
    let canonical = fs::canonicalize(&source).context("Codex auth.json is unavailable")?;
    if canonical != source {
        bail!("Codex auth source must already be canonical");
    }
    Ok(canonical)
}

#[derive(Clone, PartialEq, Eq)]
struct PrivateFileIdentity {
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

impl PrivateFileIdentity {
    fn capture(path: &Path) -> Result<Self> {
        let link = fs::symlink_metadata(path)?;
        if link.file_type().is_symlink() || !link.is_file() {
            bail!("Codex auth source must be a regular non-symlink file");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("Codex auth source must already be canonical");
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = file.metadata()?;
        if metadata.dev() != link.dev() || metadata.ino() != link.ino() {
            bail!("Codex auth source changed while it was opened");
        }
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
        if identity.uid != unsafe { libc::geteuid() }
            || identity.mode & 0o077 != 0
            || identity.mode & 0o600 != 0o600
            || identity.links != 1
        {
            bail!("Codex auth source must be a private current-user file");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self) -> Result<()> {
        if Self::capture(&self.path)? != *self {
            bail!("Codex auth source identity drifted before architect launch");
        }
        Ok(())
    }
}

fn create_empty_private_file(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    file.sync_all()?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        bail!("architect private file identity is invalid");
    }
    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IsolatedCodexConfig {
    tui: IsolatedTuiConfig,
    mcp_servers: BTreeMap<String, IsolatedMcpServer>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IsolatedTuiConfig {
    terminal_title: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IsolatedMcpServer {
    command: String,
    args: Vec<String>,
    startup_timeout_sec: u32,
    tool_timeout_sec: u32,
    enabled: bool,
}

fn write_isolated_codex_config(paths: &ArchitectLaunchPaths) -> Result<()> {
    let server = IsolatedMcpServer {
        command: INSIDE_ARCHITECT_COMPONENT.into(),
        args: vec![
            "relay".into(),
            "--socket".into(),
            INSIDE_ARCHITECT_RELAY.into(),
        ],
        startup_timeout_sec: 10,
        tool_timeout_sec: 300,
        enabled: true,
    };
    let config = IsolatedCodexConfig {
        tui: IsolatedTuiConfig {
            terminal_title: vec![],
        },
        mcp_servers: [("hcom_session_task_control".into(), server)]
            .into_iter()
            .collect(),
    };
    let bytes = toml::to_string(&config)?.into_bytes();
    if bytes.len() > 16 * 1024 {
        bail!("isolated Codex config exceeds its bound");
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&paths.config_file)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    let parsed: IsolatedCodexConfig = toml::from_str(std::str::from_utf8(&bytes)?)?;
    if parsed != config {
        bail!("isolated Codex config failed its exact round trip");
    }
    let metadata = fs::symlink_metadata(&paths.config_file)?;
    if metadata.permissions().mode() & 0o777 != 0o600 {
        bail!("isolated Codex config mode drifted");
    }
    Ok(())
}

fn validate_path_isolation(
    repository: &Path,
    paths: &ArchitectLaunchPaths,
    tools: &ExactTools,
) -> Result<()> {
    for (left, right, label) in [
        (repository, paths.state.as_path(), "repository/state"),
        (repository, paths.runtime.as_path(), "repository/runtime"),
        (
            paths.state.as_path(),
            paths.runtime.as_path(),
            "state/runtime",
        ),
    ] {
        if paths_overlap(left, right) {
            bail!("architect {label} paths overlap");
        }
    }
    for executable in [
        &tools.codex.canonical_path,
        &tools.bwrap.canonical_path,
        &tools.component.canonical_path,
    ] {
        if executable.starts_with(&paths.state) || executable.starts_with(&paths.runtime) {
            bail!("architect writable roots contain a protected executable");
        }
    }
    Ok(())
}

struct ArchitectEnvironment {
    values: BTreeMap<String, String>,
    control_environment: BTreeMap<String, String>,
    runtime_home: PathBuf,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
}

impl ArchitectEnvironment {
    fn capture() -> Result<Self> {
        let state_home = explicit_directory("XDG_STATE_HOME", dirs::state_dir)?;
        let config_home = explicit_directory("XDG_CONFIG_HOME", dirs::config_dir)?;
        let runtime_home = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("XDG_RUNTIME_DIR is required"))?;
        let runtime_home = canonical_private_runtime(&runtime_home)?;
        let mut control_environment = BTreeMap::new();
        control_environment.insert(
            "XDG_STATE_HOME".into(),
            path_text("XDG state home", &state_home)?.into(),
        );
        control_environment.insert(
            "XDG_CONFIG_HOME".into(),
            path_text("XDG config home", &config_home)?.into(),
        );
        control_environment.insert(
            "XDG_RUNTIME_DIR".into(),
            path_text("XDG runtime home", &runtime_home)?.into(),
        );
        let mut values = BTreeMap::new();
        for name in [
            "ALL_PROXY",
            "COLORTERM",
            "HTTPS_PROXY",
            "HTTP_PROXY",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "NO_PROXY",
            "PATH",
            "SSL_CERT_DIR",
            "SSL_CERT_FILE",
            "TERM",
            "TZ",
            "all_proxy",
            "http_proxy",
            "https_proxy",
            "no_proxy",
        ] {
            if let Some(value) = read_unicode_environment(name)? {
                validate_environment_value(name, &value)?;
                values.insert(name.into(), value);
            }
        }
        if !values.contains_key("PATH") || !values.contains_key("TERM") {
            bail!("architect environment requires PATH and TERM");
        }
        control_environment.extend(values.clone());
        for name in [
            "HOME",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "CODEX_HOME",
            "CLAUDE_CONFIG_DIR",
        ] {
            if let Some(value) = read_unicode_environment(name)? {
                validate_environment_value(name, &value)?;
                control_environment.insert(name.into(), value);
            }
        }
        if !control_environment.contains_key("HOME") {
            bail!("architect control environment requires parent HOME");
        }
        let parent_home = PathBuf::from(
            control_environment
                .get("HOME")
                .expect("checked architect parent HOME"),
        );
        let cargo_bin_source = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| parent_home.join(".cargo"))
            .join("bin");
        let rustup_home_source = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| parent_home.join(".rustup"));
        Ok(Self {
            values,
            control_environment,
            runtime_home,
            cargo_bin_source,
            rustup_home_source,
        })
    }

    fn sandbox_values(&self, _paths: &ArchitectLaunchPaths) -> Result<BTreeMap<String, String>> {
        let mut values = self.values.clone();
        for (name, value) in [
            ("CARGO_HOME", INSIDE_CARGO_HOME),
            ("CODEX_HOME", INSIDE_NATIVE_CONFIG),
            ("HOME", INSIDE_HOME),
            ("PATH", INSIDE_PATH),
            ("RUSTUP_HOME", INSIDE_RUSTUP_HOME),
            ("TMPDIR", INSIDE_TEMP),
            ("XDG_RUNTIME_DIR", INSIDE_RUNTIME),
        ] {
            values.insert(name.into(), value.into());
        }
        Ok(values)
    }
}

fn explicit_directory(variable: &str, fallback: fn() -> Option<PathBuf>) -> Result<PathBuf> {
    let path = std::env::var_os(variable)
        .map(PathBuf::from)
        .or_else(fallback)
        .ok_or_else(|| anyhow::anyhow!("{variable} is unavailable"))?;
    let canonical = fs::canonicalize(&path)?;
    if canonical != path || !canonical.is_dir() {
        bail!("{variable} must be an existing canonical directory");
    }
    Ok(canonical)
}

fn canonical_private_runtime(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(&canonical)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        bail!("XDG_RUNTIME_DIR must be canonical and private");
    }
    Ok(canonical)
}

fn validate_environment_value(name: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 16 * 1024
        || value
            .chars()
            .any(|character| character == '\0' || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("architect environment value {name} is invalid");
    }
    Ok(())
}

fn read_unicode_environment(name: &str) -> Result<Option<String>> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!("architect environment value {name} is not UTF-8"))
        })
        .transpose()
}

struct BridgeChild {
    child: Child,
    bootstrap: UnixStream,
}

fn spawn_bridge(component: &Path, environment: &BTreeMap<String, String>) -> Result<BridgeChild> {
    let (parent, child_stream) = UnixStream::pair()?;
    parent.set_read_timeout(Some(Duration::from_secs(10)))?;
    parent.set_write_timeout(Some(Duration::from_secs(10)))?;
    let inherited_fd = child_stream.as_raw_fd();
    let expected_parent = std::process::id() as libc::pid_t;
    let mut command = Command::new(component);
    command
        .args(["bridge", "--bootstrap-fd", &inherited_fd.to_string()])
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: pre_exec performs only async-signal-safe descriptor/prctl/syscall
    // operations before the single-threaded child exec.
    unsafe {
        command.pre_exec(move || {
            inherit_for_exec(inherited_fd)?;
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != expected_parent {
                return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .context("failed to spawn architect bridge")?;
    drop(child_stream);
    Ok(BridgeChild {
        child,
        bootstrap: parent,
    })
}

struct ArchitectSandbox {
    repository: PathBuf,
    paths: ArchitectLaunchPaths,
    auth_source: PrivateFileIdentity,
    host_runtime: PathBuf,
    empty_root: EmptyRootContract,
}

impl ArchitectSandbox {
    fn outer_argv(
        &self,
        environment: &ArchitectEnvironment,
        tools: &ExactTools,
        block_fd: RawFd,
        info_fd: RawFd,
    ) -> Result<Vec<String>> {
        if block_fd <= libc::STDERR_FILENO || info_fd <= libc::STDERR_FILENO || block_fd == info_fd
        {
            bail!("architect launch-control descriptors are invalid");
        }
        self.auth_source.revalidate()?;
        if paths_overlap(&self.paths.runtime, &self.host_runtime) {
            bail!("architect relay runtime overlaps the host runtime directory");
        }
        tools.revalidate()?;
        let mut argv = self.empty_root.base_argv()?;
        argv.push("--clearenv".into());
        for (name, value) in environment.sandbox_values(&self.paths)? {
            argv.extend(["--setenv".into(), name, value]);
        }
        argv.extend([
            "--dir".into(),
            INSIDE_ARCHITECT_RUNTIME.into(),
            "--ro-bind".into(),
            path_text("runtime launch source", &self.paths.runtime)?.into(),
            INSIDE_ARCHITECT_RUNTIME.into(),
            "--bind".into(),
            path_text("architect isolated HOME", &self.paths.home)?.into(),
            INSIDE_HOME.into(),
            "--bind".into(),
            path_text("architect native config", &self.paths.codex_home)?.into(),
            INSIDE_NATIVE_CONFIG.into(),
            "--ro-bind".into(),
            path_text("architect repository", &self.repository)?.into(),
            INSIDE_WORKSPACE.into(),
            "--ro-bind".into(),
            path_text("Codex auth source", self.auth_source.path())?.into(),
            "/hcom/native/auth.json".into(),
            "--ro-bind".into(),
            path_text("Codex executable", &tools.codex.canonical_path)?.into(),
            INSIDE_CODEX.into(),
            "--ro-bind".into(),
            path_text(
                "architect relay executable",
                &tools.component.canonical_path,
            )?
            .into(),
            INSIDE_ARCHITECT_COMPONENT.into(),
            "--chdir".into(),
            INSIDE_WORKSPACE.into(),
            "--block-fd".into(),
            block_fd.to_string(),
            "--info-fd".into(),
            info_fd.to_string(),
        ]);
        if argv.iter().any(|argument| {
            argument == "--new-session"
                || argument.contains("control.sock")
                || argument.contains("registration.sock")
        }) {
            bail!("architect sandbox manifest exposes forbidden terminal/control authority");
        }
        let relay_directory = path_text("relay directory", &self.paths.runtime)?;
        if !argv
            .windows(2)
            .any(|pair| pair == ["--tmpfs", INSIDE_RUNTIME])
            || !argv.windows(3).any(|triple| {
                triple[0] == "--ro-bind"
                    && triple[1] == relay_directory
                    && triple[2] == INSIDE_ARCHITECT_RUNTIME
            })
        {
            bail!("architect sandbox manifest does not expose exactly one relay scope");
        }
        Ok(argv)
    }
}

fn spawn_blocked_architect(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    environment: &ArchitectEnvironment,
) -> Result<(Child, OwnedFd, OwnedFd)> {
    tools.revalidate()?;
    let (gate_read, gate_write) = pipe_cloexec()?;
    let (info_read, info_write) = pipe_cloexec()?;
    let mut command = Command::new(&tools.bwrap.canonical_path);
    let gate_fd = gate_read.as_raw_fd();
    let info_fd = info_write.as_raw_fd();
    let mut argv = sandbox.outer_argv(environment, tools, gate_fd, info_fd)?;
    argv.push("--".into());
    argv.push(INSIDE_CODEX.into());
    argv.extend([
        "--model".into(),
        CODEX_DEVELOPER_MODEL.into(),
        "--config".into(),
        "model_reasoning_effort=\"high\"".into(),
        "--sandbox".into(),
        "read-only".into(),
        "--ask-for-approval".into(),
        "never".into(),
        "--cd".into(),
        INSIDE_WORKSPACE.into(),
        "--no-alt-screen".into(),
        "--strict-config".into(),
    ]);
    for feature in DISABLED_CODEX_FEATURES {
        argv.extend(["--disable".into(), (*feature).into()]);
    }
    validate_native_argv(&argv, tools, sandbox)?;
    command.args(&argv).env_clear();
    // SAFETY: pre_exec only clears CLOEXEC on the two owned launch-control
    // descriptors before exec.
    unsafe {
        command.pre_exec(move || {
            inherit_for_exec(gate_fd)?;
            inherit_for_exec(info_fd)?;
            Ok(())
        });
    }
    let child = command
        .spawn()
        .context("failed to spawn blocked architect")?;
    drop(gate_read);
    drop(info_write);
    Ok((child, gate_write, info_read))
}

fn validate_native_argv(
    argv: &[String],
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
) -> Result<()> {
    let separator = argv
        .iter()
        .position(|argument| argument == "--")
        .ok_or_else(|| anyhow::anyhow!("architect bwrap argv omitted its separator"))?;
    let native = &argv[separator + 1..];
    let expected_prefix = [
        INSIDE_CODEX,
        "--model",
        CODEX_DEVELOPER_MODEL,
        "--config",
        "model_reasoning_effort=\"high\"",
        "--sandbox",
        "read-only",
        "--ask-for-approval",
        "never",
        "--cd",
        INSIDE_WORKSPACE,
        "--no-alt-screen",
        "--strict-config",
    ];
    if native.len() != expected_prefix.len() + DISABLED_CODEX_FEATURES.len() * 2
        || !native
            .iter()
            .take(expected_prefix.len())
            .map(String::as_str)
            .eq(expected_prefix)
    {
        bail!("architect native argv drifted from its exact blank profile");
    }
    for (pair, feature) in native[expected_prefix.len()..]
        .chunks_exact(2)
        .zip(DISABLED_CODEX_FEATURES)
    {
        if pair[0] != "--disable" || pair[1] != *feature {
            bail!("architect disabled-feature inventory drifted");
        }
    }
    let relay_socket = INSIDE_ARCHITECT_RELAY;
    if native.iter().any(|argument| {
        argument == "-" || argument == "--hcom-prompt" || argument.contains(relay_socket)
    }) {
        bail!("architect native argv contains prompt or control material");
    }
    for forbidden in [
        path_text("host Codex executable", &tools.codex.canonical_path)?,
        path_text("host architect repository", &sandbox.repository)?,
        path_text("host architect relay socket", &sandbox.paths.relay_socket)?,
    ] {
        if native.iter().any(|argument| argument.contains(forbidden)) {
            bail!("architect native argv contains a host-only path");
        }
    }
    Ok(())
}

fn registration(
    client: &RegistrationClient,
    process_birth: &str,
    action: RegistrationAction,
) -> Result<u64> {
    let response = client.request(&crate::control_api::registration::RegistrationRequest {
        protocol_version: PROTOCOL_VERSION,
        request_id: format!("launch-{}", random_hex(16)?),
        caller: RegistrationCaller::Human {
            process_birth: process_birth.into(),
        },
        action,
    })?;
    if !response.ok {
        bail!("session supervisor refused architect launch registration");
    }
    response
        .binding_version
        .ok_or_else(|| anyhow::anyhow!("registration response omitted binding version"))
}

fn best_effort_close_binding(
    client: &RegistrationClient,
    process_birth: &str,
    binding_id: &str,
    versions: &[u64],
) {
    let mut attempted = std::collections::BTreeSet::new();
    for version in versions {
        if !attempted.insert(*version) {
            continue;
        }
        if matches!(
            registration(
            client,
            process_birth,
            RegistrationAction::CloseBinding {
                binding_id: binding_id.to_owned(),
                expected_version: *version,
            },
            ),
            Ok(next) if Some(next) == version.checked_add(1)
        ) {
            break;
        }
    }
}

fn release_gate(gate: OwnedFd) -> Result<()> {
    let mut file = std::fs::File::from(gate);
    file.write_all(b"1")?;
    file.flush()?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct BwrapInfo {
    child_pid: u32,
    ipc_namespace: u64,
    mnt_namespace: u64,
    pid_namespace: u64,
    uts_namespace: u64,
}

fn read_bwrap_info(fd: &OwnedFd) -> Result<BwrapInfo> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        let now = Instant::now();
        if now >= deadline {
            bail!("timed out waiting for bwrap launch identity");
        }
        let remaining = deadline.saturating_duration_since(now);
        let timeout = i32::try_from(remaining.as_millis().min(i32::MAX as u128)).unwrap();
        let mut pollfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: pollfd points to one initialized record for the call duration.
        let ready = unsafe { libc::poll(&mut pollfd, 1, timeout) };
        if ready < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("failed to poll bwrap info fd");
        }
        if ready == 0 {
            bail!("timed out waiting for bwrap launch identity");
        }
        let remaining_capacity = MAX_BWRAP_INFO_BYTES.saturating_sub(bytes.len());
        if remaining_capacity == 0 {
            bail!("bwrap launch info exceeds its bound");
        }
        let mut buffer = vec![0u8; remaining_capacity.min(1024)];
        // SAFETY: buffer is writable for its full length and fd is an owned pipe.
        let count = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("failed to read bwrap launch info");
        }
        if count == 0 {
            if bytes.is_empty() {
                bail!("bwrap closed its info fd before reporting a child");
            }
            return parse_bwrap_info(&bytes)
                .context("bwrap closed its info fd with an invalid report");
        }
        bytes.extend_from_slice(&buffer[..count as usize]);
    }
}

fn parse_bwrap_info(bytes: &[u8]) -> Result<BwrapInfo> {
    let info: BwrapInfo =
        serde_json::from_slice(bytes).context("bwrap launch info is not strict JSON")?;
    if info.child_pid <= 1
        || info.ipc_namespace == 0
        || info.mnt_namespace == 0
        || info.pid_namespace == 0
        || info.uts_namespace == 0
    {
        bail!("bwrap returned an invalid launch identity");
    }
    Ok(info)
}

fn wait_for_architect_and_bridge(
    architect: &mut Child,
    bridge: &mut Child,
    supervisor_stop: &AtomicBool,
) -> Result<i32> {
    let result = (|| -> Result<i32> {
        loop {
            if supervisor_stop.load(Ordering::Acquire) {
                bail!("architect session received a termination signal");
            }
            if let Some(status) = architect.try_wait()? {
                let deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline {
                    if let Some(bridge_status) = bridge.try_wait()? {
                        if !bridge_status.success() {
                            bail!("architect bridge failed during binding revoke: {bridge_status}");
                        }
                        return exit_code(status);
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                bail!("architect bridge did not revoke its binding after Codex exited");
            }
            if let Some(status) = bridge.try_wait()? {
                bail!("architect bridge exited before Codex: {status}");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    })();
    if result.is_err() {
        terminate_child(architect);
        terminate_child(bridge);
    }
    result
}

fn exit_code(status: ExitStatus) -> Result<i32> {
    if let Some(code) = status.code() {
        return Ok(code);
    }
    use std::os::unix::process::ExitStatusExt;
    Ok(128 + status.signal().unwrap_or(libc::SIGKILL))
}

fn terminate_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn pipe_cloexec() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    // SAFETY: fds points to storage for exactly two descriptors.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to create launch pipe");
    }
    // SAFETY: pipe2 returned two newly owned descriptors.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn inherit_for_exec(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fd is a live owned descriptor in the child. Clearing FD_CLOEXEC
    // keeps that exact descriptor available to the explicitly named child arg.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn random_hex(bytes: usize) -> Result<String> {
    if !(16..=64).contains(&bytes) {
        bail!("CSPRNG request is outside its fixed bound");
    }
    let mut random = vec![0u8; bytes];
    let mut offset = 0;
    while offset < random.len() {
        // SAFETY: the remaining slice is writable and getrandom fills at most
        // the supplied length from the kernel CSPRNG.
        let count = unsafe {
            libc::getrandom(
                random[offset..].as_mut_ptr().cast(),
                random.len() - offset,
                0,
            )
        };
        if count < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("kernel CSPRNG failed");
        }
        if count == 0 {
            bail!("kernel CSPRNG returned no bytes");
        }
        offset += count as usize;
    }
    let mut output = String::with_capacity(bytes * 2);
    use std::fmt::Write as _;
    for byte in random {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(output)
}

fn path_text<'a>(label: &str, path: &'a Path) -> Result<&'a str> {
    let text = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("{label} must be valid UTF-8"))?;
    if text.is_empty() || text.len() > 4096 || text.chars().any(|character| character.is_control())
    {
        bail!("{label} has an invalid bounded path");
    }
    Ok(text)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixListener;
    use std::os::unix::process::CommandExt;

    const BLANK_HELPER_ROOT: &str = "HCOM_PHASE7_BLANK_HELPER_ROOT";
    const RUNTIME_MODE_HELPER: &str = "HCOM_PHASE9_RUNTIME_MODE_HELPER";

    #[test]
    fn session_runtime_helper_process() {
        if std::env::var_os(RUNTIME_MODE_HELPER).is_none() {
            return;
        }
        // SAFETY: this exact-filtered helper runs in its own disposable process,
        // so changing its process-global umask cannot race another test.
        unsafe {
            libc::umask(0o002);
        }
        let runtime = create_private_session_runtime().unwrap();
        assert_eq!(
            fs::symlink_metadata(runtime.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn session_runtime_is_private_under_a_permissive_parent_umask() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "architect::launch::tests::session_runtime_helper_process",
                "--nocapture",
            ])
            .env(RUNTIME_MODE_HELPER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "runtime mode helper failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn kernel_nonce_is_csprng_shaped_and_not_reused() {
        let first = random_hex(32).unwrap();
        let second = random_hex(32).unwrap();
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn auth_identity_rejects_content_or_hardlink_drift() {
        let temp = tempfile::tempdir().unwrap();
        let auth = temp.path().join("auth.json");
        fs::write(&auth, b"first").unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        let identity = PrivateFileIdentity::capture(&auth).unwrap();

        fs::write(&auth, b"second-longer").unwrap();
        assert!(identity.revalidate().is_err());

        let hardlink = temp.path().join("auth-hardlink.json");
        fs::hard_link(&auth, &hardlink).unwrap();
        assert!(PrivateFileIdentity::capture(&auth).is_err());
    }

    #[test]
    fn native_profile_has_no_prompt_or_secret_transport() {
        let native: Vec<String> = vec![
            CODEX_DEVELOPER_EXECUTABLE.into(),
            "--model".into(),
            CODEX_DEVELOPER_MODEL.into(),
            "--config".into(),
            "model_reasoning_effort=\"high\"".into(),
            "--sandbox".into(),
            "read-only".into(),
            "--ask-for-approval".into(),
            "never".into(),
            "--cd".into(),
            "/repo".into(),
            "--no-alt-screen".into(),
            "--strict-config".into(),
        ];
        assert!(!native.iter().any(|argument| argument == "-"));
        assert!(!native.iter().any(|argument| argument.contains("nonce")));
        assert!(
            !native
                .iter()
                .any(|argument| argument.contains("capability"))
        );
    }

    #[test]
    fn blank_launch_helper_process() {
        let Some(root) = std::env::var_os(BLANK_HELPER_ROOT).map(PathBuf::from) else {
            return;
        };
        let repository = root.join("repo");
        let host_runtime = root.join("run");
        let state = root.join("architect-state");
        let home = state.join("home");
        let codex_home = home.join(".codex");
        let runtime = root.join("session-runtime/launch");
        let cargo_bin_source = root.join("cargo-bin");
        let rustup_home_source = root.join("rustup-home");
        let paths = ArchitectLaunchPaths {
            state,
            home,
            codex_home,
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: root.join("architect-state/home/.codex/auth.json"),
            config_file: root.join("architect-state/home/.codex/config.toml"),
        };
        let fake_codex = fs::canonicalize(root.join("fake-codex")).unwrap();
        let codex = capture_exact_tool(&fake_codex, CODEX_DEVELOPER_CLI_VERSION).unwrap();
        let bwrap = capture_exact_tool(Path::new(BWRAP_EXECUTABLE), BWRAP_VERSION).unwrap();
        let tools = ExactTools {
            component: codex.clone(),
            codex,
            bwrap,
        };
        let environment = ArchitectEnvironment {
            values: [
                ("LANG".into(), "C.UTF-8".into()),
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("TERM".into(), "xterm-256color".into()),
            ]
            .into_iter()
            .collect(),
            control_environment: BTreeMap::new(),
            runtime_home: host_runtime.clone(),
            cargo_bin_source: cargo_bin_source.clone(),
            rustup_home_source: rustup_home_source.clone(),
        };
        let sandbox = ArchitectSandbox {
            repository,
            paths,
            auth_source: PrivateFileIdentity::capture(&root.join("auth.json")).unwrap(),
            host_runtime,
            empty_root: EmptyRootContract::capture(&cargo_bin_source, &rustup_home_source).unwrap(),
        };
        let report = root.join("architect-state/home/blank-report");
        let write_probe = root.join("repo/architect-write-probe");
        let (mut child, gate, info) =
            spawn_blocked_architect(&tools, &sandbox, &environment).unwrap();
        let info = match read_bwrap_info(&info) {
            Ok(info) => info,
            Err(error) => {
                drop(gate);
                let status = child.wait().unwrap();
                panic!("failed to read bwrap gate info ({status}): {error:#}");
            }
        };
        assert!(!report.exists(), "the bwrap internal gate released early");
        assert!(
            process_birth_identity(info.child_pid)
                .unwrap()
                .starts_with("linux-proc:")
        );
        release_gate(gate).unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "fake blank architect failed: {status}");
        assert_eq!(fs::read_to_string(report).unwrap(), "ok\n");
        assert!(!write_probe.exists());
    }

    #[test]
    fn blank_launch_keeps_terminal_input_empty_and_exposes_only_its_relay() {
        let temp = tempfile::Builder::new()
            .prefix("hcom-phase7-blank.")
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = root.join("repo");
        let host_runtime = root.join("run");
        let control_root = host_runtime.join("legacy-control-sentinel");
        let architect_root = root.join("session-runtime");
        let runtime = architect_root.join("launch");
        let other_runtime = architect_root.join("other-launch");
        let state = root.join("architect-state");
        let home = state.join("home");
        let codex_home = home.join(".codex");
        let cargo_bin_source = root.join("cargo-bin");
        let rustup_home_source = root.join("rustup-home");
        for path in [
            &repository,
            &host_runtime,
            &control_root,
            &architect_root,
            &runtime,
            &other_runtime,
            &state,
            &home,
            &codex_home,
            &cargo_bin_source,
            &rustup_home_source,
        ] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let auth_source = root.join("auth.json");
        let auth_target = codex_home.join("auth.json");
        let config_file = codex_home.join("config.toml");
        for path in [&auth_source, &auth_target, &config_file] {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .unwrap();
            file.sync_all().unwrap();
        }
        let control_socket = control_root.join("control.sock");
        let registration_socket = control_root.join("registration.sock");
        let relay_socket = runtime.join("relay.sock");
        let other_relay_socket = other_runtime.join("relay.sock");
        let listeners: Vec<_> = [
            &control_socket,
            &registration_socket,
            &relay_socket,
            &other_relay_socket,
        ]
        .into_iter()
        .map(|path| {
            let listener = UnixListener::bind(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            listener
        })
        .collect();

        let report = home.join("blank-report");
        let mut expected_args = vec![
            "--model".to_owned(),
            CODEX_DEVELOPER_MODEL.to_owned(),
            "--config".to_owned(),
            "model_reasoning_effort=\"high\"".to_owned(),
            "--sandbox".to_owned(),
            "read-only".to_owned(),
            "--ask-for-approval".to_owned(),
            "never".to_owned(),
            "--cd".to_owned(),
            INSIDE_WORKSPACE.to_owned(),
            "--no-alt-screen".to_owned(),
            "--strict-config".to_owned(),
        ];
        for feature in DISABLED_CODEX_FEATURES {
            expected_args.extend(["--disable".to_owned(), (*feature).to_owned()]);
        }
        let fake_codex = root.join("fake-codex");
        let script = format!(
            r#"#!/usr/bin/python3
import errno
import os
import select
import socket
import sys

if sys.argv[1:] == ["--version"]:
    print("codex-cli 0.145.0")
    raise SystemExit(0)

if sys.argv[1:] != {expected_args}:
    raise SystemExit(31)
if not all(os.isatty(fd) for fd in (0, 1, 2)):
    raise SystemExit(32)
if select.select([0], [], [], 0)[0]:
    raise SystemExit(33)
try:
    open({write_probe}, "xb").close()
except OSError as error:
    if error.errno != errno.EROFS:
        raise SystemExit(34)
else:
    raise SystemExit(35)

for path in [{control_socket}, {registration_socket}, {other_relay_socket}]:
    if os.path.exists(path):
        raise SystemExit(36)
    client = socket.socket(socket.AF_UNIX)
    try:
        client.connect(path)
    except FileNotFoundError:
        pass
    else:
        raise SystemExit(37)
    finally:
        client.close()

relay = socket.socket(socket.AF_UNIX)
relay.connect({relay_socket})
relay.close()
with open({report}, "w", encoding="utf-8") as output:
    output.write("ok\n")
"#,
            expected_args = serde_json::to_string(&expected_args).unwrap(),
            write_probe = serde_json::to_string(
                &PathBuf::from(INSIDE_WORKSPACE).join("architect-write-probe")
            )
            .unwrap(),
            control_socket = serde_json::to_string(&control_socket).unwrap(),
            registration_socket = serde_json::to_string(&registration_socket).unwrap(),
            other_relay_socket = serde_json::to_string(&other_relay_socket).unwrap(),
            relay_socket = serde_json::to_string(INSIDE_ARCHITECT_RELAY).unwrap(),
            report =
                serde_json::to_string(&PathBuf::from(INSIDE_HOME).join("blank-report")).unwrap(),
        );
        fs::write(&fake_codex, script).unwrap();
        fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();

        let pty = nix::pty::openpty(None, None).unwrap();
        let slave_fd = pty.slave.as_raw_fd();
        let master_fd = pty.master.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "architect::launch::tests::blank_launch_helper_process",
                "--nocapture",
            ])
            .env(BLANK_HELPER_ROOT, &root)
            .env("XDG_CONFIG_HOME", root.join("xdg-config"))
            .env("XDG_RUNTIME_DIR", &host_runtime)
            .env("XDG_STATE_HOME", root.join("xdg-state"))
            .env("CARGO_HOME", root.join("cargo"))
            .env("RUSTUP_HOME", &rustup_home_source);
        // SAFETY: these are async-signal-safe session, terminal, and
        // descriptor operations in the disposable child before exec.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1
                    || libc::ioctl(slave_fd, libc::TIOCSCTTY, 0) == -1
                    || libc::tcsetpgrp(slave_fd, libc::getpid()) == -1
                    || libc::dup2(slave_fd, libc::STDIN_FILENO) == -1
                    || libc::dup2(slave_fd, libc::STDOUT_FILENO) == -1
                    || libc::dup2(slave_fd, libc::STDERR_FILENO) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                if slave_fd > libc::STDERR_FILENO {
                    libc::close(slave_fd);
                }
                libc::close(master_fd);
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        drop(pty.slave);
        let output = std::thread::spawn(move || {
            let mut master = std::fs::File::from(pty.master);
            let mut output = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                match master.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => output.extend_from_slice(&buffer[..count]),
                    Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                    Err(error) => panic!("failed reading disposable PTY: {error}"),
                }
            }
            output
        });
        let status = child.wait().unwrap();
        let output = output.join().unwrap();
        assert!(
            status.success(),
            "blank helper failed: {status}\n{}",
            String::from_utf8_lossy(&output)
        );
        assert_eq!(fs::read_to_string(&report).unwrap(), "ok\n");
        assert!(!repository.join("architect-write-probe").exists());
        assert!(
            listeners
                .iter()
                .all(|listener| listener.local_addr().is_ok())
        );
    }
}
