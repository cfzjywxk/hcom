use super::bridge::{
    BridgeActivation, BridgeConfiguration, activate_bridge, configure_bridge,
    relay_runtime_scope_hash, sha256_hex,
};
use super::profile::{LoadedInvocationProfiles, load_task_lane_profiles};
use crate::control_api::ActionName;
use crate::control_api::peer::{process_birth_identity, process_owns_foreground_tty};
use crate::control_api::protocol::PROTOCOL_VERSION;
use crate::control_api::registration::{
    RegistrationAction, RegistrationCaller, RegistrationClient,
};
use crate::control_api::supervisor::{ControlPaths, SessionSupervisorEndpoint};
use crate::orchestrator::SessionRuntimeSources;
use crate::worker::codex::{BWRAP_EXECUTABLE, BWRAP_VERSION};
use crate::worker::profile::{
    ArchitectAdapter, ArchitectInvocationProfile, CodexApprovalPolicy, CodexSandbox,
    DeveloperInvocationProfile, ReviewerInvocationProfile, SessionInvocationProfiles,
    validate_cli_help_contract,
};
use crate::worker::reviewer::{CLAUDE_REVIEWER_CLI_VERSION, CLAUDE_REVIEWER_EXECUTABLE};
use crate::worker::runtime::CODEX_TASK_WORKER_ADAPTER;
use crate::worker::sandbox::{HostRootAccess, HostRootContract, HostRootMounts};
use crate::worker::{ExecutableIdentity, ParentEnvironment};
use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
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
const MAX_INHERITED_CREDENTIAL_SOCKETS: usize = 8;
// Codex currently requires a finite MCP tool timeout. Two years exceeds the
// protocol's maximum 64-task, 20-review-round run at the six-hour turn bound,
// so the terminal wait remains owned by the foreground process lifecycle.
const CODEX_CONTROL_TOOL_TIMEOUT_SECS: u64 = 2 * 365 * 24 * 60 * 60;

#[derive(Parser)]
#[command(
    name = "hcom arch",
    disable_help_flag = true,
    disable_version_flag = true
)]
struct ArchitectArgs {
    /// Exact enabled architect adapter.
    adapter: String,

    #[arg(long)]
    model: Option<String>,

    #[arg(long)]
    reasoning: Option<String>,

    #[arg(long)]
    effort: Option<String>,

    #[arg(long)]
    sandbox: Option<String>,

    #[arg(long, visible_alias = "ask-for-approval")]
    approval: Option<String>,
}

fn create_private_session_runtime() -> Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix("hcom-architect-session.")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .context("failed to create private architect-session runtime")
}

pub(super) fn run_cli(argv: &[String], config_path: Option<&Path>) -> Result<i32> {
    let args = ArchitectArgs::try_parse_from(
        std::iter::once("hcom arch".to_owned()).chain(argv.iter().skip(1).cloned()),
    )?;
    let architect_adapter = ArchitectAdapter::parse(&args.adapter)?;
    // Both public entrypoints bind the same Codex-only task worker lane; a
    // configured Claude developer or reviewer fails closed inside the loader.
    let mut loaded = match config_path {
        Some(path) => load_task_lane_profiles(path, architect_adapter)?,
        None => LoadedInvocationProfiles {
            profiles: SessionInvocationProfiles::for_task_lane(architect_adapter)?,
            config_path: PathBuf::from("<built-in defaults>"),
            loaded_from_file: false,
        },
    };
    apply_architect_cli_overrides(&args, &mut loaded.profiles)?;
    let (developer_adapter, reviewer_adapter) =
        worker_adapter_bindings(architect_adapter, &loaded.profiles);
    validate_foreground_terminal()?;

    let project_root = canonical_project_directory(&std::env::current_dir()?)?;
    let session_root = create_private_session_runtime()?;
    let run_root = fs::canonicalize(session_root.path())?;
    let native_environment = ArchitectEnvironment::capture(architect_adapter)?;
    let protected_roots = if architect_adapter == ArchitectAdapter::Claude {
        capture_architect_protected_roots(&project_root, &native_environment)?
    } else {
        Vec::new()
    };
    let control_paths = ControlPaths::new(&run_root)?;
    let run_id = format!("run-{}", random_hex(16)?);
    let runtime_sources = SessionRuntimeSources::capture(
        native_environment.parent_environment.clone(),
        loaded.profiles.clone(),
    )?;
    let supervisor_endpoint = SessionSupervisorEndpoint::bind(
        control_paths.clone(),
        run_id.clone(),
        project_root.clone(),
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
            "project directory: {}",
            startup.project_root.display()
        )?;
        writeln!(
            stdout,
            "profile config: {} ({})",
            loaded.config_path.display(),
            if loaded.loaded_from_file {
                "loaded once"
            } else {
                "not present; built-in defaults"
            }
        )?;
        writeln!(stdout, "profile hash: {}", loaded.profiles.canonical_hash())?;
        write_profile_summary(&mut stdout, &loaded.profiles)?;
        if architect_adapter == ArchitectAdapter::Codex {
            writeln!(
                stdout,
                "worker runtime: codex-exec (one native process per turn)"
            )?;
        }
        writeln!(
            stdout,
            "task repositories: discovered from project documentation and bound only after explicit execution authorization; each developer commits directly there"
        )?;
        stdout.flush()?;
    }
    let registration_client = RegistrationClient::new(control_paths.registration_socket_path());
    validate_supervisor_sockets(&control_paths)?;
    let tools = ExactTools::discover(architect_adapter)?;
    let launch_id = random_hex(16)?;
    let binding_id = format!("architect-{launch_id}");
    let architect_name = format!("architect-{launch_id}");
    let launch_nonce = random_hex(32)?;
    let capability = random_hex(32)?;
    let paths = ArchitectLaunchPaths::create(&control_paths, &launch_id, architect_adapter)?;
    if architect_adapter == ArchitectAdapter::Claude {
        validate_path_isolation(&project_root, &paths, &tools)?;
    }
    let auth_source = if architect_adapter == ArchitectAdapter::Claude {
        let source = PrivateFileIdentity::capture(&discover_claude_auth_source()?)?;
        if paths_overlap(source.path(), &paths.state)
            || paths_overlap(source.path(), &paths.runtime)
        {
            bail!("native architect auth source overlaps architect writable state");
        }
        create_empty_private_file(&paths.auth_target)?;
        Some(source)
    } else {
        None
    };
    let preassigned_native_session_id =
        (architect_adapter == ArchitectAdapter::Claude).then(|| uuid::Uuid::new_v4().to_string());

    let process_birth = process_birth_identity(std::process::id())?;
    let relay_contract_hash = sha256_hex(&serde_json::to_vec(&tools.component)?);
    let relay_scope_hash = relay_runtime_scope_hash(&paths.runtime)?;
    let pending_version = match registration(
        &registration_client,
        &process_birth,
        RegistrationAction::CreateBinding {
            binding_id: binding_id.clone(),
            project_root: path_text("architect project directory", &project_root)?.into(),
            architect_name,
            architect_adapter: architect_adapter.contract_name().into(),
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
        project_root: project_root.clone(),
        run_root: control_paths.run_root().to_owned(),
        relay_socket_path: paths.relay_socket.clone(),
        registration_socket_path: control_paths.registration_socket_path(),
        control_socket_path: control_paths.socket_path(),
        relay_executable: tools.component.clone(),
        relay_runtime_scope_hash: relay_scope_hash.clone(),
        developer_adapter: developer_adapter.into(),
        reviewer_adapter: reviewer_adapter.into(),
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
        project_root: project_root.clone(),
        paths: paths.clone(),
        auth_source,
        adapter: architect_adapter,
        control_root: control_paths.run_root().to_owned(),
        host_runtime: native_environment.runtime_home.clone(),
        host_root: (architect_adapter == ArchitectAdapter::Claude)
            .then(|| {
                HostRootContract::capture(
                    &native_environment.cargo_bin_source,
                    &native_environment.rustup_home_source,
                )
            })
            .transpose()?,
        protected_roots,
    };
    let ArchitectLaunch {
        child: mut architect,
        native_pid: architect_pid,
        gate: architect_gate,
    } = match spawn_architect(
        &tools,
        &sandbox,
        &native_environment,
        &loaded.profiles.architect,
        preassigned_native_session_id.as_deref(),
    ) {
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
    let architect_birth = match process_birth_identity(architect_pid) {
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
            return Err(error).context("native architect process disappeared during registration");
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
            architect_pid,
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
            architect_pid,
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
    let live_architect_birth = process_birth_identity(architect_pid);
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
        bail!("architect launch identity changed during registration");
    }
    if let Some(gate) = architect_gate
        && let Err(error) = release_gate(gate)
    {
        terminate_child(&mut architect);
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[binding_version],
        );
        return Err(error).context("failed to release adapter launch gate");
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

fn apply_architect_cli_overrides(
    args: &ArchitectArgs,
    profiles: &mut SessionInvocationProfiles,
) -> Result<()> {
    let adapter = ArchitectAdapter::parse(&args.adapter)?;
    if profiles.architect.adapter() != adapter {
        bail!("configured architect profile differs from the selected adapter");
    }
    match &mut profiles.architect {
        ArchitectInvocationProfile::Codex { profile } => {
            if args.effort.is_some() {
                bail!("architect --effort is available only with the claude adapter");
            }
            if let Some(model) = &args.model {
                profile.model.clone_from(model);
            }
            if let Some(reasoning) = &args.reasoning {
                profile.reasoning_effort.clone_from(reasoning);
            }
            if let Some(sandbox) = &args.sandbox {
                profile.sandbox = match sandbox.as_str() {
                    "read-only" => CodexSandbox::ReadOnly,
                    "workspace-write" => CodexSandbox::WorkspaceWrite,
                    "danger-full-access" => CodexSandbox::DangerFullAccess,
                    _ => {
                        bail!(
                            "architect --sandbox must be read-only, workspace-write, or danger-full-access"
                        )
                    }
                };
            }
            if let Some(approval) = &args.approval {
                profile.approval_policy = match approval.as_str() {
                    "untrusted" => CodexApprovalPolicy::Untrusted,
                    "on-request" => CodexApprovalPolicy::OnRequest,
                    "never" => CodexApprovalPolicy::Never,
                    _ => {
                        bail!("architect --approval must be untrusted, on-request, or never")
                    }
                };
            }
        }
        ArchitectInvocationProfile::Claude { profile } => {
            if args.reasoning.is_some() || args.sandbox.is_some() || args.approval.is_some() {
                bail!(
                    "architect --reasoning, --sandbox, and --approval are available only with the codex adapter"
                );
            }
            if let Some(model) = &args.model {
                profile.model.clone_from(model);
            }
            if let Some(effort) = &args.effort {
                profile.effort.clone_from(effort);
            }
        }
    }
    profiles.validate()
}

fn write_profile_summary(
    output: &mut impl Write,
    profiles: &SessionInvocationProfiles,
) -> Result<()> {
    match &profiles.architect {
        ArchitectInvocationProfile::Codex { profile } => writeln!(
            output,
            "architect profile: codex model={} reasoning={} sandbox={} approval={}",
            profile.model,
            profile.reasoning_effort,
            profile.sandbox.as_str(),
            profile.approval_policy.as_str()
        )?,
        ArchitectInvocationProfile::Claude { profile } => writeln!(
            output,
            "architect profile: claude model={} effort={} dangerously_skip_permissions={}",
            profile.model, profile.effort, profile.dangerously_skip_permissions
        )?,
    }
    match &profiles.developer {
        DeveloperInvocationProfile::Codex { profile } => writeln!(
            output,
            "developer profile: codex model={} reasoning={} sandbox={} approval={}",
            profile.model,
            profile.reasoning_effort,
            profile.sandbox.as_str(),
            profile.approval_policy.as_str()
        )?,
        DeveloperInvocationProfile::Claude { profile } => writeln!(
            output,
            "developer profile: claude model={} effort={} dangerously_skip_permissions={}",
            profile.model, profile.effort, profile.dangerously_skip_permissions
        )?,
    }
    match &profiles.reviewer {
        ReviewerInvocationProfile::Codex { profile } => writeln!(
            output,
            "reviewer profile: codex model={} reasoning={} sandbox={} approval={}",
            profile.model,
            profile.reasoning_effort,
            profile.sandbox.as_str(),
            profile.approval_policy.as_str()
        )?,
        ReviewerInvocationProfile::Claude { profile } => writeln!(
            output,
            "reviewer profile: claude model={} effort={} dangerously_skip_permissions={}",
            profile.model, profile.effort, profile.dangerously_skip_permissions
        )?,
    }
    Ok(())
}

fn worker_adapter_bindings(
    architect_adapter: ArchitectAdapter,
    profiles: &SessionInvocationProfiles,
) -> (&'static str, &'static str) {
    // Adapter-independent: the worker lane is Codex-only for both entrypoints.
    let _ = (architect_adapter, profiles);
    (CODEX_TASK_WORKER_ADAPTER, CODEX_TASK_WORKER_ADAPTER)
}

fn validate_foreground_terminal() -> Result<()> {
    for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // SAFETY: isatty only inspects an integer descriptor.
        if unsafe { libc::isatty(fd) } != 1 {
            bail!("hcom arch requires stdin/stdout/stderr on a real terminal");
        }
    }
    let birth = process_birth_identity(std::process::id())?;
    if !process_owns_foreground_tty(std::process::id(), &birth)? {
        bail!("hcom arch must be launched by the foreground terminal process group");
    }
    let stdin = fs::metadata("/proc/self/fd/0")?;
    let stdout = fs::metadata("/proc/self/fd/1")?;
    let stderr = fs::metadata("/proc/self/fd/2")?;
    if stdin.rdev() != stdout.rdev() || stdin.rdev() != stderr.rdev() {
        bail!("hcom arch requires one exact foreground terminal");
    }
    Ok(())
}

fn canonical_project_directory(project: &Path) -> Result<PathBuf> {
    if !project.is_absolute() {
        bail!("architect current project directory must be absolute");
    }
    let canonical =
        fs::canonicalize(project).context("failed to canonicalize architect project directory")?;
    let metadata = fs::symlink_metadata(project)?;
    if canonical != project || metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("architect current project directory must already be canonical");
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
    architect: ExactArchitectTool,
    bwrap: Option<ExecutableIdentity>,
    component: ExecutableIdentity,
    hcom_executables: Vec<ExecutableIdentity>,
}

enum ExactArchitectTool {
    Codex,
    Claude(ExecutableIdentity),
}

impl ExactArchitectTool {
    fn executable_path(&self) -> &Path {
        match self {
            Self::Codex => Path::new("codex"),
            Self::Claude(executable) => &executable.canonical_path,
        }
    }

    fn revalidate(&self) -> Result<()> {
        match self {
            Self::Codex => Ok(()),
            Self::Claude(executable) => {
                revalidate_exact_tool(executable, CLAUDE_REVIEWER_CLI_VERSION)
            }
        }
    }
}

impl ExactTools {
    fn discover(adapter: ArchitectAdapter) -> Result<Self> {
        let architect = match adapter {
            ArchitectAdapter::Codex => ExactArchitectTool::Codex,
            ArchitectAdapter::Claude => {
                let executable = capture_exact_tool(
                    Path::new(CLAUDE_REVIEWER_EXECUTABLE),
                    CLAUDE_REVIEWER_CLI_VERSION,
                )?;
                validate_architect_claude_cli(&executable.canonical_path)?;
                ExactArchitectTool::Claude(executable)
            }
        };
        let (bwrap, hcom_executables) = match adapter {
            ArchitectAdapter::Codex => (None, Vec::new()),
            ArchitectAdapter::Claude => (
                Some(capture_exact_tool(
                    Path::new(BWRAP_EXECUTABLE),
                    BWRAP_VERSION,
                )?),
                discover_hcom_executables()?,
            ),
        };
        let component_path = resolve_component_path()?;
        let component = ExecutableIdentity::capture(component_path)?;
        Ok(Self {
            architect,
            bwrap,
            component,
            hcom_executables,
        })
    }

    fn revalidate(&self) -> Result<()> {
        self.architect.revalidate()?;
        if let Some(bwrap) = &self.bwrap {
            revalidate_exact_tool(bwrap, BWRAP_VERSION)?;
        }
        self.component.revalidate()?;
        for executable in &self.hcom_executables {
            executable.revalidate()?;
        }
        Ok(())
    }
}

fn discover_hcom_executables() -> Result<Vec<ExecutableIdentity>> {
    let mut candidates = BTreeSet::from([fs::canonicalize(std::env::current_exe()?)?]);
    if let Some(path) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&path).filter(|path| path.is_absolute()) {
            let candidate = directory.join("hcom");
            if fs::metadata(&candidate).is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            }) {
                candidates.insert(fs::canonicalize(candidate)?);
            }
        }
    }
    candidates
        .into_iter()
        .map(ExecutableIdentity::capture)
        .collect()
}

fn validate_architect_claude_cli(path: &Path) -> Result<()> {
    let output = Command::new(path)
        .arg("--help")
        .env_clear()
        .output()
        .context("failed to query architect Claude CLI capabilities")?;
    if !output.status.success() || !output.stderr.is_empty() || output.stdout.len() > 128 * 1024 {
        bail!("architect Claude CLI capability probe failed");
    }
    validate_cli_help_contract(
        "architect Claude CLI",
        &output.stdout,
        &[
            "--model",
            "--effort",
            "--session-id",
            "--name",
            "--tools",
            "--setting-sources",
            "--strict-mcp-config",
            "--mcp-config",
            "--disable-slash-commands",
            "--prompt-suggestions",
            "--no-chrome",
            "--dangerously-skip-permissions",
        ],
    )?;
    let help = std::str::from_utf8(&output.stdout)?;
    if !help.contains("(low, medium, high, xhigh, max)") || !help.contains("'opus'") {
        bail!("architect Claude CLI help omitted the default effort or model alias");
    }
    Ok(())
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
    hcom_state: PathBuf,
    native_config: PathBuf,
    xdg_config: PathBuf,
    xdg_state: PathBuf,
    xdg_cache: PathBuf,
    xdg_data: PathBuf,
    runtime: PathBuf,
    relay_socket: PathBuf,
    auth_target: PathBuf,
}

impl ArchitectLaunchPaths {
    fn create(control: &ControlPaths, launch_id: &str, adapter: ArchitectAdapter) -> Result<Self> {
        let state_parent = control.architect_state_root_path();
        let runtime_parent = control.architect_runtime_root_path();
        ensure_private_directory(&runtime_parent, true)?;
        let state = state_parent.join(launch_id);
        let runtime = runtime_parent.join(launch_id);
        ensure_private_directory(&runtime, false)?;
        let home = state.join("home");
        let hcom_state = state.join("hcom");
        let native_config = home.join(".claude");
        let xdg_config = state.join("xdg-config");
        let xdg_state = state.join("xdg-state");
        let xdg_cache = state.join("xdg-cache");
        let xdg_data = state.join("xdg-data");
        if adapter == ArchitectAdapter::Claude {
            ensure_private_directory(&state_parent, true)?;
            ensure_private_directory(&state, false)?;
            for directory in [
                &home,
                &hcom_state,
                &native_config,
                &xdg_config,
                &xdg_state,
                &xdg_cache,
                &xdg_data,
            ] {
                ensure_private_directory(directory, false)?;
            }
        }
        let auth_target = native_config.join(".credentials.json");
        Ok(Self {
            state,
            home,
            hcom_state,
            native_config: native_config.clone(),
            xdg_config,
            xdg_state,
            xdg_cache,
            xdg_data,
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target,
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

fn discover_claude_auth_source() -> Result<PathBuf> {
    let base = match std::env::var_os("CLAUDE_CONFIG_DIR") {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("home directory is unavailable"))?
            .join(".claude"),
    };
    let source = base.join(".credentials.json");
    let canonical =
        fs::canonicalize(&source).context("Claude architect credential is unavailable")?;
    if canonical != source {
        bail!("Claude architect credential must already be canonical");
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
            bail!("native architect auth source must be a regular non-symlink file");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("native architect auth source must already be canonical");
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = file.metadata()?;
        if metadata.dev() != link.dev() || metadata.ino() != link.ino() {
            bail!("native architect auth source changed while it was opened");
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
            bail!("native architect auth source must be a private current-user file");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self) -> Result<()> {
        if Self::capture(&self.path)? != *self {
            bail!("native architect auth source identity drifted before architect launch");
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct PrivateSocketIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    changed_seconds: i64,
    changed_nanos: i64,
    parent_device: u64,
    parent_inode: u64,
    parent_uid: u32,
    parent_mode: u32,
}

impl PrivateSocketIdentity {
    fn capture_if_present(path: &Path) -> Result<Option<Self>> {
        let link = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).context("failed to inspect inherited credential socket");
            }
        };
        if link.file_type().is_symlink() || !link.file_type().is_socket() {
            bail!("inherited credential endpoint must be a non-symlink Unix socket");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("inherited credential socket must already use its canonical path");
        }
        path_text("inherited credential socket", &canonical)?;
        let parent = canonical
            .parent()
            .ok_or_else(|| anyhow::anyhow!("inherited credential socket has no parent"))?;
        let parent_link = fs::symlink_metadata(parent)?;
        let parent_canonical = fs::canonicalize(parent)?;
        if parent_link.file_type().is_symlink()
            || !parent_link.is_dir()
            || parent_canonical != parent
        {
            bail!("inherited credential socket parent must be a canonical real directory");
        }
        let metadata = fs::metadata(&canonical)?;
        let parent_metadata = fs::metadata(parent)?;
        // SAFETY: geteuid has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        if metadata.dev() != link.dev()
            || metadata.ino() != link.ino()
            || metadata.uid() != current_uid
            || parent_metadata.uid() != current_uid
            || parent_metadata.permissions().mode() & 0o077 != 0
        {
            bail!(
                "inherited credential socket and its parent must be stable, private, and current-user owned"
            );
        }
        Ok(Some(Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
            changed_seconds: metadata.ctime(),
            changed_nanos: metadata.ctime_nsec(),
            parent_device: parent_metadata.dev(),
            parent_inode: parent_metadata.ino(),
            parent_uid: parent_metadata.uid(),
            parent_mode: parent_metadata.permissions().mode() & 0o777,
        }))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self) -> Result<()> {
        if Self::capture_if_present(&self.path)?.as_ref() != Some(self) {
            bail!("inherited credential socket identity drifted before architect launch");
        }
        Ok(())
    }
}

fn decode_dbus_address_value(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            bail!("DBUS_SESSION_BUS_ADDRESS contains an incomplete escape");
        }
        let nibble = |byte: u8| -> Result<u8> {
            match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                b'A'..=b'F' => Ok(byte - b'A' + 10),
                _ => bail!("DBUS_SESSION_BUS_ADDRESS contains an invalid escape"),
            }
        };
        decoded.push(nibble(bytes[index + 1])? << 4 | nibble(bytes[index + 2])?);
        index += 3;
    }
    let decoded = String::from_utf8(decoded)
        .context("DBUS_SESSION_BUS_ADDRESS socket path is not valid UTF-8")?;
    validate_environment_value("DBUS_SESSION_BUS_ADDRESS", &decoded)?;
    Ok(decoded)
}

fn dbus_session_socket_paths(address: &str) -> Result<Vec<PathBuf>> {
    validate_environment_value("DBUS_SESSION_BUS_ADDRESS", address)?;
    let mut paths = Vec::new();
    for endpoint in address.split(';') {
        let Some(options) = endpoint.strip_prefix("unix:") else {
            continue;
        };
        for option in options.split(',') {
            let Some(value) = option.strip_prefix("path=") else {
                continue;
            };
            paths.push(PathBuf::from(decode_dbus_address_value(value)?));
            if paths.len() > MAX_INHERITED_CREDENTIAL_SOCKETS {
                bail!("DBUS_SESSION_BUS_ADDRESS exposes too many filesystem sockets");
            }
        }
    }
    Ok(paths)
}

fn capture_inherited_credential_sockets(
    environment: &ParentEnvironment,
) -> Result<Vec<PrivateSocketIdentity>> {
    let mut candidates = BTreeSet::new();
    if let Some(value) = environment.unicode("SSH_AUTH_SOCK")?
        && !value.is_empty()
    {
        candidates.insert(PathBuf::from(value));
    }
    if let Some(value) = environment.unicode("GPG_AGENT_INFO")?
        && let Some(path) = value.split(':').next()
        && !path.is_empty()
    {
        candidates.insert(PathBuf::from(path));
    }
    if let Some(value) = environment.unicode("DBUS_SESSION_BUS_ADDRESS")? {
        candidates.extend(dbus_session_socket_paths(value)?);
    }
    if candidates.len() > MAX_INHERITED_CREDENTIAL_SOCKETS {
        bail!("parent environment exposes too many credential sockets");
    }
    candidates
        .into_iter()
        .filter_map(
            |path| match PrivateSocketIdentity::capture_if_present(&path) {
                Ok(Some(identity)) => Some(Ok(identity)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
        )
        .collect()
}

#[derive(Clone, PartialEq, Eq)]
struct ProtectedDirectoryIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
}

impl ProtectedDirectoryIdentity {
    fn capture_if_present(path: &Path, label: &str) -> Result<Option<Self>> {
        let link = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect {label} {}", path.display()));
            }
        };
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!(
                "{label} {} must be a real directory when present",
                path.display()
            );
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!(
                "{label} {} must already use its canonical path",
                path.display()
            );
        }
        let metadata = fs::metadata(path)?;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o002 != 0
        {
            bail!(
                "{label} {} must be current-user owned and not world-writable",
                path.display()
            );
        }
        Ok(Some(Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
        }))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn revalidate(&self, label: &str) -> Result<()> {
        if Self::capture_if_present(&self.path, label)?.as_ref() != Some(self) {
            bail!(
                "{label} {} identity drifted before architect launch",
                self.path.display()
            );
        }
        Ok(())
    }
}

fn capture_architect_protected_roots(
    project_root: &Path,
    environment: &ArchitectEnvironment,
) -> Result<Vec<ProtectedDirectoryIdentity>> {
    let home = PathBuf::from(
        environment
            .control_environment
            .get("HOME")
            .ok_or_else(|| anyhow::anyhow!("architect parent HOME disappeared"))?,
    );
    // Keep this path resolution aligned with paths::resolve_hcom_dir_from_env;
    // the architect module is also compiled by the standalone component crate,
    // where the retained-product paths module is intentionally unavailable.
    let active_hcom = match environment.parent_environment.unicode("HCOM_DIR")? {
        Some(value) if !value.is_empty() => {
            let expanded = if value.starts_with('~') {
                value.replacen('~', path_text("architect parent HOME", &home)?, 1)
            } else {
                value.to_owned()
            };
            let path = PathBuf::from(expanded);
            if path.is_relative() {
                project_root.join(path)
            } else {
                path
            }
        }
        _ => home.join(".hcom"),
    };
    let mut candidates = BTreeSet::from([
        home.join(".hcom"),
        home.join(".codex"),
        home.join(".claude"),
        active_hcom,
    ]);
    for name in ["CODEX_HOME", "CLAUDE_CONFIG_DIR"] {
        if let Some(value) = environment.control_environment.get(name) {
            candidates.insert(PathBuf::from(value));
        }
    }

    let mut roots = Vec::new();
    for candidate in candidates {
        if !candidate.is_absolute() {
            bail!("architect protected control root must be absolute");
        }
        if let Some(identity) =
            ProtectedDirectoryIdentity::capture_if_present(&candidate, "architect control root")?
        {
            if project_root.starts_with(identity.path()) {
                bail!("architect project directory is inside a protected control root");
            }
            roots.push(identity);
        }
    }
    Ok(roots)
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

fn validate_path_isolation(
    project_root: &Path,
    paths: &ArchitectLaunchPaths,
    tools: &ExactTools,
) -> Result<()> {
    for (left, right, label) in [
        (project_root, paths.state.as_path(), "project/state"),
        (project_root, paths.runtime.as_path(), "project/runtime"),
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
    let mut protected_executables = vec![&tools.component.canonical_path];
    if let Some(bwrap) = &tools.bwrap {
        protected_executables.push(&bwrap.canonical_path);
    }
    for executable in protected_executables {
        if executable.starts_with(&paths.state) || executable.starts_with(&paths.runtime) {
            bail!("architect writable roots contain a protected executable");
        }
    }
    if let ExactArchitectTool::Claude(executable) = &tools.architect
        && (executable.canonical_path.starts_with(&paths.state)
            || executable.canonical_path.starts_with(&paths.runtime))
    {
        bail!("architect writable roots contain a protected executable");
    }
    Ok(())
}

struct ArchitectEnvironment {
    parent_environment: ParentEnvironment,
    control_environment: BTreeMap<String, String>,
    inherited_credential_sockets: Vec<PrivateSocketIdentity>,
    runtime_home: PathBuf,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
}

impl ArchitectEnvironment {
    fn capture(adapter: ArchitectAdapter) -> Result<Self> {
        let parent_environment = ParentEnvironment::capture_current();
        if adapter == ArchitectAdapter::Codex {
            return Ok(Self {
                parent_environment,
                control_environment: BTreeMap::new(),
                inherited_credential_sockets: Vec::new(),
                runtime_home: PathBuf::new(),
                cargo_bin_source: PathBuf::new(),
                rustup_home_source: PathBuf::new(),
            });
        }
        let state_home = explicit_directory("XDG_STATE_HOME", dirs::state_dir)?;
        let config_home = explicit_directory("XDG_CONFIG_HOME", dirs::config_dir)?;
        let runtime_home = parent_environment
            .unicode("XDG_RUNTIME_DIR")?
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("XDG_RUNTIME_DIR is required"))?;
        let runtime_home = canonical_private_runtime(&runtime_home)?;
        let inherited_credential_sockets =
            capture_inherited_credential_sockets(&parent_environment)?;
        let mut control_environment: BTreeMap<String, String> = BTreeMap::new();
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
            if let Some(value) = parent_environment.unicode(name)? {
                validate_environment_value(name, value)?;
                control_environment.insert(name.into(), value.into());
            }
        }
        if control_environment
            .get("PATH")
            .is_none_or(|value| value.is_empty())
            || control_environment
                .get("TERM")
                .is_none_or(|value| value.is_empty())
        {
            bail!("architect environment requires PATH and TERM");
        }
        for name in [
            "HOME",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "CODEX_HOME",
            "CLAUDE_CONFIG_DIR",
        ] {
            if let Some(value) = parent_environment.unicode(name)? {
                validate_environment_value(name, value)?;
                if name == "HOME" || !value.is_empty() {
                    control_environment.insert(name.into(), value.into());
                }
            }
        }
        if control_environment
            .get("HOME")
            .is_none_or(|value| value.is_empty())
        {
            bail!("architect control environment requires parent HOME");
        }
        let parent_home = PathBuf::from(
            control_environment
                .get("HOME")
                .expect("checked architect parent HOME"),
        );
        let cargo_bin_source = parent_environment
            .unicode("CARGO_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| parent_home.join(".cargo"))
            .join("bin");
        let rustup_home_source = parent_environment
            .unicode("RUSTUP_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| parent_home.join(".rustup"));
        Ok(Self {
            parent_environment,
            control_environment,
            inherited_credential_sockets,
            runtime_home,
            cargo_bin_source,
            rustup_home_source,
        })
    }

    fn sandbox_values(
        &self,
        paths: &ArchitectLaunchPaths,
        adapter: ArchitectAdapter,
    ) -> Result<BTreeMap<String, String>> {
        if adapter == ArchitectAdapter::Codex {
            return Ok(BTreeMap::new());
        }
        let mut values = BTreeMap::new();
        values.insert(
            "HCOM_DIR".into(),
            path_text("architect isolated hcom state", &paths.hcom_state)?.into(),
        );
        match adapter {
            ArchitectAdapter::Codex => unreachable!("Codex returned before sandbox overlays"),
            ArchitectAdapter::Claude => {
                let home = self
                    .control_environment
                    .get("HOME")
                    .ok_or_else(|| anyhow::anyhow!("architect parent HOME disappeared"))?
                    .clone();
                values.insert("HOME".into(), home);
                values.insert(
                    "CLAUDE_CONFIG_DIR".into(),
                    path_text("architect isolated Claude config", &paths.native_config)?.into(),
                );
                for (name, label, path) in [
                    (
                        "XDG_CONFIG_HOME",
                        "architect isolated XDG config",
                        &paths.xdg_config,
                    ),
                    (
                        "XDG_STATE_HOME",
                        "architect isolated XDG state",
                        &paths.xdg_state,
                    ),
                    (
                        "XDG_CACHE_HOME",
                        "architect isolated XDG cache",
                        &paths.xdg_cache,
                    ),
                    (
                        "XDG_DATA_HOME",
                        "architect isolated XDG data",
                        &paths.xdg_data,
                    ),
                ] {
                    values.insert(name.into(), path_text(label, path)?.into());
                }
                for (name, value) in [
                    ("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS", "1"),
                    ("CLAUDE_CODE_DISABLE_FAST_MODE", "1"),
                    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
                    ("CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION", "false"),
                ] {
                    values.insert(name.into(), value.into());
                }
                values.insert(
                    "TMPDIR".into(),
                    path_text("architect private temporary directory", &paths.runtime)?.into(),
                );
                values.insert(
                    "XDG_RUNTIME_DIR".into(),
                    path_text("architect private runtime directory", &paths.runtime)?.into(),
                );
                for name in ["CARGO_HOME", "RUSTUP_HOME"] {
                    if let Some(value) = self.control_environment.get(name) {
                        values.insert(name.into(), value.clone());
                    }
                }
            }
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
    if value.len() > 16 * 1024
        || value
            .chars()
            .any(|character| character == '\0' || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("architect environment value {name} is invalid");
    }
    Ok(())
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
    project_root: PathBuf,
    paths: ArchitectLaunchPaths,
    auth_source: Option<PrivateFileIdentity>,
    adapter: ArchitectAdapter,
    control_root: PathBuf,
    host_runtime: PathBuf,
    host_root: Option<HostRootContract>,
    protected_roots: Vec<ProtectedDirectoryIdentity>,
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
        tools.revalidate()?;
        if !matches!(
            (&tools.architect, self.adapter),
            (ExactArchitectTool::Claude(_), ArchitectAdapter::Claude)
        ) {
            bail!("outer architect sandbox is only available for the Claude adapter");
        }

        let auth_source = self
            .auth_source
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Claude architect auth source is unavailable"))?;
        let host_root = self
            .host_root
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Claude architect host contract is unavailable"))?;
        auth_source.revalidate()?;
        if paths_overlap(&self.paths.runtime, &self.host_runtime) {
            bail!("architect relay runtime overlaps the host runtime directory");
        }
        for root in &self.protected_roots {
            root.revalidate("architect control root")?;
        }
        for socket in &environment.inherited_credential_sockets {
            socket.revalidate()?;
            if socket.path().starts_with(&self.paths.state)
                || socket.path().starts_with(&self.paths.runtime)
                || socket.path().starts_with(&self.control_root)
                || self
                    .protected_roots
                    .iter()
                    .any(|root| socket.path().starts_with(root.path()))
            {
                bail!("inherited credential socket overlaps hcom architect control state");
            }
        }
        let tmp = Path::new("/tmp");
        let masked_dirs: Vec<&Path> = if self.host_runtime.starts_with(tmp) {
            vec![tmp]
        } else {
            vec![tmp, &self.host_runtime]
        };
        let mut extra_writable_dirs = vec![self.paths.hcom_state.as_path()];
        if self.adapter == ArchitectAdapter::Claude {
            extra_writable_dirs.extend([
                self.paths.xdg_config.as_path(),
                self.paths.xdg_state.as_path(),
                self.paths.xdg_cache.as_path(),
                self.paths.xdg_data.as_path(),
            ]);
        }
        let protected_roots: Vec<&Path> = self
            .protected_roots
            .iter()
            .map(ProtectedDirectoryIdentity::path)
            .collect();
        let mut read_only_files =
            vec![tools.component.canonical_path.as_path(), auth_source.path()];
        if let ExactArchitectTool::Claude(executable) = &tools.architect {
            read_only_files.push(executable.canonical_path.as_path());
        }
        read_only_files.extend(
            tools
                .hcom_executables
                .iter()
                .map(|executable| executable.canonical_path.as_path()),
        );
        read_only_files.extend(
            environment
                .inherited_credential_sockets
                .iter()
                .map(PrivateSocketIdentity::path),
        );
        read_only_files.sort_unstable();
        read_only_files.dedup();
        let mut argv = host_root.host_root_argv(HostRootMounts {
            isolated_home: &self.paths.home,
            native_config: &self.paths.native_config,
            launch_cwd: &self.project_root,
            artifact_dir: &self.paths.runtime,
            auth_source: auth_source.path(),
            auth_target: &self.paths.auth_target,
            readable_roots: &protected_roots,
            writable_roots: &[&self.project_root],
            read_only_files: &read_only_files,
            extra_writable_dirs: &extra_writable_dirs,
            host_root_access: HostRootAccess::ReadWrite,
            masked_dirs: &masked_dirs,
        })?;
        for (name, value) in environment.sandbox_values(&self.paths, self.adapter)? {
            argv.extend(["--setenv".into(), name, value]);
        }
        argv.extend([
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
        Ok(argv)
    }
}

struct ArchitectLaunch {
    child: Child,
    native_pid: u32,
    gate: Option<OwnedFd>,
}

fn spawn_architect(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    environment: &ArchitectEnvironment,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<ArchitectLaunch> {
    match sandbox.adapter {
        ArchitectAdapter::Codex => {
            spawn_native_codex_architect(tools, sandbox, profile, preassigned_native_session_id)
        }
        ArchitectAdapter::Claude => {
            let (mut child, gate, info) = spawn_blocked_claude_architect(
                tools,
                sandbox,
                environment,
                profile,
                preassigned_native_session_id,
            )?;
            let native_pid = match read_bwrap_info(&info, true) {
                Ok(info) => info.child_pid,
                Err(error) => {
                    terminate_child(&mut child);
                    return Err(error);
                }
            };
            Ok(ArchitectLaunch {
                child,
                native_pid,
                gate: Some(gate),
            })
        }
    }
}

fn spawn_native_codex_architect(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<ArchitectLaunch> {
    profile.validate()?;
    if profile.adapter() != ArchitectAdapter::Codex
        || sandbox.adapter != ArchitectAdapter::Codex
        || !matches!(tools.architect, ExactArchitectTool::Codex)
    {
        bail!("native Codex architect launch received a different adapter");
    }
    if tools.bwrap.is_some() || !tools.hcom_executables.is_empty() {
        bail!("native Codex architect unexpectedly carries outer-sandbox tools");
    }
    tools.revalidate()?;
    let argv = architect_native_argv(tools, sandbox, profile, preassigned_native_session_id)?;
    validate_native_argv(
        &argv,
        tools,
        sandbox,
        profile,
        preassigned_native_session_id,
    )?;
    let expected_parent = std::process::id() as libc::pid_t;
    let mut command = Command::new(&argv[0]);
    command.args(&argv[1..]).current_dir(&sandbox.project_root);
    // Do not clear, filter, or replace the foreground Codex environment. A
    // native Architect must see exactly what `codex` launched from this
    // project in the parent terminal would see.
    // SAFETY: pre_exec performs only async-signal-safe prctl/getppid calls.
    unsafe {
        command.pre_exec(move || {
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
        .context("failed to spawn native Codex architect")?;
    let native_pid = child.id();
    Ok(ArchitectLaunch {
        child,
        native_pid,
        gate: None,
    })
}

fn spawn_blocked_claude_architect(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    environment: &ArchitectEnvironment,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<(Child, OwnedFd, OwnedFd)> {
    profile.validate()?;
    if profile.adapter() != ArchitectAdapter::Claude || sandbox.adapter != ArchitectAdapter::Claude
    {
        bail!("blocked architect launch is only available for the Claude adapter");
    }
    tools.revalidate()?;
    let (gate_read, gate_write) = pipe_cloexec()?;
    let (info_read, info_write) = pipe_cloexec()?;
    let bwrap = tools
        .bwrap
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Claude architect bwrap is unavailable"))?;
    let mut command = Command::new(&bwrap.canonical_path);
    let gate_fd = gate_read.as_raw_fd();
    let info_fd = info_write.as_raw_fd();
    let native_argv =
        architect_native_argv(tools, sandbox, profile, preassigned_native_session_id)?;
    validate_native_argv(
        &native_argv,
        tools,
        sandbox,
        profile,
        preassigned_native_session_id,
    )?;
    let mut argv = sandbox.outer_argv(environment, tools, gate_fd, info_fd)?;
    argv.push("--".into());
    argv.extend(native_argv);
    command
        .args(&argv)
        .env_clear()
        .envs(environment.parent_environment.iter());
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

fn architect_native_argv(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<Vec<String>> {
    let executable = path_text(
        "architect native executable",
        tools.architect.executable_path(),
    )?
    .into();
    match profile {
        ArchitectInvocationProfile::Codex { profile } => {
            if preassigned_native_session_id.is_some() {
                bail!("Codex architect cannot receive a preassigned native session");
            }
            let mut argv = vec![
                executable,
                "--model".into(),
                profile.model.clone(),
                "--config".into(),
                profile.reasoning_config_argument(),
                "--sandbox".into(),
                profile.sandbox.as_str().into(),
                "--ask-for-approval".into(),
                profile.approval_policy.as_str().into(),
                "--cd".into(),
                path_text("architect project directory", &sandbox.project_root)?.into(),
                "--no-alt-screen".into(),
            ];
            argv.extend(codex_control_mcp_overrides(tools, sandbox)?);
            Ok(argv)
        }
        ArchitectInvocationProfile::Claude { profile } => {
            let native_session_id = preassigned_native_session_id
                .ok_or_else(|| anyhow::anyhow!("Claude architect requires a native session id"))?;
            crate::worker::contract::validate_native_session_id(native_session_id)?;
            let mcp_config = serde_json::to_string(&serde_json::json!({
                "mcpServers": {
                    "hcom_session_task_control": {
                        "type": "stdio",
                        "command": path_text(
                            "architect MCP component",
                            &tools.component.canonical_path,
                        )?,
                        "args": [
                            "relay",
                            "--socket",
                            path_text(
                                "architect relay socket",
                                &sandbox.paths.relay_socket,
                            )?,
                        ],
                    }
                }
            }))?;
            let mut argv = vec![
                executable,
                "--model".into(),
                profile.model.clone(),
                "--effort".into(),
                profile.effort.clone(),
                "--session-id".into(),
                native_session_id.into(),
                "--name".into(),
                "hcom-architect".into(),
                "--tools".into(),
                "Bash,Read,Write,Edit,Glob,Grep".into(),
                "--setting-sources".into(),
                String::new(),
                "--strict-mcp-config".into(),
                "--mcp-config".into(),
                mcp_config,
                "--disable-slash-commands".into(),
                "--prompt-suggestions".into(),
                "false".into(),
                "--no-chrome".into(),
            ];
            if profile.dangerously_skip_permissions {
                argv.push("--dangerously-skip-permissions".into());
            }
            Ok(argv)
        }
    }
}

fn codex_control_mcp_overrides(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
) -> Result<Vec<String>> {
    let prefix = "mcp_servers.hcom_session_task_control";
    let command = toml::Value::String(
        path_text("architect MCP component", &tools.component.canonical_path)?.into(),
    )
    .to_string();
    let args = toml::Value::Array(vec![
        toml::Value::String("relay".into()),
        toml::Value::String("--socket".into()),
        toml::Value::String(
            path_text("architect relay socket", &sandbox.paths.relay_socket)?.into(),
        ),
    ])
    .to_string();
    // Replace this one hcom-owned leaf as a whole. Native user configuration
    // remains loaded, including every other MCP server, while a stale or
    // user-defined table under the reserved name cannot leave incompatible
    // transport fields merged into the control server.
    let value = format!(
        "{prefix}={{ command = {command}, args = {args}, startup_timeout_sec = 10, \
         tool_timeout_sec = {CODEX_CONTROL_TOOL_TIMEOUT_SECS}, enabled = true, \
         default_tools_approval_mode = \"approve\" }}"
    );
    Ok(vec!["--config".into(), value])
}

fn validate_native_argv(
    native: &[String],
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<()> {
    let expected = architect_native_argv(tools, sandbox, profile, preassigned_native_session_id)?;
    if native != expected {
        bail!("architect native argv drifted from its exact blank profile");
    }
    if native
        .iter()
        .any(|argument| argument == "-" || argument == "--hcom-prompt")
    {
        bail!("architect native argv contains prompt material");
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
    ipc_namespace: Option<u64>,
    mnt_namespace: u64,
    pid_namespace: Option<u64>,
    uts_namespace: Option<u64>,
}

fn read_bwrap_info(fd: &OwnedFd, require_isolated_namespaces: bool) -> Result<BwrapInfo> {
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
            return parse_bwrap_info(&bytes, require_isolated_namespaces)
                .context("bwrap closed its info fd with an invalid report");
        }
        bytes.extend_from_slice(&buffer[..count as usize]);
    }
}

fn parse_bwrap_info(bytes: &[u8], require_isolated_namespaces: bool) -> Result<BwrapInfo> {
    let info: BwrapInfo =
        serde_json::from_slice(bytes).context("bwrap launch info is not strict JSON")?;
    if info.child_pid <= 1 || info.mnt_namespace == 0 {
        bail!("bwrap returned an invalid launch identity");
    }
    if require_isolated_namespaces
        && [info.ipc_namespace, info.pid_namespace, info.uts_namespace]
            .into_iter()
            .any(|namespace| namespace.is_none_or(|namespace| namespace == 0))
    {
        bail!("bwrap omitted a required isolated namespace identity");
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
                bail!("architect bridge did not revoke its binding after native architect exited");
            }
            if let Some(status) = bridge.try_wait()? {
                bail!("architect bridge exited before native architect: {status}");
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
    use crate::worker::profile::CodexInvocationProfile;
    use std::ffi::OsString;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::net::UnixListener;
    use std::os::unix::process::CommandExt;

    const BLANK_HELPER_ROOT: &str = "HCOM_PHASE7_BLANK_HELPER_ROOT";
    const BLANK_HELPER_CONTROL_HOME: &str = "HCOM_PHASE7_BLANK_HELPER_CONTROL_HOME";
    const ENVIRONMENT_HELPER_ROOT: &str = "HCOM_PHASE9_ENVIRONMENT_HELPER_ROOT";
    const RUNTIME_MODE_HELPER: &str = "HCOM_PHASE9_RUNTIME_MODE_HELPER";
    const TEST_CLAUDE_SESSION: &str = "019fa976-e270-7a92-b5f0-6d3d8a0ad3f4";

    #[test]
    fn architect_environment_helper_process() {
        let Some(root) = std::env::var_os(ENVIRONMENT_HELPER_ROOT).map(PathBuf::from) else {
            return;
        };
        let environment = ArchitectEnvironment::capture(ArchitectAdapter::Claude).unwrap();
        assert_eq!(
            environment.parent_environment.unicode("COLORTERM").unwrap(),
            Some("")
        );
        assert_eq!(
            environment.control_environment.get("COLORTERM"),
            Some(&String::new())
        );
        assert!(!environment.control_environment.contains_key("CARGO_HOME"));
        assert!(!environment.control_environment.contains_key("CODEX_HOME"));
        let protected: BTreeSet<_> = capture_architect_protected_roots(&root, &environment)
            .unwrap()
            .into_iter()
            .map(|identity| identity.path)
            .collect();
        let home = root.join("home");
        assert_eq!(
            protected,
            BTreeSet::from([
                home.join(".hcom"),
                home.join(".codex"),
                home.join(".claude"),
                root.join("custom-hcom"),
            ])
        );
    }

    #[test]
    fn empty_optional_terminal_and_tool_overrides_do_not_block_blank_launch() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let home = root.join("home");
        let config = root.join("config");
        let state = root.join("state");
        let runtime = root.join("runtime");
        let custom_hcom = root.join("custom-hcom");
        for directory in [
            &home,
            &config,
            &state,
            &runtime,
            &custom_hcom,
            &home.join(".hcom"),
            &home.join(".codex"),
            &home.join(".claude"),
        ] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for directory in [
            &custom_hcom,
            &home.join(".hcom"),
            &home.join(".codex"),
            &home.join(".claude"),
        ] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o775)).unwrap();
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "architect::launch::tests::architect_environment_helper_process",
                "--nocapture",
            ])
            .env_clear()
            .env(ENVIRONMENT_HELPER_ROOT, &root)
            .env("HOME", &home)
            .env("PATH", "/usr/bin:/bin")
            .env("TERM", "xterm-256color")
            .env("COLORTERM", "")
            .env("CARGO_HOME", "")
            .env("CODEX_HOME", "")
            .env("HCOM_DIR", &custom_hcom)
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_STATE_HOME", &state)
            .env("XDG_RUNTIME_DIR", &runtime)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "environment helper failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let unsafe_root = root.join("world-writable-control");
        fs::create_dir(&unsafe_root).unwrap();
        fs::set_permissions(&unsafe_root, fs::Permissions::from_mode(0o777)).unwrap();
        let error =
            match ProtectedDirectoryIdentity::capture_if_present(&unsafe_root, "test control root")
            {
                Ok(_) => panic!("world-writable control root was accepted"),
                Err(error) => error.to_string(),
            };
        assert!(error.contains(&unsafe_root.to_string_lossy().into_owned()));
        assert!(error.contains("not world-writable"));
    }

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
    fn dbus_session_socket_paths_are_strict_and_bounded() {
        assert_eq!(
            dbus_session_socket_paths(
                "tcp:host=localhost;unix:path=/run/user/1000/keyring%2Cbus%3Bprimary,guid=abcd"
            )
            .unwrap(),
            vec![PathBuf::from("/run/user/1000/keyring,bus;primary")]
        );
        assert!(dbus_session_socket_paths("unix:path=/run/user/1000/%").is_err());
        assert!(dbus_session_socket_paths("unix:path=/run/user/1000/%GG").is_err());
        let oversized = (0..=MAX_INHERITED_CREDENTIAL_SOCKETS)
            .map(|index| format!("unix:path=/run/user/1000/bus-{index}"))
            .collect::<Vec<_>>()
            .join(";");
        assert!(dbus_session_socket_paths(&oversized).is_err());
    }

    #[test]
    fn inherited_credential_socket_identity_rejects_replacement_and_public_parent() {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let socket = temp.path().join("agent.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let identity = PrivateSocketIdentity::capture_if_present(&socket)
            .unwrap()
            .unwrap();
        identity.revalidate().unwrap();

        drop(listener);
        fs::remove_file(&socket).unwrap();
        let _replacement = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(identity.revalidate().is_err());

        let public = temp.path().join("public");
        fs::create_dir(&public).unwrap();
        fs::set_permissions(&public, fs::Permissions::from_mode(0o777)).unwrap();
        let public_socket = public.join("agent.sock");
        let _public_listener = UnixListener::bind(&public_socket).unwrap();
        assert!(PrivateSocketIdentity::capture_if_present(&public_socket).is_err());
    }

    #[test]
    fn pinned_claude_root_help_matches_architect_command_contract_when_installed() {
        let path = Path::new(CLAUDE_REVIEWER_EXECUTABLE);
        if path.exists() {
            validate_architect_claude_cli(path).unwrap();
        }
    }

    #[test]
    fn codex_control_server_is_an_additive_cli_overlay_not_a_private_config() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project = root.join("project");
        fs::create_dir(&project).unwrap();
        let native_config = root.join("codex-home");
        fs::create_dir(&native_config).unwrap();
        let paths = ArchitectLaunchPaths {
            state: root.join("state"),
            home: root.join("home"),
            hcom_state: root.join("state/hcom"),
            native_config,
            xdg_config: root.join("xdg-config"),
            xdg_state: root.join("xdg-state"),
            xdg_cache: root.join("xdg-cache"),
            xdg_data: root.join("xdg-data"),
            runtime: root.join("runtime"),
            relay_socket: root.join("runtime/relay.sock"),
            auth_target: root.join("codex-home/auth.json"),
        };
        let executable = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        let tools = ExactTools {
            architect: ExactArchitectTool::Codex,
            bwrap: None,
            component: executable.clone(),
            hcom_executables: Vec::new(),
        };
        let sandbox = ArchitectSandbox {
            project_root: project,
            paths,
            auth_source: None,
            adapter: ArchitectAdapter::Codex,
            control_root: root.join("control"),
            host_runtime: root.join("host-runtime"),
            host_root: None,
            protected_roots: Vec::new(),
        };
        let overrides = codex_control_mcp_overrides(&tools, &sandbox).unwrap();
        let encoded = overrides.join("\n");
        assert_eq!(
            overrides
                .iter()
                .filter(|value| *value == "--config")
                .count(),
            1
        );
        assert!(encoded.contains("mcp_servers.hcom_session_task_control={"));
        assert!(encoded.contains("command = "));
        assert!(encoded.contains("default_tools_approval_mode = \"approve\""));
        assert!(!encoded.contains("projects."));
        assert!(!encoded.contains("trust_level"));
        assert!(!sandbox.paths.native_config.join("config.toml").exists());
    }

    #[test]
    fn explicit_architect_cli_overrides_toml_profile_only() {
        let args = ArchitectArgs::try_parse_from([
            "hcom arch",
            "codex",
            "--model",
            "gpt-5.6-sol-cli",
            "--reasoning",
            "max",
            "--sandbox",
            "danger-full-access",
            "--ask-for-approval",
            "never",
        ])
        .unwrap();
        let mut profiles = SessionInvocationProfiles::default();
        let developer_before = profiles.developer.clone();
        let reviewer_before = profiles.reviewer.clone();
        apply_architect_cli_overrides(&args, &mut profiles).unwrap();
        let architect = profiles.architect.codex().unwrap();
        assert_eq!(architect.model, "gpt-5.6-sol-cli");
        assert_eq!(architect.reasoning_effort, "max");
        assert_eq!(architect.sandbox, CodexSandbox::DangerFullAccess);
        assert_eq!(architect.approval_policy, CodexApprovalPolicy::Never);
        assert_eq!(profiles.developer, developer_before);
        assert_eq!(profiles.reviewer, reviewer_before);
        let reviewer = profiles.reviewer.claude().unwrap();
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
    }

    #[test]
    fn claude_cli_overrides_do_not_change_the_implicit_reviewer() {
        let args = ArchitectArgs::try_parse_from([
            "hcom arch",
            "claude",
            "--model",
            "sonnet",
            "--effort",
            "max",
        ])
        .unwrap();
        let mut profiles = SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude);
        let developer_before = profiles.developer.clone();
        let reviewer_before = profiles.reviewer.clone();
        apply_architect_cli_overrides(&args, &mut profiles).unwrap();
        let architect = profiles.architect.claude().unwrap();
        let reviewer = profiles.reviewer.claude().unwrap();
        assert_eq!(architect.model, "sonnet");
        assert_eq!(architect.effort, "max");
        assert_eq!(profiles.reviewer, reviewer_before);
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
        assert_eq!(profiles.developer, developer_before);

        let invalid =
            ArchitectArgs::try_parse_from(["hcom arch", "claude", "--reasoning", "xhigh"]).unwrap();
        assert!(apply_architect_cli_overrides(&invalid, &mut profiles).is_err());
    }

    #[test]
    fn explicit_reviewers_are_not_replaced_by_architect_cli_overrides() {
        let codex_args = ArchitectArgs::try_parse_from([
            "hcom arch",
            "codex",
            "--model",
            "gpt-5.6-sol-cli",
            "--reasoning",
            "max",
        ])
        .unwrap();
        let mut codex_profiles = SessionInvocationProfiles {
            reviewer: ReviewerInvocationProfile::Claude {
                profile: crate::worker::profile::ClaudeInvocationProfile {
                    model: "sonnet".into(),
                    effort: "low".into(),
                    dangerously_skip_permissions: false,
                },
            },
            ..SessionInvocationProfiles::default()
        };
        let reviewer_before = codex_profiles.reviewer.clone();
        apply_architect_cli_overrides(&codex_args, &mut codex_profiles).unwrap();
        assert_eq!(codex_profiles.reviewer, reviewer_before);

        let claude_args = ArchitectArgs::try_parse_from([
            "hcom arch",
            "claude",
            "--model",
            "sonnet",
            "--effort",
            "max",
        ])
        .unwrap();
        let mut claude_profiles = SessionInvocationProfiles {
            reviewer: ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile {
                    model: "gpt-5.6-sol-reviewer".into(),
                    reasoning_effort: "low".into(),
                    sandbox: CodexSandbox::WorkspaceWrite,
                    approval_policy: CodexApprovalPolicy::OnRequest,
                },
            },
            ..SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude)
        };
        let reviewer_before = claude_profiles.reviewer.clone();
        apply_architect_cli_overrides(&claude_args, &mut claude_profiles).unwrap();
        assert_eq!(claude_profiles.reviewer, reviewer_before);
    }

    #[test]
    fn startup_summary_reports_the_claude_reviewer_default_for_both_architects() {
        for adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            let profiles = SessionInvocationProfiles::for_architect(adapter);
            let mut output = Vec::new();
            write_profile_summary(&mut output, &profiles).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(
                output.contains(
                    "reviewer profile: claude model=opus effort=xhigh dangerously_skip_permissions=true"
                ),
                "summary={output}"
            );
        }
    }

    #[test]
    fn both_public_entrypoints_bind_the_same_codex_only_worker_lane() {
        for adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            let profiles = SessionInvocationProfiles::for_task_lane(adapter).unwrap();
            assert_eq!(
                worker_adapter_bindings(adapter, &profiles),
                (CODEX_TASK_WORKER_ADAPTER, CODEX_TASK_WORKER_ADAPTER),
                "{adapter:?} must bind the Codex-only worker lane"
            );
        }
    }

    #[test]
    fn configured_claude_workers_fail_closed_for_both_entrypoints() {
        for adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            for section in ["developer", "reviewer"] {
                let temp = tempfile::tempdir().unwrap();
                let config = temp.path().join("config.toml");
                std::fs::write(
                    &config,
                    format!(
                        "[architect.{section}]\nadapter = \"claude\"\nmodel = \"opus\"\n\
                         effort = \"xhigh\"\ndangerously_skip_permissions = true\n"
                    ),
                )
                .unwrap();
                std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
                let error = match load_task_lane_profiles(&config, adapter) {
                    Ok(_) => panic!("{adapter:?}/{section}: Claude worker must be refused"),
                    Err(error) => error,
                };
                assert!(
                    error.to_string().to_lowercase().contains("claude"),
                    "{adapter:?}/{section}: {error}"
                );
            }
        }
    }

    #[test]
    fn native_profile_has_no_prompt_or_secret_transport() {
        let profile = CodexInvocationProfile::architect_default();
        let native: Vec<String> = vec![
            "codex".into(),
            "--model".into(),
            profile.model.clone(),
            "--config".into(),
            profile.reasoning_config_argument(),
            "--sandbox".into(),
            profile.sandbox.as_str().into(),
            "--ask-for-approval".into(),
            profile.approval_policy.as_str().into(),
            "--cd".into(),
            "/repo".into(),
            "--no-alt-screen".into(),
        ];
        assert!(!native.iter().any(|argument| argument == "-"));
        assert!(!native.iter().any(|argument| argument.contains("nonce")));
        assert!(
            !native
                .iter()
                .any(|argument| argument.contains("capability"))
        );
        assert!(!native.iter().any(|argument| {
            matches!(
                argument.as_str(),
                "--strict-config" | "--disable" | "--ignore-user-config" | "--ignore-rules"
            )
        }));
    }

    #[test]
    fn claude_native_profile_is_blank_and_binds_only_the_exact_mcp_relay() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project = root.join("project");
        let state = root.join("state");
        let home = state.join("home");
        let native_config = home.join(".claude");
        let runtime = root.join("runtime");
        let cargo_bin = root.join("cargo-bin");
        let rustup_home = root.join("rustup-home");
        for directory in [
            &project,
            &state,
            &home,
            &native_config,
            &runtime,
            &cargo_bin,
            &rustup_home,
        ] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let auth = root.join("auth");
        fs::write(&auth, b"auth").unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        let current = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        let tools = ExactTools {
            architect: ExactArchitectTool::Claude(current.clone()),
            bwrap: Some(current.clone()),
            component: current.clone(),
            hcom_executables: vec![current],
        };
        let paths = ArchitectLaunchPaths {
            state,
            home,
            hcom_state: root.join("state/hcom"),
            native_config: native_config.clone(),
            xdg_config: root.join("xdg-config"),
            xdg_state: root.join("xdg-state"),
            xdg_cache: root.join("xdg-cache"),
            xdg_data: root.join("xdg-data"),
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: native_config.join(".credentials.json"),
        };
        let sandbox = ArchitectSandbox {
            project_root: project,
            paths,
            auth_source: Some(PrivateFileIdentity::capture(&auth).unwrap()),
            adapter: ArchitectAdapter::Claude,
            control_root: root.join("control"),
            host_runtime: root.clone(),
            host_root: Some(HostRootContract::capture(&cargo_bin, &rustup_home).unwrap()),
            protected_roots: Vec::new(),
        };
        let profile = ArchitectInvocationProfile::Claude {
            profile: crate::worker::profile::ClaudeInvocationProfile::architect_default(),
        };
        let argv =
            architect_native_argv(&tools, &sandbox, &profile, Some(TEST_CLAUDE_SESSION)).unwrap();
        assert_eq!(
            argv.last().map(String::as_str),
            Some("--dangerously-skip-permissions")
        );
        assert!(argv.windows(2).any(|pair| pair == ["--model", "opus"]));
        assert!(argv.windows(2).any(|pair| pair == ["--effort", "xhigh"]));
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--session-id", TEST_CLAUDE_SESSION])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--setting-sources", ""])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--tools", "Bash,Read,Write,Edit,Glob,Grep"])
        );
        assert!(!argv.iter().any(|argument| argument == "-"));
        let mcp = argv
            .windows(2)
            .find(|pair| pair[0] == "--mcp-config")
            .map(|pair| &pair[1])
            .unwrap();
        let mcp: serde_json::Value = serde_json::from_str(mcp).unwrap();
        let server = &mcp["mcpServers"]["hcom_session_task_control"];
        assert_eq!(
            server["command"],
            tools.component.canonical_path.to_str().unwrap()
        );
        assert_eq!(
            server["args"],
            serde_json::json!([
                "relay",
                "--socket",
                sandbox.paths.relay_socket.to_str().unwrap()
            ])
        );
    }

    #[test]
    fn blank_launch_helper_process() {
        let Some(root) = std::env::var_os(BLANK_HELPER_ROOT).map(PathBuf::from) else {
            return;
        };
        let control_home = PathBuf::from(std::env::var_os(BLANK_HELPER_CONTROL_HOME).unwrap());
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
            hcom_state: root.join("architect-state/hcom"),
            native_config: codex_home,
            xdg_config: root.join("xdg-config"),
            xdg_state: root.join("xdg-state"),
            xdg_cache: root.join("xdg-cache"),
            xdg_data: root.join("xdg-data"),
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: root.join("architect-state/home/.codex/auth.json"),
        };
        let fake_codex = fs::canonicalize(root.join("codex")).unwrap();
        let codex = ExecutableIdentity::capture(fake_codex).unwrap();
        let tools = ExactTools {
            component: codex.clone(),
            architect: ExactArchitectTool::Codex,
            bwrap: None,
            hcom_executables: Vec::new(),
        };
        let parent_environment = ParentEnvironment::capture_current();
        let inherited_credential_sockets =
            capture_inherited_credential_sockets(&parent_environment).unwrap();
        let environment = ArchitectEnvironment {
            parent_environment,
            control_environment: BTreeMap::from([(
                "HOME".into(),
                control_home.to_string_lossy().into_owned(),
            )]),
            inherited_credential_sockets,
            runtime_home: host_runtime.clone(),
            cargo_bin_source: cargo_bin_source.clone(),
            rustup_home_source: rustup_home_source.clone(),
        };
        let sandbox = ArchitectSandbox {
            project_root: repository,
            paths,
            auth_source: None,
            adapter: ArchitectAdapter::Codex,
            control_root: root.join("session-control"),
            host_runtime,
            host_root: None,
            protected_roots: Vec::new(),
        };
        let report = root.join("architect-state/home/blank-report");
        let write_probe = root.join("repo/architect-write-probe");
        let ArchitectLaunch {
            mut child,
            native_pid,
            gate,
        } = spawn_architect(
            &tools,
            &sandbox,
            &environment,
            &ArchitectInvocationProfile::Codex {
                profile: CodexInvocationProfile::architect_default(),
            },
            None,
        )
        .unwrap();
        assert!(gate.is_none(), "native Codex must not have a launch gate");
        assert_eq!(native_pid, child.id());
        assert!(
            process_birth_identity(native_pid)
                .unwrap()
                .starts_with("linux-proc:")
        );
        let status = child.wait().unwrap();
        assert!(status.success(), "fake blank architect failed: {status}");
        assert_eq!(fs::read_to_string(report).unwrap(), "ok\n");
        assert_eq!(
            fs::read_to_string(write_probe).unwrap(),
            "architect project write\n"
        );
    }

    #[test]
    fn blank_codex_launch_keeps_input_empty_and_preserves_native_host_semantics() {
        let temp = tempfile::Builder::new()
            .prefix("hcom-phase7-blank.")
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let sibling_session = tempfile::Builder::new()
            .prefix("hcom-architect-session.sibling.")
            .tempdir()
            .unwrap();
        let sibling_runtime = fs::canonicalize(sibling_session.path()).unwrap();
        let repository = root.join("repo");
        let external_temp = tempfile::Builder::new()
            .prefix("hcom-architect-external.")
            .tempdir_in("/var/tmp")
            .unwrap();
        let external_source = fs::canonicalize(external_temp.path()).unwrap();
        // Put the protected roots under the writable project to pin the
        // load-bearing writable-parent/readonly-child mount precedence.
        let control_home = repository.clone();
        let live_hcom = control_home.join(".hcom");
        let parent_codex = control_home.join(".codex");
        let parent_claude = control_home.join(".claude");
        let installed_bin = control_home.join(".local/bin");
        let live_hcom_db = live_hcom.join("hcom.db");
        let parent_codex_config = parent_codex.join("config.toml");
        let parent_claude_config = parent_claude.join("settings.json");
        let installed_hcom = installed_bin.join("hcom");
        let host_runtime = root.join("run");
        let control_root = host_runtime.join("legacy-control-sentinel");
        let session_bus_socket = host_runtime.join("session-bus");
        let ssh_agent_socket = host_runtime.join("ssh-agent.sock");
        let gpg_agent_socket = host_runtime.join("gpg-agent.sock");
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
            &state.join("hcom"),
            &codex_home,
            &cargo_bin_source,
            &rustup_home_source,
            &live_hcom,
            &parent_codex,
            &parent_claude,
            &control_home.join(".local"),
            &installed_bin,
        ] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for path in [&live_hcom, &parent_codex, &parent_claude] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o775)).unwrap();
        }
        fs::write(
            external_source.join("source.txt"),
            "external source visible\n",
        )
        .unwrap();
        for (path, contents) in [
            (&live_hcom_db, "live hcom state\n"),
            (&parent_codex_config, "live codex config\n"),
            (&parent_claude_config, "live claude config\n"),
            (&installed_hcom, "#!/bin/sh\nexit 0\n"),
        ] {
            fs::write(path, contents).unwrap();
        }
        fs::set_permissions(&installed_hcom, fs::Permissions::from_mode(0o700)).unwrap();
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
        let sibling_relay_socket = sibling_runtime.join("relay.sock");
        let listeners: Vec<_> = [
            &control_socket,
            &registration_socket,
            &session_bus_socket,
            &ssh_agent_socket,
            &gpg_agent_socket,
            &relay_socket,
            &other_relay_socket,
            &sibling_relay_socket,
        ]
        .into_iter()
        .map(|path| {
            let listener = UnixListener::bind(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            listener
        })
        .collect();

        let report = home.join("blank-report");
        let external_write_probe = external_source.join("architect-write-probe");
        let fake_codex = root.join("codex");
        let profile = CodexInvocationProfile::architect_default();
        let mut expected_args = vec![
            "--model".to_owned(),
            profile.model.clone(),
            "--config".to_owned(),
            profile.reasoning_config_argument(),
            "--sandbox".to_owned(),
            profile.sandbox.as_str().to_owned(),
            "--ask-for-approval".to_owned(),
            profile.approval_policy.as_str().to_owned(),
            "--cd".to_owned(),
            repository.to_string_lossy().into_owned(),
            "--no-alt-screen".to_owned(),
        ];
        let command = toml::Value::String(fake_codex.to_string_lossy().into_owned()).to_string();
        let relay_args = toml::Value::Array(vec![
            toml::Value::String("relay".into()),
            toml::Value::String("--socket".into()),
            toml::Value::String(relay_socket.to_string_lossy().into_owned()),
        ])
        .to_string();
        expected_args.extend([
            "--config".to_owned(),
            format!(
                "mcp_servers.hcom_session_task_control={{ command = {command}, args = \
                 {relay_args}, startup_timeout_sec = 10, tool_timeout_sec = \
                 {CODEX_CONTROL_TOOL_TIMEOUT_SECS}, enabled = true, \
                 default_tools_approval_mode = \"approve\" }}"
            ),
        ]);
        let script = format!(
            r#"#!/usr/bin/python3
import errno
import os
import select
import socket
import subprocess
import sys

if sys.argv[1:] != {expected_args}:
    raise SystemExit(31)
if not all(os.isatty(fd) for fd in (0, 1, 2)):
    raise SystemExit(32)
if select.select([0], [], [], 0)[0]:
    raise SystemExit(33)
if os.environ.get("ARBITRARY_PARENT_VALUE") != "arbitrary-value":
    raise SystemExit(67)
if os.environ.get("SERVICE_ACCESS_TOKEN") != "secret-shaped-value":
    raise SystemExit(68)
if os.environ.get("EMPTY_PARENT_VALUE") != "":
    raise SystemExit(69)
if os.environb.get(b"RAW_\xff_NAME") != b"value-\xfe":
    raise SystemExit(70)
if os.environ.get("HCOM_DIR") != "parent-hcom-state":
    raise SystemExit(34)
if os.environ.get("CODEX_HOME") != {parent_codex_home}:
    raise SystemExit(71)
if os.environ.get("HOME") != {isolated_parent_home}:
    raise SystemExit(72)
if os.environ.get("DBUS_SESSION_BUS_ADDRESS") != {dbus_address}:
    raise SystemExit(73)
if os.environ.get("SSH_AUTH_SOCK") != {ssh_agent_socket}:
    raise SystemExit(74)
if os.environ.get("GPG_AGENT_INFO") != {gpg_agent_info}:
    raise SystemExit(75)
with open({write_probe}, "x", encoding="utf-8") as output:
    output.write("architect project write\n")
if os.path.basename(os.readlink(f"/proc/{{os.getppid()}}/exe")) == "bwrap":
    raise SystemExit(77)
subprocess.run(
    ["/bin/sh", "-c", "printf 'native child process\\n' > \"$1\"", "sh", {child_probe}],
    check=True,
)

with open({external_source}, encoding="utf-8") as source:
    if source.read() != "external source visible\n":
        raise SystemExit(35)
with open({external_write_probe}, "x", encoding="utf-8") as output:
    output.write("architect external write\n")

for index, native_writable in enumerate({native_writable_files}):
    try:
        with open(native_writable, "a", encoding="utf-8") as output:
            output.write("native-write")
    except OSError:
        raise SystemExit(40 + index)
with open({auth_source}, "a", encoding="utf-8") as output:
    output.write("native-write")
for visible in {visible_sockets}:
    if not os.path.exists(visible):
        raise SystemExit(64)

for inherited in {inherited_sockets}:
    probe = socket.socket(socket.AF_UNIX)
    try:
        probe.connect(inherited)
    except OSError:
        raise SystemExit(76)
    finally:
        probe.close()

relay = socket.socket(socket.AF_UNIX)
relay.connect({relay_socket})
relay.close()
with open({report}, "w", encoding="utf-8") as output:
    output.write("ok\n")
"#,
            expected_args = serde_json::to_string(&expected_args).unwrap(),
            parent_codex_home = serde_json::to_string("parent-codex-home").unwrap(),
            isolated_parent_home = serde_json::to_string(&control_home).unwrap(),
            dbus_address =
                serde_json::to_string(&format!("unix:path={}", session_bus_socket.display()))
                    .unwrap(),
            ssh_agent_socket = serde_json::to_string(&ssh_agent_socket).unwrap(),
            gpg_agent_info =
                serde_json::to_string(&format!("{}:0:1", gpg_agent_socket.display())).unwrap(),
            write_probe = serde_json::to_string(&repository.join("architect-write-probe")).unwrap(),
            child_probe = serde_json::to_string(&repository.join("architect-child-probe")).unwrap(),
            external_source = serde_json::to_string(&external_source.join("source.txt")).unwrap(),
            external_write_probe = serde_json::to_string(&external_write_probe).unwrap(),
            native_writable_files = serde_json::to_string(&[
                live_hcom_db.to_string_lossy().into_owned(),
                parent_codex_config.to_string_lossy().into_owned(),
                parent_claude_config.to_string_lossy().into_owned(),
            ])
            .unwrap(),
            auth_source = serde_json::to_string(&auth_source).unwrap(),
            visible_sockets = serde_json::to_string(&[
                control_socket.to_string_lossy().into_owned(),
                registration_socket.to_string_lossy().into_owned(),
                other_relay_socket.to_string_lossy().into_owned(),
                sibling_relay_socket.to_string_lossy().into_owned(),
            ])
            .unwrap(),
            inherited_sockets = serde_json::to_string(&[
                session_bus_socket.to_string_lossy().into_owned(),
                ssh_agent_socket.to_string_lossy().into_owned(),
                gpg_agent_socket.to_string_lossy().into_owned(),
            ])
            .unwrap(),
            relay_socket = serde_json::to_string(&relay_socket).unwrap(),
            report = serde_json::to_string(&report).unwrap(),
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
            .env(BLANK_HELPER_CONTROL_HOME, &control_home)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    root.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("HOME", &control_home)
            .env("XDG_CONFIG_HOME", root.join("xdg-config"))
            .env("XDG_RUNTIME_DIR", &host_runtime)
            .env("XDG_STATE_HOME", root.join("xdg-state"))
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                format!("unix:path={}", session_bus_socket.display()),
            )
            .env("SSH_AUTH_SOCK", &ssh_agent_socket)
            .env(
                "GPG_AGENT_INFO",
                format!("{}:0:1", gpg_agent_socket.display()),
            )
            .env("CARGO_HOME", root.join("cargo"))
            .env("RUSTUP_HOME", &rustup_home_source)
            .env("ARBITRARY_PARENT_VALUE", "arbitrary-value")
            .env("SERVICE_ACCESS_TOKEN", "secret-shaped-value")
            .env("EMPTY_PARENT_VALUE", "")
            .env("HCOM_DIR", "parent-hcom-state")
            .env("CODEX_HOME", "parent-codex-home")
            .env(
                OsString::from_vec(b"RAW_\xff_NAME".to_vec()),
                OsString::from_vec(b"value-\xfe".to_vec()),
            );
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
        assert_eq!(
            fs::read_to_string(repository.join("architect-write-probe")).unwrap(),
            "architect project write\n"
        );
        assert_eq!(
            fs::read_to_string(repository.join("architect-child-probe")).unwrap(),
            "native child process\n"
        );
        assert_eq!(
            fs::read_to_string(&external_write_probe).unwrap(),
            "architect external write\n"
        );
        assert_eq!(fs::read_to_string(&auth_source).unwrap(), "native-write");
        assert_eq!(
            fs::read_to_string(&live_hcom_db).unwrap(),
            "live hcom state\nnative-write"
        );
        assert_eq!(
            fs::read_to_string(&parent_codex_config).unwrap(),
            "live codex config\nnative-write"
        );
        assert_eq!(
            fs::read_to_string(&parent_claude_config).unwrap(),
            "live claude config\nnative-write"
        );
        assert_eq!(
            fs::read_to_string(&installed_hcom).unwrap(),
            "#!/bin/sh\nexit 0\n"
        );
        assert!(
            listeners
                .iter()
                .all(|listener| listener.local_addr().is_ok())
        );
    }
}
