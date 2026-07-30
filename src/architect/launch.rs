use super::bridge::{
    BridgeActivation, BridgeConfiguration, activate_bridge, configure_bridge,
    relay_runtime_scope_hash, sha256_hex,
};
use super::profile::{LoadedInvocationProfiles, load_invocation_profiles};
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
    DISABLED_CODEX_FEATURES,
};
use crate::worker::profile::{
    ArchitectAdapter, ArchitectInvocationProfile, CodexApprovalPolicy, CodexSandbox,
    DeveloperInvocationProfile, ReviewerInvocationProfile, SessionInvocationProfiles,
    validate_cli_help_contract,
};
use crate::worker::reviewer::{CLAUDE_REVIEWER_CLI_VERSION, CLAUDE_REVIEWER_EXECUTABLE};
use crate::worker::sandbox::{HostRootContract, HostRootMounts};
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

#[derive(Parser)]
#[command(
    name = "hcom architect",
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
        std::iter::once("hcom architect".to_owned()).chain(argv.iter().skip(1).cloned()),
    )?;
    let architect_adapter = ArchitectAdapter::parse(&args.adapter)?;
    let mut loaded = match config_path {
        Some(path) => load_invocation_profiles(path, architect_adapter)?,
        None => LoadedInvocationProfiles {
            profiles: SessionInvocationProfiles::for_architect(architect_adapter),
            config_path: PathBuf::from("<built-in defaults>"),
            loaded_from_file: false,
            reviewer_explicit: false,
        },
    };
    apply_architect_cli_overrides(&args, &mut loaded.profiles, loaded.reviewer_explicit)?;
    validate_foreground_terminal()?;

    let project_root = canonical_project_directory(&std::env::current_dir()?)?;
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
        writeln!(
            stdout,
            "task repositories: discovered from project documentation and bound only after exact plan approval; each developer commits directly there"
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
    validate_path_isolation(&project_root, &paths, &tools)?;
    let auth_source =
        PrivateFileIdentity::capture(&discover_architect_auth_source(architect_adapter)?)?;
    if paths_overlap(auth_source.path(), &paths.state)
        || paths_overlap(auth_source.path(), &paths.runtime)
    {
        bail!("native architect auth source overlaps architect writable state");
    }
    create_empty_private_file(&paths.auth_target)?;
    if architect_adapter == ArchitectAdapter::Codex {
        write_isolated_codex_config(&paths, &tools.component.canonical_path)?;
    }
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
        lock_root: control_paths.lock_root().to_owned(),
        relay_socket_path: paths.relay_socket.clone(),
        registration_socket_path: control_paths.registration_socket_path(),
        control_socket_path: control_paths.socket_path(),
        native_session_source: match &preassigned_native_session_id {
            Some(native_session_id) => super::bridge::ArchitectNativeSessionSource::Claude {
                native_session_id: native_session_id.clone(),
            },
            None => super::bridge::ArchitectNativeSessionSource::Codex {
                codex_home: paths.native_config.clone(),
            },
        },
        relay_executable: tools.component.clone(),
        relay_runtime_scope_hash: relay_scope_hash.clone(),
        developer_adapter: loaded.profiles.developer_adapter_name().into(),
        reviewer_adapter: loaded.profiles.reviewer_adapter_name().into(),
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
        host_runtime: native_environment.runtime_home.clone(),
        host_root: HostRootContract::capture(
            &native_environment.cargo_bin_source,
            &native_environment.rustup_home_source,
        )?,
    };
    let (mut architect, gate_write, info_read) = match spawn_blocked_architect(
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

fn apply_architect_cli_overrides(
    args: &ArchitectArgs,
    profiles: &mut SessionInvocationProfiles,
    reviewer_explicit: bool,
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
    if !reviewer_explicit {
        profiles.inherit_reviewer_from_architect();
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
    bwrap: ExecutableIdentity,
    component: ExecutableIdentity,
}

enum ExactArchitectTool {
    Codex(ExecutableIdentity),
    Claude(ExecutableIdentity),
}

impl ExactArchitectTool {
    fn executable(&self) -> &ExecutableIdentity {
        match self {
            Self::Codex(executable) | Self::Claude(executable) => executable,
        }
    }

    fn revalidate(&self) -> Result<()> {
        match self {
            Self::Codex(executable) => {
                revalidate_exact_tool(executable, CODEX_DEVELOPER_CLI_VERSION)
            }
            Self::Claude(executable) => {
                revalidate_exact_tool(executable, CLAUDE_REVIEWER_CLI_VERSION)
            }
        }
    }
}

impl ExactTools {
    fn discover(adapter: ArchitectAdapter) -> Result<Self> {
        let architect = match adapter {
            ArchitectAdapter::Codex => {
                let executable = capture_exact_tool(
                    Path::new(CODEX_DEVELOPER_EXECUTABLE),
                    CODEX_DEVELOPER_CLI_VERSION,
                )?;
                validate_architect_codex_cli(&executable.canonical_path)?;
                ExactArchitectTool::Codex(executable)
            }
            ArchitectAdapter::Claude => {
                let executable = capture_exact_tool(
                    Path::new(CLAUDE_REVIEWER_EXECUTABLE),
                    CLAUDE_REVIEWER_CLI_VERSION,
                )?;
                validate_architect_claude_cli(&executable.canonical_path)?;
                ExactArchitectTool::Claude(executable)
            }
        };
        let bwrap = capture_exact_tool(Path::new(BWRAP_EXECUTABLE), BWRAP_VERSION)?;
        let component_path = resolve_component_path()?;
        let component = ExecutableIdentity::capture(component_path)?;
        Ok(Self {
            architect,
            bwrap,
            component,
        })
    }

    fn revalidate(&self) -> Result<()> {
        self.architect.revalidate()?;
        revalidate_exact_tool(&self.bwrap, BWRAP_VERSION)?;
        self.component.revalidate()
    }
}

fn validate_architect_codex_cli(path: &Path) -> Result<()> {
    let output = Command::new(path)
        .arg("--help")
        .env_clear()
        .output()
        .context("failed to query architect Codex CLI capabilities")?;
    if !output.status.success() || !output.stderr.is_empty() {
        bail!("architect Codex CLI capability probe failed");
    }
    validate_cli_help_contract(
        "architect Codex CLI",
        &output.stdout,
        &[
            "--config",
            "--disable",
            "--strict-config",
            "--model",
            "--sandbox",
            "--ask-for-approval",
            "--cd",
            "--no-alt-screen",
        ],
    )?;
    let help = std::str::from_utf8(&output.stdout)?;
    for value in [
        "read-only",
        "workspace-write",
        "danger-full-access",
        "untrusted",
        "on-request",
        "never",
    ] {
        if !help.contains(value) {
            bail!("architect Codex CLI help omitted configured value {value}");
        }
    }
    Ok(())
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
    native_config: PathBuf,
    xdg_config: PathBuf,
    xdg_state: PathBuf,
    xdg_cache: PathBuf,
    xdg_data: PathBuf,
    runtime: PathBuf,
    relay_socket: PathBuf,
    auth_target: PathBuf,
    codex_config_file: Option<PathBuf>,
}

impl ArchitectLaunchPaths {
    fn create(control: &ControlPaths, launch_id: &str, adapter: ArchitectAdapter) -> Result<Self> {
        let state_parent = control.architect_state_root_path();
        let runtime_parent = control.architect_runtime_root_path();
        ensure_private_directory(&state_parent, true)?;
        ensure_private_directory(&runtime_parent, true)?;
        let state = state_parent.join(launch_id);
        let runtime = runtime_parent.join(launch_id);
        ensure_private_directory(&state, false)?;
        ensure_private_directory(&runtime, false)?;
        let home = state.join("home");
        ensure_private_directory(&home, false)?;
        let native_config = home.join(match adapter {
            ArchitectAdapter::Codex => ".codex",
            ArchitectAdapter::Claude => ".claude",
        });
        ensure_private_directory(&native_config, false)?;
        let xdg_config = state.join("xdg-config");
        let xdg_state = state.join("xdg-state");
        let xdg_cache = state.join("xdg-cache");
        let xdg_data = state.join("xdg-data");
        for directory in [&xdg_config, &xdg_state, &xdg_cache, &xdg_data] {
            ensure_private_directory(directory, false)?;
        }
        let auth_target = native_config.join(match adapter {
            ArchitectAdapter::Codex => "auth.json",
            ArchitectAdapter::Claude => ".credentials.json",
        });
        Ok(Self {
            state,
            home,
            native_config: native_config.clone(),
            xdg_config,
            xdg_state,
            xdg_cache,
            xdg_data,
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target,
            codex_config_file: (adapter == ArchitectAdapter::Codex)
                .then(|| native_config.join("config.toml")),
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

fn discover_architect_auth_source(adapter: ArchitectAdapter) -> Result<PathBuf> {
    let (override_name, default_directory, filename, label) = match adapter {
        ArchitectAdapter::Codex => ("CODEX_HOME", ".codex", "auth.json", "Codex"),
        ArchitectAdapter::Claude => (
            "CLAUDE_CONFIG_DIR",
            ".claude",
            ".credentials.json",
            "Claude",
        ),
    };
    let base = match std::env::var_os(override_name) {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("home directory is unavailable"))?
            .join(default_directory),
    };
    let source = base.join(filename);
    let canonical = fs::canonicalize(&source)
        .with_context(|| format!("{label} architect credential is unavailable"))?;
    if canonical != source {
        bail!("{label} architect credential must already be canonical");
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

fn write_isolated_codex_config(paths: &ArchitectLaunchPaths, component: &Path) -> Result<()> {
    let server = IsolatedMcpServer {
        command: path_text("architect MCP component", component)?.into(),
        args: vec![
            "relay".into(),
            "--socket".into(),
            path_text("architect relay socket", &paths.relay_socket)?.into(),
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
        .open(
            paths
                .codex_config_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Codex config target is unavailable"))?,
        )?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    let parsed: IsolatedCodexConfig = toml::from_str(std::str::from_utf8(&bytes)?)?;
    if parsed != config {
        bail!("isolated Codex config failed its exact round trip");
    }
    let metadata = fs::symlink_metadata(
        paths
            .codex_config_file
            .as_ref()
            .expect("validated Codex config target"),
    )?;
    if metadata.permissions().mode() & 0o777 != 0o600 {
        bail!("isolated Codex config mode drifted");
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
    for executable in [
        &tools.architect.executable().canonical_path,
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
        if values.get("PATH").is_none_or(|value| value.is_empty())
            || values.get("TERM").is_none_or(|value| value.is_empty())
        {
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
                if name == "HOME" || !value.is_empty() {
                    control_environment.insert(name.into(), value);
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
        let cargo_bin_source = std::env::var_os("CARGO_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| parent_home.join(".cargo"))
            .join("bin");
        let rustup_home_source = std::env::var_os("RUSTUP_HOME")
            .filter(|value| !value.is_empty())
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

    fn sandbox_values(
        &self,
        paths: &ArchitectLaunchPaths,
        adapter: ArchitectAdapter,
    ) -> Result<BTreeMap<String, String>> {
        let mut values = self.values.clone();
        let home = self
            .control_environment
            .get("HOME")
            .ok_or_else(|| anyhow::anyhow!("architect parent HOME disappeared"))?
            .clone();
        values.insert("HOME".into(), home);
        match adapter {
            ArchitectAdapter::Codex => {
                values.insert(
                    "CODEX_HOME".into(),
                    path_text("architect isolated Codex home", &paths.native_config)?.into(),
                );
            }
            ArchitectAdapter::Claude => {
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
            }
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
    project_root: PathBuf,
    paths: ArchitectLaunchPaths,
    auth_source: PrivateFileIdentity,
    adapter: ArchitectAdapter,
    host_runtime: PathBuf,
    host_root: HostRootContract,
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
        if !matches!(
            (&tools.architect, self.adapter),
            (ExactArchitectTool::Codex(_), ArchitectAdapter::Codex)
                | (ExactArchitectTool::Claude(_), ArchitectAdapter::Claude)
        ) {
            bail!("architect executable differs from the selected adapter");
        }
        let tmp = Path::new("/tmp");
        let masked_dirs: Vec<&Path> = if self.host_runtime.starts_with(tmp) {
            vec![tmp]
        } else {
            vec![tmp, &self.host_runtime]
        };
        let claude_writable_dirs = [
            self.paths.xdg_config.as_path(),
            self.paths.xdg_state.as_path(),
            self.paths.xdg_cache.as_path(),
            self.paths.xdg_data.as_path(),
        ];
        let extra_writable_dirs: &[&Path] = match self.adapter {
            ArchitectAdapter::Codex => &[],
            ArchitectAdapter::Claude => &claude_writable_dirs,
        };
        let mut argv = self.host_root.host_root_argv(HostRootMounts {
            isolated_home: &self.paths.home,
            native_config: &self.paths.native_config,
            launch_cwd: &self.project_root,
            artifact_dir: &self.paths.runtime,
            auth_source: self.auth_source.path(),
            auth_target: &self.paths.auth_target,
            readable_roots: &[&self.project_root],
            writable_roots: &[],
            read_only_files: &[
                &tools.architect.executable().canonical_path,
                &tools.component.canonical_path,
            ],
            extra_writable_dirs,
            expose_host_root_read_only: true,
            masked_dirs: &masked_dirs,
        })?;
        argv.push("--clearenv".into());
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

fn spawn_blocked_architect(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    environment: &ArchitectEnvironment,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<(Child, OwnedFd, OwnedFd)> {
    profile.validate()?;
    if profile.adapter() != sandbox.adapter {
        bail!("architect profile differs from its sandbox adapter");
    }
    tools.revalidate()?;
    let (gate_read, gate_write) = pipe_cloexec()?;
    let (info_read, info_write) = pipe_cloexec()?;
    let mut command = Command::new(&tools.bwrap.canonical_path);
    let gate_fd = gate_read.as_raw_fd();
    let info_fd = info_write.as_raw_fd();
    let mut argv = sandbox.outer_argv(environment, tools, gate_fd, info_fd)?;
    argv.push("--".into());
    argv.extend(architect_native_argv(
        tools,
        sandbox,
        profile,
        preassigned_native_session_id,
    )?);
    validate_native_argv(
        &argv,
        tools,
        sandbox,
        profile,
        preassigned_native_session_id,
    )?;
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

fn architect_native_argv(
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<Vec<String>> {
    let executable = path_text(
        "architect native executable",
        &tools.architect.executable().canonical_path,
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
                "--strict-config".into(),
            ];
            for feature in DISABLED_CODEX_FEATURES {
                argv.extend(["--disable".into(), (*feature).into()]);
            }
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
                "Bash,Read,Glob,Grep".into(),
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

fn validate_native_argv(
    argv: &[String],
    tools: &ExactTools,
    sandbox: &ArchitectSandbox,
    profile: &ArchitectInvocationProfile,
    preassigned_native_session_id: Option<&str>,
) -> Result<()> {
    let separator = argv
        .iter()
        .position(|argument| argument == "--")
        .ok_or_else(|| anyhow::anyhow!("architect bwrap argv omitted its separator"))?;
    let native = &argv[separator + 1..];
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
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixListener;
    use std::os::unix::process::CommandExt;

    const BLANK_HELPER_ROOT: &str = "HCOM_PHASE7_BLANK_HELPER_ROOT";
    const ENVIRONMENT_HELPER_ROOT: &str = "HCOM_PHASE9_ENVIRONMENT_HELPER_ROOT";
    const RUNTIME_MODE_HELPER: &str = "HCOM_PHASE9_RUNTIME_MODE_HELPER";
    const TEST_CLAUDE_SESSION: &str = "019fa976-e270-7a92-b5f0-6d3d8a0ad3f4";

    #[test]
    fn architect_environment_helper_process() {
        if std::env::var_os(ENVIRONMENT_HELPER_ROOT).is_none() {
            return;
        }
        let environment = ArchitectEnvironment::capture().unwrap();
        assert_eq!(environment.values.get("COLORTERM"), Some(&String::new()));
        assert_eq!(
            environment.control_environment.get("COLORTERM"),
            Some(&String::new())
        );
        assert!(!environment.control_environment.contains_key("CARGO_HOME"));
        assert!(!environment.control_environment.contains_key("CODEX_HOME"));
    }

    #[test]
    fn empty_optional_terminal_and_tool_overrides_do_not_block_blank_launch() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let home = root.join("home");
        let config = root.join("config");
        let state = root.join("state");
        let runtime = root.join("runtime");
        for directory in [&home, &config, &state, &runtime] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
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
    fn pinned_codex_root_help_matches_architect_command_contract_when_installed() {
        let path = Path::new(CODEX_DEVELOPER_EXECUTABLE);
        if path.exists() {
            validate_architect_codex_cli(path).unwrap();
        }
    }

    #[test]
    fn pinned_claude_root_help_matches_architect_command_contract_when_installed() {
        let path = Path::new(CLAUDE_REVIEWER_EXECUTABLE);
        if path.exists() {
            validate_architect_claude_cli(path).unwrap();
        }
    }

    #[test]
    fn explicit_architect_cli_overrides_toml_profile_only() {
        let args = ArchitectArgs::try_parse_from([
            "hcom architect",
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
        apply_architect_cli_overrides(&args, &mut profiles, false).unwrap();
        let architect = profiles.architect.codex().unwrap();
        assert_eq!(architect.model, "gpt-5.6-sol-cli");
        assert_eq!(architect.reasoning_effort, "max");
        assert_eq!(architect.sandbox, CodexSandbox::DangerFullAccess);
        assert_eq!(architect.approval_policy, CodexApprovalPolicy::Never);
        assert_eq!(profiles.developer, developer_before);
        let reviewer = profiles.reviewer.codex().unwrap();
        assert_eq!(reviewer.model, architect.model);
        assert_eq!(reviewer.reasoning_effort, architect.reasoning_effort);
    }

    #[test]
    fn claude_cli_overrides_flow_to_an_implicit_reviewer_only() {
        let args = ArchitectArgs::try_parse_from([
            "hcom architect",
            "claude",
            "--model",
            "sonnet",
            "--effort",
            "max",
        ])
        .unwrap();
        let mut profiles = SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude);
        let developer_before = profiles.developer.clone();
        apply_architect_cli_overrides(&args, &mut profiles, false).unwrap();
        let architect = profiles.architect.claude().unwrap();
        let reviewer = profiles.reviewer.claude().unwrap();
        assert_eq!(architect.model, "sonnet");
        assert_eq!(architect.effort, "max");
        assert_eq!(reviewer.model, architect.model);
        assert_eq!(reviewer.effort, architect.effort);
        assert_eq!(profiles.developer, developer_before);

        let invalid =
            ArchitectArgs::try_parse_from(["hcom architect", "claude", "--reasoning", "xhigh"])
                .unwrap();
        assert!(apply_architect_cli_overrides(&invalid, &mut profiles, false).is_err());
    }

    #[test]
    fn explicit_reviewer_is_not_replaced_by_architect_cli_overrides() {
        let args = ArchitectArgs::try_parse_from([
            "hcom architect",
            "codex",
            "--model",
            "gpt-5.6-sol-cli",
            "--reasoning",
            "max",
        ])
        .unwrap();
        let mut profiles = SessionInvocationProfiles {
            reviewer: ReviewerInvocationProfile::Claude {
                profile: crate::worker::profile::ClaudeInvocationProfile::reviewer_default(),
            },
            ..SessionInvocationProfiles::default()
        };
        let reviewer_before = profiles.reviewer.clone();
        apply_architect_cli_overrides(&args, &mut profiles, true).unwrap();
        assert_eq!(profiles.reviewer, reviewer_before);
    }

    #[test]
    fn native_profile_has_no_prompt_or_secret_transport() {
        let profile = CodexInvocationProfile::architect_default();
        let native: Vec<String> = vec![
            CODEX_DEVELOPER_EXECUTABLE.into(),
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
            bwrap: current.clone(),
            component: current,
        };
        let paths = ArchitectLaunchPaths {
            state,
            home,
            native_config: native_config.clone(),
            xdg_config: root.join("xdg-config"),
            xdg_state: root.join("xdg-state"),
            xdg_cache: root.join("xdg-cache"),
            xdg_data: root.join("xdg-data"),
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: native_config.join(".credentials.json"),
            codex_config_file: None,
        };
        let sandbox = ArchitectSandbox {
            project_root: project,
            paths,
            auth_source: PrivateFileIdentity::capture(&auth).unwrap(),
            adapter: ArchitectAdapter::Claude,
            host_runtime: root.clone(),
            host_root: HostRootContract::capture(&cargo_bin, &rustup_home).unwrap(),
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
            native_config: codex_home,
            xdg_config: root.join("xdg-config"),
            xdg_state: root.join("xdg-state"),
            xdg_cache: root.join("xdg-cache"),
            xdg_data: root.join("xdg-data"),
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
            auth_target: root.join("architect-state/home/.codex/auth.json"),
            codex_config_file: Some(root.join("architect-state/home/.codex/config.toml")),
        };
        let fake_codex = fs::canonicalize(root.join("fake-codex")).unwrap();
        let codex = capture_exact_tool(&fake_codex, CODEX_DEVELOPER_CLI_VERSION).unwrap();
        let bwrap = capture_exact_tool(Path::new(BWRAP_EXECUTABLE), BWRAP_VERSION).unwrap();
        let tools = ExactTools {
            component: codex.clone(),
            architect: ExactArchitectTool::Codex(codex),
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
            control_environment: BTreeMap::from([(
                "HOME".into(),
                root.to_string_lossy().into_owned(),
            )]),
            runtime_home: host_runtime.clone(),
            cargo_bin_source: cargo_bin_source.clone(),
            rustup_home_source: rustup_home_source.clone(),
        };
        let sandbox = ArchitectSandbox {
            project_root: repository,
            paths,
            auth_source: PrivateFileIdentity::capture(&root.join("auth.json")).unwrap(),
            adapter: ArchitectAdapter::Codex,
            host_runtime,
            host_root: HostRootContract::capture(&cargo_bin_source, &rustup_home_source).unwrap(),
        };
        let report = root.join("architect-state/home/blank-report");
        let write_probe = root.join("repo/architect-write-probe");
        let (mut child, gate, info) = spawn_blocked_architect(
            &tools,
            &sandbox,
            &environment,
            &ArchitectInvocationProfile::Codex {
                profile: CodexInvocationProfile::architect_default(),
            },
            None,
        )
        .unwrap();
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
    fn blank_launch_keeps_terminal_input_empty_and_preserves_project_path() {
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
        fs::write(
            external_source.join("source.txt"),
            "external source visible\n",
        )
        .unwrap();
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

with open({external_source}, encoding="utf-8") as source:
    if source.read() != "external source visible\n":
        raise SystemExit(36)
for hidden in {hidden_sockets}:
    if os.path.exists(hidden):
        raise SystemExit(37)
    probe = socket.socket(socket.AF_UNIX)
    try:
        probe.connect(hidden)
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise SystemExit(38)
    else:
        raise SystemExit(39)
    finally:
        probe.close()

relay = socket.socket(socket.AF_UNIX)
relay.connect({relay_socket})
relay.close()
with open({report}, "w", encoding="utf-8") as output:
    output.write("ok\n")
"#,
            expected_args = serde_json::to_string(&expected_args).unwrap(),
            write_probe = serde_json::to_string(&repository.join("architect-write-probe")).unwrap(),
            external_source = serde_json::to_string(&external_source.join("source.txt")).unwrap(),
            hidden_sockets = serde_json::to_string(&[
                control_socket.to_string_lossy().into_owned(),
                registration_socket.to_string_lossy().into_owned(),
                other_relay_socket.to_string_lossy().into_owned(),
                sibling_relay_socket.to_string_lossy().into_owned(),
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
