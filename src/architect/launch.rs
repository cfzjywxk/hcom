use super::bridge::{
    BridgeActivation, BridgeConfiguration, activate_bridge, configure_bridge,
    relay_runtime_scope_hash, sha256_hex,
};
use super::profile::{LoadedInvocationProfiles, load_task_lane_profiles_for_mode};
use crate::control_api::peer::{process_birth_identity, process_owns_foreground_tty};
use crate::control_api::protocol::PROTOCOL_VERSION;
use crate::control_api::registration::{
    RegistrationAction, RegistrationCaller, RegistrationClient,
};
use crate::control_api::supervisor::{ControlPaths, SessionSupervisorEndpoint};
use crate::control_api::{ActionName, GitHubDeliveryPolicy, ReviewerAdapterBinding};
use crate::orchestrator::SessionRuntimeSources;
use crate::worker::environment::{
    CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS, CLAUDE_DISABLE_BACKGROUND_TASKS,
    MaterializedWorkerEnvironment,
};
use crate::worker::guardian::{
    GUARDIAN_LIFECYCLE_BOUNDARY, GuardedCommand, GuardianCleanupDisposition, GuardianCleanupReason,
    GuardianHandle, GuardianHandleFailure, GuardianMode, GuardianPoll, GuardianSpawnFailure,
};
use crate::worker::profile::{
    ArchitectAdapter, ArchitectInvocationProfile, CodexApprovalPolicy, CodexSandbox,
    DeveloperInvocationProfile, ReviewerInvocationProfile, SessionInvocationProfiles,
};
use crate::worker::runtime::TaskWorkerProfiles;
use crate::worker::{ExecutableIdentity, ParentEnvironment};
use anyhow::{Context, Result, bail};
use clap::Parser;
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
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
use uuid::Uuid;

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

    /// Use only Reviewer1. Available only with the Codex Architect.
    #[arg(long = "single-review")]
    single_review: bool,

    /// Select the explicit GitHub Pull Request delivery lane.
    #[arg(long = "github-pr")]
    github_pr: bool,

    /// Opt in to ruleset-attested exact-head merge for the GitHub lane.
    #[arg(long = "protected-auto-merge")]
    protected_auto_merge: bool,

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

    /// Ordered external repository roots whose native Claude instructions load at startup.
    #[arg(long = "add-dir", value_name = "ABSOLUTE_DIRECTORY")]
    additional_directories: Vec<PathBuf>,

    /// Exact profile configuration file replacing $HCOM_DIR/config.toml.
    #[arg(long = "config", value_name = "ABSOLUTE_FILE")]
    profile_config: Option<PathBuf>,
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
    if args.single_review && architect_adapter != ArchitectAdapter::Codex {
        bail!("--single-review is available only with `hcom arch codex`");
    }
    if args.protected_auto_merge && !args.github_pr {
        bail!("--protected-auto-merge requires --github-pr");
    }
    let parent_environment = ParentEnvironment::capture_current()?;
    // An explicit --config replaces only the profile configuration source; every
    // other hcom setting still comes from $HCOM_DIR/config.toml.
    let explicit_config = args
        .profile_config
        .as_deref()
        .map(validate_profile_config_path)
        .transpose()?;
    // Both public entrypoints share the same provider-routed task worker lane.
    let mut loaded = match explicit_config.as_deref().or(config_path) {
        Some(path) => {
            load_task_lane_profiles_for_mode(path, architect_adapter, args.single_review)?
        }
        None => {
            let profiles = if args.single_review {
                SessionInvocationProfiles::for_single_review_task_lane(architect_adapter)?
            } else {
                SessionInvocationProfiles::for_task_lane(architect_adapter)?
            };
            LoadedInvocationProfiles {
                profiles,
                config_path: PathBuf::from("<built-in defaults>"),
                loaded_from_file: false,
                legacy_reviewer_migrated: false,
                github: None,
            }
        }
    };
    // A default path that is absent selects the built-in profiles. An explicit
    // --config never degrades to them silently, including when the validated
    // file disappears before the loader opens it.
    if explicit_config.is_some() && !loaded.loaded_from_file {
        bail!("--config profile configuration disappeared before it was loaded");
    }
    apply_architect_cli_overrides(&args, &mut loaded.profiles)?;
    let (developer_adapter, reviewer_adapters) =
        worker_adapter_bindings(architect_adapter, &loaded.profiles)?;
    let claude_environment = loaded
        .profiles
        .uses_claude()
        .then(|| parent_environment.materialize_claude())
        .transpose()?;
    let architect_additional_directories =
        validate_additional_directories(architect_adapter, &args.additional_directories)?;

    let project_root = canonical_project_directory(&std::env::current_dir()?)?;
    let run_id = format!("run-{}", Uuid::new_v4().simple());
    let github_runtime = if args.github_pr {
        let reviewer_ids = loaded
            .profiles
            .reviewers
            .iter()
            .map(|binding| binding.reviewer_id)
            .collect::<Vec<_>>();
        let github = loaded.github.take().ok_or_else(|| {
            anyhow::anyhow!("--github-pr requires a closed [architect.github] configuration")
        })?;
        let delivery_policy = if args.protected_auto_merge {
            GitHubDeliveryPolicy::ProtectedAutoMerge
        } else {
            GitHubDeliveryPolicy::Manual
        };
        let deployment = crate::orchestrator::github::parse_github_deployment_config(
            github,
            delivery_policy,
            &reviewer_ids,
            &project_root,
        )?;
        let provider = Arc::new(crate::orchestrator::github::ProductionGitHubProvider::open(
            deployment.clone(),
            reviewer_ids.clone(),
        )?);
        Some(crate::orchestrator::github::preflight_runtime(
            provider,
            &run_id,
            0,
            &deployment,
            &reviewer_ids,
        )?)
    } else {
        None
    };
    let github_startup_summary = github_runtime
        .as_ref()
        .map(|runtime| (runtime.binding.clone(), runtime.initial_inspection.clone()));
    validate_foreground_terminal()?;
    let session_root = create_private_session_runtime()?;
    let run_root = fs::canonicalize(session_root.path())?;
    let control_paths = ControlPaths::new(&run_root)?;
    let runtime_sources = match github_runtime {
        Some(github_runtime) => SessionRuntimeSources::capture_with_github(
            parent_environment.clone(),
            loaded.profiles.clone(),
            architect_additional_directories.clone(),
            github_runtime,
        )?,
        None => SessionRuntimeSources::capture(
            parent_environment.clone(),
            loaded.profiles.clone(),
            architect_additional_directories.clone(),
        )?,
    };
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
        write_profile_config_line(
            &mut stdout,
            &loaded.config_path,
            loaded.loaded_from_file,
            explicit_config.is_some(),
        )?;
        writeln!(stdout, "profile hash: {}", loaded.profiles.canonical_hash())?;
        writeln!(
            stdout,
            "session binding hash: {}",
            startup.session_binding_hash
        )?;
        write_profile_summary(&mut stdout, &loaded.profiles)?;
        write_legacy_reviewer_notice(
            &mut stdout,
            loaded.legacy_reviewer_migrated,
            &loaded.profiles,
        )?;
        if loaded.profiles.uses_claude() {
            write_claude_startup_summary(
                &mut stdout,
                architect_adapter,
                &architect_additional_directories,
            )?;
        }
        writeln!(
            stdout,
            "worker runtime: lane-router (one native provider runtime per effective worker lane)"
        )?;
        write_delivery_startup_summary(
            &mut stdout,
            github_startup_summary
                .as_ref()
                .map(|(binding, inspection)| (binding, inspection)),
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
    let paths = ArchitectLaunchPaths::create(&control_paths, &launch_id)?;

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
            actions: ActionName::architect(args.github_pr)
                .iter()
                .copied()
                .collect(),
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

    let mut bridge = match spawn_bridge(&tools.component.canonical_path) {
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
        session_binding_hash: startup.session_binding_hash.clone(),
        architect_adapter: architect_adapter.contract_name().into(),
        architect_additional_directories: architect_additional_directories.clone(),
        developer_adapter: developer_adapter.into(),
        reviewer_adapters,
        github_pr: args.github_pr,
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

    let context = ArchitectLaunchContext {
        project_root: project_root.clone(),
        paths: paths.clone(),
        adapter: architect_adapter,
        additional_directories: architect_additional_directories,
    };
    let ArchitectLaunch {
        process: mut architect,
        native_pid: architect_pid,
    } = match spawn_architect(
        &tools,
        &context,
        claude_environment.as_ref(),
        &loaded.profiles.architect,
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
            terminate_architect(&mut architect);
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
            terminate_architect(&mut architect);
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
            terminate_architect(&mut architect);
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
        terminate_architect(&mut architect);
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
        terminate_architect(&mut architect);
        terminate_child(&mut bridge.child);
        best_effort_close_binding(
            &registration_client,
            &process_birth,
            &binding_id,
            &[binding_version],
        );
        bail!("architect launch identity changed during registration");
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
    writeln!(output, "review mode: {}", profiles.review_mode_name())?;
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
    for binding in &profiles.reviewers {
        write_reviewer_profile(output, binding.reviewer_id.as_str(), &binding.profile)?;
    }
    Ok(())
}

fn write_delivery_startup_summary(
    output: &mut impl Write,
    github: Option<(
        &crate::control_api::GitHubPullRequestBinding,
        &crate::control_api::GitHubInspectionBinding,
    )>,
) -> Result<()> {
    let Some((binding, inspection)) = github else {
        writeln!(
            output,
            "task repositories: discovered from project documentation and bound only after explicit execution authorization; each developer commits directly there"
        )?;
        return Ok(());
    };

    writeln!(
        output,
        "delivery mode: GitHub Pull Request (explicit --github-pr)"
    )?;
    writeln!(
        output,
        "github delivery policy: {}",
        binding.delivery_policy.as_str()
    )?;
    writeln!(
        output,
        "github repository: {}/{} id={} visibility={} local_root={} base={}",
        binding.owner,
        binding.repository,
        binding.repository_id,
        binding.visibility,
        binding.local_repository_root,
        binding.base_branch
    )?;
    if binding.delivery_policy.is_protected_auto_merge() {
        writeln!(
            output,
            "github protected delivery: merge_method={} merge_wait_seconds={} delete_remote_branch_after_merge={} review_check={}",
            binding.merge_method,
            binding.merge_wait_seconds,
            binding.delete_remote_branch_after_merge,
            binding.review_check_name
        )?;
    } else {
        writeln!(
            output,
            "github manual delivery: review_check={}; no ruleset attestation, merge, remote-ref deletion, or merged-run finalization will be requested",
            binding.review_check_name
        )?;
        writeln!(
            output,
            "github manual enforcement disclosure: hcom verifies its own exact base/head, actor, append-only task chain, published reviews, and review Check, but GitHub Free private repositories provide no hcom-attested server-side protection against authorized external direct push or early merge"
        )?;
    }
    write_github_app_summary(output, "architect", &binding.architect_app)?;
    write_github_app_summary(output, "developer", &binding.developer_app)?;
    for reviewer in &binding.reviewer_apps {
        write_github_app_summary(output, reviewer.reviewer_id.as_str(), &reviewer.app)?;
    }
    writeln!(
        output,
        "github initial inspection: id={} repository_id={} base_ref={} base_sha={} ruleset_attestation_sha256={}",
        inspection.inspection_id,
        inspection.inspected_repository_id,
        inspection.expected_base_ref,
        inspection.expected_base_sha,
        inspection
            .ruleset_attestation_sha256
            .as_deref()
            .unwrap_or("not_applicable")
    )?;
    writeln!(
        output,
        "github authority: startup preflight was read-only; no branch, worktree, Pull Request, Check, review, comment, push, or merge is created before an inspected typed plan is explicitly approved"
    )?;
    writeln!(
        output,
        "github publication: successful Developer and Reviewer finals are opaque and published byte-for-byte without redaction or secret scanning; generated bodies have a 60 KiB UTF-8 hard cap"
    )?;
    writeln!(
        output,
        "task repositories: every task must use the frozen GitHub local repository root; one approved run owns one append-only branch, linked worktree, and Pull Request"
    )?;
    Ok(())
}

fn write_github_app_summary(
    output: &mut impl Write,
    role: &str,
    app: &crate::control_api::GitHubAppBinding,
) -> Result<()> {
    writeln!(
        output,
        "github {role} app: app_id={} installation_id={} slug={} bot_user_id={} permissions={}",
        app.app_id,
        app.installation_id,
        app.slug,
        app.bot_user_id,
        serde_json::to_string(&app.effective_permissions)?
    )?;
    Ok(())
}

fn write_legacy_reviewer_notice(
    output: &mut impl Write,
    migrated: bool,
    profiles: &SessionInvocationProfiles,
) -> Result<()> {
    if migrated {
        if profiles.reviewers.len() == 1 {
            writeln!(
                output,
                "deprecated profile config: [architect.reviewer] was resolved once for reviewer1; declare [architect.reviewer1] explicitly"
            )?;
        } else {
            writeln!(
                output,
                "deprecated profile config: [architect.reviewer] was resolved once and copied to reviewer1 and reviewer2; declare both canonical tables explicitly"
            )?;
        }
    }
    Ok(())
}

/// Disclose the exact profile configuration the run froze, and whether an
/// explicit `--config` rather than `$HCOM_DIR/config.toml` supplied it.
fn write_profile_config_line(
    output: &mut impl Write,
    config_path: &Path,
    loaded_from_file: bool,
    explicit: bool,
) -> Result<()> {
    writeln!(
        output,
        "profile config: {} ({}{})",
        config_path.display(),
        if explicit { "--config; " } else { "" },
        if loaded_from_file {
            "loaded once"
        } else {
            "not present; built-in defaults"
        }
    )?;
    Ok(())
}

fn write_reviewer_profile(
    output: &mut impl Write,
    label: &str,
    profile: &ReviewerInvocationProfile,
) -> Result<()> {
    match profile {
        ReviewerInvocationProfile::Codex { profile } => writeln!(
            output,
            "{label} profile: codex model={} reasoning={} sandbox={} approval={}",
            profile.model,
            profile.reasoning_effort,
            profile.sandbox.as_str(),
            profile.approval_policy.as_str()
        )?,
        ReviewerInvocationProfile::Claude { profile } => writeln!(
            output,
            "{label} profile: claude model={} effort={} dangerously_skip_permissions={}",
            profile.model, profile.effort, profile.dangerously_skip_permissions
        )?,
    }
    Ok(())
}

fn write_claude_startup_summary(
    output: &mut impl Write,
    architect_adapter: ArchitectAdapter,
    additional_directories: &[PathBuf],
) -> Result<()> {
    writeln!(
        output,
        "claude environment policy: {CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS}=1; {CLAUDE_DISABLE_BACKGROUND_TASKS}=1"
    )?;
    writeln!(
        output,
        "claude additional-directory instructions: external task repositories use native --add-dir with {CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS}=1; this is not a filesystem allowlist"
    )?;
    writeln!(
        output,
        "claude lifecycle backend: Linux per-invocation PR_SET_CHILD_SUBREAPER Guardian"
    )?;
    writeln!(output, "claude lifecycle: {GUARDIAN_LIFECYCLE_BOUNDARY}")?;
    if architect_adapter == ArchitectAdapter::Claude {
        if additional_directories.is_empty() {
            writeln!(output, "claude architect --add-dir roots: none")?;
        } else {
            writeln!(output, "claude architect --add-dir roots (ordered):")?;
            for (index, directory) in additional_directories.iter().enumerate() {
                writeln!(output, "  {}. {}", index + 1, directory.display())?;
            }
        }
    }
    Ok(())
}

/// Resolve an explicit `--config` path before the profile loader opens it.
///
/// The loader already enforces owner, link count, permission, size, and
/// `O_NOFOLLOW` safety on whatever path it receives. Requiring a canonical
/// absolute regular file here keeps a symlinked parent component from
/// redirecting that check, matching the `--add-dir` contract.
fn validate_profile_config_path(path: &Path) -> Result<PathBuf> {
    const INVALID: &str = "--config must be an existing canonical absolute regular file";
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4096 {
        bail!(INVALID);
    }
    let canonical = fs::canonicalize(path).map_err(|_| anyhow::anyhow!(INVALID))?;
    let metadata = fs::symlink_metadata(path)?;
    if canonical != *path || metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(INVALID);
    }
    Ok(canonical)
}

fn validate_additional_directories(
    adapter: ArchitectAdapter,
    directories: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    if adapter == ArchitectAdapter::Codex && !directories.is_empty() {
        bail!("--add-dir is available only with the claude Architect adapter");
    }
    if directories.len() > 64 {
        bail!("Claude Architect accepts at most 64 --add-dir roots");
    }
    let mut unique = BTreeSet::new();
    let mut validated = Vec::with_capacity(directories.len());
    for directory in directories {
        if !directory.is_absolute() || directory.as_os_str().as_encoded_bytes().len() > 4096 {
            bail!("Claude Architect --add-dir must be an existing canonical absolute directory");
        }
        let canonical = fs::canonicalize(directory).map_err(|_| {
            anyhow::anyhow!(
                "Claude Architect --add-dir must be an existing canonical absolute directory"
            )
        })?;
        let metadata = fs::symlink_metadata(directory)?;
        if canonical != *directory || metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("Claude Architect --add-dir must be an existing canonical absolute directory");
        }
        if !unique.insert(directory.clone()) {
            bail!("Claude Architect --add-dir roots must be unique");
        }
        validated.push(directory.clone());
    }
    Ok(validated)
}

fn worker_adapter_bindings(
    architect_adapter: ArchitectAdapter,
    profiles: &SessionInvocationProfiles,
) -> Result<(&'static str, Vec<ReviewerAdapterBinding>)> {
    // Worker routes are independent of the foreground Architect provider.
    let _ = architect_adapter;
    let profiles = TaskWorkerProfiles::from_session_profiles(profiles)
        .map_err(|error| anyhow::anyhow!(error.detail))?;
    Ok((
        profiles.developer.provider.as_str(),
        profiles
            .reviewers
            .iter()
            .map(|binding| ReviewerAdapterBinding {
                reviewer_id: binding.reviewer_id,
                adapter: binding.profile.provider.as_str().into(),
            })
            .collect(),
    ))
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
    component: ExecutableIdentity,
}

enum ExactArchitectTool {
    Codex,
    Claude,
}

impl ExactArchitectTool {
    fn executable_path(&self) -> &Path {
        match self {
            Self::Codex => Path::new("codex"),
            Self::Claude => Path::new("claude"),
        }
    }
}

impl ExactTools {
    fn discover(adapter: ArchitectAdapter) -> Result<Self> {
        let architect = match adapter {
            ArchitectAdapter::Codex => ExactArchitectTool::Codex,
            ArchitectAdapter::Claude => ExactArchitectTool::Claude,
        };
        let component_path = resolve_component_path()?;
        let component = ExecutableIdentity::capture(component_path)?;
        Ok(Self {
            architect,
            component,
        })
    }

    fn revalidate(&self) -> Result<()> {
        self.component.revalidate()?;
        Ok(())
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

#[derive(Clone)]
struct ArchitectLaunchPaths {
    runtime: PathBuf,
    relay_socket: PathBuf,
}

impl ArchitectLaunchPaths {
    fn create(control: &ControlPaths, launch_id: &str) -> Result<Self> {
        let runtime_parent = control.architect_runtime_root_path();
        ensure_private_directory(&runtime_parent, true)?;
        let runtime = runtime_parent.join(launch_id);
        ensure_private_directory(&runtime, false)?;
        Ok(Self {
            runtime: runtime.clone(),
            relay_socket: runtime.join("relay.sock"),
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

struct BridgeChild {
    child: Child,
    bootstrap: UnixStream,
}

fn spawn_bridge(component: &Path) -> Result<BridgeChild> {
    let (parent, child_stream) = UnixStream::pair()?;
    parent.set_read_timeout(Some(Duration::from_secs(10)))?;
    parent.set_write_timeout(Some(Duration::from_secs(10)))?;
    let inherited_fd = child_stream.as_raw_fd();
    let expected_parent = std::process::id() as libc::pid_t;
    let mut command = Command::new(component);
    command
        .args(["bridge", "--bootstrap-fd", &inherited_fd.to_string()])
        .env_clear()
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

struct ArchitectLaunchContext {
    project_root: PathBuf,
    paths: ArchitectLaunchPaths,
    adapter: ArchitectAdapter,
    additional_directories: Vec<PathBuf>,
}

enum ArchitectProcess {
    Native(Child),
    Guarded(GuardianHandle),
}

impl ArchitectProcess {
    fn try_wait(&mut self) -> Result<Option<i32>> {
        match self {
            Self::Native(child) => child.try_wait()?.map(exit_code).transpose(),
            Self::Guarded(handle) => match handle.try_wait() {
                GuardianPoll::Running | GuardianPoll::CleanupPending => Ok(None),
                GuardianPoll::Complete(completion) => {
                    match completion.disposition {
                        GuardianCleanupDisposition::Clean
                        | GuardianCleanupDisposition::NativeFailure => {}
                        GuardianCleanupDisposition::OrphanedDescendants => {
                            bail!(
                                "Claude Architect exited with owned residual descendants; Guardian cleaned them before refusing the session result"
                            )
                        }
                        disposition => {
                            bail!(
                                "Claude Architect lifecycle ended with Guardian disposition {disposition:?}"
                            )
                        }
                    }
                    match (completion.native_code, completion.native_signal) {
                        (Some(code), None) => Ok(Some(code)),
                        (None, Some(signal)) => Ok(Some(128 + signal)),
                        _ => bail!("Claude Architect Guardian returned an ambiguous native exit"),
                    }
                }
                GuardianPoll::OwnershipLost(detail) => {
                    bail!("Claude Architect lifecycle ownership was lost: {detail}")
                }
            },
        }
    }
}

struct ArchitectLaunch {
    process: ArchitectProcess,
    native_pid: u32,
}

fn spawn_architect(
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
    claude_environment: Option<&MaterializedWorkerEnvironment>,
    profile: &ArchitectInvocationProfile,
) -> Result<ArchitectLaunch> {
    match context.adapter {
        ArchitectAdapter::Codex => spawn_native_codex_architect(tools, context, profile),
        ArchitectAdapter::Claude => {
            let environment = claude_environment.ok_or_else(|| {
                anyhow::anyhow!("Claude Architect launch environment was not validated")
            })?;
            spawn_native_claude_architect(tools, context, environment, profile)
        }
    }
}

fn spawn_native_codex_architect(
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
    profile: &ArchitectInvocationProfile,
) -> Result<ArchitectLaunch> {
    profile.validate()?;
    if profile.adapter() != ArchitectAdapter::Codex
        || context.adapter != ArchitectAdapter::Codex
        || !matches!(tools.architect, ExactArchitectTool::Codex)
    {
        bail!("native Codex architect launch received a different adapter");
    }
    tools.revalidate()?;
    let argv = architect_native_argv(tools, context, profile)?;
    validate_native_argv(&argv, tools, context, profile)?;
    let expected_parent = std::process::id() as libc::pid_t;
    let mut command = Command::new(&argv[0]);
    command.args(&argv[1..]).current_dir(&context.project_root);
    // Do not clear, filter, or replace the foreground Codex environment.
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
        process: ArchitectProcess::Native(child),
        native_pid,
    })
}

fn spawn_native_claude_architect(
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
    materialized: &MaterializedWorkerEnvironment,
    profile: &ArchitectInvocationProfile,
) -> Result<ArchitectLaunch> {
    profile.validate()?;
    if profile.adapter() != ArchitectAdapter::Claude
        || context.adapter != ArchitectAdapter::Claude
        || !matches!(tools.architect, ExactArchitectTool::Claude)
    {
        bail!("native Claude architect launch received a different adapter");
    }
    tools.revalidate()?;
    let argv = architect_native_argv(tools, context, profile)?;
    validate_native_argv(&argv, tools, context, profile)?;
    materialized.validate_claude_proxy()?;
    let mut command =
        GuardedCommand::new(&argv[0]).context("failed to prepare the Claude Architect Guardian")?;
    command
        .args(&argv[1..])
        .mode(GuardianMode::ForegroundArchitect)
        .current_dir(&context.project_root)
        .env_clear()
        .envs(materialized.iter())
        .require_claude_proxy();
    let handle = match command.spawn() {
        Ok(handle) => handle,
        Err(failure) => return finish_failed_guardian_spawn(failure),
    };
    let native_pid = handle.ready().native.pid;
    Ok(ArchitectLaunch {
        process: ArchitectProcess::Guarded(handle),
        native_pid,
    })
}

fn finish_failed_guardian_spawn(failure: GuardianSpawnFailure) -> Result<ArchitectLaunch> {
    match failure {
        GuardianSpawnFailure::Reaped(error) => {
            Err(error).context("failed to launch bare Claude executable from inherited PATH")
        }
        GuardianSpawnFailure::OwnershipLost(detail) => {
            bail!("Claude Architect Guardian lost lifecycle ownership before launch: {detail}")
        }
        GuardianSpawnFailure::CleanupPending { detail, handle } => {
            let mut handle = *handle;
            loop {
                match handle.terminate_and_reap(
                    GuardianCleanupReason::ProtocolFailure,
                    Duration::from_secs(3),
                ) {
                    Ok(_) => {
                        bail!(
                            "Claude Architect launch failed after Guardian cleanup completed: {detail}"
                        )
                    }
                    Err(GuardianHandleFailure::CleanupPending(_)) => {}
                    Err(GuardianHandleFailure::OwnershipLost(lost)) => {
                        bail!(
                            "Claude Architect launch failed and Guardian ownership was lost: {lost}"
                        )
                    }
                }
            }
        }
    }
}

fn architect_native_argv(
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
    profile: &ArchitectInvocationProfile,
) -> Result<Vec<String>> {
    let executable = path_text(
        "architect native executable",
        tools.architect.executable_path(),
    )?
    .into();
    match profile {
        ArchitectInvocationProfile::Codex { profile } => {
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
                path_text("architect project directory", &context.project_root)?.into(),
                "--no-alt-screen".into(),
            ];
            argv.extend(codex_control_mcp_overrides(tools, context)?);
            Ok(argv)
        }
        ArchitectInvocationProfile::Claude { profile } => {
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
                                &context.paths.relay_socket,
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
            ];
            for directory in &context.additional_directories {
                argv.extend([
                    "--add-dir".into(),
                    path_text("Claude Architect --add-dir", directory)?.into(),
                ]);
            }
            argv.extend(["--mcp-config".into(), mcp_config]);
            if profile.dangerously_skip_permissions {
                argv.push("--dangerously-skip-permissions".into());
            }
            Ok(argv)
        }
    }
}

fn codex_control_mcp_overrides(
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
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
            path_text("architect relay socket", &context.paths.relay_socket)?.into(),
        ),
    ])
    .to_string();
    // Replace this one hcom-owned leaf as a whole. Native user configuration
    // remains loaded, including every other MCP server, while a stale or
    // user-defined table under the reserved name cannot leave incompatible
    // transport fields merged into the control server.
    //
    // Keep these control tools out of Codex code mode. A long-running nested
    // MCP call is hosted by a code-mode cell which periodically yields back to
    // the model even though the underlying session_wait is still pending.
    // Direct MCP exposure lets Codex own the one blocking tool call until hcom
    // returns a real worker-result/action/terminal event, without changing
    // code-mode behavior for any native or user-configured tool.
    let value = format!(
        "{prefix}={{ command = {command}, args = {args}, startup_timeout_sec = 10, \
         tool_timeout_sec = {CODEX_CONTROL_TOOL_TIMEOUT_SECS}, enabled = true, \
         default_tools_approval_mode = \"approve\", omit_tools_from = [\"code_mode\"] }}"
    );
    Ok(vec!["--config".into(), value])
}

fn validate_native_argv(
    native: &[String],
    tools: &ExactTools,
    context: &ArchitectLaunchContext,
    profile: &ArchitectInvocationProfile,
) -> Result<()> {
    let expected = architect_native_argv(tools, context, profile)?;
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

fn wait_for_architect_and_bridge(
    architect: &mut ArchitectProcess,
    bridge: &mut Child,
    supervisor_stop: &AtomicBool,
) -> Result<i32> {
    let result = (|| -> Result<i32> {
        let mut bridge_revoked = false;
        loop {
            if supervisor_stop.load(Ordering::Acquire) {
                bail!("architect session received a termination signal");
            }
            if let Some(exit_code) = architect.try_wait()? {
                if bridge_revoked {
                    return Ok(exit_code);
                }
                let deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline {
                    if let Some(bridge_status) = bridge.try_wait()? {
                        if !bridge_status.success() {
                            bail!("architect bridge failed during binding revoke: {bridge_status}");
                        }
                        return Ok(exit_code);
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                bail!("architect bridge did not revoke its binding after native architect exited");
            }
            if !bridge_revoked && let Some(status) = bridge.try_wait()? {
                if !status.success() {
                    bail!("architect bridge exited before native architect: {status}");
                }
                bridge_revoked = true;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    })();
    if result.is_err() {
        terminate_architect(architect);
        terminate_child(bridge);
    }
    result
}

fn terminate_architect(process: &mut ArchitectProcess) {
    match process {
        ArchitectProcess::Native(child) => terminate_child(child),
        ArchitectProcess::Guarded(handle) => loop {
            match handle.terminate_and_reap(GuardianCleanupReason::Cancel, Duration::from_secs(3)) {
                Ok(_) | Err(GuardianHandleFailure::OwnershipLost(_)) => break,
                Err(GuardianHandleFailure::CleanupPending(_)) => {}
            }
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::profile::{ClaudeInvocationProfile, CodexInvocationProfile};
    use std::os::unix::fs::symlink;

    const RUNTIME_MODE_HELPER: &str = "HCOM_ARCH_RUNTIME_MODE_HELPER";

    fn fixture_context(
        adapter: ArchitectAdapter,
        additional_directories: Vec<PathBuf>,
    ) -> (tempfile::TempDir, ExactTools, ArchitectLaunchContext) {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project_root = root.join("project");
        let runtime = root.join("runtime");
        fs::create_dir(&project_root).unwrap();
        fs::create_dir(&runtime).unwrap();
        let component = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        (
            temp,
            ExactTools {
                architect: match adapter {
                    ArchitectAdapter::Codex => ExactArchitectTool::Codex,
                    ArchitectAdapter::Claude => ExactArchitectTool::Claude,
                },
                component,
            },
            ArchitectLaunchContext {
                project_root,
                paths: ArchitectLaunchPaths {
                    relay_socket: runtime.join("relay.sock"),
                    runtime,
                },
                adapter,
                additional_directories,
            },
        )
    }

    #[test]
    fn session_runtime_helper_process() {
        if std::env::var_os(RUNTIME_MODE_HELPER).is_none() {
            return;
        }
        // SAFETY: this exact-filtered helper runs in its own disposable process.
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
    fn explicit_architect_cli_overrides_only_the_selected_profile() {
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
        let reviewers_before = profiles.reviewers.clone();
        apply_architect_cli_overrides(&args, &mut profiles).unwrap();
        let architect = profiles.architect.codex().unwrap();
        assert_eq!(architect.model, "gpt-5.6-sol-cli");
        assert_eq!(architect.reasoning_effort, "max");
        assert_eq!(profiles.developer, developer_before);
        assert_eq!(profiles.reviewers, reviewers_before);

        let claude_args = ArchitectArgs::try_parse_from([
            "hcom arch",
            "claude",
            "--model",
            "sonnet",
            "--effort",
            "medium",
        ])
        .unwrap();
        let mut profiles = SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude);
        apply_architect_cli_overrides(&claude_args, &mut profiles).unwrap();
        let architect = profiles.architect.claude().unwrap();
        assert_eq!(architect.model, "sonnet");
        assert_eq!(architect.effort, "medium");
    }

    #[test]
    fn startup_summary_discloses_the_exact_profile_config_source() {
        let render = |loaded_from_file, explicit| {
            let mut output = Vec::new();
            write_profile_config_line(
                &mut output,
                Path::new("/srv/profiles.toml"),
                loaded_from_file,
                explicit,
            )
            .unwrap();
            String::from_utf8(output).unwrap()
        };
        assert_eq!(
            render(true, false),
            "profile config: /srv/profiles.toml (loaded once)\n"
        );
        assert_eq!(
            render(true, true),
            "profile config: /srv/profiles.toml (--config; loaded once)\n"
        );
        assert_eq!(
            render(false, false),
            "profile config: /srv/profiles.toml (not present; built-in defaults)\n"
        );
    }

    #[test]
    fn explicit_config_requires_a_canonical_absolute_regular_file() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let file = root.join("profiles.toml");
        fs::write(&file, "").unwrap();
        assert_eq!(validate_profile_config_path(&file).unwrap(), file);

        // `Path` equality ignores `.` segments, so only a component that can
        // actually redirect the resolved file is rejected as non-canonical.
        assert!(validate_profile_config_path(&root.join(".").join("profiles.toml")).is_ok());
        let parent_relative = root
            .join("..")
            .join(root.file_name().unwrap())
            .join("profiles.toml");

        for rejected in [
            PathBuf::from("profiles.toml"),
            root.join("missing.toml"),
            root.clone(),
            parent_relative,
        ] {
            let error = validate_profile_config_path(&rejected).unwrap_err();
            assert_eq!(
                error.to_string(),
                "--config must be an existing canonical absolute regular file",
                "path={}",
                rejected.display()
            );
        }

        let file_link = root.join("link.toml");
        std::os::unix::fs::symlink(&file, &file_link).unwrap();
        assert!(validate_profile_config_path(&file_link).is_err());

        // A symlinked parent component would otherwise redirect the loader's
        // own O_NOFOLLOW check, which only covers the final component.
        let directory_link = root.join("dirlink");
        std::os::unix::fs::symlink(&root, &directory_link).unwrap();
        assert!(validate_profile_config_path(&directory_link.join("profiles.toml")).is_err());
    }

    #[test]
    fn explicit_config_replaces_the_default_path_and_never_degrades_to_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let write = |name: &str, contents: &str| {
            let path = root.join(name);
            fs::write(&path, contents).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            path
        };
        // Both sources fail closed with distinguishable messages, so the error
        // itself proves which file the Architect actually read, and neither
        // case can reach the foreground terminal preflight.
        let default_path = write(
            "config.toml",
            "[architect.developer]\nmodel = \"bad model\"\n",
        );
        let explicit = write(
            "explicit.toml",
            "[architect.reviewer1]\nreasoning_effort = \"BOGUS\"\n",
        );

        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--config".into(),
                explicit.to_string_lossy().into_owned(),
            ],
            Some(&default_path),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("reviewer reasoning_effort"),
            "explicit --config must win over the default path: {error}"
        );

        // Without --config the same invocation reads the default path instead.
        let error = run_cli(&["arch".into(), "codex".into()], Some(&default_path)).unwrap_err();
        assert!(
            error.to_string().contains("developer model"),
            "default path must still be used when --config is absent: {error}"
        );

        // An absent default path selects the built-in profiles; an absent
        // --config must not.
        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--config".into(),
                root.join("missing.toml").to_string_lossy().into_owned(),
            ],
            Some(&default_path),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "--config must be an existing canonical absolute regular file"
        );
    }

    #[test]
    fn github_flag_fails_closed_on_missing_or_invalid_config_before_terminal_launch() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let write = |name: &str, contents: &str| {
            let path = root.join(name);
            fs::write(&path, contents).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            path
        };
        let absent = write("absent.toml", "");
        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--protected-auto-merge".into(),
            ],
            Some(&absent),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "--protected-auto-merge requires --github-pr"
        );
        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--single-review".into(),
                "--github-pr".into(),
                "--config".into(),
                absent.to_string_lossy().into_owned(),
            ],
            None,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "--github-pr requires a closed [architect.github] configuration"
        );

        let invalid = write(
            "invalid.toml",
            "[architect.github]\nstale_unknown_field = true\n",
        );
        let local_error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--single-review".into(),
                "--config".into(),
                invalid.to_string_lossy().into_owned(),
            ],
            None,
        )
        .unwrap_err();
        assert_eq!(
            local_error.to_string(),
            "hcom arch requires stdin/stdout/stderr on a real terminal",
            "without --github-pr the stale table must remain semantically inert"
        );
        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--single-review".into(),
                "--github-pr".into(),
                "--config".into(),
                invalid.to_string_lossy().into_owned(),
            ],
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid [architect.github] configuration"),
            "GitHub config must be parsed before foreground terminal validation: {error}"
        );

        let repository_root = fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        let valid = write(
            "valid.toml",
            &format!(
                r#"
[architect.github]
owner = "owner"
repository = "repository"
local_repository_root = "{}"
base_branch = "master"
merge_method = "squash"
merge_wait_seconds = 21600
delete_remote_branch_after_merge = true
private_repository_required = true

[architect.github.apps.architect]
app_id = 1
slug = "hcom-arch"
private_key_file = "/var/lib/hcom-secrets/arch.pem"

[architect.github.apps.developer]
app_id = 2
slug = "hcom-dev"
private_key_file = "/var/lib/hcom-secrets/dev.pem"

[architect.github.apps.reviewer1]
app_id = 3
slug = "hcom-reviewer1"
private_key_file = "/var/lib/hcom-secrets/reviewer1.pem"
"#,
                repository_root.display()
            ),
        );
        let error = run_cli(
            &[
                "arch".into(),
                "codex".into(),
                "--single-review".into(),
                "--github-pr".into(),
                "--config".into(),
                valid.to_string_lossy().into_owned(),
            ],
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to open the configured architect GitHub App key"),
            "GitHub App keys must be opened before foreground terminal validation: {error}"
        );
    }

    #[test]
    fn single_review_flag_is_codex_only_before_terminal_or_claude_preflight() {
        let error = run_cli(
            &["arch".into(), "claude".into(), "--single-review".into()],
            None,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "--single-review is available only with `hcom arch codex`"
        );
    }

    #[test]
    fn codex_native_argv_keeps_only_hcom_control_tools_out_of_code_mode() {
        let (_temp, tools, context) = fixture_context(ArchitectAdapter::Codex, Vec::new());
        let profile = ArchitectInvocationProfile::Codex {
            profile: CodexInvocationProfile::architect_default(),
        };
        let argv = architect_native_argv(&tools, &context, &profile).unwrap();
        assert_eq!(argv[0], "codex");
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--model", "gpt-5.6-sol"])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--sandbox", "danger-full-access"])
        );
        assert!(
            argv.windows(2)
                .any(|pair| pair == ["--ask-for-approval", "never"])
        );
        assert_eq!(
            argv.iter()
                .filter(|argument| *argument == "--config")
                .count(),
            2
        );
        let encoded = argv.join("\n");
        assert!(encoded.contains("mcp_servers.hcom_session_task_control={"));
        assert!(encoded.contains("default_tools_approval_mode = \"approve\""));
        assert!(encoded.contains("omit_tools_from = [\"code_mode\"]"));
        let control_override = argv
            .windows(2)
            .find_map(|pair| {
                (pair[0] == "--config"
                    && pair[1].starts_with("mcp_servers.hcom_session_task_control="))
                .then_some(pair[1].as_str())
            })
            .unwrap();
        let parsed: toml::Value = toml::from_str(control_override).unwrap();
        assert_eq!(
            parsed["mcp_servers"]["hcom_session_task_control"]["omit_tools_from"],
            toml::Value::Array(vec![toml::Value::String("code_mode".into())])
        );
        assert!(!argv.iter().any(|argument| argument == "--disable"));
        assert!(!argv.iter().any(|argument| argument == "-"));
    }

    #[test]
    fn claude_native_argv_is_blank_additive_and_preserves_add_dir_order() {
        let temp = tempfile::tempdir().unwrap();
        let first = fs::canonicalize(temp.path()).unwrap();
        let second_temp = tempfile::tempdir().unwrap();
        let second = fs::canonicalize(second_temp.path()).unwrap();
        let (_fixture, tools, context) = fixture_context(
            ArchitectAdapter::Claude,
            vec![first.clone(), second.clone()],
        );
        let profile = ArchitectInvocationProfile::Claude {
            profile: ClaudeInvocationProfile::architect_default(),
        };
        let argv = architect_native_argv(&tools, &context, &profile).unwrap();
        assert_eq!(argv[0], "claude");
        assert!(argv.windows(2).any(|pair| pair == ["--model", "opus"]));
        assert!(argv.windows(2).any(|pair| pair == ["--effort", "xhigh"]));
        let add_dirs: Vec<_> = argv
            .windows(2)
            .filter(|pair| pair[0] == "--add-dir")
            .map(|pair| pair[1].clone())
            .collect();
        assert_eq!(
            add_dirs,
            vec![
                first.to_string_lossy().into_owned(),
                second.to_string_lossy().into_owned()
            ]
        );
        for forbidden in [
            "--name",
            "--session-id",
            "--tools",
            "--setting-sources",
            "--strict-mcp-config",
            "--disable-slash-commands",
            "--prompt-suggestions",
            "--no-chrome",
        ] {
            assert!(!argv.iter().any(|argument| argument == forbidden));
        }
        assert!(!argv.iter().any(|argument| argument == "-"));
        assert_eq!(
            argv.iter()
                .filter(|argument| *argument == "--mcp-config")
                .count(),
            1
        );
        let mcp = argv
            .windows(2)
            .find(|pair| pair[0] == "--mcp-config")
            .map(|pair| &pair[1])
            .unwrap();
        let mcp: serde_json::Value = serde_json::from_str(mcp).unwrap();
        assert_eq!(mcp["mcpServers"].as_object().unwrap().len(), 1);
        assert!(mcp["mcpServers"]["hcom_session_task_control"].is_object());
        assert_eq!(
            argv.last().map(String::as_str),
            Some("--dangerously-skip-permissions")
        );
    }

    #[test]
    fn claude_startup_summary_discloses_pins_lifecycle_and_ordered_roots() {
        let temp = tempfile::tempdir().unwrap();
        let first = fs::canonicalize(temp.path()).unwrap();
        let second_temp = tempfile::tempdir().unwrap();
        let second = fs::canonicalize(second_temp.path()).unwrap();
        let mut output = Vec::new();
        write_claude_startup_summary(
            &mut output,
            ArchitectAdapter::Claude,
            &[first.clone(), second.clone()],
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(&format!("{CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS}=1")));
        assert!(output.contains(&format!("{CLAUDE_DISABLE_BACKGROUND_TASKS}=1")));
        assert!(output.contains(GUARDIAN_LIFECYCLE_BOUNDARY));
        assert!(
            output.find(&first.display().to_string()).unwrap()
                < output.find(&second.display().to_string()).unwrap()
        );
    }

    #[test]
    fn claude_additional_directories_require_unique_canonical_absolute_roots() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        assert_eq!(
            validate_additional_directories(ArchitectAdapter::Claude, std::slice::from_ref(&root))
                .unwrap(),
            vec![root.clone()]
        );
        assert!(
            validate_additional_directories(
                ArchitectAdapter::Claude,
                &[root.clone(), root.clone()]
            )
            .is_err()
        );
        assert!(
            validate_additional_directories(ArchitectAdapter::Codex, std::slice::from_ref(&root))
                .is_err()
        );
        assert!(
            validate_additional_directories(ArchitectAdapter::Claude, &[PathBuf::from("relative")])
                .is_err()
        );
        let link = root.join("link");
        symlink(&root, &link).unwrap();
        assert!(validate_additional_directories(ArchitectAdapter::Claude, &[link]).is_err());
    }

    #[test]
    fn both_public_entrypoints_use_the_mixed_worker_default() {
        for adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            let profiles = SessionInvocationProfiles::for_task_lane(adapter).unwrap();
            assert_eq!(
                worker_adapter_bindings(adapter, &profiles).unwrap(),
                (
                    "codex-exec",
                    vec![
                        ReviewerAdapterBinding {
                            reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
                            adapter: "codex-exec".into(),
                        },
                        ReviewerAdapterBinding {
                            reviewer_id: crate::worker::profile::ReviewerId::Reviewer2,
                            adapter: "claude-exec".into(),
                        },
                    ],
                )
            );
        }
    }

    #[test]
    fn both_architects_bind_all_sixteen_worker_provider_combinations() {
        for architect_adapter in [ArchitectAdapter::Codex, ArchitectAdapter::Claude] {
            for developer_claude in [false, true] {
                for reviewer1_claude in [false, true] {
                    for reviewer2_claude in [false, true] {
                        let mut profiles =
                            SessionInvocationProfiles::for_task_lane(architect_adapter).unwrap();
                        profiles.developer = if developer_claude {
                            DeveloperInvocationProfile::Claude {
                            profile:
                                crate::worker::profile::ClaudeInvocationProfile::developer_default(),
                        }
                        } else {
                            DeveloperInvocationProfile::Codex {
                                profile: CodexInvocationProfile::developer_default(),
                            }
                        };
                        *profiles.reviewer1_mut() = if reviewer1_claude {
                            ReviewerInvocationProfile::Claude {
                            profile:
                                crate::worker::profile::ClaudeInvocationProfile::reviewer_default(),
                        }
                        } else {
                            ReviewerInvocationProfile::Codex {
                                profile: CodexInvocationProfile::reviewer_default(),
                            }
                        };
                        *profiles.reviewer2_mut() = if reviewer2_claude {
                            ReviewerInvocationProfile::Claude {
                            profile:
                                crate::worker::profile::ClaudeInvocationProfile::reviewer_default(),
                        }
                        } else {
                            ReviewerInvocationProfile::Codex {
                                profile: CodexInvocationProfile::reviewer_default(),
                            }
                        };
                        assert_eq!(profiles.architect.adapter(), architect_adapter);
                        assert_eq!(
                            worker_adapter_bindings(architect_adapter, &profiles).unwrap(),
                            (
                                if developer_claude {
                                    "claude-exec"
                                } else {
                                    "codex-exec"
                                },
                                vec![
                                    ReviewerAdapterBinding {
                                        reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
                                        adapter: if reviewer1_claude {
                                            "claude-exec"
                                        } else {
                                            "codex-exec"
                                        }
                                        .into(),
                                    },
                                    ReviewerAdapterBinding {
                                        reviewer_id: crate::worker::profile::ReviewerId::Reviewer2,
                                        adapter: if reviewer2_claude {
                                            "claude-exec"
                                        } else {
                                            "codex-exec"
                                        }
                                        .into(),
                                    },
                                ],
                            )
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn startup_summary_displays_all_roles_and_claude_platform_policy() {
        let profiles = SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        let mut output = Vec::new();
        write_profile_summary(&mut output, &profiles).unwrap();
        write_claude_startup_summary(&mut output, ArchitectAdapter::Codex, &[]).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(
            "architect profile: codex model=gpt-5.6-sol reasoning=xhigh sandbox=danger-full-access approval=never"
        ));
        assert!(output.contains(
            "developer profile: codex model=gpt-5.6-sol reasoning=xhigh sandbox=danger-full-access approval=never"
        ));
        assert!(output.contains(
            "reviewer1 profile: codex model=gpt-5.6-sol reasoning=xhigh sandbox=danger-full-access approval=never"
        ));
        assert!(output.contains(
            "reviewer2 profile: claude model=opus effort=xhigh dangerously_skip_permissions=true"
        ));
        assert!(output.contains("Linux per-invocation PR_SET_CHILD_SUBREAPER Guardian"));
        assert!(output.contains("external task repositories use native --add-dir"));
        assert!(output.contains("not a filesystem allowlist"));
        assert!(output.contains(GUARDIAN_LIFECYCLE_BOUNDARY));

        let mut notice = Vec::new();
        write_legacy_reviewer_notice(&mut notice, true, &profiles).unwrap();
        assert_eq!(
            String::from_utf8(notice).unwrap(),
            "deprecated profile config: [architect.reviewer] was resolved once and copied to reviewer1 and reviewer2; declare both canonical tables explicitly\n"
        );

        let mut single = profiles;
        single.retain_reviewer1().unwrap();
        let mut output = Vec::new();
        write_profile_summary(&mut output, &single).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("review mode: single"));
        assert!(output.contains("reviewer1 profile:"));
        assert!(!output.contains("reviewer2 profile:"));

        let mut notice = Vec::new();
        write_legacy_reviewer_notice(&mut notice, true, &single).unwrap();
        assert_eq!(
            String::from_utf8(notice).unwrap(),
            "deprecated profile config: [architect.reviewer] was resolved once for reviewer1; declare [architect.reviewer1] explicitly\n"
        );
    }

    #[test]
    fn delivery_startup_summary_distinguishes_local_and_frozen_github_authority() {
        let mut local = Vec::new();
        write_delivery_startup_summary(&mut local, None).unwrap();
        let local = String::from_utf8(local).unwrap();
        assert_eq!(
            local,
            "task repositories: discovered from project documentation and bound only after explicit execution authorization; each developer commits directly there\n"
        );

        let app = |id: u64, slug: &str, permission: &str| crate::control_api::GitHubAppBinding {
            app_id: id,
            installation_id: id + 10,
            slug: slug.into(),
            bot_user_id: id + 20,
            effective_permissions: std::collections::BTreeMap::from([(
                permission.into(),
                crate::control_api::GitHubPermissionLevel::Write,
            )]),
        };
        let binding = crate::control_api::GitHubPullRequestBinding {
            delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
            owner: "owner".into(),
            repository: "repository".into(),
            repository_id: 42,
            visibility: "private".into(),
            local_repository_root: "/source".into(),
            base_branch: "master".into(),
            merge_method: "squash".into(),
            merge_wait_seconds: 21_600,
            delete_remote_branch_after_merge: true,
            architect_app: app(1, "hcom-arch", "checks"),
            developer_app: app(2, "hcom-dev", "contents"),
            reviewer_apps: vec![crate::control_api::GitHubReviewerAppBinding {
                reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
                app: app(3, "hcom-reviewer1", "pull_requests"),
            }],
            review_check_name: crate::control_api::GITHUB_REVIEW_CHECK_NAME.into(),
        };
        let inspection = crate::control_api::GitHubInspectionBinding {
            inspected_repository_id: 42,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: "a".repeat(40),
            ruleset_attestation_sha256: Some("b".repeat(64)),
            inspection_id: "inspection-1".into(),
        };
        let mut github = Vec::new();
        write_delivery_startup_summary(&mut github, Some((&binding, &inspection))).unwrap();
        let github = String::from_utf8(github).unwrap();
        for required in [
            "delivery mode: GitHub Pull Request (explicit --github-pr)",
            "github delivery policy: protected_auto_merge",
            "github repository: owner/repository id=42 visibility=private local_root=/source base=master",
            "github architect app: app_id=1 installation_id=11 slug=hcom-arch bot_user_id=21 permissions={\"checks\":\"write\"}",
            "github reviewer1 app: app_id=3 installation_id=13 slug=hcom-reviewer1",
            "github initial inspection: id=inspection-1 repository_id=42 base_ref=refs/heads/master",
            "startup preflight was read-only",
            "published byte-for-byte without redaction or secret scanning",
            "60 KiB UTF-8 hard cap",
            "one append-only branch, linked worktree, and Pull Request",
        ] {
            assert!(github.contains(required), "missing {required:?}: {github}");
        }

        let mut manual_binding = binding;
        manual_binding.delivery_policy = crate::control_api::GitHubDeliveryPolicy::Manual;
        let mut manual_inspection = inspection;
        manual_inspection.ruleset_attestation_sha256 = None;
        let mut manual = Vec::new();
        write_delivery_startup_summary(&mut manual, Some((&manual_binding, &manual_inspection)))
            .unwrap();
        let manual = String::from_utf8(manual).unwrap();
        assert!(manual.contains("github delivery policy: manual"));
        assert!(manual.contains("no ruleset attestation, merge, remote-ref deletion"));
        assert!(manual.contains("no hcom-attested server-side protection"));
        assert!(manual.contains("ruleset_attestation_sha256=not_applicable"));
        assert!(!manual.contains("github protected delivery:"));
        assert!(!github.contains("private_key"));
        assert!(!github.contains("token"));
    }
}
