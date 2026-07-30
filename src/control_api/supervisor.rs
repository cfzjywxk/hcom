//! Private, runtime-only control endpoint for one foreground architect session.

use super::codec::{read_request_frame, write_response_frame};
use super::peer::{
    PeerCredentials, peer_credentials, process_birth_identity, process_has_ancestor,
    process_is_live_identity, process_owns_foreground_tty,
};
use super::protocol::{
    ActionName, CallerAuth, ControlAction, ControlErrorCode, ControlRequest, ControlResponse,
    ControlResult, canonical_action_set, parse_canonical_action_set,
};
use super::registration::{
    NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS, NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS,
    NATIVE_SESSION_REFUSAL_CAPABILITY, NATIVE_SESSION_REFUSAL_IDENTITY,
    NATIVE_SESSION_REFUSAL_STATE, NATIVE_SESSION_REFUSAL_UNAVAILABLE,
    NATIVE_SESSION_REFUSAL_VERSION, REGISTRATION_REFUSAL_GENERIC, RegistrationAction,
    RegistrationCaller, RegistrationRequest, RegistrationResponse, validate_request_envelope,
};
use crate::orchestrator::{SessionRuntimeSources, SessionStartup, SessionSupervisor};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_RECORDS: usize = 1024;

#[derive(Debug, Clone)]
pub struct ControlPaths {
    run_root: PathBuf,
    lock_root: PathBuf,
}

impl ControlPaths {
    pub fn new(run_root: impl AsRef<Path>, lock_root: impl AsRef<Path>) -> Result<Self> {
        let run_root = canonical_private_directory(run_root.as_ref(), "session runtime root")?;
        let lock_root = canonical_private_directory(lock_root.as_ref(), "repository lock root")?;
        Ok(Self {
            run_root,
            lock_root,
        })
    }

    pub fn socket_path(&self) -> PathBuf {
        self.run_root.join("control.sock")
    }

    pub fn registration_socket_path(&self) -> PathBuf {
        self.run_root.join("registration.sock")
    }

    pub fn architect_state_root_path(&self) -> PathBuf {
        self.run_root.join("architect-state")
    }

    pub fn architect_runtime_root_path(&self) -> PathBuf {
        self.run_root.join("architect-runtime")
    }

    pub fn run_root(&self) -> &Path {
        &self.run_root
    }

    pub fn lock_root(&self) -> &Path {
        &self.lock_root
    }
}

pub(crate) struct SessionSupervisorEndpoint {
    control: SessionSupervisorControl,
    listener: UnixListener,
    _socket_guard: SocketGuard,
    registration_listener: UnixListener,
    _registration_socket_guard: SocketGuard,
}

impl SessionSupervisorEndpoint {
    pub(crate) fn bind(
        paths: ControlPaths,
        run_id: String,
        project_root: PathBuf,
        sources: SessionRuntimeSources,
    ) -> Result<Self> {
        let control = SessionSupervisorControl::open(&paths, run_id, project_root, sources)?;
        let socket_guard = SocketGuard::bind(&paths.socket_path())?;
        let listener = socket_guard.listener.try_clone()?;
        let registration_socket_guard = SocketGuard::bind(&paths.registration_socket_path())?;
        let registration_listener = registration_socket_guard.listener.try_clone()?;
        Ok(Self {
            control,
            listener,
            _socket_guard: socket_guard,
            registration_listener,
            _registration_socket_guard: registration_socket_guard,
        })
    }

    pub(crate) fn startup(&self) -> &SessionStartup {
        self.control.supervisor.startup()
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.listener.set_nonblocking(nonblocking)?;
        self.registration_listener.set_nonblocking(nonblocking)?;
        Ok(())
    }

    pub(crate) fn try_serve_one(&mut self) -> Result<bool> {
        let (mut stream, _) = match self.listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error).context("failed to accept session control connection"),
        };
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        let _ = self.control.serve_stream(&mut stream);
        Ok(true)
    }

    pub(crate) fn try_serve_registration_one(&mut self) -> Result<bool> {
        let (mut stream, _) = match self.registration_listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => {
                return Err(error).context("failed to accept architect registration connection");
            }
        };
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        let _ = self.control.serve_registration_stream(&mut stream);
        Ok(true)
    }

    pub(crate) fn run_until_stopped(&mut self, stop: &AtomicBool) -> Result<()> {
        self.set_nonblocking(true)?;
        while !stop.load(Ordering::Acquire) {
            let mut handled = false;
            for _ in 0..16 {
                if self.try_serve_registration_one()? {
                    handled = true;
                } else {
                    break;
                }
            }
            for _ in 0..16 {
                if self.try_serve_one()? {
                    handled = true;
                } else {
                    break;
                }
            }
            let _ = self.control.supervisor.poll_once();
            if !handled {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        self.control.supervisor.shutdown()
    }
}

struct SessionSupervisorControl {
    supervisor: SessionSupervisor,
    expected_uid: u32,
    parent_pid: u32,
    parent_birth: String,
    bindings: BTreeMap<String, ArchitectBinding>,
    requests: BTreeMap<(String, String), RequestRecord>,
}

struct RequestRecord {
    payload_hash: String,
    response: Option<ControlResponse>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingState {
    Pending,
    ProcessBound,
    NativeBound,
    Closed,
}

struct ArchitectBinding {
    id: String,
    project_root: PathBuf,
    launch_nonce_hash: String,
    capability_hash: String,
    action_set_json: String,
    action_set_hash: String,
    state: BindingState,
    version: u64,
    architect_pid: Option<u32>,
    architect_process_birth: Option<String>,
    bridge_pid: Option<u32>,
    bridge_process_birth: Option<String>,
    relay_executable_contract_hash: Option<String>,
    relay_runtime_scope_hash: Option<String>,
    native_session_id: Option<String>,
}

impl SessionSupervisorControl {
    fn open(
        paths: &ControlPaths,
        run_id: String,
        project_root: PathBuf,
        sources: SessionRuntimeSources,
    ) -> Result<Self> {
        // SAFETY: geteuid has no preconditions.
        let expected_uid = unsafe { libc::geteuid() };
        let parent_pid = std::process::id();
        let parent_birth = process_birth_identity(parent_pid)?;
        let supervisor = SessionSupervisor::open(
            run_id,
            project_root,
            paths.run_root().to_owned(),
            paths.lock_root().to_owned(),
            sources,
        )?;
        Ok(Self {
            supervisor,
            expected_uid,
            parent_pid,
            parent_birth,
            bindings: BTreeMap::new(),
            requests: BTreeMap::new(),
        })
    }

    fn serve_registration_stream(&mut self, stream: &mut UnixStream) -> Result<()> {
        let peer = peer_credentials(stream)?;
        if peer.uid != self.expected_uid {
            bail!("registration peer uid mismatch");
        }
        let frame = read_request_frame(stream)?;
        let request: RegistrationRequest = match serde_json::from_slice(&frame) {
            Ok(request) => request,
            Err(_) => {
                return write_registration_response(
                    stream,
                    &RegistrationResponse::error("", "malformed registration request"),
                );
            }
        };
        let response = match validate_request_envelope(&request)
            .and_then(|()| self.handle_registration_request(peer, &request))
        {
            Ok(version) => RegistrationResponse::success(&request.request_id, version),
            Err(error) => RegistrationResponse::error(
                &request.request_id,
                registration_refusal_code(&request.action, &error),
            ),
        };
        write_registration_response(stream, &response)
    }

    fn handle_registration_request(
        &mut self,
        peer: PeerCredentials,
        request: &RegistrationRequest,
    ) -> Result<u64> {
        match (&request.caller, &request.action) {
            (
                RegistrationCaller::Human { process_birth },
                RegistrationAction::CreateBinding {
                    binding_id,
                    project_root,
                    architect_name,
                    architect_adapter,
                    launch_nonce,
                    capability,
                    actions,
                },
            ) => {
                self.authorize_parent(peer, process_birth, true)?;
                validate_id(binding_id)?;
                validate_text(architect_name, 128)?;
                validate_text(architect_adapter, 128)?;
                validate_secret(launch_nonce)?;
                validate_secret(capability)?;
                let project_root = PathBuf::from(project_root);
                if project_root != self.supervisor.startup().project_root {
                    bail!("architect binding project directory differs from this run");
                }
                if self.bindings.contains_key(binding_id) {
                    bail!("architect binding id already exists");
                }
                let (action_set_json, action_set) = canonical_action_set(actions.iter().copied())
                    .map_err(|error| anyhow::anyhow!(error))?;
                if action_set != ActionName::ARCHITECT.into_iter().collect() {
                    bail!("architect action capability differs from the exact session set");
                }
                let binding = ArchitectBinding {
                    id: binding_id.clone(),
                    project_root,
                    launch_nonce_hash: secret_hash(b"hcom-session/launch-nonce/v1", launch_nonce),
                    capability_hash: secret_hash(b"hcom-session/capability/v1", capability),
                    action_set_hash: sha256_hex(action_set_json.as_bytes()),
                    action_set_json,
                    state: BindingState::Pending,
                    version: 0,
                    architect_pid: None,
                    architect_process_birth: None,
                    bridge_pid: None,
                    bridge_process_birth: None,
                    relay_executable_contract_hash: None,
                    relay_runtime_scope_hash: None,
                    native_session_id: None,
                };
                self.bindings.insert(binding_id.clone(), binding);
                Ok(0)
            }
            (
                RegistrationCaller::Human { process_birth },
                RegistrationAction::BindProcess {
                    binding_id,
                    expected_version,
                    architect_pid,
                    architect_process_birth,
                    bridge_pid,
                    bridge_process_birth,
                    relay_executable_contract_hash,
                    relay_runtime_scope_hash,
                },
            ) => {
                self.authorize_parent(peer, process_birth, true)?;
                validate_hash(relay_executable_contract_hash)?;
                validate_hash(relay_runtime_scope_hash)?;
                if *architect_pid <= 1 || *bridge_pid <= 1 || architect_pid == bridge_pid {
                    bail!("architect binding PIDs are invalid");
                }
                if process_birth_identity(*architect_pid)? != *architect_process_birth
                    || process_birth_identity(*bridge_pid)? != *bridge_process_birth
                    || !process_has_ancestor(
                        *architect_pid,
                        &[(self.parent_pid, self.parent_birth.clone())],
                    )?
                    || !process_has_ancestor(
                        *bridge_pid,
                        &[(self.parent_pid, self.parent_birth.clone())],
                    )?
                {
                    bail!("architect binding process ancestry is invalid");
                }
                let binding = self.binding_mut(binding_id, *expected_version)?;
                if binding.state != BindingState::Pending {
                    bail!("architect binding is not pending");
                }
                binding.architect_pid = Some(*architect_pid);
                binding.architect_process_birth = Some(architect_process_birth.clone());
                binding.bridge_pid = Some(*bridge_pid);
                binding.bridge_process_birth = Some(bridge_process_birth.clone());
                binding.relay_executable_contract_hash =
                    Some(relay_executable_contract_hash.clone());
                binding.relay_runtime_scope_hash = Some(relay_runtime_scope_hash.clone());
                binding.state = BindingState::ProcessBound;
                binding.version = binding
                    .version
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("binding version overflow"))?;
                Ok(binding.version)
            }
            (
                RegistrationCaller::Bridge {
                    binding_id,
                    launch_nonce,
                    capability,
                },
                RegistrationAction::ObserveNativeSession {
                    binding_id: action_binding,
                    expected_version,
                    native_session_id,
                },
            ) if binding_id == action_binding => {
                self.authorize_bridge(peer, binding_id, launch_nonce, capability, true)?;
                validate_text(native_session_id, 256)?;
                let binding = self.binding_mut(binding_id, *expected_version)?;
                if binding.state != BindingState::ProcessBound
                    || binding.native_session_id.is_some()
                {
                    bail!("architect native session can bind exactly once");
                }
                binding.native_session_id = Some(native_session_id.clone());
                binding.state = BindingState::NativeBound;
                binding.version = binding
                    .version
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("binding version overflow"))?;
                Ok(binding.version)
            }
            (
                RegistrationCaller::Bridge {
                    binding_id,
                    launch_nonce,
                    capability,
                },
                RegistrationAction::CloseBinding {
                    binding_id: action_binding,
                    expected_version,
                },
            ) if binding_id == action_binding => {
                self.authorize_bridge(peer, binding_id, launch_nonce, capability, false)?;
                self.close_binding(binding_id, *expected_version)
            }
            (
                RegistrationCaller::Human { process_birth },
                RegistrationAction::CloseBinding {
                    binding_id,
                    expected_version,
                },
            ) => {
                self.authorize_parent(peer, process_birth, false)?;
                self.close_binding(binding_id, *expected_version)
            }
            _ => bail!("registration caller is not authorized for this operation"),
        }
    }

    fn serve_stream(&mut self, stream: &mut UnixStream) -> Result<()> {
        let peer = peer_credentials(stream)?;
        if peer.uid != self.expected_uid {
            bail!("control peer uid mismatch");
        }
        let frame = read_request_frame(stream)?;
        let request: ControlRequest = match serde_json::from_slice(&frame) {
            Ok(request) => request,
            Err(_) => {
                return write_response(
                    stream,
                    &ControlResponse::error(
                        "",
                        ControlErrorCode::InvalidRequest,
                        "malformed control request",
                    ),
                );
            }
        };
        let response = match request.validate() {
            Ok(()) => self.handle_request(peer, &request),
            Err(_) => ControlResponse::error(
                &request.request_id,
                ControlErrorCode::InvalidRequest,
                "invalid control request",
            ),
        };
        write_response(stream, &response)
    }

    fn handle_request(
        &mut self,
        peer: PeerCredentials,
        request: &ControlRequest,
    ) -> ControlResponse {
        let caller_key = match self.authorize_control(peer, request) {
            Ok(key) => key,
            Err(_) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Unauthorized,
                    "control caller is not authorized",
                );
            }
        };
        let action_bytes = match serde_json::to_vec(&request.action) {
            Ok(bytes) => bytes,
            Err(_) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Internal,
                    "control request could not be encoded",
                );
            }
        };
        let payload_hash = sha256_hex(&action_bytes);
        let key = (caller_key, request.request_id.clone());
        if let Some(record) = self.requests.get(&key) {
            return if record.payload_hash != payload_hash {
                ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    "request_id is already bound to another payload",
                )
            } else if let Some(response) = &record.response {
                response.clone()
            } else {
                ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::RequestInProgress,
                    "control request is already in progress",
                )
            };
        }
        if matches!(&request.action, ControlAction::SessionStatus) {
            return match self.dispatch_action(&request.action) {
                Ok(result) => ControlResponse::success(&request.request_id, result),
                Err(_) => ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Internal,
                    "session status could not be read",
                ),
            };
        }
        if self.requests.len() >= MAX_REQUEST_RECORDS {
            return ControlResponse::error(
                &request.request_id,
                ControlErrorCode::Conflict,
                "session request replay capacity is exhausted",
            );
        }
        self.requests.insert(
            key.clone(),
            RequestRecord {
                payload_hash,
                response: None,
            },
        );
        let response = match self.dispatch_action(&request.action) {
            Ok(result) => ControlResponse::success(&request.request_id, result),
            Err(_) => ControlResponse::error(
                &request.request_id,
                if self.supervisor.snapshot().state == super::SessionState::NeedsHuman {
                    ControlErrorCode::NeedsHuman
                } else {
                    ControlErrorCode::Conflict
                },
                "session action failed its exact state, approval, or checkout gate",
            ),
        };
        if let Some(record) = self.requests.get_mut(&key) {
            record.response = Some(response.clone());
        }
        response
    }

    fn dispatch_action(&mut self, action: &ControlAction) -> Result<ControlResult> {
        match action {
            ControlAction::SessionPlanReplace {
                expected_session_version,
                developer_adapter,
                reviewer_adapter,
                tasks,
            } => {
                let (plan_version, plan_hash) = self.supervisor.replace_plan(
                    *expected_session_version,
                    developer_adapter,
                    reviewer_adapter,
                    tasks.clone(),
                )?;
                Ok(ControlResult::Plan {
                    session: self.supervisor.snapshot(),
                    plan_version,
                    plan_hash,
                })
            }
            ControlAction::SessionApproveAndStart {
                expected_session_version,
                plan_version,
                plan_hash,
                approval_confirmed,
            } => {
                self.supervisor.approve_and_start(
                    *expected_session_version,
                    *plan_version,
                    plan_hash,
                    *approval_confirmed,
                )?;
                Ok(ControlResult::Session {
                    session: self.supervisor.snapshot(),
                })
            }
            ControlAction::SessionStatus => Ok(ControlResult::Session {
                session: self.supervisor.snapshot(),
            }),
            ControlAction::SessionCancel {
                expected_session_version,
                reason,
            } => {
                self.supervisor.cancel(*expected_session_version, reason)?;
                Ok(ControlResult::Session {
                    session: self.supervisor.snapshot(),
                })
            }
        }
    }

    fn authorize_control(&self, peer: PeerCredentials, request: &ControlRequest) -> Result<String> {
        match &request.caller {
            CallerAuth::Human { process_birth } => {
                self.authorize_parent(peer, process_birth, true)?;
                Ok(format!("human:{}:{}", peer.pid, process_birth))
            }
            CallerAuth::Architect {
                binding_id,
                launch_nonce,
                capability,
                native_session_id,
            } => {
                self.authorize_bridge(peer, binding_id, launch_nonce, capability, true)?;
                let binding = self
                    .bindings
                    .get(binding_id)
                    .ok_or_else(|| anyhow::anyhow!("architect binding disappeared"))?;
                if binding.state != BindingState::NativeBound
                    || binding.native_session_id.as_ref() != native_session_id.as_ref()
                {
                    bail!("architect native session binding mismatch");
                }
                let actions = parse_canonical_action_set(&binding.action_set_json)
                    .map_err(|error| anyhow::anyhow!(error))?;
                if sha256_hex(binding.action_set_json.as_bytes()) != binding.action_set_hash
                    || !actions.contains(&request.action.name())
                    || binding.project_root != self.supervisor.startup().project_root
                {
                    bail!("architect action is outside its bound capability");
                }
                Ok(format!("architect:{}", binding.id))
            }
        }
    }

    fn authorize_parent(
        &self,
        peer: PeerCredentials,
        process_birth: &str,
        require_foreground_tty: bool,
    ) -> Result<()> {
        if peer.pid != self.parent_pid
            || process_birth != self.parent_birth
            || process_birth_identity(peer.pid)? != self.parent_birth
        {
            bail!("parent process identity mismatch");
        }
        if require_foreground_tty && !process_owns_foreground_tty(peer.pid, process_birth)? {
            bail!("human-authorized mutation requires the foreground parent");
        }
        Ok(())
    }

    fn authorize_bridge(
        &self,
        peer: PeerCredentials,
        binding_id: &str,
        launch_nonce: &str,
        capability: &str,
        require_architect_live: bool,
    ) -> Result<()> {
        let binding = self
            .bindings
            .get(binding_id)
            .ok_or_else(|| anyhow::anyhow!("architect binding is unavailable"))?;
        if binding.state == BindingState::Closed
            || binding.bridge_pid != Some(peer.pid)
            || binding.bridge_process_birth.as_deref() != Some(&process_birth_identity(peer.pid)?)
        {
            bail!("bridge process binding mismatch");
        }
        if require_architect_live {
            let architect_pid = binding
                .architect_pid
                .ok_or_else(|| anyhow::anyhow!("architect PID is unavailable"))?;
            let architect_birth = binding
                .architect_process_birth
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("architect birth is unavailable"))?;
            if !process_is_live_identity(architect_pid, architect_birth)? {
                bail!("architect process is no longer live");
            }
        }
        if !constant_time_equal(
            &binding.launch_nonce_hash,
            &secret_hash(b"hcom-session/launch-nonce/v1", launch_nonce),
        ) || !constant_time_equal(
            &binding.capability_hash,
            &secret_hash(b"hcom-session/capability/v1", capability),
        ) {
            bail!("architect binding secret mismatch");
        }
        Ok(())
    }

    fn binding_mut(&mut self, id: &str, expected_version: u64) -> Result<&mut ArchitectBinding> {
        let binding = self
            .bindings
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("architect binding is unavailable"))?;
        if binding.version != expected_version || binding.state == BindingState::Closed {
            bail!("architect binding version is stale");
        }
        Ok(binding)
    }

    fn close_binding(&mut self, id: &str, expected_version: u64) -> Result<u64> {
        let binding = self.binding_mut(id, expected_version)?;
        binding.state = BindingState::Closed;
        binding.version = binding
            .version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("binding version overflow"))?;
        Ok(binding.version)
    }
}

fn registration_refusal_code(action: &RegistrationAction, error: &anyhow::Error) -> &'static str {
    if !matches!(action, RegistrationAction::ObserveNativeSession { .. }) {
        return REGISTRATION_REFUSAL_GENERIC;
    }
    let has = |message: &str| error.chain().any(|cause| cause.to_string() == message);
    if has("architect binding is unavailable") {
        NATIVE_SESSION_REFUSAL_UNAVAILABLE
    } else if has("bridge process binding mismatch") {
        NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS
    } else if has("architect PID is unavailable")
        || has("architect birth is unavailable")
        || has("architect process is no longer live")
    {
        NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS
    } else if has("architect binding secret mismatch") {
        NATIVE_SESSION_REFUSAL_CAPABILITY
    } else if has("invalid bounded registration text") {
        NATIVE_SESSION_REFUSAL_IDENTITY
    } else if has("architect binding version is stale") {
        NATIVE_SESSION_REFUSAL_VERSION
    } else if has("architect native session can bind exactly once") {
        NATIVE_SESSION_REFUSAL_STATE
    } else {
        REGISTRATION_REFUSAL_GENERIC
    }
}

struct SocketGuard {
    path: PathBuf,
    listener: UnixListener,
}

impl SocketGuard {
    fn bind(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            bail!("session control socket path must be absolute");
        }
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("session control socket has no parent"))?;
        let _ = canonical_private_directory(parent, "session control socket parent")?;
        match fs::symlink_metadata(path) {
            Ok(_) => bail!("session control socket path already exists"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(path)?;
        // SAFETY: geteuid has no preconditions.
        if !metadata.file_type().is_socket()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            let _ = fs::remove_file(path);
            bail!("session control socket has an unsafe identity");
        }
        Ok(Self {
            path: path.to_owned(),
            listener,
        })
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn write_response(stream: &mut UnixStream, response: &ControlResponse) -> Result<()> {
    let payload = serde_json::to_vec(response)?;
    write_response_frame(stream, &payload)?;
    Ok(())
}

fn write_registration_response(
    stream: &mut UnixStream,
    response: &RegistrationResponse,
) -> Result<()> {
    let payload = serde_json::to_vec(response)?;
    write_response_frame(stream, &payload)?;
    Ok(())
}

fn canonical_private_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("{label} must be canonical, private, and current-user owned");
    }
    Ok(canonical)
}

fn validate_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("invalid bounded identifier");
    }
    Ok(())
}

fn validate_text(value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("invalid bounded text");
    }
    Ok(())
}

fn validate_secret(value: &str) -> Result<()> {
    if !(16..=512).contains(&value.len())
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("invalid secret shape");
    }
    Ok(())
}

fn validate_hash(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("invalid hash");
    }
    Ok(())
}

fn secret_hash(domain: &[u8], secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(secret.as_bytes());
    hex_bytes(&hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::protocol::PROTOCOL_VERSION;
    use std::collections::BTreeSet;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    struct Fixture {
        _temp: tempfile::TempDir,
        paths: ControlPaths,
        repository: PathBuf,
        sources: SessionRuntimeSources,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temp.path()).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let run = root.join("run");
            let locks = root.join("locks");
            let repository = root.join("repo");
            let toolchain = root.join("toolchain");
            for path in [&run, &locks, &repository, &toolchain] {
                fs::create_dir(path).unwrap();
                fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
            }
            git(&repository, &["init", "-b", "master"]);
            fs::write(repository.join("seed.txt"), "seed\n").unwrap();
            git(&repository, &["add", "--", "seed.txt"]);
            git(
                &repository,
                &[
                    "-c",
                    "user.name=Session Control Fixture",
                    "-c",
                    "user.email=session-control@example.invalid",
                    "commit",
                    "-m",
                    "Initial fixture",
                ],
            );
            Self {
                _temp: temp,
                paths: ControlPaths::new(&run, &locks).unwrap(),
                repository: fs::canonicalize(repository).unwrap(),
                sources: SessionRuntimeSources::fake(&toolchain),
            }
        }
    }

    fn git(repository: &Path, args: &[&str]) {
        let output = Command::new("/usr/bin/git")
            .args(args)
            .current_dir(repository)
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
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn endpoint_is_runtime_only_and_removes_its_private_sockets_on_drop() {
        let fixture = Fixture::new();
        let control_socket = fixture.paths.socket_path();
        let registration_socket = fixture.paths.registration_socket_path();
        {
            let endpoint = SessionSupervisorEndpoint::bind(
                fixture.paths.clone(),
                "run-endpoint".into(),
                fixture.repository.clone(),
                fixture.sources.clone(),
            )
            .unwrap();
            assert_eq!(endpoint.startup().run_id, "run-endpoint");
            for socket in [&control_socket, &registration_socket] {
                let metadata = fs::symlink_metadata(socket).unwrap();
                assert!(metadata.file_type().is_socket());
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            }
            let names: BTreeSet<_> = fs::read_dir(fixture.paths.run_root())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            assert!(!names.iter().any(|name| {
                name.contains("store")
                    || name.contains("sqlite")
                    || name.contains("project")
                    || name.contains("recovery")
            }));
        }
        assert!(!control_socket.exists());
        assert!(!registration_socket.exists());
    }

    #[test]
    fn native_session_registration_refusal_codes_are_closed_and_non_secret() {
        let action = RegistrationAction::ObserveNativeSession {
            binding_id: "binding-refusal-code".into(),
            expected_version: 1,
            native_session_id: "native-refusal-code".into(),
        };
        for (message, expected) in [
            (
                "architect binding is unavailable",
                NATIVE_SESSION_REFUSAL_UNAVAILABLE,
            ),
            (
                "bridge process binding mismatch",
                NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS,
            ),
            (
                "architect process is no longer live",
                NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS,
            ),
            (
                "architect binding secret mismatch",
                NATIVE_SESSION_REFUSAL_CAPABILITY,
            ),
            (
                "invalid bounded registration text",
                NATIVE_SESSION_REFUSAL_IDENTITY,
            ),
            (
                "architect binding version is stale",
                NATIVE_SESSION_REFUSAL_VERSION,
            ),
            (
                "architect native session can bind exactly once",
                NATIVE_SESSION_REFUSAL_STATE,
            ),
        ] {
            assert_eq!(
                registration_refusal_code(&action, &anyhow::anyhow!(message)),
                expected
            );
        }
        assert_eq!(
            registration_refusal_code(
                &action,
                &anyhow::anyhow!("unclassified must-not-echo-value")
            ),
            REGISTRATION_REFUSAL_GENERIC
        );
        let unrelated = RegistrationAction::CloseBinding {
            binding_id: "binding-refusal-code".into(),
            expected_version: 1,
        };
        assert_eq!(
            registration_refusal_code(
                &unrelated,
                &anyhow::anyhow!("architect binding secret mismatch")
            ),
            REGISTRATION_REFUSAL_GENERIC
        );
    }

    #[test]
    fn architect_binding_is_exact_and_request_replay_is_payload_bound() {
        let fixture = Fixture::new();
        let mut control = SessionSupervisorControl::open(
            &fixture.paths,
            "run-binding".into(),
            fixture.repository,
            fixture.sources,
        )
        .unwrap();
        let binding_id = "binding-session-test";
        let launch_nonce = "launch-nonce-session-test";
        let capability = "capability-session-test";
        let native_session_id = "native-session-test";
        let (action_set_json, _) = canonical_action_set(ActionName::ARCHITECT).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        control.bindings.insert(
            binding_id.into(),
            ArchitectBinding {
                id: binding_id.into(),
                project_root: control.supervisor.startup().project_root.clone(),
                launch_nonce_hash: secret_hash(b"hcom-session/launch-nonce/v1", launch_nonce),
                capability_hash: secret_hash(b"hcom-session/capability/v1", capability),
                action_set_hash: sha256_hex(action_set_json.as_bytes()),
                action_set_json,
                state: BindingState::NativeBound,
                version: 2,
                architect_pid: Some(std::process::id()),
                architect_process_birth: Some(birth.clone()),
                bridge_pid: Some(std::process::id()),
                bridge_process_birth: Some(birth),
                relay_executable_contract_hash: Some("a".repeat(64)),
                relay_runtime_scope_hash: Some("b".repeat(64)),
                native_session_id: Some(native_session_id.into()),
            },
        );
        // SAFETY: geteuid/getegid have no preconditions.
        let peer = PeerCredentials {
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        };
        let request = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "request-replay".into(),
            caller: CallerAuth::Architect {
                binding_id: binding_id.into(),
                launch_nonce: launch_nonce.into(),
                capability: capability.into(),
                native_session_id: Some(native_session_id.into()),
            },
            action: ControlAction::SessionCancel {
                expected_session_version: 0,
                reason: "bounded replay test".into(),
            },
        };
        let first = control.handle_request(peer, &request);
        assert!(first.ok);
        assert_eq!(control.handle_request(peer, &request), first);

        let mut conflicting = request.clone();
        conflicting.action = ControlAction::SessionStatus;
        assert_eq!(
            control
                .handle_request(peer, &conflicting)
                .error
                .unwrap()
                .code,
            ControlErrorCode::Conflict
        );

        let mut wrong_native = request;
        let CallerAuth::Architect {
            native_session_id, ..
        } = &mut wrong_native.caller
        else {
            unreachable!()
        };
        *native_session_id = Some("native-session-other".into());
        assert_eq!(
            control
                .handle_request(peer, &wrong_native)
                .error
                .unwrap()
                .code,
            ControlErrorCode::Unauthorized
        );
    }

    #[test]
    fn request_replay_memory_has_a_hard_session_bound() {
        let fixture = Fixture::new();
        let mut control = SessionSupervisorControl::open(
            &fixture.paths,
            "run-request-bound".into(),
            fixture.repository,
            fixture.sources,
        )
        .unwrap();
        let binding_id = "binding-request-bound";
        let launch_nonce = "launch-nonce-request-bound";
        let capability = "capability-request-bound";
        let native_session_id = "native-session-request-bound";
        let (action_set_json, _) = canonical_action_set(ActionName::ARCHITECT).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        control.bindings.insert(
            binding_id.into(),
            ArchitectBinding {
                id: binding_id.into(),
                project_root: control.supervisor.startup().project_root.clone(),
                launch_nonce_hash: secret_hash(b"hcom-session/launch-nonce/v1", launch_nonce),
                capability_hash: secret_hash(b"hcom-session/capability/v1", capability),
                action_set_hash: sha256_hex(action_set_json.as_bytes()),
                action_set_json,
                state: BindingState::NativeBound,
                version: 2,
                architect_pid: Some(std::process::id()),
                architect_process_birth: Some(birth.clone()),
                bridge_pid: Some(std::process::id()),
                bridge_process_birth: Some(birth),
                relay_executable_contract_hash: Some("a".repeat(64)),
                relay_runtime_scope_hash: Some("b".repeat(64)),
                native_session_id: Some(native_session_id.into()),
            },
        );
        for index in 0..MAX_REQUEST_RECORDS {
            control.requests.insert(
                (
                    format!("bounded-caller-{index}"),
                    format!("bounded-{index}"),
                ),
                RequestRecord {
                    payload_hash: "a".repeat(64),
                    response: None,
                },
            );
        }
        // SAFETY: geteuid/getegid have no preconditions.
        let peer = PeerCredentials {
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        };
        let overflow = control.handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "bounded-overflow".into(),
                caller: CallerAuth::Architect {
                    binding_id: binding_id.into(),
                    launch_nonce: launch_nonce.into(),
                    capability: capability.into(),
                    native_session_id: Some(native_session_id.into()),
                },
                action: ControlAction::SessionCancel {
                    expected_session_version: 0,
                    reason: "must not execute after replay capacity is exhausted".into(),
                },
            },
        );
        assert_eq!(overflow.error.unwrap().code, ControlErrorCode::Conflict);
        assert_eq!(control.requests.len(), MAX_REQUEST_RECORDS);
        assert_eq!(
            control.supervisor.snapshot().state,
            crate::control_api::SessionState::AwaitingPlan
        );
        let status = control.handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "bounded-read-only-status".into(),
                caller: CallerAuth::Architect {
                    binding_id: binding_id.into(),
                    launch_nonce: launch_nonce.into(),
                    capability: capability.into(),
                    native_session_id: Some(native_session_id.into()),
                },
                action: ControlAction::SessionStatus,
            },
        );
        assert!(status.ok);
        assert_eq!(control.requests.len(), MAX_REQUEST_RECORDS);
    }
}
