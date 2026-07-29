use super::codec::{read_request_frame, write_response_frame};
use super::peer::{
    PeerCredentials, peer_credentials, process_birth_identity, process_has_ancestor,
    process_owns_foreground_tty,
};
use super::protocol::{
    ActionName, CallerAuth, ControlErrorCode, ControlRequest, ControlResponse,
    canonical_action_set, parse_canonical_action_set,
};
use super::registration::{
    RegistrationAction, RegistrationCaller, RegistrationRequest, RegistrationResponse,
    validate_request_envelope,
};
use crate::orchestrator::DurableScheduler;
use crate::project_store::{
    ArchitectProcessBinding, PendingArchitectBinding, ProjectControlLayout, RequestReplay,
    sha256_hex,
};
use crate::worker::contract::WorkerAdapterRegistry;
use crate::worker::process::ProcessRunner;
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPaths {
    layout: ProjectControlLayout,
}

impl ControlPaths {
    pub fn discover() -> Result<Self> {
        Ok(Self {
            layout: ProjectControlLayout::discover()?,
        })
    }

    pub fn new(
        state_root: impl AsRef<Path>,
        runtime_root: impl AsRef<Path>,
        config_file: impl AsRef<Path>,
    ) -> Self {
        Self {
            layout: ProjectControlLayout::from_app_roots(state_root, runtime_root, config_file),
        }
    }

    pub fn socket_path(&self) -> PathBuf {
        self.layout.control_socket_path()
    }

    pub fn registration_socket_path(&self) -> PathBuf {
        self.layout.registration_socket_path()
    }

    pub fn architect_state_root_path(&self) -> PathBuf {
        self.layout.architect_state_root_path()
    }

    pub fn architect_runtime_root_path(&self) -> PathBuf {
        self.layout.architect_runtime_root_path()
    }
}

pub struct DaemonEndpoint {
    control: DaemonControl,
    listener: UnixListener,
    socket_guard: SocketGuard,
    registration_listener: UnixListener,
    registration_socket_guard: SocketGuard,
}

impl DaemonEndpoint {
    pub fn bind(paths: ControlPaths) -> Result<Self> {
        let control = DaemonControl::open(&paths)?;
        let socket_guard = SocketGuard::bind(&paths.socket_path())?;
        let listener = socket_guard.listener.try_clone()?;
        let registration_socket_guard = SocketGuard::bind(&paths.registration_socket_path())?;
        let registration_listener = registration_socket_guard.listener.try_clone()?;
        Ok(Self {
            control,
            listener,
            socket_guard,
            registration_listener,
            registration_socket_guard,
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_guard.path
    }

    pub fn registration_socket_path(&self) -> &Path {
        &self.registration_socket_guard.path
    }

    pub fn control_mut(&mut self) -> &mut DaemonControl {
        &mut self.control
    }

    pub fn serve_one(&mut self) -> Result<()> {
        let (mut stream, _) = self
            .listener
            .accept()
            .context("failed to accept control connection")?;
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        self.control.serve_stream(&mut stream)
    }

    pub fn serve_registration_one(&mut self) -> Result<()> {
        let (mut stream, _) = self
            .registration_listener
            .accept()
            .context("failed to accept architect registration connection")?;
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        self.control.serve_registration_stream(&mut stream)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        self.listener.set_nonblocking(nonblocking)?;
        self.registration_listener.set_nonblocking(nonblocking)?;
        Ok(())
    }

    pub fn try_serve_one(&mut self) -> Result<bool> {
        let (mut stream, _) = match self.listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error).context("failed to accept control connection"),
        };
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        if let Err(error) = self.control.serve_stream(&mut stream) {
            // A malformed, disconnected, or slow same-UID client is scoped to
            // this accepted connection. It must not tear down the durable
            // daemon, its scheduler, or an unrelated worker.
            eprintln!("hcomd: rejected control connection: {error:#}");
        }
        Ok(true)
    }

    pub fn try_serve_registration_one(&mut self) -> Result<bool> {
        let (mut stream, _) = match self.registration_listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => {
                return Err(error).context("failed to accept architect registration connection");
            }
        };
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        if let Err(error) = self.control.serve_registration_stream(&mut stream) {
            eprintln!("hcomd: rejected architect registration connection: {error:#}");
        }
        Ok(true)
    }
}

pub struct DaemonControl {
    scheduler: DurableScheduler,
    expected_uid: u32,
    daemon_epoch: String,
}

impl DaemonControl {
    fn open(paths: &ControlPaths) -> Result<Self> {
        // SAFETY: geteuid has no preconditions.
        let expected_uid = unsafe { libc::geteuid() };
        let scheduler = DurableScheduler::open(
            &paths.layout,
            paths.layout.artifact_root_path(),
            WorkerAdapterRegistry::default(),
            ProcessRunner::default(),
        )?;
        let daemon_epoch = scheduler.daemon_epoch().to_owned();
        Ok(Self {
            scheduler,
            expected_uid,
            daemon_epoch,
        })
    }

    pub fn register_architect_binding(
        &mut self,
        registration: &ArchitectBindingRegistration,
    ) -> Result<u64> {
        validate_binding_registration(registration)?;
        let (action_set_json, _) = canonical_action_set(registration.actions.iter().copied())?;
        let action_set_hash = sha256_hex(action_set_json.as_bytes());
        let launch_nonce_hash = secret_hash(
            b"hcom-project-control/launch-nonce/v1",
            &registration.launch_nonce,
        );
        let capability_hash = secret_hash(
            b"hcom-project-control/capability/v1",
            &registration.capability,
        );
        self.scheduler
            .store_mut()
            .insert_pending_architect_binding(&PendingArchitectBinding {
                id: &registration.binding_id,
                repo_root: &registration.repo_root,
                architect_name: &registration.architect_name,
                architect_adapter: &registration.architect_adapter,
                launch_nonce_hash: &launch_nonce_hash,
                control_capability_hash: &capability_hash,
                action_set_json: &action_set_json,
                action_set_hash: &action_set_hash,
            })?;
        Ok(0)
    }

    pub fn bind_architect_process(
        &mut self,
        binding_id: &str,
        expected_version: u64,
        registration: &ArchitectProcessRegistration,
    ) -> Result<u64> {
        validate_opaque_id(binding_id)?;
        validate_process_registration(registration)?;
        self.scheduler.store_mut().bind_architect_process(
            binding_id,
            i64::try_from(expected_version).context("binding version is too large")?,
            &ArchitectProcessBinding {
                architect_pid: registration.architect_pid,
                architect_process_birth: &registration.architect_process_birth,
                bridge_pid: registration.bridge_pid,
                bridge_process_birth: &registration.bridge_process_birth,
                relay_executable_contract_hash: &registration.relay_executable_contract_hash,
                relay_runtime_scope_hash: &registration.relay_runtime_scope_hash,
            },
        )?;
        expected_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))
    }

    pub fn bind_architect_native_session(
        &mut self,
        binding_id: &str,
        expected_version: u64,
        native_session_id: &str,
    ) -> Result<u64> {
        validate_opaque_id(binding_id)?;
        validate_single_line(native_session_id, 256)?;
        self.scheduler.store_mut().bind_architect_native_session(
            binding_id,
            i64::try_from(expected_version).context("binding version is too large")?,
            native_session_id,
        )?;
        expected_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))
    }

    pub fn bind_architect_project(
        &mut self,
        binding_id: &str,
        expected_version: u64,
        project_id: &str,
    ) -> Result<u64> {
        validate_opaque_id(binding_id)?;
        validate_opaque_id(project_id)?;
        self.scheduler.store_mut().bind_architect_project(
            binding_id,
            i64::try_from(expected_version).context("binding version is too large")?,
            project_id,
        )?;
        expected_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))
    }

    pub fn close_architect_binding(
        &mut self,
        binding_id: &str,
        expected_version: u64,
    ) -> Result<u64> {
        validate_opaque_id(binding_id)?;
        self.scheduler.store_mut().close_architect_binding(
            binding_id,
            i64::try_from(expected_version).context("binding version is too large")?,
        )?;
        expected_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))
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
            Err(_) => RegistrationResponse::error(
                &request.request_id,
                "architect registration request was refused",
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
                    repo_root,
                    architect_name,
                    architect_adapter,
                    launch_nonce,
                    capability,
                    actions,
                },
            ) => {
                self.authorize_human_peer(peer, process_birth, true)?;
                self.register_architect_binding(&ArchitectBindingRegistration {
                    binding_id: binding_id.clone(),
                    repo_root: PathBuf::from(repo_root),
                    architect_name: architect_name.clone(),
                    architect_adapter: architect_adapter.clone(),
                    launch_nonce: launch_nonce.clone(),
                    capability: capability.clone(),
                    actions: actions.clone(),
                })
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
                self.authorize_human_peer(peer, process_birth, true)?;
                self.bind_architect_process(
                    binding_id,
                    *expected_version,
                    &ArchitectProcessRegistration {
                        architect_pid: *architect_pid,
                        architect_process_birth: architect_process_birth.clone(),
                        bridge_pid: *bridge_pid,
                        bridge_process_birth: bridge_process_birth.clone(),
                        relay_executable_contract_hash: relay_executable_contract_hash.clone(),
                        relay_runtime_scope_hash: relay_runtime_scope_hash.clone(),
                    },
                )
            }
            (
                RegistrationCaller::Human { process_birth },
                RegistrationAction::BindProject {
                    binding_id,
                    expected_version,
                    project_id,
                },
            ) => {
                self.authorize_human_peer(peer, process_birth, true)?;
                self.bind_architect_project(binding_id, *expected_version, project_id)
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
                self.authorize_bridge_registration(
                    peer,
                    binding_id,
                    launch_nonce,
                    capability,
                    true,
                )?;
                self.bind_architect_native_session(binding_id, *expected_version, native_session_id)
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
                self.authorize_bridge_registration(
                    peer,
                    binding_id,
                    launch_nonce,
                    capability,
                    false,
                )?;
                self.close_architect_binding(binding_id, *expected_version)
            }
            (
                RegistrationCaller::Human { process_birth },
                RegistrationAction::CloseBinding {
                    binding_id,
                    expected_version,
                },
            ) => {
                self.authorize_human_peer(peer, process_birth, true)?;
                self.close_architect_binding(binding_id, *expected_version)
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
        let caller_key = match self.authorize(peer, request) {
            Ok(caller_key) => caller_key,
            Err(_) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Unauthorized,
                    "control caller is not authorized",
                );
            }
        };
        let action_name = request.action.name();
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
        match self.scheduler.store_mut().begin_control_request(
            &self.daemon_epoch,
            &caller_key,
            &request.request_id,
            action_name.as_str(),
            &payload_hash,
        ) {
            Ok(RequestReplay::Completed(response)) => {
                return serde_json::from_str(&response).unwrap_or_else(|_| {
                    ControlResponse::error(
                        &request.request_id,
                        ControlErrorCode::Internal,
                        "stored control response is invalid",
                    )
                });
            }
            Ok(RequestReplay::InProgress) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::RequestInProgress,
                    "control request outcome is indeterminate",
                );
            }
            Ok(RequestReplay::Conflict) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    "request_id is already bound to another payload",
                );
            }
            Ok(RequestReplay::New) => {}
            Err(_) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Internal,
                    "control request could not be recorded",
                );
            }
        }

        // Phase 7 enables the bound architect transport while deliberately
        // leaving every project business transition to Phase 8.
        let response = ControlResponse::error(
            &request.request_id,
            ControlErrorCode::NotImplemented,
            "typed action is not implemented before Phase 8",
        );
        let response_json = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(_) => {
                return ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Internal,
                    "control response could not be encoded",
                );
            }
        };
        let response_hash = sha256_hex(response_json.as_bytes());
        if self
            .scheduler
            .store_mut()
            .complete_control_request(
                &self.daemon_epoch,
                &caller_key,
                &request.request_id,
                &payload_hash,
                &response_json,
                &response_hash,
            )
            .is_err()
        {
            return ControlResponse::error(
                &request.request_id,
                ControlErrorCode::Internal,
                "control response could not be committed",
            );
        }
        response
    }

    fn authorize(&self, peer: PeerCredentials, request: &ControlRequest) -> Result<String> {
        let live_birth = process_birth_identity(peer.pid)?;
        match &request.caller {
            CallerAuth::Human { process_birth } => {
                self.authorize_human_peer(
                    peer,
                    process_birth,
                    request.action.is_human_only_mutation(),
                )?;
                Ok(domain_hash(
                    b"hcom-project-control/human-caller/v1",
                    &[
                        &peer.uid.to_be_bytes(),
                        &peer.pid.to_be_bytes(),
                        process_birth.as_bytes(),
                    ],
                ))
            }
            CallerAuth::Architect {
                binding_id,
                launch_nonce,
                capability,
                native_session_id,
            } => {
                let binding = self
                    .scheduler
                    .store()
                    .architect_authorization(binding_id)?
                    .ok_or_else(|| anyhow::anyhow!("architect binding is unavailable"))?;
                if peer.pid != binding.bridge_pid
                    || !constant_time_equal(
                        live_birth.as_bytes(),
                        binding.bridge_process_birth.as_bytes(),
                    )
                {
                    bail!("bridge process binding mismatch");
                }
                let architect_birth = process_birth_identity(binding.architect_pid)?;
                if !constant_time_equal(
                    architect_birth.as_bytes(),
                    binding.architect_process_birth.as_bytes(),
                ) {
                    bail!("architect process binding mismatch");
                }
                let nonce_hash = secret_hash(b"hcom-project-control/launch-nonce/v1", launch_nonce);
                let capability_hash =
                    secret_hash(b"hcom-project-control/capability/v1", capability);
                if !constant_time_equal(nonce_hash.as_bytes(), binding.launch_nonce_hash.as_bytes())
                    || !constant_time_equal(
                        capability_hash.as_bytes(),
                        binding.control_capability_hash.as_bytes(),
                    )
                {
                    bail!("architect secret binding mismatch");
                }
                match (&binding.architect_native_session_id, native_session_id) {
                    (None, None) => {}
                    (Some(expected), Some(actual))
                        if constant_time_equal(expected.as_bytes(), actual.as_bytes()) => {}
                    _ => bail!("architect native session binding mismatch"),
                }
                let actions = parse_canonical_action_set(&binding.action_set_json)?;
                if sha256_hex(binding.action_set_json.as_bytes()) != binding.action_set_hash
                    || !actions.contains(&request.action.name())
                {
                    bail!("architect action is outside its capability");
                }
                match (&binding.project_id, request.action.project_id()) {
                    (Some(expected), Some(actual)) if expected == actual => {}
                    (None, None)
                        if matches!(
                            &request.action,
                            super::ControlAction::ProjectCreate { repo_root, .. }
                                if repo_root == &binding.repo_root
                        ) => {}
                    _ => bail!("architect project scope mismatch"),
                }
                Ok(domain_hash(
                    b"hcom-project-control/architect-caller/v1",
                    &[binding.id.as_bytes(), capability_hash.as_bytes()],
                ))
            }
        }
    }

    fn authorize_human_peer(
        &self,
        peer: PeerCredentials,
        process_birth: &str,
        require_foreground_tty: bool,
    ) -> Result<()> {
        let live_birth = process_birth_identity(peer.pid)?;
        if !constant_time_equal(live_birth.as_bytes(), process_birth.as_bytes()) {
            bail!("human process birth mismatch");
        }
        let roots: Vec<_> = self
            .scheduler
            .store()
            .managed_process_roots()?
            .into_iter()
            .map(|root| (root.pid, root.process_birth))
            .collect();
        if process_has_ancestor(peer.pid, &roots)? {
            bail!("registered agent process tree cannot claim human authority");
        }
        if require_foreground_tty && !process_owns_foreground_tty(peer.pid, process_birth)? {
            bail!("human mutation requires a real foreground terminal");
        }
        Ok(())
    }

    fn authorize_bridge_registration(
        &self,
        peer: PeerCredentials,
        binding_id: &str,
        launch_nonce: &str,
        capability: &str,
        require_architect_live: bool,
    ) -> Result<()> {
        let binding = self
            .scheduler
            .store()
            .architect_authorization(binding_id)?
            .ok_or_else(|| anyhow::anyhow!("architect binding is unavailable"))?;
        let live_bridge_birth = process_birth_identity(peer.pid)?;
        if peer.pid != binding.bridge_pid
            || !constant_time_equal(
                live_bridge_birth.as_bytes(),
                binding.bridge_process_birth.as_bytes(),
            )
        {
            bail!("bridge process binding mismatch");
        }
        if require_architect_live {
            let architect_birth = process_birth_identity(binding.architect_pid)?;
            if !constant_time_equal(
                architect_birth.as_bytes(),
                binding.architect_process_birth.as_bytes(),
            ) {
                bail!("architect process binding mismatch");
            }
        }
        let nonce_hash = secret_hash(b"hcom-project-control/launch-nonce/v1", launch_nonce);
        let capability_hash = secret_hash(b"hcom-project-control/capability/v1", capability);
        if !constant_time_equal(nonce_hash.as_bytes(), binding.launch_nonce_hash.as_bytes())
            || !constant_time_equal(
                capability_hash.as_bytes(),
                binding.control_capability_hash.as_bytes(),
            )
        {
            bail!("architect registration secret mismatch");
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_expected_uid(&mut self, uid: u32) {
        self.expected_uid = uid;
    }

    #[cfg(test)]
    fn simulate_crash_on_drop(&mut self) {
        self.scheduler.simulate_crash_on_drop();
    }

    #[cfg(test)]
    pub(crate) fn architect_binding_state_version(
        &self,
        binding_id: &str,
    ) -> Result<(String, i64)> {
        self.scheduler
            .store()
            .connection()
            .query_row(
                "SELECT binding_state, version FROM architect_bindings WHERE id = ?1",
                [binding_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("failed to read architect binding test state")
    }

    #[cfg(test)]
    pub(crate) fn phase7_business_counts(&self) -> Result<(i64, i64)> {
        let requests = self.scheduler.store().connection().query_row(
            "SELECT count(*) FROM control_requests",
            [],
            |row| row.get(0),
        )?;
        let projects = self.scheduler.store().connection().query_row(
            "SELECT count(*) FROM project_runs",
            [],
            |row| row.get(0),
        )?;
        Ok((requests, projects))
    }
}

pub struct ArchitectBindingRegistration {
    pub binding_id: String,
    pub repo_root: PathBuf,
    pub architect_name: String,
    pub architect_adapter: String,
    pub launch_nonce: String,
    pub capability: String,
    pub actions: BTreeSet<ActionName>,
}

#[derive(Debug, Clone)]
pub struct ArchitectProcessRegistration {
    pub architect_pid: u32,
    pub architect_process_birth: String,
    pub bridge_pid: u32,
    pub bridge_process_birth: String,
    pub relay_executable_contract_hash: String,
    pub relay_runtime_scope_hash: String,
}

struct SocketGuard {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketGuard {
    fn bind(path: &Path) -> Result<Self> {
        if path.exists() {
            remove_stale_socket(path)?;
        }
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("control socket has no parent directory"))?;
        let parent_metadata = fs::symlink_metadata(parent)?;
        // SAFETY: geteuid has no preconditions.
        let expected_uid = unsafe { libc::geteuid() };
        if parent_metadata.file_type().is_symlink()
            || !parent_metadata.is_dir()
            || parent_metadata.uid() != expected_uid
            || parent_metadata.permissions().mode() & 0o777 != 0o700
        {
            bail!("control socket parent is not a private owned directory");
        }
        let listener = UnixListener::bind(path)
            .with_context(|| format!("failed to bind control socket {}", path.display()))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != expected_uid
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            bail!("control socket owner/mode validation failed");
        }
        Ok(Self {
            listener,
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_socket()
        || metadata.uid() != expected_uid
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        bail!("refusing to replace an untrusted control socket path");
    }
    match UnixStream::connect(path) {
        Ok(_) => bail!("control socket already has a live listener"),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) => {}
        Err(error) => return Err(error).context("failed to probe existing control socket"),
    }
    let current = fs::symlink_metadata(path)?;
    if !current.file_type().is_socket()
        || current.dev() != metadata.dev()
        || current.ino() != metadata.ino()
    {
        bail!("control socket changed during stale recovery");
    }
    fs::remove_file(path).context("failed to remove stale control socket")
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn write_response(stream: &mut UnixStream, response: &ControlResponse) -> Result<()> {
    let payload = serde_json::to_vec(response).context("failed to encode control response")?;
    write_response_frame(stream, &payload)?;
    Ok(())
}

fn write_registration_response(
    stream: &mut UnixStream,
    response: &RegistrationResponse,
) -> Result<()> {
    let payload = serde_json::to_vec(response).context("failed to encode registration response")?;
    write_response_frame(stream, &payload)?;
    Ok(())
}

fn validate_binding_registration(registration: &ArchitectBindingRegistration) -> Result<()> {
    validate_opaque_id(&registration.binding_id)?;
    if !registration.repo_root.is_absolute() {
        bail!("architect repository root must be absolute");
    }
    let canonical_repo = fs::canonicalize(&registration.repo_root)
        .context("failed to canonicalize architect repository root")?;
    if canonical_repo != registration.repo_root || !canonical_repo.is_dir() {
        bail!("architect repository root must be an existing canonical directory");
    }
    let repo_text = registration
        .repo_root
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("architect repository root must be valid UTF-8"))?;
    validate_single_line(repo_text, 4096)?;
    validate_single_line(&registration.architect_name, 128)?;
    validate_single_line(&registration.architect_adapter, 64)?;
    validate_secret(&registration.launch_nonce)?;
    validate_secret(&registration.capability)?;
    canonical_action_set(registration.actions.iter().copied())?;
    Ok(())
}

fn validate_process_registration(registration: &ArchitectProcessRegistration) -> Result<()> {
    if registration.architect_pid <= 1 || registration.bridge_pid <= 1 {
        bail!("architect and bridge PIDs must be greater than one");
    }
    validate_single_line(&registration.architect_process_birth, 256)?;
    validate_single_line(&registration.bridge_process_birth, 256)?;
    validate_hash(&registration.relay_executable_contract_hash)?;
    validate_hash(&registration.relay_runtime_scope_hash)
}

fn validate_opaque_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("invalid opaque identifier");
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

fn validate_single_line(value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("invalid bounded single-line value");
    }
    Ok(())
}

fn validate_hash(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("invalid sha256 digest");
    }
    Ok(())
}

fn secret_hash(domain: &[u8], secret: &str) -> String {
    domain_hash(domain, &[secret.as_bytes()])
}

fn domain_hash(domain: &[u8], fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    use std::fmt::Write as _;
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::client::ControlClient;
    use crate::control_api::codec::{read_response_frame, write_request_frame};
    use crate::control_api::protocol::{MAX_REQUEST_BYTES, PROTOCOL_VERSION};
    use std::io::Write;
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command};
    use std::thread;

    fn paths(temp: &tempfile::TempDir) -> ControlPaths {
        ControlPaths::new(
            temp.path().join("state/hcom-project-control"),
            temp.path().join("run/hcom-project-control"),
            temp.path().join("config/hcom-project-control/config.toml"),
        )
    }

    fn human_request(request_id: &str) -> ControlRequest {
        ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            caller: CallerAuth::Human {
                process_birth: process_birth_identity(std::process::id()).unwrap(),
            },
            action: super::super::ControlAction::ProjectGet {
                project_id: "project-1".into(),
            },
        }
    }

    fn serve(endpoint: DaemonEndpoint, count: usize) -> thread::JoinHandle<Result<DaemonEndpoint>> {
        thread::spawn(move || {
            let mut endpoint = endpoint;
            for _ in 0..count {
                endpoint.serve_one()?;
            }
            Ok(endpoint)
        })
    }

    struct ForegroundTtyChild {
        child: Child,
        _master: OwnedFd,
    }

    impl Drop for ForegroundTtyChild {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn foreground_tty_child() -> ForegroundTtyChild {
        let pty = nix::pty::openpty(None, None).unwrap();
        let slave_fd = pty.slave.as_raw_fd();
        let master_fd = pty.master.as_raw_fd();
        let mut command = Command::new("/usr/bin/sleep");
        command.arg("30");
        // SAFETY: these are async-signal-safe session, terminal, and
        // descriptor operations in the child before exec.
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
        let child = command.spawn().unwrap();
        drop(pty.slave);
        ForegroundTtyChild {
            child,
            _master: pty.master,
        }
    }

    #[test]
    fn real_socket_validates_peer_and_replays_exact_request() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();
        let metadata = fs::symlink_metadata(&socket_path).unwrap();
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let server = serve(endpoint, 2);
        let client = ControlClient::new(&socket_path);
        let first = client.request(&human_request("req-1")).unwrap();
        let second = client.request(&human_request("req-1")).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.error.unwrap().code, ControlErrorCode::NotImplemented);
        let endpoint = server.join().unwrap().unwrap();
        drop(endpoint);
        assert!(!socket_path.exists());
    }

    #[test]
    fn registered_agent_tree_is_denied_before_mutation_ledger() {
        let temp = tempfile::tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir(&repo_root).unwrap();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let managed = foreground_tty_child();
        let managed_pid = managed.child.id();
        let managed_birth = process_birth_identity(managed_pid).unwrap();
        assert!(process_owns_foreground_tty(managed_pid, &managed_birth).unwrap());
        let peer = PeerCredentials {
            pid: managed_pid,
            // SAFETY: geteuid/getegid have no preconditions.
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        };
        let action = super::super::ControlAction::ProjectCreate {
            repo_root: repo_root.to_string_lossy().into_owned(),
            target_ref: "refs/heads/master".into(),
        };
        let eligible = endpoint.control_mut().handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "human-eligible".into(),
                caller: CallerAuth::Human {
                    process_birth: managed_birth.clone(),
                },
                action: action.clone(),
            },
        );
        assert_eq!(
            eligible.error.unwrap().code,
            ControlErrorCode::NotImplemented
        );
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM control_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM project_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0,
            "Phase 7 must not enable project business transitions"
        );

        let bridge_birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-managed".into(),
                repo_root,
                architect_name: "architect-managed".into(),
                architect_adapter: "codex-0.145.0".into(),
                launch_nonce: "launch-nonce-managed".into(),
                capability: "capability-secret-managed".into(),
                actions: [ActionName::ProjectCreate].into_iter().collect(),
            })
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-managed",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: managed_pid,
                    architect_process_birth: managed_birth.clone(),
                    bridge_pid: std::process::id(),
                    bridge_process_birth: bridge_birth,
                    relay_executable_contract_hash: std::iter::repeat_n('a', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('b', 64).collect(),
                },
            )
            .unwrap();
        let denied = endpoint.control_mut().handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "managed-denied".into(),
                caller: CallerAuth::Human {
                    process_birth: managed_birth,
                },
                action,
            },
        );
        assert_eq!(denied.error.unwrap().code, ControlErrorCode::Unauthorized);
        let denied_read = endpoint.control_mut().handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "managed-read-denied".into(),
                caller: CallerAuth::Human {
                    process_birth: process_birth_identity(managed_pid).unwrap(),
                },
                action: super::super::ControlAction::ProjectGet {
                    project_id: "project-1".into(),
                },
            },
        );
        assert_eq!(
            denied_read.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM control_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1,
            "registered agent authority must be rejected before the request ledger"
        );
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM project_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn private_registration_socket_applies_one_shot_session_and_close_cas() {
        let temp = tempfile::tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir(&repo_root).unwrap();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-registration".into(),
                repo_root,
                architect_name: "architect-registration".into(),
                architect_adapter: "codex-0.145.0".into(),
                launch_nonce: "launch-nonce-registration".into(),
                capability: "capability-registration".into(),
                actions: [ActionName::ProjectCreate].into_iter().collect(),
            })
            .unwrap();
        assert!(
            endpoint
                .control
                .scheduler
                .store()
                .architect_authorization("binding-registration")
                .unwrap()
                .is_none(),
            "a pending crash window must not authorize architect control"
        );
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-registration",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: std::process::id(),
                    architect_process_birth: birth.clone(),
                    bridge_pid: std::process::id(),
                    bridge_process_birth: birth,
                    relay_executable_contract_hash: std::iter::repeat_n('c', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('d', 64).collect(),
                },
            )
            .unwrap();
        let socket_path = endpoint.registration_socket_path().to_path_buf();
        let metadata = fs::symlink_metadata(&socket_path).unwrap();
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let server = thread::spawn(move || {
            let mut endpoint = endpoint;
            endpoint.serve_registration_one().unwrap();
            endpoint.serve_registration_one().unwrap();
            endpoint
        });
        let client = super::super::registration::RegistrationClient::new(&socket_path);
        let caller = RegistrationCaller::Bridge {
            binding_id: "binding-registration".into(),
            launch_nonce: "launch-nonce-registration".into(),
            capability: "capability-registration".into(),
        };
        let observed = client
            .request(&RegistrationRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "registration-observe".into(),
                caller: caller.clone(),
                action: RegistrationAction::ObserveNativeSession {
                    binding_id: "binding-registration".into(),
                    expected_version: 1,
                    native_session_id: "native-session-registration".into(),
                },
            })
            .unwrap();
        assert!(observed.ok);
        assert_eq!(observed.binding_version, Some(2));
        let closed = client
            .request(&RegistrationRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "registration-close".into(),
                caller,
                action: RegistrationAction::CloseBinding {
                    binding_id: "binding-registration".into(),
                    expected_version: 2,
                },
            })
            .unwrap();
        assert!(closed.ok);
        assert_eq!(closed.binding_version, Some(3));
        let endpoint = server.join().unwrap();
        assert!(
            endpoint
                .control
                .scheduler
                .store()
                .architect_authorization("binding-registration")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn nonblocking_service_isolates_disconnected_observers() {
        let temp = tempfile::tempdir().unwrap();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        endpoint.set_nonblocking(true).unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();

        drop(UnixStream::connect(&socket_path).unwrap());
        assert!(endpoint.try_serve_one().unwrap());
        assert!(socket_path.exists());

        drop(UnixStream::connect(&socket_path).unwrap());
        assert!(endpoint.try_serve_one().unwrap());
        assert!(socket_path.exists());
    }

    #[test]
    fn architect_auth_binds_capability_nonce_process_session_and_actions() {
        let temp = tempfile::tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir(&repo_root).unwrap();
        let repo_root_text = repo_root.to_string_lossy().into_owned();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-1".into(),
                repo_root: repo_root.clone(),
                architect_name: "architect-1".into(),
                architect_adapter: "codex".into(),
                launch_nonce: "launch-nonce-0001".into(),
                capability: "capability-secret-0001".into(),
                actions: [ActionName::ProjectCreate].into_iter().collect(),
            })
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-1",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: std::process::id(),
                    architect_process_birth: birth.clone(),
                    bridge_pid: std::process::id(),
                    bridge_process_birth: birth,
                    relay_executable_contract_hash: std::iter::repeat_n('a', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('b', 64).collect(),
                },
            )
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_native_session("binding-1", 1, "native-session-1")
            .unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();
        let server = serve(endpoint, 6);
        let client = ControlClient::new(&socket_path);
        let request =
            |request_id: &str, nonce: &str, capability: &str, native_session_id, action| {
                ControlRequest {
                    protocol_version: PROTOCOL_VERSION,
                    request_id: request_id.into(),
                    caller: CallerAuth::Architect {
                        binding_id: "binding-1".into(),
                        launch_nonce: nonce.into(),
                        capability: capability.into(),
                        native_session_id,
                    },
                    action,
                }
            };
        let authorized = client
            .request(&request(
                "req-ok",
                "launch-nonce-0001",
                "capability-secret-0001",
                Some("native-session-1".into()),
                super::super::ControlAction::ProjectCreate {
                    repo_root: repo_root_text.clone(),
                    target_ref: "refs/heads/master".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            authorized.error.unwrap().code,
            ControlErrorCode::NotImplemented
        );
        let wrong_nonce = client
            .request(&request(
                "req-nonce",
                "launch-nonce-wrong",
                "capability-secret-0001",
                Some("native-session-1".into()),
                super::super::ControlAction::ProjectCreate {
                    repo_root: repo_root_text.clone(),
                    target_ref: "refs/heads/master".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            wrong_nonce.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        let wrong_capability = client
            .request(&request(
                "req-capability",
                "launch-nonce-0001",
                "capability-secret-wrong",
                Some("native-session-1".into()),
                super::super::ControlAction::ProjectCreate {
                    repo_root: repo_root_text.clone(),
                    target_ref: "refs/heads/master".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            wrong_capability.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        let missing_session = client
            .request(&request(
                "req-session",
                "launch-nonce-0001",
                "capability-secret-0001",
                None,
                super::super::ControlAction::ProjectCreate {
                    repo_root: repo_root_text.clone(),
                    target_ref: "refs/heads/master".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            missing_session.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        let wrong_repo = client
            .request(&request(
                "req-repo",
                "launch-nonce-0001",
                "capability-secret-0001",
                Some("native-session-1".into()),
                super::super::ControlAction::ProjectCreate {
                    repo_root: "/other-repo".into(),
                    target_ref: "refs/heads/master".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            wrong_repo.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        let wrong_action = client
            .request(&request(
                "req-action",
                "launch-nonce-0001",
                "capability-secret-0001",
                Some("native-session-1".into()),
                super::super::ControlAction::ProjectGet {
                    project_id: "project-1".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            wrong_action.error.unwrap().code,
            ControlErrorCode::Unauthorized
        );
        let endpoint = server.join().unwrap().unwrap();
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM control_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1,
            "unauthorized requests must be rejected before dispatch/ledger mutation"
        );
    }

    #[test]
    fn project_bound_architect_is_limited_to_one_existing_same_repo_project() {
        let temp = tempfile::tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        let worktree_root = temp.path().join("worktree");
        fs::create_dir(&repo_root).unwrap();
        fs::create_dir(&worktree_root).unwrap();
        let repo_root_text = repo_root.to_string_lossy().into_owned();
        let worktree_root_text = worktree_root.to_string_lossy().into_owned();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let sha = std::iter::repeat_n('1', 40).collect::<String>();
        endpoint
            .control
            .scheduler
            .store_mut()
            .connection_mut()
            .execute(
                "INSERT INTO project_runs (
                     id, state, version, pause_reason, source_repo_root,
                     source_git_dir_identity, target_ref, target_expected_sha,
                     worktree_root, worktree_branch, checkpoint_sha,
                     applied_target_sha, approved_plan_version, approved_plan_hash,
                     run_requested_at, active_daemon_epoch, created_at, updated_at
                 ) VALUES (
                     'project-bound', 'draft', 0, NULL, ?1, 'git-dir',
                     'refs/heads/master', ?2, ?3,
                     'refs/heads/hcom-project/project-bound', ?2,
                     NULL, NULL, NULL, NULL, NULL, 1, 1
                 )",
                rusqlite::params![repo_root_text, sha, worktree_root_text],
            )
            .unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-project-bound".into(),
                repo_root,
                architect_name: "architect-project-bound".into(),
                architect_adapter: "codex-0.145.0".into(),
                launch_nonce: "launch-nonce-project-bound".into(),
                capability: "capability-project-bound".into(),
                actions: [ActionName::ProjectGet].into_iter().collect(),
            })
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-project-bound",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: std::process::id(),
                    architect_process_birth: birth.clone(),
                    bridge_pid: std::process::id(),
                    bridge_process_birth: birth,
                    relay_executable_contract_hash: std::iter::repeat_n('a', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('b', 64).collect(),
                },
            )
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_project("binding-project-bound", 1, "project-bound")
            .unwrap();
        let peer = PeerCredentials {
            pid: std::process::id(),
            // SAFETY: geteuid/getegid have no preconditions.
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        };
        let request = |request_id: &str, project_id: &str| ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            caller: CallerAuth::Architect {
                binding_id: "binding-project-bound".into(),
                launch_nonce: "launch-nonce-project-bound".into(),
                capability: "capability-project-bound".into(),
                native_session_id: None,
            },
            action: super::super::ControlAction::ProjectGet {
                project_id: project_id.into(),
            },
        };

        let allowed = endpoint
            .control_mut()
            .handle_request(peer, &request("project-bound-ok", "project-bound"));
        assert_eq!(
            allowed.error.unwrap().code,
            ControlErrorCode::NotImplemented
        );
        let denied = endpoint
            .control_mut()
            .handle_request(peer, &request("project-bound-wrong", "project-other"));
        assert_eq!(denied.error.unwrap().code, ControlErrorCode::Unauthorized);
        assert_eq!(
            endpoint.control_mut().phase7_business_counts().unwrap(),
            (1, 1),
            "only the authorized request ledger may change in Phase 7"
        );
    }

    #[test]
    fn peer_uid_is_checked_before_any_frame_is_parsed() {
        let temp = tempfile::tempdir().unwrap();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        // SAFETY: geteuid has no preconditions.
        endpoint
            .control_mut()
            .set_expected_uid(unsafe { libc::geteuid() }.wrapping_add(1));
        let socket_path = endpoint.socket_path().to_path_buf();
        let server = serve(endpoint, 1);
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        stream
            .write_all(&((MAX_REQUEST_BYTES as u32) + 1).to_be_bytes())
            .unwrap();
        drop(stream);
        let result = server.join().unwrap();
        assert!(result.is_err());
        let error = result.err().unwrap().to_string();
        assert!(error.contains("uid mismatch"), "{error}");
    }

    #[test]
    fn architect_secrets_are_hash_only_and_wrong_process_birth_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir(&repo_root).unwrap();
        let repo_root_text = repo_root.to_string_lossy().into_owned();
        let mut endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let actual_birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-process".into(),
                repo_root,
                architect_name: "architect-process".into(),
                architect_adapter: "codex".into(),
                launch_nonce: "launch-nonce-process".into(),
                capability: "capability-secret-process".into(),
                actions: [ActionName::ProjectCreate].into_iter().collect(),
            })
            .unwrap();
        let stored: (String, String) = endpoint
            .control
            .scheduler
            .store()
            .connection()
            .query_row(
                "SELECT launch_nonce_hash, control_capability_hash
                 FROM architect_bindings WHERE id = 'binding-process'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_ne!(stored.0, "launch-nonce-process");
        assert_ne!(stored.1, "capability-secret-process");
        assert_eq!(stored.0.len(), 64);
        assert_eq!(stored.1.len(), 64);
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-process",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: std::process::id(),
                    architect_process_birth: actual_birth,
                    bridge_pid: std::process::id(),
                    bridge_process_birth: "linux-proc:wrong-boot:1".into(),
                    relay_executable_contract_hash: std::iter::repeat_n('c', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('d', 64).collect(),
                },
            )
            .unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();
        let server = serve(endpoint, 1);
        let response = ControlClient::new(socket_path)
            .request(&ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "req-process".into(),
                caller: CallerAuth::Architect {
                    binding_id: "binding-process".into(),
                    launch_nonce: "launch-nonce-process".into(),
                    capability: "capability-secret-process".into(),
                    native_session_id: None,
                },
                action: super::super::ControlAction::ProjectCreate {
                    repo_root: repo_root_text,
                    target_ref: "refs/heads/master".into(),
                },
            })
            .unwrap();
        assert_eq!(response.error.unwrap().code, ControlErrorCode::Unauthorized);
        server.join().unwrap().unwrap();
    }

    #[test]
    fn oversized_frame_is_rejected_without_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();
        let server = serve(endpoint, 1);
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        stream
            .write_all(&((MAX_REQUEST_BYTES as u32) + 1).to_be_bytes())
            .unwrap();
        drop(stream);
        let result = server.join().unwrap();
        assert!(result.is_err());
        let error = result.err().unwrap().to_string();
        assert!(error.contains("bounded size"), "{error}");
    }

    #[test]
    fn decoded_control_bytes_and_unknown_fields_are_rejected_before_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint = DaemonEndpoint::bind(paths(&temp)).unwrap();
        let socket_path = endpoint.socket_path().to_path_buf();
        let birth = process_birth_identity(std::process::id()).unwrap();
        let server = serve(endpoint, 2);
        for payload in [
            serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "request_id": "req-control",
                "caller": {"kind": "human", "process_birth": birth},
                "action": {"action": "project_get", "project_id": "bad\u{1b}id"}
            }),
            serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "request_id": "req-unknown",
                "caller": {"kind": "human", "process_birth": birth},
                "action": {"action": "project_get", "project_id": "project-1"},
                "unexpected": true
            }),
        ] {
            let mut stream = UnixStream::connect(&socket_path).unwrap();
            write_request_frame(&mut stream, &serde_json::to_vec(&payload).unwrap()).unwrap();
            let response: ControlResponse =
                serde_json::from_slice(&read_response_frame(&mut stream).unwrap()).unwrap();
            assert_eq!(
                response.error.unwrap().code,
                ControlErrorCode::InvalidRequest
            );
        }
        let endpoint = server.join().unwrap().unwrap();
        assert_eq!(
            endpoint
                .control
                .scheduler
                .store()
                .connection()
                .query_row("SELECT count(*) FROM control_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn daemon_epoch_reconciles_accepted_request_to_needs_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let caller = sha256_hex(b"caller");
        let payload = sha256_hex(b"payload");
        let mut first = DaemonControl::open(&paths).unwrap();
        let first_epoch = first.daemon_epoch.clone();
        assert_eq!(
            first
                .scheduler
                .store_mut()
                .begin_control_request(
                    &first_epoch,
                    &caller,
                    "request-interrupted",
                    "project_run",
                    &payload,
                )
                .unwrap(),
            RequestReplay::New
        );
        first.simulate_crash_on_drop();
        drop(first);

        let mut recovered = DaemonControl::open(&paths).unwrap();
        assert_ne!(recovered.daemon_epoch, first_epoch);
        let response = match recovered
            .scheduler
            .store_mut()
            .begin_control_request(
                &recovered.daemon_epoch,
                &caller,
                "request-interrupted",
                "project_run",
                &payload,
            )
            .unwrap()
        {
            RequestReplay::Completed(response) => response,
            other => panic!("unexpected reconciled request state: {other:?}"),
        };
        let response: ControlResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(
            response.error.unwrap().code,
            ControlErrorCode::NeedsRecovery
        );
    }

    #[test]
    fn private_stale_socket_is_replaced_but_live_endpoint_is_not() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let endpoint = DaemonEndpoint::bind(paths.clone()).unwrap();
        let socket = endpoint.socket_path().to_path_buf();
        assert!(DaemonEndpoint::bind(paths.clone()).is_err());
        drop(endpoint);
        let stale = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        assert!(socket.exists());
        let recovered = DaemonEndpoint::bind(paths).unwrap();
        assert_eq!(recovered.socket_path(), socket);
    }
}
