//! Private, runtime-only control endpoint for one foreground architect session.

use super::codec::{read_request_frame, write_response_frame};
use super::peer::{
    PeerCredentials, peer_credentials, process_birth_identity, process_has_ancestor,
    process_is_live_identity, process_owns_foreground_tty,
};
use super::protocol::{
    ActionName, CallerAuth, ClarificationPage, ControlAction, ControlErrorCode, ControlRequest,
    ControlResponse, ControlResult, canonical_action_set, parse_canonical_action_set,
};
use super::registration::{
    REGISTRATION_REFUSAL_GENERIC, RegistrationAction, RegistrationCaller, RegistrationRequest,
    RegistrationResponse, validate_request_envelope,
};
use crate::orchestrator::task_lane::TaskLaneSupervisor;
use crate::orchestrator::{SessionRuntimeSources, SessionStartup};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::Read;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_RECORDS: usize = 1024;

type RequestKey = (String, String);

#[derive(Debug, Clone)]
pub struct ControlPaths {
    run_root: PathBuf,
}

impl ControlPaths {
    pub fn new(run_root: impl AsRef<Path>) -> Result<Self> {
        let run_root = canonical_private_directory(run_root.as_ref(), "session runtime root")?;
        Ok(Self { run_root })
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
        let (stream, _) = match self.listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error).context("failed to accept session control connection"),
        };
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        let _ = self.control.serve_stream(stream);
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
            self.control.poll_and_service_wait()?;
            if !handled {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        let result = self.control.supervisor.shutdown();
        self.control.service_pending_wait();
        result
    }
}

struct SessionSupervisorControl {
    supervisor: Box<dyn SupervisorBackend>,
    expected_uid: u32,
    parent_pid: u32,
    parent_birth: String,
    bindings: BTreeMap<String, ArchitectBinding>,
    requests: BTreeMap<RequestKey, RequestRecord>,
    request_order: VecDeque<RequestKey>,
    pending_wait: Option<PendingSessionWait>,
}

trait SupervisorBackend: Send {
    fn startup(&self) -> &SessionStartup;

    fn begin_next_run(
        &mut self,
        expected_session_version: u64,
        terminal_run_id: &str,
    ) -> Result<()>;

    fn replace_plan(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapter: &str,
        tasks: Vec<crate::control_api::TaskDraft>,
    ) -> Result<(u64, String)>;

    fn approve_and_start(
        &mut self,
        expected_session_version: u64,
        plan_version: u64,
        plan_hash: &str,
        approval_confirmed: bool,
    ) -> Result<()>;

    #[allow(clippy::too_many_arguments)]
    fn submit_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
        clarification_document_path: &str,
        human_decision_confirmed: bool,
    ) -> Result<()>;

    fn require_human_for_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
    ) -> Result<()>;

    fn cancel(&mut self, expected_session_version: u64, reason: &str) -> Result<()>;

    fn snapshot(&self) -> crate::control_api::SessionStatusSnapshot;

    fn clarification_page(
        &self,
        run_id: &str,
        task_ordinal: u32,
        task_key: &str,
        after_sequence: u32,
        limit: u8,
    ) -> Result<ClarificationPage>;

    fn progress_event_after(
        &self,
        run_id: &str,
        after_sequence: u32,
    ) -> Result<Option<crate::control_api::SessionProgressEvent>>;

    fn poll_once(&mut self) -> Result<()>;

    fn shutdown(&mut self) -> Result<()>;
}

impl SupervisorBackend for TaskLaneSupervisor {
    fn startup(&self) -> &SessionStartup {
        self.startup()
    }

    fn begin_next_run(
        &mut self,
        expected_session_version: u64,
        terminal_run_id: &str,
    ) -> Result<()> {
        self.begin_next_run(expected_session_version, terminal_run_id)
    }

    fn replace_plan(
        &mut self,
        expected_session_version: u64,
        developer_adapter: &str,
        reviewer_adapter: &str,
        tasks: Vec<crate::control_api::TaskDraft>,
    ) -> Result<(u64, String)> {
        self.replace_plan(
            expected_session_version,
            developer_adapter,
            reviewer_adapter,
            tasks,
        )
    }

    fn approve_and_start(
        &mut self,
        expected_session_version: u64,
        plan_version: u64,
        plan_hash: &str,
        approval_confirmed: bool,
    ) -> Result<()> {
        self.approve_and_start(
            expected_session_version,
            plan_version,
            plan_hash,
            approval_confirmed,
        )
    }

    fn cancel(&mut self, expected_session_version: u64, reason: &str) -> Result<()> {
        self.cancel(expected_session_version, reason)
    }

    fn submit_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
        clarification_document_path: &str,
        human_decision_confirmed: bool,
    ) -> Result<()> {
        self.submit_clarification(
            expected_session_version,
            task_ordinal,
            task_key,
            action_sequence,
            developer_request_path,
            clarification_document_path,
            human_decision_confirmed,
        )
    }

    fn require_human_for_clarification(
        &mut self,
        expected_session_version: u64,
        task_ordinal: u32,
        task_key: &str,
        action_sequence: u32,
        developer_request_path: &str,
    ) -> Result<()> {
        self.require_human_for_clarification(
            expected_session_version,
            task_ordinal,
            task_key,
            action_sequence,
            developer_request_path,
        )
    }

    fn snapshot(&self) -> crate::control_api::SessionStatusSnapshot {
        self.snapshot()
    }

    fn clarification_page(
        &self,
        run_id: &str,
        task_ordinal: u32,
        task_key: &str,
        after_sequence: u32,
        limit: u8,
    ) -> Result<ClarificationPage> {
        self.clarification_page(run_id, task_ordinal, task_key, after_sequence, limit)
    }

    fn progress_event_after(
        &self,
        run_id: &str,
        after_sequence: u32,
    ) -> Result<Option<crate::control_api::SessionProgressEvent>> {
        self.progress_event_after(run_id, after_sequence)
    }

    fn poll_once(&mut self) -> Result<()> {
        self.poll_once()
    }

    fn shutdown(&mut self) -> Result<()> {
        self.shutdown()
    }
}

struct RequestRecord {
    payload_hash: String,
    response: Option<ControlResponse>,
}

struct PendingSessionWait {
    stream: UnixStream,
    request_id: String,
    run_id: String,
    after_progress_sequence: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingState {
    Pending,
    ProcessBound,
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
        let supervisor =
            TaskLaneSupervisor::open(run_id, project_root, paths.run_root().to_owned(), sources)?;
        Ok(Self {
            supervisor: Box::new(supervisor),
            expected_uid,
            parent_pid,
            parent_birth,
            bindings: BTreeMap::new(),
            requests: BTreeMap::new(),
            request_order: VecDeque::new(),
            pending_wait: None,
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
            Err(_) => {
                RegistrationResponse::error(&request.request_id, REGISTRATION_REFUSAL_GENERIC)
            }
        };
        write_registration_response(stream, &response)
    }

    fn poll_and_service_wait(&mut self) -> Result<()> {
        if self.supervisor.poll_once().is_err() && !self.supervisor.snapshot().state.is_terminal() {
            // TaskLaneSupervisor promises that every poll failure is
            // terminalized before it is returned. Keep a final lifecycle
            // fallback here so a future backend cannot strand a pending
            // session_wait by violating that contract.
            let containment = self.supervisor.shutdown();
            self.service_pending_wait();
            return containment.context("failed to contain a non-terminal supervisor poll failure");
        }
        self.service_pending_wait();
        Ok(())
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

    fn serve_stream(&mut self, mut stream: UnixStream) -> Result<()> {
        let peer = peer_credentials(&stream)?;
        if peer.uid != self.expected_uid {
            bail!("control peer uid mismatch");
        }
        let frame = read_request_frame(&mut stream)?;
        let request: ControlRequest = match serde_json::from_slice(&frame) {
            Ok(request) => request,
            Err(_) => {
                return write_response(
                    &mut stream,
                    &ControlResponse::error(
                        "",
                        ControlErrorCode::InvalidRequest,
                        "malformed control request",
                    ),
                );
            }
        };
        if request.validate().is_ok()
            && matches!(&request.action, ControlAction::SessionWait { .. })
        {
            return self.handle_session_wait(peer, &request, stream);
        }
        let response = match request.validate() {
            Ok(()) => self.handle_request(peer, &request),
            Err(_) => ControlResponse::error(
                &request.request_id,
                ControlErrorCode::InvalidRequest,
                "invalid control request",
            ),
        };
        write_response(&mut stream, &response)
    }

    fn handle_session_wait(
        &mut self,
        peer: PeerCredentials,
        request: &ControlRequest,
        mut stream: UnixStream,
    ) -> Result<()> {
        if self.authorize_control(peer, request).is_err() {
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Unauthorized,
                    "control caller is not authorized",
                ),
            );
        }
        self.service_pending_wait();
        if self.pending_wait.is_some() {
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::RequestInProgress,
                    "another session wait is already in progress",
                ),
            );
        }
        let ControlAction::SessionWait {
            run_id,
            after_session_version,
            after_progress_sequence,
        } = &request.action
        else {
            unreachable!("session wait handler requires a wait action")
        };
        let snapshot = self.supervisor.snapshot();
        if *run_id != snapshot.run_id {
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    format!(
                        "session wait run identity does not match the current run; current run_id is {}",
                        snapshot.run_id
                    ),
                ),
            );
        }
        if *after_session_version > snapshot.version {
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    "session wait version is ahead of the current session",
                ),
            );
        }
        let progress_event = match self
            .supervisor
            .progress_event_after(run_id, *after_progress_sequence)
        {
            Ok(event) => event,
            Err(_) => {
                return write_response(
                    &mut stream,
                    &ControlResponse::error(
                        &request.request_id,
                        ControlErrorCode::Conflict,
                        "session wait progress cursor is ahead of the current run",
                    ),
                );
            }
        };
        if let Some(pending) = snapshot.pending_architect_action.as_ref() {
            if *after_session_version < pending.published_version {
                return write_response(
                    &mut stream,
                    &ControlResponse::success(
                        &request.request_id,
                        ControlResult::Session { session: snapshot },
                    ),
                );
            }
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    "pending Architect action was already published at this session version; resolve it instead of waiting again",
                ),
            );
        }
        if let Some(event) = progress_event {
            return write_response(
                &mut stream,
                &ControlResponse::success(
                    &request.request_id,
                    ControlResult::Progress {
                        run_id: snapshot.run_id,
                        session_version: snapshot.version,
                        event,
                    },
                ),
            );
        }
        if snapshot.state.is_terminal() {
            return write_response(
                &mut stream,
                &ControlResponse::success(
                    &request.request_id,
                    ControlResult::Session { session: snapshot },
                ),
            );
        }
        if snapshot.state != super::SessionState::Running {
            return write_response(
                &mut stream,
                &ControlResponse::error(
                    &request.request_id,
                    ControlErrorCode::Conflict,
                    "session wait requires a running or terminal session",
                ),
            );
        }
        stream.set_read_timeout(None)?;
        stream.set_nonblocking(true)?;
        self.pending_wait = Some(PendingSessionWait {
            stream,
            request_id: request.request_id.clone(),
            run_id: run_id.clone(),
            after_progress_sequence: *after_progress_sequence,
        });
        Ok(())
    }

    fn service_pending_wait(&mut self) {
        let disconnected = self.pending_wait.as_mut().is_some_and(|wait| {
            let mut probe = [0u8; 1];
            match wait.stream.read(&mut probe) {
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    false
                }
                Ok(_) | Err(_) => true,
            }
        });
        if disconnected {
            self.pending_wait = None;
            return;
        }
        let snapshot = self.supervisor.snapshot();
        if self
            .pending_wait
            .as_ref()
            .is_some_and(|wait| wait.run_id != snapshot.run_id)
        {
            let Some(mut wait) = self.pending_wait.take() else {
                return;
            };
            let response = ControlResponse::error(
                &wait.request_id,
                ControlErrorCode::Conflict,
                format!(
                    "session wait belonged to an earlier run; current run_id is {}",
                    snapshot.run_id
                ),
            );
            let _ = wait.stream.set_nonblocking(false);
            let _ = wait.stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT));
            let _ = write_response(&mut wait.stream, &response);
            return;
        }
        let progress_event = match self.pending_wait.as_ref() {
            Some(wait) => match self
                .supervisor
                .progress_event_after(&wait.run_id, wait.after_progress_sequence)
            {
                Ok(event) => event,
                Err(_) => {
                    let Some(mut wait) = self.pending_wait.take() else {
                        return;
                    };
                    let response = ControlResponse::error(
                        &wait.request_id,
                        ControlErrorCode::Conflict,
                        "session wait progress cursor is ahead of the current run",
                    );
                    let _ = wait.stream.set_nonblocking(false);
                    let _ = wait.stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT));
                    let _ = write_response(&mut wait.stream, &response);
                    return;
                }
            },
            None => return,
        };
        if !snapshot.state.is_terminal()
            && snapshot.pending_architect_action.is_none()
            && progress_event.is_none()
        {
            return;
        }
        let Some(mut wait) = self.pending_wait.take() else {
            return;
        };
        let result = if snapshot.pending_architect_action.is_some() {
            ControlResult::Session { session: snapshot }
        } else if let Some(event) = progress_event {
            ControlResult::Progress {
                run_id: snapshot.run_id,
                session_version: snapshot.version,
                event,
            }
        } else {
            ControlResult::Session { session: snapshot }
        };
        let response = ControlResponse::success(&wait.request_id, result);
        let _ = wait.stream.set_nonblocking(false);
        let _ = wait.stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT));
        let _ = write_response(&mut wait.stream, &response);
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
        if matches!(
            &request.action,
            ControlAction::SessionStatus | ControlAction::SessionClarificationsList { .. }
        ) {
            if let ControlAction::SessionClarificationsList { run_id, .. } = &request.action {
                let snapshot = self.supervisor.snapshot();
                if *run_id != snapshot.run_id {
                    return ControlResponse::error(
                        &request.request_id,
                        ControlErrorCode::Conflict,
                        format!(
                            "clarification page run identity does not match the current run; current run_id is {}",
                            snapshot.run_id
                        ),
                    );
                }
            }
            return match self.dispatch_action(&request.action) {
                Ok(result) => ControlResponse::success(&request.request_id, result),
                Err(_) => {
                    let (code, message) = match &request.action {
                        ControlAction::SessionClarificationsList { .. } => (
                            ControlErrorCode::Conflict,
                            "clarification page does not match the exact task or cursor",
                        ),
                        _ => (
                            ControlErrorCode::Internal,
                            "session read-only state could not be read",
                        ),
                    };
                    ControlResponse::error(&request.request_id, code, message)
                }
            };
        }
        let record_response = if self.make_request_replay_room() {
            self.requests.insert(
                key.clone(),
                RequestRecord {
                    payload_hash,
                    response: None,
                },
            );
            self.request_order.push_back(key.clone());
            true
        } else if matches!(&request.action, ControlAction::SessionCancel { .. }) {
            // Cancellation remains the protocol-level escape hatch even if an
            // impossible re-entrant caller fills the window entirely with
            // in-progress requests. The single-threaded production endpoint
            // normally reaches this branch only after a completed entry can
            // be evicted above.
            false
        } else {
            return ControlResponse::error(
                &request.request_id,
                ControlErrorCode::Conflict,
                "session request replay window has no completed entry to evict",
            );
        };
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
        if record_response && let Some(record) = self.requests.get_mut(&key) {
            record.response = Some(response.clone());
        }
        response
    }

    fn make_request_replay_room(&mut self) -> bool {
        while self.requests.len() >= MAX_REQUEST_RECORDS {
            let mut index = 0;
            let mut evicted = false;
            while index < self.request_order.len() {
                let key = &self.request_order[index];
                match self.requests.get(key) {
                    None => {
                        self.request_order.remove(index);
                    }
                    Some(record) if record.response.is_some() => {
                        let key = self
                            .request_order
                            .remove(index)
                            .expect("request order index was checked");
                        self.requests.remove(&key);
                        evicted = true;
                        break;
                    }
                    Some(_) => {
                        index += 1;
                    }
                }
            }
            if !evicted {
                return false;
            }
        }
        true
    }

    fn dispatch_action(&mut self, action: &ControlAction) -> Result<ControlResult> {
        match action {
            ControlAction::SessionRunBegin {
                expected_session_version,
                terminal_run_id,
            } => {
                self.supervisor
                    .begin_next_run(*expected_session_version, terminal_run_id)?;
                Ok(ControlResult::Session {
                    session: self.supervisor.snapshot(),
                })
            }
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
            ControlAction::SessionClarificationSubmit {
                expected_session_version,
                task_ordinal,
                task_key,
                action_sequence,
                developer_request_path,
                clarification_document_path,
                human_decision_confirmed,
            } => {
                self.supervisor.submit_clarification(
                    *expected_session_version,
                    *task_ordinal,
                    task_key,
                    *action_sequence,
                    developer_request_path,
                    clarification_document_path,
                    *human_decision_confirmed,
                )?;
                Ok(ControlResult::Session {
                    session: self.supervisor.snapshot(),
                })
            }
            ControlAction::SessionClarificationRequireHuman {
                expected_session_version,
                task_ordinal,
                task_key,
                action_sequence,
                developer_request_path,
            } => {
                self.supervisor.require_human_for_clarification(
                    *expected_session_version,
                    *task_ordinal,
                    task_key,
                    *action_sequence,
                    developer_request_path,
                )?;
                Ok(ControlResult::Session {
                    session: self.supervisor.snapshot(),
                })
            }
            ControlAction::SessionClarificationsList {
                run_id,
                task_ordinal,
                task_key,
                after_sequence,
                limit,
            } => Ok(ControlResult::Clarifications {
                page: self.supervisor.clarification_page(
                    run_id,
                    *task_ordinal,
                    task_key,
                    *after_sequence,
                    *limit,
                )?,
            }),
            ControlAction::SessionWait { .. } => {
                bail!("session wait must be served by the deferred control path")
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
            } => {
                self.authorize_bridge(peer, binding_id, launch_nonce, capability, true)?;
                let binding = self
                    .bindings
                    .get(binding_id)
                    .ok_or_else(|| anyhow::anyhow!("architect binding disappeared"))?;
                if binding.state != BindingState::ProcessBound {
                    bail!("architect process binding is unavailable");
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
    use crate::control_api::codec::{read_response_frame, write_request_frame};
    use crate::control_api::protocol::PROTOCOL_VERSION;
    use crate::worker::profile::{
        ArchitectAdapter, CodexInvocationProfile, ReviewerInvocationProfile,
        SessionInvocationProfiles,
    };
    use std::collections::BTreeSet;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    use std::time::Instant;

    fn pure_codex_profiles() -> SessionInvocationProfiles {
        let mut profiles =
            SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        profiles.reviewer = ReviewerInvocationProfile::Codex {
            profile: CodexInvocationProfile::reviewer_default(),
        };
        profiles
    }

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
            let repository = root.join("repo");
            let toolchain = root.join("toolchain");
            for path in [&run, &repository, &toolchain] {
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
            let mut sources = SessionRuntimeSources::fake(&toolchain);
            // The lane only opens against the profiles loaded for this run.
            sources.set_profiles_for_test(pure_codex_profiles());
            Self {
                _temp: temp,
                paths: ControlPaths::new(&run).unwrap(),
                repository: fs::canonicalize(repository).unwrap(),
                sources,
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

    struct FakeSupervisor {
        startup: SessionStartup,
        snapshot: crate::control_api::SessionStatusSnapshot,
        progress_events: Vec<crate::control_api::SessionProgressEvent>,
        fail_poll: bool,
    }

    impl SupervisorBackend for FakeSupervisor {
        fn startup(&self) -> &SessionStartup {
            &self.startup
        }

        fn begin_next_run(
            &mut self,
            expected_session_version: u64,
            terminal_run_id: &str,
        ) -> Result<()> {
            if self.snapshot.version != expected_session_version
                || self.snapshot.run_id != terminal_run_id
                || !self.snapshot.state.is_terminal()
            {
                bail!("fake next-run gate failed");
            }
            self.snapshot.version = self
                .snapshot
                .version
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("fake session version overflow"))?;
            self.snapshot.run_id = "run-fake-next".into();
            self.snapshot.state = crate::control_api::SessionState::AwaitingPlan;
            self.snapshot.plan_version = None;
            self.snapshot.plan_hash = None;
            self.snapshot.current_task_ordinal = None;
            self.snapshot.active_worker = None;
            self.snapshot.pending_architect_action = None;
            self.snapshot.terminal_detail = None;
            self.snapshot.tasks.clear();
            self.progress_events.clear();
            self.startup.run_id.clone_from(&self.snapshot.run_id);
            Ok(())
        }

        fn replace_plan(
            &mut self,
            _expected_session_version: u64,
            _developer_adapter: &str,
            _reviewer_adapter: &str,
            _tasks: Vec<crate::control_api::TaskDraft>,
        ) -> Result<(u64, String)> {
            bail!("unused fake replace_plan")
        }

        fn approve_and_start(
            &mut self,
            _expected_session_version: u64,
            _plan_version: u64,
            _plan_hash: &str,
            _approval_confirmed: bool,
        ) -> Result<()> {
            bail!("unused fake approve_and_start")
        }

        fn submit_clarification(
            &mut self,
            _expected_session_version: u64,
            _task_ordinal: u32,
            _task_key: &str,
            _action_sequence: u32,
            _developer_request_path: &str,
            _clarification_document_path: &str,
            _human_decision_confirmed: bool,
        ) -> Result<()> {
            bail!("unused fake submit_clarification")
        }

        fn require_human_for_clarification(
            &mut self,
            _expected_session_version: u64,
            _task_ordinal: u32,
            _task_key: &str,
            _action_sequence: u32,
            _developer_request_path: &str,
        ) -> Result<()> {
            bail!("unused fake require_human_for_clarification")
        }

        fn cancel(&mut self, expected_session_version: u64, reason: &str) -> Result<()> {
            if self.snapshot.version != expected_session_version {
                bail!("fake session version is stale");
            }
            self.snapshot.version += 1;
            self.snapshot.state = crate::control_api::SessionState::Canceled;
            self.snapshot.terminal_detail = Some(reason.into());
            for task in &mut self.snapshot.tasks {
                task.state = crate::control_api::TaskState::Canceled;
            }
            Ok(())
        }

        fn snapshot(&self) -> crate::control_api::SessionStatusSnapshot {
            self.snapshot.clone()
        }

        fn clarification_page(
            &self,
            run_id: &str,
            task_ordinal: u32,
            task_key: &str,
            after_sequence: u32,
            _limit: u8,
        ) -> Result<ClarificationPage> {
            if run_id != self.snapshot.run_id {
                bail!("fake clarification run mismatch");
            }
            let task = self
                .snapshot
                .tasks
                .get(usize::try_from(task_ordinal)?)
                .filter(|task| task.task_key == task_key)
                .ok_or_else(|| anyhow::anyhow!("fake clarification task mismatch"))?;
            Ok(ClarificationPage {
                run_id: self.snapshot.run_id.clone(),
                session_version: self.snapshot.version,
                task_ordinal,
                task_key: task.task_key.clone(),
                total_records: task.clarification_record_count,
                after_sequence,
                records: Vec::new(),
                next_after_sequence: None,
            })
        }

        fn progress_event_after(
            &self,
            run_id: &str,
            after_sequence: u32,
        ) -> Result<Option<crate::control_api::SessionProgressEvent>> {
            if run_id != self.snapshot.run_id {
                bail!("fake progress run mismatch");
            }
            if usize::try_from(after_sequence)? > self.progress_events.len() {
                bail!("fake progress cursor is ahead");
            }
            Ok(self
                .progress_events
                .get(usize::try_from(after_sequence)?)
                .cloned())
        }

        fn poll_once(&mut self) -> Result<()> {
            if self.fail_poll {
                bail!("injected non-terminal poll failure")
            } else {
                Ok(())
            }
        }

        fn shutdown(&mut self) -> Result<()> {
            if !self.snapshot.state.is_terminal() {
                let version = self.snapshot.version;
                self.cancel(version, "fake parent stopped")?;
            }
            Ok(())
        }
    }

    fn fake_wait_control(
        state: crate::control_api::SessionState,
        version: u64,
        pending_action: bool,
    ) -> (SessionSupervisorControl, CallerAuth, PeerCredentials) {
        let project_root = PathBuf::from("/project");
        let (task_state, reviewer_verdict) = match state {
            crate::control_api::SessionState::Completed => (
                crate::control_api::TaskState::Lgtm,
                crate::control_api::ReviewerVerdict::Lgtm,
            ),
            crate::control_api::SessionState::Canceled => (
                crate::control_api::TaskState::Canceled,
                crate::control_api::ReviewerVerdict::RequestChanges,
            ),
            crate::control_api::SessionState::Failed => (
                crate::control_api::TaskState::Failed,
                crate::control_api::ReviewerVerdict::RequestChanges,
            ),
            crate::control_api::SessionState::NeedsHuman => (
                crate::control_api::TaskState::NeedsHuman,
                crate::control_api::ReviewerVerdict::RequestChanges,
            ),
            _ => (
                if pending_action {
                    crate::control_api::TaskState::AwaitingArchitectAction
                } else {
                    crate::control_api::TaskState::Developing
                },
                crate::control_api::ReviewerVerdict::RequestChanges,
            ),
        };
        let pending_architect_action =
            pending_action.then(|| crate::control_api::PendingArchitectActionSnapshot {
                task_ordinal: 0,
                task_key: "wait-task".into(),
                sequence: 1,
                reason: crate::control_api::ArchitectActionReason::Clarification,
                developer_request_path: "/artifacts/developer/request.md".into(),
                clarification_output_path:
                    "/project/hcom-tasks/run-wait-test/wait-task/clarification/turn-1.md".into(),
                clarification_rounds_used: 0,
                max_clarification_rounds: 2,
                human_decision_required: false,
                published_version: version,
            });
        let snapshot = crate::control_api::SessionStatusSnapshot {
            run_id: "run-wait-test".into(),
            state,
            version,
            project_root: project_root.to_string_lossy().into_owned(),
            plan_version: Some(1),
            plan_hash: Some("a".repeat(64)),
            current_task_ordinal: Some(0),
            active_worker: None,
            pending_architect_action,
            terminal_detail: state
                .is_terminal()
                .then(|| "terminal before subscription".into()),
            tasks: vec![crate::control_api::TaskStatusSnapshot {
                task_key: "wait-task".into(),
                ordinal: 0,
                state: task_state,
                repository_root: "/source".into(),
                task_document_path: "/project/current_todo.md".into(),
                design_document_paths: vec!["/project/design.md".into()],
                task_selector: "FBTC-03".into(),
                branch: None,
                review_round: 1,
                max_review_rounds: 3,
                clarification_rounds_used: 0,
                max_clarification_rounds: 2,
                clarification_record_count: 0,
                base_revision: None,
                head_revision: None,
                developer_session_bound: true,
                reviewer_session_bound: true,
                outcome_detail: Some("Reviewer returned LGTM".into()),
                latest_developer_final_path: Some(
                    "/artifacts/developer/native-final.partial".into(),
                ),
                final_reviewer_message_paths: vec![
                    "/artifacts/reviewer/native-final.partial".into(),
                ],
                reviewer_verdict: Some(reviewer_verdict),
            }],
        };
        let binding_id = "binding-wait-test";
        let launch_nonce = "launch-nonce-wait-test";
        let capability = "capability-wait-test";
        let (action_set_json, _) = canonical_action_set(ActionName::ARCHITECT).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        let binding = ArchitectBinding {
            id: binding_id.into(),
            project_root: project_root.clone(),
            launch_nonce_hash: secret_hash(b"hcom-session/launch-nonce/v1", launch_nonce),
            capability_hash: secret_hash(b"hcom-session/capability/v1", capability),
            action_set_hash: sha256_hex(action_set_json.as_bytes()),
            action_set_json,
            state: BindingState::ProcessBound,
            version: 1,
            architect_pid: Some(std::process::id()),
            architect_process_birth: Some(birth.clone()),
            bridge_pid: Some(std::process::id()),
            bridge_process_birth: Some(birth.clone()),
            relay_executable_contract_hash: Some("b".repeat(64)),
            relay_runtime_scope_hash: Some("c".repeat(64)),
        };
        let control = SessionSupervisorControl {
            supervisor: Box::new(FakeSupervisor {
                startup: SessionStartup {
                    run_id: "run-wait-test".into(),
                    project_root,
                    session_binding_hash: "d".repeat(64),
                },
                snapshot,
                progress_events: Vec::new(),
                fail_poll: false,
            }),
            // SAFETY: geteuid has no preconditions.
            expected_uid: unsafe { libc::geteuid() },
            parent_pid: std::process::id(),
            parent_birth: birth,
            bindings: BTreeMap::from([(binding_id.into(), binding)]),
            requests: BTreeMap::new(),
            request_order: VecDeque::new(),
            pending_wait: None,
        };
        let caller = CallerAuth::Architect {
            binding_id: binding_id.into(),
            launch_nonce: launch_nonce.into(),
            capability: capability.into(),
        };
        // SAFETY: geteuid/getegid have no preconditions.
        let peer = PeerCredentials {
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        };
        (control, caller, peer)
    }

    fn wait_request(caller: CallerAuth, request_id: &str, version: u64) -> ControlRequest {
        wait_request_after(caller, request_id, version, 0)
    }

    fn wait_request_after(
        caller: CallerAuth,
        request_id: &str,
        version: u64,
        after_progress_sequence: u32,
    ) -> ControlRequest {
        ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            caller,
            action: ControlAction::SessionWait {
                run_id: "run-wait-test".into(),
                after_session_version: version,
                after_progress_sequence,
            },
        }
    }

    fn review_requested_event() -> crate::control_api::SessionProgressEvent {
        crate::control_api::SessionProgressEvent::ReviewRequested {
            sequence: 1,
            task_ordinal: 0,
            task_key: "wait-task".into(),
            completed_tasks: 0,
            total_tasks: 1,
            review_round: 1,
            max_review_rounds: 3,
            developer_final_path: "/artifacts/developer/native-final.partial".into(),
            task_document_path: "/project/current_todo.md".into(),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: "FBTC-03".into(),
            clarification_record_count: 0,
        }
    }

    fn serve_wait(control: &mut SessionSupervisorControl, request: &ControlRequest) -> UnixStream {
        let (mut client, server) = UnixStream::pair().unwrap();
        write_request_frame(&mut client, &serde_json::to_vec(request).unwrap()).unwrap();
        control.serve_stream(server).unwrap();
        client
    }

    #[test]
    fn deferred_session_wait_returns_terminal_snapshot_without_losing_the_gap() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        let request = wait_request(caller.clone(), "wait-running", 7);
        let mut client = serve_wait(&mut control, &request);
        assert!(control.pending_wait.is_some());

        control.supervisor.cancel(7, "terminal after wait").unwrap();
        control.service_pending_wait();
        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert!(response.ok);
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("wait must return a session snapshot")
        };
        assert_eq!(session.state, crate::control_api::SessionState::Canceled);
        assert_eq!(session.version, 8);
        assert_eq!(
            session.terminal_detail.as_deref(),
            Some("terminal after wait")
        );
        assert_eq!(
            session.tasks[0].final_reviewer_message_paths,
            ["/artifacts/reviewer/native-final.partial"]
        );
        assert_eq!(
            session.tasks[0].reviewer_verdict,
            Some(crate::control_api::ReviewerVerdict::RequestChanges)
        );

        let (mut terminal_control, terminal_caller, _) =
            fake_wait_control(crate::control_api::SessionState::Completed, 11, false);
        let mut replay = serve_wait(
            &mut terminal_control,
            &wait_request(terminal_caller, "wait-after-gap", 7),
        );
        assert!(terminal_control.pending_wait.is_none());
        let frame = read_response_frame(&mut replay).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("terminal replay must return a session snapshot")
        };
        assert_eq!(session.state, crate::control_api::SessionState::Completed);
        assert_eq!(session.version, 11);
    }

    #[test]
    fn session_wait_returns_retained_progress_immediately_and_by_cursor() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        let snapshot = control.supervisor.snapshot();
        control.supervisor = Box::new(FakeSupervisor {
            startup: control.supervisor.startup().clone(),
            snapshot,
            progress_events: vec![review_requested_event()],
            fail_poll: false,
        });

        let mut client = serve_wait(
            &mut control,
            &wait_request_after(caller, "wait-progress", 7, 0),
        );
        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert_eq!(
            response.result,
            Some(ControlResult::Progress {
                run_id: "run-wait-test".into(),
                session_version: 7,
                event: review_requested_event(),
            })
        );
    }

    #[test]
    fn a_progress_event_releases_an_already_pending_wait() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        let mut client = serve_wait(
            &mut control,
            &wait_request_after(caller, "wait-before-progress", 7, 0),
        );
        assert!(control.pending_wait.is_some());

        let snapshot = control.supervisor.snapshot();
        control.supervisor = Box::new(FakeSupervisor {
            startup: control.supervisor.startup().clone(),
            snapshot,
            progress_events: vec![review_requested_event()],
            fail_poll: false,
        });
        control.service_pending_wait();

        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert_eq!(
            response.result,
            Some(ControlResult::Progress {
                run_id: "run-wait-test".into(),
                session_version: 7,
                event: review_requested_event(),
            })
        );
    }

    #[test]
    fn invalidated_pending_progress_cursor_returns_the_closed_conflict() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        let mut client = serve_wait(
            &mut control,
            &wait_request_after(caller, "wait-invalidated-progress", 7, 0),
        );
        control
            .pending_wait
            .as_mut()
            .expect("wait must be pending")
            .after_progress_sequence = 1;
        control.service_pending_wait();

        let frame = read_response_frame(&mut client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert_eq!(
            response.error,
            Some(crate::control_api::ControlErrorBody {
                code: ControlErrorCode::Conflict,
                message: "session wait progress cursor is ahead of the current run".into(),
            })
        );
    }

    #[test]
    fn pending_action_precedes_progress_and_progress_precedes_terminal() {
        let (mut action_control, action_caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 9, true);
        let snapshot = action_control.supervisor.snapshot();
        action_control.supervisor = Box::new(FakeSupervisor {
            startup: action_control.supervisor.startup().clone(),
            snapshot,
            progress_events: vec![review_requested_event()],
            fail_poll: false,
        });
        let mut action_client = serve_wait(
            &mut action_control,
            &wait_request_after(action_caller, "wait-action-priority", 7, 0),
        );
        let frame = read_response_frame(&mut action_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("pending Architect action must take priority")
        };
        assert!(session.pending_architect_action.is_some());

        let (mut terminal_control, terminal_caller, _) =
            fake_wait_control(crate::control_api::SessionState::Completed, 11, false);
        let snapshot = terminal_control.supervisor.snapshot();
        terminal_control.supervisor = Box::new(FakeSupervisor {
            startup: terminal_control.supervisor.startup().clone(),
            snapshot,
            progress_events: vec![review_requested_event()],
            fail_poll: false,
        });
        let mut progress_client = serve_wait(
            &mut terminal_control,
            &wait_request_after(
                terminal_caller.clone(),
                "wait-progress-before-terminal",
                7,
                0,
            ),
        );
        let frame = read_response_frame(&mut progress_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert!(matches!(
            response.result,
            Some(ControlResult::Progress { .. })
        ));

        let mut terminal_client = serve_wait(
            &mut terminal_control,
            &wait_request_after(terminal_caller, "wait-terminal-after-progress", 11, 1),
        );
        let frame = read_response_frame(&mut terminal_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("terminal snapshot must follow drained progress")
        };
        assert_eq!(session.state, crate::control_api::SessionState::Completed);
    }

    #[test]
    fn terminal_run_begin_creates_a_new_run_and_old_wait_identity_is_rejected() {
        let (mut control, caller, peer) =
            fake_wait_control(crate::control_api::SessionState::Completed, 11, false);
        let begin = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "begin-next-run".into(),
            caller: caller.clone(),
            action: ControlAction::SessionRunBegin {
                expected_session_version: 11,
                terminal_run_id: "run-wait-test".into(),
            },
        };
        let first = control.handle_request(peer, &begin);
        assert!(first.ok);
        let Some(ControlResult::Session { session }) = first.result.as_ref() else {
            panic!("run begin must return the new session snapshot")
        };
        assert_eq!(session.run_id, "run-fake-next");
        assert_eq!(session.version, 12);
        assert_eq!(
            session.state,
            crate::control_api::SessionState::AwaitingPlan
        );
        assert!(session.tasks.is_empty());
        assert_eq!(control.supervisor.startup().run_id, "run-fake-next");
        assert_eq!(
            control.handle_request(peer, &begin),
            first,
            "a retained begin request must replay its exact new-run response"
        );

        let stale_retry = ControlRequest {
            request_id: "begin-next-run-stale-retry".into(),
            ..begin.clone()
        };
        let rejected = control.handle_request(peer, &stale_retry);
        assert_eq!(
            rejected.error.map(|error| error.code),
            Some(ControlErrorCode::Conflict)
        );
        assert_eq!(control.supervisor.snapshot().run_id, "run-fake-next");
        assert_eq!(control.supervisor.snapshot().version, 12);

        let mut stale_wait =
            serve_wait(&mut control, &wait_request(caller, "wait-from-old-run", 11));
        let frame = read_response_frame(&mut stale_wait).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let error = response.error.expect("stale wait must return an error");
        assert_eq!(error.code, ControlErrorCode::Conflict);
        assert!(error.message.contains("current run_id is run-fake-next"));
        assert!(control.pending_wait.is_none());
    }

    #[test]
    fn next_run_releases_an_unserviced_terminal_wait_from_the_old_run() {
        let (mut control, caller, peer) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        let mut old_wait = serve_wait(
            &mut control,
            &wait_request(caller.clone(), "old-run-pending-wait", 7),
        );
        assert!(control.pending_wait.is_some());
        control.supervisor.cancel(7, "old run completed").unwrap();

        let begin = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "begin-before-old-wait-service".into(),
            caller,
            action: ControlAction::SessionRunBegin {
                expected_session_version: 8,
                terminal_run_id: "run-wait-test".into(),
            },
        };
        assert!(control.handle_request(peer, &begin).ok);
        assert_eq!(control.supervisor.snapshot().run_id, "run-fake-next");
        control.service_pending_wait();
        assert!(control.pending_wait.is_none());

        let frame = read_response_frame(&mut old_wait).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let error = response
            .error
            .expect("old pending wait must return an error");
        assert_eq!(error.code, ControlErrorCode::Conflict);
        assert!(error.message.contains("current run_id is run-fake-next"));
    }

    #[test]
    fn nonterminal_poll_failure_fallback_terminalizes_and_releases_session_wait() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 7, false);
        control.supervisor = Box::new(FakeSupervisor {
            startup: control.supervisor.startup().clone(),
            snapshot: control.supervisor.snapshot(),
            progress_events: Vec::new(),
            fail_poll: true,
        });
        let mut client = serve_wait(&mut control, &wait_request(caller, "wait-poll-failure", 7));
        assert!(control.pending_wait.is_some());

        control.poll_and_service_wait().unwrap();
        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("poll-failure fallback must release the pending wait")
        };
        assert_eq!(session.state, crate::control_api::SessionState::Canceled);
        assert_eq!(session.version, 8);
        assert_eq!(
            session.terminal_detail.as_deref(),
            Some("fake parent stopped")
        );
    }

    #[test]
    fn pending_architect_action_redelivers_only_from_an_older_version() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 9, true);

        let mut gap_client = serve_wait(
            &mut control,
            &wait_request(caller.clone(), "wait-action-gap", 7),
        );
        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut gap_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("older-version wait must redeliver the pending action")
        };
        let pending = session
            .pending_architect_action
            .expect("latched action disappeared");
        assert_eq!(pending.sequence, 1);
        assert_eq!(pending.published_version, 9);
        assert_eq!(session.version, 9);

        let mut current_client = serve_wait(
            &mut control,
            &wait_request(caller, "wait-action-current", 9),
        );
        assert!(control.pending_wait.is_none());
        let frame = read_response_frame(&mut current_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        assert!(!response.ok);
        assert_eq!(
            response.error.map(|error| error.code),
            Some(ControlErrorCode::Conflict)
        );
    }

    #[test]
    fn disconnecting_session_wait_only_removes_the_subscription() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 3, false);
        let client = serve_wait(&mut control, &wait_request(caller, "wait-disconnect", 3));
        assert!(control.pending_wait.is_some());
        drop(client);
        let deadline = Instant::now() + Duration::from_secs(2);
        while control.pending_wait.is_some() && Instant::now() < deadline {
            control.service_pending_wait();
            std::thread::yield_now();
        }
        assert!(control.pending_wait.is_none());
        assert_eq!(
            control.supervisor.snapshot().state,
            crate::control_api::SessionState::Running
        );
        assert_eq!(control.supervisor.snapshot().version, 3);
    }

    #[test]
    fn session_status_remains_available_while_terminal_wait_is_pending() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Running, 5, false);
        let wait_client = serve_wait(
            &mut control,
            &wait_request(caller.clone(), "wait-before-status", 5),
        );
        assert!(control.pending_wait.is_some());

        let status_request = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "explicit-human-status".into(),
            caller,
            action: ControlAction::SessionStatus,
        };
        let (mut status_client, status_server) = UnixStream::pair().unwrap();
        write_request_frame(
            &mut status_client,
            &serde_json::to_vec(&status_request).unwrap(),
        )
        .unwrap();
        control.serve_stream(status_server).unwrap();
        let frame = read_response_frame(&mut status_client).unwrap();
        let response: ControlResponse = serde_json::from_slice(&frame).unwrap();
        let Some(ControlResult::Session { session }) = response.result else {
            panic!("status must return a session snapshot")
        };
        assert_eq!(session.state, crate::control_api::SessionState::Running);
        assert_eq!(session.version, 5);
        assert_eq!(
            session.tasks[0].latest_developer_final_path.as_deref(),
            Some("/artifacts/developer/native-final.partial")
        );
        assert_eq!(
            session.tasks[0].final_reviewer_message_paths,
            ["/artifacts/reviewer/native-final.partial"]
        );
        assert!(control.pending_wait.is_some());

        drop(wait_client);
        let deadline = Instant::now() + Duration::from_secs(2);
        while control.pending_wait.is_some() && Instant::now() < deadline {
            control.service_pending_wait();
            std::thread::yield_now();
        }
        assert!(control.pending_wait.is_none());
    }

    #[test]
    fn clarification_pages_are_read_only_and_do_not_consume_replay_capacity() {
        let (mut control, caller, _) =
            fake_wait_control(crate::control_api::SessionState::Completed, 12, false);
        let request = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "clarification-page".into(),
            caller: caller.clone(),
            action: ControlAction::SessionClarificationsList {
                run_id: "run-wait-test".into(),
                task_ordinal: 0,
                task_key: "wait-task".into(),
                after_sequence: 0,
                limit: 8,
            },
        };
        let (mut client, server) = UnixStream::pair().unwrap();
        write_request_frame(&mut client, &serde_json::to_vec(&request).unwrap()).unwrap();
        control.serve_stream(server).unwrap();
        let response: ControlResponse =
            serde_json::from_slice(&read_response_frame(&mut client).unwrap()).unwrap();
        let Some(ControlResult::Clarifications { page }) = response.result else {
            panic!("clarification list must return a bounded page")
        };
        assert_eq!(page.task_key, "wait-task");
        assert_eq!(page.total_records, 0);
        assert!(page.records.is_empty());
        assert!(control.requests.is_empty());

        let wrong_run = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "clarification-page-wrong-run".into(),
            caller,
            action: ControlAction::SessionClarificationsList {
                run_id: "run-older".into(),
                task_ordinal: 0,
                task_key: "wait-task".into(),
                after_sequence: 0,
                limit: 8,
            },
        };
        let (mut client, server) = UnixStream::pair().unwrap();
        write_request_frame(&mut client, &serde_json::to_vec(&wrong_run).unwrap()).unwrap();
        control.serve_stream(server).unwrap();
        let response: ControlResponse =
            serde_json::from_slice(&read_response_frame(&mut client).unwrap()).unwrap();
        let error = response
            .error
            .expect("wrong-run clarification list must return an error");
        assert_eq!(error.code, ControlErrorCode::Conflict);
        assert!(error.message.contains("current run_id is run-wait-test"));
        assert!(!error.message.contains("task or cursor"));
        assert!(control.requests.is_empty());
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
    fn task_lane_backend_uses_the_same_private_control_endpoint_contract() {
        let mut fixture = Fixture::new();
        fixture.sources.set_profiles_for_test(pure_codex_profiles());
        let control_socket = fixture.paths.socket_path();
        let registration_socket = fixture.paths.registration_socket_path();
        {
            let endpoint = SessionSupervisorEndpoint::bind(
                fixture.paths.clone(),
                "run-exec-endpoint".into(),
                fixture.repository.clone(),
                fixture.sources.clone(),
            )
            .unwrap();
            assert_eq!(endpoint.startup().run_id, "run-exec-endpoint");
            assert_eq!(endpoint.startup().project_root, fixture.repository);
            assert!(control_socket.exists());
            assert!(registration_socket.exists());
        }
        assert!(!control_socket.exists());
        assert!(!registration_socket.exists());
    }

    #[test]
    fn architect_process_binding_is_exact_and_request_replay_is_payload_bound() {
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
                state: BindingState::ProcessBound,
                version: 1,
                architect_pid: Some(std::process::id()),
                architect_process_birth: Some(birth.clone()),
                bridge_pid: Some(std::process::id()),
                bridge_process_birth: Some(birth),
                relay_executable_contract_hash: Some("a".repeat(64)),
                relay_runtime_scope_hash: Some("b".repeat(64)),
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

        let mut wrong_capability = request;
        let CallerAuth::Architect { capability, .. } = &mut wrong_capability.caller else {
            unreachable!()
        };
        *capability = "capability-session-other".into();
        assert_eq!(
            control
                .handle_request(peer, &wrong_capability)
                .error
                .unwrap()
                .code,
            ControlErrorCode::Unauthorized
        );
    }

    #[test]
    fn completed_request_replay_window_evicts_oldest_and_keeps_recent_replays() {
        let (mut control, caller, peer) =
            fake_wait_control(crate::control_api::SessionState::AwaitingPlan, 0, false);
        let mut newest_request = None;
        let mut newest_response = None;
        for index in 0..MAX_REQUEST_RECORDS {
            let request = ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: format!("bounded-{index}"),
                caller: caller.clone(),
                action: ControlAction::SessionCancel {
                    expected_session_version: 99,
                    reason: format!("stale bounded request {index}"),
                },
            };
            let response = control.handle_request(peer, &request);
            assert_eq!(
                response.error.as_ref().unwrap().code,
                ControlErrorCode::Conflict
            );
            newest_request = Some(request);
            newest_response = Some(response);
        }
        assert_eq!(control.requests.len(), MAX_REQUEST_RECORDS);
        assert_eq!(control.request_order.len(), MAX_REQUEST_RECORDS);
        assert_eq!(
            control.handle_request(peer, newest_request.as_ref().unwrap()),
            newest_response.unwrap(),
            "the newest completed request remains replayable"
        );

        let cancel = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "bounded-cancel".into(),
            caller,
            action: ControlAction::SessionCancel {
                expected_session_version: 0,
                reason: "cancel remains available at replay capacity".into(),
            },
        };
        let response = control.handle_request(peer, &cancel);
        assert!(response.ok);
        assert_eq!(
            control.supervisor.snapshot().state,
            crate::control_api::SessionState::Canceled
        );
        assert_eq!(control.requests.len(), MAX_REQUEST_RECORDS);
        assert_eq!(control.request_order.len(), MAX_REQUEST_RECORDS);
        assert!(
            !control
                .requests
                .contains_key(&("architect:binding-wait-test".into(), "bounded-0".into())),
            "the oldest completed request leaves the recent replay window"
        );
        assert!(control.requests.contains_key(&(
            "architect:binding-wait-test".into(),
            "bounded-cancel".into()
        )));
    }

    #[test]
    fn evicted_successful_request_cannot_reapply_after_session_version_changes() {
        let (mut control, caller, peer) =
            fake_wait_control(crate::control_api::SessionState::AwaitingPlan, 0, false);
        let successful = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "successful-before-eviction".into(),
            caller: caller.clone(),
            action: ControlAction::SessionCancel {
                expected_session_version: 0,
                reason: "first cancellation".into(),
            },
        };
        assert!(control.handle_request(peer, &successful).ok);
        assert_eq!(control.supervisor.snapshot().version, 1);

        for index in 0..MAX_REQUEST_RECORDS {
            let response = control.handle_request(
                peer,
                &ControlRequest {
                    protocol_version: PROTOCOL_VERSION,
                    request_id: format!("post-success-{index}"),
                    caller: caller.clone(),
                    action: ControlAction::SessionCancel {
                        expected_session_version: 0,
                        reason: format!("stale post-success request {index}"),
                    },
                },
            );
            assert_eq!(
                response.error.as_ref().unwrap().code,
                ControlErrorCode::Conflict
            );
        }
        assert!(
            !control.requests.contains_key(&(
                "architect:binding-wait-test".into(),
                successful.request_id.clone()
            )),
            "the oldest successful response was evicted"
        );

        let replay = control.handle_request(peer, &successful);
        assert_eq!(
            replay.error.as_ref().unwrap().code,
            ControlErrorCode::Conflict
        );
        let snapshot = control.supervisor.snapshot();
        assert_eq!(snapshot.state, crate::control_api::SessionState::Canceled);
        assert_eq!(
            snapshot.version, 1,
            "the evicted successful action cannot execute again"
        );
        assert_eq!(
            snapshot.terminal_detail.as_deref(),
            Some("first cancellation")
        );
    }

    #[test]
    fn cancellation_bypasses_a_window_containing_only_in_progress_requests() {
        let (mut control, caller, peer) =
            fake_wait_control(crate::control_api::SessionState::AwaitingPlan, 0, false);
        for index in 0..MAX_REQUEST_RECORDS {
            let key = (
                format!("in-progress-caller-{index}"),
                format!("in-progress-{index}"),
            );
            control.requests.insert(
                key.clone(),
                RequestRecord {
                    payload_hash: "a".repeat(64),
                    response: None,
                },
            );
            control.request_order.push_back(key);
        }

        let response = control.handle_request(
            peer,
            &ControlRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "cancel-with-all-requests-in-progress".into(),
                caller,
                action: ControlAction::SessionCancel {
                    expected_session_version: 0,
                    reason: "emergency unrecorded cancellation".into(),
                },
            },
        );
        assert!(response.ok);
        assert_eq!(control.requests.len(), MAX_REQUEST_RECORDS);
        assert_eq!(
            control.supervisor.snapshot().state,
            crate::control_api::SessionState::Canceled
        );
    }
}
