#[cfg(test)]
use super::tools::ARCHITECT_INSTRUCTIONS;
use super::tools::{
    architect_instructions_for_delivery, control_action_for_delivery,
    tool_definitions_for_delivery, validate_codex_tool_definitions,
};
use crate::control_api::client::ControlClient;
use crate::control_api::codec::{
    read_request_frame, read_response_frame, write_request_frame, write_response_frame,
};
use crate::control_api::peer::{
    ProcessExecutableIdentity, peer_credentials, process_birth_identity,
    process_executable_identity, process_has_ancestor, process_is_live_identity,
};
use crate::control_api::protocol::{MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, PROTOCOL_VERSION};
use crate::control_api::registration::{
    CONTROL_REFUSAL_TRANSPORT, RegistrationAction, RegistrationCaller, RegistrationClient,
    RegistrationRequest, TOOL_REFUSAL_ACTION, TOOL_REFUSAL_ENVELOPE,
};
use crate::control_api::supervisor::ControlPaths;
use crate::control_api::{
    CallerAuth, ControlAction, ControlRequest, ControlResponse, ReviewerAdapterBinding,
};
use crate::worker::ExecutableIdentity;
use crate::worker::profile::{
    ArchitectAdapter, CLAUDE_DEVELOPER_ADAPTER, CLAUDE_REVIEWER_ADAPTER, CODEX_DEVELOPER_ADAPTER,
    CODEX_REVIEWER_ADAPTER,
};
use crate::worker::runtime::{CLAUDE_TASK_WORKER_ADAPTER, CODEX_TASK_WORKER_ADAPTER};
use crate::worker::validation::{validate_opaque_id, validate_sha256};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use uuid::Uuid;

const fn max_mcp_line_bytes() -> usize {
    let duplicated_control_response = match MAX_RESPONSE_BYTES.checked_mul(3) {
        Some(value) => value,
        None => panic!("MCP line capacity overflow"),
    };
    let with_request_id = match duplicated_control_response.checked_add(MAX_REQUEST_BYTES) {
        Some(value) => value,
        None => panic!("MCP line capacity overflow"),
    };
    match with_request_id.checked_add(4096) {
        Some(value) => value,
        None => panic!("MCP line capacity overflow"),
    }
}

const MAX_MCP_LINE_BYTES: usize = max_mcp_line_bytes();
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const RELAY_SOCKET_NAME: &str = "relay.sock";
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
struct ToolCallRefusal(&'static str);

impl std::fmt::Display for ToolCallRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ToolCallRefusal {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct BridgeConfiguration {
    pub binding_id: String,
    pub launch_nonce: String,
    pub capability: String,
    pub project_root: PathBuf,
    pub run_root: PathBuf,
    pub relay_socket_path: PathBuf,
    pub registration_socket_path: PathBuf,
    pub control_socket_path: PathBuf,
    pub relay_executable: ExecutableIdentity,
    pub relay_runtime_scope_hash: String,
    pub session_binding_hash: String,
    pub architect_adapter: String,
    pub architect_additional_directories: Vec<PathBuf>,
    pub developer_adapter: String,
    pub reviewer_adapters: Vec<ReviewerAdapterBinding>,
    pub github_pr: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct BridgeActivation {
    pub architect_pid: u32,
    pub architect_process_birth: String,
    pub bridge_pid: u32,
    pub bridge_process_birth: String,
    pub binding_version: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BootstrapRequest {
    Configure {
        configuration: Box<BridgeConfiguration>,
    },
    Activate {
        activation: BridgeActivation,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BootstrapResponse {
    Ready,
    Active,
    Error { message: String },
}

pub(super) fn run_bridge(bootstrap_fd: RawFd) -> Result<()> {
    if !(3..=1024).contains(&bootstrap_fd) {
        bail!("bridge bootstrap fd is outside its closed range");
    }
    // SAFETY: the launcher transfers sole ownership of this inherited
    // descriptor to the bridge component.
    let mut bootstrap = unsafe { UnixStream::from_raw_fd(bootstrap_fd) };
    bootstrap.set_read_timeout(Some(Duration::from_secs(10)))?;
    bootstrap.set_write_timeout(Some(Duration::from_secs(10)))?;

    let configuration = match read_bootstrap(&mut bootstrap)? {
        BootstrapRequest::Configure { configuration } => *configuration,
        _ => {
            write_bootstrap(
                &mut bootstrap,
                &BootstrapResponse::Error {
                    message: "bridge configuration must be first".into(),
                },
            )?;
            bail!("bridge configuration must be first");
        }
    };
    validate_bridge_configuration(&configuration)?;
    let socket = RelaySocketGuard::bind(&configuration.relay_socket_path)?;
    write_bootstrap(&mut bootstrap, &BootstrapResponse::Ready)?;

    let activation = match read_bootstrap(&mut bootstrap)? {
        BootstrapRequest::Activate { activation } => activation,
        _ => {
            write_bootstrap(
                &mut bootstrap,
                &BootstrapResponse::Error {
                    message: "bridge activation must follow configuration".into(),
                },
            )?;
            bail!("bridge activation must follow configuration");
        }
    };
    validate_activation(&configuration, &activation)?;
    write_bootstrap(&mut bootstrap, &BootstrapResponse::Active)?;
    drop(bootstrap);

    serve_bridge(socket, configuration, activation)
}

pub(super) fn run_relay(socket_path: &Path) -> Result<()> {
    if unsafe { libc::isatty(libc::STDIN_FILENO) } == 1
        || unsafe { libc::isatty(libc::STDOUT_FILENO) } == 1
    {
        bail!("architect stdio relay refuses an interactive terminal");
    }
    validate_relay_socket_path(socket_path)?;
    let mut upstream = UnixStream::connect(socket_path).with_context(|| {
        format!(
            "failed to connect architect relay {}",
            socket_path.display()
        )
    })?;
    configure_persistent_mcp_stream(&upstream)?;
    let reader_stream = upstream.try_clone()?;
    let response_thread = std::thread::spawn(move || -> Result<()> {
        let mut reader = BufReader::new(reader_stream);
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        while let Some(line) = read_bounded_line(&mut reader)? {
            stdout.write_all(&line)?;
            stdout.flush()?;
        }
        Ok(())
    });

    let stdin = std::io::stdin();
    let mut stdin = BufReader::new(stdin.lock());
    while let Some(line) = read_bounded_line(&mut stdin)? {
        upstream.write_all(&line)?;
        upstream.flush()?;
    }
    upstream.shutdown(std::net::Shutdown::Write)?;
    response_thread
        .join()
        .map_err(|_| anyhow::anyhow!("architect relay response thread panicked"))??;
    Ok(())
}

pub(super) fn relay_runtime_scope_hash(directory: &Path) -> Result<String> {
    if !directory.is_absolute() {
        bail!("architect relay directory must be absolute");
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical != directory {
        bail!("architect relay directory must already be canonical");
    }
    let metadata = fs::symlink_metadata(directory)?;
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != uid
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("architect relay directory must be private and current-user owned");
    }
    let value = serde_json::to_vec(&(
        directory,
        metadata.dev(),
        metadata.ino(),
        metadata.uid(),
        metadata.permissions().mode() & 0o777,
    ))?;
    Ok(sha256_hex(&value))
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn validate_bridge_configuration(configuration: &BridgeConfiguration) -> Result<()> {
    validate_opaque_id("architect binding id", &configuration.binding_id)?;
    validate_secret(&configuration.launch_nonce)?;
    validate_secret(&configuration.capability)?;
    validate_canonical_directory(
        "architect project directory",
        &configuration.project_root,
        None,
    )?;
    let relay_parent = configuration
        .relay_socket_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("architect relay socket has no parent"))?;
    if configuration.relay_socket_path != relay_parent.join(RELAY_SOCKET_NAME) {
        bail!("architect relay socket path is not canonical");
    }
    let scope_hash = relay_runtime_scope_hash(relay_parent)?;
    if scope_hash != configuration.relay_runtime_scope_hash {
        bail!("architect relay runtime scope drifted before bridge start");
    }
    configuration.relay_executable.revalidate()?;
    validate_sha256(
        "architect bridge session binding hash",
        &configuration.session_binding_hash,
    )?;
    let architect_adapter = ArchitectAdapter::parse(&configuration.architect_adapter)?;
    if architect_adapter == ArchitectAdapter::Codex
        && !configuration.architect_additional_directories.is_empty()
    {
        bail!("Codex Architect bridge binding cannot carry Claude --add-dir roots");
    }
    if configuration.architect_additional_directories.len() > 64 {
        bail!("architect bridge binding contains too many --add-dir roots");
    }
    let mut previous = BTreeSet::new();
    for directory in &configuration.architect_additional_directories {
        validate_canonical_directory("Claude Architect --add-dir", directory, None)?;
        if !previous.insert(directory) {
            bail!("architect bridge binding contains duplicate --add-dir roots");
        }
    }
    validate_worker_adapter_binding(
        &configuration.developer_adapter,
        &configuration.reviewer_adapters,
    )?;
    if architect_adapter == ArchitectAdapter::Codex {
        validate_codex_tool_definitions(&tool_definitions_for_delivery(
            &configuration.developer_adapter,
            &configuration.reviewer_adapters,
            configuration.github_pr,
        ))?;
    }
    let paths = ControlPaths::new(&configuration.run_root)?;
    if paths.socket_path() != configuration.control_socket_path
        || paths.registration_socket_path() != configuration.registration_socket_path
    {
        bail!("architect bridge session socket paths drifted");
    }
    Ok(())
}

fn validate_worker_adapter_binding(
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
) -> Result<()> {
    if !matches!(
        reviewer_adapters,
        [ReviewerAdapterBinding {
            reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
            ..
        }] | [
            ReviewerAdapterBinding {
                reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
                ..
            },
            ReviewerAdapterBinding {
                reviewer_id: crate::worker::profile::ReviewerId::Reviewer2,
                ..
            }
        ]
    ) {
        bail!("architect bridge requires a canonical ordered Reviewer adapter topology");
    }
    let routed_worker_pair = matches!(
        developer_adapter,
        CODEX_TASK_WORKER_ADAPTER | CLAUDE_TASK_WORKER_ADAPTER
    ) && reviewer_adapters.iter().all(|binding| {
        matches!(
            binding.adapter.as_str(),
            CODEX_TASK_WORKER_ADAPTER | CLAUDE_TASK_WORKER_ADAPTER
        )
    });
    let retained_cli_pair = matches!(
        developer_adapter,
        CODEX_DEVELOPER_ADAPTER | CLAUDE_DEVELOPER_ADAPTER
    ) && reviewer_adapters.iter().all(|binding| {
        matches!(
            binding.adapter.as_str(),
            CODEX_REVIEWER_ADAPTER | CLAUDE_REVIEWER_ADAPTER
        )
    });
    if !(routed_worker_pair || retained_cli_pair) {
        bail!("architect bridge received an unknown worker adapter");
    }
    Ok(())
}

fn validate_activation(
    configuration: &BridgeConfiguration,
    activation: &BridgeActivation,
) -> Result<()> {
    if activation.architect_pid <= 1
        || activation.bridge_pid != std::process::id()
        || activation.binding_version == 0
    {
        bail!("architect bridge activation contains an invalid PID");
    }
    if process_birth_identity(activation.architect_pid)? != activation.architect_process_birth
        || process_birth_identity(activation.bridge_pid)? != activation.bridge_process_birth
    {
        bail!("architect bridge activation process identity drifted");
    }
    configuration.relay_executable.revalidate()?;
    if relay_runtime_scope_hash(
        configuration
            .relay_socket_path
            .parent()
            .expect("validated relay parent"),
    )? != configuration.relay_runtime_scope_hash
    {
        bail!("architect relay runtime scope drifted before activation");
    }
    Ok(())
}

fn serve_bridge(
    socket: RelaySocketGuard,
    configuration: BridgeConfiguration,
    activation: BridgeActivation,
) -> Result<()> {
    socket.listener.set_nonblocking(true)?;
    let binding_version = activation.binding_version;
    loop {
        if !matches!(
            process_is_live_identity(
                activation.architect_pid,
                &activation.architect_process_birth,
            ),
            Ok(true)
        ) {
            break;
        }
        let (mut stream, _) = match socket.listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(error) => {
                return Err(error).context("failed to accept architect relay connection");
            }
        };
        stream.set_nonblocking(false)?;
        configure_persistent_mcp_stream(&stream)?;
        if authorize_relay_peer(&stream, &configuration, &activation).is_err() {
            continue;
        }
        if let Err(_error) = serve_mcp_connection(&mut stream, &configuration) {
            continue;
        }
    }
    let close = RegistrationClient::new(&configuration.registration_socket_path).request(
        &RegistrationRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: format!("close-{}", Uuid::new_v4()),
            caller: RegistrationCaller::Bridge {
                binding_id: configuration.binding_id.clone(),
                launch_nonce: configuration.launch_nonce.clone(),
                capability: configuration.capability.clone(),
            },
            action: RegistrationAction::CloseBinding {
                binding_id: configuration.binding_id,
                expected_version: binding_version,
            },
        },
    )?;
    if !close.ok || close.binding_version != binding_version.checked_add(1) {
        bail!("architect binding close was not acknowledged");
    }
    Ok(())
}

fn configure_persistent_mcp_stream(stream: &UnixStream) -> Result<()> {
    // Human-owned architect input may remain idle for an unbounded time before
    // the first or next MCP request. EOF and owned-process cleanup terminate
    // the connection; human think time must not.
    stream.set_read_timeout(None)?;
    stream.set_write_timeout(Some(SOCKET_WRITE_TIMEOUT))?;
    Ok(())
}

fn authorize_relay_peer(
    stream: &UnixStream,
    configuration: &BridgeConfiguration,
    activation: &BridgeActivation,
) -> Result<()> {
    let peer = peer_credentials(stream)?;
    // SAFETY: geteuid has no preconditions.
    if peer.uid != unsafe { libc::geteuid() } {
        bail!("architect relay peer uid mismatch");
    }
    let peer_birth = process_birth_identity(peer.pid)?;
    if !process_has_ancestor(
        peer.pid,
        &[(
            activation.architect_pid,
            activation.architect_process_birth.clone(),
        )],
    )? {
        bail!("architect relay peer is outside the registered architect tree");
    }
    configuration.relay_executable.revalidate()?;
    let peer_executable = process_executable_identity(peer.pid)?;
    if !relay_executable_matches(&peer_executable, &configuration.relay_executable) {
        bail!("architect relay executable identity mismatch");
    }
    if process_birth_identity(peer.pid)? != peer_birth
        || process_birth_identity(activation.architect_pid)? != activation.architect_process_birth
        || process_birth_identity(activation.bridge_pid)? != activation.bridge_process_birth
    {
        bail!("architect relay process identity changed during authorization");
    }
    let final_peer_executable = process_executable_identity(peer.pid)?;
    if final_peer_executable != peer_executable
        || !relay_executable_matches(&final_peer_executable, &configuration.relay_executable)
    {
        bail!("architect relay executable identity changed during authorization");
    }
    configuration.relay_executable.revalidate()
}

fn relay_executable_matches(
    observed: &ProcessExecutableIdentity,
    expected: &ExecutableIdentity,
) -> bool {
    observed.device == expected.device
        && observed.inode == expected.inode
        && observed.size == expected.size
}

fn serve_mcp_connection(
    stream: &mut UnixStream,
    configuration: &BridgeConfiguration,
) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    let mut active_wait: Option<ActiveSessionWait> = None;
    while let Some(line) = read_bounded_line(&mut reader)? {
        reap_finished_wait(&mut active_wait);
        let request: JsonRpcRequest = match serde_json::from_slice(trim_line(&line)) {
            Ok(request) => request,
            Err(_) => {
                write_shared_json_line(
                    &writer,
                    &json_rpc_error(Value::Null, -32700, "parse error"),
                )?;
                continue;
            }
        };
        if request.jsonrpc != "2.0" || request.method.is_empty() || request.method.len() > 128 {
            if let Some(id) = request.id {
                write_shared_json_line(&writer, &json_rpc_error(id, -32600, "invalid request"))?;
            }
            continue;
        }
        let response = match request.method.as_str() {
            "initialize" => request.id.map(|id| {
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "result":{
                        "protocolVersion":MCP_PROTOCOL_VERSION,
                        "capabilities":{"tools":{"listChanged":false}},
                        "serverInfo":{"name":"hcom-session-task-control","version":"1"},
                        "instructions":architect_instructions_for_delivery(configuration.github_pr)
                    }
                })
            }),
            "notifications/initialized" => None,
            "notifications/cancelled" => {
                cancel_matching_wait(&mut active_wait, request.params.as_ref());
                None
            }
            "ping" => request
                .id
                .map(|id| json!({"jsonrpc":"2.0","id":id,"result":{}})),
            "tools/list" => request.id.map(|id| {
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                        "result":{"tools":tool_definitions_for_delivery(
                            &configuration.developer_adapter,
                            &configuration.reviewer_adapters,
                            configuration.github_pr,
                        )}
                })
            }),
            "tools/call" => {
                let Some(id) = request.id else {
                    continue;
                };
                match prepare_control_request(request.params, configuration) {
                    Ok(control_request)
                        if matches!(&control_request.action, ControlAction::SessionWait { .. }) =>
                    {
                        // A client may abandon an interrupted request without
                        // emitting notifications/cancelled. The next explicit
                        // wait replaces that stale subscription; it never
                        // changes the supervisor run itself.
                        if let Some(wait) = active_wait.take() {
                            wait.cancel_and_join();
                        }
                        match start_session_wait(
                            id.clone(),
                            control_request,
                            configuration,
                            Arc::clone(&writer),
                        ) {
                            Ok(wait) => {
                                active_wait = Some(wait);
                                None
                            }
                            Err(error) => {
                                Some(tool_call_error(id, &tool_call_refusal_text(&error)))
                            }
                        }
                    }
                    Ok(control_request) => Some(complete_tool_call(
                        id,
                        request_control(control_request, configuration),
                    )),
                    Err(error) => Some(tool_call_error(id, &tool_call_refusal_text(&error))),
                }
            }
            _ => request
                .id
                .map(|id| json_rpc_error(id, -32601, "method not found")),
        };
        if let Some(response) = response {
            write_shared_json_line(&writer, &response)?;
        }
    }
    if let Some(wait) = active_wait.take() {
        wait.cancel_and_join();
    }
    Ok(())
}

struct ActiveSessionWait {
    request_id: Value,
    cancellation_stream: UnixStream,
    cancelled: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl ActiveSessionWait {
    fn cancel_and_join(mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.cancellation_stream.shutdown(Shutdown::Both);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }

    fn join(mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn start_session_wait(
    id: Value,
    request: ControlRequest,
    configuration: &BridgeConfiguration,
    writer: Arc<Mutex<UnixStream>>,
) -> Result<ActiveSessionWait> {
    let pending = ControlClient::new(&configuration.control_socket_path)
        .begin_wait(&request)
        .map_err(|_| ToolCallRefusal(CONTROL_REFUSAL_TRANSPORT))?;
    let cancellation_stream = pending
        .cancellation_stream()
        .map_err(|_| ToolCallRefusal(CONTROL_REFUSAL_TRANSPORT))?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker_finished = Arc::clone(&finished);
    let worker_id = id.clone();
    let worker = std::thread::spawn(move || {
        let result = pending
            .wait()
            .map_err(|_| anyhow::Error::new(ToolCallRefusal(CONTROL_REFUSAL_TRANSPORT)));
        if !worker_cancelled.load(Ordering::Acquire) {
            let response = complete_tool_call(worker_id, result);
            let _ = write_shared_json_line(&writer, &response);
        }
        worker_finished.store(true, Ordering::Release);
    });
    Ok(ActiveSessionWait {
        request_id: id,
        cancellation_stream,
        cancelled,
        finished,
        worker: Some(worker),
    })
}

fn reap_finished_wait(active_wait: &mut Option<ActiveSessionWait>) {
    if active_wait
        .as_ref()
        .is_some_and(|wait| wait.finished.load(Ordering::Acquire))
        && let Some(wait) = active_wait.take()
    {
        wait.join();
    }
}

fn cancel_matching_wait(active_wait: &mut Option<ActiveSessionWait>, params: Option<&Value>) {
    let Some(cancelled_id) = params
        .and_then(Value::as_object)
        .and_then(|params| params.get("requestId"))
    else {
        return;
    };
    if active_wait
        .as_ref()
        .is_some_and(|wait| wait.request_id == *cancelled_id)
        && let Some(wait) = active_wait.take()
    {
        wait.cancel_and_join();
    }
}

#[cfg(test)]
fn handle_tool_call(
    id: Value,
    params: Option<Value>,
    configuration: &BridgeConfiguration,
) -> Value {
    let result = prepare_control_request(params, configuration)
        .and_then(|request| request_control(request, configuration));
    complete_tool_call(id, result)
}

fn prepare_control_request(
    params: Option<Value>,
    configuration: &BridgeConfiguration,
) -> Result<ControlRequest> {
    let params: ToolCallParams =
        serde_json::from_value(params.ok_or_else(|| anyhow::anyhow!("tool call omitted params"))?)
            .context("invalid typed tool call")
            .map_err(|_| ToolCallRefusal(TOOL_REFUSAL_ENVELOPE))?;
    let action = control_action_for_delivery(
        &params.name,
        params.arguments,
        &configuration.developer_adapter,
        &configuration.reviewer_adapters,
        configuration.github_pr,
    )
    .map_err(|_| ToolCallRefusal(TOOL_REFUSAL_ACTION))?;
    Ok(ControlRequest {
        protocol_version: PROTOCOL_VERSION,
        request_id: format!("architect-{}", Uuid::new_v4()),
        caller: CallerAuth::Architect {
            binding_id: configuration.binding_id.clone(),
            launch_nonce: configuration.launch_nonce.clone(),
            capability: configuration.capability.clone(),
        },
        action,
    })
}

fn request_control(
    request: ControlRequest,
    configuration: &BridgeConfiguration,
) -> Result<ControlResponse> {
    Ok(ControlClient::new(&configuration.control_socket_path)
        .request(&request)
        .map_err(|_| ToolCallRefusal(CONTROL_REFUSAL_TRANSPORT))?)
}

fn complete_tool_call(id: Value, result: Result<ControlResponse>) -> Value {
    let response_id = id.clone();
    let response = match result {
        Ok(response) => {
            let structured = serde_json::to_value(&response).unwrap_or_else(|_| json!({}));
            let text =
                serde_json::to_string(&structured).unwrap_or_else(|_| "{\"ok\":false}".to_owned());
            json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{
                    "content":[{"type":"text","text":text}],
                    "structuredContent":structured,
                    "isError":!response.ok
                }
            })
        }
        Err(error) => tool_call_error(id, &tool_call_refusal_text(&error)),
    };
    if serialized_json_line_len(&response).is_some_and(|len| len <= MAX_MCP_LINE_BYTES) {
        return response;
    }
    let bounded_error = tool_call_error(
        response_id,
        "architect control response exceeded the bounded MCP transport; retry with session_status or session_wait",
    );
    if serialized_json_line_len(&bounded_error).is_some_and(|len| len <= MAX_MCP_LINE_BYTES) {
        bounded_error
    } else {
        tool_call_error(
            Value::Null,
            "architect control response exceeded the bounded MCP transport",
        )
    }
}

fn tool_call_error(id: Value, message: &str) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id,
        "result":{
            "content":[{"type":"text","text":message}],
            "isError":true
        }
    })
}

fn tool_call_refusal_text(error: &anyhow::Error) -> String {
    match error.downcast_ref::<ToolCallRefusal>() {
        Some(error) => format!("architect control request was refused: {}", error.0),
        None => "architect control request was refused".into(),
    }
}

fn read_bootstrap(stream: &mut UnixStream) -> Result<BootstrapRequest> {
    let frame = read_request_frame(stream)?;
    serde_json::from_slice(&frame).context("bridge bootstrap request is malformed")
}

fn write_bootstrap(stream: &mut UnixStream, response: &BootstrapResponse) -> Result<()> {
    let frame = serde_json::to_vec(response)?;
    write_response_frame(stream, &frame)?;
    Ok(())
}

pub(super) fn configure_bridge(
    stream: &mut UnixStream,
    configuration: BridgeConfiguration,
) -> Result<()> {
    let request = serde_json::to_vec(&BootstrapRequest::Configure {
        configuration: Box::new(configuration),
    })?;
    write_request_frame(stream, &request)?;
    match read_bootstrap_response(stream)? {
        BootstrapResponse::Ready => Ok(()),
        _ => bail!("architect bridge refused its configuration"),
    }
}

pub(super) fn activate_bridge(stream: &mut UnixStream, activation: BridgeActivation) -> Result<()> {
    let request = serde_json::to_vec(&BootstrapRequest::Activate { activation })?;
    write_request_frame(stream, &request)?;
    match read_bootstrap_response(stream)? {
        BootstrapResponse::Active => Ok(()),
        _ => bail!("architect bridge refused its activation"),
    }
}

fn read_bootstrap_response(stream: &mut UnixStream) -> Result<BootstrapResponse> {
    let frame = read_response_frame(stream)?;
    serde_json::from_slice(&frame).context("bridge bootstrap response is malformed")
}

fn read_bounded_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let count = reader
        .take((MAX_MCP_LINE_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)?;
    if count == 0 {
        return Ok(None);
    }
    if line.len() > MAX_MCP_LINE_BYTES || !line.ends_with(b"\n") {
        bail!("MCP line exceeds its bounded newline-delimited shape");
    }
    Ok(Some(line))
}

fn trim_line(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn write_json_line(writer: &mut impl Write, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() + 1 > MAX_MCP_LINE_BYTES {
        bail!("MCP response exceeds its bounded shape");
    }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn serialized_json_line_len(value: &Value) -> Option<usize> {
    serde_json::to_vec(value).ok()?.len().checked_add(1)
}

fn write_shared_json_line(writer: &Mutex<UnixStream>, value: &Value) -> Result<()> {
    let mut writer = writer
        .lock()
        .map_err(|_| anyhow::anyhow!("architect MCP response writer is unavailable"))?;
    write_json_line(&mut *writer, value)
}

fn json_rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

fn validate_relay_socket_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some(RELAY_SOCKET_NAME)
    {
        bail!("architect relay socket path is invalid");
    }
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_socket()
        || metadata.uid() != uid
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        bail!("architect relay socket is not private");
    }
    relay_runtime_scope_hash(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("architect relay socket has no parent"))?,
    )?;
    Ok(())
}

fn validate_canonical_directory(label: &str, path: &Path, mode: Option<u32>) -> Result<()> {
    if !path.is_absolute() || fs::canonicalize(path)? != path {
        bail!("{label} must be an existing canonical directory");
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} must be a non-symlink directory");
    }
    if let Some(mode) = mode
        && metadata.permissions().mode() & 0o777 != mode
    {
        bail!("{label} mode is invalid");
    }
    Ok(())
}

fn validate_secret(value: &str) -> Result<()> {
    if !(16..=512).contains(&value.len())
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("architect bridge secret has an invalid shape");
    }
    Ok(())
}

struct RelaySocketGuard {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl RelaySocketGuard {
    fn bind(path: &Path) -> Result<Self> {
        if path.exists() {
            bail!("architect relay socket path already exists");
        }
        relay_runtime_scope_hash(
            path.parent()
                .ok_or_else(|| anyhow::anyhow!("architect relay socket has no parent"))?,
        )?;
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(path)?;
        // SAFETY: geteuid has no preconditions.
        if !metadata.file_type().is_socket()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            bail!("architect relay socket owner or mode is invalid");
        }
        Ok(Self {
            listener,
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for RelaySocketGuard {
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolCallParams {
    name: String,
    #[serde(default = "empty_object")]
    arguments: Value,
    #[serde(default, rename = "_meta")]
    _metadata: serde_json::Map<String, Value>,
}

fn empty_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::registration::{
        CONTROL_REFUSAL_TRANSPORT, TOOL_REFUSAL_ACTION, TOOL_REFUSAL_ENVELOPE,
    };
    use crate::control_api::{
        ActionName, ActiveWorkerSnapshot, ArchitectActionReason, ClarificationPage,
        ClarificationRecord, ControlAction, ControlErrorCode, ControlResult,
        MAX_CLARIFICATION_PAGE_RECORDS, ReviewerBindingSnapshot, ReviewerResultSnapshot,
        ReviewerVerdict, SessionProgressEvent, SessionState, SessionStatusSnapshot, TaskState,
        TaskStatusSnapshot,
    };
    use crate::worker::profile::ReviewerId;
    use crate::worker::runtime::WorkerLane;
    use std::process::{Command, Stdio};
    use std::thread::JoinHandle;
    use std::time::Instant;

    const RELAY_NAMESPACE_HELPER: &str = "HCOM_PHASE9_RELAY_NAMESPACE_HELPER";

    fn reviewer_adapters(reviewer1: &str, reviewer2: &str) -> Vec<ReviewerAdapterBinding> {
        vec![
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer1,
                adapter: reviewer1.into(),
            },
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer2,
                adapter: reviewer2.into(),
            },
        ]
    }

    struct BridgeTestFixture {
        _temp: tempfile::TempDir,
        configuration: BridgeConfiguration,
    }

    impl BridgeTestFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temp.path()).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let repo_root = root.join("repo");
            fs::create_dir(&repo_root).unwrap();
            let executable = ExecutableIdentity::capture(
                fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
            )
            .unwrap();
            Self {
                _temp: temp,
                configuration: BridgeConfiguration {
                    binding_id: "binding-session-test".into(),
                    launch_nonce: "launch-nonce-session-test".into(),
                    capability: "capability-session-test".into(),
                    project_root: repo_root,
                    run_root: root.clone(),
                    relay_socket_path: root.join(RELAY_SOCKET_NAME),
                    registration_socket_path: root.join("registration.sock"),
                    control_socket_path: root.join("control.sock"),
                    relay_executable: executable,
                    relay_runtime_scope_hash: relay_runtime_scope_hash(&root).unwrap(),
                    session_binding_hash: "a".repeat(64),
                    architect_adapter: "codex".into(),
                    architect_additional_directories: Vec::new(),
                    developer_adapter: "codex-developer".into(),
                    reviewer_adapters: reviewer_adapters(
                        "codex-reviewer",
                        "claude-reviewer-2.1.220",
                    ),
                    github_pr: false,
                },
            }
        }
    }

    #[test]
    fn bridge_binds_all_sixteen_architect_and_worker_provider_combinations() {
        let fixture = BridgeTestFixture::new();
        for architect in ["codex", "claude"] {
            for developer in [CODEX_TASK_WORKER_ADAPTER, CLAUDE_TASK_WORKER_ADAPTER] {
                for reviewer1 in [CODEX_TASK_WORKER_ADAPTER, CLAUDE_TASK_WORKER_ADAPTER] {
                    for reviewer2 in [CODEX_TASK_WORKER_ADAPTER, CLAUDE_TASK_WORKER_ADAPTER] {
                        let mut configuration = fixture.configuration.clone();
                        configuration.architect_adapter = architect.into();
                        configuration.developer_adapter = developer.into();
                        configuration.reviewer_adapters = reviewer_adapters(reviewer1, reviewer2);
                        validate_bridge_configuration(&configuration).unwrap();
                    }
                }
            }
        }
        validate_worker_adapter_binding(
            CODEX_DEVELOPER_ADAPTER,
            &reviewer_adapters(CODEX_REVIEWER_ADAPTER, CLAUDE_REVIEWER_ADAPTER),
        )
        .unwrap();
        validate_worker_adapter_binding(
            CODEX_DEVELOPER_ADAPTER,
            &[ReviewerAdapterBinding {
                reviewer_id: crate::worker::profile::ReviewerId::Reviewer1,
                adapter: CODEX_REVIEWER_ADAPTER.into(),
            }],
        )
        .unwrap();
        for invalid in [
            Vec::new(),
            vec![ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer2,
                adapter: CODEX_REVIEWER_ADAPTER.into(),
            }],
            vec![
                ReviewerAdapterBinding {
                    reviewer_id: ReviewerId::Reviewer1,
                    adapter: CODEX_REVIEWER_ADAPTER.into(),
                },
                ReviewerAdapterBinding {
                    reviewer_id: ReviewerId::Reviewer1,
                    adapter: CLAUDE_REVIEWER_ADAPTER.into(),
                },
            ],
            vec![
                ReviewerAdapterBinding {
                    reviewer_id: ReviewerId::Reviewer2,
                    adapter: CLAUDE_REVIEWER_ADAPTER.into(),
                },
                ReviewerAdapterBinding {
                    reviewer_id: ReviewerId::Reviewer1,
                    adapter: CODEX_REVIEWER_ADAPTER.into(),
                },
            ],
        ] {
            assert!(
                validate_worker_adapter_binding(CODEX_DEVELOPER_ADAPTER, &invalid).is_err(),
                "bridge accepted invalid Reviewer topology: {invalid:?}"
            );
        }
        assert!(
            validate_worker_adapter_binding(
                CODEX_TASK_WORKER_ADAPTER,
                &reviewer_adapters(CODEX_REVIEWER_ADAPTER, CLAUDE_REVIEWER_ADAPTER),
            )
            .is_err()
        );
        assert!(
            validate_worker_adapter_binding(
                CODEX_DEVELOPER_ADAPTER,
                &reviewer_adapters(CODEX_TASK_WORKER_ADAPTER, CLAUDE_TASK_WORKER_ADAPTER),
            )
            .is_err()
        );
    }

    fn bind_private_listener(path: &Path) -> UnixListener {
        let listener = UnixListener::bind(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        listener
    }

    fn spawn_control_server(
        listener: UnixListener,
        respond: impl FnOnce(&ControlRequest) -> ControlResponse + Send + 'static,
    ) -> JoinHandle<ControlRequest> {
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let frame = read_request_frame(&mut stream).unwrap();
            let request: ControlRequest = serde_json::from_slice(&frame).unwrap();
            let response = respond(&request);
            write_response_frame(&mut stream, &serde_json::to_vec(&response).unwrap()).unwrap();
            request
        })
    }

    fn status_snapshot() -> SessionStatusSnapshot {
        SessionStatusSnapshot {
            run_id: "run-test".into(),
            state: SessionState::AwaitingPlan,
            version: 0,
            project_root: "/project".into(),
            delivery_binding: Default::default(),
            github: None,
            plan_version: None,
            plan_hash: None,
            current_task_ordinal: None,
            active_workers: Vec::new(),
            reviewer_bindings: Vec::new(),
            pending_architect_action: None,
            terminal_detail: None,
            tasks: Vec::new(),
        }
    }

    fn reviewer_bindings() -> Vec<ReviewerBindingSnapshot> {
        vec![
            ReviewerBindingSnapshot {
                reviewer_id: ReviewerId::Reviewer1,
                provider: "codex-exec".into(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: "xhigh".into(),
                contract_sha256: "1".repeat(64),
            },
            ReviewerBindingSnapshot {
                reviewer_id: ReviewerId::Reviewer2,
                provider: "claude-exec".into(),
                model: "opus".into(),
                reasoning_effort: "xhigh".into(),
                contract_sha256: "2".repeat(64),
            },
        ]
    }

    fn reviewed_task_snapshot(reviewer_path: &Path) -> TaskStatusSnapshot {
        TaskStatusSnapshot {
            task_key: "reviewed-task".into(),
            ordinal: 0,
            state: TaskState::Lgtm,
            repository_root: "/source".into(),
            task_document_path: "/project/current_todo.md".into(),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: "FBTC-03".into(),
            branch: None,
            review_round: 1,
            review_generation: 1,
            max_review_rounds: 7,
            clarification_rounds_used: 0,
            max_clarification_rounds: 2,
            clarification_record_count: 0,
            base_revision: None,
            head_revision: None,
            github_reviews: Vec::new(),
            github_check: None,
            developer_session_bound: true,
            reviewers: [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
                .into_iter()
                .map(|reviewer_id| ReviewerResultSnapshot {
                    reviewer_id,
                    session_bound: true,
                    current_generation: Some(1),
                    current_verdict: Some(ReviewerVerdict::Lgtm),
                    current_final_message_paths: vec![reviewer_path.to_string_lossy().into_owned()],
                })
                .collect(),
            outcome_detail: Some("Reviewer returned LGTM".into()),
            latest_developer_final_path: Some("/artifacts/developer/native-final.partial".into()),
        }
    }

    fn maximum_dual_status_response(path_bytes: usize) -> ControlResponse {
        let path = format!("/{}", "\\".repeat(path_bytes.saturating_sub(1)));
        let reviewers = [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
            .into_iter()
            .map(|reviewer_id| ReviewerResultSnapshot {
                reviewer_id,
                session_bound: true,
                current_generation: Some(20),
                current_verdict: Some(ReviewerVerdict::RequestChanges),
                current_final_message_paths: vec![path.clone(), path.clone()],
            })
            .collect::<Vec<_>>();
        let tasks = (0..64)
            .map(|ordinal| TaskStatusSnapshot {
                task_key: format!("task-{ordinal}"),
                ordinal,
                state: TaskState::ReviewExhausted,
                repository_root: path.clone(),
                task_document_path: path.clone(),
                design_document_paths: vec![path.clone()],
                task_selector: path.clone(),
                branch: None,
                review_round: 20,
                review_generation: 20,
                max_review_rounds: 20,
                clarification_rounds_used: 20,
                max_clarification_rounds: 20,
                clarification_record_count: 64,
                base_revision: None,
                head_revision: None,
                github_reviews: Vec::new(),
                github_check: None,
                developer_session_bound: true,
                reviewers: reviewers.clone(),
                outcome_detail: Some(path.clone()),
                latest_developer_final_path: Some(path.clone()),
            })
            .collect();
        ControlResponse::success(
            "maximum-dual-review-status",
            ControlResult::Session {
                session: SessionStatusSnapshot {
                    run_id: "run-maximum".into(),
                    state: SessionState::Completed,
                    version: u64::MAX,
                    project_root: path.clone(),
                    delivery_binding: Default::default(),
                    github: None,
                    plan_version: Some(u64::MAX),
                    plan_hash: Some("f".repeat(64)),
                    current_task_ordinal: Some(63),
                    active_workers: Vec::new(),
                    reviewer_bindings: reviewer_bindings(),
                    pending_architect_action: None,
                    terminal_detail: Some(path),
                    tasks,
                },
            },
        )
    }

    fn maximum_github_dual_status_response(path_bytes: usize) -> ControlResponse {
        let mut response = maximum_dual_status_response(path_bytes);
        let Some(ControlResult::Session { session }) = response.result.as_mut() else {
            unreachable!()
        };
        let path = session.project_root.clone();
        let permissions = |values: &[(&str, crate::control_api::GitHubPermissionLevel)]| {
            values
                .iter()
                .map(|(name, level)| ((*name).to_owned(), *level))
                .collect()
        };
        let app = |id, slug: &str, permissions| crate::control_api::GitHubAppBinding {
            app_id: id,
            installation_id: id + 10,
            slug: slug.into(),
            bot_user_id: id + 20,
            effective_permissions: permissions,
        };
        let architect = app(
            1,
            "hcom-arch",
            permissions(&[
                (
                    "administration",
                    crate::control_api::GitHubPermissionLevel::Read,
                ),
                ("checks", crate::control_api::GitHubPermissionLevel::Write),
                ("contents", crate::control_api::GitHubPermissionLevel::Write),
                (
                    "pull_requests",
                    crate::control_api::GitHubPermissionLevel::Write,
                ),
            ]),
        );
        let developer = app(
            2,
            "hcom-dev",
            permissions(&[
                ("contents", crate::control_api::GitHubPermissionLevel::Write),
                (
                    "pull_requests",
                    crate::control_api::GitHubPermissionLevel::Write,
                ),
            ]),
        );
        let reviewer_apps = [
            (ReviewerId::Reviewer1, 3, "hcom-reviewer1"),
            (ReviewerId::Reviewer2, 4, "hcom-reviewer2"),
        ]
        .into_iter()
        .map(
            |(reviewer_id, id, slug)| crate::control_api::GitHubReviewerAppBinding {
                reviewer_id,
                app: app(
                    id,
                    slug,
                    permissions(&[(
                        "pull_requests",
                        crate::control_api::GitHubPermissionLevel::Write,
                    )]),
                ),
            },
        )
        .collect::<Vec<_>>();
        let branch = "hcom/run-maximum-0123456789ab".to_owned();
        let base_sha = "a".repeat(40);
        let head_sha = "b".repeat(40);
        let rules = "c".repeat(64);
        session.delivery_binding = crate::control_api::DeliveryBinding::GitHubPullRequest {
            binding: Box::new(crate::control_api::GitHubPullRequestBinding {
                delivery_policy: crate::control_api::GitHubDeliveryPolicy::ProtectedAutoMerge,
                owner: "owner".into(),
                repository: "repository".into(),
                repository_id: u64::MAX,
                visibility: "private".into(),
                local_repository_root: "/repository".into(),
                base_branch: "master".into(),
                merge_method: "squash".into(),
                merge_wait_seconds: 86_400,
                delete_remote_branch_after_merge: true,
                architect_app: architect,
                developer_app: developer,
                reviewer_apps,
                review_check_name: crate::control_api::GITHUB_REVIEW_CHECK_NAME.into(),
            }),
        };
        let run_binding = crate::control_api::GitHubRunBinding {
            inspected_repository_id: u64::MAX,
            expected_base_ref: "refs/heads/master".into(),
            expected_base_sha: base_sha.clone(),
            ruleset_attestation_sha256: Some(rules.clone()),
            inspection_id: "inspection-maximum".into(),
            generated_run_branch: branch.clone(),
        };
        let maximum_github_url = |kind: &str| {
            let prefix = format!("https://github.com/owner/repository/{kind}/");
            format!("{prefix}{}", "u".repeat(2048 - prefix.len()))
        };
        let check = crate::control_api::GitHubCheckSnapshot {
            check_run_id: u64::MAX,
            check_url: maximum_github_url("runs"),
            state: "action_required".into(),
            head_sha: head_sha.clone(),
        };
        session.github = Some(crate::control_api::GitHubDeliveryStatusSnapshot {
            latest_inspection: None,
            run_binding: Some(run_binding),
            worktree_path: Some(path),
            pr_number: Some(u64::MAX),
            pr_url: Some(maximum_github_url("pull")),
            published_head_sha: Some(head_sha.clone()),
            current_check: Some(check.clone()),
            phase: Some(crate::control_api::GitHubDeliveryPhase::PreservedUnmerged),
            outcome: Some(crate::control_api::GitHubDeliveryOutcome::UnmergedReviewExhausted),
            final_base_sha: Some(base_sha.clone()),
            final_ruleset_attestation_sha256: Some(rules),
            merge_already_confirmed: false,
            merge_sha: None,
            merge_url: None,
            finalization: None,
            preserved_branch: Some(branch.clone()),
            preserved_worktree: Some("/project/hcom-tasks/run-maximum/repository".into()),
        });
        for task in &mut session.tasks {
            task.branch = Some(branch.clone());
            task.base_revision = Some(base_sha.clone());
            task.head_revision = Some(head_sha.clone());
            task.github_reviews = [ReviewerId::Reviewer1, ReviewerId::Reviewer2]
                .into_iter()
                .map(|reviewer_id| crate::control_api::GitHubReviewSnapshot {
                    reviewer_id,
                    generation: 20,
                    head_sha: head_sha.clone(),
                    verdict: ReviewerVerdict::RequestChanges,
                    review_id: u64::MAX,
                    review_url: maximum_github_url("pull-request-review"),
                    final_artifact_sha256: "d".repeat(64),
                })
                .collect();
            task.github_check = Some(check.clone());
        }
        response
    }

    fn maximum_clarification_response() -> ControlResponse {
        let path = format!("/{}", "\\".repeat(4095));
        let records = (1..=u32::from(MAX_CLARIFICATION_PAGE_RECORDS))
            .map(|sequence| ClarificationRecord {
                sequence,
                reason: ArchitectActionReason::Clarification,
                developer_request_path: path.clone(),
                architect_clarification_path: path.clone(),
                human_decision_confirmed: true,
            })
            .collect();
        ControlResponse::success(
            "maximum-clarification-page",
            ControlResult::Clarifications {
                page: ClarificationPage {
                    run_id: "run-maximum".into(),
                    session_version: u64::MAX,
                    task_ordinal: 63,
                    task_key: "task-63".into(),
                    total_records: 64,
                    after_sequence: 56,
                    records,
                    next_after_sequence: None,
                },
            },
        )
    }

    fn successful_status_response(request: &ControlRequest) -> ControlResponse {
        ControlResponse::success(
            request.request_id.clone(),
            ControlResult::Session {
                session: status_snapshot(),
            },
        )
    }

    fn tool_refusal(response: &Value) -> &str {
        response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool refusal must be text")
    }

    #[test]
    fn relay_mount_namespace_helper_process() {
        let Some(socket) = std::env::var_os(RELAY_NAMESPACE_HELPER) else {
            return;
        };
        let mut stream = UnixStream::connect(PathBuf::from(socket)).unwrap();
        let mut release = [0u8; 1];
        stream.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    }

    #[test]
    fn relay_authorization_accepts_exact_executable_from_private_mount_namespace() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let socket_path = root.join("relay-test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();

        let executable_path = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let executable = ExecutableIdentity::capture(&executable_path).unwrap();
        let process_birth = process_birth_identity(std::process::id()).unwrap();
        let configuration = BridgeConfiguration {
            binding_id: "binding-namespace-test".into(),
            launch_nonce: "launch-nonce-namespace-test".into(),
            capability: "capability-namespace-test".into(),
            project_root: root.clone(),
            run_root: root.clone(),
            relay_socket_path: socket_path.clone(),
            registration_socket_path: root.join("registration.sock"),
            control_socket_path: root.join("control.sock"),
            relay_executable: executable,
            relay_runtime_scope_hash: "unused-by-authorization".into(),
            session_binding_hash: "a".repeat(64),
            architect_adapter: "codex".into(),
            architect_additional_directories: Vec::new(),
            developer_adapter: "codex-developer".into(),
            reviewer_adapters: reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
            github_pr: false,
        };
        let activation = BridgeActivation {
            architect_pid: std::process::id(),
            architect_process_birth: process_birth.clone(),
            bridge_pid: std::process::id(),
            bridge_process_birth: process_birth,
            binding_version: 1,
        };

        let inside_control = Path::new("/tmp/hcom-phase9-relay-control");
        let inside_socket = inside_control.join("relay-test.sock");
        let inside_executable = Path::new("/tmp/hcom-phase9-relay-executable");
        assert!(
            !inside_executable.exists(),
            "namespace-only executable alias unexpectedly exists on the host"
        );
        let mut child = Command::new(crate::worker::codex::BWRAP_EXECUTABLE);
        child
            .args([
                "--die-with-parent",
                "--ro-bind",
                "/",
                "/",
                "--tmpfs",
                "/tmp",
            ])
            .arg("--ro-bind")
            .arg(&root)
            .arg(inside_control)
            .arg("--ro-bind")
            .arg(&executable_path)
            .arg(inside_executable)
            .args(["--clearenv", "--setenv"])
            .arg(RELAY_NAMESPACE_HELPER)
            .arg(&inside_socket)
            .arg("--")
            .arg(inside_executable)
            .args([
                "--exact",
                "architect::bridge::tests::relay_mount_namespace_helper_process",
                "--nocapture",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = child.spawn().unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if let Some(status) = child.try_wait().unwrap() {
                        let output = child.wait_with_output().unwrap();
                        panic!(
                            "mount-namespace relay helper exited before connect: {status}\n{}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                    if Instant::now() >= deadline {
                        child.kill().unwrap();
                        let output = child.wait_with_output().unwrap();
                        panic!(
                            "mount-namespace relay helper timed out before connect\n{}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    child.kill().unwrap();
                    let output = child.wait_with_output().unwrap();
                    panic!(
                        "failed to accept mount-namespace relay helper: {error}\n{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        };

        let observation = (|| -> Result<PathBuf> {
            let peer = peer_credentials(&stream)?;
            let namespace_path = fs::read_link(format!("/proc/{}/exe", peer.pid))?;
            authorize_relay_peer(&stream, &configuration, &activation)?;
            Ok(namespace_path)
        })();

        let release = (&stream).write_all(&[1]);
        drop(stream);
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "mount-namespace relay helper failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        release.unwrap();
        let namespace_path = observation.unwrap();
        assert_eq!(namespace_path, inside_executable);
        assert!(
            fs::canonicalize(&namespace_path).is_err(),
            "namespace-only executable path unexpectedly resolves on the host"
        );
    }

    #[test]
    fn persistent_mcp_stream_has_no_human_idle_read_deadline() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(1)))
            .unwrap();
        configure_persistent_mcp_stream(&stream).unwrap();
        assert_eq!(stream.read_timeout().unwrap(), None);
        assert!(stream.write_timeout().unwrap().is_some());

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            peer.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}\n")
                .unwrap();
        });
        let mut reader = BufReader::new(stream);
        assert!(read_bounded_line(&mut reader).unwrap().is_some());
        writer.join().unwrap();
    }

    #[test]
    fn progress_result_reaches_both_mcp_response_representations_without_peer_body() {
        let response = ControlResponse::success(
            "progress-request",
            ControlResult::Progress {
                run_id: "run-test".into(),
                session_version: 8,
                event: SessionProgressEvent::ReviewResponded {
                    sequence: 2,
                    task_ordinal: 0,
                    task_key: "task-one".into(),
                    completed_tasks: 0,
                    total_tasks: 1,
                    review_round: 0,
                    review_generation: 1,
                    max_review_rounds: 7,
                    reviewer_id: ReviewerId::Reviewer1,
                    reviewer_verdict: ReviewerVerdict::RequestChanges,
                    developer_final_path: "/artifacts/developer/final.md".into(),
                    reviewer_final_message_paths: vec!["/artifacts/reviewer/final.md".into()],
                    responses_received: 1,
                    responses_expected: 2,
                    github: None,
                },
            },
        );
        let output = complete_tool_call(json!(17), Ok(response));
        let structured = &output["result"]["structuredContent"];
        assert_eq!(structured["result"]["kind"], "progress");
        assert_eq!(
            structured["result"]["event"]["developer_final_path"],
            "/artifacts/developer/final.md"
        );
        assert_eq!(
            structured["result"]["event"]["reviewer_final_message_paths"],
            json!(["/artifacts/reviewer/final.md"])
        );
        assert_eq!(structured["result"]["event"]["reviewer_id"], "reviewer1");
        assert_eq!(structured["result"]["event"]["review_round"], 0);
        assert_eq!(structured["result"]["event"]["review_generation"], 1);
        assert_eq!(structured["result"]["event"]["responses_received"], 1);
        assert_eq!(structured["result"]["event"]["responses_expected"], 2);
        let content: Value =
            serde_json::from_str(output["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(content, *structured);
        assert!(!output.to_string().contains("REVIEWER-BODY"));
    }

    #[test]
    fn status_result_preserves_v11_active_bindings_and_current_generation_without_peer_body() {
        let reviewer1_path = "/artifacts/reviewer/reviewer1/final.md";
        let mut session = status_snapshot();
        session.state = SessionState::Running;
        session.version = 4;
        session.current_task_ordinal = Some(0);
        session.active_workers = vec![
            ActiveWorkerSnapshot {
                task_ordinal: 0,
                task_key: "task-one".into(),
                worker_lane: WorkerLane::Reviewer(ReviewerId::Reviewer1),
                reviewer_id: Some(ReviewerId::Reviewer1),
                purpose: "initial_review".into(),
            },
            ActiveWorkerSnapshot {
                task_ordinal: 0,
                task_key: "task-one".into(),
                worker_lane: WorkerLane::Reviewer(ReviewerId::Reviewer2),
                reviewer_id: Some(ReviewerId::Reviewer2),
                purpose: "initial_review".into(),
            },
        ];
        session.reviewer_bindings = reviewer_bindings();
        session.tasks = vec![TaskStatusSnapshot {
            task_key: "task-one".into(),
            ordinal: 0,
            state: TaskState::Reviewing,
            repository_root: "/source".into(),
            task_document_path: "/project/task.md".into(),
            design_document_paths: vec!["/project/design.md".into()],
            task_selector: "TASK-ONE".into(),
            branch: None,
            review_round: 0,
            review_generation: 1,
            max_review_rounds: 7,
            clarification_rounds_used: 0,
            max_clarification_rounds: 2,
            clarification_record_count: 0,
            base_revision: None,
            head_revision: None,
            github_reviews: Vec::new(),
            github_check: None,
            developer_session_bound: true,
            reviewers: vec![
                ReviewerResultSnapshot {
                    reviewer_id: ReviewerId::Reviewer1,
                    session_bound: true,
                    current_generation: Some(1),
                    current_verdict: Some(ReviewerVerdict::Lgtm),
                    current_final_message_paths: vec![reviewer1_path.into()],
                },
                ReviewerResultSnapshot {
                    reviewer_id: ReviewerId::Reviewer2,
                    session_bound: true,
                    current_generation: None,
                    current_verdict: None,
                    current_final_message_paths: Vec::new(),
                },
            ],
            outcome_detail: None,
            latest_developer_final_path: Some("/artifacts/developer/final.md".into()),
        }];
        let response =
            ControlResponse::success("status-request", ControlResult::Session { session });
        let output = complete_tool_call(json!(18), Ok(response));
        let structured = &output["result"]["structuredContent"];
        assert_eq!(
            structured["result"]["session"]["active_workers"][0]["reviewer_id"],
            "reviewer1"
        );
        assert_eq!(
            structured["result"]["session"]["active_workers"][1]["reviewer_id"],
            "reviewer2"
        );
        assert_eq!(
            structured["result"]["session"]["reviewer_bindings"][0]["provider"],
            "codex-exec"
        );
        assert_eq!(
            structured["result"]["session"]["reviewer_bindings"][1]["provider"],
            "claude-exec"
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["review_round"],
            0
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["review_generation"],
            1
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][0]["current_final_message_paths"],
            json!([reviewer1_path])
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][1]["current_verdict"],
            Value::Null
        );
        let content: Value =
            serde_json::from_str(output["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(content, *structured);
        assert!(!output.to_string().contains("REVIEWER-BODY"));
    }

    #[test]
    fn session_wait_keeps_mcp_responsive_and_returns_terminal_result() {
        let fixture = BridgeTestFixture::new();
        let reviewer_path = fixture
            .configuration
            .run_root
            .join("reviewer/native-final.partial");
        fs::create_dir_all(reviewer_path.parent().unwrap()).unwrap();
        const REVIEWER_BODY: &str =
            "VERDICT: LGTM\n\nREVIEWER-BODY-MUST-REMAIN-IN-THE-DURABLE-FILE";
        fs::write(&reviewer_path, REVIEWER_BODY).unwrap();
        let reviewer_path_text = reviewer_path.to_string_lossy().into_owned();
        let response_reviewer_path = reviewer_path.clone();
        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let (request_ready_tx, request_ready_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let control_thread = spawn_control_server(control, move |request| {
            request_ready_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            let mut session = status_snapshot();
            session.state = SessionState::Completed;
            session.version = 9;
            session.terminal_detail = Some("all tasks completed".into());
            session.reviewer_bindings = reviewer_bindings();
            session.tasks = vec![reviewed_task_snapshot(&response_reviewer_path)];
            ControlResponse::success(&request.request_id, ControlResult::Session { session })
        });

        let (mut server, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let configuration = fixture.configuration.clone();
        let bridge_thread = std::thread::spawn(move || {
            serve_mcp_connection(&mut server, &configuration).unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        write_json_line(
            &mut client,
            &json!({
                "jsonrpc":"2.0",
                "id":20,
                "method":"tools/call",
                "params":{
                    "name":"session_wait",
                    "arguments":{
                        "run_id":"run-test",
                        "after_session_version":4,
                        "after_progress_sequence":0
                    }
                }
            }),
        )
        .unwrap();
        request_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        write_json_line(
            &mut client,
            &json!({"jsonrpc":"2.0","id":21,"method":"ping"}),
        )
        .unwrap();
        let ping: Value =
            serde_json::from_slice(trim_line(&read_bounded_line(&mut reader).unwrap().unwrap()))
                .unwrap();
        assert_eq!(ping["id"], 21);

        finish_tx.send(()).unwrap();
        let terminal: Value =
            serde_json::from_slice(trim_line(&read_bounded_line(&mut reader).unwrap().unwrap()))
                .unwrap();
        assert_eq!(terminal["id"], 20);
        assert_eq!(terminal["result"]["isError"], false);
        assert_eq!(
            terminal["result"]["structuredContent"]["result"]["session"]["state"],
            "completed"
        );
        assert_eq!(
            terminal["result"]["structuredContent"]["result"]["session"]["version"],
            9
        );
        let structured = &terminal["result"]["structuredContent"];
        assert_eq!(
            structured["result"]["session"]["reviewer_bindings"][0]["reviewer_id"],
            "reviewer1"
        );
        assert_eq!(
            structured["result"]["session"]["reviewer_bindings"][1]["reviewer_id"],
            "reviewer2"
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][0]["reviewer_id"],
            "reviewer1"
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][1]["reviewer_id"],
            "reviewer2"
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][0]["current_final_message_paths"],
            json!([reviewer_path_text.clone()])
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][0]["current_verdict"],
            "lgtm"
        );
        assert_eq!(
            structured["result"]["session"]["tasks"][0]["reviewers"][1]["current_final_message_paths"],
            json!([reviewer_path_text])
        );
        let content_text = terminal["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP compatibility content must be JSON text");
        let content: Value = serde_json::from_str(content_text).unwrap();
        assert_eq!(content, *structured);
        assert!(content_text.contains(reviewer_path.to_str().unwrap()));
        assert!(!content_text.contains(REVIEWER_BODY));
        assert!(!structured.to_string().contains(REVIEWER_BODY));
        assert_eq!(fs::read_to_string(&reviewer_path).unwrap(), REVIEWER_BODY);

        client.shutdown(Shutdown::Write).unwrap();
        bridge_thread.join().unwrap();
        let request = control_thread.join().unwrap();
        assert!(matches!(
            request.action,
            ControlAction::SessionWait {
                ref run_id,
                after_session_version: 4,
                after_progress_sequence: 0
            } if run_id == "run-test"
        ));
    }

    #[test]
    fn cancelling_session_wait_closes_only_its_control_subscription() {
        let fixture = BridgeTestFixture::new();
        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let (request_ready_tx, request_ready_rx) = std::sync::mpsc::channel();
        let control_thread = std::thread::spawn(move || {
            let (mut stream, _) = control.accept().unwrap();
            let frame = read_request_frame(&mut stream).unwrap();
            let request: ControlRequest = serde_json::from_slice(&frame).unwrap();
            request_ready_tx.send(()).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut probe = [0u8; 1];
            assert_eq!(stream.read(&mut probe).unwrap(), 0);
            request
        });

        let (mut server, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let configuration = fixture.configuration.clone();
        let bridge_thread = std::thread::spawn(move || {
            serve_mcp_connection(&mut server, &configuration).unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        write_json_line(
            &mut client,
            &json!({
                "jsonrpc":"2.0",
                "id":30,
                "method":"tools/call",
                "params":{
                    "name":"session_wait",
                    "arguments":{
                        "run_id":"run-test",
                        "after_session_version":6,
                        "after_progress_sequence":0
                    }
                }
            }),
        )
        .unwrap();
        request_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        write_json_line(
            &mut client,
            &json!({"jsonrpc":"2.0","id":31,"method":"ping"}),
        )
        .unwrap();
        let first_ping: Value =
            serde_json::from_slice(trim_line(&read_bounded_line(&mut reader).unwrap().unwrap()))
                .unwrap();
        assert_eq!(first_ping["id"], 31);

        write_json_line(
            &mut client,
            &json!({
                "jsonrpc":"2.0",
                "method":"notifications/cancelled",
                "params":{"requestId":30,"reason":"architect turn interrupted"}
            }),
        )
        .unwrap();
        let request = control_thread.join().unwrap();
        assert!(matches!(
            request.action,
            ControlAction::SessionWait {
                ref run_id,
                after_session_version: 6,
                after_progress_sequence: 0
            } if run_id == "run-test"
        ));

        write_json_line(
            &mut client,
            &json!({"jsonrpc":"2.0","id":32,"method":"ping"}),
        )
        .unwrap();
        let second_ping: Value =
            serde_json::from_slice(trim_line(&read_bounded_line(&mut reader).unwrap().unwrap()))
                .unwrap();
        assert_eq!(second_ping["id"], 32);
        client.shutdown(Shutdown::Write).unwrap();
        bridge_thread.join().unwrap();
    }

    #[test]
    fn a_new_session_wait_replaces_an_abandoned_subscription() {
        let fixture = BridgeTestFixture::new();
        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let (first_ready_tx, first_ready_rx) = std::sync::mpsc::channel();
        let control_thread = std::thread::spawn(move || {
            let (mut first_stream, _) = control.accept().unwrap();
            let first_frame = read_request_frame(&mut first_stream).unwrap();
            let first_request: ControlRequest = serde_json::from_slice(&first_frame).unwrap();
            first_ready_tx.send(()).unwrap();
            first_stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut probe = [0u8; 1];
            assert_eq!(first_stream.read(&mut probe).unwrap(), 0);

            let (mut second_stream, _) = control.accept().unwrap();
            let second_frame = read_request_frame(&mut second_stream).unwrap();
            let second_request: ControlRequest = serde_json::from_slice(&second_frame).unwrap();
            let mut session = status_snapshot();
            session.state = SessionState::Completed;
            session.version = 13;
            session.terminal_detail = Some("completed while the first wait was abandoned".into());
            let response = ControlResponse::success(
                &second_request.request_id,
                ControlResult::Session { session },
            );
            write_response_frame(&mut second_stream, &serde_json::to_vec(&response).unwrap())
                .unwrap();
            (first_request, second_request)
        });

        let (mut server, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let configuration = fixture.configuration.clone();
        let bridge_thread = std::thread::spawn(move || {
            serve_mcp_connection(&mut server, &configuration).unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        for id in [40, 41] {
            write_json_line(
                &mut client,
                &json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "method":"tools/call",
                    "params":{
                        "name":"session_wait",
                        "arguments":{
                            "run_id":"run-test",
                            "after_session_version":8,
                            "after_progress_sequence":0
                        }
                    }
                }),
            )
            .unwrap();
            if id == 40 {
                first_ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            }
        }

        let terminal: Value =
            serde_json::from_slice(trim_line(&read_bounded_line(&mut reader).unwrap().unwrap()))
                .unwrap();
        assert_eq!(terminal["id"], 41);
        assert_eq!(terminal["result"]["isError"], false);
        assert_eq!(
            terminal["result"]["structuredContent"]["result"]["session"]["state"],
            "completed"
        );
        assert_eq!(
            terminal["result"]["structuredContent"]["result"]["session"]["version"],
            13
        );

        client.shutdown(Shutdown::Write).unwrap();
        bridge_thread.join().unwrap();
        let (first_request, second_request) = control_thread.join().unwrap();
        for request in [first_request, second_request] {
            assert!(matches!(
                request.action,
                ControlAction::SessionWait {
                    ref run_id,
                    after_session_version: 8,
                    after_progress_sequence: 0
                } if run_id == "run-test"
            ));
        }
    }

    #[test]
    fn tool_refusal_exposes_only_closed_stage_codes() {
        for code in [
            TOOL_REFUSAL_ENVELOPE,
            TOOL_REFUSAL_ACTION,
            CONTROL_REFUSAL_TRANSPORT,
        ] {
            let error = anyhow::Error::new(ToolCallRefusal(code));
            assert_eq!(
                tool_call_refusal_text(&error),
                format!("architect control request was refused: {code}")
            );
        }
        assert_eq!(
            tool_call_refusal_text(&anyhow::anyhow!("must-not-echo-value")),
            "architect control request was refused"
        );
    }

    #[test]
    fn tool_call_params_accept_reserved_mcp_metadata_only() {
        let params: ToolCallParams = serde_json::from_value(json!({
            "name": "session_status",
            "arguments": {},
            "_meta": {
                "progressToken": 2,
                "vendor.extension": {"opaque": true}
            }
        }))
        .unwrap();
        assert_eq!(params.name, "session_status");
        assert_eq!(params.arguments, json!({}));
        assert_eq!(params._metadata.len(), 2);

        assert!(
            serde_json::from_value::<ToolCallParams>(json!({
                "name": "session_status",
                "arguments": {},
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ToolCallParams>(json!({
                "name": "session_status",
                "arguments": {},
                "_meta": "not-an-object"
            }))
            .is_err()
        );
    }

    #[test]
    fn exact_codex_mcp_fixture_reaches_private_control_socket() {
        let fixture = BridgeTestFixture::new();
        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let control_thread = spawn_control_server(control, successful_status_response);

        let (mut server, mut client) = UnixStream::pair().unwrap();
        let configuration = fixture.configuration.clone();
        let bridge_thread = std::thread::spawn(move || {
            serve_mcp_connection(&mut server, &configuration).unwrap();
        });
        for request in [
            json!({
                "jsonrpc":"2.0",
                "id":0,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"codex-mcp-client","version":"test-cli"}
                }
            }),
            json!({
                "jsonrpc":"2.0",
                "method":"notifications/initialized"
            }),
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"tools/list",
                "params":{"_meta":{"progressToken":0}}
            }),
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"tools/call",
                "params":{
                    "name":"session_plan_replace",
                    "arguments":{
                        "expected_session_version":0,
                        "developer_adapter":"codex-developer",
                        "reviewer_adapters":[
                            {"reviewer_id":"reviewer1","adapter":"codex-reviewer"},
                            {"reviewer_id":"reviewer2","adapter":"claude-reviewer-2.1.220"}
                        ],
                        "tasks":[{
                            "task_key":"p9-task-1",
                            "title":"Phase 9 Task 1",
                            "repository_root":fixture.configuration.project_root,
                            "task_document_path":"/project/current_todo.md",
                            "design_document_paths":["/project/architecture.md"],
                            "task_selector":"FBTC-01",
                            "max_review_rounds":7,
                            "max_clarification_rounds":2
                        }]
                    },
                    "_meta":{"progressToken":2}
                }
            }),
        ] {
            write_json_line(&mut client, &request).unwrap();
        }
        client.shutdown(Shutdown::Write).unwrap();
        let mut output = String::new();
        client.read_to_string(&mut output).unwrap();
        let responses: Vec<Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["id"], 0);
        assert_eq!(
            responses[0]["result"]["protocolVersion"],
            MCP_PROTOCOL_VERSION
        );
        assert_eq!(
            responses[0]["result"]["instructions"],
            ARCHITECT_INSTRUCTIONS
        );
        let initialize_instructions = responses[0]["result"]["instructions"].as_str().unwrap();
        for required in [
            "Reviewer1 in single-review mode",
            "review_generation",
            "responses_received",
            "responses_received` is less than `responses_expected",
            "Only after terminal",
        ] {
            assert!(
                initialize_instructions.contains(required),
                "initialize instructions omitted {required}"
            );
        }
        assert_eq!(responses[1]["id"], 1);
        assert_eq!(
            responses[1]["result"]["tools"].as_array().unwrap().len(),
            ActionName::ARCHITECT.len()
        );
        assert_eq!(responses[2]["id"], 2);
        assert_eq!(responses[2]["result"]["isError"], false);
        assert_eq!(
            responses[2]["result"]["structuredContent"]["ok"],
            Value::Bool(true)
        );

        bridge_thread.join().unwrap();
        let control_request = control_thread.join().unwrap();
        assert!(matches!(
            control_request.action,
            ControlAction::SessionPlanReplace {
                expected_session_version: 0,
                ref developer_adapter,
                reviewer_adapters: ref requested_reviewers,
                ref tasks,
                ..
            } if developer_adapter == "codex-developer"
                && requested_reviewers == &reviewer_adapters(
                    "codex-reviewer",
                    "claude-reviewer-2.1.220",
                )
                && tasks.len() == 1
                && tasks[0].task_document_path == "/project/current_todo.md"
                && tasks[0].design_document_paths == ["/project/architecture.md"]
                && tasks[0].task_selector == "FBTC-01"
        ));
        assert!(matches!(
            control_request.caller,
            CallerAuth::Architect { .. }
        ));

        fs::remove_file(&fixture.configuration.control_socket_path).unwrap();
        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let control_thread = spawn_control_server(control, successful_status_response);
        let response = handle_tool_call(
            json!(3),
            Some(json!({
                "name":"session_status",
                "arguments":{},
                "_meta":{"progressToken":"same-session"}
            })),
            &fixture.configuration,
        );
        assert_eq!(response["result"]["isError"], false);
        assert!(matches!(
            control_thread.join().unwrap().action,
            ControlAction::SessionStatus
        ));
    }

    #[test]
    fn tool_call_failure_stages_are_distinct_before_control_transport() {
        let fixture = BridgeTestFixture::new();
        let envelope = handle_tool_call(
            json!(1),
            Some(json!({
                "name":"session_status",
                "arguments":{},
                "unexpected":true
            })),
            &fixture.configuration,
        );
        assert_eq!(
            tool_refusal(&envelope),
            "architect control request was refused: architect_tool_envelope"
        );
        let action = handle_tool_call(
            json!(2),
            Some(json!({"name":"not-a-tool","arguments":{}})),
            &fixture.configuration,
        );
        assert_eq!(
            tool_refusal(&action),
            "architect control request was refused: architect_tool_action"
        );
    }

    #[test]
    fn control_transport_recovers_and_structured_control_errors_pass_through() {
        let fixture = BridgeTestFixture::new();
        let response = handle_tool_call(
            json!(1),
            Some(json!({"name":"session_status","arguments":{}})),
            &fixture.configuration,
        );
        assert_eq!(
            tool_refusal(&response),
            "architect control request was refused: architect_control_transport"
        );

        let control = bind_private_listener(&fixture.configuration.control_socket_path);
        let control_thread = spawn_control_server(control, successful_status_response);
        let response = handle_tool_call(
            json!(2),
            Some(json!({"name":"session_status","arguments":{}})),
            &fixture.configuration,
        );
        assert_eq!(response["result"]["isError"], false);
        control_thread.join().unwrap();

        let error = BridgeTestFixture::new();
        let control = bind_private_listener(&error.configuration.control_socket_path);
        let control_thread = spawn_control_server(control, |request| {
            ControlResponse::error(
                request.request_id.clone(),
                ControlErrorCode::Conflict,
                "expected test conflict",
            )
        });
        let response = handle_tool_call(
            json!(3),
            Some(json!({"name":"session_status","arguments":{}})),
            &error.configuration,
        );
        assert_eq!(response["result"]["isError"], true);
        assert_eq!(
            response["result"]["structuredContent"]["error"]["code"],
            "conflict"
        );
        assert_eq!(
            response["result"]["structuredContent"]["error"]["message"],
            "expected test conflict"
        );
        control_thread.join().unwrap();
    }

    #[test]
    fn bounded_line_reader_rejects_partial_or_oversize_frames() {
        let mut valid = BufReader::new(b"{\"jsonrpc\":\"2.0\"}\n".as_slice());
        assert!(read_bounded_line(&mut valid).unwrap().is_some());
        assert!(read_bounded_line(&mut valid).unwrap().is_none());

        let mut partial = BufReader::new(b"{}".as_slice());
        assert!(read_bounded_line(&mut partial).is_err());
        let oversized = vec![b'x'; MAX_MCP_LINE_BYTES + 1];
        let mut oversized = BufReader::new(oversized.as_slice());
        assert!(read_bounded_line(&mut oversized).is_err());
    }

    #[test]
    fn maximum_control_responses_fit_losslessly_in_the_duplicated_mcp_envelope() {
        assert_eq!(
            MAX_MCP_LINE_BYTES,
            MAX_RESPONSE_BYTES * 3 + MAX_REQUEST_BYTES + 4096
        );
        let dual_status = maximum_dual_status_response(4096);
        let dual_payload = serde_json::to_vec(&dual_status).unwrap();
        assert!(
            dual_payload.len() > MAX_REQUEST_BYTES,
            "maximum legal dual status did not exceed the narrower request frame: {}",
            dual_payload.len()
        );
        assert!(
            dual_payload.len() <= MAX_RESPONSE_BYTES,
            "maximum legal dual status exceeded the response frame: {}",
            dual_payload.len()
        );

        let github_dual_status = maximum_github_dual_status_response(4096);
        let github_payload = serde_json::to_vec(&github_dual_status).unwrap();
        assert!(
            github_payload.len() > dual_payload.len(),
            "maximum legal GitHub status did not add its bounded URL evidence: {}",
            github_payload.len()
        );
        assert!(
            github_payload.len() <= MAX_RESPONSE_BYTES,
            "maximum legal GitHub dual status exceeded the response frame: {}",
            github_payload.len()
        );
        assert!(
            github_payload.len() <= MAX_RESPONSE_BYTES * 3 / 4,
            "maximum legal GitHub dual status left insufficient frame margin: {}",
            github_payload.len()
        );
        let Some(ControlResult::Session { session }) = github_dual_status.result.as_ref() else {
            unreachable!()
        };
        assert_eq!(
            session
                .github
                .as_ref()
                .unwrap()
                .pr_url
                .as_ref()
                .unwrap()
                .len(),
            2048
        );
        assert!(session.tasks.iter().all(|task| {
            task.github_check
                .as_ref()
                .is_some_and(|check| check.check_url.len() == 2048)
                && task
                    .github_reviews
                    .iter()
                    .all(|review| review.review_url.len() == 2048)
        }));

        for response in [
            dual_status,
            github_dual_status,
            maximum_clarification_response(),
        ] {
            response.validate().unwrap();
            let control_payload = serde_json::to_vec(&response).unwrap();
            assert!(control_payload.len() <= MAX_RESPONSE_BYTES);
            let envelope = complete_tool_call(json!(u64::MAX), Ok(response));
            assert_eq!(envelope["result"]["isError"], false);
            let structured = &envelope["result"]["structuredContent"];
            let compatibility: Value = serde_json::from_str(
                envelope["result"]["content"][0]["text"]
                    .as_str()
                    .expect("compatibility content is text"),
            )
            .unwrap();
            assert_eq!(&compatibility, structured);
            assert!(
                serialized_json_line_len(&envelope).unwrap() <= MAX_MCP_LINE_BYTES,
                "valid control response exceeded the checked MCP line bound"
            );

            let mut encoded = Vec::new();
            write_json_line(&mut encoded, &envelope).unwrap();
            let mut reader = BufReader::new(encoded.as_slice());
            let delivered = read_bounded_line(&mut reader).unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(trim_line(&delivered)).unwrap(),
                envelope
            );
        }

        let mut impossible = maximum_dual_status_response(1);
        let Some(ControlResult::Session { session }) = impossible.result.as_mut() else {
            unreachable!()
        };
        session.terminal_detail = Some("x".repeat(MAX_MCP_LINE_BYTES));
        let rejected = complete_tool_call(json!(7), Ok(impossible));
        assert_eq!(rejected["result"]["isError"], true);
        assert!(
            rejected["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("exceeded the bounded MCP transport")
        );
    }

    #[test]
    fn bridge_configuration_binds_runtime_only_socket_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let run_root = root.join("run");
        let repo_root = root.join("repo");
        let relay_root = root.join("relay");
        for path in [&run_root, &repo_root, &relay_root] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let executable = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        let configuration = BridgeConfiguration {
            binding_id: "binding-session-test".into(),
            launch_nonce: "launch-nonce-session-test".into(),
            capability: "capability-session-test".into(),
            project_root: repo_root,
            run_root: run_root.clone(),
            relay_socket_path: relay_root.join(RELAY_SOCKET_NAME),
            registration_socket_path: run_root.join("registration.sock"),
            control_socket_path: run_root.join("control.sock"),
            relay_executable: executable,
            relay_runtime_scope_hash: relay_runtime_scope_hash(&relay_root).unwrap(),
            session_binding_hash: "a".repeat(64),
            architect_adapter: "codex".into(),
            architect_additional_directories: Vec::new(),
            developer_adapter: "codex-developer".into(),
            reviewer_adapters: reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
            github_pr: false,
        };
        validate_bridge_configuration(&configuration).unwrap();

        let mut alternate_roles = configuration.clone();
        alternate_roles.developer_adapter = CLAUDE_DEVELOPER_ADAPTER.into();
        alternate_roles.reviewer_adapters =
            reviewer_adapters(CLAUDE_REVIEWER_ADAPTER, CODEX_REVIEWER_ADAPTER);
        validate_bridge_configuration(&alternate_roles).unwrap();

        let mut invalid_binding_hash = configuration.clone();
        invalid_binding_hash.session_binding_hash = "not-a-sha256".into();
        assert!(validate_bridge_configuration(&invalid_binding_hash).is_err());

        let mut drifted = configuration.clone();
        drifted.control_socket_path = root.join("other.sock");
        assert!(validate_bridge_configuration(&drifted).is_err());

        let external = root.join("external");
        fs::create_dir(&external).unwrap();
        let mut claude = configuration.clone();
        claude.architect_adapter = "claude".into();
        claude.architect_additional_directories = vec![external.clone()];
        validate_bridge_configuration(&claude).unwrap();

        let mut duplicate = claude.clone();
        duplicate.architect_additional_directories.push(external);
        assert!(validate_bridge_configuration(&duplicate).is_err());

        let mut codex_with_claude_root = configuration;
        codex_with_claude_root.architect_additional_directories =
            duplicate.architect_additional_directories;
        assert!(validate_bridge_configuration(&codex_with_claude_root).is_err());
    }
}
