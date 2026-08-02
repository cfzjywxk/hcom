use super::tools::{control_action, tool_definitions};
use crate::control_api::client::ControlClient;
use crate::control_api::codec::{
    read_request_frame, read_response_frame, write_request_frame, write_response_frame,
};
use crate::control_api::peer::{
    ProcessExecutableIdentity, peer_credentials, process_birth_identity,
    process_executable_identity, process_has_ancestor, process_is_live_identity,
};
use crate::control_api::protocol::PROTOCOL_VERSION;
use crate::control_api::registration::{
    CONTROL_REFUSAL_TRANSPORT, RegistrationAction, RegistrationCaller, RegistrationClient,
    RegistrationRequest, TOOL_REFUSAL_ACTION, TOOL_REFUSAL_ENVELOPE,
};
use crate::control_api::supervisor::ControlPaths;
use crate::control_api::{CallerAuth, ControlRequest, ControlResponse};
use crate::worker::ExecutableIdentity;
use crate::worker::profile::{
    CLAUDE_DEVELOPER_ADAPTER, CLAUDE_REVIEWER_ADAPTER, CODEX_DEVELOPER_ADAPTER,
    CODEX_REVIEWER_ADAPTER,
};
use crate::worker::runtime::CODEX_TASK_WORKER_ADAPTER;
use crate::worker::validation::validate_opaque_id;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const MAX_MCP_LINE_BYTES: usize = 256 * 1024;
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
    pub developer_adapter: String,
    pub reviewer_adapter: String,
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
    validate_worker_adapter_binding(
        &configuration.developer_adapter,
        &configuration.reviewer_adapter,
    )?;
    let paths = ControlPaths::new(&configuration.run_root)?;
    if paths.socket_path() != configuration.control_socket_path
        || paths.registration_socket_path() != configuration.registration_socket_path
    {
        bail!("architect bridge session socket paths drifted");
    }
    Ok(())
}

fn validate_worker_adapter_binding(developer_adapter: &str, reviewer_adapter: &str) -> Result<()> {
    let exec_worker_pair = developer_adapter == CODEX_TASK_WORKER_ADAPTER
        && reviewer_adapter == CODEX_TASK_WORKER_ADAPTER;
    let retained_cli_pair = matches!(
        developer_adapter,
        CODEX_DEVELOPER_ADAPTER | CLAUDE_DEVELOPER_ADAPTER
    ) && matches!(
        reviewer_adapter,
        CODEX_REVIEWER_ADAPTER | CLAUDE_REVIEWER_ADAPTER
    );
    if !(exec_worker_pair || retained_cli_pair) {
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
    while let Some(line) = read_bounded_line(&mut reader)? {
        let request: JsonRpcRequest = match serde_json::from_slice(trim_line(&line)) {
            Ok(request) => request,
            Err(_) => {
                write_json_line(stream, &json_rpc_error(Value::Null, -32700, "parse error"))?;
                continue;
            }
        };
        if request.jsonrpc != "2.0" || request.method.is_empty() || request.method.len() > 128 {
            if let Some(id) = request.id {
                write_json_line(stream, &json_rpc_error(id, -32600, "invalid request"))?;
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
                        "serverInfo":{"name":"hcom-session-task-control","version":"1"}
                    }
                })
            }),
            "notifications/initialized" | "notifications/cancelled" => None,
            "ping" => request
                .id
                .map(|id| json!({"jsonrpc":"2.0","id":id,"result":{}})),
            "tools/list" => request.id.map(|id| {
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                        "result":{"tools":tool_definitions(
                            &configuration.developer_adapter,
                            &configuration.reviewer_adapter,
                        )}
                })
            }),
            "tools/call" => {
                let Some(id) = request.id else {
                    continue;
                };
                Some(handle_tool_call(id, request.params, configuration))
            }
            _ => request
                .id
                .map(|id| json_rpc_error(id, -32601, "method not found")),
        };
        if let Some(response) = response {
            write_json_line(stream, &response)?;
        }
    }
    Ok(())
}

fn handle_tool_call(
    id: Value,
    params: Option<Value>,
    configuration: &BridgeConfiguration,
) -> Value {
    let result = (|| -> Result<ControlResponse> {
        let params: ToolCallParams = serde_json::from_value(
            params.ok_or_else(|| anyhow::anyhow!("tool call omitted params"))?,
        )
        .context("invalid typed tool call")
        .map_err(|_| ToolCallRefusal(TOOL_REFUSAL_ENVELOPE))?;
        let action = control_action(
            &params.name,
            params.arguments,
            &configuration.developer_adapter,
            &configuration.reviewer_adapter,
        )
        .map_err(|_| ToolCallRefusal(TOOL_REFUSAL_ACTION))?;
        let request = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: format!("architect-{}", Uuid::new_v4()),
            caller: CallerAuth::Architect {
                binding_id: configuration.binding_id.clone(),
                launch_nonce: configuration.launch_nonce.clone(),
                capability: configuration.capability.clone(),
            },
            action,
        };
        Ok(ControlClient::new(&configuration.control_socket_path)
            .request(&request)
            .map_err(|_| ToolCallRefusal(CONTROL_REFUSAL_TRANSPORT))?)
    })();

    match result {
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
        Err(error) => json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":{
                "content":[{"type":"text","text":tool_call_refusal_text(&error)}],
                "isError":true
            }
        }),
    }
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
        ActionName, ControlAction, ControlErrorCode, ControlResult, SessionState,
        SessionStatusSnapshot,
    };
    use std::net::Shutdown;
    use std::process::{Command, Stdio};
    use std::thread::JoinHandle;
    use std::time::Instant;

    const RELAY_NAMESPACE_HELPER: &str = "HCOM_PHASE9_RELAY_NAMESPACE_HELPER";
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
                    developer_adapter: "codex-developer".into(),
                    reviewer_adapter: "claude-reviewer-2.1.220".into(),
                },
            }
        }
    }

    #[test]
    fn bridge_accepts_exact_exec_worker_pair_without_mixing_runtime_families() {
        validate_worker_adapter_binding(CODEX_TASK_WORKER_ADAPTER, CODEX_TASK_WORKER_ADAPTER)
            .unwrap();
        validate_worker_adapter_binding(CODEX_DEVELOPER_ADAPTER, CLAUDE_REVIEWER_ADAPTER).unwrap();
        assert!(
            validate_worker_adapter_binding(CODEX_TASK_WORKER_ADAPTER, CODEX_REVIEWER_ADAPTER,)
                .is_err()
        );
        assert!(
            validate_worker_adapter_binding(CODEX_DEVELOPER_ADAPTER, CODEX_TASK_WORKER_ADAPTER,)
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
            plan_version: None,
            plan_hash: None,
            current_task_ordinal: None,
            terminal_detail: None,
            tasks: Vec::new(),
        }
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
            developer_adapter: "codex-developer".into(),
            reviewer_adapter: "claude-reviewer-2.1.220".into(),
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
                        "reviewer_adapter":"claude-reviewer-2.1.220",
                        "tasks":[{
                            "task_key":"p9-task-1",
                            "title":"Phase 9 Task 1",
                            "objective":"Create task1.txt with exactly two lines:\nphase9-task-1\nreview-stage: pending",
                            "repository_root":fixture.configuration.project_root,
                            "acceptance_criteria":["first review requests changes"],
                            "required_checks":["/usr/bin/test -f task1.txt"],
                            "allowed_paths":["README.md","task1.txt"],
                            "forbidden_actions":["push"],
                            "max_review_rounds":3
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
                ref reviewer_adapter,
                ref tasks,
            } if developer_adapter == "codex-developer"
                && reviewer_adapter == "claude-reviewer-2.1.220"
                && tasks.len() == 1
                && tasks[0].objective
                    == "Create task1.txt with exactly two lines:\nphase9-task-1\nreview-stage: pending"
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
            developer_adapter: "codex-developer".into(),
            reviewer_adapter: "claude-reviewer-2.1.220".into(),
        };
        validate_bridge_configuration(&configuration).unwrap();

        let mut alternate_roles = configuration.clone();
        alternate_roles.developer_adapter = CLAUDE_DEVELOPER_ADAPTER.into();
        alternate_roles.reviewer_adapter = CODEX_REVIEWER_ADAPTER.into();
        validate_bridge_configuration(&alternate_roles).unwrap();

        let mut drifted = configuration.clone();
        drifted.control_socket_path = root.join("other.sock");
        assert!(validate_bridge_configuration(&drifted).is_err());
    }
}
