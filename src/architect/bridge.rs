use super::tools::{control_action, tool_definitions};
use crate::control_api::client::ControlClient;
use crate::control_api::codec::{
    read_request_frame, read_response_frame, write_request_frame, write_response_frame,
};
use crate::control_api::daemon::ControlPaths;
use crate::control_api::peer::{
    peer_credentials, process_birth_identity, process_executable_path, process_has_ancestor,
    process_is_live_identity,
};
use crate::control_api::protocol::PROTOCOL_VERSION;
use crate::control_api::registration::{
    RegistrationAction, RegistrationCaller, RegistrationClient, RegistrationRequest,
};
use crate::control_api::{CallerAuth, ControlRequest, ControlResponse};
use crate::project_store::sha256_hex;
use crate::worker::ExecutableIdentity;
use crate::worker::contract::validate_native_session_id;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const MAX_MCP_LINE_BYTES: usize = 256 * 1024;
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const RELAY_SOCKET_NAME: &str = "relay.sock";
const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct BridgeConfiguration {
    pub binding_id: String,
    pub launch_nonce: String,
    pub capability: String,
    pub repo_root: PathBuf,
    pub project_id: Option<String>,
    pub relay_socket_path: PathBuf,
    pub registration_socket_path: PathBuf,
    pub control_socket_path: PathBuf,
    pub codex_home: PathBuf,
    pub relay_executable: ExecutableIdentity,
    pub relay_runtime_scope_hash: String,
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
    upstream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
    upstream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
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

fn validate_bridge_configuration(configuration: &BridgeConfiguration) -> Result<()> {
    validate_native_session_id(&configuration.binding_id)?;
    validate_secret(&configuration.launch_nonce)?;
    validate_secret(&configuration.capability)?;
    validate_canonical_directory("architect repository", &configuration.repo_root, None)?;
    if let Some(project_id) = &configuration.project_id {
        validate_native_session_id(project_id)?;
    }
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
    validate_canonical_directory(
        "isolated Codex home",
        &configuration.codex_home,
        Some(0o700),
    )?;
    configuration.relay_executable.revalidate()?;
    let paths = ControlPaths::discover()?;
    if paths.socket_path() != configuration.control_socket_path
        || paths.registration_socket_path() != configuration.registration_socket_path
    {
        bail!("architect bridge durable socket paths drifted");
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
    let mut binding_version = activation.binding_version;
    let mut native_session_id = None;
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
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        if authorize_relay_peer(&stream, &configuration, &activation).is_err() {
            continue;
        }
        if let Err(_error) = serve_mcp_connection(
            &mut stream,
            &configuration,
            &mut binding_version,
            &mut native_session_id,
        ) {
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
        bail!("architect binding close was not durably acknowledged");
    }
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
    if process_executable_path(peer.pid)? != configuration.relay_executable.canonical_path {
        bail!("architect relay executable path mismatch");
    }
    if process_birth_identity(peer.pid)? != peer_birth
        || process_birth_identity(activation.architect_pid)? != activation.architect_process_birth
        || process_birth_identity(activation.bridge_pid)? != activation.bridge_process_birth
        || process_executable_path(peer.pid)? != configuration.relay_executable.canonical_path
    {
        bail!("architect relay process identity changed during authorization");
    }
    configuration.relay_executable.revalidate()
}

fn serve_mcp_connection(
    stream: &mut UnixStream,
    configuration: &BridgeConfiguration,
    binding_version: &mut u64,
    native_session_id: &mut Option<String>,
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
                        "serverInfo":{"name":"hcom-project-control","version":"1"}
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
                    "result":{"tools":tool_definitions()}
                })
            }),
            "tools/call" => {
                let Some(id) = request.id else {
                    continue;
                };
                Some(handle_tool_call(
                    id,
                    request.params,
                    configuration,
                    binding_version,
                    native_session_id,
                ))
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
    binding_version: &mut u64,
    native_session_id: &mut Option<String>,
) -> Value {
    let result = (|| -> Result<ControlResponse> {
        let params: ToolCallParams = serde_json::from_value(
            params.ok_or_else(|| anyhow::anyhow!("tool call omitted params"))?,
        )
        .context("invalid typed tool call")?;
        let action = control_action(&params.name, params.arguments)?;
        let observed =
            discover_codex_native_session(&configuration.codex_home, &configuration.repo_root)?;
        match native_session_id {
            Some(expected) if expected != &observed => {
                bail!("architect native session changed inside one live binding")
            }
            Some(_) => {}
            None => {
                let response = RegistrationClient::new(&configuration.registration_socket_path)
                    .request(&RegistrationRequest {
                        protocol_version: PROTOCOL_VERSION,
                        request_id: format!("observe-{}", Uuid::new_v4()),
                        caller: RegistrationCaller::Bridge {
                            binding_id: configuration.binding_id.clone(),
                            launch_nonce: configuration.launch_nonce.clone(),
                            capability: configuration.capability.clone(),
                        },
                        action: RegistrationAction::ObserveNativeSession {
                            binding_id: configuration.binding_id.clone(),
                            expected_version: *binding_version,
                            native_session_id: observed.clone(),
                        },
                    })?;
                if !response.ok {
                    bail!("architect native session binding was refused");
                }
                let next_version = binding_version
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("architect binding version overflow"))?;
                if response.binding_version != Some(next_version) {
                    bail!("native session response returned an invalid version");
                }
                *binding_version = next_version;
                *native_session_id = Some(observed.clone());
            }
        }
        let request = ControlRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: format!("architect-{}", Uuid::new_v4()),
            caller: CallerAuth::Architect {
                binding_id: configuration.binding_id.clone(),
                launch_nonce: configuration.launch_nonce.clone(),
                capability: configuration.capability.clone(),
                native_session_id: Some(observed),
            },
            action,
        };
        ControlClient::new(&configuration.control_socket_path).request(&request)
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
        Err(_) => json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":{
                "content":[{"type":"text","text":"architect control request was refused"}],
                "isError":true
            }
        }),
    }
}

fn discover_codex_native_session(codex_home: &Path, repo_root: &Path) -> Result<String> {
    let sessions = codex_home.join("sessions");
    let mut files = Vec::new();
    let mut entries = 0;
    collect_session_files(&sessions, &mut files, 0, &mut entries)?;
    if files.len() != 1 {
        bail!("architect native session is not uniquely observable");
    }
    let path = &files[0];
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        bail!("architect native session record has an invalid identity");
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        bail!("architect native session record changed before it was opened");
    }
    let mut first_line = Vec::new();
    let mut reader = BufReader::new(file);
    reader
        .by_ref()
        .take((128 * 1024 + 1) as u64)
        .read_until(b'\n', &mut first_line)?;
    if first_line.is_empty() || first_line.len() > 128 * 1024 {
        bail!("architect native session metadata exceeds its bound");
    }
    let metadata: CodexSessionMetadata =
        serde_json::from_slice(trim_line(&first_line)).context("invalid Codex session metadata")?;
    if metadata.kind != "session_meta"
        || metadata.payload.cwd != repo_root
        || metadata.payload.cli_version != "0.145.0"
    {
        bail!("architect native session metadata does not match the bound repository");
    }
    let id = Uuid::parse_str(&metadata.payload.id)
        .context("architect native session id is not a canonical UUID")?
        .to_string();
    if id != metadata.payload.id {
        bail!("architect native session id is not canonical lowercase");
    }
    validate_native_session_id(&id)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("architect native session filename is invalid"))?;
    if !name.ends_with(&format!("-{id}.jsonl")) {
        bail!("architect native session filename does not bind its metadata id");
    }
    let current = fs::symlink_metadata(path)?;
    let final_opened = reader.get_ref().metadata()?;
    if current.file_type().is_symlink()
        || current.dev() != opened.dev()
        || current.ino() != opened.ino()
        || final_opened.dev() != opened.dev()
        || final_opened.ino() != opened.ino()
    {
        bail!("architect native session record changed during observation");
    }
    Ok(id)
}

fn collect_session_files(
    directory: &Path,
    files: &mut Vec<PathBuf>,
    depth: usize,
    entries: &mut usize,
) -> Result<()> {
    if depth > 6 {
        bail!("architect session tree exceeds its bounded depth");
    }
    let metadata = fs::symlink_metadata(directory).with_context(|| {
        format!(
            "architect session directory is unavailable: {}",
            directory.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("architect session tree contains a non-directory");
    }
    let mut children = fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        *entries += 1;
        if *entries > 512 {
            bail!("architect session tree exceeds its bounded entry count");
        }
        let path = child.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bail!("architect session tree contains a symlink");
        }
        if metadata.is_dir() {
            collect_session_files(&path, files, depth + 1, entries)?;
        } else if metadata.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
        {
            files.push(path);
            if files.len() > 1 {
                bail!("architect native session is ambiguous");
            }
        }
    }
    Ok(())
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
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Deserialize)]
struct CodexSessionMetadata {
    #[serde(rename = "type")]
    kind: String,
    payload: CodexSessionPayload,
}

#[derive(Deserialize)]
struct CodexSessionPayload {
    id: String,
    cwd: PathBuf,
    cli_version: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::ActionName;
    use crate::control_api::daemon::{
        ArchitectBindingRegistration, ArchitectProcessRegistration, DaemonEndpoint,
    };
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    const BRIDGE_HELPER_FD: &str = "HCOM_PHASE7_BRIDGE_HELPER_FD";
    const RELAY_HELPER_SOCKET: &str = "HCOM_PHASE7_RELAY_HELPER_SOCKET";
    const RELAY_HELPER_PARENT: &str = "HCOM_PHASE7_RELAY_HELPER_PARENT";
    const RELAY_HELPER_EXPECT_ACK: &str = "HCOM_PHASE7_RELAY_HELPER_EXPECT_ACK";

    #[test]
    fn native_session_requires_one_exact_machine_record() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        let codex = temp.path().join("codex");
        let sessions = codex.join("sessions/2026/07/29");
        fs::create_dir_all(&repo).unwrap();
        fs::create_dir_all(&sessions).unwrap();
        let id = "019fa976-e270-7a92-b5f0-6d3d8a0ad3f4";
        let path = sessions.join(format!("rollout-2026-07-29T00-00-00-{id}.jsonl"));
        fs::write(
            &path,
            format!(
                "{{\"timestamp\":\"2026-07-29T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":{},\"cli_version\":\"0.145.0\",\"originator\":\"codex_cli_rs\"}}}}\n",
                serde_json::to_string(&repo).unwrap()
            ),
        )
        .unwrap();
        assert_eq!(discover_codex_native_session(&codex, &repo).unwrap(), id);

        fs::write(
            sessions.join("rollout-other-019fa976-e270-7a92-b5f0-6d3d8a0ad3f5.jsonl"),
            "{}\n",
        )
        .unwrap();
        assert!(discover_codex_native_session(&codex, &repo).is_err());
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
    fn bridge_helper_process() {
        let Some(fd) = std::env::var_os(BRIDGE_HELPER_FD) else {
            return;
        };
        let fd = fd.to_string_lossy().parse::<RawFd>().unwrap();
        run_bridge(fd).unwrap();
    }

    #[test]
    fn bridge_bootstrap_socket_and_exit_revoke_are_process_bound() {
        let temp = tempfile::Builder::new()
            .prefix("hcom-phase7-bridge.")
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let state_home = root.join("state");
        let runtime_home = root.join("run");
        let config_home = root.join("config");
        for path in [&state_home, &runtime_home, &config_home] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let paths = crate::control_api::daemon::ControlPaths::new(
            state_home.join("hcom-project-control"),
            runtime_home.join("hcom-project-control"),
            config_home.join("hcom-project-control/config.toml"),
        );
        let mut endpoint = DaemonEndpoint::bind(paths.clone()).unwrap();
        let repository = root.join("repo");
        let codex_home = root.join("codex-home");
        let relay_root = paths.architect_runtime_root_path().join("launch");
        for path in [&repository, &codex_home, &relay_root] {
            fs::create_dir_all(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let relay_socket = relay_root.join(RELAY_SOCKET_NAME);
        let relay_scope_hash = relay_runtime_scope_hash(&relay_root).unwrap();
        let relay_executable = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();

        let mut architect = Command::new("/usr/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let architect_pid = architect.id();
        let architect_birth = process_birth_identity(architect_pid).unwrap();

        let (mut bootstrap, child_bootstrap) = UnixStream::pair().unwrap();
        bootstrap
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        bootstrap
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let inherited_fd = child_bootstrap.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "architect::bridge::tests::bridge_helper_process",
                "--nocapture",
            ])
            .env(BRIDGE_HELPER_FD, inherited_fd.to_string())
            .env("XDG_STATE_HOME", &state_home)
            .env("XDG_RUNTIME_DIR", &runtime_home)
            .env("XDG_CONFIG_HOME", &config_home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // SAFETY: inherited_fd is a live socketpair descriptor. Clearing
        // CLOEXEC preserves only that exact test bootstrap channel.
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(inherited_fd, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut bridge = command.spawn().unwrap();
        drop(child_bootstrap);
        let bridge_pid = bridge.id();
        let bridge_birth = process_birth_identity(bridge_pid).unwrap();

        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-bridge-lifecycle".into(),
                repo_root: repository.clone(),
                architect_name: "architect-bridge-lifecycle".into(),
                architect_adapter: "codex-0.145.0".into(),
                launch_nonce: "launch-nonce-bridge-lifecycle".into(),
                capability: "capability-bridge-lifecycle".into(),
                actions: ActionName::ALL.into_iter().collect(),
            })
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-bridge-lifecycle",
                0,
                &ArchitectProcessRegistration {
                    architect_pid,
                    architect_process_birth: architect_birth.clone(),
                    bridge_pid,
                    bridge_process_birth: bridge_birth.clone(),
                    relay_executable_contract_hash: sha256_hex(
                        &serde_json::to_vec(&relay_executable).unwrap(),
                    ),
                    relay_runtime_scope_hash: relay_scope_hash.clone(),
                },
            )
            .unwrap();
        let configuration = BridgeConfiguration {
            binding_id: "binding-bridge-lifecycle".into(),
            launch_nonce: "launch-nonce-bridge-lifecycle".into(),
            capability: "capability-bridge-lifecycle".into(),
            repo_root: repository,
            project_id: None,
            relay_socket_path: relay_socket.clone(),
            registration_socket_path: paths.registration_socket_path(),
            control_socket_path: paths.socket_path(),
            codex_home,
            relay_executable,
            relay_runtime_scope_hash: relay_scope_hash,
        };
        configure_bridge(&mut bootstrap, configuration).unwrap();
        let metadata = fs::symlink_metadata(&relay_socket).unwrap();
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        activate_bridge(
            &mut bootstrap,
            BridgeActivation {
                architect_pid,
                architect_process_birth: architect_birth,
                bridge_pid,
                bridge_process_birth: bridge_birth,
                binding_version: 1,
            },
        )
        .unwrap();
        drop(bootstrap);

        endpoint.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut endpoint = endpoint;
            loop {
                if endpoint.try_serve_registration_one().unwrap() {
                    return endpoint;
                }
                assert!(
                    Instant::now() < deadline,
                    "bridge did not close its registration before the deadline"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        architect.kill().unwrap();
        architect.wait().unwrap();
        let status = bridge.wait().unwrap();
        assert!(status.success(), "bridge helper failed: {status}");
        let mut endpoint = server.join().unwrap();
        let stored = endpoint
            .control_mut()
            .architect_binding_state_version("binding-bridge-lifecycle")
            .unwrap();
        assert_eq!(stored, ("closed".into(), 2));
        assert!(!relay_socket.exists());
    }

    #[test]
    fn relay_client_helper_process() {
        let Some(socket) = std::env::var_os(RELAY_HELPER_SOCKET) else {
            return;
        };
        if std::env::var_os(RELAY_HELPER_PARENT).is_some() {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "architect::bridge::tests::relay_client_helper_process",
                    "--nocapture",
                ])
                .env(RELAY_HELPER_SOCKET, socket)
                .env_remove(RELAY_HELPER_PARENT)
                .env(RELAY_HELPER_EXPECT_ACK, "1")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let mut stream = UnixStream::connect(PathBuf::from(socket)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(&[0xff]).unwrap();
        let mut ack = [0u8; 1];
        if std::env::var_os(RELAY_HELPER_EXPECT_ACK).is_some() {
            let count = stream.read(&mut ack).unwrap();
            assert_eq!((count, ack[0]), (1, 0x7f));
        } else {
            match stream.read(&mut ack) {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                outcome => panic!("unauthorized relay received an unexpected outcome: {outcome:?}"),
            }
        }
    }

    #[test]
    fn relay_peer_requires_exact_executable_and_architect_ancestry_before_frames() {
        let temp = tempfile::Builder::new()
            .prefix("hcom-phase7-relay-auth.")
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let relay_path = root.join(RELAY_SOCKET_NAME);
        let listener = UnixListener::bind(&relay_path).unwrap();
        fs::set_permissions(&relay_path, fs::Permissions::from_mode(0o600)).unwrap();
        let executable = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        let bridge_birth = process_birth_identity(std::process::id()).unwrap();
        let configuration = BridgeConfiguration {
            binding_id: "binding-relay-auth".into(),
            launch_nonce: "launch-nonce-relay-auth".into(),
            capability: "capability-relay-auth".into(),
            repo_root: root.clone(),
            project_id: None,
            relay_socket_path: relay_path.clone(),
            registration_socket_path: root.join("registration.sock"),
            control_socket_path: root.join("control.sock"),
            codex_home: root.clone(),
            relay_executable: executable,
            relay_runtime_scope_hash: relay_runtime_scope_hash(&root).unwrap(),
        };

        let mut architect = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "architect::bridge::tests::relay_client_helper_process",
                "--nocapture",
            ])
            .env(RELAY_HELPER_SOCKET, &relay_path)
            .env(RELAY_HELPER_PARENT, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let activation = BridgeActivation {
            architect_pid: architect.id(),
            architect_process_birth: process_birth_identity(architect.id()).unwrap(),
            bridge_pid: std::process::id(),
            bridge_process_birth: bridge_birth.clone(),
            binding_version: 1,
        };
        let (mut authorized, _) = listener.accept().unwrap();
        authorize_relay_peer(&authorized, &configuration, &activation).unwrap();
        let mut invalid_frame = [0u8; 1];
        authorized.read_exact(&mut invalid_frame).unwrap();
        assert_eq!(invalid_frame, [0xff]);
        authorized.write_all(&[0x7f]).unwrap();
        drop(authorized);
        assert!(architect.wait().unwrap().success());

        let mut wrong_ancestry = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "architect::bridge::tests::relay_client_helper_process",
                "--nocapture",
            ])
            .env(RELAY_HELPER_SOCKET, &relay_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let (unauthorized, _) = listener.accept().unwrap();
        assert!(authorize_relay_peer(&unauthorized, &configuration, &activation).is_err());
        drop(unauthorized);
        assert!(wrong_ancestry.wait().unwrap().success());

        let script = "import socket,sys\ns=socket.socket(socket.AF_UNIX)\ns.connect(sys.argv[1])\ns.sendall(b'x')\ntry:\n    result=s.recv(1)\nexcept ConnectionResetError:\n    result=b''\nraise SystemExit(0 if result == b'' else 1)";
        let mut wrong_executable = Command::new("/usr/bin/python3")
            .args(["-c", script, relay_path.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let (unauthorized, _) = listener.accept().unwrap();
        let parent_activation = BridgeActivation {
            architect_pid: std::process::id(),
            architect_process_birth: bridge_birth.clone(),
            bridge_pid: std::process::id(),
            bridge_process_birth: bridge_birth,
            binding_version: 1,
        };
        assert!(authorize_relay_peer(&unauthorized, &configuration, &parent_activation).is_err());
        drop(unauthorized);
        assert!(wrong_executable.wait().unwrap().success());
    }

    #[test]
    fn first_typed_tool_call_binds_one_exact_native_session_then_only_uses_control() {
        let temp = tempfile::Builder::new()
            .prefix("hcom-phase7-tool-call.")
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let state_home = root.join("state");
        let runtime_home = root.join("run");
        let config_home = root.join("config");
        let repository = root.join("repo");
        let codex_home = root.join("codex-home");
        for path in [
            &state_home,
            &runtime_home,
            &config_home,
            &repository,
            &codex_home,
        ] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let paths = crate::control_api::daemon::ControlPaths::new(
            state_home.join("hcom-project-control"),
            runtime_home.join("hcom-project-control"),
            config_home.join("hcom-project-control/config.toml"),
        );
        let mut endpoint = DaemonEndpoint::bind(paths.clone()).unwrap();
        let birth = process_birth_identity(std::process::id()).unwrap();
        endpoint
            .control_mut()
            .register_architect_binding(&ArchitectBindingRegistration {
                binding_id: "binding-tool-call".into(),
                repo_root: repository.clone(),
                architect_name: "architect-tool-call".into(),
                architect_adapter: "codex-0.145.0".into(),
                launch_nonce: "launch-nonce-tool-call".into(),
                capability: "capability-tool-call".into(),
                actions: ActionName::ALL.into_iter().collect(),
            })
            .unwrap();
        endpoint
            .control_mut()
            .bind_architect_process(
                "binding-tool-call",
                0,
                &ArchitectProcessRegistration {
                    architect_pid: std::process::id(),
                    architect_process_birth: birth.clone(),
                    bridge_pid: std::process::id(),
                    bridge_process_birth: birth,
                    relay_executable_contract_hash: std::iter::repeat_n('e', 64).collect(),
                    relay_runtime_scope_hash: std::iter::repeat_n('f', 64).collect(),
                },
            )
            .unwrap();
        let session_id = "019fa976-e270-7a92-b5f0-6d3d8a0ad3f4";
        let sessions = codex_home.join("sessions/2026/07/29");
        fs::create_dir_all(&sessions).unwrap();
        fs::write(
            sessions.join(format!("rollout-2026-07-29T00-00-00-{session_id}.jsonl")),
            format!(
                "{{\"timestamp\":\"2026-07-29T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{session_id}\",\"cwd\":{},\"cli_version\":\"0.145.0\",\"originator\":\"codex_cli_rs\"}}}}\n",
                serde_json::to_string(&repository).unwrap()
            ),
        )
        .unwrap();
        endpoint.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut endpoint = endpoint;
            let mut registrations = 0;
            let mut controls = 0;
            while registrations < 1 || controls < 2 {
                if endpoint.try_serve_registration_one().unwrap() {
                    registrations += 1;
                }
                if endpoint.try_serve_one().unwrap() {
                    controls += 1;
                }
                assert!(
                    Instant::now() < deadline,
                    "typed tool calls did not reach the daemon before the deadline"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
            endpoint
        });
        let executable = ExecutableIdentity::capture(
            fs::canonicalize(std::env::current_exe().unwrap()).unwrap(),
        )
        .unwrap();
        let configuration = BridgeConfiguration {
            binding_id: "binding-tool-call".into(),
            launch_nonce: "launch-nonce-tool-call".into(),
            capability: "capability-tool-call".into(),
            repo_root: repository.clone(),
            project_id: None,
            relay_socket_path: root.join("relay.sock"),
            registration_socket_path: paths.registration_socket_path(),
            control_socket_path: paths.socket_path(),
            codex_home,
            relay_executable: executable,
            relay_runtime_scope_hash: std::iter::repeat_n('f', 64).collect(),
        };
        let params = || {
            Some(json!({
                "name":"project_create",
                "arguments":{
                    "repo_root":repository,
                    "target_ref":"refs/heads/master"
                }
            }))
        };
        let mut binding_version = 1;
        let mut native_session = None;
        for id in [1, 2] {
            let response = handle_tool_call(
                json!(id),
                params(),
                &configuration,
                &mut binding_version,
                &mut native_session,
            );
            assert_eq!(response["result"]["isError"], true);
            assert_eq!(
                response["result"]["structuredContent"]["error"]["code"],
                "not_implemented"
            );
        }
        assert_eq!(binding_version, 2);
        assert_eq!(native_session.as_deref(), Some(session_id));
        let mut endpoint = server.join().unwrap();
        assert_eq!(
            endpoint
                .control_mut()
                .architect_binding_state_version("binding-tool-call")
                .unwrap(),
            ("bound".into(), 2)
        );
        assert_eq!(
            endpoint.control_mut().phase7_business_counts().unwrap(),
            (2, 0),
            "Phase 7 may write only the request ledger, not project business rows"
        );
    }
}
