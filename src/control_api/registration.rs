//! Private launch-registration protocol for the architect control bridge.
//!
//! This socket is never mounted into an architect or worker sandbox. It carries
//! launch/process binding material between the foreground `hcom architect`
//! launcher, the separately spawned bridge, and the in-process session
//! supervisor; it is not an MCP or public task-action surface.

use super::codec::{read_response_frame, write_request_frame};
use super::protocol::{ActionName, PROTOCOL_VERSION};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const REGISTRATION_REFUSAL_GENERIC: &str = "architect_registration";
pub(crate) const TOOL_REFUSAL_ENVELOPE: &str = "architect_tool_envelope";
pub(crate) const TOOL_REFUSAL_ACTION: &str = "architect_tool_action";
pub(crate) const NATIVE_SESSION_REFUSAL_DISCOVERY: &str = "native_session_discovery";
pub(crate) const NATIVE_SESSION_REFUSAL_CHANGED: &str = "native_session_changed";
pub(crate) const NATIVE_SESSION_REFUSAL_REGISTRATION_TRANSPORT: &str =
    "native_session_registration_transport";
pub(crate) const NATIVE_SESSION_REFUSAL_REGISTRATION_VERSION: &str =
    "native_session_registration_version";
pub(crate) const NATIVE_SESSION_REFUSAL_UNAVAILABLE: &str = "native_session_binding_unavailable";
pub(crate) const NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS: &str = "native_session_bridge_process";
pub(crate) const NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS: &str =
    "native_session_architect_liveness";
pub(crate) const NATIVE_SESSION_REFUSAL_CAPABILITY: &str = "native_session_capability";
pub(crate) const NATIVE_SESSION_REFUSAL_IDENTITY: &str = "native_session_identity";
pub(crate) const NATIVE_SESSION_REFUSAL_VERSION: &str = "native_session_binding_version";
pub(crate) const NATIVE_SESSION_REFUSAL_STATE: &str = "native_session_binding_state";
pub(crate) const CONTROL_REFUSAL_TRANSPORT: &str = "architect_control_transport";

pub(crate) fn closed_native_session_refusal_code(value: Option<&str>) -> &'static str {
    match value {
        Some(TOOL_REFUSAL_ENVELOPE) => TOOL_REFUSAL_ENVELOPE,
        Some(TOOL_REFUSAL_ACTION) => TOOL_REFUSAL_ACTION,
        Some(NATIVE_SESSION_REFUSAL_DISCOVERY) => NATIVE_SESSION_REFUSAL_DISCOVERY,
        Some(NATIVE_SESSION_REFUSAL_CHANGED) => NATIVE_SESSION_REFUSAL_CHANGED,
        Some(NATIVE_SESSION_REFUSAL_REGISTRATION_TRANSPORT) => {
            NATIVE_SESSION_REFUSAL_REGISTRATION_TRANSPORT
        }
        Some(NATIVE_SESSION_REFUSAL_REGISTRATION_VERSION) => {
            NATIVE_SESSION_REFUSAL_REGISTRATION_VERSION
        }
        Some(NATIVE_SESSION_REFUSAL_UNAVAILABLE) => NATIVE_SESSION_REFUSAL_UNAVAILABLE,
        Some(NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS) => NATIVE_SESSION_REFUSAL_BRIDGE_PROCESS,
        Some(NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS) => {
            NATIVE_SESSION_REFUSAL_ARCHITECT_LIVENESS
        }
        Some(NATIVE_SESSION_REFUSAL_CAPABILITY) => NATIVE_SESSION_REFUSAL_CAPABILITY,
        Some(NATIVE_SESSION_REFUSAL_IDENTITY) => NATIVE_SESSION_REFUSAL_IDENTITY,
        Some(NATIVE_SESSION_REFUSAL_VERSION) => NATIVE_SESSION_REFUSAL_VERSION,
        Some(NATIVE_SESSION_REFUSAL_STATE) => NATIVE_SESSION_REFUSAL_STATE,
        Some(CONTROL_REFUSAL_TRANSPORT) => CONTROL_REFUSAL_TRANSPORT,
        _ => REGISTRATION_REFUSAL_GENERIC,
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistrationRequest {
    pub protocol_version: u32,
    pub request_id: String,
    pub caller: RegistrationCaller,
    pub action: RegistrationAction,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistrationCaller {
    Human {
        process_birth: String,
    },
    Bridge {
        binding_id: String,
        launch_nonce: String,
        capability: String,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistrationAction {
    CreateBinding {
        binding_id: String,
        project_root: String,
        architect_name: String,
        architect_adapter: String,
        launch_nonce: String,
        capability: String,
        actions: BTreeSet<ActionName>,
    },
    BindProcess {
        binding_id: String,
        expected_version: u64,
        architect_pid: u32,
        architect_process_birth: String,
        bridge_pid: u32,
        bridge_process_birth: String,
        relay_executable_contract_hash: String,
        relay_runtime_scope_hash: String,
    },
    ObserveNativeSession {
        binding_id: String,
        expected_version: u64,
        native_session_id: String,
    },
    CloseBinding {
        binding_id: String,
        expected_version: u64,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistrationResponse {
    pub protocol_version: u32,
    pub request_id: String,
    pub ok: bool,
    pub binding_version: Option<u64>,
    pub error: Option<String>,
}

impl RegistrationResponse {
    pub(crate) fn success(request_id: &str, binding_version: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.to_owned(),
            ok: true,
            binding_version: Some(binding_version),
            error: None,
        }
    }

    pub(crate) fn error(request_id: &str, message: &str) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.to_owned(),
            ok: false,
            binding_version: None,
            error: Some(message.to_owned()),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION
            || self.request_id.is_empty()
            || self.request_id.len() > 128
            || self.ok != self.binding_version.is_some()
            || self.ok == self.error.is_some()
        {
            bail!("session supervisor returned an invalid registration response");
        }
        if self
            .error
            .as_deref()
            .is_some_and(|message| message.is_empty() || message.len() > 1024)
        {
            bail!("session supervisor returned an invalid registration error");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RegistrationClient {
    socket_path: PathBuf,
}

impl RegistrationClient {
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    pub fn request(&self, request: &RegistrationRequest) -> Result<RegistrationResponse> {
        validate_request_envelope(request)?;
        validate_private_socket(&self.socket_path)?;
        let payload =
            serde_json::to_vec(request).context("failed to encode registration request")?;
        let mut stream = UnixStream::connect(&self.socket_path).with_context(|| {
            format!(
                "failed to connect to architect registration socket {}",
                self.socket_path.display()
            )
        })?;
        stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        write_request_frame(&mut stream, &payload)?;
        let frame = read_response_frame(&mut stream)?;
        let response: RegistrationResponse = serde_json::from_slice(&frame)
            .context("session supervisor returned malformed registration JSON")?;
        response.validate()?;
        if response.request_id != request.request_id {
            bail!("session supervisor returned a mismatched registration request id");
        }
        Ok(response)
    }
}

pub(crate) fn validate_request_envelope(request: &RegistrationRequest) -> Result<()> {
    if request.protocol_version != PROTOCOL_VERSION {
        bail!("unsupported registration protocol version");
    }
    validate_id(&request.request_id)?;
    match &request.caller {
        RegistrationCaller::Human { process_birth } => validate_text(process_birth, 256),
        RegistrationCaller::Bridge {
            binding_id,
            launch_nonce,
            capability,
        } => {
            validate_id(binding_id)?;
            validate_secret(launch_nonce)?;
            validate_secret(capability)
        }
    }
}

fn validate_private_socket(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("registration socket path must be absolute");
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect registration socket {}", path.display()))?;
    // SAFETY: geteuid has no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_socket()
        || metadata.uid() != expected_uid
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        bail!("registration socket is not a private current-user socket");
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("registration socket has no parent directory"))?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink()
        || !parent_metadata.is_dir()
        || parent_metadata.uid() != expected_uid
        || parent_metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("registration socket parent is not private");
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("invalid registration identifier");
    }
    Ok(())
}

fn validate_secret(value: &str) -> Result<()> {
    if !(16..=512).contains(&value.len())
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("invalid registration secret shape");
    }
    Ok(())
}

fn validate_text(value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || ('\u{80}'..='\u{9f}').contains(&character))
    {
        bail!("invalid bounded registration text");
    }
    Ok(())
}
