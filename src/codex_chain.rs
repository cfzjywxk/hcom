//! Exact Codex contract used by the private same-terminal generation adapter.
//!
//! This module is deliberately not wired into clap or the generic launcher.
//! It has one fixed executable, one supported native version, and one plain
//! create argv shape.

use std::fmt;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::handoff::{MAX_IDENTITY_BYTES, MAX_OPAQUE_ID_BYTES, TerminalChain};

pub(crate) const SUPPORTED_CODEX_VERSION: &str = "0.145.0";
pub(crate) const SUPPORTED_CODEX_VERSION_OUTPUT: &str = "codex-cli 0.145.0";
pub(crate) const MAX_CHAIN_HOOK_PAYLOAD_BYTES: usize = 16 * 1024;
pub(crate) const CODEX_VERSION_ENV: &str = "HCOM_CHAIN_CODEX_VERSION";
pub(crate) const HANDOFF_ID_ENV: &str = "HCOM_CHAIN_HANDOFF_ID";
const MAX_TERMINAL_RECORD_TAIL_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CodexApproval {
    Never,
    OnRequest,
    Untrusted,
}

impl CodexApproval {
    fn cli(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::OnRequest => "on-request",
            Self::Untrusted => "untrusted",
        }
    }

    pub(crate) fn hook_permission_mode(self) -> &'static str {
        match self {
            Self::Never => "bypassPermissions",
            Self::OnRequest | Self::Untrusted => "default",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CodexSandbox {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl CodexSandbox {
    fn cli(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CodexLaunchProfile {
    pub workspace: PathBuf,
    pub model: String,
    pub reasoning: String,
    pub approval: CodexApproval,
    pub sandbox: CodexSandbox,
    pub append_reply_handoff: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexContractError {
    UnsupportedVersion,
    InvalidPinnedPolicy,
    InvalidHandoffIdentity,
    NonCanonicalWorkspace,
    NonFreshArgv,
    InvalidTranscriptPath,
    InvalidTranscriptRecord,
}

impl fmt::Display for CodexContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedVersion => "unsupported Codex version",
            Self::InvalidPinnedPolicy => "invalid pinned Codex chain policy",
            Self::InvalidHandoffIdentity => "invalid opaque handoff identity",
            Self::NonCanonicalWorkspace => "Codex chain workspace is not canonical",
            Self::NonFreshArgv => "Codex chain argv is not the fixed fresh-create profile",
            Self::InvalidTranscriptPath => "Codex transcript path is not exact",
            Self::InvalidTranscriptRecord => "Codex transcript terminal record is invalid",
        })
    }
}

pub(crate) fn exact_transcript_path(
    raw: &str,
    native_session_id: &str,
) -> Result<PathBuf, CodexContractError> {
    validate_native_identity(native_session_id)?;
    let path = std::fs::canonicalize(raw).map_err(|_| CodexContractError::InvalidTranscriptPath)?;
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| crate::runtime_env::user_home().map(|home| home.join(".codex")))
        .ok_or(CodexContractError::InvalidTranscriptPath)?;
    let sessions = std::fs::canonicalize(codex_home.join("sessions"))
        .map_err(|_| CodexContractError::InvalidTranscriptPath)?;
    validate_transcript_filename(&path, native_session_id)?;
    if !path.starts_with(sessions) {
        return Err(CodexContractError::InvalidTranscriptPath);
    }
    Ok(path)
}

/// Return true only after the exact Stop turn has a complete, newline-terminated
/// TurnComplete rollout record. `raw` was already pinned by the exact Stop
/// hook; it is canonicalized again and must remain byte-for-byte identical.
pub(crate) fn terminal_record_persisted(
    raw: &str,
    native_session_id: &str,
    turn_id: &str,
) -> Result<bool, CodexContractError> {
    validate_native_identity(native_session_id)?;
    validate_native_identity(turn_id)?;
    let canonical =
        std::fs::canonicalize(raw).map_err(|_| CodexContractError::InvalidTranscriptPath)?;
    if canonical.to_str() != Some(raw) {
        return Err(CodexContractError::InvalidTranscriptPath);
    }
    validate_transcript_filename(&canonical, native_session_id)?;

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&canonical)
        .map_err(|_| CodexContractError::InvalidTranscriptPath)?;
    let metadata = file
        .metadata()
        .map_err(|_| CodexContractError::InvalidTranscriptPath)?;
    if !metadata.is_file() {
        return Err(CodexContractError::InvalidTranscriptPath);
    }
    let len = metadata.len();
    let start = len.saturating_sub(MAX_TERMINAL_RECORD_TAIL_BYTES);
    let read_start = start.saturating_sub(1);
    file.seek(SeekFrom::Start(read_start))
        .map_err(|_| CodexContractError::InvalidTranscriptRecord)?;
    let mut bytes = Vec::with_capacity((len - start) as usize);
    file.read_to_end(&mut bytes)
        .map_err(|_| CodexContractError::InvalidTranscriptRecord)?;
    if bytes.len() as u64 > MAX_TERMINAL_RECORD_TAIL_BYTES + u64::from(start > 0) {
        return Err(CodexContractError::InvalidTranscriptRecord);
    }

    let (starts_at_boundary, bytes) = if start == 0 {
        (true, bytes.as_slice())
    } else {
        (bytes.first() == Some(&b'\n'), &bytes[1..])
    };
    let complete_end = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    if complete_end == 0 {
        return Ok(false);
    }
    let mut complete = &bytes[..complete_end];
    if !starts_at_boundary {
        let first_end = complete
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or(CodexContractError::InvalidTranscriptRecord)?;
        complete = &complete[first_end + 1..];
        if complete.is_empty() && start > 0 {
            return Err(CodexContractError::InvalidTranscriptRecord);
        }
    }

    for line in complete.split(|byte| *byte == b'\n').rev() {
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_slice(line)
            .map_err(|_| CodexContractError::InvalidTranscriptRecord)?;
        if value.get("type").and_then(serde_json::Value::as_str) != Some("event_msg")
            || value
                .pointer("/payload/type")
                .and_then(serde_json::Value::as_str)
                != Some("task_complete")
        {
            continue;
        }
        if value
            .pointer("/payload/turn_id")
            .and_then(serde_json::Value::as_str)
            == Some(turn_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_transcript_filename(
    path: &Path,
    native_session_id: &str,
) -> Result<(), CodexContractError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(CodexContractError::InvalidTranscriptPath)?;
    if !file_name.starts_with("rollout-")
        || !file_name.ends_with(&format!("-{native_session_id}.jsonl"))
    {
        return Err(CodexContractError::InvalidTranscriptPath);
    }
    Ok(())
}

fn validate_native_identity(value: &str) -> Result<(), CodexContractError> {
    if value.is_empty() || value.len() > MAX_IDENTITY_BYTES || value.chars().any(char::is_control) {
        Err(CodexContractError::InvalidTranscriptRecord)
    } else {
        Ok(())
    }
}

impl std::error::Error for CodexContractError {}

pub(crate) fn validate_codex_version_output(output: &str) -> Result<(), CodexContractError> {
    if output.trim_end_matches(['\r', '\n']) == SUPPORTED_CODEX_VERSION_OUTPUT {
        Ok(())
    } else {
        Err(CodexContractError::UnsupportedVersion)
    }
}

impl CodexLaunchProfile {
    pub(crate) fn from_chain(chain: &TerminalChain) -> Result<Self, CodexContractError> {
        let workspace = std::fs::canonicalize(&chain.workspace)
            .map_err(|_| CodexContractError::NonCanonicalWorkspace)?;
        if workspace.to_string_lossy() != chain.workspace {
            return Err(CodexContractError::NonCanonicalWorkspace);
        }
        if chain.model_ref.is_empty()
            || chain.model_ref.len() > 128
            || chain.model_ref.chars().any(char::is_control)
        {
            return Err(CodexContractError::InvalidPinnedPolicy);
        }
        if !matches!(
            chain.reasoning_ref.as_str(),
            "minimal" | "low" | "medium" | "high" | "xhigh"
        ) {
            return Err(CodexContractError::InvalidPinnedPolicy);
        }
        let (approval, sandbox) = parse_permission_policy(&chain.permission_policy_ref)?;
        Ok(Self {
            workspace,
            model: chain.model_ref.clone(),
            reasoning: chain.reasoning_ref.clone(),
            approval,
            sandbox,
            append_reply_handoff: false,
        })
    }

    pub(crate) fn argv(&self, handoff_id: &str) -> Result<Vec<String>, CodexContractError> {
        validate_handoff_id(handoff_id)?;
        let argv = vec![
            "--model".to_string(),
            self.model.clone(),
            "--config".to_string(),
            format!("model_reasoning_effort=\"{}\"", self.reasoning),
            "--sandbox".to_string(),
            self.sandbox.cli().to_string(),
            "--ask-for-approval".to_string(),
            self.approval.cli().to_string(),
            "--cd".to_string(),
            self.workspace.to_string_lossy().into_owned(),
            format!("Continue hcom handoff {handoff_id}"),
        ];
        self.validate_exact_argv(handoff_id, &argv)?;
        Ok(argv)
    }

    pub(crate) fn validate_exact_argv(
        &self,
        handoff_id: &str,
        argv: &[String],
    ) -> Result<(), CodexContractError> {
        validate_handoff_id(handoff_id)?;
        let expected = [
            "--model",
            self.model.as_str(),
            "--config",
            &format!("model_reasoning_effort=\"{}\"", self.reasoning),
            "--sandbox",
            self.sandbox.cli(),
            "--ask-for-approval",
            self.approval.cli(),
            "--cd",
            self.workspace
                .to_str()
                .ok_or(CodexContractError::NonCanonicalWorkspace)?,
            &format!("Continue hcom handoff {handoff_id}"),
        ];
        if argv.iter().map(String::as_str).eq(expected) {
            Ok(())
        } else {
            Err(CodexContractError::NonFreshArgv)
        }
    }
}

fn validate_handoff_id(value: &str) -> Result<(), CodexContractError> {
    if value.is_empty()
        || value.len() > MAX_OPAQUE_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        Err(CodexContractError::InvalidHandoffIdentity)
    } else {
        Ok(())
    }
}

fn parse_permission_policy(
    value: &str,
) -> Result<(CodexApproval, CodexSandbox), CodexContractError> {
    let mut approval = None;
    let mut sandbox = None;
    for part in value.split(';') {
        if let Some(value) = part.strip_prefix("approval=") {
            approval = Some(match value {
                "never" => CodexApproval::Never,
                "on-request" => CodexApproval::OnRequest,
                "untrusted" => CodexApproval::Untrusted,
                _ => return Err(CodexContractError::InvalidPinnedPolicy),
            });
        } else if let Some(value) = part.strip_prefix("sandbox=") {
            sandbox = Some(match value {
                "read-only" => CodexSandbox::ReadOnly,
                "workspace-write" => CodexSandbox::WorkspaceWrite,
                "danger-full-access" => CodexSandbox::DangerFullAccess,
                _ => return Err(CodexContractError::InvalidPinnedPolicy),
            });
        } else {
            return Err(CodexContractError::InvalidPinnedPolicy);
        }
    }
    match (approval, sandbox) {
        (Some(approval), Some(sandbox)) => Ok((approval, sandbox)),
        _ => Err(CodexContractError::InvalidPinnedPolicy),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn profile() -> CodexLaunchProfile {
        CodexLaunchProfile {
            workspace: PathBuf::from("/tmp/workspace"),
            model: "gpt-test".to_string(),
            reasoning: "high".to_string(),
            approval: CodexApproval::Never,
            sandbox: CodexSandbox::DangerFullAccess,
            append_reply_handoff: false,
        }
    }

    #[test]
    fn accepts_only_the_exact_pinned_version() {
        assert!(validate_codex_version_output("codex-cli 0.145.0\n").is_ok());
        for value in [
            "",
            "0.145.0",
            "codex-cli 0.144.0",
            "codex-cli 0.145.1",
            "codex-cli 0.145.0 extra",
        ] {
            assert_eq!(
                validate_codex_version_output(value),
                Err(CodexContractError::UnsupportedVersion)
            );
        }
    }

    #[test]
    fn fixed_profile_rejects_every_continuation_or_extra_shape() {
        let profile = profile();
        let id = "ho-opaque";
        let exact = profile.argv(id).unwrap();
        assert!(!profile.append_reply_handoff);
        assert!(profile.validate_exact_argv(id, &exact).is_ok());

        let mut corpus = Vec::new();
        for continuation in [
            "resume",
            "fork",
            "--last",
            "exec",
            "review",
            "--remote",
            "--image",
            "--add-dir",
            "--profile",
        ] {
            let mut candidate = exact.clone();
            candidate.push(continuation.to_string());
            corpus.push(candidate);
        }
        let mut changed_prompt = exact.clone();
        *changed_prompt.last_mut().unwrap() = "raw task body".to_string();
        corpus.push(changed_prompt);
        for candidate in corpus {
            assert_eq!(
                profile.validate_exact_argv(id, &candidate),
                Err(CodexContractError::NonFreshArgv)
            );
        }
    }

    #[test]
    fn terminal_gate_requires_complete_matching_turn_record() {
        let directory = tempfile::tempdir().unwrap();
        let native = "019d-phase3-native";
        let turn = "019d-phase3-turn";
        let path = directory
            .path()
            .join(format!("rollout-2026-07-27T00-00-00-{native}.jsonl"));
        std::fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"t\",\"type\":\"event_msg\",",
                "\"payload\":{\"type\":\"task_started\",\"turn_id\":\"other\"}}\n"
            ),
        )
        .unwrap();
        let raw = path.to_str().unwrap();
        assert!(!terminal_record_persisted(raw, native, turn).unwrap());

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        write!(
            file,
            "{{\"timestamp\":\"t\",\"type\":\"event_msg\",\"payload\":",
        )
        .unwrap();
        file.flush().unwrap();
        assert!(!terminal_record_persisted(raw, native, turn).unwrap());

        writeln!(
            file,
            "{{\"type\":\"task_complete\",\"turn_id\":\"{turn}\"}}}}"
        )
        .unwrap();
        file.flush().unwrap();
        assert!(terminal_record_persisted(raw, native, turn).unwrap());
        assert!(!terminal_record_persisted(raw, native, "wrong-turn").unwrap());
    }

    #[test]
    fn terminal_gate_rejects_noncanonical_or_malformed_rollout() {
        let directory = tempfile::tempdir().unwrap();
        let native = "native";
        let path = directory.path().join(format!("rollout-x-{native}.jsonl"));
        std::fs::write(&path, b"{not-json}\n").unwrap();
        assert_eq!(
            terminal_record_persisted(path.to_str().unwrap(), native, "turn"),
            Err(CodexContractError::InvalidTranscriptRecord)
        );

        let link = directory
            .path()
            .join(format!("rollout-link-{native}.jsonl"));
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(
            terminal_record_persisted(link.to_str().unwrap(), native, "turn"),
            Err(CodexContractError::InvalidTranscriptPath)
        );
    }
}
