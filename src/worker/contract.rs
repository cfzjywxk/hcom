use super::environment::ExactEnvironmentRequirement;
use super::result::{DeveloperResult, ReviewerResult};
use super::validation::{
    MAX_PATH_BYTES, validate_git_oid, validate_list, validate_opaque_id, validate_relative_path,
    validate_sha256, validate_text,
};
use crate::control_api::{CapabilitySnapshot, NativeSessionMode, WorkerRole};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_PROMPT_BYTES: usize = 256 * 1024;
const MAX_ARG_BYTES: usize = 4096;
const MAX_ARGV_BYTES: usize = 64 * 1024;
const MAX_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_NATIVE_STREAM_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResultTransport {
    Envelope,
    FinalFile,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdapterCapabilities {
    pub roles: Vec<WorkerRole>,
    pub native_session_mode: NativeSessionMode,
    pub result_transport: ResultTransport,
    pub features: Vec<String>,
}

impl AdapterCapabilities {
    fn validate(&self) -> Result<()> {
        if self.roles.is_empty() || self.roles.len() > 2 {
            bail!("adapter must support one or two worker roles");
        }
        let mut roles = BTreeSet::new();
        for role in &self.roles {
            let key = match role {
                WorkerRole::Developer => "developer",
                WorkerRole::Reviewer => "reviewer",
            };
            if !roles.insert(key) {
                bail!("adapter roles must be unique");
            }
        }
        validate_list("adapter features", &self.features)?;
        let mut features = BTreeSet::new();
        for feature in &self.features {
            validate_text("adapter feature", feature, 128, false)?;
            if !features.insert(feature) {
                bail!("adapter features must be unique");
            }
        }
        Ok(())
    }

    fn supports(&self, role: WorkerRole) -> bool {
        self.roles.contains(&role)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdapterDescriptor {
    pub name: String,
    pub contract_version: u32,
    pub cli_version: String,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub capabilities: AdapterCapabilities,
    pub capability_contract_hash: String,
}

impl AdapterDescriptor {
    pub fn new(
        name: impl Into<String>,
        contract_version: u32,
        cli_version: impl Into<String>,
        model: impl Into<String>,
        reasoning: impl Into<String>,
        policy: impl Into<String>,
        capabilities: AdapterCapabilities,
    ) -> Result<Self> {
        let mut descriptor = Self {
            name: name.into(),
            contract_version,
            cli_version: cli_version.into(),
            model: model.into(),
            reasoning: reasoning.into(),
            policy: policy.into(),
            capabilities,
            capability_contract_hash: String::new(),
        };
        descriptor.validate_without_hash()?;
        let canonical = serde_json::to_vec(&(
            &descriptor.name,
            descriptor.contract_version,
            &descriptor.cli_version,
            &descriptor.model,
            &descriptor.reasoning,
            &descriptor.policy,
            &descriptor.capabilities,
        ))?;
        descriptor.capability_contract_hash = sha256_hex(&canonical);
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_without_hash()?;
        validate_sha256(
            "adapter capability contract hash",
            &self.capability_contract_hash,
        )?;
        let expected = Self::new(
            self.name.clone(),
            self.contract_version,
            self.cli_version.clone(),
            self.model.clone(),
            self.reasoning.clone(),
            self.policy.clone(),
            self.capabilities.clone(),
        )?;
        if expected.capability_contract_hash != self.capability_contract_hash {
            bail!("adapter capability contract hash mismatch");
        }
        Ok(())
    }

    fn validate_without_hash(&self) -> Result<()> {
        validate_text("adapter name", &self.name, 64, false)?;
        if self.contract_version == 0 {
            bail!("adapter contract version must be positive");
        }
        validate_text("adapter CLI version", &self.cli_version, 128, false)?;
        validate_text("adapter model", &self.model, 256, false)?;
        validate_text("adapter reasoning", &self.reasoning, 64, false)?;
        validate_text("adapter policy", &self.policy, 2048, false)?;
        self.capabilities.validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutableIdentity {
    pub canonical_path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub sha256: String,
}

impl ExecutableIdentity {
    pub fn capture(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.is_absolute() {
            bail!("worker executable path must be absolute");
        }
        let path_text = path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("worker executable path must be valid UTF-8"))?;
        validate_text("worker executable path", path_text, MAX_PATH_BYTES, false)?;
        let link_metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect executable {}", path.display()))?;
        if link_metadata.file_type().is_symlink() || !link_metadata.is_file() {
            bail!("worker executable must be a regular non-symlink file");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("worker executable path must already be canonical");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = file.metadata()?;
        if metadata.dev() != link_metadata.dev()
            || metadata.ino() != link_metadata.ino()
            || metadata.len() > MAX_EXECUTABLE_BYTES
            || metadata.permissions().mode() & 0o111 == 0
        {
            bail!("worker executable identity or mode is invalid");
        }
        let mut hasher = Sha256::new();
        let mut copied = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            copied = copied
                .checked_add(count as u64)
                .ok_or_else(|| anyhow::anyhow!("worker executable size overflow"))?;
            if copied > MAX_EXECUTABLE_BYTES {
                bail!("worker executable exceeds its bound");
            }
            hasher.update(&buffer[..count]);
        }
        if copied != metadata.len() || copied > MAX_EXECUTABLE_BYTES {
            bail!("worker executable changed while hashing or exceeds its bound");
        }
        Ok(Self {
            canonical_path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            sha256: hex_bytes(&hasher.finalize()),
        })
    }

    pub fn revalidate(&self) -> Result<()> {
        if Self::capture(&self.canonical_path)? != *self {
            bail!("worker executable identity drifted");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerProfile {
    pub role: WorkerRole,
    pub adapter: String,
    pub model: String,
    pub reasoning: String,
    pub policy: String,
    pub executable: ExecutableIdentity,
    pub cli_version: String,
    pub adapter_contract_version: u32,
    pub native_session_mode: NativeSessionMode,
    pub capability: CapabilitySnapshot,
}

impl WorkerProfile {
    pub fn validate_for(&self, adapter: &dyn WorkerAdapter) -> Result<()> {
        let descriptor = adapter.descriptor();
        descriptor.validate()?;
        if self.adapter != descriptor.name
            || self.model != descriptor.model
            || self.reasoning != descriptor.reasoning
            || self.policy != descriptor.policy
            || self.cli_version != descriptor.cli_version
            || self.adapter_contract_version != descriptor.contract_version
            || self.native_session_mode != descriptor.capabilities.native_session_mode
            || !descriptor.capabilities.supports(self.role)
            || self.executable != *adapter.executable_contract()
            || self.capability.contract_hash != descriptor.capability_contract_hash
            || self.capability.features != descriptor.capabilities.features
        {
            bail!("worker profile does not exactly match the adapter contract");
        }
        validate_text("worker adapter", &self.adapter, 64, false)?;
        validate_text("worker model", &self.model, 256, false)?;
        validate_text("worker reasoning", &self.reasoning, 64, false)?;
        validate_text("worker policy", &self.policy, 2048, false)?;
        validate_text("worker CLI version", &self.cli_version, 128, false)?;
        self.executable.revalidate()
    }

    pub fn canonical_hash(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self)?;
        Ok(sha256_hex(&bytes))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TurnControl {
    pub project_id: String,
    pub task_id: String,
    pub role: WorkerRole,
    pub logical_session_id: String,
    pub native_session_id: Option<String>,
    pub turn_sequence: u32,
    pub attempt: u32,
    pub task_version: u64,
    pub review_round: u32,
    pub base_revision: String,
    pub head_revision: Option<String>,
    pub artifact_dir: String,
}

impl TurnControl {
    pub fn validate(&self) -> Result<()> {
        validate_opaque_id("project_id", &self.project_id)?;
        validate_opaque_id("task_id", &self.task_id)?;
        validate_opaque_id("logical_session_id", &self.logical_session_id)?;
        if let Some(native_session_id) = &self.native_session_id {
            validate_native_session_id(native_session_id)?;
        }
        if self.turn_sequence == 0 || self.attempt == 0 || self.task_version == 0 {
            bail!("turn sequence, attempt, and task version must be positive");
        }
        validate_git_oid("turn base_revision", &self.base_revision)?;
        if let Some(head) = &self.head_revision {
            validate_git_oid("turn head_revision", head)?;
        }
        if self.role == WorkerRole::Reviewer
            && (self.review_round == 0 || self.head_revision.is_none())
        {
            bail!("reviewer turn must bind a positive round and exact head revision");
        }
        validate_relative_path("turn artifact_dir", &self.artifact_dir)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SchemaTransport {
    None,
    InlineArgument {
        flag: String,
        json: String,
    },
    File {
        argument: String,
        relative_path: String,
        contents: Vec<u8>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum NativeOutputKind {
    StdoutEnvelope,
    FinalFile,
    Activity,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OutputDeclaration {
    pub kind: NativeOutputKind,
    pub relative_path: String,
    pub max_bytes: usize,
    pub output_argument: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OuterLaunchEnvelope {
    pub executable: ExecutableIdentity,
    pub fixed_argv: Vec<String>,
    pub expected_artifact_dir: PathBuf,
}

impl OuterLaunchEnvelope {
    fn validate(&self) -> Result<()> {
        self.executable.revalidate()?;
        validate_argv("outer launch argv", &self.fixed_argv)?;
        if self.fixed_argv.iter().any(|argument| argument == "--") {
            bail!("outer launch argv cannot contain the native command separator");
        }
        validate_absolute_lexical_path(
            "outer launch expected artifact directory",
            &self.expected_artifact_dir,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub executable: ExecutableIdentity,
    pub fixed_argv: Vec<String>,
    pub schema_transport: SchemaTransport,
    pub expected_outputs: Vec<OutputDeclaration>,
    pub stdin_prompt_argument: Option<String>,
    pub workspace_cwd: PathBuf,
    pub outer_launch: Option<OuterLaunchEnvelope>,
    pub exact_environment: Vec<ExactEnvironmentRequirement>,
}

impl CommandSpec {
    pub fn validate(&self) -> Result<()> {
        self.executable.revalidate()?;
        validate_argv("fixed argv", &self.fixed_argv)?;
        match &self.schema_transport {
            SchemaTransport::None => {}
            SchemaTransport::InlineArgument { flag, json } => {
                validate_text("schema flag", flag, 128, false)?;
                validate_json_schema(json.as_bytes())?;
            }
            SchemaTransport::File {
                argument,
                relative_path,
                contents,
            } => {
                validate_text("schema file argument", argument, 128, false)?;
                validate_relative_path("schema relative path", relative_path)?;
                validate_json_schema(contents)?;
            }
        }
        validate_list("expected outputs", &self.expected_outputs)?;
        let mut kinds = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for output in &self.expected_outputs {
            validate_relative_path("expected output path", &output.relative_path)?;
            if output.max_bytes == 0 || output.max_bytes > MAX_NATIVE_STREAM_BYTES {
                bail!("expected output bound is invalid");
            }
            match (output.kind, output.output_argument.as_deref()) {
                (NativeOutputKind::FinalFile, Some(argument)) => {
                    validate_text("final output argument", argument, 128, false)?;
                }
                (NativeOutputKind::FinalFile, None) => {
                    bail!("final-file output requires an explicit path argument");
                }
                (_, Some(_)) => {
                    bail!("only final-file output may declare a path argument");
                }
                (_, None) => {}
            }
            if !kinds.insert(output.kind) || !paths.insert(&output.relative_path) {
                bail!("expected output kinds and paths must be unique");
            }
        }
        if let Some(argument) = &self.stdin_prompt_argument {
            validate_text("stdin prompt argument", argument, 128, false)?;
        }
        let cwd = self
            .workspace_cwd
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("worker cwd must be valid UTF-8"))?;
        validate_text("worker cwd", cwd, MAX_PATH_BYTES, false)?;
        if !self.workspace_cwd.is_absolute()
            || fs::canonicalize(&self.workspace_cwd)? != self.workspace_cwd
            || !self.workspace_cwd.is_dir()
        {
            bail!("worker cwd must be an existing canonical directory");
        }
        if let Some(outer) = &self.outer_launch {
            outer.validate()?;
            if outer.executable == self.executable {
                bail!("outer launch executable must be distinct from the native executable");
            }
        }
        let mut previous = None;
        for requirement in &self.exact_environment {
            if previous.is_some_and(|name| name >= requirement.name()) {
                bail!("exact environment requirements must use unique canonical order");
            }
            previous = Some(requirement.name());
        }
        Ok(())
    }

    pub fn materialized_control_argv(&self) -> Vec<String> {
        let native = self.materialized_native_control_argv();
        let Some(outer) = &self.outer_launch else {
            return native;
        };
        let mut argv = outer.fixed_argv.clone();
        argv.push("--".into());
        argv.push(
            self.executable
                .canonical_path
                .to_string_lossy()
                .into_owned(),
        );
        argv.extend(native);
        argv
    }

    pub(crate) fn materialized_native_control_argv(&self) -> Vec<String> {
        let mut argv = self.fixed_argv.clone();
        match &self.schema_transport {
            SchemaTransport::None => {}
            SchemaTransport::InlineArgument { flag, json } => {
                argv.push(flag.clone());
                argv.push(json.clone());
            }
            SchemaTransport::File {
                argument,
                relative_path,
                ..
            } => {
                argv.push(argument.clone());
                argv.push(relative_path.clone());
            }
        }
        for output in &self.expected_outputs {
            if let Some(argument) = &output.output_argument {
                argv.push(argument.clone());
                argv.push(output.relative_path.clone());
            }
        }
        if let Some(argument) = &self.stdin_prompt_argument {
            argv.push(argument.clone());
        }
        argv
    }

    fn rejects_prompt_copy(&self, prompt: &[u8]) -> Result<()> {
        if prompt.len() < 16 {
            return Ok(());
        }
        for argument in self.materialized_control_argv() {
            if contains_bytes(argument.as_bytes(), prompt) {
                bail!("worker prompt must not appear in command argv");
            }
        }
        let schema_bytes = match &self.schema_transport {
            SchemaTransport::InlineArgument { json, .. } => Some(json.as_bytes()),
            SchemaTransport::File { contents, .. } => Some(contents.as_slice()),
            SchemaTransport::None => None,
        };
        if schema_bytes.is_some_and(|bytes| contains_bytes(bytes, prompt)) {
            bail!("worker prompt must not appear in adapter schema transport");
        }
        Ok(())
    }
}

pub struct PreparedTurn {
    command: CommandSpec,
    prompt: Vec<u8>,
}

impl PreparedTurn {
    pub fn command(&self) -> &CommandSpec {
        &self.command
    }

    pub fn prompt(&self) -> &[u8] {
        &self.prompt
    }

    pub fn into_parts(self) -> (CommandSpec, Vec<u8>) {
        (self.command, self.prompt)
    }
}

#[derive(Clone)]
pub struct NativeArtifacts {
    role: WorkerRole,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    final_output: Option<Vec<u8>>,
}

impl NativeArtifacts {
    pub fn new(
        role: WorkerRole,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        final_output: Option<Vec<u8>>,
    ) -> Result<Self> {
        for (label, bytes) in [("native stdout", &stdout), ("native stderr", &stderr)] {
            if bytes.len() > MAX_NATIVE_STREAM_BYTES {
                bail!("{label} exceeds its hard cap");
            }
        }
        if final_output
            .as_ref()
            .is_some_and(|bytes| bytes.len() > super::result::MAX_RESULT_BYTES)
        {
            bail!("native final output exceeds its hard cap");
        }
        Ok(Self {
            role,
            stdout,
            stderr,
            final_output,
        })
    }

    pub fn role(&self) -> WorkerRole {
        self.role
    }

    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }

    pub fn final_output(&self) -> Option<&[u8]> {
        self.final_output.as_deref()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum NativeObservation {
    SessionStarted { native_session_id: String },
    Activity { kind: String, message: String },
}

pub enum NativeResult {
    Developer {
        native_session_id: String,
        result: DeveloperResult,
    },
    Reviewer {
        native_session_id: String,
        result: ReviewerResult,
    },
}

impl NativeResult {
    pub fn native_session_id(&self) -> &str {
        match self {
            Self::Developer {
                native_session_id, ..
            }
            | Self::Reviewer {
                native_session_id, ..
            } => native_session_id,
        }
    }

    pub fn role(&self) -> WorkerRole {
        match self {
            Self::Developer { .. } => WorkerRole::Developer,
            Self::Reviewer { .. } => WorkerRole::Reviewer,
        }
    }
}

pub struct NativeSessionBinding {
    role: WorkerRole,
    mode: NativeSessionMode,
    native_session_id: Option<String>,
    observed: bool,
    sealed: bool,
}

impl NativeSessionBinding {
    pub fn new(
        role: WorkerRole,
        mode: NativeSessionMode,
        preassigned: Option<String>,
    ) -> Result<Self> {
        match (mode, preassigned.as_ref()) {
            (NativeSessionMode::Preassigned, Some(id)) => validate_native_session_id(id)?,
            (NativeSessionMode::Preassigned, None) => {
                bail!("preassigned session mode requires an exact native session id");
            }
            (NativeSessionMode::Discovered, None) => {}
            (NativeSessionMode::Discovered, Some(_)) => {
                bail!("discovered session mode must start without a native session id");
            }
        }
        Ok(Self {
            role,
            mode,
            native_session_id: preassigned,
            observed: false,
            sealed: false,
        })
    }

    pub fn observe(&mut self, observation: &NativeObservation) -> Result<()> {
        if self.sealed {
            bail!("native session observation arrived after result sealing");
        }
        let NativeObservation::SessionStarted { native_session_id } = observation else {
            return Ok(());
        };
        validate_native_session_id(native_session_id)?;
        if self.observed {
            bail!("native session may be observed exactly once");
        }
        match &self.native_session_id {
            Some(expected) if expected == native_session_id => {}
            None if self.mode == NativeSessionMode::Discovered => {
                self.native_session_id = Some(native_session_id.clone());
            }
            _ => bail!("native session observation does not match its binding mode"),
        }
        self.observed = true;
        Ok(())
    }

    pub fn require_resume_id(&self, native_session_id: &str) -> Result<()> {
        validate_native_session_id(native_session_id)?;
        if !self.sealed || self.native_session_id.as_deref() != Some(native_session_id) {
            bail!("resume requires the exact sealed native session id");
        }
        Ok(())
    }

    pub fn begin_resume(&mut self, native_session_id: &str) -> Result<()> {
        self.require_resume_id(native_session_id)?;
        self.observed = false;
        self.sealed = false;
        Ok(())
    }

    pub fn seal_result(&mut self, result: &NativeResult) -> Result<()> {
        if self.sealed
            || result.role() != self.role
            || self.native_session_id.as_deref() != Some(result.native_session_id())
        {
            bail!("native result does not match the exact session binding");
        }
        self.sealed = true;
        Ok(())
    }

    pub fn native_session_id(&self) -> Option<&str> {
        self.native_session_id.as_deref()
    }
}

pub trait WorkerAdapter: Send + Sync {
    fn descriptor(&self) -> &AdapterDescriptor;
    fn executable_contract(&self) -> &ExecutableIdentity;
    fn build_create(&self, control: &TurnControl) -> Result<CommandSpec>;
    fn build_resume(&self, native_session_id: &str, control: &TurnControl) -> Result<CommandSpec>;
    fn observe_native_record(&self, record: &[u8]) -> Result<Vec<NativeObservation>>;
    fn extract_result(
        &self,
        control: &TurnControl,
        artifacts: &NativeArtifacts,
    ) -> Result<NativeResult>;
}

#[derive(Default)]
pub struct WorkerAdapterRegistry {
    adapters: BTreeMap<String, Arc<dyn WorkerAdapter>>,
}

impl WorkerAdapterRegistry {
    pub fn register(&mut self, adapter: Arc<dyn WorkerAdapter>) -> Result<()> {
        let name = adapter.descriptor().name.clone();
        adapter.descriptor().validate()?;
        if self.adapters.contains_key(&name) {
            bail!("worker adapter {name} is already registered");
        }
        self.adapters.insert(name, adapter);
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Result<Arc<dyn WorkerAdapter>> {
        self.adapters
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown or disabled worker adapter"))
    }
}

pub fn prepare_create_turn(
    adapter: &dyn WorkerAdapter,
    profile: &WorkerProfile,
    control: &TurnControl,
    prompt: Vec<u8>,
) -> Result<PreparedTurn> {
    prepare_turn(adapter, profile, control, None, prompt)
}

pub fn prepare_resume_turn(
    adapter: &dyn WorkerAdapter,
    profile: &WorkerProfile,
    control: &TurnControl,
    native_session_id: &str,
    prompt: Vec<u8>,
) -> Result<PreparedTurn> {
    validate_native_session_id(native_session_id)?;
    prepare_turn(adapter, profile, control, Some(native_session_id), prompt)
}

fn prepare_turn(
    adapter: &dyn WorkerAdapter,
    profile: &WorkerProfile,
    control: &TurnControl,
    resume_session_id: Option<&str>,
    prompt: Vec<u8>,
) -> Result<PreparedTurn> {
    profile.validate_for(adapter)?;
    control.validate()?;
    if profile.role != control.role {
        bail!("turn role does not match its pinned worker profile");
    }
    validate_prompt(&prompt)?;
    let command = match resume_session_id {
        Some(session_id) => {
            if control.native_session_id.as_deref() != Some(session_id) {
                bail!("resume turn control must bind the exact native session id");
            }
            adapter.build_resume(session_id, control)?
        }
        None => {
            match profile.native_session_mode {
                NativeSessionMode::Preassigned if control.native_session_id.is_none() => {
                    bail!("preassigned create turn requires a native session id");
                }
                NativeSessionMode::Discovered if control.native_session_id.is_some() => {
                    bail!("discovered create turn cannot pre-bind a native session id");
                }
                NativeSessionMode::Preassigned | NativeSessionMode::Discovered => {}
            }
            adapter.build_create(control)?
        }
    };
    command.validate()?;
    if command.executable != profile.executable {
        bail!("adapter command executable differs from the pinned profile");
    }
    command.rejects_prompt_copy(&prompt)?;
    Ok(PreparedTurn { command, prompt })
}

fn validate_prompt(prompt: &[u8]) -> Result<()> {
    if prompt.is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        bail!("worker prompt exceeds its bounded stdin size");
    }
    let prompt = std::str::from_utf8(prompt).context("worker prompt must be valid UTF-8")?;
    validate_text("worker prompt", prompt, MAX_PROMPT_BYTES, true)
}

fn validate_json_schema(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_SCHEMA_BYTES {
        bail!("adapter schema transport exceeds its bound");
    }
    let text = std::str::from_utf8(bytes).context("adapter schema must be valid UTF-8")?;
    validate_text("adapter schema", text, MAX_SCHEMA_BYTES, false)?;
    let value: serde_json::Value =
        serde_json::from_slice(bytes).context("adapter schema is not valid JSON")?;
    if !value.is_object() {
        bail!("adapter schema must be a JSON object");
    }
    Ok(())
}

fn validate_argv(label: &str, argv: &[String]) -> Result<()> {
    validate_list(label, argv)?;
    let mut total = 0usize;
    for argument in argv {
        validate_text(label, argument, MAX_ARG_BYTES, false)?;
        total = total
            .checked_add(argument.len())
            .ok_or_else(|| anyhow::anyhow!("{label} length overflow"))?;
    }
    if total > MAX_ARGV_BYTES {
        bail!("{label} exceeds its aggregate bound");
    }
    Ok(())
}

fn validate_absolute_lexical_path(label: &str, path: &Path) -> Result<()> {
    let text = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("{label} must be valid UTF-8"))?;
    validate_text(label, text, MAX_PATH_BYTES, false)?;
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::RootDir))
        || !components.all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        bail!("{label} must be an absolute normalized path");
    }
    Ok(())
}

pub(crate) fn validate_native_session_id(value: &str) -> Result<()> {
    validate_text("native session id", value, 256, false)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("native session id is not a bounded opaque identifier");
    }
    Ok(())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::fake::FakeWorkerAdapter;
    use std::os::unix::fs::PermissionsExt;

    fn executable(temp: &tempfile::TempDir) -> ExecutableIdentity {
        let path = temp.path().join("fake-worker");
        fs::write(&path, b"fake executable").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableIdentity::capture(&path).unwrap()
    }

    fn control(_temp: &tempfile::TempDir, role: WorkerRole) -> TurnControl {
        TurnControl {
            project_id: "project-1".into(),
            task_id: "task-1".into(),
            role,
            logical_session_id: "logical-1".into(),
            native_session_id: Some("native-preassigned".into()),
            turn_sequence: 1,
            attempt: 1,
            task_version: 2,
            review_round: 0,
            base_revision: std::iter::repeat_n('a', 40).collect(),
            head_revision: None,
            artifact_dir: "project-1/task-1/developer/session-1/turn-1/attempt-1".into(),
        }
    }

    #[test]
    fn prompt_is_core_owned_and_never_materialized_in_argv_or_schema() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        let adapter = FakeWorkerAdapter::preassigned(executable(&temp), cwd).unwrap();
        let profile = adapter.profile(WorkerRole::Developer);
        let prompt = b"private task body sentinel 9e8d356b".to_vec();
        let prepared = prepare_create_turn(
            &adapter,
            &profile,
            &control(&temp, WorkerRole::Developer),
            prompt.clone(),
        )
        .unwrap();
        assert_eq!(prepared.prompt(), prompt);
        let joined = prepared.command().materialized_control_argv().join("\0");
        assert!(!joined.contains("private task body sentinel"));
        assert!(
            !prepared
                .command()
                .materialized_control_argv()
                .iter()
                .any(|argument| argument
                    .as_bytes()
                    .windows(prompt.len())
                    .any(|window| window == prompt))
        );
    }

    #[test]
    fn profile_and_executable_drift_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        let adapter = FakeWorkerAdapter::preassigned(executable(&temp), cwd).unwrap();
        let control = control(&temp, WorkerRole::Developer);
        let original = adapter.profile(WorkerRole::Developer);
        let mut profiles = Vec::new();
        let mut profile = original.clone();
        profile.adapter = "drifted-adapter".into();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.model = "drifted-model".into();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.reasoning = "drifted-reasoning".into();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.policy = "drifted-policy".into();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.cli_version = "drifted-version".into();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.adapter_contract_version += 1;
        profiles.push(profile);
        let mut profile = original.clone();
        profile.native_session_mode = NativeSessionMode::Discovered;
        profiles.push(profile);
        let mut profile = original.clone();
        profile.capability.contract_hash = std::iter::repeat_n('0', 64).collect();
        profiles.push(profile);
        let mut profile = original.clone();
        profile.capability.features.push("drifted-feature".into());
        profiles.push(profile);
        let mut profile = original.clone();
        profile.executable.canonical_path = temp.path().join("missing-worker");
        profiles.push(profile);
        for profile in profiles {
            assert!(
                prepare_create_turn(
                    &adapter,
                    &profile,
                    &control,
                    b"bounded prompt body".to_vec()
                )
                .is_err()
            );
        }

        let mut profile = adapter.profile(WorkerRole::Developer);
        fs::write(&profile.executable.canonical_path, b"changed executable").unwrap();
        assert!(profile.executable.revalidate().is_err());
        profile.executable =
            ExecutableIdentity::capture(&profile.executable.canonical_path).unwrap();
        assert!(profile.validate_for(&adapter).is_err());
    }

    #[test]
    fn session_binding_rejects_missing_wrong_duplicate_and_late_ids() {
        let mut discovered =
            NativeSessionBinding::new(WorkerRole::Developer, NativeSessionMode::Discovered, None)
                .unwrap();
        let observation = NativeObservation::SessionStarted {
            native_session_id: "native-1".into(),
        };
        discovered.observe(&observation).unwrap();
        assert!(discovered.observe(&observation).is_err());
        assert!(discovered.require_resume_id("native-wrong").is_err());
        let result = NativeResult::Developer {
            native_session_id: "native-1".into(),
            result: DeveloperResult {
                decision: super::super::result::DeveloperDecision::Blocked,
                summary: "bounded failure".into(),
                head_revision: None,
                commits: vec![],
                checks: vec![],
                questions: vec![],
                risks: vec![],
                changed_paths: vec![],
            },
        };
        discovered.seal_result(&result).unwrap();
        assert!(discovered.observe(&observation).is_err());
        discovered.require_resume_id("native-1").unwrap();
        assert!(discovered.begin_resume("native-wrong").is_err());
        discovered.begin_resume("native-1").unwrap();
        discovered.observe(&observation).unwrap();
        discovered.seal_result(&result).unwrap();
        assert!(discovered.begin_resume("native-1").is_ok());

        assert!(
            NativeSessionBinding::new(WorkerRole::Developer, NativeSessionMode::Preassigned, None)
                .is_err()
        );
        assert!(
            NativeSessionBinding::new(
                WorkerRole::Developer,
                NativeSessionMode::Discovered,
                Some("native-1".into())
            )
            .is_err()
        );

        let mut preassigned = NativeSessionBinding::new(
            WorkerRole::Developer,
            NativeSessionMode::Preassigned,
            Some("native-preassigned".into()),
        )
        .unwrap();
        let wrong = NativeResult::Developer {
            native_session_id: "native-other".into(),
            result: DeveloperResult {
                decision: super::super::result::DeveloperDecision::Blocked,
                summary: "bounded mismatch".into(),
                head_revision: None,
                commits: vec![],
                checks: vec![],
                questions: vec![],
                risks: vec![],
                changed_paths: vec![],
            },
        };
        assert!(preassigned.seal_result(&wrong).is_err());
        let wrong_role = NativeResult::Reviewer {
            native_session_id: "native-preassigned".into(),
            result: ReviewerResult {
                decision: super::super::result::ReviewDecision::Lgtm,
                summary: "bounded wrong role".into(),
                findings: vec![],
                checks: vec![],
            },
        };
        assert!(preassigned.seal_result(&wrong_role).is_err());
        let matching = NativeObservation::SessionStarted {
            native_session_id: "native-preassigned".into(),
        };
        preassigned.observe(&matching).unwrap();
        let valid = NativeResult::Developer {
            native_session_id: "native-preassigned".into(),
            result: DeveloperResult {
                decision: super::super::result::DeveloperDecision::Blocked,
                summary: "bounded failure".into(),
                head_revision: None,
                commits: vec![],
                checks: vec![],
                questions: vec![],
                risks: vec![],
                changed_paths: vec![],
            },
        };
        preassigned.seal_result(&valid).unwrap();
        preassigned.begin_resume("native-preassigned").unwrap();
        assert!(preassigned.observe(&observation).is_err());
    }

    #[test]
    fn unknown_and_duplicate_adapters_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        let adapter = Arc::new(FakeWorkerAdapter::preassigned(executable(&temp), cwd).unwrap());
        let mut registry = WorkerAdapterRegistry::default();
        registry.register(adapter.clone()).unwrap();
        assert!(registry.register(adapter).is_err());
        assert!(registry.resolve("missing-adapter").is_err());
    }

    #[test]
    fn native_artifact_inputs_are_bounded_before_adapter_parsing() {
        assert!(
            NativeArtifacts::new(
                WorkerRole::Developer,
                vec![0; MAX_NATIVE_STREAM_BYTES + 1],
                vec![],
                None,
            )
            .is_err()
        );
        assert!(
            NativeArtifacts::new(
                WorkerRole::Reviewer,
                vec![],
                vec![0; MAX_NATIVE_STREAM_BYTES + 1],
                None,
            )
            .is_err()
        );
        assert!(
            NativeArtifacts::new(
                WorkerRole::Reviewer,
                vec![],
                vec![],
                Some(vec![0; super::super::result::MAX_RESULT_BYTES + 1]),
            )
            .is_err()
        );
    }
}
