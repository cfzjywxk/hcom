//! Private, bounded artifacts for one foreground-session worker turn attempt.

use crate::control_api::WorkerRole;
use crate::worker::contract::{MAX_PROMPT_BYTES, validate_native_session_id};
use crate::worker::environment::{ExecutionEnvironmentLease, SecretRedactor};
use crate::worker::result::{DeveloperResult, ReviewerResult};
use crate::worker::validation::{
    validate_git_oid, validate_opaque_id, validate_relative_path, validate_sha256, validate_text,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub const MAX_NATIVE_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_ACTIVITY_ARTIFACT_BYTES: u64 = 512 * 1024;
pub const MAX_RESULT_ARTIFACT_BYTES: u64 = 256 * 1024;
pub const MAX_MANIFEST_ARTIFACT_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// The exact prompt handed to the worker. Streamed rather than written as
    /// a control file so a legal full-size prompt cannot fail the turn.
    NativePrompt,
    NativeStdout,
    NativeStderr,
    NativeFinal,
    Activity,
    Result,
}

impl ArtifactKind {
    pub fn file_name(self) -> &'static str {
        match self {
            Self::NativePrompt => "prompt.md",
            Self::NativeStdout => "native.stdout.partial",
            Self::NativeStderr => "native.stderr.partial",
            Self::NativeFinal => "native-final.partial",
            Self::Activity => "activity.ndjson",
            Self::Result => "result.json",
        }
    }

    fn hard_cap(self) -> u64 {
        match self {
            Self::NativePrompt | Self::NativeStdout | Self::NativeStderr | Self::NativeFinal => {
                MAX_NATIVE_ARTIFACT_BYTES
            }
            Self::Activity => MAX_ACTIVITY_ARTIFACT_BYTES,
            Self::Result => MAX_RESULT_ARTIFACT_BYTES,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactScope {
    pub run_id: String,
    pub task_id: String,
    pub role: WorkerRole,
    pub logical_session_id: String,
    pub turn_sequence: u32,
    pub attempt: u32,
}

impl ArtifactScope {
    pub fn validate(&self) -> Result<()> {
        validate_opaque_id("artifact run id", &self.run_id)?;
        validate_opaque_id("artifact task id", &self.task_id)?;
        validate_opaque_id("artifact logical session id", &self.logical_session_id)?;
        if self.turn_sequence == 0 || self.attempt == 0 {
            bail!("artifact turn sequence and attempt must be positive");
        }
        Ok(())
    }

    pub fn relative_path(&self) -> String {
        format!(
            "{}/{}/{}/{}/turn-{}/attempt-{}",
            self.run_id,
            self.task_id,
            role_name(self.role),
            self.logical_session_id,
            self.turn_sequence,
            self.attempt
        )
    }
}

pub struct ArtifactRoot {
    canonical_path: PathBuf,
    directory: File,
}

impl ArtifactRoot {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.is_absolute() {
            bail!("artifact root must be an absolute path");
        }
        let path_metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect artifact root {}", path.display()))?;
        if path_metadata.file_type().is_symlink() || !path_metadata.is_dir() {
            bail!("artifact root must be a real directory");
        }
        let canonical = fs::canonicalize(path)
            .with_context(|| format!("failed to canonicalize artifact root {}", path.display()))?;
        if canonical != path {
            bail!("artifact root path must already be canonical and contain no symlink");
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("failed to open artifact root {}", path.display()))?;
        verify_private_directory(&directory, "artifact root")?;
        let opened_metadata = directory.metadata()?;
        if opened_metadata.dev() != path_metadata.dev()
            || opened_metadata.ino() != path_metadata.ino()
        {
            bail!("artifact root changed while it was opened");
        }
        Ok(Self {
            canonical_path: canonical,
            directory,
        })
    }

    pub fn path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn load_turn_manifest(&self, scope: &ArtifactScope) -> Result<TurnManifest> {
        scope.validate()?;
        let relative_path = scope.relative_path();
        validate_relative_path("artifact attempt path", &relative_path)?;
        let expected = self.canonical_path.join(&relative_path);
        let canonical = fs::canonicalize(&expected)
            .context("failed to canonicalize persisted artifact attempt")?;
        if canonical != expected || !canonical.starts_with(&self.canonical_path) {
            bail!("persisted artifact attempt escaped its canonical root");
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&canonical)?;
        verify_private_directory(&directory, "persisted artifact attempt")?;
        let manifest_file = open_private_file(
            directory.as_raw_fd(),
            "manifest.json",
            MAX_MANIFEST_ARTIFACT_BYTES,
        )?;
        let mut encoded = Vec::new();
        (&manifest_file)
            .take(MAX_MANIFEST_ARTIFACT_BYTES + 1)
            .read_to_end(&mut encoded)?;
        if encoded.is_empty() || encoded.len() as u64 > MAX_MANIFEST_ARTIFACT_BYTES {
            bail!("persisted artifact manifest exceeds its bound");
        }
        let manifest: TurnManifest =
            serde_json::from_slice(&encoded).context("persisted artifact manifest is malformed")?;
        manifest.validate()?;
        if manifest.run_id != scope.run_id
            || manifest.task_id != scope.task_id
            || manifest.role != scope.role
            || manifest.logical_session_id != scope.logical_session_id
            || manifest.turn_sequence != scope.turn_sequence
            || manifest.attempt != scope.attempt
        {
            bail!("persisted artifact manifest scope mismatch");
        }
        for receipt in &manifest.artifacts {
            verify_receipt(&directory, receipt)?;
        }
        Ok(manifest)
    }
}

pub struct ArtifactAttempt {
    scope: ArtifactScope,
    relative_path: String,
    directory_path: PathBuf,
    directory: Arc<File>,
    supervisor_epoch: String,
    environment_hash: String,
    redactor: Arc<SecretRedactor>,
    registry: Arc<Mutex<ReceiptRegistry>>,
}

impl ArtifactAttempt {
    /// Create an attempt whose redactor also treats the turn prompt as
    /// sensitive. Use this when the prompt itself may carry private material.
    pub fn create(
        root: &ArtifactRoot,
        scope: ArtifactScope,
        environment: &ExecutionEnvironmentLease,
        prompt: &[u8],
    ) -> Result<Self> {
        Self::create_with_prompt_secrecy(root, scope, environment, prompt, true)
    }

    /// Create an attempt that redacts only the environment's secrets.
    ///
    /// The exec lane's prompt is an hcom-generated task description, not a
    /// credential: seeding the redactor with it would replace the prompt
    /// evidence with `[REDACTED]` and destroy reproducibility. Real secrets
    /// live in the environment inventory and are still redacted everywhere.
    pub fn create_with_environment_secrets_only(
        root: &ArtifactRoot,
        scope: ArtifactScope,
        environment: &ExecutionEnvironmentLease,
        prompt: &[u8],
    ) -> Result<Self> {
        Self::create_with_prompt_secrecy(root, scope, environment, prompt, false)
    }

    fn create_with_prompt_secrecy(
        root: &ArtifactRoot,
        scope: ArtifactScope,
        environment: &ExecutionEnvironmentLease,
        prompt: &[u8],
        prompt_is_secret: bool,
    ) -> Result<Self> {
        scope.validate()?;
        environment.descriptor().validate()?;
        if prompt.is_empty() || prompt.len() > MAX_PROMPT_BYTES {
            bail!("artifact prompt redaction input exceeds its bound");
        }
        let prompt =
            std::str::from_utf8(prompt).context("artifact prompt redaction input is not UTF-8")?;
        validate_text(
            "artifact prompt redaction input",
            prompt,
            MAX_PROMPT_BYTES,
            true,
        )?;
        let relative_path = scope.relative_path();
        validate_relative_path("artifact attempt path", &relative_path)?;
        let components: Vec<_> = Path::new(&relative_path)
            .components()
            .map(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .expect("validated path is UTF-8")
                    .to_owned()
            })
            .collect();
        let mut parent = root.directory.try_clone()?;
        for (index, component) in components.iter().enumerate() {
            parent = create_or_open_private_directory(
                &parent,
                component,
                index + 1 == components.len(),
            )?;
        }
        parent.sync_all()?;
        Ok(Self {
            scope,
            directory_path: root.canonical_path.join(&relative_path),
            relative_path,
            directory: Arc::new(parent),
            supervisor_epoch: environment.descriptor().supervisor_epoch.clone(),
            environment_hash: environment.descriptor().environment_hash.clone(),
            redactor: Arc::new(if prompt_is_secret {
                environment.redactor().with_value(prompt)
            } else {
                environment.redactor()
            }),
            registry: Arc::new(Mutex::new(ReceiptRegistry::default())),
        })
    }

    pub fn scope(&self) -> &ArtifactScope {
        &self.scope
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn directory_path(&self) -> &Path {
        &self.directory_path
    }

    pub fn artifact_path(&self, kind: ArtifactKind) -> PathBuf {
        self.directory_path.join(kind.file_name())
    }

    pub fn write_control_file(&self, relative_path: &str, contents: &[u8]) -> Result<PathBuf> {
        validate_relative_path("adapter control file", relative_path)?;
        if Path::new(relative_path).components().count() != 1
            || contents.is_empty()
            || contents.len() > 64 * 1024
            || is_reserved_artifact_name(relative_path)
            || relative_path == "manifest.json"
        {
            bail!("adapter control file shape is invalid");
        }
        let mut file = create_private_file(self.directory.as_raw_fd(), relative_path)?;
        file.write_all(contents)?;
        file.flush()?;
        file.sync_all()?;
        verify_private_regular_file(&file, relative_path)?;
        Ok(self.directory_path.join(relative_path))
    }

    pub fn ingest_native_final(&self, relative_path: &str, hard_cap: u64) -> Result<Vec<u8>> {
        validate_relative_path("native final output", relative_path)?;
        if relative_path != ArtifactKind::NativeFinal.file_name()
            || hard_cap == 0
            || hard_cap > ArtifactKind::NativeFinal.hard_cap()
        {
            bail!("native final output shape is invalid");
        }
        let mut file = open_private_file(self.directory.as_raw_fd(), relative_path, hard_cap)?;
        let mut raw = Vec::new();
        (&mut file).take(hard_cap + 1).read_to_end(&mut raw)?;
        if raw.is_empty() || raw.len() as u64 > hard_cap {
            bail!("native final output exceeds its declared bound");
        }
        verify_private_regular_file(&file, relative_path)?;
        if file.metadata()?.len() != raw.len() as u64 {
            bail!("native final output changed while it was ingested");
        }
        drop(file);
        unlink_required(self.directory.as_raw_fd(), relative_path)?;

        let sanitized = sanitize_untrusted(&raw, &self.redactor);
        let accepted = utf8_prefix(
            sanitized.as_bytes(),
            usize::try_from(hard_cap).context("native final cap does not fit usize")?,
        );
        let truncated = accepted.len() < sanitized.len();
        reserve_receipt(&self.registry, ArtifactKind::NativeFinal)?;
        let receipt = atomic_write(
            &self.directory,
            ArtifactKind::NativeFinal,
            accepted,
            truncated,
        )?;
        complete_receipt(&self.registry, receipt)?;
        Ok(raw)
    }

    pub fn start_native_stream(
        &self,
        kind: ArtifactKind,
        hard_cap: u64,
    ) -> Result<BoundedArtifactWriter> {
        if !matches!(
            kind,
            ArtifactKind::NativePrompt
                | ArtifactKind::NativeStdout
                | ArtifactKind::NativeStderr
                | ArtifactKind::NativeFinal
        ) {
            bail!("only native output kinds use a bounded stream writer");
        }
        if hard_cap == 0 || hard_cap > kind.hard_cap() {
            bail!("native artifact cap exceeds the contract maximum");
        }
        reserve_receipt(&self.registry, kind)?;
        let file = create_private_file(self.directory.as_raw_fd(), kind.file_name())?;
        Ok(BoundedArtifactWriter {
            kind,
            file,
            hard_cap,
            carry: Vec::new(),
            written: 0,
            hasher: Sha256::new(),
            truncated: false,
            redactor: self.redactor.clone(),
            registry: self.registry.clone(),
        })
    }

    pub fn start_activity_log(&self, hard_cap: u64) -> Result<ActivityLogWriter> {
        if hard_cap == 0 || hard_cap > MAX_ACTIVITY_ARTIFACT_BYTES {
            bail!("activity artifact cap exceeds the contract maximum");
        }
        let marker = activity_truncation_line(u64::MAX)?;
        if marker.len() as u64 > hard_cap {
            bail!("activity artifact cap cannot hold its truncation marker");
        }
        reserve_receipt(&self.registry, ArtifactKind::Activity)?;
        let file = create_private_file(
            self.directory.as_raw_fd(),
            ArtifactKind::Activity.file_name(),
        )?;
        Ok(ActivityLogWriter {
            file,
            hard_cap,
            bytes_written: 0,
            next_sequence: 1,
            truncated: false,
            hasher: Sha256::new(),
            redactor: self.redactor.clone(),
            registry: self.registry.clone(),
        })
    }

    pub fn write_result_json(&self, bytes: &[u8]) -> Result<ArtifactReceipt> {
        let canonical = match self.scope.role {
            WorkerRole::Developer => DeveloperResult::parse(bytes)?.canonical_json()?,
            WorkerRole::Reviewer => ReviewerResult::parse(bytes)?.canonical_json()?,
        };
        let value: serde_json::Value = serde_json::from_slice(&canonical)?;
        if json_contains_sensitive_value(&value, &self.redactor) {
            bail!("validated result contains a raw execution-environment value");
        }
        if canonical.len() as u64 > MAX_RESULT_ARTIFACT_BYTES {
            bail!("canonical result exceeds its artifact cap");
        }
        reserve_receipt(&self.registry, ArtifactKind::Result)?;
        let receipt = atomic_write(&self.directory, ArtifactKind::Result, &canonical, false)?;
        complete_receipt(&self.registry, receipt.clone())?;
        Ok(receipt)
    }

    pub fn finalize_manifest(&self, metadata: ManifestMetadata) -> Result<TurnManifest> {
        metadata.validate_for(&self.scope)?;
        if metadata.supervisor_epoch != self.supervisor_epoch
            || metadata.environment_hash != self.environment_hash
        {
            bail!("manifest environment lease does not match its artifact attempt");
        }
        let receipts = seal_and_collect_receipts(&self.registry)?;
        let result = receipts
            .get(&ArtifactKind::Result)
            .ok_or_else(|| anyhow::anyhow!("artifact attempt has no validated result"))?;
        if result.sha256 != metadata.result_hash {
            bail!("manifest result hash does not match result.json");
        }
        for receipt in receipts.values() {
            verify_receipt(&self.directory, receipt)?;
        }
        let manifest = TurnManifest {
            format_version: 1,
            run_id: self.scope.run_id.clone(),
            task_id: self.scope.task_id.clone(),
            role: self.scope.role,
            logical_session_id: self.scope.logical_session_id.clone(),
            native_session_id: metadata.native_session_id,
            turn_sequence: self.scope.turn_sequence,
            attempt: self.scope.attempt,
            task_version: metadata.task_version,
            review_round: metadata.review_round,
            base_revision: metadata.base_revision,
            head_revision: metadata.head_revision,
            review_workspace_digest: metadata.review_workspace_digest,
            supervisor_epoch: metadata.supervisor_epoch,
            environment_hash: metadata.environment_hash,
            adapter_contract_hash: metadata.adapter_contract_hash,
            result_hash: metadata.result_hash,
            created_at: metadata.created_at,
            completed_at: metadata.completed_at,
            activity_truncated: receipts
                .get(&ArtifactKind::Activity)
                .is_some_and(|receipt| receipt.truncated),
            artifacts: receipts.into_values().collect(),
        };
        manifest.validate()?;
        let encoded = serde_json::to_vec(&manifest)?;
        if encoded.len() as u64 > MAX_MANIFEST_ARTIFACT_BYTES {
            bail!("artifact manifest exceeds its hard cap");
        }
        atomic_write_manifest(&self.directory, &encoded)?;
        Ok(manifest)
    }
}

pub struct BoundedArtifactWriter {
    kind: ArtifactKind,
    file: File,
    hard_cap: u64,
    /// Only the tail that may still be part of a secret spanning two chunks,
    /// plus any partial UTF-8 sequence. Everything before it is already
    /// sanitized and on disk, so memory stays bounded no matter how large the
    /// artifact grows.
    carry: Vec<u8>,
    written: u64,
    hasher: Sha256,
    truncated: bool,
    redactor: Arc<SecretRedactor>,
    registry: Arc<Mutex<ReceiptRegistry>>,
}

impl BoundedArtifactWriter {
    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<ArtifactWriteStatus> {
        if self.truncated {
            return Ok(ArtifactWriteStatus::Truncated);
        }
        self.carry.extend_from_slice(bytes);
        // Hold back the guard window: it may be the head of a secret whose
        // tail arrives in the next chunk.
        let guard = self.redactor.trailing_guard_bytes();
        if self.carry.len() <= guard {
            return Ok(ArtifactWriteStatus::Accepted);
        }
        let mut split = self.carry.len() - guard;
        while split > 0 && !is_utf8_start(self.carry[split]) {
            split -= 1;
        }
        if split == 0 {
            return Ok(ArtifactWriteStatus::Accepted);
        }
        let piece: Vec<u8> = self.carry.drain(..split).collect();
        self.emit(&piece)
    }

    /// Sanitize and append one already-safe-to-cut piece, honouring the cap.
    fn emit(&mut self, piece: &[u8]) -> Result<ArtifactWriteStatus> {
        let sanitized = sanitize_untrusted(piece, &self.redactor);
        let mut bytes = sanitized.as_bytes();
        let remaining = self.hard_cap.saturating_sub(self.written);
        if (bytes.len() as u64) > remaining {
            // Reserve room for the marker, and drop a guard window so the cut
            // cannot leave the head of a secret whose tail is discarded.
            let guard = self.redactor.trailing_guard_bytes() as u64;
            let reserve = guard + TRUNCATION_MARKER.len() as u64;
            let keep = usize::try_from(remaining.saturating_sub(reserve))
                .context("artifact cap does not fit usize")?;
            bytes = utf8_prefix(bytes, keep);
            self.truncated = true;
        }
        self.file.write_all(bytes)?;
        self.hasher.update(bytes);
        self.written += bytes.len() as u64;
        if self.truncated {
            self.file.write_all(TRUNCATION_MARKER)?;
            self.hasher.update(TRUNCATION_MARKER);
            self.written += TRUNCATION_MARKER.len() as u64;
        }
        Ok(if self.truncated {
            ArtifactWriteStatus::Truncated
        } else {
            ArtifactWriteStatus::Accepted
        })
    }

    pub fn finish(mut self) -> Result<ArtifactReceipt> {
        if !self.truncated && !self.carry.is_empty() {
            let tail = std::mem::take(&mut self.carry);
            self.emit(&tail)?;
        }
        self.file.flush()?;
        self.file.sync_all()?;
        verify_private_regular_file(&self.file, self.kind.file_name())?;
        let receipt = ArtifactReceipt {
            kind: self.kind,
            file_name: self.kind.file_name().into(),
            bytes: self.written,
            sha256: hex_bytes(&self.hasher.clone().finalize()),
            truncated: self.truncated,
        };
        receipt.validate()?;
        complete_receipt(&self.registry, receipt.clone())?;
        Ok(receipt)
    }
}

/// Appended whenever a stream hits its cap, so a reader can always tell a
/// complete artifact from a prefix.
const TRUNCATION_MARKER: &[u8] = b"\n[hcom: truncated at the artifact cap]\n";

fn is_utf8_start(byte: u8) -> bool {
    (byte & 0xC0) != 0x80
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactWriteStatus {
    Accepted,
    Truncated,
}

pub struct ActivityLogWriter {
    file: File,
    hard_cap: u64,
    bytes_written: u64,
    next_sequence: u64,
    truncated: bool,
    hasher: Sha256,
    redactor: Arc<SecretRedactor>,
    registry: Arc<Mutex<ReceiptRegistry>>,
}

impl ActivityLogWriter {
    pub fn record(&mut self, kind: &str, message: &[u8]) -> Result<ArtifactWriteStatus> {
        if self.truncated {
            return Ok(ArtifactWriteStatus::Truncated);
        }
        validate_text("activity kind", kind, 128, false)?;
        if message.len() > 64 * 1024 {
            let marker = activity_truncation_line(self.next_sequence)?;
            self.write_exact(&marker)?;
            self.truncated = true;
            self.next_sequence += 1;
            return Ok(ArtifactWriteStatus::Truncated);
        }
        let record = ActivityRecord {
            sequence: self.next_sequence,
            kind: kind.to_owned(),
            message: sanitize_untrusted(message, &self.redactor),
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        let marker = activity_truncation_line(self.next_sequence)?;
        let normal_limit = self
            .hard_cap
            .checked_sub(marker.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("activity cap cannot reserve truncation marker"))?;
        if self.bytes_written + line.len() as u64 > normal_limit {
            self.write_exact(&marker)?;
            self.truncated = true;
            self.next_sequence += 1;
            return Ok(ArtifactWriteStatus::Truncated);
        }
        self.write_exact(&line)?;
        self.next_sequence += 1;
        Ok(ArtifactWriteStatus::Accepted)
    }

    pub fn finish(mut self) -> Result<ArtifactReceipt> {
        self.file.flush()?;
        self.file.sync_all()?;
        verify_private_regular_file(&self.file, ArtifactKind::Activity.file_name())?;
        let receipt = ArtifactReceipt {
            kind: ArtifactKind::Activity,
            file_name: ArtifactKind::Activity.file_name().into(),
            bytes: self.bytes_written,
            sha256: hex_bytes(&self.hasher.finalize()),
            truncated: self.truncated,
        };
        receipt.validate()?;
        complete_receipt(&self.registry, receipt.clone())?;
        Ok(receipt)
    }

    fn write_exact(&mut self, bytes: &[u8]) -> Result<()> {
        if self.bytes_written + bytes.len() as u64 > self.hard_cap {
            bail!("activity writer exceeded its hard cap");
        }
        self.file.write_all(bytes)?;
        self.hasher.update(bytes);
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ActivityRecord {
    sequence: u64,
    kind: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReceipt {
    pub kind: ArtifactKind,
    pub file_name: String,
    pub bytes: u64,
    pub sha256: String,
    pub truncated: bool,
}

impl ArtifactReceipt {
    pub fn validate(&self) -> Result<()> {
        if self.file_name != self.kind.file_name() {
            bail!("artifact receipt filename does not match its kind");
        }
        validate_relative_path("artifact receipt filename", &self.file_name)?;
        validate_sha256("artifact receipt sha256", &self.sha256)?;
        if self.bytes > self.kind.hard_cap() {
            bail!("artifact receipt exceeds its kind hard cap");
        }
        if self.kind == ArtifactKind::Result && self.truncated {
            bail!("validated result artifact cannot be truncated");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ManifestMetadata {
    pub native_session_id: String,
    pub task_version: u64,
    pub review_round: u32,
    pub base_revision: String,
    pub head_revision: Option<String>,
    pub review_workspace_digest: Option<String>,
    pub supervisor_epoch: String,
    pub environment_hash: String,
    pub adapter_contract_hash: String,
    pub result_hash: String,
    pub created_at: i64,
    pub completed_at: i64,
}

impl ManifestMetadata {
    fn validate_for(&self, scope: &ArtifactScope) -> Result<()> {
        scope.validate()?;
        validate_native_session_id(&self.native_session_id)?;
        if self.task_version == 0 {
            bail!("manifest task version must be positive");
        }
        validate_git_oid("manifest base revision", &self.base_revision)?;
        if let Some(head_revision) = &self.head_revision {
            validate_git_oid("manifest head revision", head_revision)?;
        }
        match scope.role {
            WorkerRole::Developer if self.review_workspace_digest.is_some() => {
                bail!("developer manifest cannot bind a reviewer workspace");
            }
            WorkerRole::Reviewer => {
                if self.review_round == 0 || self.head_revision.is_none() {
                    bail!("reviewer manifest must bind its exact round and head revision");
                }
                validate_sha256(
                    "manifest review workspace digest",
                    self.review_workspace_digest
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("reviewer manifest lost its workspace"))?,
                )?;
            }
            WorkerRole::Developer => {}
        }
        validate_opaque_id("manifest supervisor epoch", &self.supervisor_epoch)?;
        validate_sha256("manifest environment hash", &self.environment_hash)?;
        validate_sha256(
            "manifest adapter contract hash",
            &self.adapter_contract_hash,
        )?;
        validate_sha256("manifest result hash", &self.result_hash)?;
        if self.created_at <= 0 || self.completed_at < self.created_at {
            bail!("manifest timestamps are invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TurnManifest {
    pub format_version: u32,
    pub run_id: String,
    pub task_id: String,
    pub role: WorkerRole,
    pub logical_session_id: String,
    pub native_session_id: String,
    pub turn_sequence: u32,
    pub attempt: u32,
    pub task_version: u64,
    pub review_round: u32,
    pub base_revision: String,
    pub head_revision: Option<String>,
    pub review_workspace_digest: Option<String>,
    pub supervisor_epoch: String,
    pub environment_hash: String,
    pub adapter_contract_hash: String,
    pub result_hash: String,
    pub created_at: i64,
    pub completed_at: i64,
    pub activity_truncated: bool,
    pub artifacts: Vec<ArtifactReceipt>,
}

impl TurnManifest {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != 1 {
            bail!("unsupported artifact manifest format");
        }
        let scope = ArtifactScope {
            run_id: self.run_id.clone(),
            task_id: self.task_id.clone(),
            role: self.role,
            logical_session_id: self.logical_session_id.clone(),
            turn_sequence: self.turn_sequence,
            attempt: self.attempt,
        };
        ManifestMetadata {
            native_session_id: self.native_session_id.clone(),
            task_version: self.task_version,
            review_round: self.review_round,
            base_revision: self.base_revision.clone(),
            head_revision: self.head_revision.clone(),
            review_workspace_digest: self.review_workspace_digest.clone(),
            supervisor_epoch: self.supervisor_epoch.clone(),
            environment_hash: self.environment_hash.clone(),
            adapter_contract_hash: self.adapter_contract_hash.clone(),
            result_hash: self.result_hash.clone(),
            created_at: self.created_at,
            completed_at: self.completed_at,
        }
        .validate_for(&scope)?;
        let mut kinds = BTreeMap::new();
        for receipt in &self.artifacts {
            receipt.validate()?;
            if kinds.insert(receipt.kind, receipt).is_some() {
                bail!("manifest artifact kinds must be unique");
            }
        }
        let result = kinds
            .get(&ArtifactKind::Result)
            .ok_or_else(|| anyhow::anyhow!("manifest is missing result.json"))?;
        if result.sha256 != self.result_hash {
            bail!("manifest result hash mismatch");
        }
        if self.activity_truncated
            != kinds
                .get(&ArtifactKind::Activity)
                .is_some_and(|receipt| receipt.truncated)
        {
            bail!("manifest activity truncation flag is inconsistent");
        }
        Ok(())
    }
}

#[derive(Default)]
struct ReceiptRegistry {
    entries: BTreeMap<ArtifactKind, Option<ArtifactReceipt>>,
    finalized: bool,
}

fn reserve_receipt(registry: &Arc<Mutex<ReceiptRegistry>>, kind: ArtifactKind) -> Result<()> {
    let mut registry = lock_registry(registry)?;
    if registry.finalized || registry.entries.contains_key(&kind) {
        bail!("artifact kind is already reserved or the attempt is finalized");
    }
    registry.entries.insert(kind, None);
    Ok(())
}

fn complete_receipt(
    registry: &Arc<Mutex<ReceiptRegistry>>,
    receipt: ArtifactReceipt,
) -> Result<()> {
    let mut registry = lock_registry(registry)?;
    let Some(slot) = registry.entries.get_mut(&receipt.kind) else {
        bail!("artifact receipt was not reserved");
    };
    if slot.is_some() {
        bail!("artifact receipt is already complete");
    }
    *slot = Some(receipt);
    Ok(())
}

fn seal_and_collect_receipts(
    registry: &Arc<Mutex<ReceiptRegistry>>,
) -> Result<BTreeMap<ArtifactKind, ArtifactReceipt>> {
    let mut registry = lock_registry(registry)?;
    if registry.finalized {
        bail!("artifact attempt is already finalized");
    }
    let mut completed = BTreeMap::new();
    for (kind, receipt) in &registry.entries {
        let receipt = receipt
            .clone()
            .ok_or_else(|| anyhow::anyhow!("artifact writer is not finished"))?;
        completed.insert(*kind, receipt);
    }
    registry.finalized = true;
    Ok(completed)
}

fn lock_registry(
    registry: &Arc<Mutex<ReceiptRegistry>>,
) -> Result<std::sync::MutexGuard<'_, ReceiptRegistry>> {
    registry
        .lock()
        .map_err(|_| anyhow::anyhow!("artifact receipt registry is poisoned"))
}

fn create_or_open_private_directory(
    parent: &File,
    component: &str,
    exclusive: bool,
) -> Result<File> {
    let component = CString::new(component)?;
    // SAFETY: parent is a live directory descriptor and component is a bounded C string.
    let mkdir_result = unsafe { libc::mkdirat(parent.as_raw_fd(), component.as_ptr(), 0o700) };
    let created = if mkdir_result == 0 {
        true
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EEXIST) || exclusive {
            return Err(error).context("failed to create private artifact directory");
        }
        false
    };
    // SAFETY: openat receives a live directory descriptor, a valid C string, and no mode argument.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            component.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to open private artifact directory");
    }
    // SAFETY: openat returned a newly owned file descriptor.
    let directory = unsafe { File::from_raw_fd(fd) };
    if created {
        // SAFETY: directory is a live descriptor owned by this process.
        if unsafe { libc::fchmod(directory.as_raw_fd(), 0o700) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to set private artifact directory mode");
        }
    }
    verify_private_directory(&directory, "artifact path component")?;
    parent.sync_all()?;
    Ok(directory)
}

fn create_private_file(directory_fd: RawFd, file_name: &str) -> Result<File> {
    let file_name = CString::new(file_name)?;
    // SAFETY: directory_fd is live and file_name is a valid fixed C string.
    let fd = unsafe {
        libc::openat(
            directory_fd,
            file_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to create private artifact file");
    }
    // SAFETY: openat returned a newly owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    // SAFETY: file owns a live regular-file descriptor.
    if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to set private artifact file mode");
    }
    verify_private_regular_file(&file, "new artifact file")?;
    Ok(file)
}

fn open_private_file(directory_fd: RawFd, file_name: &str, hard_cap: u64) -> Result<File> {
    let file_name = CString::new(file_name)?;
    // SAFETY: directory_fd is live and file_name is a validated fixed C string.
    let fd = unsafe {
        libc::openat(
            directory_fd,
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to open private artifact file");
    }
    // SAFETY: openat returned a newly owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    verify_private_regular_file(&file, "persisted artifact file")?;
    if file.metadata()?.len() > hard_cap {
        bail!("persisted artifact file exceeds its hard cap");
    }
    Ok(file)
}

fn is_reserved_artifact_name(name: &str) -> bool {
    [
        ArtifactKind::NativeStdout.file_name(),
        ArtifactKind::NativeStderr.file_name(),
        ArtifactKind::NativeFinal.file_name(),
        ArtifactKind::Activity.file_name(),
        ArtifactKind::Result.file_name(),
    ]
    .contains(&name)
}

fn atomic_write(
    directory: &File,
    kind: ArtifactKind,
    bytes: &[u8],
    truncated: bool,
) -> Result<ArtifactReceipt> {
    let temp_name = format!(".{}.{}.tmp", kind.file_name(), Uuid::new_v4());
    let mut file = create_private_file(directory.as_raw_fd(), &temp_name)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        verify_private_regular_file(&file, &temp_name)?;
        rename_noreplace(directory.as_raw_fd(), &temp_name, kind.file_name())?;
        directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        unlink_file(directory.as_raw_fd(), &temp_name);
    }
    result?;
    let receipt = ArtifactReceipt {
        kind,
        file_name: kind.file_name().into(),
        bytes: bytes.len() as u64,
        sha256: hex_bytes(&Sha256::digest(bytes)),
        truncated,
    };
    receipt.validate()?;
    verify_receipt(directory, &receipt)?;
    Ok(receipt)
}

fn atomic_write_manifest(directory: &File, bytes: &[u8]) -> Result<()> {
    let temp_name = format!(".manifest.json.{}.tmp", Uuid::new_v4());
    let mut file = create_private_file(directory.as_raw_fd(), &temp_name)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        verify_private_regular_file(&file, &temp_name)?;
        rename_noreplace(directory.as_raw_fd(), &temp_name, "manifest.json")?;
        directory.sync_all()?;
        verify_named_blob(
            directory,
            "manifest.json",
            bytes,
            MAX_MANIFEST_ARTIFACT_BYTES,
        )?;
        Ok(())
    })();
    if result.is_err() {
        unlink_file(directory.as_raw_fd(), &temp_name);
    }
    result
}

fn rename_noreplace(directory_fd: RawFd, source: &str, destination: &str) -> Result<()> {
    let source = CString::new(source)?;
    let destination = CString::new(destination)?;
    // SAFETY: both names are valid C strings scoped to the same live directory descriptor.
    let result = unsafe {
        libc::renameat2(
            directory_fd,
            source.as_ptr(),
            directory_fd,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to atomically publish artifact");
    }
    Ok(())
}

fn unlink_file(directory_fd: RawFd, file_name: &str) {
    if let Ok(file_name) = CString::new(file_name) {
        // SAFETY: this best-effort cleanup names only the attempt-local file just created above.
        unsafe {
            libc::unlinkat(directory_fd, file_name.as_ptr(), 0);
        }
    }
}

fn unlink_required(directory_fd: RawFd, file_name: &str) -> Result<()> {
    let file_name = CString::new(file_name)?;
    // SAFETY: directory_fd is live and file_name is a validated attempt-local component.
    if unsafe { libc::unlinkat(directory_fd, file_name.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to consume native final output");
    }
    Ok(())
}

fn verify_private_directory(directory: &File, label: &str) -> Result<()> {
    let metadata = directory.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("{label} must be a current-user-owned mode 0700 directory");
    }
    Ok(())
}

fn verify_private_regular_file(file: &File, label: &str) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        bail!("{label} must be a current-user-owned single-link mode 0600 regular file");
    }
    Ok(())
}

fn verify_receipt(directory: &File, receipt: &ArtifactReceipt) -> Result<()> {
    receipt.validate()?;
    let file_name = CString::new(receipt.file_name.as_str())?;
    // SAFETY: directory is live and file_name was validated as a fixed normal component.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to reopen artifact receipt");
    }
    // SAFETY: openat returned a newly owned descriptor.
    let mut file = unsafe { File::from_raw_fd(fd) };
    verify_private_regular_file(&file, &receipt.file_name)?;
    let metadata = file.metadata()?;
    if metadata.len() != receipt.bytes {
        bail!("artifact receipt byte count mismatch");
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
            .ok_or_else(|| anyhow::anyhow!("artifact receipt size overflow"))?;
        if copied > receipt.kind.hard_cap() {
            bail!("artifact receipt content exceeds its bound");
        }
        hasher.update(&buffer[..count]);
    }
    if copied != receipt.bytes || copied > receipt.kind.hard_cap() {
        bail!("artifact receipt content exceeds its bound or changed");
    }
    if hex_bytes(&hasher.finalize()) != receipt.sha256 {
        bail!("artifact receipt hash mismatch");
    }
    Ok(())
}

fn verify_named_blob(directory: &File, file_name: &str, expected: &[u8], cap: u64) -> Result<()> {
    let file_name_c = CString::new(file_name)?;
    // SAFETY: directory is live and file_name is a fixed, validated C string.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name_c.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to reopen atomic artifact");
    }
    // SAFETY: openat returned a newly owned descriptor.
    let mut file = unsafe { File::from_raw_fd(fd) };
    verify_private_regular_file(&file, file_name)?;
    if file.metadata()?.len() != expected.len() as u64 || expected.len() as u64 > cap {
        bail!("atomic artifact size mismatch");
    }
    let mut actual = Vec::with_capacity(expected.len().min(cap as usize));
    (&mut file).take(cap + 1).read_to_end(&mut actual)?;
    if actual != expected {
        bail!("atomic artifact content mismatch");
    }
    Ok(())
}

fn sanitize_untrusted(bytes: &[u8], redactor: &SecretRedactor) -> String {
    let decoded = String::from_utf8_lossy(bytes);
    let mut sanitized = String::with_capacity(decoded.len());
    for character in decoded.chars() {
        if character == '\n' || character == '\t' {
            sanitized.push(character);
        } else if character == '\r'
            || character == '\u{1b}'
            || character.is_control()
            || ('\u{80}'..='\u{9f}').contains(&character)
        {
            sanitized.push('\u{fffd}');
        } else {
            sanitized.push(character);
        }
    }
    redactor.redact(&sanitized)
}

fn json_contains_sensitive_value(value: &serde_json::Value, redactor: &SecretRedactor) -> bool {
    match value {
        serde_json::Value::String(value) => redactor.would_redact(value),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_sensitive_value(value, redactor)),
        serde_json::Value::Object(values) => values
            .values()
            .any(|value| json_contains_sensitive_value(value, redactor)),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            false
        }
    }
}

fn utf8_prefix(bytes: &[u8], maximum: usize) -> &[u8] {
    if bytes.len() <= maximum {
        return bytes;
    }
    let mut end = maximum;
    while end > 0 && std::str::from_utf8(&bytes[..end]).is_err() {
        end -= 1;
    }
    &bytes[..end]
}

fn activity_truncation_line(sequence: u64) -> Result<Vec<u8>> {
    let marker = ActivityRecord {
        sequence,
        kind: "truncated".into(),
        message: "activity output truncated at hard cap".into(),
    };
    let mut encoded = serde_json::to_vec(&marker)?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

fn role_name(role: WorkerRole) -> &'static str {
    match role {
        WorkerRole::Developer => "developer",
        WorkerRole::Reviewer => "reviewer",
    }
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
    use crate::worker::environment::{EnvironmentPolicy, ParentEnvironment};
    use crate::worker::result::{
        CheckResult, CheckStatus, CommitSummary, DeveloperDecision, FindingSeverity,
        ReviewDecision, ReviewFinding,
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    const TEST_PROMPT: &[u8] = b"private turn prompt sentinel";

    fn fixture_root() -> (tempfile::TempDir, ArtifactRoot) {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root = ArtifactRoot::open(temp.path()).unwrap();
        (temp, root)
    }

    fn scope() -> ArtifactScope {
        ArtifactScope {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            role: WorkerRole::Developer,
            logical_session_id: "session-1".into(),
            turn_sequence: 1,
            attempt: 1,
        }
    }

    fn fixture_environment(proxy: Option<&str>) -> ExecutionEnvironmentLease {
        let mut values = vec![("PATH".into(), "/usr/bin:/bin".into())];
        if let Some(proxy) = proxy {
            values.push(("HTTPS_PROXY".into(), proxy.into()));
        }
        ExecutionEnvironmentLease::capture(
            "lease-1",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            values,
        )
        .unwrap()
    }

    fn create_attempt(root: &ArtifactRoot, scope: ArtifactScope) -> ArtifactAttempt {
        ArtifactAttempt::create(root, scope, &fixture_environment(None), TEST_PROMPT).unwrap()
    }

    fn result_json() -> Vec<u8> {
        let head: String = std::iter::repeat_n('a', 40).collect();
        DeveloperResult {
            decision: DeveloperDecision::Completed,
            summary: "completed bounded fake task".into(),
            head_revision: Some(head.clone()),
            commits: vec![CommitSummary {
                sha: head,
                subject: "Implement bounded fake task".into(),
            }],
            checks: vec![CheckResult {
                command: "fake check".into(),
                status: CheckStatus::Passed,
                summary: "passed".into(),
            }],
            questions: vec![],
            risks: vec![],
            changed_paths: vec!["src/worker/result.rs".into()],
        }
        .canonical_json()
        .unwrap()
    }

    #[test]
    fn paths_are_private_relative_and_attempts_are_exclusive() {
        let (_temp, root) = fixture_root();
        let attempt = create_attempt(&root, scope());
        assert_eq!(
            attempt.relative_path(),
            "run-1/task-1/developer/session-1/turn-1/attempt-1"
        );
        assert!(
            ArtifactAttempt::create(&root, scope(), &fixture_environment(None), TEST_PROMPT)
                .is_err(),
            "attempt directory must be create-once"
        );
        let metadata = fs::metadata(attempt.directory_path()).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(metadata.uid(), effective_uid());

        let mut traversal = scope();
        traversal.run_id = "..".into();
        assert!(
            ArtifactAttempt::create(&root, traversal, &fixture_environment(None), TEST_PROMPT)
                .is_err()
        );
    }

    #[test]
    fn symlink_and_hardlink_escape_fail_closed() {
        let (temp, root) = fixture_root();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), temp.path().join("run-1")).unwrap();
        assert!(
            ArtifactAttempt::create(&root, scope(), &fixture_environment(None), TEST_PROMPT)
                .is_err()
        );

        let (temp, root) = fixture_root();
        let attempt = create_attempt(&root, scope());
        let mut writer = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 1024)
            .unwrap();
        writer.write_chunk(b"bounded").unwrap();
        fs::hard_link(
            attempt.artifact_path(ArtifactKind::NativeStdout),
            temp.path().join("outside-hardlink"),
        )
        .unwrap();
        assert!(writer.finish().is_err());
    }

    #[test]
    fn streams_are_bounded_sanitized_and_redacted() {
        let (_temp, root) = fixture_root();
        let environment =
            fixture_environment(Some("http://worker-user:top-secret@proxy.invalid:8080"));
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let mut writer = attempt
            .start_native_stream(ArtifactKind::NativeStderr, 64)
            .unwrap();
        let mut prompt_writer = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 128)
            .unwrap();
        prompt_writer.write_chunk(&TEST_PROMPT[..9]).unwrap();
        prompt_writer.write_chunk(&TEST_PROMPT[9..]).unwrap();
        assert_eq!(
            writer
                .write_chunk(b"prefix \x1b]0;title top-secret\r\n")
                .unwrap(),
            ArtifactWriteStatus::Accepted
        );
        assert_eq!(
            writer.write_chunk(&[b'x'; 128]).unwrap(),
            ArtifactWriteStatus::Truncated
        );
        let receipt = writer.finish().unwrap();
        assert!(receipt.truncated);
        assert!(receipt.bytes <= 64);
        let stored = fs::read_to_string(attempt.artifact_path(ArtifactKind::NativeStderr)).unwrap();
        assert!(!stored.contains('\u{1b}'));
        assert!(!stored.contains('\r'));
        assert!(!stored.contains("top-secret"));
        assert!(
            stored.len() < 64,
            "truncated streams must drop a longest-secret tail guard before sealing"
        );

        prompt_writer.finish().unwrap();
        let prompt_output =
            fs::read_to_string(attempt.artifact_path(ArtifactKind::NativeStdout)).unwrap();
        assert!(!prompt_output.contains("private turn prompt sentinel"));
        assert!(prompt_output.contains("[REDACTED]"));
    }

    #[test]
    fn truncated_stream_drops_partial_secret_at_the_cap_boundary() {
        let (_temp, root) = fixture_root();
        let secret = "boundary-secret-value-12345";
        let environment = ExecutionEnvironmentLease::capture(
            "lease-boundary",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            vec![
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("SERVICE_TOKEN".into(), secret.into()),
            ],
        )
        .unwrap();
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let hard_cap = (b"prefix ".len() + secret.len() - 3) as u64;
        let mut writer = attempt
            .start_native_stream(ArtifactKind::NativeStderr, hard_cap)
            .unwrap();
        writer
            .write_chunk(format!("prefix {secret} trailing").as_bytes())
            .unwrap();
        let receipt = writer.finish().unwrap();
        assert!(receipt.truncated);
        let stored = fs::read_to_string(attempt.artifact_path(ArtifactKind::NativeStderr)).unwrap();
        assert!(!stored.contains("boundary-secret"));
        assert!(!stored.contains("boundary-secret-value-12"));
    }

    #[test]
    fn native_final_ingest_keeps_raw_only_in_memory_and_seals_redacted_bytes() {
        let (_temp, root) = fixture_root();
        let environment =
            fixture_environment(Some("http://worker-user:top-secret@proxy.invalid:8080"));
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let raw = br#"{"summary":"top-secret","safe":"kept"}"#;
        let mut worker_file = create_private_file(
            attempt.directory.as_raw_fd(),
            ArtifactKind::NativeFinal.file_name(),
        )
        .unwrap();
        worker_file.write_all(raw).unwrap();
        worker_file.sync_all().unwrap();
        drop(worker_file);

        let in_memory = attempt
            .ingest_native_final(ArtifactKind::NativeFinal.file_name(), 1024)
            .unwrap();
        assert_eq!(in_memory, raw);
        let stored = fs::read_to_string(attempt.artifact_path(ArtifactKind::NativeFinal)).unwrap();
        assert!(!stored.contains("top-secret"));
        assert!(stored.contains("[REDACTED]"));
        assert!(stored.contains("kept"));
    }

    #[test]
    fn stdout_and_stderr_writers_finalize_concurrently() {
        let (_temp, root) = fixture_root();
        let attempt = create_attempt(&root, scope());
        let stdout = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 1024)
            .unwrap();
        let stderr = attempt
            .start_native_stream(ArtifactKind::NativeStderr, 1024)
            .unwrap();
        let (stdout_receipt, stderr_receipt) = std::thread::scope(|scope| {
            let stdout = scope.spawn(move || {
                let mut stdout = stdout;
                stdout.write_chunk(b"stdout event")?;
                stdout.finish()
            });
            let stderr = scope.spawn(move || {
                let mut stderr = stderr;
                stderr.write_chunk(b"stderr event")?;
                stderr.finish()
            });
            (
                stdout
                    .join()
                    .expect("stdout writer thread panicked")
                    .unwrap(),
                stderr
                    .join()
                    .expect("stderr writer thread panicked")
                    .unwrap(),
            )
        });
        assert_eq!(stdout_receipt.kind, ArtifactKind::NativeStdout);
        assert_eq!(stderr_receipt.kind, ArtifactKind::NativeStderr);
    }

    #[test]
    fn activity_writes_one_truncation_marker_without_state_authority() {
        let (_temp, root) = fixture_root();
        let attempt = create_attempt(&root, scope());
        let marker_len = activity_truncation_line(1).unwrap().len() as u64;
        let mut activity = attempt.start_activity_log(marker_len + 80).unwrap();
        activity.record("progress", TEST_PROMPT).unwrap();
        assert_eq!(
            activity.record("progress", &[b'x'; 256]).unwrap(),
            ArtifactWriteStatus::Truncated
        );
        assert_eq!(
            activity
                .record("completed", b"must not be authority")
                .unwrap(),
            ArtifactWriteStatus::Truncated
        );
        let receipt = activity.finish().unwrap();
        assert!(receipt.truncated);
        let stored = fs::read_to_string(attempt.artifact_path(ArtifactKind::Activity)).unwrap();
        assert_eq!(stored.matches("\"kind\":\"truncated\"").count(), 1);
        assert!(!stored.contains("private turn prompt sentinel"));
        assert!(!stored.contains("must not be authority"));
    }

    #[test]
    fn native_streams_are_incremental_and_sealed_only_on_finish() {
        let (_temp, root) = fixture_root();
        let attempt = create_attempt(&root, scope());
        let mut native = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 1024)
            .unwrap();
        let mut activity = attempt.start_activity_log(1024).unwrap();
        native
            .write_chunk(b"native partial must not be followed")
            .unwrap();
        activity
            .record("progress", b"safe incremental event")
            .unwrap();
        // Native output streams to disk as it arrives — memory must not grow
        // with the artifact — while the receipt is produced only by finish().
        let activity_on_disk =
            fs::read_to_string(attempt.artifact_path(ArtifactKind::Activity)).unwrap();
        assert!(activity_on_disk.contains("safe incremental event"));
        let receipt = native.finish().unwrap();
        activity.finish().unwrap();
        let native_on_disk = fs::read(attempt.artifact_path(ArtifactKind::NativeStdout)).unwrap();
        assert_eq!(receipt.bytes, native_on_disk.len() as u64);
        assert!(
            String::from_utf8_lossy(&native_on_disk).contains("native partial must not be followed")
        );
    }

    #[test]
    fn result_and_manifest_are_atomic_hashed_and_secret_free() {
        let (_temp, root) = fixture_root();
        let environment = fixture_environment(None);
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let result = attempt.write_result_json(&result_json()).unwrap();
        let metadata = ManifestMetadata {
            native_session_id: "native-1".into(),
            task_version: 1,
            review_round: 0,
            base_revision: "a".repeat(40),
            head_revision: None,
            review_workspace_digest: None,
            supervisor_epoch: "epoch-1".into(),
            environment_hash: environment.descriptor().environment_hash.clone(),
            adapter_contract_hash: hex_bytes(&Sha256::digest(b"adapter contract")),
            result_hash: result.sha256.clone(),
            created_at: 1,
            completed_at: 2,
        };
        let mut wrong_environment = metadata.clone();
        wrong_environment.environment_hash = hex_bytes(&Sha256::digest(b"wrong environment"));
        assert!(attempt.finalize_manifest(wrong_environment).is_err());
        let mut wrong_epoch = metadata.clone();
        wrong_epoch.supervisor_epoch = "epoch-other".into();
        assert!(attempt.finalize_manifest(wrong_epoch).is_err());
        let manifest = attempt.finalize_manifest(metadata).unwrap();
        manifest.validate().unwrap();
        let encoded = fs::read(attempt.directory_path().join("manifest.json")).unwrap();
        let parsed: TurnManifest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(parsed.result_hash, result.sha256);
        assert!(
            !String::from_utf8(encoded)
                .unwrap()
                .contains("/usr/bin:/bin")
        );

        for name in ["result.json", "manifest.json"] {
            let metadata = fs::metadata(attempt.directory_path().join(name)).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            assert_eq!(metadata.nlink(), 1);
        }
        assert!(
            attempt
                .finalize_manifest(ManifestMetadata {
                    native_session_id: "native-1".into(),
                    task_version: 1,
                    review_round: 0,
                    base_revision: "a".repeat(40),
                    head_revision: None,
                    review_workspace_digest: None,
                    supervisor_epoch: "epoch-1".into(),
                    environment_hash: environment.descriptor().environment_hash.clone(),
                    adapter_contract_hash: hex_bytes(&Sha256::digest(b"adapter contract")),
                    result_hash: result.sha256,
                    created_at: 1,
                    completed_at: 2,
                })
                .is_err()
        );
    }

    #[test]
    fn validated_result_cannot_persist_a_raw_environment_value() {
        let (_temp, root) = fixture_root();
        let environment =
            fixture_environment(Some("http://worker-user:worker-pass@proxy.invalid:8080"));
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let mut result: DeveloperResult = serde_json::from_slice(&result_json()).unwrap();
        result.summary = String::from_utf8(TEST_PROMPT.to_vec()).unwrap();
        assert!(
            attempt
                .write_result_json(&result.canonical_json().unwrap())
                .is_err()
        );
        result.summary = "failure included worker-pass".into();
        assert!(
            attempt
                .write_result_json(&result.canonical_json().unwrap())
                .is_err()
        );
        assert!(!attempt.artifact_path(ArtifactKind::Result).exists());
    }

    #[test]
    fn short_cli_environment_atoms_do_not_corrupt_structured_artifacts() {
        let (_temp, root) = fixture_root();
        let names = vec![
            "CLAUDE_CODE_DISABLE_FAST_MODE".into(),
            "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION".into(),
            "PATH".into(),
        ];
        let environment = ExecutionEnvironmentLease::capture(
            "lease-claude-fixed-values",
            "epoch-1",
            &EnvironmentPolicy::new(names.clone(), names).unwrap(),
            vec![
                ("CLAUDE_CODE_DISABLE_FAST_MODE".into(), "1".into()),
                (
                    "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION".into(),
                    "false".into(),
                ),
                ("PATH".into(), "/usr/bin:/bin".into()),
            ],
        )
        .unwrap();
        let mut reviewer_scope = scope();
        reviewer_scope.role = WorkerRole::Reviewer;
        let attempt =
            ArtifactAttempt::create(&root, reviewer_scope, &environment, TEST_PROMPT).unwrap();

        let native =
            br#"{"type":"result","is_error":false,"summary":"Round 1 reviewed task1.txt"}"#;
        let mut stdout = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 1024)
            .unwrap();
        stdout.write_chunk(native).unwrap();
        stdout.finish().unwrap();
        let stored_native = fs::read(attempt.artifact_path(ArtifactKind::NativeStdout)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stored_native).unwrap(),
            serde_json::from_slice::<serde_json::Value>(native).unwrap()
        );

        let result = ReviewerResult {
            decision: ReviewDecision::RequestChanges,
            summary: "Round 1 reviewed task1.txt without false-positive redaction".into(),
            findings: vec![ReviewFinding {
                severity: FindingSeverity::Major,
                title: "Task 1 remains pending".into(),
                body: "Change task1.txt to review-stage: complete in the second commit".into(),
                file: Some("task1.txt".into()),
                line: Some(2),
            }],
            checks: vec![CheckResult {
                command: "/usr/bin/test -f task1.txt".into(),
                status: CheckStatus::Passed,
                summary: "Task 1 file exists".into(),
            }],
        };
        let canonical = result.canonical_json().unwrap();
        attempt.write_result_json(&canonical).unwrap();
        assert_eq!(
            fs::read(attempt.artifact_path(ArtifactKind::Result)).unwrap(),
            canonical
        );
    }

    #[test]
    fn operational_parent_environment_does_not_redact_worker_evidence_or_result() {
        let (_temp, root) = fixture_root();
        let repository = "/home/example/work/project";
        let environment = ExecutionEnvironmentLease::capture_complete(
            "lease-real-shell-shape",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            &ParentEnvironment::from_unicode(BTreeMap::from([
                ("LANG".into(), "en_US.UTF-8".into()),
                ("OLDPWD".into(), "/home/example/work".into()),
                ("PATH".into(), "/usr/local/bin:/usr/bin:/bin".into()),
                ("PWD".into(), repository.into()),
                (
                    "SERVICE_ACCESS_TOKEN".into(),
                    "service-access-token-value".into(),
                ),
                ("SHELL".into(), "/bin/bash".into()),
            ])),
            Vec::new(),
        )
        .unwrap();
        let attempt = ArtifactAttempt::create(&root, scope(), &environment, TEST_PROMPT).unwrap();
        let banner = format!(
            "workdir={repository} shell=/bin/bash locale=en_US.UTF-8 parent=/home/example/work"
        );
        let mut stdout = attempt
            .start_native_stream(ArtifactKind::NativeStdout, 1024)
            .unwrap();
        stdout.write_chunk(banner.as_bytes()).unwrap();
        stdout.finish().unwrap();
        assert_eq!(
            fs::read_to_string(attempt.artifact_path(ArtifactKind::NativeStdout)).unwrap(),
            banner
        );

        let mut result: DeveloperResult = serde_json::from_slice(&result_json()).unwrap();
        result.summary = format!("updated {repository}/src/lib.rs while preserving #!/bin/bash");
        result.risks = vec!["verified with en_US.UTF-8 from /home/example/work".into()];
        let canonical = result.canonical_json().unwrap();
        attempt.write_result_json(&canonical).unwrap();
        assert_eq!(
            fs::read(attempt.artifact_path(ArtifactKind::Result)).unwrap(),
            canonical
        );

        let mut secret_scope = scope();
        secret_scope.attempt = 2;
        let secret_attempt =
            ArtifactAttempt::create(&root, secret_scope, &environment, TEST_PROMPT).unwrap();
        result.summary = "leaked service-access-token-value".into();
        assert!(
            secret_attempt
                .write_result_json(&result.canonical_json().unwrap())
                .is_err()
        );
    }
}
