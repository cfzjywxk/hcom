//! Bundle helpers for creating and validating bundle events.
//!
//! packages with event references, file lists, and transcript ranges.
//! Used by `hcom bundle` and `hcom send --title`.

use rand::RngExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use super::detail_levels::validate_detail_level;
use crate::shared::errors::HcomError;
use crate::shared::{SenderIdentity, SenderKind};

pub const MAX_BUNDLE_REPOSITORIES: usize = 16;
const MAX_REPOSITORY_PATH_BYTES: usize = 4096;
const MAX_REPOSITORY_FIELD_BYTES: usize = 1024;
const MAX_GIT_SNAPSHOT_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_UNTRACKED_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositorySnapshot {
    pub root: String,
    pub revision: String,
    pub branch: String,
    pub dirty_summary: String,
    pub state_digest: String,
}

fn framed_hash_update(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn finalize_hex(hasher: Sha256) -> String {
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn bounded_git_output(root: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("git metadata is unavailable: {error}"))?;
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err("git snapshot output pipe is unavailable".to_string());
    };
    let mut output = Vec::new();
    let read_result = stdout
        .take((MAX_GIT_SNAPSHOT_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut output);
    if let Err(error) = read_result {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("git snapshot output cannot be read: {error}"));
    }
    if output.len() > MAX_GIT_SNAPSHOT_OUTPUT_BYTES {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!(
            "git snapshot output exceeds the {} byte bound for '{}'",
            MAX_GIT_SNAPSHOT_OUTPUT_BYTES,
            root.display()
        ));
    }
    let status = child
        .wait()
        .map_err(|error| format!("git metadata process cannot be reaped: {error}"))?;
    if !status.success() {
        return Err(format!(
            "git metadata cannot be resolved for '{}'",
            root.display(),
        ));
    }
    Ok(output)
}

fn clean_git_text(bytes: Vec<u8>, field: &str, max_bytes: usize) -> Result<String, String> {
    let value = String::from_utf8(bytes).map_err(|_| format!("{field} must be valid UTF-8"))?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(format!(
            "{field} is empty, unbounded, or contains control bytes"
        ));
    }
    Ok(value.to_string())
}

fn canonical_repository_root(path: &Path) -> Result<PathBuf, String> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        format!(
            "repository path '{}' is unavailable: {error}",
            path.display()
        )
    })?;
    let top_level = clean_git_text(
        bounded_git_output(&canonical, &["rev-parse", "--show-toplevel"])?,
        "repository root",
        MAX_REPOSITORY_PATH_BYTES,
    )?;
    let root = std::fs::canonicalize(&top_level).map_err(|error| {
        format!(
            "repository root '{}' cannot be canonicalized: {error}",
            top_level
        )
    })?;
    let root_text = root
        .to_str()
        .ok_or_else(|| "repository root must be valid UTF-8".to_string())?;
    if root_text.is_empty() || root_text.len() > MAX_REPOSITORY_PATH_BYTES {
        return Err("repository root exceeds the path bound".to_string());
    }
    Ok(root)
}

fn repository_branch(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .output()
        .map_err(|error| format!("git branch metadata is unavailable: {error}"))?;
    if output.status.success() {
        clean_git_text(
            output.stdout,
            "repository branch",
            MAX_REPOSITORY_FIELD_BYTES,
        )
    } else if output.status.code() == Some(1) {
        Ok("(detached)".to_string())
    } else {
        Err(format!(
            "git branch metadata cannot be resolved for '{}'",
            root.display()
        ))
    }
}

fn dirty_summary(status: &[u8]) -> Result<String, String> {
    let mut staged = 0usize;
    let mut unstaged = 0usize;
    let mut untracked = 0usize;
    let mut conflicted = 0usize;
    let mut records = status
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        if record.len() < 3 || record[2] != b' ' {
            return Err("repository git status output is malformed".to_string());
        }
        let x = record[0];
        let y = record[1];
        if x == b'?' && y == b'?' {
            untracked += 1;
            continue;
        }
        if matches!(
            (x, y),
            (b'D', b'D')
                | (b'A', b'U')
                | (b'U', b'D')
                | (b'U', b'A')
                | (b'D', b'U')
                | (b'A', b'A')
                | (b'U', b'U')
        ) {
            conflicted += 1;
            continue;
        }
        if x != b' ' {
            staged += 1;
        }
        if y != b' ' {
            unstaged += 1;
        }
        if matches!(x, b'R' | b'C') || matches!(y, b'R' | b'C') {
            let Some(path_record) = records.next() else {
                return Err("repository git rename status is incomplete".to_string());
            };
            if path_record.is_empty() {
                return Err("repository git rename status is incomplete".to_string());
            }
        }
    }
    Ok(format!(
        "staged={staged},unstaged={unstaged},untracked={untracked},conflicted={conflicted}"
    ))
}

#[cfg(unix)]
fn path_from_git_bytes(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()))
}

#[cfg(not(unix))]
fn path_from_git_bytes(bytes: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
}

#[cfg(unix)]
fn os_path_bytes(path: &Path) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

#[cfg(not(unix))]
fn os_path_bytes(path: &Path) -> &[u8] {
    path.to_str().unwrap_or("").as_bytes()
}

fn validate_relative_git_path(path: &Path) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err("repository snapshot contains an empty path".to_string());
    }
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("repository snapshot path escapes its root".to_string());
            }
        }
    }
    Ok(())
}

fn hash_untracked_files(root: &Path, paths: &[u8], hasher: &mut Sha256) -> Result<(), String> {
    let mut total_bytes = 0u64;
    for raw in paths
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let relative = path_from_git_bytes(raw);
        validate_relative_git_path(&relative)?;
        framed_hash_update(hasher, raw);
        let path = root.join(&relative);
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            format!(
                "untracked repository path '{}' changed during snapshot: {error}",
                path.display()
            )
        })?;
        if metadata.file_type().is_symlink() {
            framed_hash_update(hasher, b"symlink");
            let target = std::fs::read_link(&path).map_err(|error| {
                format!(
                    "untracked symlink '{}' changed during snapshot: {error}",
                    path.display()
                )
            })?;
            framed_hash_update(hasher, os_path_bytes(&target));
        } else if metadata.is_file() {
            framed_hash_update(hasher, b"file");
            let mut file = File::open(&path).map_err(|error| {
                format!(
                    "untracked file '{}' changed during snapshot: {error}",
                    path.display()
                )
            })?;
            let mut buffer = [0u8; 64 * 1024];
            let mut file_hasher = Sha256::new();
            loop {
                let read = file.read(&mut buffer).map_err(|error| {
                    format!(
                        "untracked file '{}' changed during snapshot: {error}",
                        path.display()
                    )
                })?;
                if read == 0 {
                    break;
                }
                total_bytes = total_bytes.checked_add(read as u64).ok_or_else(|| {
                    "untracked repository content exceeds the snapshot bound".to_string()
                })?;
                if total_bytes > MAX_UNTRACKED_SNAPSHOT_BYTES {
                    return Err(format!(
                        "untracked repository content exceeds the {} byte snapshot bound",
                        MAX_UNTRACKED_SNAPSHOT_BYTES
                    ));
                }
                file_hasher.update(&buffer[..read]);
            }
            framed_hash_update(hasher, &file_hasher.finalize());
        } else {
            return Err(format!(
                "untracked repository path '{}' has an unsupported file type",
                path.display()
            ));
        }
    }
    Ok(())
}

fn ensure_initialized_submodules_are_clean(root: &Path) -> Result<(), String> {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "submodule",
            "foreach",
            "--recursive",
            "--quiet",
            "test -z \"$(git status --porcelain=v1 --untracked-files=all --ignore-submodules=none)\"",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("git submodule metadata is unavailable: {error}"))?;
    if !status.success() {
        return Err(format!(
            "repository '{}' has a dirty or unreadable initialized submodule",
            root.display()
        ));
    }
    Ok(())
}

pub fn snapshot_repository(path: &Path) -> Result<RepositorySnapshot, String> {
    #[cfg(test)]
    let _env_read = crate::hooks::test_helpers::process_env_read();
    let root = canonical_repository_root(path)?;
    let root_text = root
        .to_str()
        .ok_or_else(|| "repository root must be valid UTF-8".to_string())?
        .to_string();
    let revision = clean_git_text(
        bounded_git_output(&root, &["rev-parse", "--verify", "HEAD"])?,
        "repository revision",
        MAX_REPOSITORY_FIELD_BYTES,
    )?;
    let branch = repository_branch(&root)?;
    let status = bounded_git_output(
        &root,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
    )?;
    let dirty_summary = dirty_summary(&status)?;
    let unstaged = bounded_git_output(
        &root,
        &[
            "diff",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--ignore-submodules=none",
            "--",
        ],
    )?;
    let staged = bounded_git_output(
        &root,
        &[
            "diff",
            "--cached",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--ignore-submodules=none",
            "--",
        ],
    )?;
    let untracked =
        bounded_git_output(&root, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    let submodules = bounded_git_output(&root, &["submodule", "status", "--recursive"])?;
    ensure_initialized_submodules_are_clean(&root)?;

    let mut hasher = Sha256::new();
    for part in [
        revision.as_bytes(),
        branch.as_bytes(),
        status.as_slice(),
        unstaged.as_slice(),
        staged.as_slice(),
        submodules.as_slice(),
    ] {
        framed_hash_update(&mut hasher, part);
    }
    hash_untracked_files(&root, &untracked, &mut hasher)?;
    let state_digest = finalize_hex(hasher);
    Ok(RepositorySnapshot {
        root: root_text,
        revision,
        branch,
        dirty_summary,
        state_digest,
    })
}

pub fn normalize_bundle_repositories(bundle: &mut Value, base: &Path) -> Result<(), String> {
    let Some(raw_repositories) = bundle.get("repositories").cloned() else {
        return Ok(());
    };
    let repositories = raw_repositories
        .as_array()
        .ok_or("repositories must be a list")?;
    if repositories.len() > MAX_BUNDLE_REPOSITORIES {
        return Err(format!(
            "repositories exceeds the {} entry bound",
            MAX_BUNDLE_REPOSITORIES
        ));
    }
    let mut snapshots = Vec::with_capacity(repositories.len());
    for repository in repositories {
        let raw = match repository {
            Value::String(path) => path.as_str(),
            Value::Object(object) => object
                .get("root")
                .and_then(Value::as_str)
                .ok_or("repository snapshot object must contain a string root")?,
            _ => return Err("repository entries must be paths or snapshot objects".to_string()),
        };
        if raw.is_empty() || raw.len() > MAX_REPOSITORY_PATH_BYTES {
            return Err("repository path is empty or exceeds the path bound".to_string());
        }
        let path = Path::new(raw);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            base.join(path)
        };
        snapshots.push(snapshot_repository(&path)?);
    }
    snapshots.sort_by(|left, right| left.root.cmp(&right.root));
    if snapshots
        .windows(2)
        .any(|pair| pair[0].root == pair[1].root)
    {
        return Err("repositories contains the same canonical Git root more than once".to_string());
    }
    bundle["repositories"] = serde_json::to_value(snapshots).map_err(|error| error.to_string())?;
    Ok(())
}

pub fn repository_snapshots(bundle: &Value) -> Result<Vec<RepositorySnapshot>, String> {
    let Some(value) = bundle.get("repositories") else {
        return Ok(Vec::new());
    };
    let snapshots: Vec<RepositorySnapshot> = serde_json::from_value(value.clone())
        .map_err(|error| format!("repositories contains invalid snapshot metadata: {error}"))?;
    if snapshots.len() > MAX_BUNDLE_REPOSITORIES {
        return Err(format!(
            "repositories exceeds the {} entry bound",
            MAX_BUNDLE_REPOSITORIES
        ));
    }
    let mut previous: Option<&str> = None;
    for snapshot in &snapshots {
        if snapshot.root.is_empty()
            || snapshot.root.len() > MAX_REPOSITORY_PATH_BYTES
            || !Path::new(&snapshot.root).is_absolute()
            || snapshot.revision.is_empty()
            || snapshot.revision.len() > MAX_REPOSITORY_FIELD_BYTES
            || snapshot.branch.is_empty()
            || snapshot.branch.len() > MAX_REPOSITORY_FIELD_BYTES
            || snapshot.dirty_summary.is_empty()
            || snapshot.dirty_summary.len() > MAX_REPOSITORY_FIELD_BYTES
            || snapshot.state_digest.len() != 64
            || !snapshot
                .state_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(
                "repositories contains unbounded or malformed snapshot metadata".to_string(),
            );
        }
        if previous.is_some_and(|root| root >= snapshot.root.as_str()) {
            return Err(
                "repositories must contain unique canonical roots in sorted order".to_string(),
            );
        }
        previous = Some(&snapshot.root);
    }
    Ok(snapshots)
}

pub fn repository_manifest_digest(snapshots: &[RepositorySnapshot]) -> Result<String, String> {
    let canonical = serde_json::to_vec(snapshots).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    framed_hash_update(&mut hasher, &canonical);
    Ok(finalize_hex(hasher))
}

pub fn verify_bundle_repositories(bundle: &Value) -> Result<Vec<RepositorySnapshot>, String> {
    let expected = repository_snapshots(bundle)?;
    for snapshot in &expected {
        let current = snapshot_repository(Path::new(&snapshot.root))?;
        if current != *snapshot {
            return Err(format!(
                "repository '{}' changed after the bundle snapshot",
                snapshot.root
            ));
        }
    }
    Ok(expected)
}

/// Parse comma-separated list into list of non-empty trimmed strings.
pub fn parse_csv_list(raw: Option<&str>) -> Vec<String> {
    match raw {
        None => vec![],
        Some(s) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
    }
}

/// Get bundle instance name from identity.
pub fn get_bundle_instance_name(identity: &SenderIdentity) -> String {
    match identity.kind {
        SenderKind::External => format!("ext_{}", identity.name),
        SenderKind::System => format!("sys_{}", identity.name),
        SenderKind::Instance => identity.name.clone(),
    }
}

/// Generate a short random bundle id.
pub fn generate_bundle_id() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 4] = rng.random();
    format!("bundle:{}", hex::encode(&bytes))
}

// Inline hex encoding (avoids adding `hex` crate).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Parse a transcript reference into normalized format.
///
/// Accepts string "range:detail" (e.g., "3-14:normal", "6:full")
/// or object {"range": "6", "detail": "full", "note": "..."}.
pub fn parse_transcript_ref(ref_val: &Value) -> Result<Value, String> {
    match ref_val {
        Value::Object(map) => {
            let _range = map
                .get("range")
                .and_then(|v| v.as_str())
                .ok_or("Transcript ref object must have 'range' field")?;
            let detail = map
                .get("detail")
                .and_then(|v| v.as_str())
                .ok_or("Transcript ref object must have 'detail' field")?;
            validate_detail_level(detail)?;
            // Return as-is (already normalized)
            Ok(ref_val.clone())
        }
        Value::String(s) => {
            if !s.contains(':') {
                return Err(format!(
                    "Transcript ref must include detail level. Got: '{}'\n\
                     Format: \"range:detail\" (e.g., \"3-14:normal\", \"10:full\", \"20-25:detailed\")",
                    s
                ));
            }
            let (range_part, detail) = s.split_once(':').unwrap();
            let range_trimmed = range_part.trim();
            let detail_trimmed = detail.trim();

            if range_trimmed.is_empty() {
                return Err(format!("Empty range in transcript ref: '{}'", s));
            }
            if detail_trimmed.is_empty() {
                return Err(format!("Empty detail level in transcript ref: '{}'", s));
            }

            validate_detail_level(detail_trimmed)?;

            let mut obj = serde_json::Map::new();
            obj.insert("range".into(), Value::String(range_trimmed.into()));
            obj.insert("detail".into(), Value::String(detail_trimmed.into()));
            Ok(Value::Object(obj))
        }
        _ => Err(format!(
            "Transcript ref must be a string or object, got {:?}",
            ref_val
        )),
    }
}

/// Maximum estimated lines for bundle output to prevent massive dumps.
const MAX_ESTIMATED_LINES: usize = 15_000;

/// Validate bundle payload fields and types.
pub fn validate_bundle(bundle: &mut Value) -> Result<(), String> {
    let obj = bundle
        .as_object_mut()
        .ok_or("bundle must be a JSON object")?;

    // Required fields
    let missing: Vec<&str> = ["title", "description", "refs"]
        .iter()
        .filter(|k| !obj.contains_key(**k))
        .copied()
        .collect();
    if !missing.is_empty() {
        return Err(format!("Missing required fields: {}", missing.join(", ")));
    }

    // Estimate bundle size
    if let Some(Value::Object(refs)) = obj.get("refs") {
        let files_len = refs
            .get("files")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let events_len = refs
            .get("events")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let transcript_len = refs
            .get("transcript")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);

        let estimated = files_len + events_len * 50 + transcript_len * 500;
        if estimated > MAX_ESTIMATED_LINES {
            return Err(format!(
                "Bundle too large (estimated {} lines of output). \
                 Limit is {} lines. Split into multiple smaller bundles.",
                estimated, MAX_ESTIMATED_LINES
            ));
        }
    }

    // Type checks
    if !obj.get("title").is_some_and(|v| v.is_string()) {
        return Err("title must be a string".into());
    }
    if !obj.get("description").is_some_and(|v| v.is_string()) {
        return Err("description must be a string".into());
    }

    let refs = obj.get("refs").ok_or("refs must be an object")?;
    if !refs.is_object() {
        return Err("refs must be an object".into());
    }
    let refs_obj = refs.as_object().unwrap();

    for key in &["events", "files", "transcript"] {
        if !refs_obj.contains_key(*key) {
            return Err(format!("refs.{} is required", key));
        }
        if !refs_obj[*key].is_array() {
            return Err(format!("refs.{} must be a list", key));
        }
    }

    // Non-empty refs
    if refs_obj["transcript"]
        .as_array()
        .is_some_and(|a| a.is_empty())
    {
        return Err("refs.transcript is required\n\
             Find ranges: hcom transcript <agent> [--last N]\n\
             Format: \"1-5:normal,10:full\""
            .into());
    }
    if refs_obj["events"].as_array().is_some_and(|a| a.is_empty()) {
        return Err("refs.events is required\n\
             Find events: hcom events [--last N]\n\
             Format: \"123,124\" or \"100-105\""
            .into());
    }
    if refs_obj["files"].as_array().is_some_and(|a| a.is_empty()) {
        return Err("refs.files is required\n\
             Include files you created, modified, discussed, or are relevant\n\
             Format: \"src/main.py,tests/test.py\""
            .into());
    }

    // Parse and normalize transcript refs
    let transcript_arr = refs_obj["transcript"].as_array().unwrap().clone();
    let mut normalized = Vec::with_capacity(transcript_arr.len());
    for ref_val in &transcript_arr {
        let parsed =
            parse_transcript_ref(ref_val).map_err(|e| format!("Invalid transcript ref: {}", e))?;
        normalized.push(parsed);
    }

    // Write normalized transcript back
    let refs_mut = obj.get_mut("refs").unwrap().as_object_mut().unwrap();
    refs_mut.insert("transcript".into(), Value::Array(normalized));

    // Check file existence (warn but don't error)
    if let Some(files) = refs_mut.get("files").and_then(|v| v.as_array()) {
        let missing_files: Vec<&str> = files
            .iter()
            .filter_map(|f| f.as_str())
            .filter(|path| !Path::new(path).exists())
            .collect();
        if !missing_files.is_empty() {
            eprintln!(
                "Warning: {} file(s) not found locally:",
                missing_files.len()
            );
            for f in missing_files.iter().take(5) {
                eprintln!("  - {}", f);
            }
            if missing_files.len() > 5 {
                eprintln!("  ... and {} more", missing_files.len() - 5);
            }
        }
    }

    // Validate extends
    if let Some(extends) = obj.get("extends")
        && !extends.is_string()
    {
        return Err("extends must be a string".into());
    }
    repository_snapshots(&Value::Object(obj.clone()))?;
    // Note: parent bundle existence check requires DB access.
    // Call validate_extends_reference() separately when DB is available.

    // Validate bundle_id
    if let Some(bid) = obj.get("bundle_id")
        && !bid.is_string()
    {
        return Err("bundle_id must be a string".into());
    }

    Ok(())
}

/// Validate extends reference against DB (checks parent bundle exists).
///
/// Warns to stderr if parent not found (non-fatal).
/// Call after validate_bundle when a DB handle is available.
pub fn validate_extends_reference(bundle: &Value, db: &crate::db::HcomDb) {
    let extends_val = match bundle.get("extends").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v,
        _ => return,
    };

    let search_id = if extends_val.starts_with("bundle:") {
        extends_val.to_string()
    } else {
        format!("bundle:{}", extends_val)
    };

    match db.conn().prepare(
        "SELECT id FROM events WHERE type = 'bundle' AND json_extract(data, '$.bundle_id') = ?1",
    ) {
        Ok(mut stmt) => {
            match stmt.query_row(rusqlite::params![search_id], |_| Ok(())) {
                Ok(()) => {} // Found
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    eprintln!("Warning: Parent bundle not found: {}", extends_val);
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Could not validate parent bundle '{}': {}",
                        extends_val, e
                    );
                }
            }
        }
        Err(e) => {
            eprintln!(
                "Warning: Could not validate parent bundle '{}': {}",
                extends_val, e
            );
        }
    }
}

/// Create a bundle event and return its bundle_id.
pub fn create_bundle_event(
    bundle: &mut Value,
    instance: &str,
    created_by: Option<&str>,
    db: &crate::db::HcomDb,
) -> Result<String, HcomError> {
    match bundle.get("repositories") {
        Some(Value::Array(repositories)) if repositories.is_empty() => {
            normalize_bundle_repositories(bundle, Path::new("."))
                .map_err(HcomError::InvalidInput)?;
        }
        Some(_) => {
            let base = std::env::current_dir().map_err(|error| {
                HcomError::InvalidInput(format!(
                    "Cannot resolve bundle creation directory: {error}"
                ))
            })?;
            normalize_bundle_repositories(bundle, &base).map_err(HcomError::InvalidInput)?;
        }
        None => {}
    }
    validate_bundle(bundle).map_err(HcomError::InvalidInput)?;
    validate_extends_reference(bundle, db);

    let obj = bundle.as_object_mut().unwrap();

    let bundle_id = obj
        .get("bundle_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(generate_bundle_id);

    obj.insert("bundle_id".into(), Value::String(bundle_id.clone()));

    if let Some(by) = created_by {
        obj.insert("created_by".into(), Value::String(by.into()));
    }

    db.log_event("bundle", instance, &bundle.clone())
        .map_err(|e| HcomError::DatabaseError(format!("Failed to persist bundle event: {e}")))?;

    Ok(bundle_id)
}

/// Parse inline bundle creation flags from argv.
///
/// Returns (bundle_json, remaining_argv) if --title present, (None, argv) otherwise.
pub fn parse_inline_bundle_flags(argv: &[String]) -> Result<(Option<Value>, Vec<String>), String> {
    let bundle_flags = &[
        "--title",
        "--description",
        "--events",
        "--files",
        "--transcript",
        "--extends",
        "--repos",
    ];

    let has_any = bundle_flags.iter().any(|f| argv.contains(&f.to_string()));

    // Check for duplicate flags
    for flag in bundle_flags {
        let count = argv.iter().filter(|a| a.as_str() == *flag).count();
        if count > 1 {
            return Err(format!("Duplicate flag {} (found {} times)", flag, count));
        }
    }

    // If bundle flags present but no --title, error
    if has_any && !argv.contains(&"--title".to_string()) {
        let present: Vec<&&str> = bundle_flags
            .iter()
            .filter(|f| argv.contains(&f.to_string()))
            .collect();
        return Err(format!(
            "Bundle flags require --title: found {} without --title",
            present.iter().map(|f| **f).collect::<Vec<_>>().join(", ")
        ));
    }

    if !argv.contains(&"--title".to_string()) {
        return Ok((None, argv.to_vec()));
    }

    // Extract flag values
    let mut remaining = Vec::new();
    let mut flag_values: std::collections::HashMap<&str, Option<String>> =
        std::collections::HashMap::new();

    let mut i = 0;
    while i < argv.len() {
        let is_bundle_flag = bundle_flags.contains(&argv[i].as_str());
        if is_bundle_flag {
            let flag = argv[i].as_str();
            if i + 1 < argv.len() && !argv[i + 1].starts_with("--") {
                flag_values.insert(flag, Some(argv[i + 1].clone()));
                i += 2;
            } else {
                return Err(format!("Flag {} requires a value", flag));
            }
        } else {
            remaining.push(argv[i].clone());
            i += 1;
        }
    }

    let title = flag_values
        .get("--title")
        .and_then(|v| v.clone())
        .ok_or("--title is required for inline bundle creation")?;

    let description = flag_values
        .get("--description")
        .and_then(|v| v.clone())
        .ok_or("--description is required when --title is present")?;

    let events = parse_csv_list(flag_values.get("--events").and_then(|v| v.as_deref()));
    let files = parse_csv_list(flag_values.get("--files").and_then(|v| v.as_deref()));
    let transcript = parse_csv_list(flag_values.get("--transcript").and_then(|v| v.as_deref()));
    let repositories = parse_csv_list(flag_values.get("--repos").and_then(|v| v.as_deref()));

    let mut bundle = serde_json::json!({
        "title": title,
        "description": description,
        "refs": {
            "events": events,
            "files": files,
            "transcript": transcript,
        },
        "repositories": repositories,
    });

    if let Some(extends) = flag_values.get("--extends").and_then(|v| v.clone()) {
        bundle
            .as_object_mut()
            .unwrap()
            .insert("extends".into(), Value::String(extends));
    }

    Ok((Some(bundle), remaining))
}

/// Categorize an event by its file operation context.
pub fn is_file_op_context(context: &str) -> bool {
    super::filters::FILE_OP_CONTEXTS.contains(&context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn run_git(root: &Path, args: &[&str]) {
        let _env_read = crate::hooks::test_helpers::process_env_read();
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository(dir: &TempDir, name: &str) -> PathBuf {
        let root = dir.path().join(name);
        std::fs::create_dir(&root).unwrap();
        run_git(&root, &["init", "-b", "main"]);
        run_git(&root, &["config", "user.name", "hcom test"]);
        run_git(
            &root,
            &["config", "user.email", "hcom-test@example.invalid"],
        );
        std::fs::write(root.join("tracked.txt"), "base\n").unwrap();
        run_git(&root, &["add", "tracked.txt"]);
        run_git(&root, &["commit", "-m", "base"]);
        root
    }

    // ===== parse_csv_list =====

    #[test]
    fn test_parse_csv_list_basic() {
        assert_eq!(parse_csv_list(Some("a,b,c")), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_list_trim() {
        assert_eq!(parse_csv_list(Some(" a , b , c ")), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_list_empty() {
        let empty: Vec<String> = vec![];
        assert_eq!(parse_csv_list(None), empty);
        assert_eq!(parse_csv_list(Some("")), empty);
        assert_eq!(parse_csv_list(Some(",,,")), empty);
    }

    // ===== get_bundle_instance_name =====

    #[test]
    fn test_bundle_name_instance() {
        let id = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        assert_eq!(get_bundle_instance_name(&id), "luna");
    }

    #[test]
    fn test_bundle_name_external() {
        let id = SenderIdentity {
            kind: SenderKind::External,
            name: "user".into(),
            instance_data: None,
            session_id: None,
        };
        assert_eq!(get_bundle_instance_name(&id), "ext_user");
    }

    // ===== generate_bundle_id =====

    #[test]
    fn test_bundle_id_format() {
        let id = generate_bundle_id();
        assert!(id.starts_with("bundle:"));
        assert_eq!(id.len(), "bundle:".len() + 8); // 4 bytes = 8 hex chars
    }

    #[test]
    fn test_bundle_id_unique() {
        let a = generate_bundle_id();
        let b = generate_bundle_id();
        assert_ne!(a, b);
    }

    // ===== parse_transcript_ref =====

    #[test]
    fn test_parse_ref_string() {
        let val = serde_json::json!("3-14:normal");
        let parsed = parse_transcript_ref(&val).unwrap();
        assert_eq!(parsed["range"], "3-14");
        assert_eq!(parsed["detail"], "normal");
    }

    #[test]
    fn test_parse_ref_object() {
        let val = serde_json::json!({"range": "6", "detail": "full", "note": "design"});
        let parsed = parse_transcript_ref(&val).unwrap();
        assert_eq!(parsed["range"], "6");
        assert_eq!(parsed["detail"], "full");
        assert_eq!(parsed["note"], "design");
    }

    #[test]
    fn test_parse_ref_no_colon() {
        let val = serde_json::json!("3-14");
        let err = parse_transcript_ref(&val).unwrap_err();
        assert!(err.contains("must include detail level"));
    }

    #[test]
    fn test_parse_ref_invalid_detail() {
        let val = serde_json::json!("3-14:verbose");
        let err = parse_transcript_ref(&val).unwrap_err();
        assert!(err.contains("Invalid detail level"));
    }

    #[test]
    fn test_parse_ref_empty_range() {
        let val = serde_json::json!(":normal");
        let err = parse_transcript_ref(&val).unwrap_err();
        assert!(err.contains("Empty range"));
    }

    // ===== validate_bundle =====

    #[test]
    fn test_validate_bundle_valid() {
        let mut bundle = serde_json::json!({
            "title": "Test bundle",
            "description": "Testing",
            "refs": {
                "events": ["123"],
                "files": ["/tmp/test.rs"],
                "transcript": ["1-5:normal"]
            }
        });
        assert!(validate_bundle(&mut bundle).is_ok());
        // Check transcript was normalized
        let refs = bundle["refs"]["transcript"].as_array().unwrap();
        assert_eq!(refs[0]["range"], "1-5");
        assert_eq!(refs[0]["detail"], "normal");
    }

    #[test]
    fn repository_snapshot_detects_content_changes_with_identical_dirty_counts() {
        let dir = tempfile::tempdir().unwrap();
        let root = repository(&dir, "repo");
        std::fs::write(root.join("tracked.txt"), "first dirty value\n").unwrap();
        std::fs::write(root.join("untracked.txt"), "first untracked value\n").unwrap();
        let first = snapshot_repository(&root).unwrap();
        let bundle = serde_json::json!({"repositories": [first.clone()]});

        std::fs::write(root.join("tracked.txt"), "second dirty value\n").unwrap();
        std::fs::write(root.join("untracked.txt"), "second untracked value\n").unwrap();
        let second = snapshot_repository(&root).unwrap();

        assert_eq!(first.revision, second.revision);
        assert_eq!(first.branch, second.branch);
        assert_eq!(first.dirty_summary, second.dirty_summary);
        assert_ne!(first.state_digest, second.state_digest);
        assert!(verify_bundle_repositories(&bundle).is_err());
    }

    #[test]
    fn repository_snapshot_rejects_dirty_submodules_instead_of_missing_their_content() {
        let dir = tempfile::tempdir().unwrap();
        let parent = repository(&dir, "parent");
        let child = repository(&dir, "child-source");
        run_git(
            &parent,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                child.to_str().unwrap(),
                "child",
            ],
        );
        run_git(&parent, &["commit", "-am", "add submodule"]);
        snapshot_repository(&parent).unwrap();

        std::fs::write(
            parent.join("child/tracked.txt"),
            "dirty submodule content\n",
        )
        .unwrap();
        let error = snapshot_repository(&parent).unwrap_err();
        assert!(
            error.contains("dirty or unreadable initialized submodule"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn bundle_repository_snapshots_support_zero_or_multiple_external_roots() {
        let dir = tempfile::tempdir().unwrap();
        let launch_cwd = dir.path().join("non-git-launch");
        std::fs::create_dir(&launch_cwd).unwrap();
        let later = repository(&dir, "z-repository");
        let earlier = repository(&dir, "a-repository");

        let mut zero = serde_json::json!({"repositories": []});
        normalize_bundle_repositories(&mut zero, &launch_cwd).unwrap();
        assert!(repository_snapshots(&zero).unwrap().is_empty());

        let mut multiple = serde_json::json!({
            "repositories": [later, earlier],
        });
        normalize_bundle_repositories(&mut multiple, &launch_cwd).unwrap();
        let snapshots = repository_snapshots(&multiple).unwrap();
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots[0].root < snapshots[1].root);
        verify_bundle_repositories(&multiple).unwrap();
    }

    #[test]
    fn test_validate_bundle_missing_title() {
        let mut bundle = serde_json::json!({
            "description": "Testing",
            "refs": {"events": [], "files": [], "transcript": []}
        });
        let err = validate_bundle(&mut bundle).unwrap_err();
        assert!(err.contains("Missing required fields"));
    }

    #[test]
    fn test_validate_bundle_empty_transcript() {
        let mut bundle = serde_json::json!({
            "title": "Test",
            "description": "Testing",
            "refs": {"events": ["1"], "files": ["a.py"], "transcript": []}
        });
        let err = validate_bundle(&mut bundle).unwrap_err();
        assert!(err.contains("refs.transcript is required"));
    }

    #[test]
    fn test_validate_bundle_empty_events() {
        let mut bundle = serde_json::json!({
            "title": "Test",
            "description": "Testing",
            "refs": {"events": [], "files": ["a.py"], "transcript": ["1:normal"]}
        });
        let err = validate_bundle(&mut bundle).unwrap_err();
        assert!(err.contains("refs.events is required"));
    }

    #[test]
    fn test_validate_bundle_empty_files() {
        let mut bundle = serde_json::json!({
            "title": "Test",
            "description": "Testing",
            "refs": {"events": ["1"], "files": [], "transcript": ["1:normal"]}
        });
        let err = validate_bundle(&mut bundle).unwrap_err();
        assert!(err.contains("refs.files is required"));
    }

    // ===== parse_inline_bundle_flags =====

    #[test]
    fn test_parse_inline_no_flags() {
        let argv: Vec<String> = vec!["--last".into(), "20".into()];
        let (bundle, remaining) = parse_inline_bundle_flags(&argv).unwrap();
        assert!(bundle.is_none());
        assert_eq!(remaining, argv);
    }

    #[test]
    fn test_parse_inline_with_title() {
        let argv: Vec<String> = vec![
            "--title".into(),
            "Test".into(),
            "--description".into(),
            "Desc".into(),
            "--events".into(),
            "1,2".into(),
            "--files".into(),
            "a.py".into(),
            "--transcript".into(),
            "1:normal".into(),
            "--repos".into(),
            "/repo/one,/repo/two".into(),
        ];
        let (bundle, remaining) = parse_inline_bundle_flags(&argv).unwrap();
        assert!(bundle.is_some());
        let b = bundle.unwrap();
        assert_eq!(b["title"], "Test");
        assert_eq!(b["description"], "Desc");
        assert_eq!(b["repositories"][0], "/repo/one");
        assert_eq!(b["repositories"][1], "/repo/two");
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_parse_inline_flags_without_title() {
        let argv: Vec<String> = vec!["--description".into(), "Desc".into()];
        let err = parse_inline_bundle_flags(&argv).unwrap_err();
        assert!(err.contains("require --title"));
    }

    #[test]
    fn test_parse_inline_duplicate_flag() {
        let argv: Vec<String> = vec!["--title".into(), "A".into(), "--title".into(), "B".into()];
        let err = parse_inline_bundle_flags(&argv).unwrap_err();
        assert!(err.contains("Duplicate flag"));
    }
}
