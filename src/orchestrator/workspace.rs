//! `<project>/hcom-tasks/` — the durable, human-browsable task workspace.
//!
//! The workspace is evidence and human handoff material, never a recovery
//! store: the supervisor's state lives only in memory, and a restarted hcom
//! never reads this tree to resume a run. Only hcom writes here; workers get
//! no writable mount anywhere inside it.

use anyhow::{Context, Result, bail};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

pub const WORKSPACE_DIR_NAME: &str = "hcom-tasks";
const MARKER_FILE: &str = ".hcom-arch-tasks";
const LOCK_FILE: &str = ".lock";
const GITIGNORE_FILE: &str = ".gitignore";
const LATEST_LINK: &str = "latest";
const LATEST_TMP: &str = ".latest.tmp";
const MAX_RUN_FILE_BYTES: usize = 256 * 1024;
const MAX_DECISION_LINE_BYTES: usize = 4096;

/// The per-run workspace under `<project>/hcom-tasks/<run-id>/`, holding the
/// exclusive whole-workspace lock for the lifetime of the run.
#[derive(Debug)]
pub struct TasksWorkspace {
    root: PathBuf,
    run_dir: PathBuf,
    run_id: String,
    _lock: File,
}

impl TasksWorkspace {
    /// Open (creating if needed) the project workspace and claim a fresh run
    /// directory. Fails closed on foreign directories, unsafe permissions,
    /// and concurrent runs in the same project.
    pub fn open(project_root: &Path, run_id: &str) -> Result<Self> {
        if !project_root.is_absolute() {
            bail!("tasks workspace requires an absolute project root");
        }
        validate_run_id(run_id)?;
        let root = project_root.join(WORKSPACE_DIR_NAME);

        match fs::symlink_metadata(&root) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    bail!(
                        "refusing to use {}: not a directory (symlinks are rejected)",
                        root.display()
                    );
                }
                if !root.join(MARKER_FILE).is_file() {
                    bail!(
                        "refusing to touch {}: directory exists without the {MARKER_FILE} \
                         ownership marker; it was not created by hcom arch",
                        root.display()
                    );
                }
                validate_root_safety(&root, &metadata)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&root)
                    .with_context(|| format!("failed to create {}", root.display()))?;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                    .with_context(|| format!("failed to set mode on {}", root.display()))?;
                write_exclusive_file(
                    &root.join(MARKER_FILE),
                    format!(
                        "schema=1\ncreated-by=hcom-arch\ncrate-version={}\n",
                        env!("CARGO_PKG_VERSION")
                    )
                    .as_bytes(),
                )?;
                // Written only on fresh creation: a human deleting it later is
                // a deliberate choice to track evidence in version control.
                write_exclusive_file(&root.join(GITIGNORE_FILE), b"*\n")?;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to inspect {}", root.display()));
            }
        }

        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(root.join(LOCK_FILE))
            .with_context(|| format!("failed to open {}/{LOCK_FILE}", root.display()))?;
        // SAFETY: flock operates on this live file descriptor.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!(
                "another hcom arch run already holds {}/{LOCK_FILE} for this project",
                root.display()
            );
        }

        let run_dir = root.join(run_id);
        fs::create_dir(&run_dir)
            .with_context(|| format!("failed to create run directory {}", run_dir.display()))?;
        fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to set mode on {}", run_dir.display()))?;

        // Atomic `latest` update: build a fresh symlink and rename it over.
        let tmp = root.join(LATEST_TMP);
        let _ = fs::remove_file(&tmp);
        symlink(run_id, &tmp)
            .with_context(|| format!("failed to stage latest link in {}", root.display()))?;
        fs::rename(&tmp, root.join(LATEST_LINK))
            .with_context(|| format!("failed to publish latest link in {}", root.display()))?;

        Ok(Self {
            root,
            run_dir,
            run_id: run_id.to_string(),
            _lock: lock,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Write a run-level control file (for example `plan.md`) exactly once.
    pub fn write_run_file(&self, name: &str, contents: &[u8]) -> Result<()> {
        validate_file_name(name)?;
        if contents.len() > MAX_RUN_FILE_BYTES {
            bail!("run file {name} exceeds {MAX_RUN_FILE_BYTES} bytes");
        }
        write_exclusive_file(&self.run_dir.join(name), contents)
    }

    /// Append one single-line entry to the run's `decision.log`.
    pub fn append_decision(&self, line: &str) -> Result<()> {
        if line.len() > MAX_DECISION_LINE_BYTES {
            bail!("decision log line exceeds {MAX_DECISION_LINE_BYTES} bytes");
        }
        let sanitized: String = line
            .chars()
            .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
            .collect();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.run_dir.join("decision.log"))
            .with_context(|| {
                format!("failed to open decision log in {}", self.run_dir.display())
            })?;
        writeln!(file, "{sanitized}").context("failed to append decision log entry")
    }
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty()
        || run_id.len() > 64
        || !run_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        bail!("run id must be 1..=64 chars of [a-z0-9-]");
    }
    Ok(())
}

fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || name.starts_with('.')
        || name.contains('/')
        || name.contains('\\')
        || name == LATEST_LINK
    {
        bail!("run file name must be one plain non-hidden path component");
    }
    Ok(())
}

fn validate_root_safety(root: &Path, metadata: &fs::Metadata) -> Result<()> {
    // SAFETY: geteuid has no failure modes.
    let euid = unsafe { libc::geteuid() };
    if metadata.uid() != euid {
        bail!(
            "refusing to use {}: owned by uid {} instead of the current user ({})",
            root.display(),
            metadata.uid(),
            euid
        );
    }
    if metadata.permissions().mode() & 0o002 != 0 {
        bail!(
            "refusing to use {}: directory is world-writable",
            root.display()
        );
    }
    Ok(())
}

fn write_exclusive_file(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_project() -> tempfile::TempDir {
        tempfile::tempdir().expect("temp project")
    }

    #[test]
    fn fresh_workspace_is_created_with_marker_gitignore_and_latest() {
        let project = temp_project();
        let workspace = TasksWorkspace::open(project.path(), "run-1").unwrap();
        let root = project.path().join(WORKSPACE_DIR_NAME);
        assert!(root.join(MARKER_FILE).is_file());
        assert_eq!(
            fs::read_to_string(root.join(GITIGNORE_FILE)).unwrap(),
            "*\n"
        );
        assert_eq!(
            fs::read_link(root.join(LATEST_LINK)).unwrap(),
            PathBuf::from("run-1")
        );
        assert_eq!(
            fs::metadata(workspace.run_dir())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn foreign_directory_without_marker_is_refused() {
        let project = temp_project();
        fs::create_dir(project.path().join(WORKSPACE_DIR_NAME)).unwrap();
        let error = TasksWorkspace::open(project.path(), "run-1").unwrap_err();
        assert!(error.to_string().contains("ownership marker"), "{error}");
    }

    #[test]
    fn world_writable_root_is_refused() {
        let project = temp_project();
        drop(TasksWorkspace::open(project.path(), "run-1").unwrap());
        let root = project.path().join(WORKSPACE_DIR_NAME);
        fs::set_permissions(&root, fs::Permissions::from_mode(0o707)).unwrap();
        let error = TasksWorkspace::open(project.path(), "run-2").unwrap_err();
        assert!(error.to_string().contains("world-writable"), "{error}");
    }

    #[test]
    fn concurrent_open_fails_while_lock_is_held() {
        let project = temp_project();
        let first = TasksWorkspace::open(project.path(), "run-1").unwrap();
        let error = TasksWorkspace::open(project.path(), "run-2").unwrap_err();
        assert!(error.to_string().contains("already holds"), "{error}");
        drop(first);
        TasksWorkspace::open(project.path(), "run-3").unwrap();
    }

    #[test]
    fn reuse_updates_latest_and_does_not_recreate_deleted_gitignore() {
        let project = temp_project();
        drop(TasksWorkspace::open(project.path(), "run-1").unwrap());
        let root = project.path().join(WORKSPACE_DIR_NAME);
        fs::remove_file(root.join(GITIGNORE_FILE)).unwrap();
        drop(TasksWorkspace::open(project.path(), "run-2").unwrap());
        assert!(!root.join(GITIGNORE_FILE).exists());
        assert_eq!(
            fs::read_link(root.join(LATEST_LINK)).unwrap(),
            PathBuf::from("run-2")
        );
        assert!(root.join("run-1").is_dir());
        assert!(root.join("run-2").is_dir());
    }

    #[test]
    fn duplicate_run_id_is_refused() {
        let project = temp_project();
        let first = TasksWorkspace::open(project.path(), "run-1").unwrap();
        drop(first);
        let error = TasksWorkspace::open(project.path(), "run-1").unwrap_err();
        assert!(error.to_string().contains("run directory"), "{error}");
    }

    #[test]
    fn run_files_are_exclusive_and_decisions_append_single_lines() {
        let project = temp_project();
        let workspace = TasksWorkspace::open(project.path(), "run-1").unwrap();
        workspace.write_run_file("plan.md", b"# plan\n").unwrap();
        assert!(workspace.write_run_file("plan.md", b"again").is_err());
        assert!(workspace.write_run_file("../escape", b"x").is_err());
        assert!(workspace.write_run_file(".hidden", b"x").is_err());
        workspace
            .append_decision("task 0: developer turn started")
            .unwrap();
        workspace
            .append_decision("multi\nline gets\rflattened")
            .unwrap();
        let log = fs::read_to_string(workspace.run_dir().join("decision.log")).unwrap();
        assert_eq!(
            log,
            "task 0: developer turn started\nmulti line gets flattened\n"
        );
    }

    #[test]
    fn invalid_run_ids_are_refused() {
        let project = temp_project();
        for bad in ["", "UPPER", "has/slash", "a b", &"x".repeat(65)] {
            assert!(TasksWorkspace::open(project.path(), bad).is_err(), "{bad}");
        }
    }
}
