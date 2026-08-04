//! `<project>/hcom-tasks/` — the durable, human-browsable task workspace.
//!
//! The workspace is evidence and human handoff material, never a recovery
//! store: the supervisor's state lives only in memory, and a restarted hcom
//! never reads this tree to resume a run. The hcom APIs are its intended
//! writer, but native-equivalent workers have ordinary same-user host access;
//! this directory is not a tamper-proof boundary.

use anyhow::{Context, Result, bail};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink};
use std::path::{Component, Path, PathBuf};

pub const WORKSPACE_DIR_NAME: &str = "hcom-tasks";
const MARKER_FILE: &str = ".hcom-arch-tasks";
const LOCK_FILE: &str = ".lock";
const GITIGNORE_FILE: &str = ".gitignore";
const LATEST_LINK: &str = "latest";
const LATEST_TMP: &str = ".latest.tmp";
const MAX_RUN_FILE_BYTES: usize = 256 * 1024;
const MAX_CLARIFICATION_FILE_BYTES: usize = 256 * 1024;
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

        // O_CLOEXEC is load-bearing, not hygiene: every worker is fork+exec'd
        // from this process and would otherwise inherit this descriptor. The
        // flock lives on the open file description, so an inherited copy keeps
        // the lock held for as long as any worker survives — including after
        // this run exits.
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(root.join(LOCK_FILE))
            .with_context(|| format!("failed to open {}/{LOCK_FILE}", root.display()))?;
        // SAFETY: flock operates on this live file descriptor.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            // Only EWOULDBLOCK means someone else holds it; anything else is a
            // real failure and must not be reported as contention.
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                bail!(
                    "another hcom arch run already holds {}/{LOCK_FILE} for this project",
                    root.display()
                );
            }
            return Err(error)
                .with_context(|| format!("failed to lock {}/{LOCK_FILE}", root.display()));
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

    /// Reserve the exact path the Architect may use for one clarification.
    ///
    /// The file itself is intentionally not created: the Architect must
    /// create one new document at the returned path. Existing files are never
    /// edited or reused.
    pub fn prepare_clarification_path(&self, task_key: &str, sequence: u32) -> Result<PathBuf> {
        validate_task_key(task_key)?;
        if sequence == 0 {
            bail!("clarification sequence must be positive");
        }
        let task_dir = self.run_dir.join(task_key);
        ensure_private_plain_directory(&task_dir)?;
        let clarification_dir = task_dir.join("clarification");
        ensure_private_plain_directory(&clarification_dir)?;
        let path = clarification_dir.join(format!("turn-{sequence}.md"));
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path),
            Ok(_) => bail!("clarification path already exists: {}", path.display()),
            Err(error) => Err(error).with_context(|| {
                format!("failed to inspect clarification path {}", path.display())
            }),
        }
    }

    /// Validate the Architect-created clarification as bounded path transport.
    ///
    /// This deliberately does not interpret Markdown or infer requirements.
    pub fn validate_clarification_document(&self, expected: &Path) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(expected)
            .with_context(|| {
                format!(
                    "failed to open clarification document {}",
                    expected.display()
                )
            })?;
        let before = file.metadata()?;
        if !before.is_file() {
            bail!(
                "clarification document is not a regular file: {}",
                expected.display()
            );
        }
        if before.len() == 0 || before.len() > MAX_CLARIFICATION_FILE_BYTES as u64 {
            bail!(
                "clarification document must contain 1..={} bytes",
                MAX_CLARIFICATION_FILE_BYTES
            );
        }
        let mut bytes = Vec::with_capacity(before.len() as usize);
        (&mut file)
            .take(MAX_CLARIFICATION_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        let after = file.metadata()?;
        if (before.dev(), before.ino(), before.len()) != (after.dev(), after.ino(), after.len()) {
            bail!("clarification document changed while it was validated");
        }
        if bytes.len() != before.len() as usize {
            bail!("clarification document could not be read completely");
        }
        std::str::from_utf8(&bytes).context("clarification document must be valid UTF-8")?;
        Ok(())
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

fn validate_task_key(task_key: &str) -> Result<()> {
    if task_key.is_empty()
        || task_key.len() > 128
        || !task_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
        || !matches!(
            Path::new(task_key)
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [Component::Normal(_)]
        )
    {
        bail!("task key must be a bounded plain path component");
    }
    Ok(())
}

fn ensure_private_plain_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => bail!(
            "refusing to use {}: not a directory (symlinks are rejected)",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)
                .with_context(|| format!("failed to create directory {}", path.display()))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to set mode on {}", path.display()))
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to inspect directory {}", path.display()))
        }
    }
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
    use serial_test::serial;

    /// A fixture directory that is never recycled during the run.
    ///
    /// `flock` is keyed by inode. A deleted fixture's inode can be handed
    /// straight back to the next fixture, so a sibling test thread whose lock
    /// file is not yet closed appears to hold *this* test's lock. Using a
    /// stable per-test path that is only cleaned on entry — never deleted
    /// while the suite runs — removes the aliasing. Production is unaffected:
    /// there `.lock` is a long-lived file inside the project directory.
    fn test_project(name: &str) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/test-workspaces");
        fs::create_dir_all(&root).expect("workspace test root");
        let project = root.join(name);
        let _ = fs::remove_dir_all(&project);
        fs::create_dir(&project).expect("test project");
        project
    }

    #[test]
    fn fresh_workspace_is_created_with_marker_gitignore_and_latest() {
        const TEST_NAME: &str = "fresh_workspace_is_created_with_marker_gitignore_and_latest";
        let project = test_project(TEST_NAME);
        let workspace = TasksWorkspace::open(&project, "run-1").unwrap();
        let root = &project.join(WORKSPACE_DIR_NAME);
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
            fs::metadata(root).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn clarification_paths_are_unique_and_documents_are_bounded_plain_utf8_files() {
        const TEST_NAME: &str =
            "clarification_paths_are_unique_and_documents_are_bounded_plain_utf8_files";
        let project = test_project(TEST_NAME);
        let workspace = TasksWorkspace::open(&project, "run-1").unwrap();
        assert!(workspace.prepare_clarification_path("..", 1).is_err());

        let first = workspace.prepare_clarification_path("task-one", 1).unwrap();
        assert_eq!(
            first,
            project.join("hcom-tasks/run-1/task-one/clarification/turn-1.md")
        );
        fs::write(&first, "bounded clarification\n").unwrap();
        workspace.validate_clarification_document(&first).unwrap();
        assert!(
            workspace.prepare_clarification_path("task-one", 1).is_err(),
            "an existing clarification must never be reused"
        );

        let empty = workspace.prepare_clarification_path("task-one", 2).unwrap();
        fs::write(&empty, []).unwrap();
        assert!(workspace.validate_clarification_document(&empty).is_err());

        let invalid_utf8 = workspace.prepare_clarification_path("task-one", 3).unwrap();
        fs::write(&invalid_utf8, [0xff]).unwrap();
        assert!(
            workspace
                .validate_clarification_document(&invalid_utf8)
                .is_err()
        );

        let symlink_path = workspace.prepare_clarification_path("task-one", 4).unwrap();
        symlink(&first, &symlink_path).unwrap();
        assert!(
            workspace
                .validate_clarification_document(&symlink_path)
                .is_err()
        );
    }

    #[test]
    fn foreign_directory_without_marker_is_refused() {
        const TEST_NAME: &str = "foreign_directory_without_marker_is_refused";
        let project = test_project(TEST_NAME);
        fs::create_dir(project.join(WORKSPACE_DIR_NAME)).unwrap();
        let error = TasksWorkspace::open(&project, "run-1").unwrap_err();
        assert!(error.to_string().contains("ownership marker"), "{error}");
    }

    #[test]
    #[serial]
    fn world_writable_root_is_refused() {
        const TEST_NAME: &str = "world_writable_root_is_refused";
        let project = test_project(TEST_NAME);
        drop(TasksWorkspace::open(&project, "run-1").unwrap());
        let root = &project.join(WORKSPACE_DIR_NAME);
        fs::set_permissions(root, fs::Permissions::from_mode(0o707)).unwrap();
        let error = TasksWorkspace::open(&project, "run-2").unwrap_err();
        assert!(error.to_string().contains("world-writable"), "{error}");
    }

    /// Mutual exclusion is a cross-process property (two `hcom arch` runs in
    /// one project), so it is asserted against a real second process. Testing
    /// it inside this process would instead measure same-process flock
    /// aliasing: `/tmp` recycles inodes fast enough that a sibling test
    /// thread's not-yet-closed lock file can land on the inode this test just
    /// created.
    #[test]
    #[serial]
    fn a_second_process_cannot_open_the_workspace_while_it_is_locked() {
        const TEST_NAME: &str = "a_second_process_cannot_open_the_workspace_while_it_is_locked";
        let project = test_project(TEST_NAME);
        let workspace = TasksWorkspace::open(&project, "run-1").unwrap();
        let lock_path = workspace.root().join(LOCK_FILE);

        let held = std::process::Command::new("/usr/bin/flock")
            .args(["--nonblock", "--exclusive"])
            .arg(&lock_path)
            .args(["true"])
            .status()
            .expect("run flock(1)");
        assert!(
            !held.success(),
            "a second process acquired the lock while this run holds it"
        );

        drop(workspace);
        let free = std::process::Command::new("/usr/bin/flock")
            .args(["--nonblock", "--exclusive"])
            .arg(&lock_path)
            .args(["true"])
            .status()
            .expect("run flock(1)");
        assert!(
            free.success(),
            "the lock was not released when the workspace was dropped"
        );
    }

    #[test]
    #[serial]
    fn opening_a_second_run_while_one_is_live_is_refused() {
        const TEST_NAME: &str = "opening_a_second_run_while_one_is_live_is_refused";
        let project = test_project(TEST_NAME);
        let first = TasksWorkspace::open(&project, "run-1").unwrap();
        let error = TasksWorkspace::open(&project, "run-2").unwrap_err();
        assert!(error.to_string().contains("already holds"), "{error}");
        drop(first);
    }

    #[test]
    fn run_files_are_exclusive_and_decisions_append_single_lines() {
        const TEST_NAME: &str = "run_files_are_exclusive_and_decisions_append_single_lines";
        let project = test_project(TEST_NAME);
        let workspace = TasksWorkspace::open(&project, "run-1").unwrap();
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
        const TEST_NAME: &str = "invalid_run_ids_are_refused";
        let project = test_project(TEST_NAME);
        for bad in ["", "UPPER", "has/slash", "a b", &"x".repeat(65)] {
            assert!(TasksWorkspace::open(&project, bad).is_err(), "{bad}");
        }
    }
}
