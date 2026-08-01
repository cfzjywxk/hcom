//! Foreground, in-memory task supervisor for one `hcom arch` invocation.

pub mod core;
#[cfg(target_os = "linux")]
pub(crate) mod task_lane;
#[cfg(target_os = "linux")]
pub mod workspace;

use crate::worker::ExecutableIdentity;
use crate::worker::codex::{GIT_EXECUTABLE, GIT_VERSION};
use crate::worker::environment::ParentEnvironment;
use crate::worker::profile::SessionInvocationProfiles;
use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

const MAX_GIT_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const GIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionStartup {
    pub(crate) run_id: String,
    pub(crate) project_root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct SessionRuntimeSources {
    parent_environment: ParentEnvironment,
    codex_auth_source: Option<PathBuf>,
    cargo_bin_source: PathBuf,
    rustup_home_source: PathBuf,
    profiles: Option<SessionInvocationProfiles>,
}

impl SessionRuntimeSources {
    pub(crate) fn capture(
        parent_environment: impl Into<ParentEnvironment>,
        host_runtime_dir: PathBuf,
        profiles: SessionInvocationProfiles,
    ) -> Result<Self> {
        profiles.validate()?;
        let parent_environment = parent_environment.into();
        let home = parent_environment
            .unicode("HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("session worker environment requires non-empty parent HOME"))?;
        let _host_runtime_dir =
            canonical_private_directory(&host_runtime_dir, "host XDG runtime directory")?;
        let codex_home = parent_environment
            .unicode("CODEX_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        let uses_codex =
            profiles.developer.codex().is_some() || profiles.reviewer.codex().is_some();
        let codex_auth_source = if uses_codex {
            Some(canonical_private_file(
                &codex_home.join("auth.json"),
                "Codex auth source",
            )?)
        } else {
            None
        };
        let cargo_bin_source = parent_environment
            .unicode("CARGO_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cargo"))
            .join("bin");
        let rustup_home_source = parent_environment
            .unicode("RUSTUP_HOME")?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".rustup"));
        let cargo_bin_source =
            canonical_readable_directory(&cargo_bin_source, "Rust cargo-bin source")?;
        let rustup_home_source =
            canonical_readable_directory(&rustup_home_source, "Rust rustup source")?;
        Ok(Self {
            parent_environment,
            codex_auth_source,
            cargo_bin_source,
            rustup_home_source,
            profiles: Some(profiles),
        })
    }

    #[cfg(test)]
    pub(crate) fn fake(path: &Path) -> Self {
        Self {
            parent_environment: std::collections::BTreeMap::from([(
                "PATH".into(),
                "/usr/bin:/bin".into(),
            )])
            .into(),
            codex_auth_source: None,
            cargo_bin_source: path.to_owned(),
            rustup_home_source: path.to_owned(),
            profiles: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_profiles_for_test(&mut self, profiles: SessionInvocationProfiles) {
        self.profiles = Some(profiles);
    }
}

struct CanonicalRepository {
    root: PathBuf,
    git: ExecutableIdentity,
    root_identity: DirectoryIdentity,
    git_dir: DirectoryIdentity,
    common_dir: DirectoryIdentity,
    object_dir: DirectoryIdentity,
}

struct ManagedRepository {
    repository: CanonicalRepository,
    _lock: RepositoryLock,
    current_head: String,
}

impl ManagedRepository {
    fn open(root: &Path, lock_root: &Path) -> Result<Self> {
        let repository = CanonicalRepository::open(root)?;
        let lock = RepositoryLock::acquire(&repository, lock_root)?;
        let branch = repository.branch()?;
        let start_head = repository.head()?;
        repository.require_exact(&branch, &start_head)?;
        Ok(Self {
            repository,
            _lock: lock,
            current_head: start_head,
        })
    }
}

impl CanonicalRepository {
    fn open(root: &Path) -> Result<Self> {
        if !root.is_absolute() {
            bail!("session repository must be absolute");
        }
        let root = fs::canonicalize(root).context("failed to canonicalize session repository")?;
        let root_identity =
            DirectoryIdentity::capture(&root, false).context("unsafe canonical checkout root")?;
        let git = capture_exact_git()?;
        let runner = GitRunner {
            git: &git,
            root: &root,
        };
        let top = canonical_git_path(&runner.one_line(&["rev-parse", "--show-toplevel"])?)?;
        if top != root {
            bail!("task repository_root must name the exact canonical Git top level");
        }
        let git_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
        ])?)?;
        let common_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        ])?)?;
        let object_dir = canonical_git_path(&runner.one_line(&[
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "objects",
        ])?)?;
        if !git_dir.starts_with(&root)
            || !common_dir.starts_with(&root)
            || !object_dir.starts_with(&root)
        {
            bail!("canonical checkout Git administration must remain inside the checkout");
        }
        let repository = Self {
            root,
            git,
            root_identity,
            git_dir: DirectoryIdentity::capture(&git_dir, false)
                .context("unsafe canonical checkout Git directory")?,
            common_dir: DirectoryIdentity::capture(&common_dir, false)
                .context("unsafe canonical checkout common Git directory")?,
            object_dir: DirectoryIdentity::capture(&object_dir, false)
                .context("unsafe canonical checkout object directory")?,
        };
        repository.reject_indirections()?;
        repository.require_clean_start()?;
        Ok(repository)
    }

    fn revalidate_identity(&self) -> Result<()> {
        self.git.revalidate()?;
        if DirectoryIdentity::capture(&self.root, false)? != self.root_identity
            || DirectoryIdentity::capture(self.git_dir.path(), false)? != self.git_dir
            || DirectoryIdentity::capture(self.common_dir.path(), false)? != self.common_dir
            || DirectoryIdentity::capture(self.object_dir.path(), false)? != self.object_dir
        {
            bail!("canonical repository identity drifted");
        }
        self.reject_indirections()
    }

    fn branch(&self) -> Result<String> {
        GitRunner {
            git: &self.git,
            root: &self.root,
        }
        .one_line(&["symbolic-ref", "--quiet", "--short", "HEAD"])
        .context("canonical checkout must have an attached branch")
    }

    fn head(&self) -> Result<String> {
        let head = GitRunner {
            git: &self.git,
            root: &self.root,
        }
        .one_line(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        validate_git_oid("canonical checkout HEAD", &head)?;
        Ok(head)
    }

    fn require_clean(&self) -> Result<()> {
        let runner = GitRunner {
            git: &self.git,
            root: &self.root,
        };
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
        {
            bail!("canonical checkout must be completely clean");
        }
        if !runner
            .success(&["for-each-ref", "--format=%(refname)", "refs/replace/"])?
            .is_empty()
        {
            bail!("canonical checkout contains replacement refs");
        }
        Ok(())
    }

    fn require_clean_start(&self) -> Result<()> {
        let runner = GitRunner {
            git: &self.git,
            root: &self.root,
        };
        if !runner
            .success(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])?
            .is_empty()
        {
            bail!("task repository is dirty before plan start");
        }
        if !runner
            .success(&["for-each-ref", "--format=%(refname)", "refs/replace/"])?
            .is_empty()
        {
            bail!("task repository contains replacement refs before plan start");
        }
        Ok(())
    }

    fn require_exact(&self, branch: &str, head: &str) -> Result<()> {
        self.revalidate_identity()?;
        self.require_clean()?;
        if self.branch()? != branch || self.head()? != head {
            bail!("canonical checkout branch or HEAD drifted");
        }
        self.revalidate_identity()?;
        self.require_clean()
    }

    fn reject_indirections(&self) -> Result<()> {
        for path in [
            self.common_dir.path().join("info/grafts"),
            self.object_dir.path().join("info/alternates"),
            self.object_dir.path().join("info/http-alternates"),
        ] {
            match fs::symlink_metadata(&path) {
                Ok(_) => bail!("canonical checkout uses forbidden Git object indirection"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

struct RepositoryLock {
    _file: File,
}

impl RepositoryLock {
    fn acquire(repository: &CanonicalRepository, root: &Path) -> Result<Self> {
        ensure_private_directory(root)?;
        let metadata = fs::metadata(&repository.root)?;
        let key = sha256_hex(serde_json::to_vec(&(metadata.dev(), metadata.ino()))?.as_slice());
        let path = root.join(format!("{key}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        let file_metadata = file.metadata()?;
        // SAFETY: geteuid has no preconditions.
        if file_metadata.uid() != unsafe { libc::geteuid() }
            || file_metadata.permissions().mode() & 0o777 != 0o600
            || file_metadata.nlink() != 1
        {
            bail!("repository lock file has an unsafe identity");
        }
        // SAFETY: flock operates on this live file descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                bail!("another architect session already owns this canonical checkout");
            }
            return Err(error.into());
        }
        Ok(Self { _file: file })
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DirectoryIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
}

impl DirectoryIdentity {
    fn capture(path: &Path, private: bool) -> Result<Self> {
        let link = fs::symlink_metadata(path)?;
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!("directory identity requires a real directory");
        }
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            bail!("directory identity path must already be canonical");
        }
        let metadata = fs::metadata(path)?;
        let identity = Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
        };
        // SAFETY: geteuid has no preconditions.
        if identity.uid != unsafe { libc::geteuid() } {
            bail!("directory identity is not owned by the current user");
        }
        if private && identity.mode != 0o700 {
            bail!("private directory must be mode 0700");
        }
        Ok(identity)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

struct GitRunner<'a> {
    git: &'a ExecutableIdentity,
    root: &'a Path,
}

struct BoundedOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl GitRunner<'_> {
    fn run(&self, args: &[&str]) -> Result<BoundedOutput> {
        let mut command = Command::new(&self.git.canonical_path);
        command
            .arg("--no-replace-objects")
            .args(["-c", "core.fsmonitor=false"])
            .args(["-c", "core.untrackedCache=false"])
            .args(["-c", "core.hooksPath=/dev/null"])
            .args(args)
            .current_dir(self.root)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_PAGER", "/bin/cat")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C");
        let output = run_bounded_command(command, MAX_GIT_OUTPUT_BYTES, GIT_TIMEOUT)?;
        Ok(output)
    }

    fn success(&self, args: &[&str]) -> Result<Vec<u8>> {
        let output = self.run(args)?;
        if !output.status.success() || !output.stderr.is_empty() {
            bail!("bounded Git evidence command failed");
        }
        Ok(output.stdout)
    }

    fn one_line(&self, args: &[&str]) -> Result<String> {
        let bytes = self.success(args)?;
        let text = std::str::from_utf8(&bytes)?
            .strip_suffix('\n')
            .unwrap_or(std::str::from_utf8(&bytes)?);
        if text.is_empty() || text.contains('\n') || text.contains('\r') {
            bail!("Git evidence did not contain one bounded line");
        }
        Ok(text.to_owned())
    }
}

fn capture_exact_git() -> Result<ExecutableIdentity> {
    let git = ExecutableIdentity::capture(Path::new(GIT_EXECUTABLE))?;
    let output = run_bounded_command(
        {
            let mut command = Command::new(GIT_EXECUTABLE);
            command.arg("--version").env_clear();
            command
        },
        4096,
        Duration::from_secs(5),
    )?;
    if !output.status.success()
        || !output.stderr.is_empty()
        || std::str::from_utf8(&output.stdout)?.trim_end() != GIT_VERSION
    {
        bail!("Git executable does not match the exact enabled version");
    }
    git.revalidate()?;
    Ok(git)
}

fn run_bounded_command(
    mut command: Command,
    cap: usize,
    timeout: Duration,
) -> Result<BoundedOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("missing stderr"))?;
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout_overflow = Arc::clone(&overflow);
    let stderr_overflow = Arc::clone(&overflow);
    let stdout_thread = std::thread::spawn(move || read_bounded(stdout, cap, stdout_overflow));
    let stderr_thread = std::thread::spawn(move || read_bounded(stderr, cap, stderr_overflow));
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if overflow.load(Ordering::Acquire) || started.elapsed() >= timeout {
            terminate_child(&mut child);
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            bail!("bounded Git command exceeded its output or time limit");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("stderr reader panicked"))??;
    if overflow.load(Ordering::Acquire) {
        bail!("bounded Git output exceeded its cap");
    }
    Ok(BoundedOutput {
        status,
        stdout,
        stderr,
    })
}

fn read_bounded(mut reader: impl Read, cap: usize, overflow: Arc<AtomicBool>) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if output.len().saturating_add(count) > cap {
            overflow.store(true, Ordering::Release);
            break;
        }
        output.extend_from_slice(&buffer[..count]);
    }
    Ok(output)
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for component in bytes.split(|byte| *byte == 0) {
        if component.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(component)?;
        crate::worker::validation::validate_relative_path("Git changed path", path)?;
        paths.push(path.to_owned());
        if paths.len() > 256 {
            bail!("Git changed paths exceed their bound");
        }
    }
    Ok(paths)
}

fn canonical_git_path(value: &str) -> Result<PathBuf> {
    if value.len() > 4096 {
        bail!("Git path exceeds its bound");
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() || fs::canonicalize(&path)? != path {
        bail!("Git path must be absolute and canonical");
    }
    Ok(path)
}

fn canonical_project_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("architect project directory must be absolute");
    }
    let canonical =
        fs::canonicalize(path).context("failed to resolve architect project directory")?;
    let metadata = fs::symlink_metadata(path)?;
    if canonical != path || metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("architect project directory must be an existing canonical directory");
    }
    Ok(canonical)
}

fn path_value(label: &str, path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{label} is not valid UTF-8"))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| {
                format!("failed to create private directory {}", path.display())
            })?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Err(error) => return Err(error.into()),
    }
    let _ = canonical_private_directory(path, "private session directory")?;
    Ok(())
}

fn canonical_private_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).with_context(|| format!("failed to resolve {label}"))?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("{label} must be canonical, current-user owned, and mode 0700");
    }
    Ok(canonical)
}

fn canonical_readable_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o002 != 0
    {
        bail!("{label} has an unsafe identity");
    }
    Ok(canonical)
}

fn canonical_private_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions.
    if canonical != path
        || metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o600 != 0o600
        || metadata.nlink() != 1
    {
        bail!("{label} has an unsafe identity");
    }
    Ok(canonical)
}

fn prepare_auth_mount_target(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if !metadata.file_type().is_symlink()
                && metadata.is_file()
                && metadata.permissions().mode() & 0o777 == 0o600
                && metadata.nlink() == 1 =>
        {
            return Ok(());
        }
        Ok(_) => bail!("worker auth mount target has an unsafe identity"),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        Err(_) => {}
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    file.sync_all()?;
    Ok(())
}

fn validate_git_oid(label: &str, value: &str) -> Result<()> {
    crate::worker::validation::validate_git_oid(label, value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    fn git(repository: &Path, arguments: &[&str]) {
        let output = Command::new("/usr/bin/git")
            .args(arguments)
            .current_dir(repository)
            .env_clear()
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("HOME", "/nonexistent")
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(output.status.success(), "git {arguments:?} failed");
    }

    fn seeded_repository(root: &Path) -> PathBuf {
        let repository = root.join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-b", "master"]);
        fs::write(repository.join("seed.txt"), "seed\n").unwrap();
        git(&repository, &["add", "--", "seed.txt"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "seed",
            ],
        );
        fs::canonicalize(repository).unwrap()
    }

    fn locks(root: &Path) -> PathBuf {
        let locks = root.join("locks");
        fs::create_dir(&locks).unwrap();
        fs::set_permissions(&locks, fs::Permissions::from_mode(0o700)).unwrap();
        locks
    }

    #[test]
    fn managed_repository_binds_the_exact_canonical_top_level() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = seeded_repository(&root);
        let managed = ManagedRepository::open(&repository, &locks(&root)).unwrap();
        assert_eq!(managed.repository.root, repository);
        assert_eq!(managed.current_head.len(), 40);
    }

    #[test]
    fn managed_repository_rejects_a_subdirectory_as_a_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let repository = seeded_repository(&root);
        let nested = repository.join("nested");
        fs::create_dir(&nested).unwrap();
        let error = match ManagedRepository::open(&nested, &locks(&root)) {
            Ok(_) => panic!("a subdirectory must not bind as a repository root"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("canonical Git top level"),
            "{error}"
        );
    }

    #[test]
    fn parse_nul_paths_splits_and_rejects_traversal() {
        assert_eq!(
            parse_nul_paths(b"a.txt\0dir/b.txt\0").unwrap(),
            vec!["a.txt".to_string(), "dir/b.txt".to_string()]
        );
        assert!(parse_nul_paths(b"../escape\0").is_err());
        assert!(parse_nul_paths(b"/absolute\0").is_err());
    }
}
