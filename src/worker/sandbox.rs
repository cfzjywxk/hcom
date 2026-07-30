//! Shared path-preserving bubblewrap manifest for exact session profiles.

use anyhow::{Context, Result, anyhow, bail};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

const SYSTEM_USR: &str = "/usr";
const SYSTEM_ETC: &str = "/etc";
const SYSTEM_RESOLVER: &str = "/run/systemd/resolve";

#[derive(Clone, PartialEq, Eq)]
struct ReadOnlyTreeIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    modified_seconds: i64,
    modified_nanos: i64,
    changed_seconds: i64,
    changed_nanos: i64,
}

impl ReadOnlyTreeIdentity {
    fn capture(path: &Path, expected_uid: Option<u32>, label: &str) -> Result<Self> {
        let link = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect {label} {}", path.display()))?;
        if link.file_type().is_symlink() || !link.is_dir() {
            bail!("{label} must be a real directory");
        }
        let canonical =
            fs::canonicalize(path).with_context(|| format!("failed to canonicalize {label}"))?;
        if canonical != path {
            bail!("{label} must already use its canonical path");
        }
        let metadata = fs::metadata(path)?;
        if expected_uid.is_some_and(|uid| metadata.uid() != uid) {
            bail!("{label} owner differs from its exact lease");
        }
        if metadata.permissions().mode() & 0o002 != 0 {
            bail!("{label} cannot be writable by other users");
        }
        Ok(Self {
            path: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode() & 0o777,
            modified_seconds: metadata.mtime(),
            modified_nanos: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanos: metadata.ctime_nsec(),
        })
    }

    fn revalidate(&self, expected_uid: Option<u32>, label: &str) -> Result<()> {
        if Self::capture(&self.path, expected_uid, label)? != *self {
            bail!("{label} identity drifted");
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct HostRootContract {
    usr: ReadOnlyTreeIdentity,
    etc: ReadOnlyTreeIdentity,
    resolver: ReadOnlyTreeIdentity,
    cargo_bin: ReadOnlyTreeIdentity,
    rustup_home: ReadOnlyTreeIdentity,
}

pub(crate) struct HostRootMounts<'a> {
    pub(crate) isolated_home: &'a Path,
    pub(crate) native_config: &'a Path,
    pub(crate) launch_cwd: &'a Path,
    pub(crate) artifact_dir: &'a Path,
    pub(crate) auth_source: &'a Path,
    pub(crate) auth_target: &'a Path,
    pub(crate) readable_roots: &'a [&'a Path],
    pub(crate) writable_roots: &'a [&'a Path],
    pub(crate) read_only_files: &'a [&'a Path],
    pub(crate) extra_writable_dirs: &'a [&'a Path],
    pub(crate) host_root_access: HostRootAccess,
    pub(crate) masked_dirs: &'a [&'a Path],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostRootAccess {
    Hidden,
    ReadWrite,
}

impl HostRootContract {
    pub(crate) fn capture(cargo_bin: &Path, rustup_home: &Path) -> Result<Self> {
        // SAFETY: geteuid has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        Ok(Self {
            usr: ReadOnlyTreeIdentity::capture(Path::new(SYSTEM_USR), Some(0), "system /usr")?,
            etc: ReadOnlyTreeIdentity::capture(Path::new(SYSTEM_ETC), Some(0), "system /etc")?,
            resolver: ReadOnlyTreeIdentity::capture(
                Path::new(SYSTEM_RESOLVER),
                None,
                "system resolver directory",
            )?,
            cargo_bin: ReadOnlyTreeIdentity::capture(
                cargo_bin,
                Some(current_uid),
                "Rust cargo-bin lease",
            )?,
            rustup_home: ReadOnlyTreeIdentity::capture(
                rustup_home,
                Some(current_uid),
                "Rust rustup lease",
            )?,
        })
    }

    pub(crate) fn revalidate(&self) -> Result<()> {
        // SAFETY: geteuid has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        self.usr.revalidate(Some(0), "system /usr")?;
        self.etc.revalidate(Some(0), "system /etc")?;
        self.resolver
            .revalidate(None, "system resolver directory")?;
        self.cargo_bin
            .revalidate(Some(current_uid), "Rust cargo-bin lease")?;
        self.rustup_home
            .revalidate(Some(current_uid), "Rust rustup lease")
    }

    /// Build a path-preserving mount namespace. Task workers use the minimal
    /// explicit-root form; the interactive Architect may use a host-root view
    /// with explicit masks. Both retain real paths and never invent a
    /// workspace path.
    pub(crate) fn host_root_argv(&self, mounts: HostRootMounts<'_>) -> Result<Vec<String>> {
        self.revalidate()?;
        for (label, path) in [
            ("isolated HOME", mounts.isolated_home),
            ("isolated native config", mounts.native_config),
            ("launch cwd", mounts.launch_cwd),
            ("artifact attempt", mounts.artifact_dir),
            ("native auth source", mounts.auth_source),
            ("native auth target", mounts.auth_target),
        ] {
            if !path.is_absolute() {
                bail!("{label} must be absolute");
            }
        }
        for (label, paths) in [
            ("readable root", mounts.readable_roots),
            ("writable root", mounts.writable_roots),
            ("read-only file", mounts.read_only_files),
            ("extra writable directory", mounts.extra_writable_dirs),
            ("masked directory", mounts.masked_dirs),
        ] {
            if paths.iter().any(|path| !path.is_absolute()) {
                bail!("{label} must be absolute");
            }
        }
        if !mounts.native_config.starts_with(mounts.isolated_home)
            || mounts.native_config == mounts.isolated_home
        {
            bail!("isolated native config must be a strict child of isolated HOME");
        }
        if !mounts.auth_target.starts_with(mounts.native_config) {
            bail!("native auth target must remain inside isolated native config");
        }
        let text = |label: &str, path: &Path| -> Result<String> {
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("{label} path is not valid UTF-8"))
        };
        let private_writable_dirs: Vec<_> = [
            mounts.isolated_home,
            mounts.native_config,
            mounts.artifact_dir,
        ]
        .into_iter()
        .chain(mounts.extra_writable_dirs.iter().copied())
        .collect();

        if let HostRootAccess::ReadWrite = mounts.host_root_access {
            let mut argv = vec![
                "--die-with-parent".into(),
                "--unshare-pid".into(),
                "--unshare-ipc".into(),
                "--unshare-uts".into(),
                "--bind".into(),
                "/".into(),
                "/".into(),
                "--proc".into(),
                "/proc".into(),
                "--dev".into(),
                "/dev".into(),
                "--tmpfs".into(),
                "/dev/shm".into(),
            ];
            for directory in mounts.masked_dirs {
                argv.extend(["--tmpfs".into(), text("masked directory", directory)?]);
            }
            let mut directory_targets = BTreeSet::new();
            for target in mounts
                .readable_roots
                .iter()
                .copied()
                .chain(mounts.writable_roots.iter().copied())
                .chain(private_writable_dirs.iter().copied())
            {
                collect_directory_chain(target, true, &mut directory_targets)?;
            }
            for target in mounts
                .read_only_files
                .iter()
                .copied()
                .chain([mounts.auth_target])
            {
                collect_directory_chain(target, false, &mut directory_targets)?;
            }
            for directory in directory_targets {
                if mounts
                    .masked_dirs
                    .iter()
                    .any(|masked| directory != **masked && directory.starts_with(masked))
                {
                    argv.extend(["--dir".into(), text("private mount target", &directory)?]);
                }
            }
            for directory in mounts.writable_roots {
                argv.extend([
                    "--bind".into(),
                    text("writable root", directory)?,
                    text("writable root", directory)?,
                ]);
            }
            for directory in &private_writable_dirs {
                argv.extend([
                    "--bind".into(),
                    text("private writable directory", directory)?,
                    text("private writable directory", directory)?,
                ]);
            }
            // Persistent control/config roots must win over a writable project
            // parent (for example project_root == $HOME).
            for directory in mounts.readable_roots {
                argv.extend([
                    "--ro-bind".into(),
                    text("readable root", directory)?,
                    text("readable root", directory)?,
                ]);
            }
            for file in mounts.read_only_files {
                argv.extend([
                    "--ro-bind".into(),
                    text("read-only file", file)?,
                    text("read-only file", file)?,
                ]);
            }
            argv.extend([
                "--ro-bind".into(),
                text("native auth source", mounts.auth_source)?,
                text("native auth target", mounts.auth_target)?,
                "--chdir".into(),
                text("launch cwd", mounts.launch_cwd)?,
            ]);
            return Ok(argv);
        }

        let mut argv = vec![
            "--die-with-parent".into(),
            "--unshare-pid".into(),
            "--unshare-ipc".into(),
            "--unshare-uts".into(),
            "--ro-bind".into(),
            SYSTEM_USR.into(),
            SYSTEM_USR.into(),
            "--ro-bind".into(),
            SYSTEM_ETC.into(),
            SYSTEM_ETC.into(),
            "--dir".into(),
            "/run".into(),
            "--dir".into(),
            "/run/systemd".into(),
            "--ro-bind".into(),
            SYSTEM_RESOLVER.into(),
            SYSTEM_RESOLVER.into(),
            "--symlink".into(),
            "usr/bin".into(),
            "/bin".into(),
            "--symlink".into(),
            "usr/sbin".into(),
            "/sbin".into(),
            "--symlink".into(),
            "usr/lib".into(),
            "/lib".into(),
            "--symlink".into(),
            "usr/lib64".into(),
            "/lib64".into(),
            "--proc".into(),
            "/proc".into(),
            "--dev".into(),
            "/dev".into(),
            "--tmpfs".into(),
            "/dev/shm".into(),
            "--tmpfs".into(),
            "/tmp".into(),
            "--dir".into(),
            "/var".into(),
            "--tmpfs".into(),
            "/var/tmp".into(),
        ];

        let readable_roots = mounts.readable_roots.iter().copied().chain([
            self.cargo_bin.path.as_path(),
            self.rustup_home.path.as_path(),
        ]);
        let writable_dirs = private_writable_dirs.iter().copied();

        let mut directory_targets = BTreeSet::new();
        for target in readable_roots
            .clone()
            .chain(mounts.writable_roots.iter().copied())
            .chain(writable_dirs.clone())
        {
            collect_directory_chain(target, true, &mut directory_targets)?;
        }
        for target in mounts
            .read_only_files
            .iter()
            .copied()
            .chain([mounts.auth_target])
        {
            collect_directory_chain(target, false, &mut directory_targets)?;
        }
        for directory in directory_targets {
            argv.extend(["--dir".into(), text("mount target", &directory)?]);
        }

        for directory in readable_roots {
            argv.extend([
                "--ro-bind".into(),
                text("readable root", directory)?,
                text("readable root", directory)?,
            ]);
        }
        for directory in mounts.writable_roots {
            argv.extend([
                "--bind".into(),
                text("writable root", directory)?,
                text("writable root", directory)?,
            ]);
        }
        for directory in writable_dirs {
            argv.extend([
                "--bind".into(),
                text("private writable directory", directory)?,
                text("private writable directory", directory)?,
            ]);
        }
        for file in mounts.read_only_files {
            argv.extend([
                "--ro-bind".into(),
                text("read-only file", file)?,
                text("read-only file", file)?,
            ]);
        }
        argv.extend([
            "--ro-bind".into(),
            text("native auth source", mounts.auth_source)?,
            text("native auth target", mounts.auth_target)?,
            "--chdir".into(),
            text("launch cwd", mounts.launch_cwd)?,
        ]);
        Ok(argv)
    }
}

fn collect_directory_chain(
    target: &Path,
    include_target: bool,
    directories: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    if !target.is_absolute() {
        bail!("mount target must be absolute");
    }
    let endpoint = if include_target {
        target
    } else {
        target
            .parent()
            .ok_or_else(|| anyhow!("mount file target has no parent"))?
    };
    for ancestor in endpoint.ancestors() {
        if ancestor == Path::new("/")
            || ancestor == Path::new("/tmp")
            || ancestor == Path::new("/var")
            || ancestor.starts_with(SYSTEM_USR)
            || ancestor.starts_with(SYSTEM_ETC)
            || ancestor.starts_with("/run")
        {
            continue;
        }
        directories.insert(ancestor.to_owned());
    }
    Ok(())
}
