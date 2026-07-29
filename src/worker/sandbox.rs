//! Shared empty-root bubblewrap manifest for exact session worker profiles.

use super::ExecutableIdentity;
use anyhow::{Context, Result, anyhow, bail};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub(crate) const INSIDE_HOME: &str = "/hcom/home";
pub(crate) const INSIDE_NATIVE_CONFIG: &str = "/hcom/native";
pub(crate) const INSIDE_WORKSPACE: &str = "/hcom/workspace";
pub(crate) const INSIDE_ARTIFACTS: &str = "/hcom/artifacts";
pub(crate) const INSIDE_RUNTIME: &str = "/hcom/run";
pub(crate) const INSIDE_TEMP: &str = "/tmp";
pub(crate) const INSIDE_CODEX: &str = "/hcom/bin/codex";
pub(crate) const INSIDE_CLAUDE: &str = "/hcom/bin/claude";
pub(crate) const INSIDE_CARGO_HOME: &str = "/hcom/home/.cargo";
pub(crate) const INSIDE_RUSTUP_HOME: &str = "/hcom/toolchains/rust/rustup";
pub(crate) const INSIDE_PATH: &str = "/hcom/toolchains/rust/bin:/usr/bin:/bin";

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
pub(crate) struct EmptyRootContract {
    usr: ReadOnlyTreeIdentity,
    etc: ReadOnlyTreeIdentity,
    resolver: ReadOnlyTreeIdentity,
    cargo_bin: ReadOnlyTreeIdentity,
    rustup_home: ReadOnlyTreeIdentity,
}

pub(crate) struct EmptyRootMounts<'a> {
    pub(crate) native: &'a ExecutableIdentity,
    pub(crate) inside_native: &'static str,
    pub(crate) isolated_home: &'a Path,
    pub(crate) native_config: &'a Path,
    pub(crate) workspace: &'a Path,
    pub(crate) workspace_writable: bool,
    pub(crate) artifact_dir: &'a Path,
    pub(crate) auth_source: &'a Path,
    pub(crate) auth_target: &'static str,
}

impl EmptyRootContract {
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

    pub(crate) fn outer_argv(&self, mounts: EmptyRootMounts<'_>) -> Result<Vec<String>> {
        mounts.native.revalidate()?;
        for (label, path) in [
            ("isolated HOME", mounts.isolated_home),
            ("isolated native config", mounts.native_config),
            ("worker workspace", mounts.workspace),
            ("worker artifact attempt", mounts.artifact_dir),
            ("native auth source", mounts.auth_source),
        ] {
            if !path.is_absolute() {
                bail!("{label} must be absolute");
            }
        }
        if !mounts.native_config.starts_with(mounts.isolated_home)
            || mounts.native_config == mounts.isolated_home
        {
            bail!("isolated native config must be a strict child of isolated HOME");
        }
        if !matches!(mounts.inside_native, INSIDE_CODEX | INSIDE_CLAUDE) {
            bail!("empty-root native target is not an enabled exact adapter");
        }
        if !matches!(
            mounts.auth_target,
            "/hcom/native/auth.json" | "/hcom/native/.credentials.json"
        ) {
            bail!("empty-root auth target is outside the closed manifest");
        }
        let text = |label: &str, path: &Path| -> Result<String> {
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("{label} path is not valid UTF-8"))
        };
        let workspace_bind = if mounts.workspace_writable {
            "--bind"
        } else {
            "--ro-bind"
        };
        let mut argv = self.base_argv()?;
        argv.extend([
            "--bind".into(),
            text("isolated HOME", mounts.isolated_home)?,
            INSIDE_HOME.into(),
            "--bind".into(),
            text("isolated native config", mounts.native_config)?,
            INSIDE_NATIVE_CONFIG.into(),
            "--bind".into(),
            text("worker artifact attempt", mounts.artifact_dir)?,
            INSIDE_ARTIFACTS.into(),
            workspace_bind.into(),
            text("worker workspace", mounts.workspace)?,
            INSIDE_WORKSPACE.into(),
            "--ro-bind".into(),
            text("native auth source", mounts.auth_source)?,
            mounts.auth_target.into(),
            "--ro-bind".into(),
            text("native executable", &mounts.native.canonical_path)?,
            mounts.inside_native.into(),
            "--chdir".into(),
            INSIDE_WORKSPACE.into(),
        ]);
        Ok(argv)
    }

    pub(crate) fn base_argv(&self) -> Result<Vec<String>> {
        self.revalidate()?;
        let text = |label: &str, path: &Path| -> Result<String> {
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("{label} path is not valid UTF-8"))
        };
        Ok(vec![
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
            "--dir".into(),
            "/hcom".into(),
            "--dir".into(),
            INSIDE_RUNTIME.into(),
            "--tmpfs".into(),
            INSIDE_RUNTIME.into(),
            "--dir".into(),
            "/hcom/bin".into(),
            "--dir".into(),
            INSIDE_HOME.into(),
            "--dir".into(),
            INSIDE_NATIVE_CONFIG.into(),
            "--dir".into(),
            INSIDE_WORKSPACE.into(),
            "--dir".into(),
            INSIDE_ARTIFACTS.into(),
            "--dir".into(),
            "/hcom/toolchains".into(),
            "--dir".into(),
            "/hcom/toolchains/rust".into(),
            "--dir".into(),
            "/hcom/toolchains/rust/bin".into(),
            "--dir".into(),
            INSIDE_RUSTUP_HOME.into(),
            "--ro-bind".into(),
            text("Rust cargo-bin lease", &self.cargo_bin.path)?,
            "/hcom/toolchains/rust/bin".into(),
            "--ro-bind".into(),
            text("Rust rustup lease", &self.rustup_home.path)?,
            INSIDE_RUSTUP_HOME.into(),
        ])
    }
}
