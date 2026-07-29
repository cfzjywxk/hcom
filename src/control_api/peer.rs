use anyhow::{Context, Result, bail};
use std::fs;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerCredentials {
    pub(crate) pid: u32,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

pub(crate) fn peer_credentials(stream: &UnixStream) -> Result<PeerCredentials> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: credentials points to writable ucred storage, length describes it,
    // and stream owns a live Unix socket file descriptor.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("SO_PEERCRED failed");
    }
    if length as usize != std::mem::size_of::<libc::ucred>() || credentials.pid <= 1 {
        bail!("SO_PEERCRED returned an invalid process identity");
    }
    Ok(PeerCredentials {
        pid: u32::try_from(credentials.pid).context("peer PID is out of range")?,
        uid: credentials.uid,
        gid: credentials.gid,
    })
}

pub fn process_birth_identity(pid: u32) -> Result<String> {
    if pid <= 1 {
        bail!("process PID must be greater than one");
    }
    let stat_path = format!("/proc/{pid}/stat");
    let stat = fs::read_to_string(&stat_path)
        .with_context(|| format!("failed to read process identity for PID {pid}"))?;
    let close_paren = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("malformed /proc stat record"))?;
    let fields: Vec<&str> = stat[close_paren + 1..].split_whitespace().collect();
    // The suffix begins at field 3 (`state`); starttime is field 22.
    let start_time = fields
        .get(19)
        .ok_or_else(|| anyhow::anyhow!("missing process start time"))?;
    if !start_time.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid process start time");
    }
    let boot_id = boot_identity()?;
    Ok(format!("linux-proc:{boot_id}:{start_time}"))
}

pub(crate) fn process_parent_pid(pid: u32) -> Result<u32> {
    let stat = read_proc_stat(pid)?;
    let close_paren = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("malformed /proc stat record"))?;
    let fields: Vec<&str> = stat[close_paren + 1..].split_whitespace().collect();
    // The suffix begins at field 3 (`state`); ppid is field 4.
    let parent = fields
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing parent process id"))?;
    let parent = parent.parse::<u32>().context("invalid parent process id")?;
    Ok(parent)
}

pub(crate) fn process_has_ancestor(pid: u32, roots: &[(u32, String)]) -> Result<bool> {
    let mut current = pid;
    let mut visited = std::collections::BTreeSet::new();
    for _ in 0..256 {
        if current <= 1 || !visited.insert(current) {
            return Ok(false);
        }
        let birth = process_birth_identity(current)?;
        if roots
            .iter()
            .any(|(root_pid, root_birth)| *root_pid == current && *root_birth == birth)
        {
            return Ok(true);
        }
        current = process_parent_pid(current)?;
    }
    bail!("process ancestry exceeds its bounded depth")
}

pub(crate) fn process_executable_path(pid: u32) -> Result<std::path::PathBuf> {
    let before = process_birth_identity(pid)?;
    let link = fs::read_link(format!("/proc/{pid}/exe"))
        .with_context(|| format!("failed to resolve executable for PID {pid}"))?;
    let canonical = fs::canonicalize(&link)
        .with_context(|| format!("failed to canonicalize executable for PID {pid}"))?;
    let after = process_birth_identity(pid)?;
    if before != after {
        bail!("process identity changed while resolving its executable");
    }
    Ok(canonical)
}

pub(crate) fn process_is_live_identity(pid: u32, expected_birth: &str) -> Result<bool> {
    if process_birth_identity(pid)? != expected_birth {
        return Ok(false);
    }
    let stat = read_proc_stat(pid)?;
    let close_paren = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("malformed /proc stat record"))?;
    let state = stat[close_paren + 1..]
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing process state"))?;
    Ok(state != "Z" && state != "X")
}

pub(crate) fn process_owns_foreground_tty(pid: u32, expected_birth: &str) -> Result<bool> {
    if process_birth_identity(pid)? != expected_birth {
        return Ok(false);
    }
    let stdin_path = format!("/proc/{pid}/fd/0");
    let stdout_path = format!("/proc/{pid}/fd/1");
    let stderr_path = format!("/proc/{pid}/fd/2");
    let stdin_metadata = fs::metadata(&stdin_path)?;
    let stdout_metadata = fs::metadata(&stdout_path)?;
    let stderr_metadata = fs::metadata(&stderr_path)?;
    if !stdin_metadata.file_type().is_char_device()
        || !stdout_metadata.file_type().is_char_device()
        || !stderr_metadata.file_type().is_char_device()
        || stdin_metadata.rdev() != stdout_metadata.rdev()
        || stdin_metadata.rdev() != stderr_metadata.rdev()
    {
        return Ok(false);
    }

    let stat = read_proc_stat(pid)?;
    let close_paren = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("malformed /proc stat record"))?;
    let fields: Vec<&str> = stat[close_paren + 1..].split_whitespace().collect();
    // The suffix begins at field 3 (`state`): pgrp/session/tty_nr/tpgid are
    // fields 5/6/7/8 and therefore indexes 2/3/4/5 here.
    let process_group = parse_proc_i64(&fields, 2, "process group")?;
    let session = parse_proc_i64(&fields, 3, "session")?;
    let tty_device = parse_proc_i64(&fields, 4, "controlling terminal")?;
    let foreground_group = parse_proc_i64(&fields, 5, "foreground process group")?;
    if process_group <= 1
        || session <= 1
        || tty_device <= 0
        || foreground_group != process_group
        || u64::try_from(tty_device).ok() != Some(stdin_metadata.rdev())
    {
        return Ok(false);
    }
    // SAFETY: getpgid/getsid only inspect the positive caller PID.
    let live_group = unsafe { libc::getpgid(pid as libc::pid_t) };
    let live_session = unsafe { libc::getsid(pid as libc::pid_t) };
    if live_group != process_group as libc::pid_t || live_session != session as libc::pid_t {
        return Ok(false);
    }
    if process_birth_identity(pid)? != expected_birth {
        return Ok(false);
    }
    let final_stdin = fs::metadata(stdin_path)?;
    let final_stdout = fs::metadata(stdout_path)?;
    let final_stderr = fs::metadata(stderr_path)?;
    Ok(final_stdin.rdev() == stdin_metadata.rdev()
        && final_stdout.rdev() == stdin_metadata.rdev()
        && final_stderr.rdev() == stdin_metadata.rdev())
}

fn parse_proc_i64(fields: &[&str], index: usize, label: &str) -> Result<i64> {
    fields
        .get(index)
        .ok_or_else(|| anyhow::anyhow!("missing process {label}"))?
        .parse::<i64>()
        .with_context(|| format!("invalid process {label}"))
}

fn read_proc_stat(pid: u32) -> Result<String> {
    if pid <= 1 {
        bail!("process PID must be greater than one");
    }
    let stat_path = format!("/proc/{pid}/stat");
    fs::read_to_string(&stat_path)
        .with_context(|| format!("failed to read process identity for PID {pid}"))
}

pub(crate) fn boot_identity() -> Result<String> {
    let boot_id =
        fs::read_to_string("/proc/sys/kernel/random/boot_id").context("failed to read boot ID")?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty()
        || boot_id.len() > 64
        || !boot_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        bail!("invalid boot ID");
    }
    Ok(boot_id.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    #[test]
    fn peer_credentials_and_birth_match_the_live_client() {
        let (left, right) = UnixStream::pair().unwrap();
        let credentials = peer_credentials(&left).unwrap();
        assert_eq!(credentials.pid, std::process::id());
        // SAFETY: geteuid/getegid have no preconditions.
        assert_eq!(credentials.uid, unsafe { libc::geteuid() });
        assert_eq!(credentials.gid, unsafe { libc::getegid() });
        assert_eq!(
            process_birth_identity(credentials.pid).unwrap(),
            process_birth_identity(std::process::id()).unwrap()
        );
        drop(right);
    }
}
