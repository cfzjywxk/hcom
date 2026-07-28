use anyhow::{Context, Result, bail};
use std::fs;
use std::os::fd::AsRawFd;
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
