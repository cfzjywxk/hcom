//! Chain-only Linux PTY/process primitives.
//!
//! These helpers are intentionally smaller than the ordinary interactive
//! `Proxy` lifecycle. They never spawn a command, never press Enter, and never
//! escalate a timeout. The caller must durably authorize a signal before
//! calling `send_process_group_signal`, and must record every independent
//! cleanup result before launching a successor.

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;

use crate::chain_supervisor::{ChainSignal, SignalSendResult, linux_process_birth_identity};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedChildExit {
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReapedChild {
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

/// Arm a freshly forked wrapper or inner child with SIGHUP on parent death.
/// Applied at both edges, an abrupt foreground-supervisor death closes the
/// wrapper's PTY ownership and also reaches the inner process directly. The
/// parent check closes the standard prctl race where the parent exits
/// immediately before the death signal is installed.
///
/// Call this in the single-threaded wrapper before it forks the inner child.
pub fn arm_parent_death_hangup(expected_parent_pid: i32) -> io::Result<()> {
    if expected_parent_pid <= 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected supervisor PID must be greater than one",
        ));
    }
    // SAFETY: this is called in the freshly forked wrapper. Resetting SIGHUP
    // avoids inheriting the foreground supervisor's self-pipe handler.
    if unsafe { libc::signal(libc::SIGHUP, libc::SIG_DFL) } == libc::SIG_ERR {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: prctl receives scalar arguments; SIGHUP is a valid death signal.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGHUP, 0, 0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: getppid has no preconditions.
    if unsafe { libc::getppid() } != expected_parent_pid {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "foreground supervisor exited before wrapper arming completed",
        ));
    }
    Ok(())
}

/// Send one of the three chain-supported signals to an exact, live inner
/// process-group leader. The pidfd supplies a race-resistant liveness check;
/// the chain adapter's separate no-reap-before-finish contract keeps an exit
/// that races this check from releasing the numeric PGID. `ChainSignal` has no
/// SIGKILL variant.
pub fn send_process_group_signal(
    child_pid: i32,
    child_pgid: i32,
    expected_birth_identity: &str,
    signal: ChainSignal,
) -> SignalSendResult {
    send_process_group_signal_impl(
        child_pid,
        child_pgid,
        expected_birth_identity,
        signal,
        || {},
    )
}

fn send_process_group_signal_impl<F>(
    child_pid: i32,
    child_pgid: i32,
    expected_birth_identity: &str,
    signal: ChainSignal,
    before_group_kill: F,
) -> SignalSendResult
where
    F: FnOnce(),
{
    if child_pid <= 1
        || child_pgid != child_pid
        || expected_birth_identity.is_empty()
        || expected_birth_identity.len() > 256
    {
        return SignalSendResult::Error;
    }
    // SAFETY: pidfd_open takes scalar arguments and returns a new descriptor.
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, child_pid, 0) };
    if pidfd < 0 {
        return match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => SignalSendResult::NotFound,
            Some(libc::EPERM) | Some(libc::EACCES) => SignalSendResult::PermissionDenied,
            _ => SignalSendResult::Error,
        };
    }
    let exact_birth = linux_process_birth_identity(child_pid)
        .is_ok_and(|actual| actual == expected_birth_identity);
    if !exact_birth {
        // SAFETY: pidfd was opened above and is owned by this function.
        unsafe {
            libc::close(pidfd as RawFd);
        }
        return SignalSendResult::NotFound;
    }
    let mut pollfd = libc::pollfd {
        fd: pidfd as RawFd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pollfd points to one initialized descriptor entry.
    let liveness = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if liveness != 0 {
        // SAFETY: pidfd was opened above and is owned by this function.
        unsafe {
            libc::close(pidfd as RawFd);
        }
        return if liveness > 0 {
            SignalSendResult::NotFound
        } else {
            SignalSendResult::Error
        };
    }
    let raw = match signal {
        ChainSignal::Interrupt => libc::SIGINT,
        ChainSignal::Terminate => libc::SIGTERM,
        ChainSignal::Hangup => libc::SIGHUP,
    };
    // Tests stop here to force the exact liveness-check -> child-exit race.
    // Production supplies a no-op. The adapter still owns the unreaped child,
    // so its numeric process group cannot be reused across this boundary.
    before_group_kill();
    // SAFETY: a negative, birth-validated group-leader PID selects the exact
    // inner process group. The adapter keeps any racing exit unreaped until the
    // supervisor's explicit finish step, so this PGID cannot be recycled here.
    let result = unsafe { libc::kill(-child_pgid, raw) };
    // SAFETY: pidfd is caller-owned and no longer needed after the syscall.
    unsafe {
        libc::close(pidfd as RawFd);
    }
    if result == 0 {
        SignalSendResult::Sent
    } else {
        match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => SignalSendResult::NotFound,
            Some(libc::EPERM) => SignalSendResult::PermissionDenied,
            _ => SignalSendResult::Error,
        }
    }
}

/// Observe a direct child exit without consuming it. A later exact
/// `reap_direct_child` is mandatory before successor launch.
pub fn observe_direct_child(pid: i32) -> io::Result<Option<ObservedChildExit>> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "child PID must be positive",
        ));
    }
    let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: info is writable and WNOWAIT preserves the waitpid obligation.
    if unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful waitid initialized siginfo_t.
    let info = unsafe { info.assume_init() };
    // SAFETY: successful Linux waitid initialized the union payload.
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    // SAFETY: successful Linux waitid initialized the union payload.
    let status = unsafe { info.si_status() };
    if info.si_code == libc::CLD_EXITED {
        Ok(Some(ObservedChildExit {
            exit_code: Some(status),
            exit_signal: None,
        }))
    } else {
        Ok(Some(ObservedChildExit {
            exit_code: None,
            exit_signal: Some(status),
        }))
    }
}

/// Reap one exact direct child. There is no timeout or escalation branch.
pub fn reap_direct_child(pid: i32) -> io::Result<ReapedChild> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "child PID must be positive",
        ));
    }
    let mut status = 0;
    // SAFETY: status is writable and pid is the caller's exact direct child.
    let reaped = unsafe { libc::waitpid(pid, &mut status, 0) };
    if reaped == -1 {
        return Err(io::Error::last_os_error());
    }
    if reaped != pid {
        return Err(io::Error::other("waitpid returned a different child"));
    }
    if libc::WIFEXITED(status) {
        Ok(ReapedChild {
            exit_code: Some(libc::WEXITSTATUS(status)),
            exit_signal: None,
        })
    } else if libc::WIFSIGNALED(status) {
        Ok(ReapedChild {
            exit_code: None,
            exit_signal: Some(libc::WTERMSIG(status)),
        })
    } else {
        Err(io::Error::other(
            "waitpid returned a non-terminal child status",
        ))
    }
}

/// Re-read the outer size at the apply point and apply it to the current inner
/// PTY. No cached fd/PID from a retired generation is accepted by this helper.
pub fn copy_winsize(outer_fd: RawFd, inner_master_fd: RawFd) -> io::Result<libc::winsize> {
    if outer_fd < 0 || inner_master_fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TTY descriptors must be non-negative",
        ));
    }
    let mut size = MaybeUninit::<libc::winsize>::zeroed();
    // SAFETY: size is writable and request is read-only on the outer TTY.
    if unsafe { libc::ioctl(outer_fd, libc::TIOCGWINSZ, size.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful ioctl initialized size.
    let size = unsafe { size.assume_init() };
    // SAFETY: size is initialized and request only updates the exact inner PTY.
    if unsafe { libc::ioctl(inner_master_fd, libc::TIOCSWINSZ, &size) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn invalid_process_group_fails_without_signaling() {
        assert_eq!(
            send_process_group_signal(0, 0, "", ChainSignal::Terminate),
            SignalSendResult::Error
        );
        assert!(arm_parent_death_hangup(1).is_err());
    }

    #[test]
    fn stale_child_birth_refuses_without_signaling_reused_group() {
        let mut ready = [-1; 2];
        // SAFETY: ready has two writable descriptor slots.
        assert_eq!(unsafe { libc::pipe(ready.as_mut_ptr()) }, 0);
        // SAFETY: the child only uses async-signal-safe libc calls.
        let child = unsafe { libc::fork() };
        assert!(child >= 0);
        if child == 0 {
            unsafe {
                libc::close(ready[0]);
                if libc::setsid() == -1 {
                    libc::_exit(90);
                }
                let marker = 1u8;
                libc::write(ready[1], (&marker as *const u8).cast(), 1);
                libc::pause();
                libc::_exit(0);
            }
        }
        // SAFETY: parent owns these exact descriptors.
        unsafe {
            libc::close(ready[1]);
            let mut marker = 0u8;
            assert_eq!(libc::read(ready[0], (&mut marker as *mut u8).cast(), 1), 1);
            libc::close(ready[0]);
        }
        assert_eq!(
            send_process_group_signal(
                child,
                child,
                "linux-v1:999999:1:stale",
                ChainSignal::Terminate,
            ),
            SignalSendResult::NotFound
        );
        // SAFETY: signal 0 is read-only; the exact fixture child remains live.
        assert_eq!(unsafe { libc::kill(child, 0) }, 0);
        // SAFETY: exact fixture child, then exact waitpid cleanup.
        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status = 0;
            assert_eq!(libc::waitpid(child, &mut status, 0), child);
        }
    }

    #[test]
    fn wait_helpers_reject_unresolved_pid() {
        assert!(observe_direct_child(0).is_err());
        assert!(reap_direct_child(0).is_err());
    }

    #[test]
    fn exit_racing_verified_signal_remains_unreaped_until_adapter_finish() {
        let mut ready = [-1; 2];
        // SAFETY: ready has two writable descriptor slots.
        assert_eq!(
            unsafe { libc::pipe2(ready.as_mut_ptr(), libc::O_CLOEXEC) },
            0
        );
        // SAFETY: the fixture child uses only async-signal-safe operations.
        let child = unsafe { libc::fork() };
        assert!(child >= 0);
        if child == 0 {
            unsafe {
                libc::close(ready[0]);
                if libc::setsid() == -1 {
                    libc::_exit(90);
                }
                let marker = 1u8;
                libc::write(ready[1], (&marker as *const u8).cast(), 1);
                libc::pause();
                libc::_exit(0);
            }
        }
        unsafe {
            libc::close(ready[1]);
            let mut marker = 0u8;
            assert_eq!(libc::read(ready[0], (&mut marker as *mut u8).cast(), 1), 1);
            libc::close(ready[0]);
        }
        let birth = linux_process_birth_identity(child).unwrap();
        let result =
            send_process_group_signal_impl(child, child, &birth, ChainSignal::Interrupt, || {
                // Force exit after pidfd/birth/liveness validation but before
                // kill(-pgid). A pidfd wait observes terminal state without
                // consuming the parent's waitpid obligation.
                assert_eq!(unsafe { libc::kill(child, libc::SIGTERM) }, 0);
                let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, child, 0) };
                assert!(pidfd >= 0);
                let mut descriptor = libc::pollfd {
                    fd: pidfd as RawFd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                assert_eq!(
                    unsafe {
                        libc::poll(
                            &mut descriptor,
                            1,
                            Duration::from_secs(5).as_millis() as i32,
                        )
                    },
                    1
                );
                unsafe {
                    libc::close(pidfd as RawFd);
                }
            });
        assert!(matches!(
            result,
            SignalSendResult::Sent | SignalSendResult::NotFound
        ));

        // This exact waitpid must still consume the child. ECHILD would prove
        // that the signal helper (or another adapter path) reaped too early.
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(child, &mut status, libc::WNOHANG) },
            child
        );
        assert!(libc::WIFSIGNALED(status));
        assert_eq!(libc::WTERMSIG(status), libc::SIGTERM);
    }
}
