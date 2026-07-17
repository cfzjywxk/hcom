//! Self-pipe wakeup for the proxy main loop.
//!
//! Closes the lost-wakeup race between the loop's signal-flag check and
//! `poll()` entering the kernel: a signal that lands in that window only sets
//! its `AtomicBool` back to true and does NOT interrupt the poll that starts
//! afterwards, so the flag could sit unnoticed for the full base timeout
//! (observed: a window-drag's final SIGWINCH applied 10s late). Handlers now
//! also write one byte into a non-blocking pipe whose read end is in the poll
//! set, so a signal in the race window makes poll return immediately.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::{Context, Result};
use nix::fcntl::OFlag;
use nix::unistd::pipe2;

/// Write end of the wake pipe, readable from async signal handlers.
/// -1 until installed. One proxy per `hcom pty` process, so this only
/// transitions -1 → fd once outside tests.
static WAKE_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

/// Called from signal handlers after they set their flag. Async-signal-safe:
/// one Relaxed load plus one write(2) on a non-blocking fd. A full pipe means
/// a wakeup is already pending, so a failed write is deliberately ignored.
pub(super) fn notify_from_handler() {
    let fd = WAKE_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = 1u8;
        unsafe { libc::write(fd, std::ptr::from_ref(&byte).cast(), 1) };
    }
}

pub(super) struct SignalWakePipe {
    read: OwnedFd,
    // Keeps the fd published to the handlers valid for the proxy's lifetime.
    _write: OwnedFd,
}

impl SignalWakePipe {
    /// Create the pipe and publish its write end to the signal handlers.
    pub(super) fn install() -> Result<Self> {
        let (read, write) =
            pipe2(OFlag::O_NONBLOCK | OFlag::O_CLOEXEC).context("signal wake pipe")?;
        WAKE_WRITE_FD.store(write.as_raw_fd(), Ordering::Release);
        Ok(Self {
            read,
            _write: write,
        })
    }

    pub(super) fn read_fd(&self) -> BorrowedFd<'_> {
        self.read.as_fd()
    }

    /// Drain pending wake bytes so a level-triggered poll doesn't spin.
    /// The flags themselves are re-checked at the loop top; losing the exact
    /// byte count is fine, the pipe is purely a wakeup edge.
    pub(super) fn drain(&self) {
        let mut buf = [0u8; 4096];
        loop {
            let n =
                unsafe { libc::read(self.read.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break; // EAGAIN/EINTR (or impossible EOF): nothing left to drain
            }
        }
    }
}

impl Drop for SignalWakePipe {
    fn drop(&mut self) {
        // Unpublish before the fds close so handlers stop writing. A handler
        // that loaded the fd just before this store still writes to a
        // still-open fd; the close happens after.
        WAKE_WRITE_FD.store(-1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use serial_test::serial;
    use std::time::Instant;

    fn poll_read(pipe: &SignalWakePipe, timeout_ms: u16) -> bool {
        let mut fds = [PollFd::new(pipe.read_fd(), PollFlags::POLLIN)];
        let n = poll(&mut fds, PollTimeout::from(timeout_ms)).unwrap();
        n == 1
            && fds[0]
                .revents()
                .is_some_and(|r| r.contains(PollFlags::POLLIN))
    }

    // The regression this module fixes: a signal notification posted BEFORE
    // poll() starts must make poll return immediately instead of sleeping out
    // its full base timeout.
    #[test]
    #[serial]
    fn notify_before_poll_wakes_immediately() {
        let pipe = SignalWakePipe::install().unwrap();
        notify_from_handler();

        let start = Instant::now();
        assert!(poll_read(&pipe, 10_000));
        assert!(
            start.elapsed().as_millis() < 1000,
            "poll must wake immediately, waited {:?}",
            start.elapsed()
        );
    }

    #[test]
    #[serial]
    fn drain_clears_pending_wakeups() {
        let pipe = SignalWakePipe::install().unwrap();
        for _ in 0..5 {
            notify_from_handler();
        }
        assert!(poll_read(&pipe, 0));
        pipe.drain();
        assert!(!poll_read(&pipe, 0), "drained pipe must not report POLLIN");
    }

    #[test]
    #[serial]
    fn notify_after_drop_is_noop() {
        let pipe = SignalWakePipe::install().unwrap();
        drop(pipe);
        // Must neither crash nor write to a recycled fd.
        notify_from_handler();
    }

    #[test]
    #[serial]
    fn notify_without_install_is_noop() {
        // WAKE_WRITE_FD is -1 (fresh or after drop in a prior serial test).
        notify_from_handler();
    }
}
