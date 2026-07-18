//! Bounded pending-write queue for the non-blocking PTY master.
//!
//! The old path called a `write_all` that spun on EAGAIN with a 1ms sleep on
//! the main loop thread. With the kernel PTY buffer measured at ~18KiB, a
//! child that stops reading stdin while flooding stdout deadlocks the proxy:
//! the spin can neither drain child output nor process inject/resize/SIGTERM
//! (reproduced: proxy stuck in hrtimer_nanosleep, SIGTERM dead, SIGKILL
//! required).
//!
//! This queue makes master writes event-driven instead: bytes that don't fit
//! are retained (with a partial offset) and flushed when poll() reports the
//! master writable. While anything is pending the caller stops polling outer
//! stdin — natural backpressure that also bounds memory — but keeps draining
//! child output and processing signals.

use std::collections::VecDeque;
use std::os::fd::AsFd;

use anyhow::{Result, bail};
use nix::errno::Errno;
use nix::unistd::write;

/// Cap on buffered non-user (inject) bytes. User stdin is exempt: it is
/// bounded structurally, because stdin stops being polled while anything is
/// pending, so at most one extra stdin chunk can arrive.
const MAX_PENDING_INJECT_BYTES: usize = 256 * 1024;

pub(super) struct PtyWriteQueue {
    pending: VecDeque<Vec<u8>>,
    /// Consumed prefix of `pending.front()`.
    front_offset: usize,
    total_pending: usize,
}

impl PtyWriteQueue {
    pub(super) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            front_offset: 0,
            total_pending: 0,
        }
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Queue user stdin bytes (never dropped) and attempt immediate progress.
    pub(super) fn write_user<F: AsFd>(&mut self, fd: &F, bytes: &[u8]) -> Result<()> {
        self.enqueue(bytes);
        self.flush(fd)
    }

    /// Queue injected bytes and attempt immediate progress. Returns false —
    /// without queueing — when the inject cap is exceeded; the caller logs
    /// and relies on its own retry (delivery re-gates, term reports failure).
    pub(super) fn write_injected<F: AsFd>(&mut self, fd: &F, bytes: &[u8]) -> Result<bool> {
        if self.total_pending.saturating_add(bytes.len()) > MAX_PENDING_INJECT_BYTES {
            return Ok(false);
        }
        self.enqueue(bytes);
        self.flush(fd)?;
        Ok(true)
    }

    /// Drive pending bytes into the master until it stops accepting.
    /// EAGAIN returns cleanly (poll will report POLLOUT); other errors
    /// propagate exactly like the old direct write path.
    pub(super) fn flush<F: AsFd>(&mut self, fd: &F) -> Result<()> {
        while let Some(front) = self.pending.front() {
            let chunk = &front[self.front_offset..];
            match write(fd.as_fd(), chunk) {
                Ok(n) => {
                    self.front_offset += n;
                    self.total_pending -= n;
                    if self.front_offset >= front.len() {
                        self.pending.pop_front();
                        self.front_offset = 0;
                    }
                }
                Err(Errno::EINTR) => continue,
                Err(Errno::EAGAIN) => return Ok(()),
                Err(e) => bail!("write failed: {}", e),
            }
        }
        Ok(())
    }

    fn enqueue(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.total_pending += bytes.len();
        self.pending.push_back(bytes.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use std::io::Read;
    use std::os::fd::OwnedFd;

    /// Non-blocking pipe: the write end backpressures once the kernel buffer
    /// fills, which is exactly the PTY master EAGAIN behavior.
    fn nonblocking_pipe() -> (OwnedFd, OwnedFd) {
        let (read, write) = nix::unistd::pipe().unwrap();
        fcntl(&write, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).unwrap();
        (read, write)
    }

    fn drain(read: &OwnedFd) -> Vec<u8> {
        let mut file = std::fs::File::from(read.try_clone().unwrap());
        fcntl(read, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).unwrap();
        let mut out = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        out
    }

    #[test]
    fn immediate_write_leaves_queue_idle() {
        let (read, write) = nonblocking_pipe();
        let mut q = PtyWriteQueue::new();
        q.write_user(&write, b"hello").unwrap();
        assert!(!q.has_pending());
        assert_eq!(drain(&read), b"hello");
    }

    // The deadlock regression: a full master must NOT spin — the overflow is
    // retained with a partial offset and the call returns to the event loop.
    #[test]
    fn full_pipe_retains_overflow_without_blocking() {
        let (read, write) = nonblocking_pipe();
        let mut q = PtyWriteQueue::new();
        let payload = vec![b'x'; 256 * 1024];

        let start = std::time::Instant::now();
        q.write_user(&write, &payload).unwrap();
        assert!(
            start.elapsed().as_millis() < 500,
            "write must return to the event loop instead of spinning"
        );
        assert!(
            q.has_pending(),
            "overflow beyond the pipe buffer is retained"
        );

        // Reader drains → flush completes the remainder, in order, byte-exact.
        let mut received = drain(&read);
        while q.has_pending() {
            q.flush(&write).unwrap();
            received.extend_from_slice(&drain(&read));
        }
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
    }

    #[test]
    fn queued_chunks_flush_in_fifo_order() {
        let (read, write) = nonblocking_pipe();
        let mut q = PtyWriteQueue::new();
        let filler = vec![b'a'; 128 * 1024];
        q.write_user(&write, &filler).unwrap();
        q.write_user(&write, b"SECOND").unwrap();
        q.write_user(&write, b"THIRD").unwrap();

        let mut received = drain(&read);
        while q.has_pending() {
            q.flush(&write).unwrap();
            received.extend_from_slice(&drain(&read));
        }
        let tail = &received[received.len() - 11..];
        assert_eq!(tail, b"SECONDTHIRD");
    }

    #[test]
    fn inject_cap_refuses_without_queueing_but_user_bytes_always_queue() {
        let (_read, write) = nonblocking_pipe();
        let mut q = PtyWriteQueue::new();
        // Fill until PENDING (not written) exceeds the inject cap — the pipe
        // buffer itself absorbs the first ~64KiB, so write twice the cap.
        q.write_user(&write, &vec![b'u'; 2 * MAX_PENDING_INJECT_BYTES])
            .unwrap();
        let before = q.total_pending;
        assert!(before > MAX_PENDING_INJECT_BYTES);

        assert!(!q.write_injected(&write, b"inject").unwrap());
        assert_eq!(q.total_pending, before, "refused inject must not queue");

        q.write_user(&write, b"user").unwrap();
        assert_eq!(q.total_pending, before + 4);
    }

    #[test]
    fn write_error_propagates() {
        let (read, write) = nonblocking_pipe();
        drop(read); // EPIPE on next write
        let mut q = PtyWriteQueue::new();
        assert!(q.write_user(&write, b"x").is_err());
    }
}
