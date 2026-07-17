//! Trailing-edge-preserving debounce for terminal resize signals.
//!
//! The old leading-edge-only debounce dropped the last SIGWINCH of a window
//! drag whenever it fell inside the debounce window, leaving the child at a
//! stale size until the next unrelated resize. This debouncer keeps only
//! timing state (never a size): rapid signals coalesce into one pending
//! trailing apply, and the caller re-queries the real terminal size at apply
//! time.

use std::time::{Duration, Instant};

/// Outcome of a resize signal hitting the debouncer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResizeAction {
    /// Apply the resize immediately (leading edge).
    ApplyNow,
    /// Coalesced into a pending trailing apply due at the deadline.
    Scheduled(Instant),
}

pub(crate) struct ResizeDebouncer {
    debounce: Duration,
    last_applied: Option<Instant>,
    pending_deadline: Option<Instant>,
}

impl ResizeDebouncer {
    pub(crate) fn new(debounce: Duration) -> Self {
        Self {
            debounce,
            last_applied: None,
            pending_deadline: None,
        }
    }

    /// A resize signal arrived at `now`.
    pub(crate) fn on_signal(&mut self, now: Instant) -> ResizeAction {
        if let Some(last) = self.last_applied
            && now.duration_since(last) < self.debounce
        {
            let deadline = last + self.debounce;
            self.pending_deadline = Some(deadline);
            return ResizeAction::Scheduled(deadline);
        }
        self.last_applied = Some(now);
        self.pending_deadline = None;
        ResizeAction::ApplyNow
    }

    /// Deadline of the pending trailing apply, if any.
    pub(crate) fn pending_deadline(&self) -> Option<Instant> {
        self.pending_deadline
    }

    /// Consume a due trailing apply. Returns true when the caller must apply
    /// the latest terminal size now.
    pub(crate) fn take_due(&mut self, now: Instant) -> bool {
        match self.pending_deadline {
            Some(deadline) if now >= deadline => {
                self.pending_deadline = None;
                self.last_applied = Some(now);
                true
            }
            _ => false,
        }
    }
}

/// Cap a poll timeout (ms) so the main loop wakes in time for a pending
/// trailing resize apply instead of sleeping out its full base timeout.
pub(crate) fn poll_timeout_capped(base_ms: u16, pending: Option<Instant>, now: Instant) -> u16 {
    let Some(deadline) = pending else {
        return base_ms;
    };
    let remaining = deadline.saturating_duration_since(now);
    // Round up so the wakeup lands at/after the deadline; an already-due
    // deadline is consumed by take_due at the next loop top.
    let remaining_ms = remaining
        .as_millis()
        .saturating_add(1)
        .min(u128::from(u16::MAX)) as u16;
    base_ms.min(remaining_ms.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: Duration = Duration::from_millis(50);

    fn t(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    #[test]
    fn leading_edge_applies_immediately() {
        let base = Instant::now();
        let mut d = ResizeDebouncer::new(D);
        assert_eq!(d.on_signal(base), ResizeAction::ApplyNow);
        assert_eq!(d.pending_deadline(), None);
    }

    #[test]
    fn rapid_signals_coalesce_into_single_trailing_deadline() {
        let base = Instant::now();
        let mut d = ResizeDebouncer::new(D);
        assert_eq!(d.on_signal(base), ResizeAction::ApplyNow);
        assert_eq!(
            d.on_signal(t(base, 10)),
            ResizeAction::Scheduled(t(base, 50))
        );
        assert_eq!(
            d.on_signal(t(base, 20)),
            ResizeAction::Scheduled(t(base, 50))
        );
        assert!(!d.take_due(t(base, 49)));
        assert_eq!(d.pending_deadline(), Some(t(base, 50)));
    }

    #[test]
    fn trailing_apply_fires_at_deadline_without_new_signals() {
        // The regression this module exists to fix: the LAST resize of a drag
        // must be applied even though no further signal ever arrives.
        let base = Instant::now();
        let mut d = ResizeDebouncer::new(D);
        d.on_signal(base);
        d.on_signal(t(base, 10));
        assert!(d.take_due(t(base, 50)));
        assert_eq!(d.pending_deadline(), None);
        assert!(!d.take_due(t(base, 51)), "trailing apply is consumed once");
    }

    #[test]
    fn trailing_apply_restarts_debounce_window() {
        let base = Instant::now();
        let mut d = ResizeDebouncer::new(D);
        d.on_signal(base);
        d.on_signal(t(base, 10));
        assert!(d.take_due(t(base, 50)));
        // Still dragging: a signal inside the new window schedules again.
        assert_eq!(
            d.on_signal(t(base, 60)),
            ResizeAction::Scheduled(t(base, 100))
        );
        assert!(!d.take_due(t(base, 99)));
        assert!(d.take_due(t(base, 100)));
    }

    #[test]
    fn quiet_period_signal_is_leading_edge_again() {
        let base = Instant::now();
        let mut d = ResizeDebouncer::new(D);
        d.on_signal(base);
        assert_eq!(d.on_signal(t(base, 200)), ResizeAction::ApplyNow);
    }

    #[test]
    fn poll_timeout_capped_to_pending_deadline() {
        let base = Instant::now();
        assert_eq!(poll_timeout_capped(10_000, None, base), 10_000);
        assert_eq!(poll_timeout_capped(10_000, Some(t(base, 40)), base), 41);
        assert_eq!(
            poll_timeout_capped(10_000, Some(t(base, 40)), t(base, 39)),
            2
        );
        assert_eq!(
            poll_timeout_capped(10_000, Some(t(base, 40)), t(base, 40)),
            1
        );
        assert_eq!(poll_timeout_capped(5, Some(t(base, 40)), base), 5);
    }
}
