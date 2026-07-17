//! Guarded input-ownership contract for the PTY proxy.
//!
//! Core invariant: **hcom must never submit (press Enter) into a prompt the
//! human may own.** Every decision here runs at the APPLY point on the proxy
//! main loop, which serializes real stdin traffic with guarded operations —
//! a human byte in the same poll batch is forwarded (and bumps the user
//! generation) before any guarded op is processed, so an approved decision
//! cannot be stale by construction.
//!
//! The lease a client holds is `{user_gen, input_epoch}` read from one
//! coherent screen snapshot:
//! - `user_gen` counts real user stdin chunks (focus-event-only bytes
//!   excluded). Any change means the human touched the session.
//! - `input_epoch` counts every non-user write into the PTY (raw injects and
//!   guarded injects alike). A guarded inject returns the new epoch as the
//!   token for the follow-up submit; any other writer in between invalidates
//!   the token.

use serde::Deserialize;

/// What the prompt parser can currently say about the input box.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InputObservation {
    /// This tool has no prompt parser at all — emptiness/ownership can never
    /// be proven. Distinct from `Unavailable` so fail-closed logic does not
    /// permanently lock out parserless tools from explicit `--force` flows.
    Unsupported,
    /// A parser exists but cannot find the prompt in the current frame.
    Unavailable,
    /// The parsed prompt content.
    Text(String),
}

/// Live proxy-side state a guarded operation is checked against.
#[derive(Debug, Clone)]
pub(crate) struct GuardSnapshot {
    pub user_gen: u64,
    pub input_epoch: u64,
    pub observation: InputObservation,
    pub approval_waiting: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InjectIfRequest {
    pub user_gen: u64,
    pub input_epoch: u64,
    #[serde(default)]
    pub require_empty: bool,
    pub payload: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmitIfRequest {
    pub user_gen: u64,
    /// Token from a preceding guarded inject. Optional: callers that did not
    /// inject through the guarded path (e.g. delivery's render-verified text)
    /// pass None and rely on user_gen + exact-text match.
    #[serde(default)]
    pub input_epoch: Option<u64>,
    pub expected_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefuseReason {
    Unsupported,
    Unavailable,
    StaleUserGen,
    StaleEpoch,
    PromptNotEmpty,
    Mismatch,
    ApprovalWaiting,
    EmptyExpected,
}

impl RefuseReason {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            RefuseReason::Unsupported => "unsupported",
            RefuseReason::Unavailable => "unavailable",
            RefuseReason::StaleUserGen => "stale_user_gen",
            RefuseReason::StaleEpoch => "stale_epoch",
            RefuseReason::PromptNotEmpty => "prompt_not_empty",
            RefuseReason::Mismatch => "mismatch",
            RefuseReason::ApprovalWaiting => "approval_waiting",
            RefuseReason::EmptyExpected => "empty_expected",
        }
    }
}

pub(crate) type Decision = Result<(), RefuseReason>;

/// Apply-point check for a guarded text inject. Refusal means zero bytes are
/// written.
pub(crate) fn check_inject(req: &InjectIfRequest, snap: &GuardSnapshot) -> Decision {
    let text = match &snap.observation {
        InputObservation::Unsupported => return Err(RefuseReason::Unsupported),
        InputObservation::Unavailable => return Err(RefuseReason::Unavailable),
        InputObservation::Text(text) => text,
    };
    if snap.user_gen != req.user_gen {
        return Err(RefuseReason::StaleUserGen);
    }
    if snap.input_epoch != req.input_epoch {
        return Err(RefuseReason::StaleEpoch);
    }
    if req.require_empty && !text.is_empty() {
        return Err(RefuseReason::PromptNotEmpty);
    }
    Ok(())
}

/// Apply-point check for a guarded submit (Enter). Refusal means the CR is
/// never written — this is the line that keeps hcom from pressing Enter on
/// the human's behalf.
pub(crate) fn check_submit(req: &SubmitIfRequest, snap: &GuardSnapshot) -> Decision {
    if req.expected_text.is_empty() {
        // Enter-only cannot prove ownership of anything; explicit --force on
        // the client is the only path to a bare CR.
        return Err(RefuseReason::EmptyExpected);
    }
    let text = match &snap.observation {
        InputObservation::Unsupported => return Err(RefuseReason::Unsupported),
        InputObservation::Unavailable => return Err(RefuseReason::Unavailable),
        InputObservation::Text(text) => text,
    };
    if snap.user_gen != req.user_gen {
        return Err(RefuseReason::StaleUserGen);
    }
    if let Some(epoch) = req.input_epoch
        && snap.input_epoch != epoch
    {
        return Err(RefuseReason::StaleEpoch);
    }
    if snap.approval_waiting {
        return Err(RefuseReason::ApprovalWaiting);
    }
    if text != &req.expected_text {
        return Err(RefuseReason::Mismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(user_gen: u64, epoch: u64, obs: InputObservation) -> GuardSnapshot {
        GuardSnapshot {
            user_gen,
            input_epoch: epoch,
            observation: obs,
            approval_waiting: false,
        }
    }

    fn inject_req(user_gen: u64, epoch: u64, payload: &str) -> InjectIfRequest {
        InjectIfRequest {
            user_gen,
            input_epoch: epoch,
            require_empty: true,
            payload: payload.into(),
        }
    }

    fn submit_req(user_gen: u64, epoch: Option<u64>, expected: &str) -> SubmitIfRequest {
        SubmitIfRequest {
            user_gen,
            input_epoch: epoch,
            expected_text: expected.into(),
        }
    }

    #[test]
    fn inject_allows_matching_lease_on_empty_prompt() {
        let s = snap(3, 7, InputObservation::Text(String::new()));
        assert_eq!(check_inject(&inject_req(3, 7, "hi"), &s), Ok(()));
    }

    // The plan's race test: the user typed after the lease was taken (the
    // main loop bumped user_gen before this op was processed) — the payload
    // must not be written.
    #[test]
    fn inject_refuses_after_user_typed_since_lease() {
        let s = snap(4, 7, InputObservation::Text(String::new()));
        assert_eq!(
            check_inject(&inject_req(3, 7, "hi"), &s),
            Err(RefuseReason::StaleUserGen)
        );
    }

    #[test]
    fn inject_refuses_after_other_writer_since_lease() {
        let s = snap(3, 8, InputObservation::Text(String::new()));
        assert_eq!(
            check_inject(&inject_req(3, 7, "hi"), &s),
            Err(RefuseReason::StaleEpoch)
        );
    }

    #[test]
    fn inject_refuses_nonempty_prompt_when_empty_required() {
        // A human draft is present: the payload must not be appended to it.
        let s = snap(3, 7, InputObservation::Text("draft".into()));
        assert_eq!(
            check_inject(&inject_req(3, 7, "hi"), &s),
            Err(RefuseReason::PromptNotEmpty)
        );
    }

    #[test]
    fn inject_distinguishes_unsupported_from_unavailable() {
        let unsupported = snap(3, 7, InputObservation::Unsupported);
        let unavailable = snap(3, 7, InputObservation::Unavailable);
        assert_eq!(
            check_inject(&inject_req(3, 7, "hi"), &unsupported),
            Err(RefuseReason::Unsupported)
        );
        assert_eq!(
            check_inject(&inject_req(3, 7, "hi"), &unavailable),
            Err(RefuseReason::Unavailable)
        );
    }

    #[test]
    fn submit_allows_exact_render_with_valid_lease() {
        let s = snap(3, 8, InputObservation::Text("hi".into()));
        assert_eq!(check_submit(&submit_req(3, Some(8), "hi"), &s), Ok(()));
    }

    #[test]
    fn submit_refuses_enter_only() {
        let s = snap(3, 8, InputObservation::Text(String::new()));
        assert_eq!(
            check_submit(&submit_req(3, Some(8), ""), &s),
            Err(RefuseReason::EmptyExpected)
        );
    }

    // Human typed between the guarded inject and the submit: the CR must
    // never be written even though the visible text still matches.
    #[test]
    fn submit_refuses_when_user_typed_after_inject() {
        let s = snap(4, 8, InputObservation::Text("hi".into()));
        assert_eq!(
            check_submit(&submit_req(3, Some(8), "hi"), &s),
            Err(RefuseReason::StaleUserGen)
        );
    }

    #[test]
    fn submit_refuses_when_another_writer_took_the_prompt() {
        let s = snap(3, 9, InputObservation::Text("hi".into()));
        assert_eq!(
            check_submit(&submit_req(3, Some(8), "hi"), &s),
            Err(RefuseReason::StaleEpoch)
        );
    }

    #[test]
    fn submit_refuses_mixed_prompt() {
        // The human's draft got mixed with the injected text: pressing Enter
        // would submit the human's words. Exact match only.
        let s = snap(3, 8, InputObservation::Text("hi and my draft".into()));
        assert_eq!(
            check_submit(&submit_req(3, Some(8), "hi"), &s),
            Err(RefuseReason::Mismatch)
        );
    }

    #[test]
    fn submit_refuses_during_approval_prompt() {
        let mut s = snap(3, 8, InputObservation::Text("hi".into()));
        s.approval_waiting = true;
        assert_eq!(
            check_submit(&submit_req(3, Some(8), "hi"), &s),
            Err(RefuseReason::ApprovalWaiting)
        );
    }

    #[test]
    fn submit_without_epoch_relies_on_gen_and_text() {
        let s = snap(3, 42, InputObservation::Text("hi".into()));
        assert_eq!(check_submit(&submit_req(3, None, "hi"), &s), Ok(()));
        let typed = snap(4, 42, InputObservation::Text("hi".into()));
        assert_eq!(
            check_submit(&submit_req(3, None, "hi"), &typed),
            Err(RefuseReason::StaleUserGen)
        );
    }

    #[test]
    fn submit_refuses_unsupported_and_unavailable() {
        for (obs, reason) in [
            (InputObservation::Unsupported, RefuseReason::Unsupported),
            (InputObservation::Unavailable, RefuseReason::Unavailable),
        ] {
            let s = snap(3, 8, obs);
            assert_eq!(check_submit(&submit_req(3, Some(8), "hi"), &s), Err(reason));
        }
    }
}
