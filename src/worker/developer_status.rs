//! Exact first-line classifier for Developer final messages.
//!
//! The three recognized lines are control signals. Every other form is
//! deliberately fail-open to `Ready`, allowing the Reviewer to catch an
//! incomplete implementation without pausing the run for a formatting error.

use super::runtime::DeveloperOutcomeStatus;

pub fn classify_developer_status(text: &str) -> DeveloperOutcomeStatus {
    let first_line = text.split('\n').next().unwrap_or_default();
    // Treat CRLF as the same line ending while keeping every other byte exact.
    let first_line = first_line.strip_suffix('\r').unwrap_or(first_line);
    match first_line {
        "STATUS: CLARIFICATION_REQUIRED" => DeveloperOutcomeStatus::ClarificationRequired,
        "STATUS: BLOCKED" => DeveloperOutcomeStatus::Blocked,
        "STATUS: READY" => DeveloperOutcomeStatus::Ready,
        _ => DeveloperOutcomeStatus::Ready,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_exact_anchored_status_lines() {
        assert_eq!(
            classify_developer_status("STATUS: READY\nfinished"),
            DeveloperOutcomeStatus::Ready
        );
        assert_eq!(
            classify_developer_status("STATUS: CLARIFICATION_REQUIRED\r\nquestion"),
            DeveloperOutcomeStatus::ClarificationRequired
        );
        assert_eq!(
            classify_developer_status("STATUS: BLOCKED\nobservations"),
            DeveloperOutcomeStatus::Blocked
        );
    }

    #[test]
    fn malformed_missing_or_late_status_fails_open_to_ready() {
        for text in [
            "",
            "\nSTATUS: BLOCKED",
            "# STATUS: BLOCKED",
            "**STATUS: CLARIFICATION_REQUIRED**",
            "summary\nSTATUS: BLOCKED",
            "status: blocked",
            "STATUS: MAYBE",
        ] {
            assert_eq!(
                classify_developer_status(text),
                DeveloperOutcomeStatus::Ready,
                "{text:?}"
            );
        }
    }
}
