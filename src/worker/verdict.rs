//! Exact first-line verdict classifier for reviewer final messages.
//!
//! The rest of a reviewer final is opaque payload. A missing or malformed
//! first line triggers one same-session format clarification; hcom never
//! searches later text or guesses from prose.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Lgtm,
    RequestChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeterminedReason {
    NoVerdictFound,
    UnrecognizedForm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictClassification {
    Determined(Verdict),
    Undetermined(UndeterminedReason),
}

pub fn classify_verdict(text: &str) -> VerdictClassification {
    let first_line = text.split('\n').next().unwrap_or_default();
    // Treat CRLF as the same line ending while keeping every other byte exact.
    let first_line = first_line.strip_suffix('\r').unwrap_or(first_line);
    match first_line {
        "VERDICT: LGTM" => VerdictClassification::Determined(Verdict::Lgtm),
        "VERDICT: REQUEST_CHANGES" => VerdictClassification::Determined(Verdict::RequestChanges),
        "" => VerdictClassification::Undetermined(UndeterminedReason::NoVerdictFound),
        _ => VerdictClassification::Undetermined(UndeterminedReason::UnrecognizedForm),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exact_anchored_first_lines_are_accepted() {
        assert_eq!(
            classify_verdict("VERDICT: LGTM\nall good"),
            VerdictClassification::Determined(Verdict::Lgtm)
        );
        assert_eq!(
            classify_verdict("VERDICT: REQUEST_CHANGES\r\nmajor finding"),
            VerdictClassification::Determined(Verdict::RequestChanges)
        );
    }

    #[test]
    fn later_or_decorated_verdicts_are_not_used() {
        for text in [
            "summary\nVERDICT: LGTM",
            "# VERDICT: LGTM",
            "VERDICT: LGTM.",
            "verdict: lgtm",
            "LGTM",
            "VERDICT: maybe",
        ] {
            assert_eq!(
                classify_verdict(text),
                VerdictClassification::Undetermined(UndeterminedReason::UnrecognizedForm),
                "{text:?}"
            );
        }
        assert_eq!(
            classify_verdict("\nVERDICT: REQUEST_CHANGES"),
            VerdictClassification::Undetermined(UndeterminedReason::NoVerdictFound)
        );
    }

    #[test]
    fn empty_message_has_no_verdict() {
        assert_eq!(
            classify_verdict(""),
            VerdictClassification::Undetermined(UndeterminedReason::NoVerdictFound)
        );
    }
}
