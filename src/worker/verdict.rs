//! Anchored affirmative verdict classifier for reviewer final messages.
//!
//! The supervisor extracts exactly one bit (LGTM / REQUEST_CHANGES) from the
//! reviewer's final message. Everything else in the message is opaque payload.
//! Grammar is deliberately asymmetric: a false REQUEST_CHANGES costs one wasted
//! round, a false LGTM advances a task wrongly — so the LGTM direction only
//! accepts an anchored token with a closed-allowlist tail, and every negated,
//! conditional, interrogative, or otherwise unrecognized form is undetermined
//! (the supervisor re-asks once, then stops for a human).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Lgtm,
    RequestChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeterminedReason {
    /// No line yielded a verdict: empty message, no anchored token on the
    /// first non-empty line, and no labeled `VERDICT` line in the scanned view.
    NoVerdictFound,
    /// A candidate line carried an LGTM-class token with a tail outside the
    /// closed allowlist (conditions, reservations, questions, free text).
    UnrecognizedForm,
    /// Two determinations disagree (first line vs labeled line, or labeled
    /// lines among themselves). Never guessed, never defaulted to LGTM.
    Conflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictClassification {
    Determined(Verdict),
    Undetermined(UndeterminedReason),
}

/// Classify a reviewer final message (already redacted and bounded by the
/// caller; this function never reads beyond the given text).
pub fn classify_verdict(text: &str) -> VerdictClassification {
    let mut first_line_result: Option<LineOutcome> = None;
    let mut labeled: Vec<Verdict> = Vec::new();
    let mut saw_unrecognized_labeled = false;

    for raw_line in text.lines() {
        let stripped = strip_decorations(raw_line);
        if stripped.is_empty() {
            continue;
        }
        let is_first = first_line_result.is_none();
        let outcome = classify_line(&stripped);
        if is_first {
            first_line_result = Some(outcome);
        }
        // Labeled lines participate in conflict detection wherever they
        // appear, including when they are also the first line.
        if stripped.to_ascii_lowercase().starts_with("verdict") {
            match outcome {
                LineOutcome::Determined(v) => labeled.push(v),
                LineOutcome::Unrecognized => saw_unrecognized_labeled = true,
                LineOutcome::NoToken => saw_unrecognized_labeled = true,
            }
        }
    }

    let Some(first) = first_line_result else {
        return VerdictClassification::Undetermined(UndeterminedReason::NoVerdictFound);
    };

    if labeled.windows(2).any(|w| w[0] != w[1]) {
        return VerdictClassification::Undetermined(UndeterminedReason::Conflict);
    }
    let labeled_verdict = labeled.first().copied();

    match first {
        LineOutcome::Determined(v) => match labeled_verdict {
            Some(l) if l != v => VerdictClassification::Undetermined(UndeterminedReason::Conflict),
            _ => VerdictClassification::Determined(v),
        },
        LineOutcome::Unrecognized => match labeled_verdict {
            // The first line carried an affirmative-looking token with an
            // unrecognized tail; a later labeled line cannot override that
            // ambiguity silently.
            Some(_) => VerdictClassification::Undetermined(UndeterminedReason::UnrecognizedForm),
            None => VerdictClassification::Undetermined(UndeterminedReason::UnrecognizedForm),
        },
        LineOutcome::NoToken => match labeled_verdict {
            Some(v) => VerdictClassification::Determined(v),
            None if saw_unrecognized_labeled => {
                VerdictClassification::Undetermined(UndeterminedReason::UnrecognizedForm)
            }
            None => VerdictClassification::Undetermined(UndeterminedReason::NoVerdictFound),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineOutcome {
    Determined(Verdict),
    /// An anchored LGTM-class token was present but its tail is not in the
    /// closed allowlist, or a labeled VERDICT line had no recognizable value.
    Unrecognized,
    /// The line carries no anchored token at all.
    NoToken,
}

/// Strip common markdown decoration and collapse whitespace. Keeps the result
/// lowercase-insensitive comparisons to the caller.
fn strip_decorations(line: &str) -> String {
    let line = line.trim_matches('\u{feff}');
    let trimmed = line
        .trim()
        .trim_start_matches(|c: char| {
            matches!(c, '#' | '>' | '-' | '*' | '`' | '\u{feff}') || c.is_whitespace()
        })
        .trim_end_matches(|c: char| matches!(c, '*' | '`') || c.is_whitespace());
    let mut out = String::with_capacity(trimmed.len());
    let mut last_space = false;
    for c in trimmed.chars() {
        if c.is_whitespace() {
            if !last_space && !out.is_empty() {
                out.push(' ');
            }
            last_space = true;
        } else {
            out.push(c);
            last_space = false;
        }
    }
    out.trim_end().to_string()
}

fn classify_line(stripped: &str) -> LineOutcome {
    let lower = stripped.to_ascii_lowercase();
    let mut rest = lower.as_str();

    let had_label = rest.starts_with("verdict");
    if had_label {
        rest = rest["verdict".len()..].trim_start();
        rest = rest
            .strip_prefix(':')
            .or_else(|| rest.strip_prefix('-'))
            .or_else(|| rest.strip_prefix('='))
            .unwrap_or(rest)
            .trim_start();
    }

    if let Some(tail) = match_changes_token(rest) {
        let _ = tail; // tail is arbitrary for REQUEST_CHANGES (cheap direction)
        return LineOutcome::Determined(Verdict::RequestChanges);
    }
    if let Some(tail) = match_lgtm_token(rest) {
        return if lgtm_tail_allowed(tail) {
            LineOutcome::Determined(Verdict::Lgtm)
        } else {
            LineOutcome::Unrecognized
        };
    }
    if had_label {
        // "VERDICT: <something unrecognizable>" is an explicit but unusable
        // labeling attempt, not the mere absence of a verdict.
        return LineOutcome::Unrecognized;
    }
    LineOutcome::NoToken
}

/// Match an anchored CHANGES-class token at the start of `rest` (already
/// lowercased). Returns the tail after the token when matched.
fn match_changes_token(rest: &str) -> Option<&str> {
    const FORMS: &[&str] = &[
        "request_changes",
        "request changes",
        "request-changes",
        "changes_requested",
        "changes requested",
        "changes-requested",
    ];
    anchored_token(rest, FORMS)
}

/// Match an anchored LGTM-class token at the start of `rest`.
fn match_lgtm_token(rest: &str) -> Option<&str> {
    const FORMS: &[&str] = &["lgtm", "approved", "approve"];
    anchored_token(rest, FORMS)
}

fn anchored_token<'a>(rest: &'a str, forms: &[&str]) -> Option<&'a str> {
    for form in forms {
        if let Some(tail) = rest.strip_prefix(form) {
            let boundary_ok = tail
                .chars()
                .next()
                .map(|c| !c.is_alphanumeric() && c != '_')
                .unwrap_or(true);
            if boundary_ok {
                return Some(tail);
            }
        }
    }
    None
}

/// The closed tail allowlist for the LGTM direction: empty, punctuation that
/// is not a question mark, or one of two fixed comment phrases (optionally
/// followed by non-question punctuation).
fn lgtm_tail_allowed(tail: &str) -> bool {
    let tail = tail.trim();
    if tail.is_empty() {
        return true;
    }
    if is_non_question_punctuation(tail) {
        return true;
    }
    const PHRASES: &[&str] = &["with minor comments", "with non-blocking comments"];
    for phrase in PHRASES {
        if let Some(after) = tail.strip_prefix(phrase) {
            let after = after.trim();
            if after.is_empty() || is_non_question_punctuation(after) {
                return true;
            }
        }
    }
    false
}

fn is_non_question_punctuation(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| !c.is_alphanumeric() && c != '?' && c != '？' && c != '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lgtm(text: &str) {
        assert_eq!(
            classify_verdict(text),
            VerdictClassification::Determined(Verdict::Lgtm),
            "expected LGTM for {text:?}"
        );
    }

    fn changes(text: &str) {
        assert_eq!(
            classify_verdict(text),
            VerdictClassification::Determined(Verdict::RequestChanges),
            "expected REQUEST_CHANGES for {text:?}"
        );
    }

    fn undetermined(text: &str, reason: UndeterminedReason) {
        assert_eq!(
            classify_verdict(text),
            VerdictClassification::Undetermined(reason),
            "expected undetermined({reason:?}) for {text:?}"
        );
    }

    #[test]
    fn canonical_forms() {
        lgtm("VERDICT: LGTM");
        changes("VERDICT: REQUEST_CHANGES");
    }

    #[test]
    fn case_and_whitespace_tolerance() {
        lgtm("verdict: lgtm");
        lgtm("  VERDICT:   LGTM  ");
        changes("Verdict: Request_Changes");
        lgtm("VERDICT: LGTM\r\nrest of findings");
    }

    #[test]
    fn preceding_blank_lines_are_skipped() {
        lgtm("\n\n   \nVERDICT: LGTM\nfindings");
    }

    #[test]
    fn markdown_decoration_is_stripped() {
        lgtm("**VERDICT: LGTM**");
        lgtm("## VERDICT: LGTM");
        lgtm("> VERDICT: LGTM");
        lgtm("- VERDICT: LGTM");
        changes("`VERDICT: REQUEST_CHANGES`");
    }

    #[test]
    fn prefix_is_optional() {
        lgtm("LGTM");
        changes("REQUEST_CHANGES");
        changes("REQUEST CHANGES");
        changes("CHANGES REQUESTED");
    }

    #[test]
    fn approve_synonyms() {
        lgtm("VERDICT: APPROVE");
        lgtm("APPROVED");
        lgtm("APPROVED!");
    }

    #[test]
    fn allowlist_tails_pass() {
        lgtm("VERDICT: LGTM with minor comments");
        lgtm("LGTM with non-blocking comments.");
        lgtm("LGTM.");
        lgtm("LGTM!!");
    }

    #[test]
    fn question_marks_are_not_affirmative() {
        undetermined("LGTM?", UndeterminedReason::UnrecognizedForm);
        undetermined("VERDICT: LGTM?", UndeterminedReason::UnrecognizedForm);
        undetermined(
            "APPROVED? No, tests fail",
            UndeterminedReason::UnrecognizedForm,
        );
    }

    #[test]
    fn unrecognized_tails_are_undetermined() {
        undetermined(
            "VERDICT: LGTM except for one blocker",
            UndeterminedReason::UnrecognizedForm,
        );
        undetermined(
            "LGTM but the tests are red",
            UndeterminedReason::UnrecognizedForm,
        );
        undetermined(
            "LGTM if you fix the docs",
            UndeterminedReason::UnrecognizedForm,
        );
        undetermined(
            "VERDICT: LGTM with reservations",
            UndeterminedReason::UnrecognizedForm,
        );
    }

    #[test]
    fn negations_never_pass() {
        undetermined("NOT LGTM", UndeterminedReason::NoVerdictFound);
        undetermined("DO NOT APPROVE", UndeterminedReason::NoVerdictFound);
        undetermined("I cannot APPROVE", UndeterminedReason::NoVerdictFound);
        undetermined(
            "The developer claims LGTM, but the fix is wrong",
            UndeterminedReason::NoVerdictFound,
        );
    }

    #[test]
    fn changes_direction_tolerates_tails() {
        changes("REQUEST_CHANGES: three findings below");
        changes("VERDICT: REQUEST_CHANGES because the tests fail");
        changes("request changes — see findings");
    }

    #[test]
    fn labeled_fallback_after_preamble() {
        lgtm("Here is my review of the change.\n\nVERDICT: LGTM\ndetails");
        changes("Summary first.\nVERDICT: REQUEST_CHANGES\n- finding one");
    }

    #[test]
    fn bare_tokens_later_in_message_do_not_count() {
        undetermined(
            "Overall summary of the diff.\nIf tests pass I would say LGTM eventually.",
            UndeterminedReason::NoVerdictFound,
        );
    }

    #[test]
    fn conflicts_are_never_guessed() {
        undetermined(
            "LGTM\nVERDICT: REQUEST_CHANGES",
            UndeterminedReason::Conflict,
        );
        undetermined(
            "VERDICT: LGTM\nVERDICT: REQUEST_CHANGES",
            UndeterminedReason::Conflict,
        );
        undetermined(
            "Preamble.\nVERDICT: LGTM\nmore\nVERDICT: REQUEST_CHANGES",
            UndeterminedReason::Conflict,
        );
    }

    #[test]
    fn repeated_consistent_labels_pass() {
        lgtm("VERDICT: LGTM\nfindings\nVERDICT: LGTM");
    }

    #[test]
    fn word_boundaries_hold() {
        undetermined("LGTMX", UndeterminedReason::NoVerdictFound);
        undetermined(
            "APPROVES the general direction",
            UndeterminedReason::NoVerdictFound,
        );
        undetermined("REQUEST_CHANGESX", UndeterminedReason::NoVerdictFound);
    }

    #[test]
    fn labeled_but_unusable_is_unrecognized() {
        undetermined("VERDICT: maybe", UndeterminedReason::UnrecognizedForm);
        undetermined("VERDICT:", UndeterminedReason::UnrecognizedForm);
        undetermined(
            "Preamble.\nVERDICT: unclear outcome",
            UndeterminedReason::UnrecognizedForm,
        );
    }

    #[test]
    fn empty_and_whitespace_only() {
        undetermined("", UndeterminedReason::NoVerdictFound);
        undetermined("\n\n   \n", UndeterminedReason::NoVerdictFound);
    }

    #[test]
    fn first_line_ambiguity_not_overridden_by_later_label() {
        // The first line already looks like an affirmative attempt with an
        // unusable tail; a later clean label must not silently win.
        undetermined(
            "LGTM except one blocker\nVERDICT: LGTM",
            UndeterminedReason::UnrecognizedForm,
        );
    }
}
