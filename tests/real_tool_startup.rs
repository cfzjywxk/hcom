//! Deterministic tests for the Codex real-tool fixture's startup gate.
//!
//! These classify synthetic screen snapshots only: no real Codex, provider, or
//! PTY is launched.

mod support;

use serde_json::{Value, json};

use support::codex_mock::{CodexStartupAction, classify_codex_startup_screen};

fn screen(lines: &[&str], ready: bool, prompt_empty: bool, input_text: Value) -> Value {
    json!({
        "lines": lines,
        "ready": ready,
        "prompt_empty": prompt_empty,
        "input_text": input_text,
    })
}

fn exact_trust_screen() -> Value {
    screen(
        &[
            "> You are in /tmp/hcom-owned-fixture",
            "Do you trust the contents of this directory? Working with untrusted contents comes with higher risk of prompt injection.",
            "› 1. Yes, continue",
            "  2. No, quit",
            "  Press enter to continue",
        ],
        true,
        false,
        json!("1. Yes, continue"),
    )
}

#[test]
fn exact_trust_surface_is_the_only_confirmation_candidate() {
    assert_eq!(
        classify_codex_startup_screen(&exact_trust_screen(), false).unwrap(),
        CodexStartupAction::ConfirmTrust
    );

    let incomplete = screen(
        &[
            "Do you trust the contents of this directory?",
            "› 1. Yes, continue",
        ],
        true,
        false,
        json!("1. Yes, continue"),
    );
    let error = classify_codex_startup_screen(&incomplete, false).unwrap_err();
    assert!(error.contains("incomplete Codex trust surface"), "{error}");
}

#[test]
fn unknown_startup_surface_fails_closed() {
    let unknown = screen(
        &["Choose an account", "› 1. Sign in with ChatGPT"],
        true,
        false,
        json!("1. Sign in with ChatGPT"),
    );
    let error = classify_codex_startup_screen(&unknown, false).unwrap_err();
    assert!(error.contains("unknown Codex startup menu"), "{error}");
}

#[test]
fn ordinary_draft_is_never_treated_as_onboarding() {
    let draft = screen(
        &["› explain this repository"],
        true,
        false,
        json!("explain this repository"),
    );
    let error = classify_codex_startup_screen(&draft, false).unwrap_err();
    assert!(error.contains("ordinary draft"), "{error}");
}

#[test]
fn confirmation_must_be_followed_by_a_fresh_empty_prompt() {
    assert_eq!(
        classify_codex_startup_screen(&exact_trust_screen(), true).unwrap(),
        CodexStartupAction::Wait,
        "a stale trust repaint must not trigger a second Enter"
    );

    let incomplete_prompt = screen(&["›"], true, false, json!(""));
    assert!(classify_codex_startup_screen(&incomplete_prompt, true).is_err());

    let empty_prompt = screen(&["›"], true, true, json!(""));
    assert_eq!(
        classify_codex_startup_screen(&empty_prompt, true).unwrap(),
        CodexStartupAction::Ready
    );
}
