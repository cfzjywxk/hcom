//! Deterministic tests for the real-tool fixture's endpoint recovery boundary.
//!
//! These exercise only the retry state machine: no real Codex, Claude, provider,
//! or PTY is launched.

mod support;

use std::cell::Cell;
use std::time::{Duration, Instant};

use support::real_tool::{EndpointAttemptError, FixtureProcessState, retry_fixture_endpoint};

fn process_state(process_bound: bool, process_alive: bool) -> FixtureProcessState {
    FixtureProcessState {
        instance_present: true,
        process_bound,
        pid: Some(4242),
        process_alive,
        inject_port: Some(31337),
        status: Some(if process_alive { "active" } else { "inactive" }.to_string()),
        status_context: Some(if process_alive { "pty:ready" } else { "exit:1" }.to_string()),
    }
}

#[test]
fn first_connection_refusal_retries_after_live_endpoint_recovers() {
    let attempts = Cell::new(0usize);
    let inspections = Cell::new(0usize);

    let value = retry_fixture_endpoint(
        "deterministic recovered endpoint",
        3,
        Instant::now() + Duration::from_secs(1),
        Duration::ZERO,
        || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            if attempt == 1 {
                Err(EndpointAttemptError::from_command(
                    "inject prompt",
                    1,
                    "connect: Connection refused (os error 111)",
                    "",
                ))
            } else {
                Ok("submitted")
            }
        },
        || {
            inspections.set(inspections.get() + 1);
            Ok(process_state(true, true))
        },
    )
    .expect("a live fixture must retry a transient refused connection");

    assert_eq!(value, "submitted");
    assert_eq!(attempts.get(), 2);
    assert_eq!(inspections.get(), 1);
}

#[test]
fn exited_child_stops_after_first_connection_refusal() {
    let attempts = Cell::new(0usize);
    let inspections = Cell::new(0usize);

    let error = retry_fixture_endpoint::<()>(
        "deterministic exited child",
        100,
        Instant::now() + Duration::from_secs(1),
        Duration::ZERO,
        || {
            attempts.set(attempts.get() + 1);
            Err(EndpointAttemptError::from_command(
                "inject prompt",
                1,
                "connect: Connection refused (os error 111)",
                "",
            ))
        },
        || {
            inspections.set(inspections.get() + 1);
            Ok(process_state(false, false))
        },
    )
    .expect_err("a dead fixture child must not be retried");

    assert!(error.contains("no longer process-bound/alive"), "{error}");
    assert!(error.contains("inject_port=Some(31337)"), "{error}");
    assert_eq!(attempts.get(), 1);
    assert_eq!(inspections.get(), 1);
}
