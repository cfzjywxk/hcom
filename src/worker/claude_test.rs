//! Fail-closed opt-in contract for model-backed Claude tests.
//!
//! Production profiles intentionally default to Opus. Real tests must instead
//! name the bounded Haiku/medium profile explicitly and inherit the exact
//! operator-supplied proxy environment before any Claude executable is
//! resolved or spawned.

use super::environment::{ParentEnvironment, validate_claude_proxy_environment};
use super::profile::ClaudeInvocationProfile;
use anyhow::{Result, bail};
use std::ffi::OsStr;

pub const CLAUDE_TEST_MODEL_ENV: &str = "CLAUDE_TEST_MODEL";
pub const CLAUDE_TEST_EFFORT_ENV: &str = "CLAUDE_TEST_EFFORT";
pub const CLAUDE_TEST_MODEL: &str = "haiku";
pub const CLAUDE_TEST_EFFORT: &str = "medium";

#[derive(Clone)]
pub struct ClaudeModelTestGate {
    parent_environment: ParentEnvironment,
    profile: ClaudeInvocationProfile,
}

impl ClaudeModelTestGate {
    /// Capture the current environment only after the caller explicitly
    /// selected Haiku/medium. No executable lookup or provider probe occurs
    /// here, and the proxy variables are only validated, never added or
    /// repaired.
    pub fn capture() -> Result<Self> {
        validate_requested_profile(
            std::env::var_os(CLAUDE_TEST_MODEL_ENV).as_deref(),
            std::env::var_os(CLAUDE_TEST_EFFORT_ENV).as_deref(),
        )?;
        let parent_environment = ParentEnvironment::capture_current()?;
        validate_claude_proxy_environment(&parent_environment)?;
        // Validate conflicts with the two required Claude policy pins now as
        // well, still before a native executable can be selected.
        let _ = parent_environment.materialize_claude()?;
        Ok(Self {
            parent_environment,
            profile: ClaudeInvocationProfile {
                model: CLAUDE_TEST_MODEL.into(),
                effort: CLAUDE_TEST_EFFORT.into(),
                dangerously_skip_permissions: true,
            },
        })
    }

    pub fn parent_environment(&self) -> &ParentEnvironment {
        &self.parent_environment
    }

    pub fn profile(&self) -> &ClaudeInvocationProfile {
        &self.profile
    }
}

fn validate_requested_profile(model: Option<&OsStr>, effort: Option<&OsStr>) -> Result<()> {
    match model.and_then(OsStr::to_str) {
        Some(CLAUDE_TEST_MODEL) => {}
        Some(_) => bail!(
            "{CLAUDE_TEST_MODEL_ENV} must be exactly {CLAUDE_TEST_MODEL} for model-backed Claude tests"
        ),
        None if model.is_some() => {
            bail!("{CLAUDE_TEST_MODEL_ENV} must be valid UTF-8 and exactly {CLAUDE_TEST_MODEL}")
        }
        None => bail!(
            "{CLAUDE_TEST_MODEL_ENV} must be explicitly set to {CLAUDE_TEST_MODEL} for model-backed Claude tests"
        ),
    }
    match effort.and_then(OsStr::to_str) {
        Some(CLAUDE_TEST_EFFORT) => Ok(()),
        Some(_) => bail!(
            "{CLAUDE_TEST_EFFORT_ENV} must be exactly {CLAUDE_TEST_EFFORT} for model-backed Claude tests"
        ),
        None if effort.is_some() => {
            bail!("{CLAUDE_TEST_EFFORT_ENV} must be valid UTF-8 and exactly {CLAUDE_TEST_EFFORT}")
        }
        None => bail!(
            "{CLAUDE_TEST_EFFORT_ENV} must be explicitly set to {CLAUDE_TEST_EFFORT} for model-backed Claude tests"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn model_backed_profile_requires_explicit_exact_haiku_medium() {
        validate_requested_profile(Some(OsStr::new("haiku")), Some(OsStr::new("medium"))).unwrap();
        for (model, effort) in [
            (None, Some(OsStr::new("medium"))),
            (Some(OsStr::new("opus")), Some(OsStr::new("medium"))),
            (Some(OsStr::new("haiku")), None),
            (Some(OsStr::new("haiku")), Some(OsStr::new("high"))),
        ] {
            assert!(validate_requested_profile(model, effort).is_err());
        }
        let invalid = OsString::from_vec(b"haiku\xff".to_vec());
        assert!(
            validate_requested_profile(Some(invalid.as_os_str()), Some(OsStr::new("medium")))
                .is_err()
        );
    }
}
