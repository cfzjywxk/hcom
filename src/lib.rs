//! Reusable components shared by hcom's additive session-task runtime.

#[cfg(target_os = "linux")]
pub mod architect;
#[cfg(target_os = "linux")]
pub mod artifact;
#[cfg(not(target_os = "linux"))]
pub mod architect {
    use anyhow::{Result, bail};

    pub fn run_component(_args: &[String]) -> Result<()> {
        bail!("hcom architect is supported only on Linux")
    }

    pub fn run_cli(_args: &[String]) -> Result<i32> {
        bail!("hcom architect is supported only on Linux")
    }

    pub fn help_text() -> &'static str {
        "Usage:\n  hcom architect\n\nhcom architect is supported only on Linux."
    }
}
pub mod control_api;
#[cfg(target_os = "linux")]
pub mod orchestrator;
#[cfg(target_os = "linux")]
pub mod worker;
