//! Reusable components shared by hcom's additive durable-control binaries.

pub mod control_api;
#[cfg(target_os = "linux")]
mod project_store;
