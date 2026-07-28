//! Bounded local control protocol for additive durable projects.

#[cfg(target_os = "linux")]
mod codec;
pub mod protocol;

#[cfg(target_os = "linux")]
pub mod client;
#[cfg(target_os = "linux")]
pub mod daemon;

#[cfg(target_os = "linux")]
pub(crate) mod peer;

pub use protocol::{
    ActionName, CallerAuth, CapabilitySnapshot, ContextKind, ContextRef, ControlAction,
    ControlErrorBody, ControlErrorCode, ControlRequest, ControlResponse, ControlResult,
    NativeSessionMode, TaskDraft, WorkerProfileDraft, WorkerRole,
};
