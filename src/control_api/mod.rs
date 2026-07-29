//! Bounded local control protocol for one foreground architect session.

#[cfg(target_os = "linux")]
pub(crate) mod codec;
pub mod protocol;

#[cfg(target_os = "linux")]
pub mod client;
#[cfg(target_os = "linux")]
pub mod registration;
#[cfg(target_os = "linux")]
pub mod supervisor;

#[cfg(target_os = "linux")]
pub(crate) mod peer;

pub use protocol::{
    ActionName, CallerAuth, CapabilitySnapshot, ControlAction, ControlErrorBody, ControlErrorCode,
    ControlRequest, ControlResponse, ControlResult, NativeSessionMode, SessionState,
    SessionStatusSnapshot, TaskDraft, TaskState, TaskStatusSnapshot, WorkerRole,
};
