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
    ActionName, ActiveWorkerSnapshot, ArchitectActionReason, CallerAuth, CapabilitySnapshot,
    ClarificationPage, ClarificationRecord, ControlAction, ControlErrorBody, ControlErrorCode,
    ControlRequest, ControlResponse, ControlResult, MAX_CLARIFICATION_PAGE_RECORDS,
    MAX_CLARIFICATION_RECORDS_PER_RUN, MAX_CLARIFICATION_RECORDS_PER_TASK,
    MAX_PROGRESS_EVENTS_PER_RUN, NativeSessionMode, PendingArchitectActionSnapshot,
    ReviewerBindingSnapshot, ReviewerResultSnapshot, ReviewerVerdict, SessionProgressEvent,
    SessionState, SessionStatusSnapshot, TaskCompletionOutcome, TaskDraft, TaskState,
    TaskStatusSnapshot, WorkerRole,
};
