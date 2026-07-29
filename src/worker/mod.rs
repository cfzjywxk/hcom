//! Tool-neutral contracts for foreground-session, no-TUI task workers.

pub(crate) mod validation;

#[cfg(target_os = "linux")]
pub mod codex;
pub mod contract;
pub mod environment;
pub mod fake;
#[cfg(target_os = "linux")]
pub mod process;
pub mod result;
#[cfg(target_os = "linux")]
pub mod reviewer;
#[cfg(target_os = "linux")]
pub(crate) mod sandbox;

pub use contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeResult, NativeSessionBinding, OuterLaunchEnvelope, OutputDeclaration,
    PreparedTurn, ResultTransport, SchemaTransport, TurnControl, WorkerAdapter,
    WorkerAdapterRegistry, WorkerProfile, prepare_create_turn, prepare_resume_turn,
};
pub use environment::{
    EnvironmentLeaseDescriptor, EnvironmentPolicy, ExactEnvironmentRequirement,
    ExecutionEnvironmentLease, MaterializedWorkerEnvironment, WorkerEnvironmentIdentity,
};
#[cfg(target_os = "linux")]
pub use process::{
    HeartbeatControl, ProcessCompletion, ProcessIdentity, ProcessRunner, RunningWorker, WorkerExit,
    WorkerTermination,
};
