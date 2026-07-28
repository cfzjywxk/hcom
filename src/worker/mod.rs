//! Tool-neutral contracts for durable, no-TUI task workers.

pub(crate) mod validation;

pub mod contract;
pub mod environment;
pub mod fake;
pub mod result;

pub use contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeResult, NativeSessionBinding, OutputDeclaration, PreparedTurn,
    ResultTransport, SchemaTransport, TurnControl, WorkerAdapter, WorkerAdapterRegistry,
    WorkerProfile, prepare_create_turn, prepare_resume_turn,
};
pub use environment::{
    EnvironmentLeaseDescriptor, EnvironmentPolicy, ExecutionEnvironmentLease,
    MaterializedWorkerEnvironment, WorkerEnvironmentIdentity,
};
