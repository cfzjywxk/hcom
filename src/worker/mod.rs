//! Tool-neutral contracts for foreground-session, no-TUI task workers.

pub(crate) mod validation;

#[cfg(target_os = "linux")]
pub mod codex;
#[cfg(target_os = "linux")]
pub mod codex_app_server;
#[cfg(target_os = "linux")]
pub(crate) mod codex_rpc;
pub mod contract;
pub mod environment;
#[cfg(target_os = "linux")]
pub mod exec_runtime;
pub mod fake;
pub mod fake_runtime;
#[cfg(target_os = "linux")]
pub mod process;
pub mod profile;
pub mod result;
#[cfg(target_os = "linux")]
pub mod reviewer;
pub mod runtime;
#[cfg(target_os = "linux")]
pub(crate) mod sandbox;
pub mod verdict;

pub use contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeResult, NativeSessionBinding, OuterLaunchEnvelope, OutputDeclaration,
    PreparedTurn, ResultTransport, SchemaTransport, TurnControl, WorkerAdapter,
    WorkerAdapterRegistry, WorkerProfile, prepare_create_turn, prepare_resume_turn,
};
pub use environment::{
    EnvironmentLeaseDescriptor, EnvironmentPolicy, ExactEnvironmentRequirement,
    ExecutionEnvironmentLease, MaterializedWorkerEnvironment, ParentEnvironment,
    WorkerEnvironmentIdentity,
};
#[cfg(target_os = "linux")]
pub use process::{
    HeartbeatControl, ProcessCompletion, ProcessIdentity, ProcessRunner, RunningWorker, WorkerExit,
    WorkerTermination,
};
pub use runtime::{
    AppServerWorkerProfiles, DeveloperOutcomeStatus, DeveloperOutcomeV1, OutcomeContract,
    ReviewFindingSeverity, ReviewFindingV1, ReviewerOutcomeV1, ReviewerVerdict, RoleSessionSpec,
    RuntimeApprovalPolicy, RuntimeContractIdentity, RuntimeError, RuntimeFailureClass,
    RuntimeOutcome, RuntimeProfile, RuntimeProvider, RuntimeSandbox, RuntimeSessionKey,
    RuntimeTelemetry, RuntimeThreadProfileFields, RuntimeTurnKey, RuntimeTurnPoll,
    RuntimeTurnProfileFields, RuntimeTurnPurpose, RuntimeTurnSpec, SanitizedRuntimeFailure,
    TaskWorkerRuntime,
};

/// Unit tests create temporary executables while other test threads are
/// concurrently forking helpers. A fork can briefly inherit another thread's
/// still-open write descriptor and make Linux return ETXTBSY for an otherwise
/// complete executable. Production only executes pre-existing pinned tools, so
/// keep the bounded retry in test builds rather than weakening production
/// executable validation.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn spawn_test_command_with_etxtbsy_retry(
    command: &mut std::process::Command,
) -> std::io::Result<std::process::Child> {
    const MAX_ATTEMPTS: usize = 32;
    for attempt in 0..MAX_ATTEMPTS {
        match command.spawn() {
            Err(error)
                if error.raw_os_error() == Some(libc::ETXTBSY) && attempt + 1 < MAX_ATTEMPTS =>
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            result => return result,
        }
    }
    unreachable!("the final spawn attempt always returns")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::spawn_test_command_with_etxtbsy_retry;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn temporary_executable_spawn_retries_etxtbsy() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("eventually-executable");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();

        let mut writer = OpenOptions::new().write(true).open(&executable).unwrap();
        writer.flush().unwrap();
        let direct_error = Command::new(&executable).spawn().unwrap_err();
        assert_eq!(direct_error.raw_os_error(), Some(libc::ETXTBSY));

        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(writer);
        });

        let mut command = Command::new(&executable);
        let mut child = spawn_test_command_with_etxtbsy_retry(&mut command).unwrap();
        assert!(child.wait().unwrap().success());
        release.join().unwrap();
    }
}
