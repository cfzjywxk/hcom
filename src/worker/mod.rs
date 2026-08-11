//! Tool-neutral contracts for foreground-session, no-TUI task workers.

pub(crate) mod validation;

#[cfg(target_os = "linux")]
pub mod claude_exec_runtime;
#[cfg(target_os = "linux")]
pub mod claude_test;
#[cfg(target_os = "linux")]
pub mod codex;
pub mod contract;
pub mod developer_status;
pub mod environment;
#[cfg(target_os = "linux")]
pub mod exec_runtime;
pub mod fake;
pub mod fake_runtime;
#[cfg(target_os = "linux")]
pub mod guardian;
#[cfg(target_os = "linux")]
pub mod process;
pub mod profile;
pub mod result;
#[cfg(target_os = "linux")]
pub mod reviewer;
pub(crate) mod role_router;
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
    DeveloperOutcomeStatus, DeveloperOutcomeV1, OutcomeContract, ReviewerOutcomeV1,
    ReviewerVerdict, RoleSessionSpec, RuntimeApprovalPolicy, RuntimeClaudePermissions,
    RuntimeContractIdentity, RuntimeError, RuntimeFailureClass, RuntimeOutcome, RuntimeProfile,
    RuntimeProvider, RuntimeSandbox, RuntimeSessionKey, RuntimeTelemetry,
    RuntimeThreadProfileFields, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnProfileFields,
    RuntimeTurnPurpose, RuntimeTurnSpec, SanitizedRuntimeFailure, TaskWorkerProfiles,
    TaskWorkerRuntime, WorkerLane,
};

/// Give production-style unit fixtures a stable Git identity while preserving
/// the runner's real Git behavior for every operation other than `--version`.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn create_hermetic_git_facade(
    directory: &std::path::Path,
    real_git: &std::path::Path,
    exact_version: &str,
) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    let real_git = std::fs::canonicalize(real_git).expect("resolve fixture Git executable");
    let real_git = real_git.to_str().expect("fixture Git path is UTF-8");
    let facade = directory.join("git");
    let script = format!(
        "#!/bin/sh\n\
         set -eu\n\
         if [ \"$#\" -eq 1 ] && [ \"$1\" = \"--version\" ]; then\n\
             printf '%s\\n' {}\n\
             exit 0\n\
         fi\n\
         exec {} \"$@\"\n",
        shell_quote(exact_version),
        shell_quote(real_git),
    );
    std::fs::write(&facade, script).expect("write fixture Git facade");
    std::fs::set_permissions(&facade, std::fs::Permissions::from_mode(0o700))
        .expect("make fixture Git facade executable");
    std::fs::canonicalize(facade).expect("resolve fixture Git facade")
}

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
pub(crate) fn output_test_command_with_etxtbsy_retry(
    command: &mut std::process::Command,
) -> std::io::Result<std::process::Output> {
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    spawn_test_command_with_etxtbsy_retry(command)?.wait_with_output()
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        create_hermetic_git_facade, output_test_command_with_etxtbsy_retry,
        spawn_test_command_with_etxtbsy_retry,
    };
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn hermetic_git_facade_pins_version_and_transparently_delegates() {
        let temp = tempfile::tempdir().unwrap();
        let real_git = std::fs::canonicalize("/usr/bin/git").unwrap();
        let expected_version = "git version fixture-contract";
        let facade = create_hermetic_git_facade(temp.path(), &real_git, expected_version);

        let mut version_command = Command::new(&facade);
        version_command.arg("--version");
        let version = output_test_command_with_etxtbsy_retry(&mut version_command).unwrap();
        assert!(version.status.success());
        assert_eq!(
            String::from_utf8(version.stdout).unwrap(),
            format!("{expected_version}\n")
        );
        assert!(version.stderr.is_empty());

        let repository = temp.path().join("repository");
        let mut init_command = Command::new(&facade);
        init_command
            .args(["init", "--quiet", "--initial-branch=main"])
            .arg(&repository);
        let initialized = output_test_command_with_etxtbsy_retry(&mut init_command).unwrap();
        assert!(
            initialized.status.success(),
            "fixture Git init failed: {}",
            String::from_utf8_lossy(&initialized.stderr)
        );

        let run = |executable: &std::path::Path, arguments: &[&str]| {
            let mut command = Command::new(executable);
            command
                .args(arguments)
                .current_dir(&repository)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null");
            output_test_command_with_etxtbsy_retry(&mut command).unwrap()
        };
        for arguments in [
            &["rev-parse", "--show-toplevel"][..],
            &["rev-parse", "--verify", "refs/heads/missing"][..],
        ] {
            let delegated = run(&facade, arguments);
            let direct = run(&real_git, arguments);
            assert_eq!(delegated.status.code(), direct.status.code());
            assert_eq!(delegated.stdout, direct.stdout);
            assert_eq!(delegated.stderr, direct.stderr);
        }
    }

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
