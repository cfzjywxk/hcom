#![cfg(target_os = "linux")]

//! Opt-in real Claude native-contract and lifecycle probes.
//!
//! Run individual scenarios through the scripts documented in
//! `docs/claude-task-lane-testing.md`. Every real turn is gated before native
//! executable discovery by explicit `CLAUDE_TEST_MODEL=haiku`,
//! `CLAUDE_TEST_EFFORT=medium`, and the inherited exact four-proxy contract.

use hcom::control_api::WorkerRole;
use hcom::worker::claude_exec_runtime::{ClaudeExecRuntimeConfig, ClaudeExecTaskWorkerRuntime};
use hcom::worker::claude_test::ClaudeModelTestGate;
use hcom::worker::environment::{EnvironmentPolicy, ExecutionEnvironmentLease};
use hcom::worker::guardian::{CleanupRegistryInterlock, GuardianCleanupRegistry};
use hcom::worker::{
    OutcomeContract, RoleSessionSpec, RuntimeFailureClass, RuntimeOutcome, RuntimeProfile,
    RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnPurpose, RuntimeTurnSpec,
    TaskWorkerRuntime,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const PARENT_HELPER_ENV: &str = "HCOM_CLAUDE_PARENT_DEATH_HELPER";
const PARENT_ROOT_ENV: &str = "HCOM_CLAUDE_PARENT_DEATH_ROOT";

fn hcom_binary() -> PathBuf {
    fs::canonicalize(env!("CARGO_BIN_EXE_hcom")).expect("resolve test hcom binary")
}

struct RealClaudeFixture {
    _temp: Option<tempfile::TempDir>,
    project: PathBuf,
    repository: PathBuf,
    artifacts: PathBuf,
    registry: GuardianCleanupRegistry,
    runtime: ClaudeExecTaskWorkerRuntime,
    profile: RuntimeProfile,
}

impl RealClaudeFixture {
    fn new(label: &str) -> Self {
        let temp = tempfile::Builder::new()
            .prefix(&format!("hcom-real-claude-{label}."))
            .tempdir()
            .unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let retained = std::env::var_os("HCOM_REAL_E2E_KEEP").is_some();
        let temp = if retained {
            eprintln!("preserving real Claude fixture at {}", root.display());
            let _ = temp.keep();
            None
        } else {
            Some(temp)
        };
        Self::open(root, temp)
    }

    fn open(root: PathBuf, temp: Option<tempfile::TempDir>) -> Self {
        let gate = ClaudeModelTestGate::capture().unwrap();
        let project = root.join("project");
        let repository = root.join("repository");
        let artifacts = root.join("artifacts");
        for directory in [&root, &project, &repository, &artifacts] {
            fs::create_dir_all(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let materialized = gate.parent_environment().materialize_claude().unwrap();
        let environment = materialized
            .iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        let lease = ExecutionEnvironmentLease::capture_complete(
            "real-claude-contract-lease",
            "real-claude-contract-epoch",
            &EnvironmentPolicy::baseline(),
            gate.parent_environment(),
            Vec::new(),
        )
        .unwrap();
        let registry = GuardianCleanupRegistry::default();
        let runtime = ClaudeExecTaskWorkerRuntime::open(ClaudeExecRuntimeConfig {
            claude: "claude".into(),
            guardian_executable: hcom_binary(),
            environment,
            lease,
            artifact_root_path: artifacts.clone(),
            run_id: "run-real-claude-contract".into(),
            task_id: "task-real-claude-contract".into(),
            cleanup_registry: registry.clone(),
        })
        .unwrap();
        let profile =
            RuntimeProfile::from_claude("real Claude test profile", gate.profile()).unwrap();
        Self {
            _temp: temp,
            project,
            repository,
            artifacts,
            registry,
            runtime,
            profile,
        }
    }

    fn open_developer(&mut self) -> RuntimeSessionKey {
        self.runtime
            .open_session(RoleSessionSpec {
                role: WorkerRole::Developer,
                task_key: "task-real-claude-contract".into(),
                cwd: self.project.clone(),
                task_repository: self.repository.clone(),
                profile: self.profile.clone(),
                developer_instructions: "This is a controlled real Claude contract probe. Your final first line must be exactly STATUS: READY.".into(),
            })
            .unwrap()
    }

    fn start(
        &mut self,
        session: RuntimeSessionKey,
        purpose: RuntimeTurnPurpose,
        prompt: String,
        timeout: Duration,
    ) -> RuntimeTurnKey {
        self.runtime
            .start_turn(
                session,
                RuntimeTurnSpec {
                    role: WorkerRole::Developer,
                    task_key: "task-real-claude-contract".into(),
                    purpose,
                    cwd: self.project.clone(),
                    task_repository: self.repository.clone(),
                    prompt,
                    profile: self.profile.clone(),
                    outcome_contract: OutcomeContract::DeveloperV1,
                    timeout,
                },
            )
            .unwrap()
    }

    fn poll_until_terminal(&mut self, turn: RuntimeTurnKey, deadline: Instant) -> RuntimeTurnPoll {
        loop {
            let poll = self.runtime.poll_turn(turn).unwrap();
            if poll.is_terminal() {
                return poll;
            }
            assert!(
                Instant::now() < deadline,
                "real Claude turn exceeded test deadline"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn native_session_ids(&self) -> Vec<String> {
        let mut found = Vec::new();
        for path in walk_files(&self.artifacts) {
            if path.file_name().and_then(|name| name.to_str()) != Some("native.stdout.partial") {
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let Some(rest) = text.lines().next().and_then(|line| {
                line.split("\"session_id\":\"")
                    .nth(1)
                    .and_then(|rest| rest.split('"').next())
            }) else {
                continue;
            };
            found.push((path, rest.to_string()));
        }
        found.sort_by(|left, right| left.0.cmp(&right.0));
        found.into_iter().map(|(_, id)| id).collect()
    }
}

fn walk_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

fn process_birth(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    stat[close + 1..].split_whitespace().nth(19)?.parse().ok()
}

fn read_process_identity(path: &Path, deadline: Instant) -> (u32, u64) {
    loop {
        if let Ok(value) = fs::read_to_string(path) {
            let mut fields = value.split_ascii_whitespace();
            if let (Some(pid), Some(birth)) = (fields.next(), fields.next())
                && let (Ok(pid), Ok(birth)) = (pid.parse(), birth.parse())
            {
                return (pid, birth);
            }
        }
        assert!(
            Instant::now() < deadline,
            "Claude did not run lifecycle helper"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_identity_gone(pid: u32, birth: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while process_birth(pid) == Some(birth) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_ne!(
        process_birth(pid),
        Some(birth),
        "escaped descendant {pid}/{birth} survived"
    );
}

fn write_hanging_double_fork(root: &Path, label: &str) -> (PathBuf, PathBuf) {
    let helper = root.join(format!("{label}-hang.py"));
    let identity = root.join(format!("{label}-descendant.identity"));
    fs::write(
        &helper,
        r#"#!/usr/bin/python3
import os
import sys
import time

first = os.fork()
if first == 0:
    os.setsid()
    second = os.fork()
    if second == 0:
        stat = open(f"/proc/{os.getpid()}/stat", encoding="ascii").read()
        birth = stat.rsplit(")", 1)[1].split()[19]
        with open(sys.argv[1], "w", encoding="ascii") as output:
            output.write(f"{os.getpid()} {birth}\n")
            output.flush()
            os.fsync(output.fileno())
        time.sleep(300)
    os._exit(0)
os.waitpid(first, 0)
time.sleep(300)
"#,
    )
    .unwrap();
    (helper, identity)
}

#[test]
#[ignore = "validates explicit Haiku/medium and exact inherited Claude proxy without spawning Claude"]
fn real_claude_test_gate_only() {
    let gate = ClaudeModelTestGate::capture().unwrap();
    assert_eq!(gate.profile().model, "haiku");
    assert_eq!(gate.profile().effort, "medium");
}

#[test]
#[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
fn real_claude_native_contract_create_and_exact_resume() {
    let mut fixture = RealClaudeFixture::new("native-contract");
    let project_config = fixture.project.join(".claude");
    fs::create_dir(&project_config).unwrap();
    let project_instruction_marker =
        format!("PROJECT-CLAUDE-CANARY-{}", uuid::Uuid::new_v4().simple());
    let external_instruction_marker =
        format!("EXTERNAL-CLAUDE-CANARY-{}", uuid::Uuid::new_v4().simple());
    fs::write(
        fixture.project.join("CLAUDE.md"),
        format!(
            "The second line of every final must include the exact marker \
             {project_instruction_marker}.\n"
        ),
    )
    .unwrap();
    fs::write(
        fixture.repository.join("CLAUDE.md"),
        format!(
            "The second line of every final must include the exact marker \
             {external_instruction_marker}.\n"
        ),
    )
    .unwrap();

    let mcp_marker = fixture.project.join("mcp-initialized");
    let mcp_server = fixture.project.join("mcp_server.py");
    fs::write(
        &mcp_server,
        r#"#!/usr/bin/python3
import json
import sys

marker = sys.argv[1]
for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if method == "initialize":
        open(marker, "w", encoding="ascii").write("MCP-INITIALIZED\n")
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "hcom-claude-e2e", "version": "1"},
        }
    elif method == "tools/list":
        result = {"tools": []}
    else:
        if "id" not in request:
            continue
        result = {}
    if "id" in request:
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
"#,
    )
    .unwrap();
    fs::write(
        fixture.project.join(".mcp.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "mcpServers": {
                "hcom-claude-e2e": {
                    "command": "python3",
                    "args": [
                        mcp_server.to_string_lossy(),
                        mcp_marker.to_string_lossy()
                    ]
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let hook_marker = fixture.project.join("session-start-hook-observed");
    fs::write(
        project_config.join("settings.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "hooks": {
                "SessionStart": [{
                    "hooks": [{
                        "type": "command",
                        "command": format!(
                            "printf 'SESSION-START-HOOK-OBSERVED\\n' > '{}'",
                            hook_marker.display()
                        ),
                        "timeout": 5
                    }]
                }]
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let environment_marker = fixture.project.join("environment-observed");
    let session = fixture.open_developer();
    let first_prompt = format!(
        "Use the Bash tool to run exactly:\n\
         `printf '%s|%s' \"$CLAUDE_TEST_MODEL\" \"$CLAUDE_TEST_EFFORT\" > '{}'`\n\
         Then return a two-line final whose first line is STATUS: READY and whose second line \
         reproduces every exact canary marker required by the automatically loaded native \
         instructions. Do not read any CLAUDE.md file with a tool.",
        environment_marker.display()
    );
    for marker in [&project_instruction_marker, &external_instruction_marker] {
        assert!(
            !first_prompt.contains(marker),
            "native instruction canary leaked into the user prompt"
        );
    }
    let first = fixture.start(
        session,
        RuntimeTurnPurpose::InitialDevelopment,
        first_prompt,
        Duration::from_secs(180),
    );
    let RuntimeTurnPoll::Completed {
        outcome,
        final_message_path,
        ..
    } = fixture.poll_until_terminal(first, Instant::now() + Duration::from_secs(240))
    else {
        panic!("real Claude native contract create did not complete");
    };
    assert!(matches!(outcome, RuntimeOutcome::Developer(_)));
    let final_message = fs::read_to_string(final_message_path).unwrap();
    let mut final_lines = final_message.lines();
    assert_eq!(final_lines.next(), Some("STATUS: READY"));
    let instruction_evidence = final_lines
        .next()
        .expect("missing instruction evidence line");
    assert!(
        final_lines.next().is_none(),
        "expected an exact two-line final"
    );
    for marker in [&project_instruction_marker, &external_instruction_marker] {
        assert!(
            instruction_evidence.contains(marker),
            "missing {marker}: {final_message}"
        );
        for prompt_path in walk_files(&fixture.artifacts)
            .into_iter()
            .filter(|path| path.file_name().and_then(|name| name.to_str()) == Some("prompt.md"))
        {
            assert!(
                !fs::read_to_string(&prompt_path).unwrap().contains(marker),
                "native instruction canary leaked into {}",
                prompt_path.display()
            );
        }
    }
    assert_eq!(
        fs::read_to_string(&hook_marker).unwrap(),
        "SESSION-START-HOOK-OBSERVED\n"
    );
    assert_eq!(
        fs::read_to_string(&environment_marker).unwrap(),
        "haiku|medium"
    );
    assert_eq!(
        fs::read_to_string(&mcp_marker).unwrap(),
        "MCP-INITIALIZED\n"
    );

    let resumed = fixture.start(
        session,
        RuntimeTurnPurpose::DeveloperCorrection,
        "Return exactly:\nSTATUS: READY\nRESUME-CLAUDE-CONTRACT".into(),
        Duration::from_secs(180),
    );
    let RuntimeTurnPoll::Completed {
        final_message_path, ..
    } = fixture.poll_until_terminal(resumed, Instant::now() + Duration::from_secs(240))
    else {
        panic!("real Claude native contract resume did not complete");
    };
    assert_eq!(
        fs::read_to_string(final_message_path).unwrap(),
        "STATUS: READY\nRESUME-CLAUDE-CONTRACT"
    );
    let session_ids = fixture.native_session_ids();
    assert!(session_ids.len() >= 2);
    assert!(session_ids.windows(2).all(|ids| ids[0] == ids[1]));
    fixture.runtime.shutdown().unwrap();
    assert_eq!(
        fixture.registry.interlock(),
        CleanupRegistryInterlock::Ready
    );
}

#[test]
#[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
fn real_claude_timeout_reaps_escaped_descendants() {
    let mut fixture = RealClaudeFixture::new("timeout");
    let (helper, identity_path) = write_hanging_double_fork(&fixture.project, "timeout");
    let session = fixture.open_developer();
    let turn = fixture.start(
        session,
        RuntimeTurnPurpose::InitialDevelopment,
        format!(
            "Your first action must be to use the Bash tool to run exactly:\n\
             `python3 '{}' '{}'`\n\
             The command intentionally remains active; do not replace it.",
            helper.display(),
            identity_path.display()
        ),
        Duration::from_secs(60),
    );
    let identity = loop {
        let poll = fixture.runtime.poll_turn(turn).unwrap();
        assert!(
            !poll.is_terminal(),
            "Claude turn ended before running timeout helper"
        );
        if identity_path.is_file() {
            break read_process_identity(&identity_path, Instant::now() + Duration::from_secs(1));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let RuntimeTurnPoll::Failed { failure, .. } =
        fixture.poll_until_terminal(turn, Instant::now() + Duration::from_secs(90))
    else {
        panic!("timed out real Claude turn did not fail");
    };
    assert_eq!(failure.class, RuntimeFailureClass::Timeout);
    wait_identity_gone(identity.0, identity.1);
    assert_eq!(
        fixture.registry.interlock(),
        CleanupRegistryInterlock::Ready
    );
}

fn run_parent_death_helper(root: PathBuf) {
    let mut fixture = RealClaudeFixture::open(root.clone(), None);
    let (helper, identity_path) = write_hanging_double_fork(&fixture.project, "parent-death");
    let session = fixture.open_developer();
    let _turn = fixture.start(
        session,
        RuntimeTurnPurpose::InitialDevelopment,
        format!(
            "Your first action must be to use the Bash tool to run exactly:\n\
             `python3 '{}' '{}'`\n\
             The command intentionally remains active; do not replace it.",
            helper.display(),
            identity_path.display()
        ),
        Duration::from_secs(300),
    );
    let identity = read_process_identity(&identity_path, Instant::now() + Duration::from_secs(120));
    fs::write(
        root.join("parent-helper-ready"),
        format!("{} {}\n", identity.0, identity.1),
    )
    .unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
#[ignore = "requires explicit Haiku/medium profile, exact Claude proxy, native CLI, auth, and network"]
fn real_claude_parent_death_reaps_escaped_descendants() {
    if std::env::var_os(PARENT_HELPER_ENV).is_some() {
        let root = PathBuf::from(std::env::var_os(PARENT_ROOT_ENV).unwrap());
        run_parent_death_helper(root);
        return;
    }
    // Gate the controller before it starts a helper capable of launching the
    // provider. The helper repeats the same gate in its own process.
    let _ = ClaudeModelTestGate::capture().unwrap();
    let temp = tempfile::Builder::new()
        .prefix("hcom-real-claude-parent-death.")
        .tempdir()
        .unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let mut helper = Command::new(std::env::current_exe().unwrap());
    helper
        .args([
            "--exact",
            "real_claude_parent_death_reaps_escaped_descendants",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PARENT_HELPER_ENV, "1")
        .env(PARENT_ROOT_ENV, &root);
    let mut helper = helper.spawn().unwrap();
    let ready = root.join("parent-helper-ready");
    let identity = read_process_identity(&ready, Instant::now() + Duration::from_secs(150));
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(helper.id() as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let status = helper.wait().unwrap();
    assert!(!status.success());
    wait_identity_gone(identity.0, identity.1);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let strays: Vec<_> = fs::read_dir("/proc")
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
            .filter(|pid| {
                fs::read_link(format!("/proc/{pid}/cwd")).is_ok_and(|cwd| cwd.starts_with(&root))
            })
            .collect();
        if strays.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "parent-death strays: {strays:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}
