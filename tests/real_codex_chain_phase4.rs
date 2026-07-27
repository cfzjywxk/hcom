//! Bounded, opt-in Phase 4 probe of the public foreground Codex-chain CLI.
//!
//! Run explicitly with:
//!   cargo test --test real_codex_chain_phase4 -- --ignored --nocapture --test-threads=1
//!
//! The outer half owns a private PTY, isolated HOME/HCOM_DIR/CODEX_HOME and a
//! localhost Responses mock. The inner half acts as a minimal job-control
//! shell: every public `hcom chain` command gets an exact foreground process
//! group, while the production hcom binary remains the persistent supervisor.

#![cfg(unix)]

mod support;

use std::fs;
use std::io::{self, Read as _};
use std::os::fd::{FromRawFd as _, RawFd};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use support::Hcom;
use support::codex_mock::{MockResponses, Reply, completed, created, message, shell_call, sse};

const DRIVER_ENV: &str = "HCOM_PHASE4_PUBLIC_DRIVER";
const SCENARIO_ENV: &str = "HCOM_PHASE4_PUBLIC_SCENARIO";
const HCOM_BIN_ENV: &str = "HCOM_PHASE4_PUBLIC_HCOM_BIN";
const SECRET_ENV: &str = "HCOM_PHASE4_PUBLIC_SECRET";
const NORMAL: &str = "normal";
const RECOVERY: &str = "recovery";
const TEST_NAME: &str = "bounded_public_codex_chain_and_recovery_probe";

#[derive(Clone, Debug)]
struct ChainSnapshot {
    id: String,
    version: i64,
}

#[derive(Clone, Debug)]
struct ProcessSnapshot {
    generation: i64,
    wrapper_pid: i32,
    child_pid: i32,
    materialized_at: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProbeReport {
    scenario: String,
    chain_id: String,
    source_native: String,
    target_native: String,
    max_live_codex_children: usize,
    automatic_sigkill_count: usize,
    generation_count: i64,
    recovery_attempt_count: i64,
    recovery_absence_count: i64,
    normal_cleanup_proved: bool,
    recovered_without_forged_cleanup: bool,
    concurrent_recovery_single_winner: bool,
    unknown_recovery_zero_spawn: bool,
    live_recovery_zero_spawn: bool,
    post_native_recovery_zero_spawn: bool,
}

struct ForegroundJob {
    child: Child,
    keeper: Child,
    pgid: i32,
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn wait_until<T>(description: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn latest_bundle_event(db_path: &Path) -> i64 {
    wait_until("bundle event", Duration::from_secs(10), || {
        let connection = Connection::open(db_path).ok()?;
        connection
            .query_row(
                "SELECT MAX(id) FROM events WHERE type = 'bundle'",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
    })
}

fn latest_handoff(db_path: &Path) -> (String, String, i64) {
    wait_until("terminal handoff", Duration::from_secs(10), || {
        let connection = Connection::open(db_path).ok()?;
        connection
            .query_row(
                "SELECT id, state, version FROM terminal_handoffs
                 ORDER BY created_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .ok()
            .flatten()
    })
}

fn start_mock(h: &Hcom, scenario: &str, secret: &str) -> MockResponses {
    let db_path = h.path().join("hcom.db");
    let source_gate = h.root_path().join("phase4-source-actions-allowed");
    let crash_gate = h.root_path().join("phase4-source-crashed");
    let hcom = shell_quote(env!("CARGO_BIN_EXE_hcom"));
    let secret = secret.to_string();
    let scenario = scenario.to_string();
    MockResponses::start(move |body: &str| {
        let has_output =
            |call_id: &str| body.contains("function_call_output") && body.contains(call_id);
        if has_output("CALL_P4_ACCEPT") {
            return Reply::Sse(sse(&[
                created("RESP_P4_TARGET_DONE"),
                message("ITEM_P4_TARGET_DONE", "PHASE4_TARGET_ACCEPTED"),
                completed("RESP_P4_TARGET_DONE"),
            ]));
        }
        if has_output("CALL_P4_INSPECT") {
            let (id, state, version) = latest_handoff(&db_path);
            assert_eq!(state, "awaiting_acceptance");
            let command = format!("{hcom} handoff accept {id} --version {version} --json");
            return Reply::Sse(sse(&[
                created("RESP_P4_ACCEPT"),
                shell_call("CALL_P4_ACCEPT", &command),
                completed("RESP_P4_ACCEPT"),
            ]));
        }
        if has_output("CALL_P4_COMMIT") {
            if scenario == RECOVERY {
                wait_until("source crash gate", Duration::from_secs(10), || {
                    crash_gate.is_file().then_some(())
                });
            }
            return Reply::Sse(sse(&[
                created("RESP_P4_SOURCE_DONE"),
                message("ITEM_P4_SOURCE_DONE", "PHASE4_SOURCE_COMMITTED"),
                completed("RESP_P4_SOURCE_DONE"),
            ]));
        }
        if has_output("CALL_P4_PREPARE") {
            let (id, state, version) = latest_handoff(&db_path);
            assert_eq!(state, "prepared");
            let command = format!("{hcom} handoff commit {id} --version {version} --json");
            return Reply::Sse(sse(&[
                created("RESP_P4_COMMIT"),
                shell_call("CALL_P4_COMMIT", &command),
                completed("RESP_P4_COMMIT"),
            ]));
        }
        if has_output("CALL_P4_CREATE") {
            let event = latest_bundle_event(&db_path);
            let command = format!("{hcom} handoff prepare --bundle-event {event} --json");
            return Reply::Sse(sse(&[
                created("RESP_P4_PREPARE"),
                shell_call("CALL_P4_PREPARE", &command),
                completed("RESP_P4_PREPARE"),
            ]));
        }
        if body.contains("Continue hcom handoff ho-") {
            let (id, state, version) = latest_handoff(&db_path);
            assert!(
                matches!(state.as_str(), "launching_target" | "awaiting_acceptance"),
                "unexpected target state {state}"
            );
            let command = format!("{hcom} handoff inspect {id} --version {version} --json");
            return Reply::Sse(sse(&[
                created("RESP_P4_INSPECT"),
                shell_call("CALL_P4_INSPECT", &command),
                completed("RESP_P4_INSPECT"),
            ]));
        }
        if body.contains("Start hcom chain tc-") {
            wait_until("source action gate", Duration::from_secs(10), || {
                source_gate.is_file().then_some(())
            });
            let command = format!(
                "{hcom} bundle create phase4-public --description {} \
                 --events 1 --files README.md --transcript 1:full --json",
                shell_quote(&secret)
            );
            return Reply::Sse(sse(&[
                created("RESP_P4_CREATE"),
                shell_call("CALL_P4_CREATE", &command),
                completed("RESP_P4_CREATE"),
            ]));
        }
        Reply::Status(500)
    })
    .expect("start localhost Responses mock")
}

fn setup_workspace(h: &Hcom, instruction_secret: &str) {
    fs::write(
        h.workspace.join("AGENTS.md"),
        format!("Phase 4 isolated instruction {instruction_secret}\n"),
    )
    .unwrap();
    fs::write(h.workspace.join("README.md"), "phase4 public chain\n").unwrap();
    for args in [
        &["init", "-b", "main"][..],
        &["config", "user.name", "hcom phase4"][..],
        &["config", "user.email", "phase4@example.invalid"][..],
        &["add", "AGENTS.md", "README.md"][..],
        &["commit", "-m", "fixture"][..],
    ] {
        let output = Command::new("git")
            .arg("-C")
            .arg(&h.workspace)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn trust_workspace(h: &Hcom) {
    let workspace = fs::canonicalize(&h.workspace)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "\\\\");
    let mut config = fs::read_to_string(h.codex_home.join("config.toml")).unwrap();
    config.push_str(&format!(
        "\n[projects.\"{workspace}\"]\ntrust_level = \"trusted\"\n"
    ));
    fs::write(h.codex_home.join("config.toml"), config).unwrap();
}

fn open_outer_pty() -> (RawFd, RawFd) {
    let mut master = -1;
    let mut slave = -1;
    let size = libc::winsize {
        ws_row: 40,
        ws_col: 120,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: openpty initializes both descriptor slots.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        },
        0
    );
    (master, slave)
}

fn close_fd(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: callers transfer or discard each raw descriptor exactly once.
        unsafe {
            libc::close(fd);
        }
    }
}

fn run_outer_scenario(scenario: &str) {
    let h = Hcom::new();
    let version = h.codex_version().expect("installed Codex is required");
    assert!(
        version.split_whitespace().any(|part| part == "0.145.0"),
        "Phase 4 probe requires Codex 0.145.0, found {version}"
    );
    let secret = format!("PHASE4_BUNDLE_SECRET_{}_{}", scenario, std::process::id());
    let instruction_secret = format!(
        "PHASE4_WORKSPACE_INSTRUCTION_{}_X{}",
        scenario,
        std::process::id()
    );
    setup_workspace(&h, &instruction_secret);
    let mock = start_mock(&h, scenario, &secret);
    h.prepare_codex_config(&mock.base_url());
    trust_workspace(&h);

    let (master, slave) = open_outer_pty();
    let mut command = h.external_cmd(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            TEST_NAME,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(DRIVER_ENV, "1")
        .env(SCENARIO_ENV, scenario)
        .env(HCOM_BIN_ENV, env!("CARGO_BIN_EXE_hcom"))
        .env(SECRET_ENV, &secret)
        .env("RUST_BACKTRACE", "0")
        .current_dir(&h.workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: only async-signal-safe libc operations run between fork and exec.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() == -1
                || libc::ioctl(slave, libc::TIOCSCTTY, 0) == -1
                || libc::dup2(slave, libc::STDIN_FILENO) == -1
                || libc::dup2(slave, libc::STDOUT_FILENO) == -1
                || libc::dup2(slave, libc::STDERR_FILENO) == -1
            {
                return Err(io::Error::last_os_error());
            }
            if slave > libc::STDERR_FILENO {
                libc::close(slave);
            }
            libc::close(master);
            Ok(())
        });
    }
    let mut driver = command.spawn().expect("spawn private-PTY driver");
    let driver_pid = driver.id() as i32;
    h.track_cleanup_pid(i64::from(driver_pid));
    close_fd(slave);
    let output_reader = std::thread::spawn(move || {
        // SAFETY: this thread exclusively owns the PTY master.
        let mut file = unsafe { fs::File::from_raw_fd(master) };
        let mut output = Vec::new();
        let mut buffer = [0u8; 8192];
        while let Ok(count) = file.read(&mut buffer) {
            if count == 0 {
                break;
            }
            if output.len() < 2 * 1024 * 1024 {
                let keep = count.min(2 * 1024 * 1024 - output.len());
                output.extend_from_slice(&buffer[..keep]);
            }
        }
        output
    });

    let deadline = Instant::now() + Duration::from_secs(90);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = driver.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            // SAFETY: the driver is a private session/process-group leader.
            unsafe {
                libc::kill(-driver_pid, libc::SIGHUP);
            }
            timed_out = true;
            break driver.wait().unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let terminal = output_reader.join().unwrap();
    let terminal_text = String::from_utf8_lossy(&terminal);
    assert!(
        !timed_out,
        "Phase 4 {scenario} driver timed out:\n{terminal_text}"
    );
    assert!(
        status.success(),
        "Phase 4 {scenario} driver failed ({status:?}):\n{terminal_text}"
    );

    let report_path = h.root_path().join(format!("phase4-{scenario}-report.json"));
    let report: ProbeReport =
        serde_json::from_slice(&fs::read(&report_path).expect("read driver report")).unwrap();
    assert_eq!(report.scenario, scenario);
    assert_ne!(report.source_native, report.target_native);
    assert_eq!(report.max_live_codex_children, 1);
    assert_eq!(report.automatic_sigkill_count, 0);
    assert_eq!(
        report.generation_count,
        if scenario == NORMAL { 2 } else { 3 }
    );
    assert!(report.live_recovery_zero_spawn);
    assert!(report.post_native_recovery_zero_spawn);
    if scenario == NORMAL {
        assert!(report.normal_cleanup_proved);
        assert_eq!(report.recovery_attempt_count, 0);
    } else {
        assert!(report.recovered_without_forged_cleanup);
        assert!(report.concurrent_recovery_single_winner);
        assert!(report.unknown_recovery_zero_spawn);
        assert_eq!(report.recovery_attempt_count, 1);
        assert_eq!(report.recovery_absence_count, 5);
    }

    assert!(mock.unexpected().is_empty(), "{:?}", mock.unexpected());
    assert!(
        mock.transport_errors().is_empty(),
        "{:?}",
        mock.transport_errors()
    );
    let requests = mock.requests();
    let target_initial = requests
        .iter()
        .find(|body| {
            body.contains("Continue hcom handoff ho-") && !body.contains("function_call_output")
        })
        .expect("fresh public target request");
    assert!(!target_initial.contains(&secret));
    assert!(
        target_initial.contains(&instruction_secret),
        "fresh Codex must retain its native workspace instructions"
    );
    assert_eq!(
        last_user_message_texts(target_initial),
        vec![format!("Continue hcom handoff {}", report_handoff_id(&h))]
    );
    let mut remaining = terminal.as_slice();
    while let Some(start) = remaining.windows(4).position(|window| window == b"\x1b]0;") {
        remaining = &remaining[start + 4..];
        let end = remaining
            .iter()
            .position(|byte| *byte == 0x07)
            .expect("bounded OSC title terminator");
        let title = &remaining[..end];
        for private in [&secret, &instruction_secret] {
            assert!(
                !title
                    .windows(private.len())
                    .any(|window| window == private.as_bytes()),
                "private value leaked to terminal title"
            );
        }
        remaining = &remaining[end + 1..];
    }
    assert!(terminal_text.contains("hcom codex g1"));
    assert!(terminal_text.contains(if scenario == NORMAL {
        "hcom codex g2"
    } else {
        "hcom codex g3"
    }));
    assert!(
        terminal
            .windows(b"\x1b[23;0t".len())
            .any(|window| window == b"\x1b[23;0t"),
        "terminal title stack was not restored"
    );
    let db_path = h.path().join("hcom.db");
    let (audit, durable_private) = durable_privacy_values(&db_path);
    let logs = collect_text_files(h.path());
    for private in [
        secret.as_str(),
        instruction_secret.as_str(),
        report.source_native.as_str(),
        report.target_native.as_str(),
    ] {
        assert!(!audit.contains(private), "private value leaked to audit");
        assert!(!logs.contains(private), "private value leaked to hcom logs");
    }
    for (label, private) in durable_private {
        assert!(!logs.contains(&private), "{label} leaked to hcom logs");
    }
}

fn last_user_message_texts(body: &str) -> Vec<String> {
    let value: serde_json::Value = serde_json::from_str(body).expect("valid Responses request");
    value
        .get("input")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| {
            items.iter().rev().find(|item| {
                item.get("type").and_then(serde_json::Value::as_str) == Some("message")
                    && item.get("role").and_then(serde_json::Value::as_str) == Some("user")
            })
        })
        .and_then(|message| message.get("content"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|content| {
            content.get("type").and_then(serde_json::Value::as_str) == Some("input_text")
        })
        .map(|content| {
            content
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

fn report_handoff_id(h: &Hcom) -> String {
    let connection = Connection::open(h.path().join("hcom.db")).unwrap();
    connection
        .query_row(
            "SELECT id FROM terminal_handoffs ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn collect_text_files(root: &Path) -> String {
    fn visit(path: &Path, output: &mut String) {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return;
        };
        if metadata.is_dir() {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    visit(&entry.path(), output);
                }
            }
        } else if metadata.is_file()
            && metadata.len() <= 1024 * 1024
            && let Ok(value) = fs::read_to_string(path)
        {
            output.push_str(&value);
        }
    }
    let mut output = String::new();
    visit(root, &mut output);
    output
}

fn durable_privacy_values(db_path: &Path) -> (String, Vec<(&'static str, String)>) {
    let connection = Connection::open(db_path).unwrap();
    let audit: String = connection
        .query_row(
            "SELECT COALESCE(group_concat(
                 chain_id || object_kind || object_id || from_state ||
                 to_state || actor_instance_name || actor_hcom_session_id ||
                 actor_process_id || actor_process_birth_identity ||
                 actor_role || action || request_hash, '|'
             ), '') FROM terminal_transition_audit",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut private = Vec::new();
    for (label, query) in [
        (
            "launch nonce",
            "SELECT launch_nonce FROM terminal_generations",
        ),
        (
            "native session",
            "SELECT native_session_id FROM terminal_generations
             WHERE native_session_id IS NOT NULL",
        ),
        (
            "process birth",
            "SELECT process_birth_identity FROM terminal_generations
             WHERE process_birth_identity IS NOT NULL",
        ),
        (
            "materialized wrapper birth",
            "SELECT wrapper_birth_identity FROM terminal_generation_processes",
        ),
        (
            "materialized child birth",
            "SELECT child_birth_identity FROM terminal_generation_processes",
        ),
        (
            "supervisor birth",
            "SELECT supervisor_process_birth_identity FROM terminal_chains",
        ),
        (
            "prepare supervisor birth",
            "SELECT supervisor_process_birth_identity
             FROM terminal_generation_prepare_intents",
        ),
        (
            "recovery supervisor birth",
            "SELECT supervisor_process_birth_identity
             FROM terminal_recovery_attempts",
        ),
        (
            "target validation",
            "SELECT target_validation_token FROM terminal_handoffs
             WHERE target_validation_token IS NOT NULL",
        ),
    ] {
        let mut statement = connection.prepare(query).unwrap();
        private.extend(
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .flatten()
                .filter(|value| !value.is_empty())
                .map(|value| (label, value)),
        );
    }
    private.sort();
    private.dedup();
    (audit, private)
}

fn chain_snapshot(db_path: &Path) -> Option<ChainSnapshot> {
    let connection = Connection::open(db_path).ok()?;
    connection
        .query_row(
            "SELECT id, version
             FROM terminal_chains ORDER BY created_at LIMIT 1",
            [],
            |row| {
                Ok(ChainSnapshot {
                    id: row.get(0)?,
                    version: row.get(1)?,
                })
            },
        )
        .optional()
        .ok()
        .flatten()
}

fn process_snapshot(db_path: &Path, generation: i64) -> Option<ProcessSnapshot> {
    let connection = Connection::open(db_path).ok()?;
    connection
        .query_row(
            "SELECT generation, wrapper_pid, child_pid, materialized_at
             FROM terminal_generation_processes
             WHERE generation = ?1",
            [generation],
            |row| {
                Ok(ProcessSnapshot {
                    generation: row.get(0)?,
                    wrapper_pid: row.get(1)?,
                    child_pid: row.get(2)?,
                    materialized_at: row.get(3)?,
                })
            },
        )
        .optional()
        .ok()
        .flatten()
}

fn generation_native(db_path: &Path, generation: i64) -> Option<String> {
    let connection = Connection::open(db_path).ok()?;
    connection
        .query_row(
            "SELECT native_session_id FROM terminal_generations
             WHERE generation = ?1",
            [generation],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .ok()
        .flatten()
        .flatten()
}

fn current_supervisor_pid(db_path: &Path, chain_id: &str) -> Option<i32> {
    let connection = Connection::open(db_path).ok()?;
    let recovered = connection
        .query_row(
            "SELECT supervisor_pid FROM terminal_recovery_attempts
             WHERE chain_id = ?1 AND state != 'manual'
             ORDER BY sequence DESC LIMIT 1",
            [chain_id],
            |row| row.get::<_, i32>(0),
        )
        .optional()
        .ok()
        .flatten();
    recovered.or_else(|| {
        connection
            .query_row(
                "SELECT supervisor_pid FROM terminal_chains WHERE id = ?1",
                [chain_id],
                |row| row.get::<_, Option<i32>>(0),
            )
            .optional()
            .ok()
            .flatten()
            .flatten()
    })
}

fn process_live(pid: i32) -> bool {
    if pid <= 1 {
        return false;
    }
    // SAFETY: signal zero performs a read-only liveness probe.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn task_complete_for_generation(db_path: &Path, generation: i64) -> bool {
    let Ok(connection) = Connection::open(db_path) else {
        return false;
    };
    let transcript = connection
        .query_row(
            "SELECT i.transcript_path
             FROM terminal_generations g
             JOIN instances i ON i.name = g.instance_name
             WHERE g.generation = ?1",
            [generation],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .ok()
        .flatten();
    transcript.is_some_and(|path| {
        fs::read_to_string(path).is_ok_and(|body| {
            body.lines().any(|line| {
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|value| {
                        value
                            .pointer("/payload/type")
                            .and_then(serde_json::Value::as_str)
                            .map(|kind| kind == "task_complete")
                    })
                    .unwrap_or(false)
            })
        })
    })
}

fn hcom_command(hcom_bin: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(hcom_bin);
    command.args(args);
    for key in [DRIVER_ENV, SCENARIO_ENV, HCOM_BIN_ENV, SECRET_ENV] {
        command.env_remove(key);
    }
    command
}

fn traced_hcom_command(hcom_bin: &Path, trace: &Path, args: &[&str]) -> Command {
    let mut command = Command::new("strace");
    command
        .args([
            "-qq",
            "-I",
            "1",
            "-e",
            "trace=kill,tgkill,tkill,wait4,waitid",
            "-o",
        ])
        .arg(trace)
        .arg(hcom_bin)
        .args(args);
    for key in [DRIVER_ENV, SCENARIO_ENV, HCOM_BIN_ENV, SECRET_ENV] {
        command.env_remove(key);
    }
    command
}

fn spawn_in_group(mut command: Command, pgid: i32) -> Child {
    command
        .process_group(pgid)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command.spawn().expect("spawn foreground group member")
}

fn set_foreground(pgid: i32) {
    // SAFETY: fd 0 is the driver's controlling terminal.
    assert_eq!(unsafe { libc::tcsetpgrp(libc::STDIN_FILENO, pgid) }, 0);
}

fn create_foreground_group() -> (Child, i32) {
    let mut command = Command::new("sleep");
    command
        .arg("600")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let keeper = command.spawn().expect("spawn foreground group keeper");
    let pgid = keeper.id() as i32;
    set_foreground(pgid);
    (keeper, pgid)
}

fn spawn_foreground(command: Command) -> ForegroundJob {
    let (keeper, pgid) = create_foreground_group();
    let child = spawn_in_group(command, pgid);
    ForegroundJob {
        child,
        keeper,
        pgid,
    }
}

fn regain_driver_foreground() {
    // SAFETY: the driver is its own process-group leader.
    set_foreground(unsafe { libc::getpgrp() });
}

fn wait_child(child: &mut Child, description: &str) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "{description} did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn stop_keeper(keeper: &mut Child) {
    let pid = keeper.id() as i32;
    // SAFETY: pid belongs to the exact private helper child.
    let result = unsafe { libc::kill(pid, libc::SIGTERM) };
    assert!(
        result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
        "failed to stop foreground group keeper"
    );
    let _ = wait_child(keeper, "foreground group keeper");
}

fn run_in_existing_foreground_group(hcom_bin: &Path, pgid: i32, args: &[&str]) -> ExitStatus {
    let mut command = hcom_command(hcom_bin, args);
    command
        .process_group(pgid)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().expect("spawn same-terminal command");
    wait_child(&mut child, "same-terminal command")
}

fn run_in_new_foreground_group(hcom_bin: &Path, args: &[&str]) -> ExitStatus {
    let mut job = spawn_foreground(hcom_command(hcom_bin, args));
    let status = wait_child(&mut job.child, "foreground command");
    regain_driver_foreground();
    stop_keeper(&mut job.keeper);
    wait_job_group_absent(job.pgid);
    status
}

fn count_rows(db_path: &Path, table: &str) -> i64 {
    let connection = Connection::open(db_path).unwrap();
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn inspect_child_argv(process: &ProcessSnapshot, expected_prompt: &str, secret: &str) {
    assert!(process.generation >= 1);
    let cmdline = fs::read(format!("/proc/{}/cmdline", process.child_pid)).unwrap();
    let args: Vec<&[u8]> = cmdline
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .collect();
    assert!(
        args.contains(&expected_prompt.as_bytes()),
        "missing exact prompt in argv"
    );
    for forbidden in [b"resume".as_slice(), b"fork", b"--last"] {
        assert!(!args.contains(&forbidden));
    }
    assert!(
        !cmdline
            .windows(secret.len())
            .any(|window| window == secret.as_bytes())
    );
}

fn sample_max_live(db_path: &Path, max_live: &mut usize) {
    let Ok(connection) = Connection::open(db_path) else {
        return;
    };
    let Ok(mut statement) =
        connection.prepare("SELECT child_pid FROM terminal_generation_processes")
    else {
        return;
    };
    let live = statement
        .query_map([], |row| row.get::<_, i32>(0))
        .map(|rows| rows.flatten().filter(|pid| process_live(*pid)).count())
        .unwrap_or_default();
    *max_live = (*max_live).max(live);
}

fn wait_with_sampling<T>(
    db_path: &Path,
    max_live: &mut usize,
    description: &str,
    timeout: Duration,
    mut probe: impl FnMut() -> Option<T>,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        sample_max_live(db_path, max_live);
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn corrupt_wrapper_birth_for_unknown(db_path: &Path, chain_id: &str) -> (i64, String) {
    let connection = Connection::open(db_path).unwrap();
    let (generation, original): (i64, String) = connection
        .query_row(
            "SELECT generation, wrapper_birth_identity
             FROM terminal_generation_processes
             WHERE chain_id = ?1 ORDER BY generation DESC LIMIT 1",
            [chain_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    connection
        .execute_batch("DROP TRIGGER terminal_generation_processes_no_update")
        .unwrap();
    connection
        .execute(
            "UPDATE terminal_generation_processes
             SET wrapper_birth_identity = 'malformed-test-evidence'
             WHERE chain_id = ?1 AND generation = ?2",
            params![chain_id, generation],
        )
        .unwrap();
    (generation, original)
}

fn restore_wrapper_birth(db_path: &Path, chain_id: &str, generation: i64, original: &str) {
    let connection = Connection::open(db_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER terminal_generation_processes_no_update")
        .unwrap();
    connection
        .execute(
            "UPDATE terminal_generation_processes
             SET wrapper_birth_identity = ?1
             WHERE chain_id = ?2 AND generation = ?3",
            params![original, chain_id, generation],
        )
        .unwrap();
}

fn trace_sigkill_calls(root: &Path) -> usize {
    let mut count = 0;
    let Ok(entries) = fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("phase4-trace-") {
            continue;
        }
        if let Ok(body) = fs::read_to_string(entry.path()) {
            count += body
                .lines()
                .filter(|line| line.contains("kill(") && line.contains("SIGKILL"))
                .count();
        }
    }
    count
}

fn normal_cleanup_proved(db_path: &Path, target: &ProcessSnapshot) -> bool {
    let connection = Connection::open(db_path).unwrap();
    connection
        .query_row(
            "SELECT waitpid_reaped = 1
                    AND inject_cleanup_succeeded = 1
                    AND delivery_cleanup_succeeded = 1
                    AND pty_cleanup_succeeded = 1
                    AND screen_cleanup_succeeded = 1
                    AND write_queue_cleanup_succeeded = 1
                    AND cleanup_completed_at IS NOT NULL
                    AND cleanup_completed_at <= ?1
             FROM terminal_handoffs ORDER BY created_at DESC LIMIT 1",
            [target.materialized_at],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
}

fn recovered_without_forged_cleanup(db_path: &Path) -> bool {
    let connection = Connection::open(db_path).unwrap();
    connection
        .query_row(
            "SELECT waitpid_reaped IS NULL AND cleanup_completed_at IS NULL
             FROM terminal_handoffs ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
}

fn wait_job_group_absent(pgid: i32) {
    wait_until(
        "old foreground process group absence",
        Duration::from_secs(10),
        || {
            // SAFETY: signal zero is a read-only process-group probe.
            let result = unsafe { libc::kill(-pgid, 0) };
            (result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH))
                .then_some(())
        },
    );
}

fn run_driver() {
    // A job-control shell must not stop itself while moving the foreground PG.
    // SAFETY: the test process is the private PTY session leader.
    unsafe {
        libc::signal(libc::SIGTTOU, libc::SIG_IGN);
        libc::signal(libc::SIGTTIN, libc::SIG_IGN);
        libc::signal(libc::SIGTSTP, libc::SIG_IGN);
    }
    let scenario = std::env::var(SCENARIO_ENV).unwrap();
    let hcom_bin = PathBuf::from(std::env::var_os(HCOM_BIN_ENV).unwrap());
    let secret = std::env::var(SECRET_ENV).unwrap();
    let hcom_dir = PathBuf::from(std::env::var_os("HCOM_DIR").unwrap());
    let db_path = hcom_dir.join("hcom.db");
    let root = hcom_dir.parent().unwrap().to_path_buf();
    let start_args = [
        "chain",
        "codex",
        "--model",
        "gpt-5.5",
        "--reasoning",
        "high",
        "--sandbox",
        "danger-full-access",
        "--approval",
        "never",
    ];
    let trace = root.join(format!("phase4-trace-{scenario}-start"));
    let mut start_job = spawn_foreground(traced_hcom_command(&hcom_bin, &trace, &start_args));
    let mut max_live = 0usize;
    let chain = wait_with_sampling(
        &db_path,
        &mut max_live,
        "public chain reservation",
        Duration::from_secs(20),
        || chain_snapshot(&db_path),
    );
    let source = wait_with_sampling(
        &db_path,
        &mut max_live,
        "source process materialization",
        Duration::from_secs(20),
        || process_snapshot(&db_path, 1),
    );
    let source_native = wait_with_sampling(
        &db_path,
        &mut max_live,
        "source SessionStart",
        Duration::from_secs(30),
        || generation_native(&db_path, 1),
    );
    inspect_child_argv(&source, &format!("Start hcom chain {}", chain.id), &secret);

    let generations_before_live = count_rows(&db_path, "terminal_generations");
    let attempts_before_live = count_rows(&db_path, "terminal_recovery_attempts");
    let live_status = run_in_existing_foreground_group(
        &hcom_bin,
        start_job.pgid,
        &[
            "chain",
            "recover",
            &chain.id,
            "--version",
            &chain.version.to_string(),
            "--json",
        ],
    );
    assert!(!live_status.success());
    let live_recovery_zero_spawn = count_rows(&db_path, "terminal_generations")
        == generations_before_live
        && count_rows(&db_path, "terminal_recovery_attempts") == attempts_before_live;
    fs::write(root.join("phase4-source-actions-allowed"), b"1").unwrap();

    let (
        target_native,
        target,
        concurrent_recovery_single_winner,
        unknown_recovery_zero_spawn,
        recovery_job_pgid,
        recovery_children,
        mut recovery_keeper,
    ) = if scenario == NORMAL {
        let target_native = wait_with_sampling(
            &db_path,
            &mut max_live,
            "normal target SessionStart",
            Duration::from_secs(40),
            || generation_native(&db_path, 2),
        );
        let target = process_snapshot(&db_path, 2).unwrap();
        inspect_child_argv(
            &target,
            &format!("Continue hcom handoff {}", latest_handoff(&db_path).0),
            &secret,
        );
        wait_with_sampling(
            &db_path,
            &mut max_live,
            "normal target acceptance",
            Duration::from_secs(30),
            || {
                let connection = Connection::open(&db_path).ok()?;
                connection
                    .query_row(
                        "SELECT state = 'accepted' FROM terminal_handoffs
                         ORDER BY created_at DESC LIMIT 1",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .ok()
                    .filter(|accepted| *accepted)
            },
        );
        wait_with_sampling(
            &db_path,
            &mut max_live,
            "normal target task_complete",
            Duration::from_secs(30),
            || task_complete_for_generation(&db_path, 2).then_some(()),
        );
        (
            target_native,
            target,
            false,
            false,
            start_job.pgid,
            Vec::new(),
            None,
        )
    } else {
        wait_with_sampling(
            &db_path,
            &mut max_live,
            "committed source handoff",
            Duration::from_secs(30),
            || {
                let (_, state, _) = latest_handoff(&db_path);
                (state == "committed").then_some(())
            },
        );
        let supervisor = current_supervisor_pid(&db_path, &chain.id).unwrap();
        // SAFETY: this is the exact durable supervisor in the isolated fixture.
        assert_eq!(unsafe { libc::kill(supervisor, libc::SIGKILL) }, 0);
        let crashed = wait_child(&mut start_job.child, "crashed public supervisor observer");
        assert!(!crashed.success());
        regain_driver_foreground();
        stop_keeper(&mut start_job.keeper);
        fs::write(root.join("phase4-source-crashed"), b"1").unwrap();
        wait_job_group_absent(start_job.pgid);
        wait_until("source wrapper absence", Duration::from_secs(10), || {
            (!process_live(source.wrapper_pid) && !process_live(source.child_pid)).then_some(())
        });

        let status =
            run_in_new_foreground_group(&hcom_bin, &["chain", "status", &chain.id, "--json"]);
        assert!(status.success());
        let after_crash = chain_snapshot(&db_path).unwrap();
        let generations_before_unknown = count_rows(&db_path, "terminal_generations");
        let attempts_before_unknown = count_rows(&db_path, "terminal_recovery_attempts");
        let (corrupt_generation, original_birth) =
            corrupt_wrapper_birth_for_unknown(&db_path, &chain.id);
        let unknown = run_in_new_foreground_group(
            &hcom_bin,
            &[
                "chain",
                "recover",
                &chain.id,
                "--version",
                &after_crash.version.to_string(),
                "--json",
            ],
        );
        assert!(!unknown.success());
        let unknown_recovery_zero_spawn = count_rows(&db_path, "terminal_generations")
            == generations_before_unknown
            && count_rows(&db_path, "terminal_recovery_attempts") == attempts_before_unknown;
        restore_wrapper_birth(&db_path, &chain.id, corrupt_generation, &original_birth);

        let trace_a = root.join("phase4-trace-recovery-a");
        let trace_b = root.join("phase4-trace-recovery-b");
        let recover_args = [
            "chain",
            "recover",
            &chain.id,
            "--version",
            &after_crash.version.to_string(),
            "--json",
        ];
        let (keeper, recovery_pgid) = create_foreground_group();
        let child_a = spawn_in_group(
            traced_hcom_command(&hcom_bin, &trace_a, &recover_args),
            recovery_pgid,
        );
        let child_b = spawn_in_group(
            traced_hcom_command(&hcom_bin, &trace_b, &recover_args),
            recovery_pgid,
        );
        let mut recovery_children = vec![child_a, child_b];

        let target_native = wait_with_sampling(
            &db_path,
            &mut max_live,
            "recovered target SessionStart",
            Duration::from_secs(40),
            || generation_native(&db_path, 3),
        );
        let target = process_snapshot(&db_path, 3).unwrap();
        inspect_child_argv(
            &target,
            &format!("Continue hcom handoff {}", latest_handoff(&db_path).0),
            &secret,
        );
        wait_with_sampling(
            &db_path,
            &mut max_live,
            "recovered target acceptance",
            Duration::from_secs(30),
            || {
                let connection = Connection::open(&db_path).ok()?;
                connection
                    .query_row(
                        "SELECT state = 'accepted' FROM terminal_handoffs
                         ORDER BY created_at DESC LIMIT 1",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .ok()
                    .filter(|accepted| *accepted)
            },
        );
        wait_with_sampling(
            &db_path,
            &mut max_live,
            "recovered target task_complete",
            Duration::from_secs(30),
            || task_complete_for_generation(&db_path, 3).then_some(()),
        );
        let exited = recovery_children
            .iter_mut()
            .filter_map(|child| child.try_wait().ok().flatten())
            .count();
        assert_eq!(exited, 1, "exactly one concurrent recover must lose");
        let concurrent_recovery_single_winner = count_rows(&db_path, "terminal_recovery_attempts")
            == 1
            && count_rows(&db_path, "terminal_generations") == 3;
        (
            target_native,
            target,
            concurrent_recovery_single_winner,
            unknown_recovery_zero_spawn,
            recovery_pgid,
            recovery_children,
            Some(keeper),
        )
    };

    let current = chain_snapshot(&db_path).unwrap();
    let generations_before_post_native = count_rows(&db_path, "terminal_generations");
    let attempts_before_post_native = count_rows(&db_path, "terminal_recovery_attempts");
    let post_native = run_in_existing_foreground_group(
        &hcom_bin,
        recovery_job_pgid,
        &[
            "chain",
            "recover",
            &chain.id,
            "--version",
            &current.version.to_string(),
            "--json",
        ],
    );
    assert!(!post_native.success());
    let post_native_recovery_zero_spawn = count_rows(&db_path, "terminal_generations")
        == generations_before_post_native
        && count_rows(&db_path, "terminal_recovery_attempts") == attempts_before_post_native;

    let supervisor = current_supervisor_pid(&db_path, &chain.id).unwrap();
    // SAFETY: this is the exact current supervisor and SIGHUP exercises its
    // bounded outer-terminal shutdown path.
    assert_eq!(unsafe { libc::kill(supervisor, libc::SIGHUP) }, 0);
    if scenario == NORMAL {
        let status = wait_child(&mut start_job.child, "normal public supervisor");
        assert!(status.success(), "{status:?}");
        regain_driver_foreground();
        stop_keeper(&mut start_job.keeper);
    } else {
        for mut child in recovery_children {
            let _ = wait_child(&mut child, "recovery contender");
        }
        regain_driver_foreground();
        stop_keeper(
            recovery_keeper
                .as_mut()
                .expect("recovery foreground group keeper"),
        );
    }
    wait_job_group_absent(recovery_job_pgid);
    wait_until("target process cleanup", Duration::from_secs(10), || {
        (!process_live(target.wrapper_pid) && !process_live(target.child_pid)).then_some(())
    });
    sample_max_live(&db_path, &mut max_live);

    let generation_count = count_rows(&db_path, "terminal_generations");
    let recovery_attempt_count = count_rows(&db_path, "terminal_recovery_attempts");
    let recovery_absence_count = count_rows(&db_path, "terminal_recovery_absence_evidence");
    let report = ProbeReport {
        scenario: scenario.clone(),
        chain_id: chain.id,
        source_native,
        target_native,
        max_live_codex_children: max_live,
        automatic_sigkill_count: trace_sigkill_calls(&root),
        generation_count,
        recovery_attempt_count,
        recovery_absence_count,
        normal_cleanup_proved: scenario == NORMAL && normal_cleanup_proved(&db_path, &target),
        recovered_without_forged_cleanup: scenario == RECOVERY
            && recovered_without_forged_cleanup(&db_path),
        concurrent_recovery_single_winner,
        unknown_recovery_zero_spawn,
        live_recovery_zero_spawn,
        post_native_recovery_zero_spawn,
    };
    fs::write(
        root.join(format!("phase4-{scenario}-report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}

#[test]
#[ignore = "bounded public Codex 0.145.0 normal and recovery probe"]
#[serial_test::serial]
fn bounded_public_codex_chain_and_recovery_probe() {
    if std::env::var_os(DRIVER_ENV).is_some() {
        run_driver();
        return;
    }
    run_outer_scenario(NORMAL);
    run_outer_scenario(RECOVERY);
}
