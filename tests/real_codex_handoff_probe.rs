//! Bounded, opt-in Phase 2 characterization of the installed Codex CLI.
//!
//! Run explicitly with:
//!   cargo test --test real_codex_handoff_probe -- --ignored --nocapture --test-threads=1
//!
//! This launches exactly one real Codex under the ordinary hcom PTY in an
//! isolated HCOM_DIR/CODEX_HOME and points it at a localhost Responses mock.
//! It does not install a production handoff hook, launch a successor, or
//! replace the installed hcom binary.

#![cfg(unix)]

mod support;

use std::fs;
use std::io::{self, Read};
use std::os::fd::RawFd;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use rusqlite::OptionalExtension;
use serde_json::Value;
use support::Hcom;
use support::codex_mock::{MockResponses, Reply, completed, created, message, sse};

#[test]
#[ignore = "bounded local characterization of the installed Codex CLI"]
fn bounded_sigterm_exit_reap_and_delivery_context_probe() {
    let h = Hcom::new();
    let codex_version = h.codex_version().expect("installed Codex is required");
    assert!(
        codex_version.contains("0.145.0"),
        "probe baseline requires Codex 0.145.0, found {codex_version}"
    );
    let token = format!("HCOM_PHASE2_PROBE_{}", std::process::id());
    let response_token = token.clone();
    let mock = MockResponses::start(move |body: &str| {
        if body.contains(&response_token) {
            Reply::Sse(sse(&[
                created("RESP_PHASE2"),
                message(
                    "ITEM_PHASE2",
                    &format!("PHASE2_PROBE_COMPLETE {response_token}"),
                ),
                completed("RESP_PHASE2"),
            ]))
        } else {
            Reply::Status(500)
        }
    })
    .expect("start localhost Responses mock");
    h.prepare_codex_config(&mock.base_url());
    let (config_code, config_stdout, config_stderr) =
        h.run(["config", "codex_sandbox_mode", "danger-full-access"]);
    assert_eq!(
        config_code, 0,
        "sandbox config failed: stdout={config_stdout} stderr={config_stderr}"
    );
    let (trust_code, trust_stdout, trust_stderr) =
        h.run(["config", "auto_trust_workspace", "true"]);
    assert_eq!(
        trust_code, 0,
        "workspace trust fixture failed: stdout={trust_stdout} stderr={trust_stderr}"
    );

    let mut outer_master = -1;
    let mut outer_slave = -1;
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
                &mut outer_master,
                &mut outer_slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        },
        0
    );
    let strace_prefix = h.root_path().join("phase2-signal-trace");
    let mut command = h.external_cmd("strace");
    command
        // Separate files let the probe inspect only the exact DB-pinned PTY
        // wrapper for signal policy while independently proving its parent
        // performed the matching wait.
        .args([
            "-ff",
            "-qq",
            // `-o FILE PROG` otherwise selects `-I 3`, which blocks SIGINT
            // forever and makes the bounded observer teardown depend on every
            // short-lived traced descendant exiting first.
            "-I",
            "1",
            "-e",
            "trace=kill,tgkill,tkill,wait4,waitid",
            "-o",
        ])
        .arg(&strace_prefix)
        .arg(env!("CARGO_BIN_EXE_hcom"))
        .args(["codex", "--run-here", "--dir"])
        .arg(&h.workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: the closure performs only async-signal-safe libc operations in
    // the child between fork and exec.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            if libc::ioctl(outer_slave, libc::TIOCSCTTY, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            if libc::dup2(outer_slave, libc::STDIN_FILENO) == -1
                || libc::dup2(outer_slave, libc::STDOUT_FILENO) == -1
                || libc::dup2(outer_slave, libc::STDERR_FILENO) == -1
            {
                return Err(io::Error::last_os_error());
            }
            if outer_slave > libc::STDERR_FILENO {
                libc::close(outer_slave);
            }
            libc::close(outer_master);
            Ok(())
        });
    }
    let mut traced = command.spawn().expect("launch isolated Codex probe");
    let traced_pid = traced.id() as i32;
    h.track_cleanup_pid(i64::from(traced_pid));
    close_fd(outer_slave);
    let reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        // SAFETY: the thread exclusively owns the PTY master.
        let mut file = unsafe { fs::File::from_raw_fd(outer_master) };
        let _ = file.read_to_end(&mut output);
        output
    });

    let name = h.eventually(
        "single Codex probe instance",
        Duration::from_secs(45),
        || {
            let instances = h.instances_for_tool("codex")?;
            if instances.len() == 1 {
                Ok(instances[0]
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string))
            } else {
                Ok(None)
            }
        },
    );
    h.eventually("probe PTY ready", Duration::from_secs(45), || {
        let (code, stdout, _stderr) = h.run(["term", &name, "--json"]);
        if code == 0 && stdout.contains("\"prompt_empty\":true") {
            Ok(Some(()))
        } else {
            Ok(None)
        }
    });

    let prompt = format!("Reply with the probe completion marker {token}");
    let inject_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (code, stdout, stderr) = h.run(["term", "inject", &name, &prompt, "--enter"]);
        if code == 0 {
            break;
        }
        let retryable = stdout.contains("prompt")
            || stdout.contains("No inject port")
            || stderr.contains("No inject port")
            || stderr.contains("No response from");
        assert!(
            retryable && Instant::now() < inject_deadline,
            "guarded probe prompt failed: stdout={stdout} stderr={stderr}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    h.eventually("probe request accepted", Duration::from_secs(30), || {
        if mock
            .requests()
            .iter()
            .any(|request| request.contains(&token))
        {
            Ok(Some(()))
        } else {
            Ok(None)
        }
    });
    let proof = || {
        let (code, stdout, _stderr) = h.run(["transcript", &name, "--full"]);
        code == 0 && stdout.contains("PHASE2_PROBE_COMPLETE") && stdout.contains(&token)
    };
    h.eventually("probe response persisted", Duration::from_secs(30), || {
        if proof() { Ok(Some(())) } else { Ok(None) }
    });

    let stop_event_id = h.eventually(
        "post-active Codex Stop candidate",
        Duration::from_secs(30),
        || {
            let db_path = h.path().join("hcom.db");
            let conn = rusqlite::Connection::open(&db_path)
                .map_err(|error| format!("open probe DB: {error}"))?;
            let active_id: Option<i64> = conn
                .query_row(
                    "SELECT MIN(id) FROM events
                     WHERE instance = ?1 AND type = 'status'
                       AND json_extract(data, '$.status') = 'active'",
                    [&name],
                    |row| row.get(0),
                )
                .map_err(|error| format!("query active event: {error}"))?;
            let Some(active_id) = active_id else {
                return Ok(None);
            };
            let stop_id = conn
                .query_row(
                    "SELECT MIN(id) FROM events
                     WHERE instance = ?1 AND type = 'status' AND id > ?2
                       AND json_extract(data, '$.status') = 'listening'
                       AND COALESCE(json_extract(data, '$.context'), '') = ''",
                    rusqlite::params![name, active_id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .map_err(|error| format!("query Stop candidate: {error}"))?;
            Ok(stop_id)
        },
    );
    assert!(stop_event_id > 0);
    assert!(proof(), "assistant response was not durable before SIGTERM");

    let inner_pid = h
        .instance_pid(&name)
        .expect("read inner Codex PID")
        .expect("inner Codex PID exists") as i32;
    let wrapper_pid = proc_parent(inner_pid).expect("read exact PTY wrapper parent");
    let wrapper_parent_pid = proc_parent(wrapper_pid).expect("read exact PTY wrapper wait parent");
    let inner_pgid = proc_process_group(inner_pid).expect("read inner Codex process group");
    assert!(wrapper_pid > 1 && wrapper_pid != inner_pid);
    assert!(wrapper_parent_pid > 1 && wrapper_parent_pid != wrapper_pid);
    let wrapper_pidfd = pidfd_open(wrapper_pid).expect("open exact wrapper pidfd");

    let requested_monotonic_ns = monotonic_ns();
    let requested_wall_seconds = wall_seconds();
    // SAFETY: wrapper_pid is the exact parent of the DB-pinned inner child.
    assert_eq!(unsafe { libc::kill(wrapper_pid, libc::SIGTERM) }, 0);

    wait_pidfd_readable(wrapper_pidfd, Duration::from_secs(10))
        .expect("Codex wrapper exceeded bounded SIGTERM exit deadline");
    close_fd(wrapper_pidfd);
    let observed_monotonic_ns = monotonic_ns();
    let observed_wall_seconds = wall_seconds();
    let elapsed_ms = (observed_monotonic_ns - requested_monotonic_ns) / 1_000_000;
    assert!((0..5_000).contains(&elapsed_ms));

    let (delivery_context, delivery_event_id, delivery_evidence) = h.eventually(
        "final delivery exit context",
        Duration::from_secs(10),
        || {
            let conn = rusqlite::Connection::open(h.path().join("hcom.db"))
                .map_err(|error| format!("open probe DB for exit context: {error}"))?;
            conn.query_row(
                "SELECT context, id, evidence FROM (
                     SELECT id, json_extract(data, '$.context') AS context,
                            'status.context' AS evidence
                     FROM events
                     WHERE instance = ?1 AND type = 'status'
                       AND json_extract(data, '$.context') IN ('exit:killed', 'exit:closed')
                     UNION ALL
                     SELECT id,
                            CASE json_extract(data, '$.reason')
                                WHEN 'killed' THEN 'exit:killed'
                                WHEN 'closed' THEN 'exit:closed'
                            END AS context,
                            'life.reason' AS evidence
                     FROM events
                     WHERE instance = ?1 AND type = 'life'
                       AND json_extract(data, '$.action') = 'stopped'
                       AND json_extract(data, '$.reason') IN ('killed', 'closed')
                 )
                 ORDER BY id DESC LIMIT 1",
                [&name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| format!("query delivery exit context: {error}"))
        },
    );
    assert_eq!(delivery_context, "exit:killed");

    let wrapper_trace = h.eventually(
        "exact wrapper signal and exit trace",
        Duration::from_secs(10),
        || {
            let text = read_strace_pid(&strace_prefix, wrapper_pid);
            let forwarded = format!("kill(-{inner_pgid}, SIGTERM)");
            if text.contains(&forwarded) {
                Ok(Some(text))
            } else {
                Ok(None)
            }
        },
    );
    assert!(
        !wrapper_trace.contains("SIGKILL"),
        "exact PTY wrapper invoked automatic SIGKILL"
    );
    let wait_trace = h.eventually(
        "exact parent waitpid trace",
        Duration::from_secs(10),
        || {
            let text = read_strace_pid(&strace_prefix, wrapper_parent_pid);
            let wait4_return = format!("= {wrapper_pid}");
            let waitid_pid = format!("si_pid={wrapper_pid}");
            let reaped = text.lines().any(|line| {
                (line.starts_with("wait4(")
                    && line.contains("WEXITSTATUS(s) == 143")
                    && line.ends_with(&wait4_return))
                    || (line.starts_with("waitid(")
                        && line.contains(&waitid_pid)
                        && line.contains("si_status=143"))
            });
            if reaped { Ok(Some(text)) } else { Ok(None) }
        },
    );
    assert!(
        wait_trace.contains("WEXITSTATUS(s) == 143") || wait_trace.contains("si_status=143"),
        "wait evidence did not preserve wrapper exit code 143: {wait_trace}"
    );
    let wrapper_exit_code = 143;
    let waitpid_reaped = true;

    // SAFETY: signal 0 performs liveness checks only.
    assert_eq!(unsafe { libc::kill(wrapper_pid, 0) }, -1);
    assert_eq!(unsafe { libc::kill(inner_pid, 0) }, -1);
    let remaining = h.instances_for_tool("codex").unwrap();
    assert!(remaining.is_empty(), "probe left a live Codex instance");

    let observer_reaped = reap_strace_observer(&mut traced, traced_pid, Duration::from_secs(5))
        .expect("reap bounded strace observer");
    let group_deadline = Instant::now() + Duration::from_secs(5);
    while h.process_group_alive(i64::from(traced_pid)) && Instant::now() < group_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        !h.process_group_alive(i64::from(traced_pid)),
        "probe outer process group remained after observer reap"
    );
    let terminal_output = reader.join().expect("join outer PTY reader");
    assert!(terminal_output.len() < 2 * 1024 * 1024);
    println!(
        "REAL_PROBE_JSON {}",
        serde_json::json!({
            "codex_version": codex_version,
            "stop_event_id_observed": true,
            "requested_wall_seconds": requested_wall_seconds,
            "requested_monotonic_ns": requested_monotonic_ns,
            "observed_wall_seconds": observed_wall_seconds,
            "observed_monotonic_ns": observed_monotonic_ns,
            "sigterm_to_exit_ms": elapsed_ms,
            "wrapper_exit_code": wrapper_exit_code,
            "waitpid_reaped": waitpid_reaped,
            "observer_reaped": observer_reaped,
            "delivery_context": delivery_context,
            "delivery_event_id": delivery_event_id,
            "delivery_evidence": delivery_evidence,
            "automatic_sigkill_syscall": false,
            "successor_spawned": false,
        })
    );
}

fn proc_parent(pid: i32) -> io::Result<i32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat.rfind(')').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "malformed Linux process stat")
    })?;
    stat[close + 1..]
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid parent PID"))
}

fn proc_process_group(pid: i32) -> io::Result<i32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat.rfind(')').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "malformed Linux process stat")
    })?;
    stat[close + 1..]
        .split_whitespace()
        .nth(2)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid process group"))
}

fn read_strace_pid(prefix: &Path, pid: i32) -> String {
    let file_name = format!(
        "{}.{pid}",
        prefix.file_name().unwrap_or_default().to_string_lossy()
    );
    fs::read_to_string(prefix.with_file_name(file_name)).unwrap_or_default()
}

fn pidfd_open(pid: i32) -> io::Result<RawFd> {
    // SAFETY: pidfd_open takes scalar arguments and returns a new descriptor.
    let result = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as RawFd)
    }
}

fn wait_pidfd_readable(pidfd: RawFd, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pidfd did not become readable",
            ));
        }
        let mut pollfd = libc::pollfd {
            fd: pidfd,
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
        // SAFETY: pollfd points to one initialized entry for this call.
        let result = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if result > 0 && pollfd.revents & libc::POLLIN != 0 {
            return Ok(());
        }
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pidfd did not become readable",
            ));
        }
        if result < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
    }
}

fn reap_strace_observer(
    child: &mut std::process::Child,
    pid: i32,
    timeout: Duration,
) -> io::Result<bool> {
    if child.try_wait()?.is_none() {
        // SIGINT asks strace to detach any already-irrelevant short-lived
        // descendants. It is sent to the observer PID, never the PTY group.
        // SAFETY: pid names the exact Child handle above.
        let result = unsafe { libc::kill(pid, libc::SIGINT) };
        if result != 0 && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(io::Error::last_os_error());
        }
    }
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "strace observer did not exit after SIGINT",
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn monotonic_ns() -> i64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) },
        0
    );
    value.tv_sec * 1_000_000_000 + value.tv_nsec
}

fn wall_seconds() -> u64 {
    // SAFETY: null requests seconds only.
    unsafe { libc::time(std::ptr::null_mut()) as u64 }
}

fn close_fd(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: the caller owns the descriptor.
        unsafe {
            libc::close(fd);
        }
    }
}

use std::os::fd::FromRawFd;
