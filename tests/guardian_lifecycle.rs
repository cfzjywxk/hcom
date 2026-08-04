#![cfg(target_os = "linux")]

use hcom::worker::guardian::{
    CleanupRegistryInterlock, GuardedCommand, GuardianCleanupDisposition, GuardianCleanupReason,
    GuardianCleanupRegistry, GuardianHandle, GuardianHandleFailure, GuardianMode, GuardianPoll,
    GuardianSpawnFailure,
};
use nix::pty::openpty;
use nix::sys::termios;
use nix::unistd::dup;
use std::fs;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(8);

fn hcom_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hcom")
}

fn executable(root: &tempfile::TempDir, name: &str, body: &str) -> std::path::PathBuf {
    let path = root.path().join(name);
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn guarded(program: impl Into<std::ffi::OsString>) -> GuardedCommand {
    GuardedCommand::with_guardian_executable(hcom_binary(), program).unwrap()
}

fn wait_completion(handle: &mut GuardianHandle) -> hcom::worker::guardian::GuardianCompletion {
    let deadline = Instant::now() + WAIT;
    loop {
        match handle.try_wait() {
            GuardianPoll::Complete(completion) => return completion,
            GuardianPoll::OwnershipLost(detail) => panic!("Guardian ownership lost: {detail}"),
            GuardianPoll::Running | GuardianPoll::CleanupPending => {}
        }
        assert!(
            Instant::now() < deadline,
            "Guardian did not complete within the test bound"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn process_birth(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    stat[close + 1..].split_whitespace().nth(19)?.parse().ok()
}

fn wait_identity_gone(pid: u32, birth: u64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while process_birth(pid) == Some(birth) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_ne!(
        process_birth(pid),
        Some(birth),
        "exact fake descendant {pid}/{birth} survived Guardian cleanup"
    );
}

fn wait_pid_file(path: &std::path::Path) -> (u32, u64) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(value) = fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse::<u32>()
            && let Some(birth) = process_birth(pid)
        {
            return (pid, birth);
        }
        assert!(
            Instant::now() < deadline,
            "fake process did not publish its PID"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn guardian_preserves_blank_pipe_transport_and_does_not_parse_native_output() {
    let root = tempfile::tempdir().unwrap();
    let script = executable(
        &root,
        "native-transport",
        r#"
[ ! -t 0 ] && [ ! -t 1 ] && [ ! -t 2 ]
payload=$(sed -n '1,$p')
printf 'stdout:%s' "$payload"
printf 'stderr:%s' "$payload" >&2
"#,
    );
    let mut command = guarded(script);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut handle = command.spawn().unwrap();
    let mut stdin = handle.take_stdin().unwrap();
    let mut stdout = handle.take_stdout().unwrap();
    let mut stderr = handle.take_stderr().unwrap();
    let stdout_thread = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let stderr_thread = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    stdin.write_all(b"opaque final-like payload\n").unwrap();
    drop(stdin);

    let completion = wait_completion(&mut handle);
    assert_eq!(completion.disposition, GuardianCleanupDisposition::Clean);
    assert_eq!(completion.native_code, Some(0));
    assert_eq!(
        stdout_thread.join().unwrap(),
        b"stdout:opaque final-like payload"
    );
    assert_eq!(
        stderr_thread.join().unwrap(),
        b"stderr:opaque final-like payload"
    );
}

#[test]
fn guardian_exactly_cleans_pipe_redirected_setsid_and_double_fork_descendants() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = executable(
        &root,
        "ordinary-background",
        r#"
mode=$1
pidfile=$2
case "$mode" in
  pipe)
    sleep 30 &
    echo "$!" >"$pidfile"
    ;;
  redirected)
    sleep 30 </dev/null >/dev/null 2>&1 &
    echo "$!" >"$pidfile"
    ;;
  setsid)
    setsid sh -c 'echo "$$" >"$1"; exec sleep 30' sh "$pidfile" \
      </dev/null >/dev/null 2>&1 &
    ;;
  ignore-term)
    sh -c 'trap "" TERM; echo "$$" >"$1"; exec sleep 30' sh "$pidfile" \
      </dev/null >/dev/null 2>&1 &
    ;;
  *)
    exit 64
    ;;
esac
exit 0
"#,
    );
    let double_fork = root.path().join("double-fork.py");
    fs::write(
        &double_fork,
        r#"#!/usr/bin/python3
import os, sys, time
pidfile = sys.argv[1]
first = os.fork()
if first == 0:
    os.setsid()
    second = os.fork()
    if second == 0:
        with open(pidfile, "w", encoding="ascii") as out:
            out.write(str(os.getpid()))
            out.flush()
            os.fsync(out.fileno())
        time.sleep(30)
    os._exit(0)
os.waitpid(first, 0)
"#,
    )
    .unwrap();
    fs::set_permissions(&double_fork, fs::Permissions::from_mode(0o700)).unwrap();

    for mode in ["pipe", "redirected", "setsid", "ignore-term"] {
        let pidfile = root.path().join(format!("{mode}.pid"));
        let mut command = guarded(&ordinary);
        command
            .args([mode.into(), pidfile.as_os_str().to_owned()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut handle = command.spawn().unwrap();
        let identity = wait_pid_file(&pidfile);
        let completion = wait_completion(&mut handle);
        assert_eq!(
            completion.disposition,
            GuardianCleanupDisposition::OrphanedDescendants,
            "unexpected disposition for {mode}"
        );
        assert!(completion.forced_signal_count > 0);
        wait_identity_gone(identity.0, identity.1);
    }

    let pidfile = root.path().join("double-fork.pid");
    let mut command = guarded(double_fork);
    command
        .arg(pidfile.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut handle = command.spawn().unwrap();
    let identity = wait_pid_file(&pidfile);
    let completion = wait_completion(&mut handle);
    assert_eq!(
        completion.disposition,
        GuardianCleanupDisposition::OrphanedDescendants
    );
    wait_identity_gone(identity.0, identity.1);
}

#[test]
fn plausible_native_final_with_a_residual_descendant_is_not_clean_success() {
    let root = tempfile::tempdir().unwrap();
    let pidfile = root.path().join("plausible.pid");
    let native = executable(
        &root,
        "plausible-final",
        r#"
sleep 30 </dev/null >/dev/null 2>&1 &
echo "$!" >"$1"
printf 'STATUS: READY\nplausible but lifecycle-unsafe final\n'
"#,
    );
    let mut command = guarded(native);
    command
        .arg(pidfile.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut handle = command.spawn().unwrap();
    let mut stdout = handle.take_stdout().unwrap();
    let output_thread = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let identity = wait_pid_file(&pidfile);
    let completion = wait_completion(&mut handle);
    assert_eq!(
        completion.disposition,
        GuardianCleanupDisposition::OrphanedDescendants
    );
    assert_eq!(
        output_thread.join().unwrap(),
        b"STATUS: READY\nplausible but lifecycle-unsafe final\n"
    );
    wait_identity_gone(identity.0, identity.1);
}

#[test]
fn registry_transfer_retains_pending_handle_beyond_one_attempt_then_releases_it() {
    let root = tempfile::tempdir().unwrap();
    let pidfile = root.path().join("registry.pid");
    let native = executable(
        &root,
        "registry-child",
        r#"
sh -c 'trap "" TERM; echo "$$" >"$1"; exec sleep 30' sh "$1" \
  </dev/null >/dev/null 2>&1 &
exit 0
"#,
    );
    let mut command = guarded(native);
    command
        .arg(pidfile.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let handle = command.spawn().unwrap();
    let identity = wait_pid_file(&pidfile);
    let registry = GuardianCleanupRegistry::default();
    registry.register(handle).unwrap();

    assert_eq!(
        registry.cleanup_for(Duration::from_millis(1)),
        CleanupRegistryInterlock::Pending { claims: 1 }
    );
    assert!(registry.ensure_available().is_err());
    assert_eq!(registry.cleanup_for(WAIT), CleanupRegistryInterlock::Ready);
    wait_identity_gone(identity.0, identity.1);
}

#[test]
fn guardian_reaps_fast_forks_and_preserves_nonzero_signal_cancel_timeout_classes() {
    let root = tempfile::tempdir().unwrap();
    let fast = executable(
        &root,
        "fast-fork",
        r#"
i=0
while [ "$i" -lt 24 ]; do
  (exit 0) &
  i=$((i + 1))
done
exit 0
"#,
    );
    for _ in 0..8 {
        let mut command = guarded(&fast);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut handle = command.spawn().unwrap();
        let completion = wait_completion(&mut handle);
        assert_eq!(completion.native_code, Some(0));
        assert!(matches!(
            completion.disposition,
            GuardianCleanupDisposition::Clean | GuardianCleanupDisposition::OrphanedDescendants
        ));
    }

    let nonzero = executable(&root, "nonzero", "exit 17");
    let mut command = guarded(nonzero);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut handle = command.spawn().unwrap();
    let completion = wait_completion(&mut handle);
    assert_eq!(
        completion.disposition,
        GuardianCleanupDisposition::NativeFailure
    );
    assert_eq!(completion.native_code, Some(17));

    let signaled = executable(&root, "signaled", "kill -TERM $$");
    let mut command = guarded(signaled);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut handle = command.spawn().unwrap();
    let completion = wait_completion(&mut handle);
    assert_eq!(
        completion.disposition,
        GuardianCleanupDisposition::NativeFailure
    );
    assert_eq!(completion.native_signal, Some(libc::SIGTERM));

    let sleeper = executable(&root, "sleeper", "exec sleep 30");
    for (reason, disposition) in [
        (
            GuardianCleanupReason::Cancel,
            GuardianCleanupDisposition::Canceled,
        ),
        (
            GuardianCleanupReason::Timeout,
            GuardianCleanupDisposition::TimedOut,
        ),
    ] {
        let mut command = guarded(&sleeper);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut handle = command.spawn().unwrap();
        let native = handle.ready().native.clone();
        let completion = handle.terminate_and_reap(reason, WAIT).unwrap();
        assert_eq!(completion.disposition, disposition);
        wait_identity_gone(native.pid, native.birth);
    }
}

#[test]
fn guardian_capability_failure_occurs_before_native_spawn() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("native-spawned");
    let native = executable(
        &root,
        "must-not-spawn",
        &format!("printf spawned >'{}'", marker.display()),
    );
    let control_file = fs::File::open("/dev/null").unwrap();
    let control_fd = control_file.as_raw_fd();
    // SAFETY: fcntl only updates this test-owned descriptor before fork.
    unsafe {
        let flags = libc::fcntl(control_fd, libc::F_GETFD);
        assert!(flags >= 0);
        assert_eq!(
            libc::fcntl(control_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC),
            0
        );
    }
    let status = Command::new(hcom_binary())
        .arg("__hcom_internal_claude_guardian_v1")
        .args(["--control-fd", &control_fd.to_string()])
        .args(["--expected-parent", &std::process::id().to_string()])
        .args(["--mode", "headless", "--"])
        .arg(native)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success());
    assert!(!marker.exists(), "native process crossed capability gate");

    let mut missing_native = guarded(root.path().join("missing-native"));
    missing_native
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    assert!(matches!(
        missing_native.spawn(),
        Err(GuardianSpawnFailure::Reaped(_))
    ));
}

#[test]
fn guardian_parent_normal_exit_and_sigkill_both_cleanup_owned_native_process() {
    const HELPER: &str = "HCOM_GUARDIAN_PARENT_HELPER";
    if let Some(mode) = std::env::var_os(HELPER) {
        let report = std::path::PathBuf::from(std::env::var_os("HCOM_GUARDIAN_REPORT").unwrap());
        let native = std::env::var_os("HCOM_GUARDIAN_NATIVE").unwrap();
        let mut command = GuardedCommand::with_guardian_executable(hcom_binary(), native).unwrap();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let handle = command.spawn().unwrap();
        let ready = handle.ready();
        fs::write(
            report,
            format!(
                "{} {} {} {}",
                ready.guardian.pid, ready.guardian.birth, ready.native.pid, ready.native.birth
            ),
        )
        .unwrap();
        if mode == "sigkill" {
            // SAFETY: this deliberately kills only the current disposable test
            // helper so the Guardian's PDEATHSIG path is exercised.
            unsafe {
                libc::kill(libc::getpid(), libc::SIGKILL);
            }
            unreachable!();
        }
        std::process::exit(0);
    }

    let root = tempfile::tempdir().unwrap();
    let sleeper = executable(&root, "parent-death-sleeper", "exec sleep 30");
    for mode in ["normal", "sigkill"] {
        let report = root.path().join(format!("{mode}.identity"));
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("guardian_parent_normal_exit_and_sigkill_both_cleanup_owned_native_process")
            .arg("--nocapture")
            .env(HELPER, mode)
            .env("HCOM_GUARDIAN_REPORT", &report)
            .env("HCOM_GUARDIAN_NATIVE", &sleeper)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        if mode == "normal" {
            assert!(status.success());
        } else {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        }
        let fields: Vec<u64> = fs::read_to_string(report)
            .unwrap()
            .split_ascii_whitespace()
            .map(|value| value.parse().unwrap())
            .collect();
        wait_identity_gone(fields[0] as u32, fields[1]);
        wait_identity_gone(fields[2] as u32, fields[3]);
    }
}

#[test]
fn foreground_mode_keeps_tty_fds_blank_and_preserves_terminal_attributes() {
    let root = tempfile::tempdir().unwrap();
    let native = root.path().join("foreground.py");
    fs::write(
        &native,
        r#"#!/usr/bin/python3
import os, select, signal, sys, time
assert os.isatty(0) and os.isatty(1) and os.isatty(2)
assert os.ttyname(0) == os.ttyname(1) == os.ttyname(2)
assert not select.select([0], [], [], 0.15)[0]
print("BLANK", flush=True)
def winch(_sig, _frame):
    print("WINCH", flush=True)
def interrupt(_sig, _frame):
    print("INT", flush=True)
    sys.exit(0)
signal.signal(signal.SIGWINCH, winch)
signal.signal(signal.SIGINT, interrupt)
while True:
    time.sleep(0.05)
"#,
    )
    .unwrap();
    fs::set_permissions(&native, fs::Permissions::from_mode(0o700)).unwrap();

    let pty = openpty(None, None).unwrap();
    let before = termios::tcgetattr(&pty.slave).unwrap();
    let stdin = dup(&pty.slave).unwrap();
    let stdout = dup(&pty.slave).unwrap();
    let stderr = dup(&pty.slave).unwrap();
    let mut command = guarded(native);
    command
        .mode(GuardianMode::ForegroundArchitect)
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut handle = command.spawn().unwrap();
    let native_identity = handle.ready().native.clone();
    thread::sleep(Duration::from_millis(250));
    // SAFETY: the native PID/birth remains pinned by the live Guardian and has
    // just been validated through the readiness frame.
    unsafe {
        assert_eq!(
            libc::kill(native_identity.pid as libc::pid_t, libc::SIGWINCH),
            0
        );
        thread::sleep(Duration::from_millis(30));
        assert_eq!(
            libc::kill(native_identity.pid as libc::pid_t, libc::SIGINT),
            0
        );
    }
    let completion = wait_completion(&mut handle);
    assert_eq!(completion.disposition, GuardianCleanupDisposition::Clean);
    let after = termios::tcgetattr(&pty.slave).unwrap();
    assert_eq!(before, after);

    let flags = unsafe { libc::fcntl(pty.master.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    unsafe {
        assert_eq!(
            libc::fcntl(
                pty.master.as_raw_fd(),
                libc::F_SETFL,
                flags | libc::O_NONBLOCK
            ),
            0
        );
    }
    let raw_master = pty.master.as_raw_fd();
    std::mem::forget(pty.master);
    // SAFETY: ownership of the forgotten master descriptor transfers here.
    let mut master = unsafe { fs::File::from_raw_fd(raw_master) };
    let mut output = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        match master.read_to_end(&mut output) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("PTY read failed: {error}"),
        }
    }
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("BLANK"), "{output:?}");
    assert!(output.contains("WINCH"), "{output:?}");
    assert!(output.contains("INT"), "{output:?}");
}

#[test]
fn bounded_cleanup_never_escalates_by_killing_the_guardian() {
    let root = tempfile::tempdir().unwrap();
    let sleeper = executable(&root, "bounded", "exec sleep 30");
    let mut command = guarded(sleeper);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut handle = command.spawn().unwrap();
    let guardian = handle.ready().guardian.clone();
    match handle.terminate_and_reap(GuardianCleanupReason::Cancel, Duration::from_millis(1)) {
        Ok(_) | Err(GuardianHandleFailure::CleanupPending(_)) => {}
        Err(GuardianHandleFailure::OwnershipLost(detail)) => {
            panic!("bounded cleanup lost ownership: {detail}")
        }
    }
    assert_eq!(process_birth(guardian.pid), Some(guardian.birth));
    let completion = handle
        .terminate_and_reap(GuardianCleanupReason::Cancel, WAIT)
        .unwrap();
    assert_eq!(completion.disposition, GuardianCleanupDisposition::Canceled);
}
