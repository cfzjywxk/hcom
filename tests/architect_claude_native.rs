#![cfg(target_os = "linux")]

use nix::pty::openpty;
use nix::unistd::dup;
use std::fs;
use std::io::{ErrorKind, Read};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn hcom_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hcom")
}

fn fake_claude(root: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = root.join("claude");
    fs::write(&path, format!("#!/usr/bin/python3\n{body}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn exact_proxy_environment(command: &mut Command) {
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
        command.env(name, "http://127.0.0.1:7890");
    }
}

fn run_on_disposable_pty(mut command: Command) -> (std::process::ExitStatus, Vec<u8>) {
    let pty = openpty(None, None).unwrap();
    let stdin = dup(&pty.slave).unwrap();
    let stdout = dup(&pty.slave).unwrap();
    let stderr = dup(&pty.slave).unwrap();
    command
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    // SAFETY: the disposable child creates and owns this new controlling
    // terminal before hcom validates its foreground terminal contract.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1
                || libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY, 0) == -1
                || libc::tcsetpgrp(libc::STDIN_FILENO, libc::getpid()) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    drop(pty.slave);
    let status = child.wait().unwrap();
    let flags = unsafe { libc::fcntl(pty.master.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe {
            libc::fcntl(
                pty.master.as_raw_fd(),
                libc::F_SETFL,
                flags | libc::O_NONBLOCK,
            )
        },
        0
    );
    let mut master = fs::File::from(pty.master);
    let mut terminal = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut buffer = [0u8; 4096];
    loop {
        match master.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => terminal.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => panic!("failed reading disposable Architect PTY: {error}"),
        }
    }
    (status, terminal)
}

#[test]
fn claude_architect_uses_native_environment_guardian_and_additive_mcp() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let project = root.join("project");
    let external = root.join("external");
    let home = root.join("home");
    let hcom_dir = root.join("hcom-state");
    let claude_config = root.join("claude-config");
    for directory in [&project, &external, &home, &hcom_dir, &claude_config] {
        fs::create_dir(directory).unwrap();
    }
    let report = root.join("report.json");
    let script = format!(
        r#"
import json
import os
import select
import sys
import time

time.sleep(1.0)
if not all(os.isatty(fd) for fd in (0, 1, 2)):
    raise SystemExit(31)
if select.select([0], [], [], 0)[0]:
    raise SystemExit(32)
if os.path.basename(os.readlink(f"/proc/{{os.getppid()}}/exe")) != "hcom":
    raise SystemExit(33)
if os.environ.get("PARENT_CANARY") != "preserved":
    raise SystemExit(34)
if os.environ.get("HCOM_DIR") != {hcom_dir}:
    raise SystemExit(35)
if os.environ.get("HOME") != {home}:
    raise SystemExit(36)
if os.environ.get("CLAUDE_CONFIG_DIR") != {claude_config}:
    raise SystemExit(37)
if os.environ.get("CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD") != "1":
    raise SystemExit(38)
if os.environ.get("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS") != "1":
    raise SystemExit(39)
for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    if os.environ.get(name) != "http://127.0.0.1:7890":
        raise SystemExit(40)

with open({report}, "w", encoding="utf-8") as output:
    json.dump({{"argv": sys.argv[1:]}}, output)
time.sleep(0.6)
"#,
        hcom_dir = serde_json::to_string(&hcom_dir.to_string_lossy()).unwrap(),
        home = serde_json::to_string(&home.to_string_lossy()).unwrap(),
        claude_config = serde_json::to_string(&claude_config.to_string_lossy()).unwrap(),
        report = serde_json::to_string(&report.to_string_lossy()).unwrap(),
    );
    fake_claude(&root, &script);

    let mut command = Command::new(hcom_binary());
    command
        .args([
            "arch",
            "claude",
            "--model",
            "haiku",
            "--effort",
            "medium",
            "--add-dir",
        ])
        .arg(&external)
        .current_dir(&project)
        .env_clear()
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .env("TERM", "xterm-256color")
        .env("HOME", &home)
        .env("HCOM_DIR", &hcom_dir)
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .env("PARENT_CANARY", "preserved");
    exact_proxy_environment(&mut command);
    let (status, terminal) = run_on_disposable_pty(command);
    assert!(
        status.success(),
        "fake Claude Architect failed: {status}\n{}",
        String::from_utf8_lossy(&terminal)
    );

    let report: serde_json::Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    let argv = report["argv"].as_array().unwrap();
    let argv: Vec<_> = argv.iter().map(|value| value.as_str().unwrap()).collect();
    assert!(argv.windows(2).any(|pair| pair == ["--model", "haiku"]));
    assert!(argv.windows(2).any(|pair| pair == ["--effort", "medium"]));
    assert!(
        argv.windows(2)
            .any(|pair| { pair[0] == "--add-dir" && pair[1] == external.to_string_lossy() })
    );
    assert!(argv.contains(&"--dangerously-skip-permissions"));
    for forbidden in [
        "--name",
        "--session-id",
        "--tools",
        "--setting-sources",
        "--strict-mcp-config",
        "--disable-slash-commands",
        "--prompt-suggestions",
        "--no-chrome",
    ] {
        assert!(!argv.contains(&forbidden));
    }
    let mcp = argv
        .windows(2)
        .find(|pair| pair[0] == "--mcp-config")
        .map(|pair| pair[1])
        .unwrap();
    let mcp: serde_json::Value = serde_json::from_str(mcp).unwrap();
    assert_eq!(mcp["mcpServers"].as_object().unwrap().len(), 1);
    assert!(mcp["mcpServers"]["hcom_session_task_control"].is_object());

    let terminal = String::from_utf8_lossy(&terminal);
    assert!(terminal.contains("CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1"));
    assert!(terminal.contains("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1"));
    assert!(terminal.contains(&external.to_string_lossy().into_owned()));
}

#[test]
fn missing_claude_executable_returns_an_actionable_native_launch_failure() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let project = root.join("project");
    let home = root.join("home");
    let hcom_dir = root.join("hcom-state");
    let empty_path = root.join("empty-path");
    for directory in [&project, &home, &hcom_dir, &empty_path] {
        fs::create_dir(directory).unwrap();
    }
    let mut command = Command::new(hcom_binary());
    command
        .args(["arch", "claude"])
        .current_dir(project)
        .env_clear()
        .env("PATH", &empty_path)
        .env("TERM", "xterm-256color")
        .env("HOME", home)
        .env("HCOM_DIR", hcom_dir);
    exact_proxy_environment(&mut command);
    let (status, terminal) = run_on_disposable_pty(command);
    assert!(!status.success());
    let terminal = String::from_utf8_lossy(&terminal);
    assert!(
        terminal.contains("failed to launch bare Claude executable from inherited PATH"),
        "{terminal}"
    );
}

#[test]
fn unsupported_claude_cli_reports_the_native_option_failure() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let project = root.join("project");
    let home = root.join("home");
    let hcom_dir = root.join("hcom-state");
    for directory in [&project, &home, &hcom_dir] {
        fs::create_dir(directory).unwrap();
    }
    fake_claude(
        &root,
        "import sys\nimport time\ntime.sleep(0.5)\nprint('error: unknown option --effort', file=sys.stderr)\nraise SystemExit(64)",
    );

    let mut command = Command::new(hcom_binary());
    command
        .args(["arch", "claude"])
        .current_dir(project)
        .env_clear()
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .env("TERM", "xterm-256color")
        .env("HOME", home)
        .env("HCOM_DIR", hcom_dir);
    exact_proxy_environment(&mut command);
    let (status, terminal) = run_on_disposable_pty(command);
    let terminal = String::from_utf8_lossy(&terminal);
    assert!(!status.success(), "{terminal}");
    assert!(
        terminal.contains("error: unknown option --effort"),
        "{terminal}"
    );
}

#[test]
fn invalid_proxy_environment_fails_before_the_fake_claude_executable() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let project = root.join("project");
    let home = root.join("home");
    let hcom_dir = root.join("hcom-state");
    for directory in [&project, &home, &hcom_dir] {
        fs::create_dir(directory).unwrap();
    }
    let marker = root.join("spawned");
    fake_claude(
        &root,
        &format!(
            "from pathlib import Path\nPath({}).write_text('spawned')",
            serde_json::to_string(&marker.to_string_lossy()).unwrap()
        ),
    );

    for invalid in ["missing", "mismatch", "non-utf8"] {
        let mut command = Command::new(hcom_binary());
        command
            .args(["arch", "claude"])
            .current_dir(&project)
            .env_clear()
            .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
            .env("TERM", "xterm-256color")
            .env("HOME", &home)
            .env("HCOM_DIR", &hcom_dir)
            .env("HTTP_PROXY", "http://127.0.0.1:7890")
            .env("HTTPS_PROXY", "http://127.0.0.1:7890")
            .env("http_proxy", "http://127.0.0.1:7890");
        match invalid {
            "missing" => {}
            "mismatch" => {
                command.env("https_proxy", "unexpected-secret-value");
            }
            "non-utf8" => {
                command.env(
                    "https_proxy",
                    std::ffi::OsString::from_vec(b"http://127.0.0.1:7890\xff".to_vec()),
                );
            }
            _ => unreachable!(),
        }
        let (status, terminal) = run_on_disposable_pty(command);
        assert!(!status.success(), "{invalid} unexpectedly succeeded");
        assert!(!marker.exists(), "{invalid} crossed the pre-spawn gate");
        let terminal = String::from_utf8_lossy(&terminal);
        assert!(terminal.contains("https_proxy"), "{invalid}: {terminal}");
        assert!(
            !terminal.contains("unexpected-secret-value"),
            "diagnostic echoed the mismatched value"
        );
    }
}
