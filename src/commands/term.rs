//! `hcom term` command — terminal admin: screen queries, text injection, debug logging.
//!
//!
//! Talks to PTY instances via their TCP inject ports.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::db::HcomDb;

/// Parsed arguments for `hcom term`.
#[derive(clap::Parser, Debug)]
#[command(
    name = "term",
    about = "Terminal admin: screen query, injection, debug"
)]
pub struct TermArgs {
    /// Subcommand and arguments
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}
use crate::identity::resolve_display_name;
use crate::paths::hcom_dir;
use crate::shared::CommandContext;

/// PTY debug flag file path.
fn flag_path() -> PathBuf {
    hcom_dir().join(".tmp").join("pty_debug_on")
}

/// Look up inject port for an instance.
///
/// The inject port is a bidirectional RPC server (input bytes / `\x00SCREEN\n`
/// query) — it shares the `notify_endpoints` table with wake endpoints but
/// uses a different protocol. See `crate::notify::WakeKind` for the wake kinds.
fn get_inject_port(db: &HcomDb, instance_name: &str) -> Option<i32> {
    db.conn()
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = ?1 AND kind = 'inject'",
            rusqlite::params![instance_name],
            |row| row.get(0),
        )
        .ok()
}

/// Get all instances that have an inject port registered.
///
/// Returns `(instance_name, inject_port)` pairs. An inject port means the
/// instance is running a PTY screen-query RPC server (registered by the PTY
/// manager); having one is the queryable-via-`hcom term` signal.
fn get_pty_instances(db: &HcomDb) -> Vec<(String, i32)> {
    let mut stmt = match db
        .conn()
        .prepare("SELECT instance, port FROM notify_endpoints WHERE kind = 'inject'")
    {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
    })
    .ok()
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

/// Send data on a single TCP connection.
fn inject_raw(port: i32, data: &[u8]) -> Result<(), String> {
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).map_err(|e| format!("connect: {e}"))?;
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    stream.write_all(data).map_err(|e| format!("write: {e}"))?;
    Ok(())
}

/// Send a guarded RPC frame and return the raw response line.
fn guarded_rpc(port: i32, frame: &str) -> Result<String, String> {
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    stream
        .write_all(frame.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|e| format!("shutdown: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("read: {e}"))?;
    Ok(response.trim().to_string())
}

/// Parse `OK epoch=<n> [gen=<g>]` responses from guarded RPCs.
fn parse_ok_epoch(response: &str) -> Option<u64> {
    response
        .strip_prefix("OK ")?
        .split_whitespace()
        .find_map(|token| token.strip_prefix("epoch="))
        .and_then(|n| n.parse().ok())
}

pub fn inject_text_remote_result(
    db: &HcomDb,
    name: &str,
    text: &str,
    enter: bool,
    force: bool,
) -> Result<String, String> {
    let port = get_inject_port(db, name).ok_or_else(|| format!("No inject port for '{name}'."))?;

    // --force: legacy raw behavior, explicitly requested. This is the ONLY
    // path that can write a CR without proving prompt ownership.
    if force {
        if !text.is_empty() {
            inject_raw(port, text.as_bytes())?;
        }
        if enter {
            if !text.is_empty() {
                let _ = wait_for_exact_render(port, text, TEXT_RENDER_TIMEOUT);
            }
            inject_raw(port, b"\r")?;
        }
        let label = match (text.is_empty(), enter) {
            (false, true) => format!("Injected {} chars + enter to {} (forced)", text.len(), name),
            (false, false) => format!("Injected {} chars to {}", text.len(), name),
            (true, _) => format!("Injected enter to {} (forced)", name),
        };
        return Ok(label);
    }

    // Text-only inject: no submit at stake, keep raw semantics.
    if !enter {
        if text.is_empty() {
            return Err("Nothing to inject".into());
        }
        inject_raw(port, text.as_bytes())?;
        return Ok(format!("Injected {} chars to {}", text.len(), name));
    }

    // Guarded submit contract from here on: hcom must not press Enter unless
    // it can prove the prompt contains exactly (and only) what it injected,
    // with no human input in between. Every failure below sends NO Enter.
    if text.is_empty() {
        return Err(format!(
            "enter-only cannot prove prompt ownership; use --force to send a raw enter to {name}"
        ));
    }

    let screen = query_screen(port).ok_or_else(|| {
        format!("screen query failed for '{name}'; enter NOT sent (use --force to bypass)")
    })?;
    match screen.get("input_state").and_then(|v| v.as_str()) {
        Some("text") => {}
        Some("unsupported") => {
            return Err(format!(
                "'{name}' has no prompt parser; ownership cannot be proven. Use --force for raw injection"
            ));
        }
        Some("unavailable") => {
            return Err(format!(
                "prompt not detectable on '{name}' right now; enter NOT sent (retry, or --force)"
            ));
        }
        _ => {
            return Err(format!(
                "'{name}' does not support the guarded submit protocol; enter NOT sent (use --force)"
            ));
        }
    }
    if screen.get("input_text").and_then(|v| v.as_str()) != Some("") {
        return Err(format!(
            "prompt on '{name}' is not empty (a draft may be present); enter NOT sent"
        ));
    }
    let user_gen = screen.get("user_gen").and_then(|v| v.as_u64());
    let input_epoch = screen.get("input_epoch").and_then(|v| v.as_u64());
    let (Some(user_gen), Some(input_epoch)) = (user_gen, input_epoch) else {
        return Err(format!(
            "'{name}' does not report guarded-input state; enter NOT sent (use --force)"
        ));
    };

    let inject_req = serde_json::json!({
        "user_gen": user_gen,
        "input_epoch": input_epoch,
        "require_empty": true,
        "payload": text,
    });
    let response = guarded_rpc(port, &format!("\x00INJECT_IF {inject_req}\n"))?;
    let Some(token_epoch) = parse_ok_epoch(&response) else {
        return Err(format!("inject refused ({response}); nothing was written"));
    };

    // Wait until the tool actually rendered exactly our text before asking
    // for the submit; on timeout the Enter is NOT sent.
    if !wait_for_exact_render(port, text, TEXT_RENDER_TIMEOUT) {
        return Err(format!(
            "injected text was not confirmed rendered on '{name}'; enter NOT sent"
        ));
    }

    let submit_req = serde_json::json!({
        "user_gen": user_gen,
        "input_epoch": token_epoch,
        "expected_text": text,
    });
    let response = guarded_rpc(port, &format!("\x00SUBMIT_IF {submit_req}\n"))?;
    if parse_ok_epoch(&response).is_none() {
        return Err(format!("submit refused ({response}); enter NOT sent"));
    }
    Ok(format!(
        "Injected {} chars + guarded enter to {}",
        text.len(),
        name
    ))
}

/// Poll the screen until the injected text exclusively fills the input box.
/// In the guarded flow a `false` return means the Enter is NOT sent; the
/// `--force` path uses it best-effort only.
const TEXT_RENDER_TIMEOUT: Duration = Duration::from_secs(2);

fn wait_for_exact_render(port: i32, text: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(screen) = query_screen(port)
            && screen.get("input_text").and_then(|v| v.as_str()) == Some(text)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// Inject text into PTY via inject port (CLI wrapper).
fn inject_text(db: &HcomDb, name: &str, text: &str, enter: bool, force: bool) -> i32 {
    match inject_text_remote_result(db, name, text, enter, force) {
        Ok(msg) => {
            println!("{msg}");
            0
        }
        Err(e) => {
            println!("{e}");
            1
        }
    }
}

/// Send screen query to inject port, get back parsed JSON.
fn query_screen(port: i32) -> Option<serde_json::Value> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    stream.write_all(b"\x00SCREEN\n").ok()?;
    stream.shutdown(std::net::Shutdown::Write).ok()?;

    let mut data = Vec::new();
    stream.read_to_end(&mut data).ok()?;
    if data.is_empty() {
        return None;
    }
    serde_json::from_slice(&data).ok()
}

pub fn read_instance_screen(
    db: &HcomDb,
    name: &str,
    raw_json: bool,
    clean: bool,
) -> Result<String, String> {
    let port = get_inject_port(db, name).ok_or_else(|| {
        format!(
            "No inject port for '{}'. Instance not running or not PTY-managed.",
            name
        )
    })?;
    let result = query_screen(port)
        .ok_or_else(|| format!("No response from '{}' (port {}).", name, port))?;
    if raw_json {
        Ok(serde_json::to_string(&result).unwrap_or_default())
    } else {
        Ok(format_screen(&result, clean))
    }
}

/// Format screen JSON as readable text.
fn format_screen(data: &serde_json::Value, clean: bool) -> String {
    let lines = data["lines"].as_array();
    let cursor = data["cursor"].as_array();
    let size = data["size"].as_array();

    let (rows, cols) = size
        .map(|s| {
            (
                s.first().and_then(|v| v.as_i64()).unwrap_or(0),
                s.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));

    let (cr, cc) = cursor
        .map(|c| {
            (
                c.first().and_then(|v| v.as_i64()).unwrap_or(0),
                c.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));

    let ready = data.get("ready");
    let prompt_empty = data.get("prompt_empty");
    let input_text = data.get("input_text");

    let mut out = Vec::new();
    if !clean {
        out.push(format!("Screen {rows}x{cols}  cursor ({cr},{cc})"));
        out.push(format!(
            "ready={ready}  prompt_empty={prompt_empty}  input_text={input_text}",
            ready = ready.map(|v| v.to_string()).unwrap_or("null".into()),
            prompt_empty = prompt_empty.map(|v| v.to_string()).unwrap_or("null".into()),
            input_text = input_text
                .map(|v| match v.as_str() {
                    Some(s) => format!("\"{}\"", s),
                    None => v.to_string(),
                })
                .unwrap_or("null".into()),
        ));
        out.push(String::new());
    }

    if let Some(lines) = lines {
        for (i, line) in lines.iter().enumerate() {
            let text = line.as_str().unwrap_or("");
            if clean {
                out.push(text.to_string());
            } else if !text.is_empty() {
                out.push(format!("  {i:3}: {text}"));
            }
        }
    }

    out.join("\n")
}

/// Handle: hcom term debug on|off|logs
fn handle_debug(argv: &[String]) -> i32 {
    let sub = argv.first().map(|s| s.as_str());

    match sub {
        Some("on") => {
            let path = flag_path();
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::File::create(&path);
            println!("PTY debug logging enabled. Running instances pick up within ~10s.");
            0
        }
        Some("off") => {
            let _ = std::fs::remove_file(flag_path());
            println!("PTY debug logging disabled.");
            0
        }
        Some("logs") => list_logs(),
        _ => {
            let status = if flag_path().exists() { "on" } else { "off" };
            println!("PTY debug logging is {status}. Usage: hcom term debug on|off|logs");
            0
        }
    }
}

/// List PTY debug log files.
fn list_logs() -> i32 {
    let debug_dir = hcom_dir().join(".tmp").join("logs").join("pty_debug");
    if !debug_dir.exists() {
        println!("No PTY debug logs found.");
        return 0;
    }

    let mut logs: Vec<(PathBuf, u64)> = std::fs::read_dir(&debug_dir)
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("log"))
                .filter_map(|e| {
                    let size = e.metadata().ok()?.len();
                    Some((e.path(), size))
                })
                .collect()
        })
        .unwrap_or_default();

    if logs.is_empty() {
        println!("No PTY debug logs found.");
        return 0;
    }

    // Sort by modification time, newest first
    logs.sort_by(|a, b| {
        let a_time = std::fs::metadata(&a.0)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let b_time = std::fs::metadata(&b.0)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        b_time.cmp(&a_time)
    });

    let enabled = flag_path().exists();
    println!("Debug logging: {}", if enabled { "ON" } else { "OFF" });
    println!("Log dir: {}", debug_dir.display());
    for (path, size) in &logs {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        println!("  {name}  ({size} bytes)");
    }
    0
}

/// Handle screen query: hcom term [name] [--json]
fn handle_screen(db: &HcomDb, argv: &[String]) -> i32 {
    let raw_json = argv.iter().any(|a| a == "--json");
    let clean = argv.iter().any(|a| a == "--clean");
    let args: Vec<&str> = argv
        .iter()
        .filter(|a| a.as_str() != "--json" && a.as_str() != "--clean")
        .map(|s| s.as_str())
        .collect();
    let name = args.first().copied();

    // Resolve display name if provided
    let name = name.map(|n| resolve_display_name(db, n).unwrap_or_else(|| n.to_string()));

    if let Some(ref name) = name {
        let port = match get_inject_port(db, name) {
            Some(p) => p,
            None => {
                println!("No inject port for '{name}'. Instance not running or not PTY-managed.");
                return 1;
            }
        };
        match query_screen(port) {
            Some(result) => {
                if raw_json {
                    println!("{}", serde_json::to_string(&result).unwrap_or_default());
                } else {
                    println!("{}", format_screen(&result, clean));
                }
                0
            }
            None => {
                println!("No response from '{name}' (port {port}).");
                1
            }
        }
    } else {
        // No name — query all PTY instances
        let instances = get_pty_instances(db);
        if instances.is_empty() {
            println!("No PTY instances found.");
            return 1;
        }

        let mut found = false;
        for (inst_name, port) in &instances {
            if let Some(result) = query_screen(*port) {
                if found {
                    println!();
                }
                if raw_json {
                    let mut merged = result.clone();
                    merged["name"] = serde_json::json!(inst_name);
                    println!("{}", serde_json::to_string(&merged).unwrap_or_default());
                } else {
                    println!("[{inst_name}]");
                    println!("{}", format_screen(&result, clean));
                }
                found = true;
            } else {
                println!("[{inst_name}] not responding (port {port})");
            }
        }

        if found { 0 } else { 1 }
    }
}

pub fn cmd_term(db: &HcomDb, args: &TermArgs, _ctx: Option<&CommandContext>) -> i32 {
    let argv = &args.args;
    let sub = argv.first().map(|s| s.as_str());

    if sub == Some("--help") || sub == Some("-h") {
        println!(
            "hcom term - Terminal admin: screen query, text injection, debug logging\n\n\
             Usage:\n  \
             hcom term                  Query all PTY screens\n  \
             hcom term <name>           Query specific instance screen\n  \
             hcom term <name> --json    JSON output\n  \
             hcom term <name> --clean   Plain text, no header or line numbers\n  \
             hcom term inject <name> [text] [--enter]   Inject text; --enter submits only after\n  \
                                                        the prompt provably shows exactly that text\n  \
                                                        (guarded; refuses over human drafts)\n  \
             hcom term inject <name> --enter --force    Raw enter without ownership proof\n  \
             hcom term debug on|off|logs                 PTY debug logging"
        );
        return 0;
    }

    if sub == Some("inject") {
        let enter = argv.iter().any(|a| a == "--enter");
        let force = argv.iter().any(|a| a == "--force");
        let args: Vec<&str> = argv[1..]
            .iter()
            .filter(|a| a.as_str() != "--enter" && a.as_str() != "--force")
            .map(|s| s.as_str())
            .collect();
        if args.is_empty() {
            println!("Usage: hcom term inject <name> [text] [--enter] [--force]");
            return 1;
        }
        let name = resolve_display_name(db, args[0]).unwrap_or_else(|| args[0].to_string());
        let text = if args.len() > 1 {
            args[1..].join(" ")
        } else {
            String::new()
        };
        if text.is_empty() && !enter {
            println!("Nothing to inject (provide text or --enter)");
            return 1;
        }
        if let Some((base_name, device)) = crate::relay::control::split_device_suffix(&name) {
            return crate::relay::control::dispatch_remote_and_print(
                db,
                device,
                Some(&name),
                crate::relay::control::rpc_action::TERM_INJECT,
                &serde_json::json!({"target": base_name, "text": text, "enter": enter, "force": force}),
                crate::relay::control::RPC_DEFAULT_TIMEOUT,
                "message",
                "Remote term inject completed",
            );
        }
        return inject_text(db, &name, &text, enter, force);
    }

    if sub == Some("debug") {
        return handle_debug(&argv[1..]);
    }

    // Find the first non-flag positional to check for a `name:DEVICE` remote
    // target. `hcom term --json luna:ABCD` must route through the RPC path
    // just like `hcom term luna:ABCD --json`.
    if let Some(name_arg) = argv.iter().find(|arg| !arg.starts_with('-')) {
        let name = resolve_display_name(db, name_arg).unwrap_or_else(|| name_arg.clone());
        if let Some((base_name, device)) = crate::relay::control::split_device_suffix(&name) {
            let raw_json = argv.iter().any(|a| a == "--json");
            let clean = argv.iter().any(|a| a == "--clean");
            return crate::relay::control::dispatch_remote_and_print(
                db,
                device,
                Some(&name),
                crate::relay::control::rpc_action::TERM_SCREEN,
                &serde_json::json!({"target": base_name, "json": raw_json, "clean": clean}),
                crate::relay::control::RPC_DEFAULT_TIMEOUT,
                "content",
                "No remote screen content",
            );
        }
    }

    // Screen query
    handle_screen(db, argv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn test_db() -> HcomDb {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = HcomDb::open_raw(&db_path).unwrap();
        db.init_db().unwrap();
        std::mem::forget(dir);
        db
    }

    #[test]
    fn test_format_screen() {
        let data = serde_json::json!({
            "lines": ["hello", "", "world"],
            "cursor": [2, 5],
            "size": [24, 80],
            "ready": true,
            "prompt_empty": false,
            "input_text": "test",
        });
        let result = format_screen(&data, false);
        assert!(result.contains("Screen 24x80"));
        assert!(result.contains("cursor (2,5)"));
        assert!(result.contains("hello"));
        assert!(result.contains("world"));
    }

    #[test]
    fn test_flag_path() {
        // Just verify it returns something sensible
        let path = flag_path();
        assert!(path.to_string_lossy().contains("pty_debug_on"));
    }

    #[test]
    fn test_remote_term_screen_positional_detection_skips_leading_flags() {
        // The remote fast-path in cmd_term must locate the `name:DEVICE`
        // positional even when flags (e.g. `--json`) precede it. Mirrors the
        // scan used at the top of cmd_term's term_screen branch.
        fn first_positional(argv: &[String]) -> Option<&String> {
            argv.iter().find(|arg| !arg.starts_with('-'))
        }

        let name_only = vec!["luna:ABCD".to_string()];
        assert_eq!(
            first_positional(&name_only).map(String::as_str),
            Some("luna:ABCD")
        );

        let json_first = vec!["--json".to_string(), "luna:ABCD".to_string()];
        assert_eq!(
            first_positional(&json_first).map(String::as_str),
            Some("luna:ABCD")
        );

        let json_after = vec!["luna:ABCD".to_string(), "--json".to_string()];
        assert_eq!(
            first_positional(&json_after).map(String::as_str),
            Some("luna:ABCD")
        );

        let flags_only = vec!["--json".to_string()];
        assert_eq!(first_positional(&flags_only), None);
    }

    #[test]
    fn test_inject_text_remote_result_matches_cli_feedback() {
        let db = test_db();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port() as i32;
        db.conn()
            .execute(
                "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?1, 'inject', ?2, 0)",
                rusqlite::params!["luna", port],
            )
            .unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).unwrap();
            buf
        });

        let result = inject_text_remote_result(&db, "luna", "status", false, false).unwrap();
        let received = handle.join().unwrap();

        assert_eq!(result, "Injected 6 chars to luna");
        assert_eq!(received, "status");
    }

    #[test]
    fn test_read_instance_screen_formats_contract_output() {
        let db = test_db();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port() as i32;
        db.conn()
            .execute(
                "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?1, 'inject', ?2, 0)",
                rusqlite::params!["luna", port],
            )
            .unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).unwrap();
            assert_eq!(request, b"\x00SCREEN\n");
            stream
                .write_all(
                    serde_json::json!({
                        "lines": ["hello", "", "world"],
                        "cursor": [2, 5],
                        "size": [24, 80],
                        "ready": true,
                        "prompt_empty": false,
                        "input_text": "status",
                    })
                    .to_string()
                    .as_bytes(),
                )
                .unwrap();
        });

        let rendered = read_instance_screen(&db, "luna", false, false).unwrap();
        handle.join().unwrap();

        assert!(rendered.contains("Screen 24x80  cursor (2,5)"));
        assert!(rendered.contains("ready=true  prompt_empty=false  input_text=\"status\""));
        assert!(rendered.contains("  0: hello"));
        assert!(rendered.contains("  2: world"));
    }

    // ---- guarded submit contract (client side) ----

    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// Fake proxy: answers one scripted response per connection, records every
    /// request frame. When the script runs out the listener closes, so further
    /// client connects fail fast instead of hanging.
    fn spawn_fake_proxy(
        db: &HcomDb,
        name: &str,
        responses: Vec<&str>,
    ) -> (Arc<Mutex<Vec<String>>>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port() as i32;
        db.conn()
            .execute(
                "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?1, 'inject', ?2, 0)",
                rusqlite::params![name, port],
            )
            .unwrap();
        let mut script: VecDeque<String> = responses.into_iter().map(|s| s.to_string()).collect();
        let frames: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorded = frames.clone();
        let handle = thread::spawn(move || {
            while let Some(response) = script.pop_front() {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut request = Vec::new();
                let _ = stream.read_to_end(&mut request);
                recorded
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request).into_owned());
                let _ = stream.write_all(response.as_bytes());
            }
        });
        (frames, handle)
    }

    fn screen_json(
        input_state: &str,
        input_text: Option<&str>,
        user_gen: u64,
        epoch: u64,
    ) -> String {
        serde_json::json!({
            "lines": [], "size": [24, 80], "cursor": [0, 0],
            "ready": true,
            "prompt_empty": input_text == Some(""),
            "input_parser_supported": input_state != "unsupported",
            "input_state": input_state,
            "user_gen": user_gen,
            "input_epoch": epoch,
            "approval_waiting": false,
            "input_text": input_text,
        })
        .to_string()
    }

    fn assert_no_cr(frames: &Arc<Mutex<Vec<String>>>) {
        let frames = frames.lock().unwrap();
        assert!(
            !frames.iter().any(|f| f == "\r" || f.starts_with('\r')),
            "a raw CR must never be written in the guarded flow: {frames:?}"
        );
    }

    #[test]
    fn guarded_enter_sends_submit_only_after_exact_render() {
        let db = test_db();
        let (frames, handle) = spawn_fake_proxy(
            &db,
            "luna",
            vec![
                &screen_json("text", Some(""), 1, 5),
                "OK epoch=6 gen=1\n",
                &screen_json("text", Some("hello"), 1, 6),
                "OK epoch=7 gen=1\n",
            ],
        );

        let result = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap();
        handle.join().unwrap();

        assert!(result.contains("guarded enter"));
        let recorded = frames.lock().unwrap();
        assert!(recorded[1].starts_with("\u{0}INJECT_IF "));
        assert!(recorded[1].contains("\"require_empty\":true"));
        assert!(recorded[3].starts_with("\u{0}SUBMIT_IF "));
        assert!(recorded[3].contains("\"expected_text\":\"hello\""));
        assert!(recorded[3].contains("\"input_epoch\":6"));
        assert!(!recorded.iter().any(|f| f == "\r"));
    }

    #[test]
    fn guarded_enter_refuses_unsupported_tool_without_any_write() {
        let db = test_db();
        let (frames, handle) =
            spawn_fake_proxy(&db, "luna", vec![&screen_json("unsupported", None, 0, 0)]);

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("no prompt parser"));
        assert_eq!(frames.lock().unwrap().len(), 1, "only the screen query");
        assert_no_cr(&frames);
    }

    #[test]
    fn guarded_enter_refuses_unavailable_prompt() {
        let db = test_db();
        let (frames, handle) =
            spawn_fake_proxy(&db, "luna", vec![&screen_json("unavailable", None, 0, 0)]);

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("enter NOT sent"));
        assert_eq!(frames.lock().unwrap().len(), 1);
        assert_no_cr(&frames);
    }

    #[test]
    fn guarded_enter_refuses_nonempty_prompt() {
        // A human draft is in the box: neither text nor CR may be written.
        let db = test_db();
        let (frames, handle) =
            spawn_fake_proxy(&db, "luna", vec![&screen_json("text", Some("draft"), 1, 5)]);

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("not empty"));
        assert_eq!(frames.lock().unwrap().len(), 1);
        assert_no_cr(&frames);
    }

    #[test]
    fn guarded_enter_only_requires_force() {
        let db = test_db();
        let (frames, handle) = spawn_fake_proxy(&db, "luna", vec![]);

        let err = inject_text_remote_result(&db, "luna", "", true, false).unwrap_err();
        drop(handle);

        assert!(err.contains("--force"));
        assert!(frames.lock().unwrap().is_empty(), "no connection at all");
    }

    #[test]
    fn guarded_enter_propagates_server_refusal_without_cr() {
        let db = test_db();
        let (frames, handle) = spawn_fake_proxy(
            &db,
            "luna",
            vec![
                &screen_json("text", Some(""), 1, 5),
                "OK epoch=6 gen=1\n",
                &screen_json("text", Some("hello"), 1, 6),
                "REFUSED stale_user_gen\n",
            ],
        );

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("stale_user_gen"));
        assert!(err.contains("enter NOT sent"));
        assert_no_cr(&frames);
    }

    #[test]
    fn guarded_enter_refuses_when_inject_is_refused() {
        let db = test_db();
        let (frames, handle) = spawn_fake_proxy(
            &db,
            "luna",
            vec![
                &screen_json("text", Some(""), 1, 5),
                "REFUSED prompt_not_empty\n",
            ],
        );

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("nothing was written"));
        assert_eq!(frames.lock().unwrap().len(), 2);
        assert_no_cr(&frames);
    }

    #[test]
    fn guarded_enter_refuses_legacy_proxy_without_contract_fields() {
        // An old proxy answers SCREEN without input_state/user_gen: fail
        // closed rather than guessing.
        let db = test_db();
        let legacy = serde_json::json!({
            "lines": [], "size": [24, 80], "cursor": [0, 0],
            "ready": true, "prompt_empty": true, "input_text": "",
        })
        .to_string();
        let (frames, handle) = spawn_fake_proxy(&db, "luna", vec![&legacy]);

        let err = inject_text_remote_result(&db, "luna", "hello", true, false).unwrap_err();
        handle.join().unwrap();

        assert!(err.contains("enter NOT sent") || err.contains("--force"));
        assert_eq!(frames.lock().unwrap().len(), 1);
        assert_no_cr(&frames);
    }

    #[test]
    fn wait_for_exact_render_times_out_quickly_without_match() {
        let db = test_db();
        let (_frames, handle) =
            spawn_fake_proxy(&db, "luna", vec![&screen_json("text", Some("other"), 1, 5)]);
        let port = get_inject_port(&db, "luna").unwrap();

        let start = Instant::now();
        assert!(!wait_for_exact_render(
            port,
            "hello",
            Duration::from_millis(80)
        ));
        assert!(start.elapsed() < Duration::from_secs(1));
        handle.join().unwrap();
    }
}
