//! Shared hook infrastructure for all tools (Claude, Gemini, Codex, OpenCode, Kilo, Pi, Oh My Pi, Antigravity, Cursor, Kimi, Copilot).

pub mod antigravity;
pub mod claude;
pub mod codex;
pub mod codex_file_edits;
pub mod common;
pub mod copilot;
pub mod cursor;
pub mod family;
pub mod gemini;
pub mod kimi;
pub mod opencode;
pub mod pi;
pub mod utils;

use serde_json::Value;

/// Shared test helpers for hook test modules (claude, codex, gemini).
#[cfg(test)]
pub mod test_helpers {
    use std::cell::Cell;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

    // Process-global serialization for tests that mutate environment or cwd.
    // Without this, parallel tests can observe a partial multi-variable update
    // or resolve a subprocess through another test's temporary PATH.
    // Recover from poison so a panic in one test doesn't cascade-fail the
    // next — the shared state is just "one set of env vars at a time."
    static TEST_ENV_LOCK: OnceLock<RwLock<()>> = OnceLock::new();

    thread_local! {
        static TEST_ENV_READ_DEPTH: Cell<usize> = const { Cell::new(0) };
        static TEST_ENV_WRITE_DEPTH: Cell<usize> = const { Cell::new(0) };
    }

    // Every literal process-global key mutated by the unit-test binary, plus
    // dynamic config and terminal-detection keys used by local test helpers.
    // Local guards may save additional dynamic keys, but they must still hold
    // EnvGuard so all writers share TEST_ENV_LOCK.
    const TEST_ENV_KEYS: &[&str] = &[
        "ANTIGRAVITY_AGENT",
        "CARGO_TARGET_DIR",
        "CARGO_TEST_PARENT",
        "CI",
        "CLAUDECODE",
        "CLAUDE_CONFIG_DIR",
        "CODEX_HOME",
        "CODEX_THREAD_ID",
        "COPILOT_HOME",
        "CURSOR_CONFIG_DIR",
        "GEMINI_API_KEY",
        "GEMINI_CLI_HOME",
        "GEMINI_PTY_INFO",
        "HERDR_ENV",
        "HERDR_PANE_ID",
        "HERDR_SOCKET_PATH",
        "HCOM_AUTO_SUBSCRIBE",
        "HCOM_CHAIN_CODEX_VERSION",
        "HCOM_CHAIN_GENERATION",
        "HCOM_CHAIN_HANDOFF_ID",
        "HCOM_CHAIN_ID",
        "HCOM_CHAIN_LAUNCH_NONCE",
        "HCOM_CHAIN_PROCESS_BIRTH_IDENTITY",
        "HCOM_DEV_ROOT",
        "HCOM_DIR",
        "HCOM_INSTANCE_NAME",
        "HCOM_LAUNCHED_PRESET",
        "HCOM_PANE_TITLE",
        "HCOM_PROCESS_ID",
        "HCOM_TAG",
        "HCOM_TERMINAL",
        "HCOM_TEST_CODEX_CLI_VERSION",
        "HCOM_TIMEOUT",
        "HCOM_TOOL",
        "HOME",
        "KILO_CONFIG_DIR",
        "KIMI_CODE_HOME",
        "KITTY_LISTEN_ON",
        "KITTY_WINDOW_ID",
        "NO_COLOR",
        "OMP_PROFILE",
        "OPENCODE_CONFIG_DIR",
        "OPENROUTER_API_KEY",
        "PATH",
        "PI_CODING_AGENT_DIR",
        "PI_CODING_AGENT_SESSION_DIR",
        "PI_CONFIG_DIR",
        "PI_PROFILE",
        "RORI_BACKGROUND_PARENT",
        "RORI_PARENT_SENTINEL",
        "RORI_PARENT_VALUE",
        "RORI_TEST_MY_VAR",
        "RORI_TEST_OPENROUTER_API_KEY",
        "RORI_TEST_PI_OFFLINE",
        "TMUX_PANE",
        "WEZTERM_PANE",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "ZELLIJ_PANE_ID",
        "https_proxy",
        // Terminal identity and detection variables.
        "ALACRITTY_WINDOW_ID",
        "CMUX_SURFACE_ID",
        "CMUX_WORKSPACE_ID",
        "GNOME_TERMINAL_SCREEN",
        "GHOSTTY_RESOURCES_DIR",
        "ITERM_SESSION_ID",
        "KITTY_PID",
        "KONSOLE_DBUS_WINDOW",
        "TERM_PROGRAM",
        "TERM_SESSION_ID",
        "TERMINATOR_UUID",
        "TILIX_ID",
        "WAVETERM_BLOCKID",
        "WT_SESSION",
    ];

    fn acquire_env_lock() -> RwLockWriteGuard<'static, ()> {
        TEST_ENV_LOCK
            .get_or_init(|| RwLock::new(()))
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    pub struct EnvReadGuard {
        _lock: Option<RwLockReadGuard<'static, ()>>,
    }

    pub fn process_env_read() -> EnvReadGuard {
        let write_depth = TEST_ENV_WRITE_DEPTH.with(Cell::get);
        let read_depth = TEST_ENV_READ_DEPTH.with(Cell::get);
        let lock = if write_depth > 0 || read_depth > 0 {
            None
        } else {
            Some(
                TEST_ENV_LOCK
                    .get_or_init(|| RwLock::new(()))
                    .read()
                    .unwrap_or_else(|error| error.into_inner()),
            )
        };
        TEST_ENV_READ_DEPTH.with(|depth| depth.set(read_depth + 1));
        EnvReadGuard { _lock: lock }
    }

    impl Drop for EnvReadGuard {
        fn drop(&mut self) {
            TEST_ENV_READ_DEPTH.with(|depth| {
                let current = depth.get();
                debug_assert!(current > 0);
                depth.set(current.saturating_sub(1));
            });
        }
    }

    /// RAII guard that serializes process-global test mutation, restores the
    /// complete shared env/cwd snapshot on unwind, and resets Config.
    pub struct EnvGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
        saved_cwd: Option<PathBuf>,
        // Declared last so it drops AFTER Drop::drop restores env vars,
        // releasing the lock only once this test's env state is gone.
        _lock: Option<RwLockWriteGuard<'static, ()>>,
    }

    impl Default for EnvGuard {
        fn default() -> Self {
            Self::new()
        }
    }

    impl EnvGuard {
        pub fn new() -> Self {
            let write_depth = TEST_ENV_WRITE_DEPTH.with(Cell::get);
            let read_depth = TEST_ENV_READ_DEPTH.with(Cell::get);
            assert!(
                write_depth > 0 || read_depth == 0,
                "test environment write guard cannot upgrade an active read guard"
            );
            let lock = (write_depth == 0).then(acquire_env_lock);
            TEST_ENV_WRITE_DEPTH.with(|depth| depth.set(write_depth + 1));
            Self {
                saved: TEST_ENV_KEYS
                    .iter()
                    .map(|key| (*key, std::env::var_os(key)))
                    .collect(),
                saved_cwd: std::env::current_dir().ok(),
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                for (key, value) in &self.saved {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
            if let Some(cwd) = &self.saved_cwd {
                let _ = std::env::set_current_dir(cwd);
            }
            crate::config::Config::reset();
            crate::config::Config::init();
            TEST_ENV_WRITE_DEPTH.with(|depth| {
                let current = depth.get();
                debug_assert!(current > 0);
                depth.set(current.saturating_sub(1));
            });
        }
    }

    /// Create an isolated test env: tempdir with .hcom dir, env vars set.
    /// Returns (tempdir, hcom_dir, test_home, guard).
    pub fn isolated_test_env() -> (tempfile::TempDir, PathBuf, PathBuf, EnvGuard) {
        let guard = EnvGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let test_home = dir.path().to_path_buf();
        let hcom_dir = test_home.join(".hcom");
        std::fs::create_dir_all(&hcom_dir).unwrap();
        unsafe {
            std::env::set_var("HCOM_DIR", &hcom_dir);
            std::env::set_var("HOME", &test_home);
            std::env::remove_var("CLAUDE_CONFIG_DIR");
            std::env::remove_var("CODEX_HOME");
            std::env::set_var("HCOM_TEST_CODEX_CLI_VERSION", "codex-cli 0.129.0");
        }
        crate::config::Config::reset();
        crate::config::Config::init();
        (dir, hcom_dir, test_home, guard)
    }

    #[test]
    fn nested_env_guards_restore_each_scope_without_deadlock() {
        let original = std::env::var_os("OPENCODE_CONFIG_DIR");
        {
            let _outer = EnvGuard::new();
            unsafe {
                std::env::set_var("OPENCODE_CONFIG_DIR", "outer");
            }
            {
                let _inner = EnvGuard::new();
                unsafe {
                    std::env::set_var("OPENCODE_CONFIG_DIR", "inner");
                }
            }
            assert_eq!(
                std::env::var_os("OPENCODE_CONFIG_DIR"),
                Some(OsString::from("outer"))
            );
        }
        assert_eq!(std::env::var_os("OPENCODE_CONFIG_DIR"), original);
    }

    #[test]
    fn poisoned_env_lock_restores_before_reuse() {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            let _guard = EnvGuard::new();
            sender
                .send(std::env::var_os("OPENCODE_CONFIG_DIR"))
                .unwrap();
            unsafe {
                std::env::set_var("OPENCODE_CONFIG_DIR", "panic-value");
            }
            panic!("intentional test lock poison");
        });
        let original = receiver.recv().unwrap();
        assert!(thread.join().is_err());

        let _read = process_env_read();
        assert_eq!(std::env::var_os("OPENCODE_CONFIG_DIR"), original);
    }

    #[test]
    fn env_reader_waits_for_atomic_writer_snapshot() {
        let original = crate::runtime_env::user_config_home();
        let (writer_ready_tx, writer_ready_rx) = std::sync::mpsc::sync_channel(1);
        let (writer_release_tx, writer_release_rx) = std::sync::mpsc::sync_channel(1);
        let writer = std::thread::spawn(move || {
            let _guard = EnvGuard::new();
            unsafe {
                std::env::set_var("HOME", "/tmp/hcom-env-writer-home");
                std::env::set_var("XDG_CONFIG_HOME", "/tmp/hcom-env-writer-xdg");
            }
            writer_ready_tx.send(()).unwrap();
            writer_release_rx.recv().unwrap();
        });
        writer_ready_rx.recv().unwrap();

        let (reader_started_tx, reader_started_rx) = std::sync::mpsc::sync_channel(1);
        let (reader_result_tx, reader_result_rx) = std::sync::mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            reader_started_tx.send(()).unwrap();
            reader_result_tx
                .send(crate::runtime_env::user_config_home())
                .unwrap();
        });
        reader_started_rx.recv().unwrap();
        assert!(
            reader_result_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "reader observed a writer's partial process environment"
        );

        writer_release_tx.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(reader_result_rx.recv().unwrap(), original);
        reader.join().unwrap();
    }
}

// Re-export key types.
pub use common::{
    deliver_pending_messages, finalize_session, find_last_bind_marker, get_pending_instances,
    init_hook_context, inject_bootstrap_once, poll_messages, stop_instance,
};
pub use family::{bind_vanilla_instance, extract_tool_detail};
pub use utils::{HOOK_REGISTRY, HookCategory, HookInfo};

/// Delivery cursor/status update to apply after hook output is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAck {
    pub instance_name: String,
    pub last_event_id: i64,
    pub status_context: String,
    pub msg_ts: String,
}

/// Normalized hook payload — unified across all tools.
///
/// Each tool's raw hook JSON is different. Factory methods normalize into
/// this common struct so shared functions work identically across tools.
///
#[derive(Debug, Clone)]
pub struct HookPayload {
    /// Claude/Gemini session ID, Codex thread ID. None if not provided.
    pub session_id: Option<String>,
    /// Path to tool's JSONL transcript (Claude) or conversation log. None if not provided.
    pub transcript_path: Option<String>,
    /// Hook name (e.g., "Stop", "PostToolUse", "PreToolUse").
    pub hook_name: String,
    /// Tool type string ("claude", "gemini", "codex", "opencode", "kilo", "pi", "omp", "antigravity", "cursor", "kimi", "copilot").
    pub tool: String,
    /// Tool name from hook (e.g., "Bash", "Write" for PostToolUse).
    pub tool_name: String,
    /// Tool input dict (for extract_tool_detail).
    pub tool_input: Value,
    /// Tool result/response (for AfterTool/PostToolUse hooks).
    pub tool_result: String,
    /// Notification type (for Notification hooks, e.g., "ToolPermission").
    pub notification_type: Option<String>,
    /// Raw hook payload for tool-specific access.
    pub raw: Value,
}

impl HookPayload {
    /// Extract a string from the first matching key, or empty string.
    fn str_field(raw: &Value, keys: &[&str]) -> String {
        for key in keys {
            if let Some(s) = raw.get(*key).and_then(|v| v.as_str()) {
                return s.to_string();
            }
        }
        String::new()
    }

    /// Extract an optional string from the first matching key.
    fn opt_str_field(raw: &Value, keys: &[&str]) -> Option<String> {
        for key in keys {
            if let Some(s) = raw.get(*key).and_then(|v| v.as_str())
                && !s.is_empty()
            {
                return Some(s.to_string());
            }
        }
        None
    }

    /// Extract a value from the first matching key, or empty object.
    fn obj_field(raw: &Value, keys: &[&str]) -> Value {
        for key in keys {
            if let Some(v) = raw.get(*key) {
                return v.clone();
            }
        }
        Value::Object(Default::default())
    }

    /// Build from Claude hook JSON.
    ///
    /// Claude hook stdin format (all keys at root level):
    ///   { "session_id", "transcript_path", "tool_name", "tool_input",
    ///     "tool_response", "notification_type", "agent_id", "agent_type" }
    pub fn from_claude(raw: Value) -> Self {
        let tool_result = match raw.get("tool_response") {
            Some(Value::Object(obj)) => obj
                .get("stdout")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };

        Self {
            session_id: Self::opt_str_field(&raw, &["session_id", "sessionId"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path"]),
            hook_name: Self::str_field(&raw, &["hook_name"]),
            tool: "claude".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name"]),
            tool_input: Self::obj_field(&raw, &["tool_input"]),
            tool_result,
            notification_type: Self::opt_str_field(&raw, &["notification_type"]),
            raw,
        }
    }

    /// Build from Gemini hook JSON.
    ///
    /// Gemini hook stdin format (all keys at root level):
    ///   { "session_id"/"sessionId", "transcript_path"/"session_path",
    ///     "tool_name"/"toolName", "tool_input"/"toolInput",
    ///     "tool_response", "notification_type" }
    pub fn from_gemini(raw: Value) -> Self {
        let tool_result = match raw.get("tool_response") {
            Some(Value::Object(obj)) => obj
                .get("llmContent")
                .or_else(|| obj.get("output"))
                .or_else(|| obj.get("response").and_then(|r| r.get("output")))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            Some(v) => v
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| v.to_string()),
            None => String::new(),
        };

        Self {
            session_id: Self::opt_str_field(&raw, &["session_id", "sessionId"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path", "session_path"]),
            hook_name: Self::str_field(&raw, &["hook_name"]),
            tool: "gemini".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name", "toolName"]),
            tool_input: Self::obj_field(&raw, &["tool_input", "toolInput"]),
            tool_result,
            notification_type: Self::opt_str_field(&raw, &["notification_type"]),
            raw,
        }
    }

    /// Build from Antigravity hook JSON.
    ///
    /// Antigravity stdin format (nested toolCall):
    ///   { "conversationId", "transcriptPath", "stepIdx",
    ///     "toolCall": { "name", "args": { ... } },
    ///     "workspacePaths", "artifactDirectoryPath" }
    pub fn from_antigravity(raw: Value, hook_name: &str) -> Self {
        let tool_call = raw.get("toolCall").cloned().unwrap_or_default();
        let tool_name = tool_call
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tool_input = tool_call
            .get("args")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));

        Self {
            session_id: Self::opt_str_field(&raw, &["conversationId"]),
            transcript_path: Self::opt_str_field(&raw, &["transcriptPath"]),
            hook_name: hook_name.to_string(),
            tool: "antigravity".to_string(),
            tool_name,
            tool_input,
            tool_result: String::new(),
            notification_type: None,
            raw,
        }
    }

    /// Build from native Codex hook JSON.
    ///
    /// Codex hooks pass JSON on stdin with snake_case fields such as:
    ///   { "session_id", "transcript_path", "hook_event_name",
    ///     "tool_name", "tool_input", "tool_response", "prompt", "source" }
    pub fn from_codex_native(hook_type: &str, raw: Value) -> Self {
        Self {
            session_id: Self::opt_str_field(&raw, &["session_id"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path", "session_path"]),
            hook_name: if hook_type.is_empty() {
                Self::str_field(&raw, &["hook_event_name"])
            } else {
                hook_type.to_string()
            },
            tool: "codex".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name"]),
            tool_input: Self::obj_field(&raw, &["tool_input"]),
            tool_result: match raw.get("tool_response") {
                Some(Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            },
            notification_type: None,
            raw,
        }
    }

    /// Build from Kimi Code CLI hook JSON.
    ///
    /// Kimi hooks pass JSON on stdin with snake_case fields such as:
    ///   { "session_id", "hook_event_name", "tool_name", "tool_input",
    ///     "tool_output", "prompt", "source", "cwd" }
    pub fn from_kimi(hook_type: &str, raw: Value) -> Self {
        Self {
            session_id: Self::opt_str_field(&raw, &["session_id"]),
            transcript_path: None,
            hook_name: if hook_type.is_empty() {
                Self::str_field(&raw, &["hook_event_name"])
            } else {
                hook_type.to_string()
            },
            tool: "kimi".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name"]),
            tool_input: Self::obj_field(&raw, &["tool_input"]),
            tool_result: match raw.get("tool_output") {
                Some(Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            },
            notification_type: Self::opt_str_field(&raw, &["notification_type", "sink"]),
            raw,
        }
    }

    /// Build from native Cursor Agent hook JSON.
    ///
    /// Cursor hooks use snake_case and include a common conversation ID on
    /// every agent hook. `sessionStart` also includes the same value as
    /// `session_id`.
    pub fn from_cursor_native(hook_type: &str, raw: Value) -> Self {
        Self {
            session_id: Self::opt_str_field(&raw, &["session_id", "conversation_id"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path"]),
            hook_name: hook_type.to_string(),
            tool: "cursor".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name"]),
            tool_input: Self::obj_field(&raw, &["tool_input"]),
            tool_result: match raw.get("tool_output") {
                Some(Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            },
            notification_type: None,
            raw,
        }
    }

    /// Build from GitHub Copilot CLI native hook JSON.
    ///
    /// PascalCase hook names yield mostly snake_case payloads. `Notification`
    /// is mixed-cased in current Copilot builds, so accept both styles.
    pub fn from_copilot_native(hook_type: &str, raw: Value) -> Self {
        let tool_result = raw
            .get("tool_result")
            .or_else(|| raw.get("toolResult"))
            .and_then(|v| {
                v.get("text_result_for_llm")
                    .or_else(|| v.get("textResultForLlm"))
                    .or_else(|| v.get("output"))
                    .or_else(|| v.get("text"))
                    .or(Some(v))
            })
            .map(|v| {
                v.as_str()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| v.to_string())
            })
            .unwrap_or_default();

        Self {
            session_id: Self::opt_str_field(&raw, &["session_id", "sessionId"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path", "transcriptPath"]),
            hook_name: if hook_type.is_empty() {
                Self::str_field(&raw, &["hook_event_name", "hookEventName"])
            } else {
                hook_type.to_string()
            },
            tool: "copilot".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name", "toolName"]),
            tool_input: Self::obj_field(&raw, &["tool_input", "toolInput"]),
            tool_result,
            notification_type: Self::opt_str_field(
                &raw,
                &["notification_type", "notificationType"],
            ),
            raw,
        }
    }

    /// Build from OpenCode hook JSON.
    ///
    /// OpenCode hooks: session_id from env, minimal tool info.
    pub fn from_opencode(raw: Value) -> Self {
        Self {
            session_id: Self::opt_str_field(&raw, &["session_id"]),
            transcript_path: Self::opt_str_field(&raw, &["transcript_path"]),
            hook_name: Self::str_field(&raw, &["hook_name"]),
            tool: "opencode".to_string(),
            tool_name: Self::str_field(&raw, &["tool_name"]),
            tool_input: Self::obj_field(&raw, &["tool_input"]),
            tool_result: String::new(),
            notification_type: None,
            raw,
        }
    }
}

/// Hook handler result — determines exit code and stdout output.
///
/// the dispatcher into exit codes + JSON output.
#[derive(Debug, Clone)]
pub enum HookResult {
    /// Allow the operation (exit 0, optional additionalContext/systemMessage).
    Allow {
        /// Additional context injected into the model's context window.
        additional_context: Option<String>,
        /// System message update (Claude-specific).
        system_message: Option<String>,
        /// Delivery ack to commit after stdout is successfully written.
        delivery_ack: Option<DeliveryAck>,
    },

    /// Block the operation (exit 2, with reason for blocking).
    /// Used by Stop hook to deliver messages.
    Block {
        /// Reason text (formatted messages for delivery).
        reason: String,
    },

    /// Update the tool input before execution (exit 0, updatedInput field).
    /// Used by PreToolUse to modify tool arguments.
    UpdateInput {
        /// Modified tool input JSON.
        updated_input: Value,
    },
}

impl HookResult {
    /// Exit code for this result.
    pub fn exit_code(&self) -> i32 {
        match self {
            HookResult::Allow { .. } => 0,
            HookResult::Block { .. } => 2,
            HookResult::UpdateInput { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_payload_from_claude() {
        // Matches actual Claude hook stdin: all keys at root level
        let raw = serde_json::json!({
            "session_id": "sess-123",
            "transcript_path": "/tmp/transcript.jsonl",
            "hook_name": "PostToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "ls"}
        });
        let payload = HookPayload::from_claude(raw);
        assert_eq!(payload.session_id.as_deref(), Some("sess-123"));
        assert_eq!(
            payload.transcript_path.as_deref(),
            Some("/tmp/transcript.jsonl")
        );
        assert_eq!(payload.hook_name, "PostToolUse");
        assert_eq!(payload.tool, "claude");
        assert_eq!(payload.tool_name, "Bash");
        assert_eq!(payload.notification_type, None);
    }

    #[test]
    fn test_hook_payload_from_gemini() {
        // Matches actual Gemini hook stdin: tool_name/tool_input at root
        let raw = serde_json::json!({
            "session_id": "gem-456",
            "hook_name": "after_tool_call",
            "tool_name": "run_shell_command",
            "tool_input": {"command": "echo hi"}
        });
        let payload = HookPayload::from_gemini(raw);
        assert_eq!(payload.session_id.as_deref(), Some("gem-456"));
        assert_eq!(payload.tool, "gemini");
        assert_eq!(payload.tool_name, "run_shell_command");
        assert_eq!(payload.tool_input["command"], "echo hi");
    }

    #[test]
    fn test_hook_payload_from_antigravity() {
        let raw = serde_json::json!({
            "conversationId": "6f000787-c5d3-4485-b266-142a15f7d79d",
            "transcriptPath": "/tmp/transcript.jsonl",
            "toolCall": {
                "name": "run_command",
                "args": { "CommandLine": "echo hi", "Cwd": "/tmp" }
            }
        });
        let payload = HookPayload::from_antigravity(raw, "gemini-beforetool");
        assert_eq!(
            payload.session_id.as_deref(),
            Some("6f000787-c5d3-4485-b266-142a15f7d79d")
        );
        assert_eq!(payload.tool, "antigravity");
        assert_eq!(payload.tool_name, "run_command");
        assert_eq!(payload.tool_input["CommandLine"], "echo hi");
        assert_eq!(payload.hook_name, "gemini-beforetool");
    }

    #[test]
    fn test_hook_payload_from_antigravity_no_toolcall() {
        let raw = serde_json::json!({"conversationId": "abc-123"});
        let payload = HookPayload::from_antigravity(raw, "gemini-sessionstart");
        assert_eq!(payload.tool_name, "");
        assert!(payload.tool_input.is_object());
        assert_eq!(payload.hook_name, "gemini-sessionstart");
    }

    #[test]
    fn test_hook_payload_from_codex() {
        // Matches native Codex stdin payload
        let raw = serde_json::json!({
            "session_id": "thread-789",
            "tool_name": "Bash",
            "tool_input": {"command": "pwd"},
            "tool_response": {"output": "ok"}
        });
        let payload = HookPayload::from_codex_native("PostToolUse", raw);
        assert_eq!(payload.session_id.as_deref(), Some("thread-789"));
        assert_eq!(payload.tool, "codex");
        assert_eq!(payload.hook_name, "PostToolUse");
        assert_eq!(payload.tool_name, "Bash");
        assert_eq!(payload.tool_input["command"], "pwd");
    }

    #[test]
    fn test_hook_payload_from_opencode() {
        let raw = serde_json::json!({
            "session_id": "oc-111",
            "hook_name": "PostToolUse",
            "tool_name": "bash",
            "tool_input": {"command": "pwd"}
        });
        let payload = HookPayload::from_opencode(raw);
        assert_eq!(payload.session_id.as_deref(), Some("oc-111"));
        assert_eq!(payload.tool, "opencode");
        assert_eq!(payload.tool_name, "bash");
    }

    #[test]
    fn test_hook_payload_from_copilot_mixed_notification() {
        let raw = serde_json::json!({
            "sessionId": "cop-1",
            "hook_event_name": "Notification",
            "notification_type": "agent_idle"
        });
        let payload = HookPayload::from_copilot_native("Notification", raw);
        assert_eq!(payload.session_id.as_deref(), Some("cop-1"));
        assert_eq!(payload.tool, "copilot");
        assert_eq!(payload.notification_type.as_deref(), Some("agent_idle"));
    }

    #[test]
    fn test_hook_payload_missing_fields() {
        let raw = serde_json::json!({});
        let payload = HookPayload::from_claude(raw);
        assert_eq!(payload.session_id, None);
        assert_eq!(payload.transcript_path, None);
        assert_eq!(payload.tool_name, "");
    }

    #[test]
    fn test_hook_payload_from_gemini_camelcase_fallbacks() {
        // sessionId fallback
        let raw = serde_json::json!({
            "sessionId": "gem-camel",
            "session_path": "/tmp/gemini/chat.json",
            "hook_name": "BeforeAgent"
        });
        let payload = HookPayload::from_gemini(raw);
        assert_eq!(payload.session_id.as_deref(), Some("gem-camel"));
        assert_eq!(
            payload.transcript_path.as_deref(),
            Some("/tmp/gemini/chat.json")
        );
    }

    #[test]
    fn test_hook_payload_from_gemini_tool_response_string() {
        // String tool_response should not be JSON-quoted
        let raw = serde_json::json!({
            "session_id": "gem-1",
            "tool_response": "plain text output"
        });
        let payload = HookPayload::from_gemini(raw);
        assert_eq!(payload.tool_result, "plain text output");
    }

    #[test]
    fn test_hook_payload_from_claude_notification_type() {
        let raw = serde_json::json!({
            "session_id": "claude-1",
            "hook_name": "Notification",
            "notification_type": "permission_prompt",
            "message": "Claude needs your permission to use Bash"
        });
        let payload = HookPayload::from_claude(raw);
        assert_eq!(
            payload.notification_type.as_deref(),
            Some("permission_prompt")
        );
    }

    #[test]
    fn test_hook_result_allow() {
        let result = HookResult::Allow {
            additional_context: Some("bootstrap text".into()),
            system_message: None,
            delivery_ack: None,
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::Allow {
                additional_context,
                system_message,
                delivery_ack,
            } => {
                assert_eq!(additional_context.as_deref(), Some("bootstrap text"));
                assert!(system_message.is_none());
                assert!(delivery_ack.is_none());
            }
            _ => panic!("expected Allow"),
        }
    }

    #[test]
    fn test_hook_result_allow_empty() {
        let result = HookResult::Allow {
            additional_context: None,
            system_message: None,
            delivery_ack: None,
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::Allow {
                additional_context,
                system_message,
                delivery_ack,
            } => {
                assert!(additional_context.is_none());
                assert!(system_message.is_none());
                assert!(delivery_ack.is_none());
            }
            _ => panic!("expected Allow"),
        }
    }

    #[test]
    fn test_hook_result_block() {
        let result = HookResult::Block {
            reason: "<hcom>message here</hcom>".into(),
        };
        assert_eq!(result.exit_code(), 2);
        match &result {
            HookResult::Block { reason } => {
                assert_eq!(reason, "<hcom>message here</hcom>");
            }
            _ => panic!("expected Block"),
        }
    }

    #[test]
    fn test_hook_result_update_input() {
        let result = HookResult::UpdateInput {
            updated_input: serde_json::json!({"command": "echo modified"}),
        };
        assert_eq!(result.exit_code(), 0);
        match &result {
            HookResult::UpdateInput { updated_input } => {
                assert_eq!(updated_input["command"], "echo modified");
            }
            _ => panic!("expected UpdateInput"),
        }
    }
}
pub mod omp;
