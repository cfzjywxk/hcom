//! Codex launch preprocessing — sandbox flags, DB access, bootstrap injection.

use crate::paths;

/// Sandbox modes aligned with Codex TUI presets.
///
/// - `workspace`: --sandbox workspace-write (interactive: on-request approvals)
/// - `untrusted`: Workspace writes, approval before untrusted commands
/// - `danger-full-access`: Full Access — --dangerously-bypass-approvals-and-sandbox
/// - `none`: Raw Codex permission policy; hcom changes no permission arguments
///
/// Codex 0.128.0 removed `--full-auto` from the TUI (it was sugar for
/// workspace-write + on-failure approvals). The current shape — --sandbox
/// workspace-write with default on-request approvals — matches the prior
/// behavior closely enough for the TUI flow.
pub fn get_sandbox_flags(mode: &str) -> Vec<String> {
    // Seatbelt blocks Unix sockets by default, breaking tmux/kitty terminal launches.
    // network_access=true adds (allow system-socket) to the seatbelt profile.
    let net = vec![
        "-c".to_string(),
        "sandbox_workspace_write.network_access=true".to_string(),
    ];

    match mode {
        "workspace" => {
            let mut flags = vec!["--sandbox".to_string(), "workspace-write".to_string()];
            flags.extend(net);
            flags
        }
        "untrusted" => {
            // Read-only-equivalent UX for hcom: codex's actual read-only sandbox
            // can't be used (hcom needs DB writes), so we keep workspace-write FS
            // and gate every non-safe command on user approval via -a untrusted.
            let mut flags = vec![
                "--sandbox".to_string(),
                "workspace-write".to_string(),
                "-a".to_string(),
                "untrusted".to_string(),
            ];
            flags.extend(net);
            flags
        }
        "danger-full-access" => {
            vec!["--dangerously-bypass-approvals-and-sandbox".to_string()]
        }
        "none" => vec![],
        // Invalid values are rejected by config validation. Fail transparent
        // here as a final guard instead of silently changing Codex policy.
        _ => vec![],
    }
}

fn has_explicit_sandbox_or_approval(tokens: &[String]) -> bool {
    const POLICY_FLAGS: &[&str] = &[
        "--sandbox",
        "-s",
        "--ask-for-approval",
        "-a",
        "--dangerously-bypass-approvals-and-sandbox",
        "--full-auto",
        "--yolo",
    ];

    tokens.iter().any(|token| {
        POLICY_FLAGS.iter().any(|flag| {
            token == flag
                || token
                    .strip_prefix(flag)
                    .is_some_and(|suffix| suffix.starts_with('='))
        })
    })
}

/// Add only HCOM_DIR as an extra Codex writable directory.
///
/// `--add-dir` is additive and does not replace the user's configured sandbox,
/// approval policy, or existing writable roots. Full-access invocations need
/// no extra directory. This helper is used only by an explicitly selected
/// hcom sandbox preset; transparent mode never calls it.
pub fn ensure_hcom_writable(tokens: &[String]) -> Vec<String> {
    if tokens.iter().any(|token| {
        matches!(
            token.as_str(),
            "--dangerously-bypass-approvals-and-sandbox" | "--yolo"
        )
    }) {
        return tokens.to_vec();
    }

    let hcom_dir = paths::hcom_dir().to_string_lossy().to_string();

    for (i, token) in tokens.iter().enumerate() {
        if token == "--add-dir" && i + 1 < tokens.len() && tokens[i + 1] == hcom_dir {
            return tokens.to_vec();
        }
        if token
            .strip_prefix("--add-dir=")
            .is_some_and(|value| value == hcom_dir)
        {
            return tokens.to_vec();
        }
    }

    let mut result = tokens.to_vec();
    result.extend(["--add-dir".to_string(), hcom_dir]);
    result
}

/// Add hcom bootstrap to codex developer_instructions.
///
/// Builds full bootstrap and adds via `-c developer_instructions=...` flag.
/// If user also provided developer_instructions, bootstrap comes first,
/// then separator, then user content.
///
pub fn add_codex_developer_instructions(
    codex_args: &[String],
    bootstrap_text: &str,
) -> Vec<String> {
    let mut existing_dev_instructions: Option<String> = None;
    let mut remaining = Vec::with_capacity(codex_args.len() + 2);
    let mut i = 0;
    while i < codex_args.len() {
        let token = &codex_args[i];
        if let Some(value) = token
            .strip_prefix("-c=developer_instructions=")
            .or_else(|| token.strip_prefix("--config=developer_instructions="))
        {
            existing_dev_instructions = Some(value.to_string());
            i += 1;
            continue;
        }
        if (token == "-c" || token == "--config")
            && i + 1 < codex_args.len()
            && let Some(value) = codex_args[i + 1].strip_prefix("developer_instructions=")
        {
            existing_dev_instructions = Some(value.to_string());
            i += 2;
            continue;
        }
        remaining.push(token.clone());
        i += 1;
    }

    let combined = if let Some(existing) = existing_dev_instructions {
        format!("{}\n---\n{}", bootstrap_text, existing)
    } else {
        bootstrap_text.to_string()
    };

    // `-c` values are TOML expressions. A raw multiline string happened to be
    // accepted by older Codex builds but is ignored by current builds,
    // silently dropping the hcom identity bootstrap. Serialize a real TOML
    // string so quotes, backslashes, and newlines survive on every platform.
    let encoded = toml::Value::String(combined).to_string();
    remaining.extend([
        "-c".to_string(),
        format!("developer_instructions={encoded}"),
    ]);
    remaining
}

/// Remove any Codex `developer_instructions=...` config entries.
///
/// Resume/fork should not carry the previous instance's embedded hcom session
/// block because it hard-codes the original instance name. A fresh bootstrap is
/// injected later for the new instance.
pub fn strip_codex_developer_instructions(codex_args: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let mut i = 0;

    while i < codex_args.len() {
        let token = &codex_args[i];

        if token.starts_with("-c=developer_instructions=")
            || token.starts_with("--config=developer_instructions=")
        {
            i += 1;
            continue;
        }

        if (token == "-c" || token == "--config") && i + 1 < codex_args.len() {
            let next = &codex_args[i + 1];
            if next.starts_with("developer_instructions=") {
                i += 2;
                continue;
            }
        }

        result.push(token.clone());
        i += 1;
    }

    result
}

/// Preprocess Codex CLI arguments for hcom integration.
///
/// Applies:
/// 1. Strip stale developer_instructions (resume/fork only — they carry old identity)
/// 2. Optional hcom sandbox preset, only when explicitly configured
/// 3. Add HCOM_DIR only when an explicit hcom sandbox preset owns the policy
/// 4. Bootstrap injection via developer_instructions
pub fn preprocess_codex_args(
    codex_args: &[String],
    bootstrap_text: &str,
    sandbox_mode: &str,
) -> Vec<String> {
    // 1. Strip stale developer_instructions for resume/fork only.
    //    Fresh launches may have user system_prompt in developer_instructions
    //    that add_codex_developer_instructions will merge with bootstrap.
    let codex_args = if codex_args
        .iter()
        .any(|arg| matches!(arg.as_str(), "resume" | "fork"))
    {
        strip_codex_developer_instructions(codex_args)
    } else {
        codex_args.to_vec()
    };

    let mut args = codex_args;

    // 2. Inject the configured policy only as a default. An explicit user
    // sandbox, approval, or bypass selector owns the complete Codex policy;
    // appending hcom's profile would make clap's last-value-wins behavior
    // silently override it.
    let hcom_owns_policy = sandbox_mode != "none" && !has_explicit_sandbox_or_approval(&args);
    if hcom_owns_policy {
        args.extend(get_sandbox_flags(sandbox_mode));
        // The hcom preset must also make hcom's own state writable. In
        // transparent mode or with any user-owned policy, inject nothing:
        // Codex rejects --add-dir under some effective permission modes.
        args = ensure_hcom_writable(&args);
    }

    // 3. Add bootstrap to developer_instructions.
    args = add_codex_developer_instructions(&args, bootstrap_text);

    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|i| i.to_string()).collect()
    }

    fn has_hcom_add_dir(result: &[String]) -> bool {
        let hcom_dir = paths::hcom_dir().to_string_lossy().to_string();
        result
            .windows(2)
            .any(|pair| pair[0] == "--add-dir" && pair[1] == hcom_dir)
    }

    fn init_config() {
        // Config::init is idempotent-ish but needs to be called before paths::hcom_dir()
        crate::config::Config::init();
    }

    #[test]
    fn test_sandbox_flags_workspace() {
        let flags = get_sandbox_flags("workspace");
        assert!(flags.contains(&"--sandbox".to_string()));
        assert!(flags.contains(&"workspace-write".to_string()));
        assert!(flags.contains(&"sandbox_workspace_write.network_access=true".to_string()));
    }

    #[test]
    fn test_sandbox_flags_untrusted() {
        let flags = get_sandbox_flags("untrusted");
        assert!(flags.contains(&"--sandbox".to_string()));
        assert!(flags.contains(&"workspace-write".to_string()));
        assert!(flags.contains(&"-a".to_string()));
        assert!(flags.contains(&"untrusted".to_string()));
    }

    #[test]
    fn test_sandbox_flags_danger() {
        let flags = get_sandbox_flags("danger-full-access");
        assert_eq!(
            flags,
            vec!["--dangerously-bypass-approvals-and-sandbox".to_string()]
        );
    }

    #[test]
    fn test_sandbox_flags_none() {
        let flags = get_sandbox_flags("none");
        assert!(flags.is_empty());
    }

    #[test]
    fn test_sandbox_flags_unknown_does_not_change_policy() {
        let flags = get_sandbox_flags("bogus");
        assert!(flags.is_empty());
    }

    #[test]
    #[serial]
    fn test_ensure_hcom_writable_adds_only_hcom_dir() {
        init_config();
        let tokens = s(&["--model", "gpt-5", "-c", "model_reasoning_effort=high"]);
        let result = ensure_hcom_writable(&tokens);
        assert_eq!(&result[..tokens.len()], tokens.as_slice());
        assert!(has_hcom_add_dir(&result));
        assert!(!result.iter().any(|arg| arg.contains("writable_roots")));
    }

    #[test]
    #[serial]
    fn test_ensure_hcom_writable_skips_full_access() {
        init_config();
        let tokens = s(&["--yolo"]);
        let result = ensure_hcom_writable(&tokens);
        assert_eq!(result, tokens);
    }

    #[test]
    #[serial]
    fn test_ensure_hcom_writable_respects_explicit_add_dir() {
        init_config();
        let hcom_dir = paths::hcom_dir().to_string_lossy().to_string();
        let tokens = vec!["--full-auto".to_string(), "--add-dir".to_string(), hcom_dir];
        let result = ensure_hcom_writable(&tokens);
        assert_eq!(result, tokens, "explicit --add-dir must suppress injection");
    }

    #[test]
    #[serial]
    fn test_ensure_hcom_writable_preserves_user_writable_roots() {
        init_config();
        let tokens = s(&[
            "--sandbox",
            "workspace-write",
            "-c",
            r#"sandbox_workspace_write.writable_roots=["/my/dir"]"#,
        ]);
        let result = ensure_hcom_writable(&tokens);
        assert_eq!(&result[..tokens.len()], tokens.as_slice());
        assert!(has_hcom_add_dir(&result));
    }

    #[test]
    fn test_add_developer_instructions_basic() {
        let args = s(&["-m", "o3"]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        assert_eq!(
            result,
            s(&["-m", "o3", "-c", "developer_instructions=\"BOOTSTRAP\""])
        );
    }

    #[test]
    fn test_add_developer_instructions_keeps_resume() {
        let args = s(&["resume"]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        assert_eq!(result[0], "resume");
        assert_eq!(result[1], "-c");
        assert_eq!(result[2], "developer_instructions=\"BOOTSTRAP\"");
    }

    #[test]
    fn test_add_developer_instructions_keeps_resume_session_first() {
        let args = s(&["resume", "thread-1", "--model", "gpt-5"]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        assert_eq!(result[0], "resume");
        assert_eq!(result[1], "thread-1");
        assert_eq!(result[2], "--model");
        assert_eq!(result[3], "gpt-5");
        assert_eq!(result[4], "-c");
        assert_eq!(result[5], "developer_instructions=\"BOOTSTRAP\"");
    }

    #[test]
    fn test_add_developer_instructions_keeps_fork_session_first_with_existing_config() {
        let args = s(&[
            "fork",
            "thread-1",
            "-c",
            "developer_instructions=OLD",
            "--model",
            "gpt-5",
        ]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        assert_eq!(result[0], "fork");
        assert_eq!(result[1], "thread-1");
        assert_eq!(result[2], "--model");
        assert_eq!(result[3], "gpt-5");
        assert_eq!(result[4], "-c");
        assert!(result[5].contains("BOOTSTRAP"));
        assert!(result[5].contains("OLD"));
    }

    #[test]
    fn test_add_developer_instructions_merge_existing() {
        let args = s(&["-c", "developer_instructions=USER_NOTES", "-m", "o3"]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        let injected = result.last().unwrap();
        assert!(injected.contains("BOOTSTRAP"));
        assert!(injected.contains("USER_NOTES"));
        assert!(injected.contains("---"));
        let di_count = result
            .iter()
            .filter(|t| t.starts_with("developer_instructions="))
            .count();
        assert_eq!(di_count, 1);
    }

    #[test]
    fn test_add_developer_instructions_preserves_fork_subcommand() {
        let args = s(&["fork", "-m", "o3"]);
        let result = add_codex_developer_instructions(&args, "BOOTSTRAP");
        assert_eq!(result[0], "fork");
        assert_eq!(result[result.len() - 2], "-c");
    }

    #[test]
    fn test_strip_developer_instructions_space_syntax() {
        let args = s(&["fork", "-c", "developer_instructions=OLD", "--model", "o3"]);
        let result = strip_codex_developer_instructions(&args);
        assert_eq!(result, s(&["fork", "--model", "o3"]));
    }

    #[test]
    fn test_strip_developer_instructions_equals_syntax() {
        let args = s(&[
            "resume",
            "--config=developer_instructions=OLD",
            "--full-auto",
        ]);
        let result = strip_codex_developer_instructions(&args);
        assert_eq!(result, s(&["resume", "--full-auto"]));
    }

    #[test]
    #[serial]
    fn test_preprocess_codex_args_full_pipeline() {
        init_config();
        let args = s(&["-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");
        assert!(result.contains(&"--sandbox".to_string()));
        assert!(result.contains(&"workspace-write".to_string()));
        assert!(has_hcom_add_dir(&result));
        assert!(!result.contains(&"--dangerously-bypass-hook-trust".to_string()));
        assert!(result.iter().any(|t| t.contains("developer_instructions=")));
    }

    #[test]
    #[serial]
    fn test_preprocess_resume_keeps_session_first() {
        init_config();
        let args = s(&["resume", "thread-1", "--model", "gpt-5"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");
        assert_eq!(result[0], "resume");
        assert_eq!(result[1], "thread-1");
        assert!(has_hcom_add_dir(&result));
        assert!(result.iter().any(|t| t.contains("developer_instructions=")));
    }

    #[test]
    #[serial]
    fn test_preprocess_user_sandbox_suppresses_hcom_policy_defaults() {
        init_config();
        let args = s(&["--sandbox", "read-only", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");
        let sandbox_position = result.iter().position(|t| t == "--sandbox").unwrap();
        assert_eq!(result[sandbox_position + 1], "read-only");
        assert_eq!(result.iter().filter(|t| *t == "--sandbox").count(), 1);
        assert!(!result.contains(&"workspace-write".to_string()));
        assert!(!has_hcom_add_dir(&result));
        assert!(!result.contains(&"sandbox_workspace_write.network_access=true".to_string()));
    }

    #[test]
    #[serial]
    fn test_preprocess_yolo_suppresses_hcom_policy_defaults() {
        init_config();
        let args = s(&["--yolo", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");

        assert!(result.contains(&"--yolo".to_string()));
        assert!(!result.contains(&"--sandbox".to_string()));
        assert!(!result.contains(&"workspace-write".to_string()));
        assert!(!result.contains(&"sandbox_workspace_write.network_access=true".to_string()));
        assert!(!has_hcom_add_dir(&result));
    }

    #[test]
    #[serial]
    fn test_preprocess_user_approval_suppresses_hcom_policy_defaults() {
        init_config();
        let args = s(&["-a", "on-request", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "untrusted");
        let approval_position = result.iter().position(|t| t == "-a").unwrap();
        assert_eq!(result[approval_position + 1], "on-request");
        assert_eq!(result.iter().filter(|t| *t == "-a").count(), 1);
        assert!(!result.contains(&"untrusted".to_string()));
        assert!(!result.contains(&"--sandbox".to_string()));
        assert!(!result.contains(&"sandbox_workspace_write.network_access=true".to_string()));
        assert!(!has_hcom_add_dir(&result));
    }

    #[test]
    #[serial]
    fn test_preprocess_bypass_suppresses_hcom_policy_defaults() {
        init_config();
        let args = s(&["--dangerously-bypass-approvals-and-sandbox", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "untrusted");

        assert_eq!(
            result
                .iter()
                .filter(|t| *t == "--dangerously-bypass-approvals-and-sandbox")
                .count(),
            1
        );
        assert!(!result.contains(&"--sandbox".to_string()));
        assert!(!result.contains(&"-a".to_string()));
        assert!(!result.contains(&"sandbox_workspace_write.network_access=true".to_string()));
        assert!(!has_hcom_add_dir(&result));
    }

    #[test]
    #[serial]
    fn test_preprocess_equals_policy_flags_suppress_hcom_defaults() {
        init_config();
        let args = s(&["--sandbox=read-only", "-a=on-request", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");

        assert!(result.contains(&"--sandbox=read-only".to_string()));
        assert!(result.contains(&"-a=on-request".to_string()));
        assert!(!result.contains(&"--sandbox".to_string()));
        assert!(!result.contains(&"workspace-write".to_string()));
        assert!(!result.contains(&"sandbox_workspace_write.network_access=true".to_string()));
        assert!(!has_hcom_add_dir(&result));
    }

    #[test]
    #[serial]
    fn test_preprocess_none_preserves_model_effort_and_user_policy() {
        init_config();
        let args = s(&[
            "--model",
            "gpt-5.4",
            "-c",
            "model_reasoning_effort=high",
            "-a",
            "on-request",
        ]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "none");
        assert_eq!(&result[..args.len()], args.as_slice());
        assert!(!result.contains(&"--sandbox".to_string()));
        assert!(!result.contains(&"workspace-write".to_string()));
        assert!(!result.contains(&"--dangerously-bypass-hook-trust".to_string()));
        assert!(!has_hcom_add_dir(&result));
        assert!(result.iter().any(|t| t.contains("developer_instructions=")));
    }

    #[test]
    #[serial]
    fn test_preprocess_strips_stale_on_resume() {
        init_config();
        let args = s(&[
            "resume",
            "-c",
            "developer_instructions=STALE_BOOTSTRAP",
            "-m",
            "o3",
        ]);
        let result = preprocess_codex_args(&args, "FRESH", "workspace");
        let di: Vec<&String> = result
            .iter()
            .filter(|t| t.starts_with("developer_instructions="))
            .collect();
        assert_eq!(di.len(), 1);
        assert!(di[0].contains("FRESH"));
        assert!(!di[0].contains("STALE"));
    }

    #[test]
    #[serial]
    fn test_preprocess_preserves_user_instructions_on_fresh_launch() {
        init_config();
        let args = s(&["-c", "developer_instructions=USER_NOTES", "-m", "o3"]);
        let result = preprocess_codex_args(&args, "BOOTSTRAP", "workspace");
        let di: Vec<&String> = result
            .iter()
            .filter(|t| t.starts_with("developer_instructions="))
            .collect();
        assert_eq!(di.len(), 1);
        assert!(di[0].contains("BOOTSTRAP"));
        assert!(di[0].contains("USER_NOTES"));
    }
}
