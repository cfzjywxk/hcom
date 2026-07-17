//! Canonical environment-based AI tool detection and the marker inventory
//! from which hcom's own (replaced-at-launch) marker subset is derived.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::tool::Tool;

#[derive(Debug, Clone, Copy)]
pub enum EnvMatch {
    Set,
    NonEmpty,
    Equals(&'static str),
}

#[derive(Debug, Clone, Copy)]
pub struct EnvPredicate {
    pub var: &'static str,
    pub condition: EnvMatch,
}

#[derive(Debug)]
pub struct ToolDetectionRule {
    pub tool: Tool,
    pub predicates: &'static [EnvPredicate],
    /// Additional marker vars associated with this tool, beyond `predicates`.
    /// Together they form the `tool_marker_vars()` inventory. Native tool
    /// markers are inherited by children exactly like a bare child process;
    /// only the hcom-owned subset (`hcom_owned_marker_vars()`) is replaced
    /// at launch.
    pub extra_marker_vars: &'static [&'static str],
}

const CLAUDE_NATIVE: &[EnvPredicate] = &[
    EnvPredicate {
        var: "CLAUDECODE",
        condition: EnvMatch::Equals("1"),
    },
    EnvPredicate {
        var: "CLAUDE_ENV_FILE",
        condition: EnvMatch::NonEmpty,
    },
];
const ANTIGRAVITY_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "ANTIGRAVITY_AGENT",
    condition: EnvMatch::Set,
}];
const GEMINI_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "GEMINI_CLI",
    condition: EnvMatch::Equals("1"),
}];
const CODEX_NATIVE: &[EnvPredicate] = &[
    EnvPredicate {
        var: "CODEX_SANDBOX",
        condition: EnvMatch::Set,
    },
    EnvPredicate {
        var: "CODEX_SANDBOX_NETWORK_DISABLED",
        condition: EnvMatch::Set,
    },
    EnvPredicate {
        var: "CODEX_MANAGED_BY_NPM",
        condition: EnvMatch::Set,
    },
    EnvPredicate {
        var: "CODEX_MANAGED_BY_BUN",
        condition: EnvMatch::Set,
    },
    EnvPredicate {
        var: "CODEX_THREAD_ID",
        condition: EnvMatch::Set,
    },
];
const OPENCODE_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "OPENCODE",
    condition: EnvMatch::Equals("1"),
}];
const KILO_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "KILO",
    condition: EnvMatch::Equals("1"),
}];
const CURSOR_NATIVE: &[EnvPredicate] = &[
    EnvPredicate {
        var: "CURSOR_AGENT",
        condition: EnvMatch::Set,
    },
    EnvPredicate {
        var: "CURSOR_PROJECT_DIR",
        condition: EnvMatch::Set,
    },
];
const KIMI_NATIVE: &[EnvPredicate] = &[
    EnvPredicate {
        var: "KIMI_CODE_CLI",
        condition: EnvMatch::Equals("1"),
    },
    EnvPredicate {
        var: "KIMI_SESSION_ID",
        condition: EnvMatch::Set,
    },
];
const PI_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "HCOM_PI",
    condition: EnvMatch::Equals("1"),
}];
const OMP_NATIVE: &[EnvPredicate] = &[EnvPredicate {
    var: "HCOM_OMP",
    condition: EnvMatch::Equals("1"),
}];

macro_rules! hcom_tool_predicate {
    ($name:literal, $ident:ident) => {
        const $ident: &[EnvPredicate] = &[EnvPredicate {
            var: "HCOM_TOOL",
            condition: EnvMatch::Equals($name),
        }];
    };
}

hcom_tool_predicate!("claude", HCOM_TOOL_CLAUDE);
hcom_tool_predicate!("antigravity", HCOM_TOOL_ANTIGRAVITY);
hcom_tool_predicate!("gemini", HCOM_TOOL_GEMINI);
hcom_tool_predicate!("codex", HCOM_TOOL_CODEX);
hcom_tool_predicate!("opencode", HCOM_TOOL_OPENCODE);
hcom_tool_predicate!("kilo", HCOM_TOOL_KILO);
hcom_tool_predicate!("cursor", HCOM_TOOL_CURSOR);
hcom_tool_predicate!("kimi", HCOM_TOOL_KIMI);
hcom_tool_predicate!("copilot", HCOM_TOOL_COPILOT);
hcom_tool_predicate!("pi", HCOM_TOOL_PI);
hcom_tool_predicate!("omp", HCOM_TOOL_OMP);

/// Detection precedence: native markers first, then hcom's explicit fallback.
pub static TOOL_DETECTION_RULES: &[ToolDetectionRule] = &[
    ToolDetectionRule {
        tool: Tool::Claude,
        predicates: CLAUDE_NATIVE,
        extra_marker_vars: &["CLAUDECODE", "CLAUDE_ENV_FILE"],
    },
    ToolDetectionRule {
        tool: Tool::Antigravity,
        predicates: ANTIGRAVITY_NATIVE,
        extra_marker_vars: &["ANTIGRAVITY_AGENT"],
    },
    ToolDetectionRule {
        tool: Tool::Gemini,
        predicates: GEMINI_NATIVE,
        extra_marker_vars: &["GEMINI_CLI", "GEMINI_SYSTEM_MD"],
    },
    ToolDetectionRule {
        tool: Tool::Codex,
        predicates: CODEX_NATIVE,
        extra_marker_vars: &[
            "CODEX_SANDBOX",
            "CODEX_SANDBOX_NETWORK_DISABLED",
            "CODEX_MANAGED_BY_NPM",
            "CODEX_MANAGED_BY_BUN",
            "CODEX_THREAD_ID",
        ],
    },
    ToolDetectionRule {
        tool: Tool::OpenCode,
        predicates: OPENCODE_NATIVE,
        extra_marker_vars: &["OPENCODE"],
    },
    ToolDetectionRule {
        tool: Tool::Kilo,
        predicates: KILO_NATIVE,
        extra_marker_vars: &["KILO"],
    },
    ToolDetectionRule {
        tool: Tool::Cursor,
        predicates: CURSOR_NATIVE,
        extra_marker_vars: &["CURSOR_AGENT", "CURSOR_PROJECT_DIR"],
    },
    ToolDetectionRule {
        tool: Tool::Kimi,
        predicates: KIMI_NATIVE,
        extra_marker_vars: &["KIMI_CODE_CLI", "KIMI_SESSION_ID"],
    },
    ToolDetectionRule {
        tool: Tool::Pi,
        predicates: PI_NATIVE,
        extra_marker_vars: &["HCOM_PI", "PI_CODING_AGENT", "PI_CODING_AGENT_SESSION_DIR"],
    },
    ToolDetectionRule {
        tool: Tool::Omp,
        predicates: OMP_NATIVE,
        extra_marker_vars: &["HCOM_OMP", "PI_CODING_AGENT", "PI_CODING_AGENT_SESSION_DIR"],
    },
    ToolDetectionRule {
        tool: Tool::Claude,
        predicates: HCOM_TOOL_CLAUDE,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Antigravity,
        predicates: HCOM_TOOL_ANTIGRAVITY,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Gemini,
        predicates: HCOM_TOOL_GEMINI,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Codex,
        predicates: HCOM_TOOL_CODEX,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::OpenCode,
        predicates: HCOM_TOOL_OPENCODE,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Kilo,
        predicates: HCOM_TOOL_KILO,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Cursor,
        predicates: HCOM_TOOL_CURSOR,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Kimi,
        predicates: HCOM_TOOL_KIMI,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Copilot,
        predicates: HCOM_TOOL_COPILOT,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Pi,
        predicates: HCOM_TOOL_PI,
        extra_marker_vars: &["HCOM_TOOL"],
    },
    ToolDetectionRule {
        tool: Tool::Omp,
        predicates: HCOM_TOOL_OMP,
        extra_marker_vars: &["HCOM_TOOL"],
    },
];

fn predicate_matches(env: &HashMap<String, String>, predicate: &EnvPredicate) -> bool {
    match predicate.condition {
        EnvMatch::Set => env.contains_key(predicate.var),
        EnvMatch::NonEmpty => env
            .get(predicate.var)
            .is_some_and(|value| !value.is_empty()),
        EnvMatch::Equals(expected) => env
            .get(predicate.var)
            .is_some_and(|value| value == expected),
    }
}

pub fn detect_tool(env: &HashMap<String, String>) -> Tool {
    TOOL_DETECTION_RULES
        .iter()
        .find(|rule| {
            rule.predicates
                .iter()
                .any(|predicate| predicate_matches(env, predicate))
        })
        .map(|rule| rule.tool)
        .unwrap_or(Tool::Adhoc)
}

pub fn tool_marker_vars() -> &'static [&'static str] {
    static VARS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
        let mut seen = HashSet::new();
        let mut vars = Vec::new();
        for rule in TOOL_DETECTION_RULES {
            for var in rule
                .predicates
                .iter()
                .map(|predicate| predicate.var)
                .chain(rule.extra_marker_vars.iter().copied())
            {
                if seen.insert(var) {
                    vars.push(var);
                }
            }
        }
        vars
    });
    VARS.as_slice()
}

/// Marker vars that hcom itself owns and replaces when spawning an agent:
/// the `HCOM_*` markers plus `ANTIGRAVITY_AGENT`, which hcom sets at
/// antigravity launch to attribute the shared Gemini hooks. Native tool
/// markers are excluded — a wrapped tool inherits them exactly like a bare
/// child process.
pub fn hcom_owned_marker_vars() -> Vec<&'static str> {
    tool_marker_vars()
        .iter()
        .copied()
        .filter(|var| var.starts_with("HCOM_") || *var == "ANTIGRAVITY_AGENT")
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn native_markers_beat_hcom_tool_fallback() {
        assert_eq!(
            detect_tool(&env(&[("GEMINI_CLI", "1"), ("HCOM_TOOL", "claude")])),
            Tool::Gemini
        );
    }

    #[test]
    fn antigravity_precedes_overlapping_gemini_marker() {
        assert_eq!(
            detect_tool(&env(&[("ANTIGRAVITY_AGENT", "1"), ("GEMINI_CLI", "1")])),
            Tool::Antigravity
        );
    }

    #[test]
    fn every_detection_var_is_in_marker_inventory() {
        let inventory: HashSet<&str> = tool_marker_vars().iter().copied().collect();
        for rule in TOOL_DETECTION_RULES {
            for predicate in rule.predicates {
                assert!(
                    inventory.contains(predicate.var),
                    "{} detection marker must be in the marker inventory",
                    predicate.var
                );
            }
        }
    }

    #[test]
    fn previously_missing_markers_are_in_marker_inventory() {
        assert!(tool_marker_vars().contains(&"CLAUDE_ENV_FILE"));
        assert!(tool_marker_vars().contains(&"HCOM_TOOL"));
    }

    #[test]
    fn hcom_owned_marker_vars_covers_only_hcom_owned_markers() {
        let owned = hcom_owned_marker_vars();
        assert!(owned.contains(&"HCOM_TOOL"));
        assert!(owned.contains(&"HCOM_PI"));
        assert!(owned.contains(&"HCOM_OMP"));
        assert!(owned.contains(&"ANTIGRAVITY_AGENT"));
        for native in [
            "CLAUDECODE",
            "CLAUDE_ENV_FILE",
            "GEMINI_CLI",
            "CODEX_THREAD_ID",
        ] {
            assert!(!owned.contains(&native), "{native} is not hcom-owned");
        }
    }
}
