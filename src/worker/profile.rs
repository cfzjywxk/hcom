//! Typed, session-frozen native CLI invocation profiles.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CODEX_DEVELOPER_ADAPTER: &str = "codex-developer-0.145.0";
pub const CLAUDE_DEVELOPER_ADAPTER: &str = "claude-developer-2.1.220";
pub const CODEX_REVIEWER_ADAPTER: &str = "codex-reviewer-0.145.0";
pub const CLAUDE_REVIEWER_ADAPTER: &str = "claude-reviewer-2.1.220";
pub const CODEX_ARCHITECT_ADAPTER: &str = "codex-0.145.0";
pub const CLAUDE_ARCHITECT_ADAPTER: &str = "claude-2.1.220";

const DEFAULT_CODEX_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_CLAUDE_DEVELOPER_MODEL: &str = "claude-opus-5";
const DEFAULT_CLAUDE_ARCHITECT_MODEL: &str = "opus";
const DEFAULT_DEVELOPER_REASONING: &str = "xhigh";
const DEFAULT_ARCHITECT_REASONING: &str = "xhigh";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchitectAdapter {
    Codex,
    Claude,
}

impl ArchitectAdapter {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "codex" => Ok(Self::Codex),
            "claude" => Ok(Self::Claude),
            _ => bail!("session architect adapter must be codex or claude"),
        }
    }

    pub fn contract_name(self) -> &'static str {
        match self {
            Self::Codex => CODEX_ARCHITECT_ADAPTER,
            Self::Claude => CLAUDE_ARCHITECT_ADAPTER,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CodexSandbox {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl CodexSandbox {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CodexApprovalPolicy {
    Untrusted,
    OnRequest,
    Never,
}

impl CodexApprovalPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Untrusted => "untrusted",
            Self::OnRequest => "on-request",
            Self::Never => "never",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodexInvocationProfile {
    pub model: String,
    pub reasoning_effort: String,
    pub sandbox: CodexSandbox,
    #[serde(rename = "ask_for_approval")]
    pub approval_policy: CodexApprovalPolicy,
}

impl CodexInvocationProfile {
    pub fn architect_default() -> Self {
        Self {
            model: DEFAULT_CODEX_MODEL.into(),
            reasoning_effort: DEFAULT_ARCHITECT_REASONING.into(),
            sandbox: CodexSandbox::DangerFullAccess,
            approval_policy: CodexApprovalPolicy::Never,
        }
    }

    pub fn developer_default() -> Self {
        Self {
            model: DEFAULT_CODEX_MODEL.into(),
            reasoning_effort: DEFAULT_DEVELOPER_REASONING.into(),
            sandbox: CodexSandbox::DangerFullAccess,
            approval_policy: CodexApprovalPolicy::Never,
        }
    }

    pub fn reviewer_default() -> Self {
        Self {
            model: DEFAULT_CODEX_MODEL.into(),
            reasoning_effort: DEFAULT_ARCHITECT_REASONING.into(),
            sandbox: CodexSandbox::DangerFullAccess,
            approval_policy: CodexApprovalPolicy::Never,
        }
    }

    pub fn validate(&self, label: &str) -> Result<()> {
        validate_model(&format!("{label} model"), &self.model)?;
        if !matches!(
            self.reasoning_effort.as_str(),
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        ) {
            bail!(
                "{label} reasoning_effort must be one of none, minimal, low, medium, high, xhigh, or max"
            );
        }
        Ok(())
    }

    pub fn reasoning_config_argument(&self) -> String {
        format!("model_reasoning_effort=\"{}\"", self.reasoning_effort)
    }

    pub fn approval_config_argument(&self) -> String {
        format!("approval_policy=\"{}\"", self.approval_policy.as_str())
    }

    pub fn effective_policy(&self, outer: &str) -> String {
        format!(
            "native={};outer={outer};approval={}",
            self.sandbox.as_str(),
            self.approval_policy.as_str()
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClaudeInvocationProfile {
    pub model: String,
    pub effort: String,
    pub dangerously_skip_permissions: bool,
}

impl ClaudeInvocationProfile {
    pub fn architect_default() -> Self {
        Self {
            model: DEFAULT_CLAUDE_ARCHITECT_MODEL.into(),
            effort: DEFAULT_ARCHITECT_REASONING.into(),
            dangerously_skip_permissions: true,
        }
    }

    pub fn developer_default() -> Self {
        Self {
            model: DEFAULT_CLAUDE_DEVELOPER_MODEL.into(),
            effort: DEFAULT_DEVELOPER_REASONING.into(),
            dangerously_skip_permissions: true,
        }
    }

    pub fn reviewer_default() -> Self {
        Self::architect_default()
    }

    pub fn validate(&self, label: &str) -> Result<()> {
        validate_model(&format!("{label} model"), &self.model)?;
        if !matches!(
            self.effort.as_str(),
            "low" | "medium" | "high" | "xhigh" | "max"
        ) {
            bail!("{label} effort must be one of low, medium, high, xhigh, or max");
        }
        Ok(())
    }

    pub fn effective_policy(&self, outer: &str) -> String {
        let native = if self.dangerously_skip_permissions {
            "dangerously-skip-permissions"
        } else {
            "default-permissions"
        };
        format!("native={native};outer={outer}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "adapter", rename_all = "lowercase", deny_unknown_fields)]
pub enum ArchitectInvocationProfile {
    Codex {
        #[serde(flatten)]
        profile: CodexInvocationProfile,
    },
    Claude {
        #[serde(flatten)]
        profile: ClaudeInvocationProfile,
    },
}

impl ArchitectInvocationProfile {
    pub fn adapter(&self) -> ArchitectAdapter {
        match self {
            Self::Codex { .. } => ArchitectAdapter::Codex,
            Self::Claude { .. } => ArchitectAdapter::Claude,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Codex { profile } => profile.validate("Codex architect"),
            Self::Claude { profile } => profile.validate("Claude architect"),
        }
    }

    pub fn codex(&self) -> Option<&CodexInvocationProfile> {
        match self {
            Self::Codex { profile } => Some(profile),
            Self::Claude { .. } => None,
        }
    }

    pub fn claude(&self) -> Option<&ClaudeInvocationProfile> {
        match self {
            Self::Claude { profile } => Some(profile),
            Self::Codex { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "adapter", rename_all = "lowercase", deny_unknown_fields)]
pub enum DeveloperInvocationProfile {
    Codex {
        #[serde(flatten)]
        profile: CodexInvocationProfile,
    },
    Claude {
        #[serde(flatten)]
        profile: ClaudeInvocationProfile,
    },
}

impl DeveloperInvocationProfile {
    pub fn adapter_name(&self) -> &'static str {
        match self {
            Self::Codex { .. } => CODEX_DEVELOPER_ADAPTER,
            Self::Claude { .. } => CLAUDE_DEVELOPER_ADAPTER,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Codex { profile } => {
                profile.validate("Codex developer")?;
                if profile.sandbox == CodexSandbox::ReadOnly {
                    bail!(
                        "Codex developer sandbox must be workspace-write or danger-full-access because a completed developer turn must commit"
                    );
                }
                Ok(())
            }
            Self::Claude { profile } => profile.validate("Claude developer"),
        }
    }

    pub fn codex(&self) -> Option<&CodexInvocationProfile> {
        match self {
            Self::Codex { profile } => Some(profile),
            Self::Claude { .. } => None,
        }
    }

    pub fn claude(&self) -> Option<&ClaudeInvocationProfile> {
        match self {
            Self::Claude { profile } => Some(profile),
            Self::Codex { .. } => None,
        }
    }
}

impl Default for DeveloperInvocationProfile {
    fn default() -> Self {
        Self::Codex {
            profile: CodexInvocationProfile::developer_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "adapter", rename_all = "lowercase", deny_unknown_fields)]
pub enum ReviewerInvocationProfile {
    Codex {
        #[serde(flatten)]
        profile: CodexInvocationProfile,
    },
    Claude {
        #[serde(flatten)]
        profile: ClaudeInvocationProfile,
    },
}

impl ReviewerInvocationProfile {
    pub fn adapter_name(&self) -> &'static str {
        match self {
            Self::Codex { .. } => CODEX_REVIEWER_ADAPTER,
            Self::Claude { .. } => CLAUDE_REVIEWER_ADAPTER,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Codex { profile } => profile.validate("Codex reviewer"),
            Self::Claude { profile } => profile.validate("Claude reviewer"),
        }
    }

    pub fn codex(&self) -> Option<&CodexInvocationProfile> {
        match self {
            Self::Codex { profile } => Some(profile),
            Self::Claude { .. } => None,
        }
    }

    pub fn claude(&self) -> Option<&ClaudeInvocationProfile> {
        match self {
            Self::Claude { profile } => Some(profile),
            Self::Codex { .. } => None,
        }
    }
}

impl Default for ReviewerInvocationProfile {
    fn default() -> Self {
        Self::Claude {
            profile: ClaudeInvocationProfile::reviewer_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionInvocationProfiles {
    pub architect: ArchitectInvocationProfile,
    pub developer: DeveloperInvocationProfile,
    pub reviewer: ReviewerInvocationProfile,
}

impl SessionInvocationProfiles {
    pub fn for_architect(adapter: ArchitectAdapter) -> Self {
        let architect = match adapter {
            ArchitectAdapter::Codex => ArchitectInvocationProfile::Codex {
                profile: CodexInvocationProfile::architect_default(),
            },
            ArchitectAdapter::Claude => ArchitectInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::architect_default(),
            },
        };
        Self {
            architect,
            developer: DeveloperInvocationProfile::default(),
            reviewer: ReviewerInvocationProfile::default(),
        }
    }

    /// Built-in profiles for the Codex exec worker task-runtime lane.
    ///
    /// This is intentionally separate from [`Self::for_architect`] while the
    /// released CLI worker path is retained. Production selects this resolver
    /// only when the exec worker integration phase is enabled.
    pub fn for_task_lane(adapter: ArchitectAdapter) -> Result<Self> {
        // Both `hcom arch codex` and `hcom arch claude` keep their own
        // foreground Architect adapter but share one Codex-only worker lane.
        let architect = match adapter {
            ArchitectAdapter::Codex => ArchitectInvocationProfile::Codex {
                profile: CodexInvocationProfile::architect_default(),
            },
            ArchitectAdapter::Claude => ArchitectInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::architect_default(),
            },
        };
        Ok(Self {
            architect,
            developer: DeveloperInvocationProfile::Codex {
                profile: CodexInvocationProfile::developer_default(),
            },
            reviewer: ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile::reviewer_default(),
            },
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.architect.validate()?;
        self.developer.validate()?;
        self.reviewer.validate()
    }

    pub fn developer_adapter_name(&self) -> &'static str {
        self.developer.adapter_name()
    }

    pub fn reviewer_adapter_name(&self) -> &'static str {
        self.reviewer.adapter_name()
    }

    pub fn canonical_hash(&self) -> String {
        let encoded = serde_json::to_vec(&("hcom-session-invocation-profiles-v3", self))
            .expect("typed invocation profiles are serializable");
        let digest = Sha256::digest(encoded);
        let mut output = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

impl Default for SessionInvocationProfiles {
    fn default() -> Self {
        Self::for_architect(ArchitectAdapter::Codex)
    }
}

fn validate_model(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('-')
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':' | b'@')
        })
    {
        bail!(
            "{label} must be a 1-128 byte model name containing only ASCII letters, digits, '.', '_', '-', '/', ':', or '@', and cannot begin with '-'"
        );
    }
    Ok(())
}

pub(crate) fn validate_cli_help_contract(
    label: &str,
    bytes: &[u8],
    required_options: &[&str],
) -> Result<()> {
    if bytes.is_empty() || bytes.len() > 128 * 1024 {
        bail!("{label} help output exceeds its closed bound");
    }
    let text =
        std::str::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{label} help is not UTF-8"))?;
    let tokens = text
        .split_ascii_whitespace()
        .map(|token| token.trim_end_matches(','))
        .collect::<Vec<_>>();
    for option in required_options {
        if !tokens.iter().any(|token| token == option) {
            bail!("{label} help omitted required option {option}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_reviewed_outer_safety_and_native_profiles() {
        let profiles = SessionInvocationProfiles::default();
        profiles.validate().unwrap();
        let architect = profiles.architect.codex().unwrap();
        assert_eq!(architect.model, "gpt-5.6-sol");
        assert_eq!(architect.reasoning_effort, "xhigh");
        assert_eq!(architect.sandbox, CodexSandbox::DangerFullAccess);
        assert_eq!(architect.approval_policy, CodexApprovalPolicy::Never);
        assert_eq!(
            profiles.developer.codex().unwrap().sandbox,
            CodexSandbox::DangerFullAccess
        );
        assert_eq!(
            profiles.developer.codex().unwrap().reasoning_effort,
            "xhigh"
        );
        assert_eq!(profiles.developer_adapter_name(), CODEX_DEVELOPER_ADAPTER);
        assert_eq!(profiles.reviewer_adapter_name(), CLAUDE_REVIEWER_ADAPTER);
        let reviewer = profiles.reviewer.claude().unwrap();
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
        assert_eq!(profiles.canonical_hash().len(), 64);
    }

    #[test]
    fn claude_architect_uses_the_independent_claude_reviewer_default() {
        let profiles = SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude);
        profiles.validate().unwrap();
        let architect = profiles.architect.claude().unwrap();
        let reviewer = profiles.reviewer.claude().unwrap();
        assert_eq!(architect.model, "opus");
        assert_eq!(architect.effort, "xhigh");
        assert_eq!(profiles.reviewer_adapter_name(), CLAUDE_REVIEWER_ADAPTER);
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
    }

    #[test]
    fn codex_exec_worker_lane_defaults_both_workers_to_exact_codex_profiles() {
        let profiles = SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        profiles.validate().unwrap();
        assert_eq!(profiles.developer_adapter_name(), CODEX_DEVELOPER_ADAPTER);
        assert_eq!(profiles.reviewer_adapter_name(), CODEX_REVIEWER_ADAPTER);
        assert_eq!(
            profiles.developer.codex().unwrap(),
            profiles.reviewer.codex().unwrap()
        );
        assert_eq!(
            profiles.developer.codex().unwrap(),
            &CodexInvocationProfile {
                model: "gpt-5.6-sol".into(),
                reasoning_effort: "xhigh".into(),
                sandbox: CodexSandbox::DangerFullAccess,
                approval_policy: CodexApprovalPolicy::Never,
            }
        );
        // A Claude Architect keeps its own foreground adapter but binds the
        // same Codex-only worker lane.
        let claude = SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Claude).unwrap();
        claude.validate().unwrap();
        assert!(matches!(
            claude.architect,
            ArchitectInvocationProfile::Claude { .. }
        ));
        assert_eq!(claude.developer_adapter_name(), CODEX_DEVELOPER_ADAPTER);
        assert_eq!(claude.reviewer_adapter_name(), CODEX_REVIEWER_ADAPTER);
        assert_eq!(
            claude.developer.codex().unwrap(),
            profiles.developer.codex().unwrap()
        );
    }

    #[test]
    fn typed_profiles_reject_argv_and_config_injection_material() {
        for model in ["--resume", "safe model", "safe\nmodel", "safe=model"] {
            let mut profile = CodexInvocationProfile::developer_default();
            profile.model = model.into();
            assert!(profile.validate("test").is_err(), "{model:?} was accepted");
        }
        let mut profile = CodexInvocationProfile::developer_default();
        profile.reasoning_effort = "max --config mcp_servers".into();
        assert!(profile.validate("test").is_err());

        let developer = DeveloperInvocationProfile::Codex {
            profile: CodexInvocationProfile {
                sandbox: CodexSandbox::ReadOnly,
                ..CodexInvocationProfile::developer_default()
            },
        };
        assert!(developer.validate().is_err());
    }

    #[test]
    fn worker_toml_is_adapter_tagged_and_hash_binds_every_option() {
        let developer: DeveloperInvocationProfile = toml::from_str(
            r#"
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
"#,
        )
        .unwrap();
        assert_eq!(developer.adapter_name(), CLAUDE_DEVELOPER_ADAPTER);
        developer.validate().unwrap();

        let claude: ReviewerInvocationProfile = toml::from_str(
            r#"
adapter = "claude"
model = "opus"
effort = "xhigh"
dangerously_skip_permissions = true
"#,
        )
        .unwrap();
        assert_eq!(claude.adapter_name(), CLAUDE_REVIEWER_ADAPTER);
        claude.validate().unwrap();

        let codex: ReviewerInvocationProfile = toml::from_str(
            r#"
adapter = "codex"
model = "gpt-5.6-sol"
reasoning_effort = "max"
sandbox = "danger-full-access"
ask_for_approval = "never"
"#,
        )
        .unwrap();
        assert_eq!(codex.adapter_name(), CODEX_REVIEWER_ADAPTER);
        codex.validate().unwrap();

        let left = SessionInvocationProfiles {
            developer,
            reviewer: claude,
            ..SessionInvocationProfiles::default()
        };
        let mut right = left.clone();
        right.developer = DeveloperInvocationProfile::Codex {
            profile: CodexInvocationProfile::developer_default(),
        };
        assert_ne!(left.canonical_hash(), right.canonical_hash());
    }

    #[test]
    fn cli_help_contract_requires_every_closed_option() {
        let help = b"Options:\n  -m, --model <MODEL>\n  --sandbox <MODE>\n";
        validate_cli_help_contract("fixture", help, &["--model", "--sandbox"]).unwrap();
        assert!(
            validate_cli_help_contract(
                "fixture",
                help,
                &["--model", "--dangerously-skip-permissions"],
            )
            .is_err()
        );
    }
}
