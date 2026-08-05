//! Typed native CLI invocation profiles loaded for one foreground run.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CODEX_DEVELOPER_ADAPTER: &str = "codex-developer";
pub const CLAUDE_DEVELOPER_ADAPTER: &str = "claude-developer-2.1.220";
pub const CODEX_REVIEWER_ADAPTER: &str = "codex-reviewer";
pub const CLAUDE_REVIEWER_ADAPTER: &str = "claude-reviewer-2.1.220";
pub const CODEX_ARCHITECT_ADAPTER: &str = "codex";
pub const CLAUDE_ARCHITECT_ADAPTER: &str = "claude";

const DEFAULT_CODEX_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_CLAUDE_MODEL: &str = "opus";
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
            model: DEFAULT_CLAUDE_MODEL.into(),
            effort: DEFAULT_ARCHITECT_REASONING.into(),
            dangerously_skip_permissions: true,
        }
    }

    pub fn developer_default() -> Self {
        Self {
            model: DEFAULT_CLAUDE_MODEL.into(),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ReviewerId {
    Reviewer1,
    Reviewer2,
}

impl ReviewerId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reviewer1 => "reviewer1",
            Self::Reviewer2 => "reviewer2",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReviewerInvocationBinding {
    pub reviewer_id: ReviewerId,
    pub profile: ReviewerInvocationProfile,
}

impl ReviewerInvocationBinding {
    pub fn new(reviewer_id: ReviewerId, profile: ReviewerInvocationProfile) -> Self {
        Self {
            reviewer_id,
            profile,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionInvocationProfiles {
    pub architect: ArchitectInvocationProfile,
    pub developer: DeveloperInvocationProfile,
    pub reviewers: Vec<ReviewerInvocationBinding>,
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
            reviewers: default_reviewer_bindings(),
        }
    }

    /// Built-in profiles for the provider-routed task-runtime lane.
    ///
    /// The foreground Architect adapter is selected independently from the
    /// default Codex Developer + Codex Reviewer1 + Claude Reviewer2 workers.
    pub fn for_task_lane(adapter: ArchitectAdapter) -> Result<Self> {
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
            reviewers: vec![
                ReviewerInvocationBinding::new(
                    ReviewerId::Reviewer1,
                    ReviewerInvocationProfile::Codex {
                        profile: CodexInvocationProfile::reviewer_default(),
                    },
                ),
                ReviewerInvocationBinding::new(
                    ReviewerId::Reviewer2,
                    ReviewerInvocationProfile::Claude {
                        profile: ClaudeInvocationProfile::reviewer_default(),
                    },
                ),
            ],
        })
    }

    pub fn legacy_reviewer_pair(
        profile: ReviewerInvocationProfile,
    ) -> Vec<ReviewerInvocationBinding> {
        vec![
            ReviewerInvocationBinding::new(ReviewerId::Reviewer1, profile.clone()),
            ReviewerInvocationBinding::new(ReviewerId::Reviewer2, profile),
        ]
    }

    pub fn validate(&self) -> Result<()> {
        self.architect.validate()?;
        self.developer.validate()?;
        let [reviewer1, reviewer2] = self.reviewers.as_slice() else {
            bail!(
                "session reviewer collection must contain ordered Reviewer1 and Reviewer2 entries"
            );
        };
        if reviewer1.reviewer_id != ReviewerId::Reviewer1
            || reviewer2.reviewer_id != ReviewerId::Reviewer2
        {
            bail!(
                "session reviewer collection must contain ordered Reviewer1 and Reviewer2 entries"
            );
        }
        reviewer1.profile.validate()?;
        reviewer2.profile.validate()
    }

    pub fn developer_adapter_name(&self) -> &'static str {
        self.developer.adapter_name()
    }

    pub fn reviewer(&self, reviewer_id: ReviewerId) -> &ReviewerInvocationProfile {
        &self
            .reviewers
            .iter()
            .find(|binding| binding.reviewer_id == reviewer_id)
            .expect("validated session profiles contain both Reviewer lanes")
            .profile
    }

    pub fn reviewer_mut(&mut self, reviewer_id: ReviewerId) -> &mut ReviewerInvocationProfile {
        &mut self
            .reviewers
            .iter_mut()
            .find(|binding| binding.reviewer_id == reviewer_id)
            .expect("built-in session profiles contain both Reviewer lanes")
            .profile
    }

    pub fn reviewer1(&self) -> &ReviewerInvocationProfile {
        self.reviewer(ReviewerId::Reviewer1)
    }

    pub fn reviewer2(&self) -> &ReviewerInvocationProfile {
        self.reviewer(ReviewerId::Reviewer2)
    }

    pub fn reviewer1_mut(&mut self) -> &mut ReviewerInvocationProfile {
        self.reviewer_mut(ReviewerId::Reviewer1)
    }

    pub fn reviewer2_mut(&mut self) -> &mut ReviewerInvocationProfile {
        self.reviewer_mut(ReviewerId::Reviewer2)
    }

    pub fn reviewer_adapter_name(&self) -> &'static str {
        self.reviewer1().adapter_name()
    }

    pub fn uses_claude(&self) -> bool {
        self.architect.adapter() == ArchitectAdapter::Claude
            || matches!(&self.developer, DeveloperInvocationProfile::Claude { .. })
            || self
                .reviewers
                .iter()
                .any(|binding| matches!(&binding.profile, ReviewerInvocationProfile::Claude { .. }))
    }

    pub fn canonical_hash(&self) -> String {
        let encoded = serde_json::to_vec(&("hcom-session-invocation-profiles-v5", self))
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

fn default_reviewer_bindings() -> Vec<ReviewerInvocationBinding> {
    vec![
        ReviewerInvocationBinding::new(
            ReviewerId::Reviewer1,
            ReviewerInvocationProfile::Codex {
                profile: CodexInvocationProfile::reviewer_default(),
            },
        ),
        ReviewerInvocationBinding::new(
            ReviewerId::Reviewer2,
            ReviewerInvocationProfile::Claude {
                profile: ClaudeInvocationProfile::reviewer_default(),
            },
        ),
    ]
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
        assert_eq!(profiles.reviewer_adapter_name(), CODEX_REVIEWER_ADAPTER);
        assert_eq!(
            profiles.reviewer1().codex().unwrap(),
            &CodexInvocationProfile::reviewer_default()
        );
        let reviewer2 = profiles.reviewer2().claude().unwrap();
        assert_eq!(reviewer2.model, "opus");
        assert_eq!(reviewer2.effort, "xhigh");
        assert!(reviewer2.dangerously_skip_permissions);
        assert_eq!(profiles.canonical_hash().len(), 64);
    }

    #[test]
    fn claude_architect_uses_the_independent_claude_reviewer_default() {
        let profiles = SessionInvocationProfiles::for_architect(ArchitectAdapter::Claude);
        profiles.validate().unwrap();
        let architect = profiles.architect.claude().unwrap();
        let reviewer = profiles.reviewer2().claude().unwrap();
        assert_eq!(architect.model, "opus");
        assert_eq!(architect.effort, "xhigh");
        assert_eq!(profiles.reviewer_adapter_name(), CODEX_REVIEWER_ADAPTER);
        assert_eq!(reviewer.model, "opus");
        assert_eq!(reviewer.effort, "xhigh");
        assert!(reviewer.dangerously_skip_permissions);
    }

    #[test]
    fn all_three_claude_roles_share_the_production_default() {
        let architect = ClaudeInvocationProfile::architect_default();
        let developer = ClaudeInvocationProfile::developer_default();
        let reviewer = ClaudeInvocationProfile::reviewer_default();
        for profile in [&architect, &developer, &reviewer] {
            assert_eq!(profile.model, "opus");
            assert_eq!(profile.effort, "xhigh");
            assert!(profile.dangerously_skip_permissions);
        }
        assert_eq!(architect, developer);
        assert_eq!(developer, reviewer);
    }

    #[test]
    fn task_lane_defaults_to_codex_developer_codex_reviewer1_and_claude_reviewer2() {
        let profiles = SessionInvocationProfiles::for_task_lane(ArchitectAdapter::Codex).unwrap();
        profiles.validate().unwrap();
        assert_eq!(profiles.developer_adapter_name(), CODEX_DEVELOPER_ADAPTER);
        assert_eq!(profiles.reviewer_adapter_name(), CODEX_REVIEWER_ADAPTER);
        assert_eq!(
            profiles.developer.codex().unwrap(),
            &CodexInvocationProfile {
                model: "gpt-5.6-sol".into(),
                reasoning_effort: "xhigh".into(),
                sandbox: CodexSandbox::DangerFullAccess,
                approval_policy: CodexApprovalPolicy::Never,
            }
        );
        assert_eq!(
            profiles.reviewer1().codex().unwrap(),
            &CodexInvocationProfile::reviewer_default()
        );
        assert_eq!(
            profiles.reviewer2().claude().unwrap(),
            &ClaudeInvocationProfile {
                model: "opus".into(),
                effort: "xhigh".into(),
                dangerously_skip_permissions: true,
            }
        );
        // A Claude Architect changes only the foreground role.
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
        assert_eq!(claude.reviewers, profiles.reviewers);
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

        let mut left = SessionInvocationProfiles {
            developer,
            ..SessionInvocationProfiles::default()
        };
        *left.reviewer1_mut() = claude;
        left.validate().unwrap();
        let mut right = left.clone();
        right.developer = DeveloperInvocationProfile::Codex {
            profile: CodexInvocationProfile::developer_default(),
        };
        assert_ne!(left.canonical_hash(), right.canonical_hash());
    }

    #[test]
    fn reviewer_collection_validation_and_hash_bind_lane_identity_and_order() {
        let profiles = SessionInvocationProfiles::default();
        profiles.validate().unwrap();
        assert_eq!(profiles.reviewers.len(), 2);
        assert_eq!(profiles.reviewers[0].reviewer_id, ReviewerId::Reviewer1);
        assert_eq!(profiles.reviewers[1].reviewer_id, ReviewerId::Reviewer2);

        let mut wrong_identity = profiles.clone();
        wrong_identity.reviewers.swap(0, 1);
        assert!(wrong_identity.validate().is_err());
        assert_ne!(
            profiles.canonical_hash(),
            wrong_identity.canonical_hash(),
            "reviewer lane identity must be hash-bound"
        );

        let mut changed_profile = profiles.clone();
        let ReviewerInvocationProfile::Claude { profile } = changed_profile.reviewer2_mut() else {
            unreachable!()
        };
        profile.effort = "medium".into();
        assert_ne!(
            profiles.canonical_hash(),
            changed_profile.canonical_hash(),
            "the complete reviewer profile must be hash-bound"
        );

        let mut missing_reviewer2 = profiles.clone();
        missing_reviewer2.reviewers.pop();
        let mut reviewer2_then_reviewer1 = profiles.clone();
        reviewer2_then_reviewer1.reviewers.swap(0, 1);
        assert!(missing_reviewer2.validate().is_err());
        assert!(reviewer2_then_reviewer1.validate().is_err());
        assert_ne!(
            profiles.canonical_hash(),
            reviewer2_then_reviewer1.canonical_hash(),
            "reviewer lane order must be hash-bound"
        );
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
