use super::validation::{
    MAX_TEXT_BYTES, validate_git_oid, validate_list, validate_relative_path, validate_text,
    validate_unique_texts,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_RESULT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperDecision {
    Completed,
    NeedsInput,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Passed,
    Failed,
    NotRun,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitSummary {
    pub sha: String,
    pub subject: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckResult {
    pub command: String,
    pub status: CheckStatus,
    pub summary: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeveloperResult {
    pub decision: DeveloperDecision,
    pub summary: String,
    pub head_revision: Option<String>,
    pub commits: Vec<CommitSummary>,
    pub checks: Vec<CheckResult>,
    pub questions: Vec<String>,
    pub risks: Vec<String>,
    pub changed_paths: Vec<String>,
}

impl DeveloperResult {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_RESULT_BYTES {
            bail!("developer result exceeds its bounded JSON size");
        }
        let result: Self =
            serde_json::from_slice(bytes).context("developer result is not strict JSON")?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        validate_text("developer summary", &self.summary, MAX_TEXT_BYTES, true)?;
        if let Some(head) = &self.head_revision {
            validate_git_oid("developer head_revision", head)?;
        }

        validate_list("developer commits", &self.commits)?;
        let mut commit_ids = BTreeSet::new();
        for commit in &self.commits {
            validate_git_oid("commit sha", &commit.sha)?;
            validate_text("commit subject", &commit.subject, 512, false)?;
            if !commit_ids.insert(&commit.sha) {
                bail!("developer commit shas must be unique");
            }
        }
        if let (Some(head), Some(last_commit)) = (&self.head_revision, self.commits.last())
            && last_commit.sha != *head
        {
            bail!("last developer commit must equal head_revision");
        }

        validate_checks(&self.checks)?;
        validate_unique_texts("developer question", &self.questions, 16 * 1024, true)?;
        validate_unique_texts("developer risk", &self.risks, 16 * 1024, true)?;
        validate_list("changed paths", &self.changed_paths)?;
        let mut changed_paths = BTreeSet::new();
        for path in &self.changed_paths {
            validate_relative_path("changed path", path)?;
            if !changed_paths.insert(path) {
                bail!("changed paths must be unique");
            }
        }

        match self.decision {
            DeveloperDecision::Completed => {
                if self.head_revision.is_none() {
                    bail!("completed developer result requires head_revision");
                }
                if !self.questions.is_empty() {
                    bail!("completed developer result cannot contain questions");
                }
                if self
                    .checks
                    .iter()
                    .any(|check| check.status != CheckStatus::Passed)
                {
                    bail!("completed developer result cannot report an incomplete check");
                }
            }
            DeveloperDecision::NeedsInput if self.questions.is_empty() => {
                bail!("needs_input developer result requires at least one question");
            }
            DeveloperDecision::NeedsInput | DeveloperDecision::Blocked => {}
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).context("failed to encode developer result")
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    Lgtm,
    RequestChanges,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Major,
    Minor,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReviewFinding {
    pub severity: FindingSeverity,
    pub title: String,
    pub body: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReviewerResult {
    pub decision: ReviewDecision,
    pub summary: String,
    pub findings: Vec<ReviewFinding>,
    pub checks: Vec<CheckResult>,
}

impl ReviewerResult {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_RESULT_BYTES {
            bail!("reviewer result exceeds its bounded JSON size");
        }
        let result: Self =
            serde_json::from_slice(bytes).context("reviewer result is not strict JSON")?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        validate_text("review summary", &self.summary, MAX_TEXT_BYTES, true)?;
        validate_list("review findings", &self.findings)?;
        let mut major_count = 0usize;
        for finding in &self.findings {
            validate_text("finding title", &finding.title, 512, false)?;
            validate_text("finding body", &finding.body, MAX_TEXT_BYTES, true)?;
            match (&finding.file, finding.line) {
                (Some(file), Some(line)) if line > 0 => {
                    validate_relative_path("finding file", file)?;
                }
                (Some(file), None) => validate_relative_path("finding file", file)?,
                (None, None) => {}
                _ => bail!("finding line requires a file and must be positive"),
            }
            if finding.severity == FindingSeverity::Major {
                major_count += 1;
            }
        }
        validate_checks(&self.checks)?;
        match self.decision {
            ReviewDecision::RequestChanges if major_count == 0 => {
                bail!("request_changes reviewer result requires at least one major finding");
            }
            ReviewDecision::Lgtm if major_count != 0 => {
                bail!("lgtm reviewer result cannot contain a major finding");
            }
            ReviewDecision::Lgtm
                if self
                    .checks
                    .iter()
                    .any(|check| check.status == CheckStatus::Failed) =>
            {
                bail!("lgtm reviewer result cannot report a failed check");
            }
            ReviewDecision::Lgtm | ReviewDecision::RequestChanges => {}
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).context("failed to encode reviewer result")
    }
}

fn validate_checks(checks: &[CheckResult]) -> Result<()> {
    validate_list("checks", checks)?;
    let mut commands = BTreeSet::new();
    for check in checks {
        // Codex preserves embedded newlines for shell scripts in its
        // command events. Result commands must be able to carry that exact
        // evidence while still rejecting escape, CR, C1, and other controls.
        validate_text("check command", &check.command, 4096, true)?;
        validate_text("check summary", &check.summary, 4096, true)?;
        if !commands.insert(&check.command) {
            bail!("check commands must be unique");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(character: char) -> String {
        std::iter::repeat_n(character, 40).collect()
    }

    fn completed() -> DeveloperResult {
        let head = oid('a');
        DeveloperResult {
            decision: DeveloperDecision::Completed,
            summary: "implemented the bounded task".into(),
            head_revision: Some(head.clone()),
            commits: vec![CommitSummary {
                sha: head,
                subject: "Implement bounded task".into(),
            }],
            checks: vec![CheckResult {
                command: "cargo test".into(),
                status: CheckStatus::Passed,
                summary: "all tests passed".into(),
            }],
            questions: vec![],
            risks: vec!["none observed".into()],
            changed_paths: vec!["src/worker/result.rs".into()],
        }
    }

    #[test]
    fn developer_decision_semantics_are_strict() {
        let valid = completed();
        assert!(valid.validate().is_ok());
        assert!(DeveloperResult::parse(&serde_json::to_vec(&valid).unwrap()).is_ok());

        let mut missing_head = completed();
        missing_head.head_revision = None;
        assert!(missing_head.validate().is_err());

        let mut question = completed();
        question.questions.push("should this be complete?".into());
        assert!(question.validate().is_err());

        let mut failed_check = completed();
        failed_check.checks[0].status = CheckStatus::Failed;
        assert!(failed_check.validate().is_err());

        let mut needs_input = completed();
        needs_input.decision = DeveloperDecision::NeedsInput;
        needs_input.head_revision = None;
        needs_input.commits.clear();
        needs_input.checks.clear();
        needs_input.questions = vec!["which exact behavior is approved?".into()];
        assert!(needs_input.validate().is_ok());
    }

    #[test]
    fn reviewer_decision_semantics_are_strict() {
        let major = ReviewFinding {
            severity: FindingSeverity::Major,
            title: "Late result can apply".into(),
            body: "The callback omits the expected attempt CAS.".into(),
            file: Some("src/worker/result.rs".into()),
            line: Some(42),
        };
        let request_changes = ReviewerResult {
            decision: ReviewDecision::RequestChanges,
            summary: "one blocking correctness issue".into(),
            findings: vec![major.clone()],
            checks: vec![],
        };
        assert!(request_changes.validate().is_ok());

        let mut no_major = request_changes.clone();
        no_major.findings[0].severity = FindingSeverity::Minor;
        assert!(no_major.validate().is_err());

        let mut bad_lgtm = request_changes;
        bad_lgtm.decision = ReviewDecision::Lgtm;
        assert!(bad_lgtm.validate().is_err());

        let lgtm = ReviewerResult {
            decision: ReviewDecision::Lgtm,
            summary: "no blocking issue".into(),
            findings: vec![ReviewFinding {
                severity: FindingSeverity::Minor,
                title: "Naming".into(),
                body: "A future rename could improve clarity.".into(),
                file: None,
                line: None,
            }],
            checks: vec![],
        };
        assert!(lgtm.validate().is_ok());
    }

    #[test]
    fn result_shape_rejects_unknown_fields_controls_and_unsafe_paths() {
        let mut encoded: serde_json::Value =
            serde_json::to_value(completed()).expect("serialize fixture");
        encoded
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), true.into());
        assert!(DeveloperResult::parse(&serde_json::to_vec(&encoded).unwrap()).is_err());

        let mut control = completed();
        control.summary = "bad\u{1b}]0;title".into();
        assert!(control.validate().is_err());

        let mut multiline_check = completed();
        multiline_check.checks[0].command = "python3 - <<'PY'\nprint('bounded check')\nPY".into();
        assert!(multiline_check.validate().is_ok());
        multiline_check.checks[0].command.push('\r');
        assert!(multiline_check.validate().is_err());

        let mut traversal = completed();
        traversal.changed_paths = vec!["../outside".into()];
        assert!(traversal.validate().is_err());
    }
}
