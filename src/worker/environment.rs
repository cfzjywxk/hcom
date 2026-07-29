use super::validation::{validate_opaque_id, validate_sha256, validate_text};
use crate::control_api::WorkerRole;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const MAX_ENVIRONMENT_ENTRIES: usize = 64;
const MAX_ENVIRONMENT_VALUE_BYTES: usize = 16 * 1024;

const BASELINE_INHERITABLE_NAMES: &[&str] = &[
    "ALL_PROXY",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "NO_PROXY",
    "PATH",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TZ",
    "all_proxy",
    "http_proxy",
    "https_proxy",
    "no_proxy",
];

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPolicy {
    pub inherited_names: Vec<String>,
    pub required_names: Vec<String>,
}

impl EnvironmentPolicy {
    pub fn baseline() -> Self {
        Self {
            inherited_names: BASELINE_INHERITABLE_NAMES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            required_names: vec!["PATH".into()],
        }
    }

    pub fn new(inherited_names: Vec<String>, required_names: Vec<String>) -> Result<Self> {
        let policy = Self {
            inherited_names,
            required_names,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<()> {
        validate_environment_names("inherited environment", &self.inherited_names)?;
        validate_environment_names("required environment", &self.required_names)?;
        validate_case_unique("required environment", &self.required_names)?;
        let inherited: BTreeSet<_> = self.inherited_names.iter().map(String::as_str).collect();
        for name in &self.required_names {
            if !inherited.contains(name.as_str()) {
                bail!("required environment names must be inherited");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentLeaseDescriptor {
    pub lease_id: String,
    pub daemon_epoch: String,
    pub environment_hash: String,
    pub inherited_names: Vec<String>,
    pub required_names: Vec<String>,
}

impl EnvironmentLeaseDescriptor {
    pub fn validate(&self) -> Result<()> {
        validate_opaque_id("environment lease id", &self.lease_id)?;
        validate_opaque_id("environment daemon epoch", &self.daemon_epoch)?;
        validate_sha256("environment hash", &self.environment_hash)?;
        validate_environment_names("inherited environment", &self.inherited_names)?;
        validate_environment_names("required environment", &self.required_names)?;
        validate_case_unique("captured environment", &self.inherited_names)?;
        validate_case_unique("required environment", &self.required_names)?;
        EnvironmentPolicy {
            inherited_names: self.inherited_names.clone(),
            required_names: self.required_names.clone(),
        }
        .validate()?;
        let inherited: BTreeSet<_> = self.inherited_names.iter().map(String::as_str).collect();
        if self
            .required_names
            .iter()
            .any(|name| !inherited.contains(name.as_str()))
        {
            bail!("lease descriptor required names must be inherited");
        }
        Ok(())
    }

    pub fn require_daemon_epoch(&self, daemon_epoch: &str) -> Result<()> {
        validate_opaque_id("current daemon epoch", daemon_epoch)?;
        if self.daemon_epoch != daemon_epoch {
            bail!("environment lease belongs to a different daemon epoch");
        }
        Ok(())
    }
}

pub struct ExecutionEnvironmentLease {
    descriptor: EnvironmentLeaseDescriptor,
    values: BTreeMap<String, String>,
}

impl ExecutionEnvironmentLease {
    pub fn capture(
        lease_id: impl Into<String>,
        daemon_epoch: impl Into<String>,
        policy: &EnvironmentPolicy,
        values: Vec<(String, String)>,
    ) -> Result<Self> {
        policy.validate()?;
        if values.len() > MAX_ENVIRONMENT_ENTRIES {
            bail!("environment lease exceeds its bounded entry count");
        }
        let approved: BTreeSet<_> = policy.inherited_names.iter().map(String::as_str).collect();
        let mut captured = BTreeMap::new();
        let mut captured_casefolded = BTreeSet::new();
        for (name, value) in values {
            validate_environment_name(&name)?;
            if !approved.contains(name.as_str()) {
                bail!("environment name {name} is outside the closed lease policy");
            }
            validate_environment_value(&name, &value)?;
            if !captured_casefolded.insert(name.to_ascii_uppercase()) {
                bail!("environment lease contains a case-ambiguous name");
            }
            if captured.insert(name, value).is_some() {
                bail!("environment lease contains a duplicate name");
            }
        }
        for required in &policy.required_names {
            if !captured.contains_key(required) {
                bail!("environment lease is missing required name {required}");
            }
        }

        let lease_id = lease_id.into();
        let daemon_epoch = daemon_epoch.into();
        validate_opaque_id("environment lease id", &lease_id)?;
        validate_opaque_id("environment daemon epoch", &daemon_epoch)?;
        let descriptor = EnvironmentLeaseDescriptor {
            lease_id,
            daemon_epoch,
            environment_hash: environment_hash(&captured),
            inherited_names: captured.keys().cloned().collect(),
            required_names: policy.required_names.clone(),
        };
        descriptor.validate()?;
        Ok(Self {
            descriptor,
            values: captured,
        })
    }

    pub fn descriptor(&self) -> &EnvironmentLeaseDescriptor {
        &self.descriptor
    }

    pub fn materialize(
        &self,
        daemon_epoch: &str,
        identity: &WorkerEnvironmentIdentity,
    ) -> Result<MaterializedWorkerEnvironment> {
        self.descriptor.require_daemon_epoch(daemon_epoch)?;
        identity.validate()?;
        if environment_hash(&self.values) != self.descriptor.environment_hash {
            bail!("in-memory environment lease no longer matches its descriptor");
        }
        let mut values = self.values.clone();
        values.insert(
            "HCOM_WORKER_ROLE".into(),
            match identity.role {
                WorkerRole::Developer => "developer",
                WorkerRole::Reviewer => "reviewer",
            }
            .into(),
        );
        values.insert("HCOM_PROJECT_ID".into(), identity.project_id.clone());
        values.insert("HCOM_TASK_ID".into(), identity.task_id.clone());
        Ok(MaterializedWorkerEnvironment { values })
    }

    pub(crate) fn redactor(&self) -> SecretRedactor {
        SecretRedactor::from_values(self.values.values().map(String::as_str))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct WorkerEnvironmentIdentity {
    pub role: WorkerRole,
    pub project_id: String,
    pub task_id: String,
}

impl WorkerEnvironmentIdentity {
    fn validate(&self) -> Result<()> {
        validate_opaque_id("worker project id", &self.project_id)?;
        validate_opaque_id("worker task id", &self.task_id)
    }
}

pub struct MaterializedWorkerEnvironment {
    values: BTreeMap<String, String>,
}

impl MaterializedWorkerEnvironment {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    pub(crate) fn require_exact(&self, requirements: &[ExactEnvironmentRequirement]) -> Result<()> {
        let mut previous = None;
        for requirement in requirements {
            requirement.validate()?;
            if previous.is_some_and(|name| name >= requirement.name.as_str()) {
                bail!("exact environment requirements must use unique canonical order");
            }
            previous = Some(requirement.name.as_str());
            if self.values.get(&requirement.name) != Some(&requirement.value) {
                bail!("materialized worker environment does not match its exact path contract");
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ExactEnvironmentRequirement {
    name: String,
    value: String,
}

impl ExactEnvironmentRequirement {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let requirement = Self {
            name: name.into(),
            value: value.into(),
        };
        requirement.validate()?;
        Ok(requirement)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    fn validate(&self) -> Result<()> {
        validate_environment_name(&self.name)?;
        validate_environment_value(&self.name, &self.value)
    }
}

pub(crate) struct SecretRedactor {
    sensitive_values: Vec<String>,
    replacement: &'static str,
}

impl SecretRedactor {
    pub(crate) fn from_values<'a>(values: impl IntoIterator<Item = &'a str>) -> Self {
        let mut sensitive = BTreeSet::new();
        for value in values {
            if !value.is_empty() {
                sensitive.insert(value.to_owned());
                collect_proxy_credentials(value, &mut sensitive);
            }
        }
        let mut sensitive_values: Vec<_> = sensitive.into_iter().collect();
        sensitive_values.sort_by_key(|value| std::cmp::Reverse(value.len()));
        let replacement = redaction_replacement(&sensitive_values);
        Self {
            sensitive_values,
            replacement,
        }
    }

    pub(crate) fn redact(&self, input: &str) -> String {
        let mut redacted = input.to_owned();
        for value in &self.sensitive_values {
            redacted = redacted.replace(value, self.replacement);
        }
        redacted
    }

    pub(crate) fn would_redact(&self, input: &str) -> bool {
        self.sensitive_values
            .iter()
            .any(|value| input.contains(value))
    }

    pub(crate) fn with_value(&self, value: &str) -> Self {
        let mut sensitive: BTreeSet<_> = self.sensitive_values.iter().cloned().collect();
        if !value.is_empty() {
            sensitive.insert(value.to_owned());
        }
        let mut sensitive_values: Vec<_> = sensitive.into_iter().collect();
        sensitive_values.sort_by_key(|value| std::cmp::Reverse(value.len()));
        let replacement = redaction_replacement(&sensitive_values);
        Self {
            sensitive_values,
            replacement,
        }
    }

    pub(crate) fn trailing_guard_bytes(&self) -> usize {
        self.sensitive_values
            .iter()
            .map(String::len)
            .max()
            .unwrap_or(0)
    }
}

fn validate_environment_names(label: &str, names: &[String]) -> Result<()> {
    if names.len() > MAX_ENVIRONMENT_ENTRIES {
        bail!("{label} exceeds its bounded entry count");
    }
    let mut unique = BTreeSet::new();
    for name in names {
        validate_environment_name(name)?;
        if !unique.insert(name) {
            bail!("{label} names must be unique");
        }
    }
    if !names.windows(2).all(|pair| pair[0] < pair[1]) {
        bail!("{label} names must use canonical sorted order");
    }
    Ok(())
}

fn validate_case_unique(label: &str, names: &[String]) -> Result<()> {
    let mut casefolded = BTreeSet::new();
    for name in names {
        if !casefolded.insert(name.to_ascii_uppercase()) {
            bail!("{label} names must not be case-ambiguous");
        }
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || name.starts_with("HCOM_")
        || is_runtime_identity_marker(name)
        || is_secret_shaped(name)
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("environment name is forbidden or malformed");
    }
    Ok(())
}

fn is_runtime_identity_marker(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "COLORTERM" | "STY" | "TERM" | "TERM_PROGRAM" | "TMUX" | "WINDOWID"
    ) || upper.contains("_AGENT_")
        || upper.ends_with("_AGENT")
        || upper.ends_with("_SESSION")
        || upper.ends_with("_SESSION_ID")
        || upper.ends_with("_THREAD_ID")
        || upper.contains("ARCHITECT")
        || upper.contains("CHAIN")
        || upper.contains("HANDOFF")
}

fn is_secret_shaped(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "COOKIE",
        "API_KEY",
        "APIKEY",
        "AUTH",
        "BEARER",
        "CREDENTIAL",
    ]
    .iter()
    .any(|fragment| upper.contains(fragment))
}

fn validate_environment_value(name: &str, value: &str) -> Result<()> {
    validate_text(
        &format!("environment value for {name}"),
        value,
        MAX_ENVIRONMENT_VALUE_BYTES,
        false,
    )
}

fn environment_hash(values: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"hcom-worker-environment-lease-v1\0");
    for (name, value) in values {
        hash_field(&mut hasher, name.as_bytes());
        hash_field(&mut hasher, value.as_bytes());
    }
    hex_bytes(&hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn collect_proxy_credentials(value: &str, sensitive: &mut BTreeSet<String>) {
    let Some((_, authority_and_path)) = value.split_once("://") else {
        return;
    };
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    let Some((userinfo, _)) = authority.rsplit_once('@') else {
        return;
    };
    if !userinfo.is_empty() {
        sensitive.insert(userinfo.to_owned());
    }
    if let Some((username, password)) = userinfo.split_once(':') {
        if !username.is_empty() {
            sensitive.insert(username.to_owned());
        }
        if !password.is_empty() {
            sensitive.insert(password.to_owned());
        }
    }
}

fn redaction_replacement(sensitive_values: &[String]) -> &'static str {
    ["[REDACTED]", "<masked>", "***", ""]
        .into_iter()
        .find(|candidate| {
            sensitive_values
                .iter()
                .all(|value| !candidate.contains(value))
        })
        .expect("the empty replacement cannot contain a nonempty sensitive value")
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_lease(proxy: &str) -> ExecutionEnvironmentLease {
        ExecutionEnvironmentLease::capture(
            "lease-1",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            vec![
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("LANG".into(), "C.UTF-8".into()),
                ("HTTPS_PROXY".into(), proxy.into()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn closed_policy_rejects_unknown_secret_and_identity_names() {
        let policy = EnvironmentPolicy::baseline();
        for name in [
            "UNAPPROVED",
            "SERVICE_TOKEN",
            "HCOM_AGENT",
            "FAKE_AGENT",
            "FAKE_SESSION_ID",
            "ARCHITECT_BINDING",
            "CHAIN_ID",
            "HANDOFF_ID",
            "TERM_PROGRAM",
        ] {
            assert!(
                ExecutionEnvironmentLease::capture(
                    "lease-1",
                    "epoch-1",
                    &policy,
                    vec![
                        ("PATH".into(), "/usr/bin".into()),
                        (name.into(), "sentinel".into())
                    ],
                )
                .is_err(),
                "{name} unexpectedly entered the lease"
            );
        }
        assert!(
            ExecutionEnvironmentLease::capture(
                "lease-1",
                "epoch-1",
                &policy,
                vec![
                    ("PATH".into(), "/usr/bin".into()),
                    ("PATH".into(), "/bin".into())
                ],
            )
            .is_err()
        );
        assert!(
            ExecutionEnvironmentLease::capture(
                "lease-1",
                "epoch-1",
                &policy,
                vec![
                    ("PATH".into(), "/usr/bin".into()),
                    ("HTTPS_PROXY".into(), "http://upper.invalid".into()),
                    ("https_proxy".into(), "http://lower.invalid".into()),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_generated_roots_can_be_pinned_without_open_inheritance() {
        let names = vec![
            "FAKE_CONFIG_ROOT".into(),
            "HOME".into(),
            "PATH".into(),
            "TMPDIR".into(),
            "XDG_RUNTIME_DIR".into(),
        ];
        let policy = EnvironmentPolicy::new(names.clone(), names).unwrap();
        let values = vec![
            ("FAKE_CONFIG_ROOT".into(), "/private/config".into()),
            ("HOME".into(), "/private/home".into()),
            ("PATH".into(), "/usr/bin".into()),
            ("TMPDIR".into(), "/private/tmp".into()),
            ("XDG_RUNTIME_DIR".into(), "/private/runtime".into()),
        ];
        let lease =
            ExecutionEnvironmentLease::capture("lease-1", "epoch-1", &policy, values.clone())
                .unwrap();
        assert_eq!(lease.descriptor().required_names, policy.required_names);

        let mut unexpected = values;
        unexpected.push(("UNLISTED".into(), "must-not-enter".into()));
        assert!(
            ExecutionEnvironmentLease::capture("lease-2", "epoch-1", &policy, unexpected,).is_err()
        );
    }

    #[test]
    fn descriptor_contains_only_names_hash_and_epoch() {
        let lease = fixture_lease("http://worker-user:worker-pass@proxy.invalid:8080");
        let encoded = serde_json::to_string(lease.descriptor()).unwrap();
        assert!(!encoded.contains("worker-user"));
        assert!(!encoded.contains("worker-pass"));
        assert!(!encoded.contains("/usr/bin:/bin"));
        lease.descriptor().validate().unwrap();

        let changed = fixture_lease("http://worker-user:changed@proxy.invalid:8080");
        assert_ne!(
            lease.descriptor().environment_hash,
            changed.descriptor().environment_hash
        );
        assert!(lease.descriptor().require_daemon_epoch("epoch-2").is_err());
    }

    #[test]
    fn materialization_adds_only_closed_worker_identity_markers() {
        let lease = fixture_lease("http://proxy.invalid:8080");
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Reviewer,
                    project_id: "project-1".into(),
                    task_id: "task-2".into(),
                },
            )
            .unwrap();
        let values: BTreeMap<_, _> = materialized.iter().collect();
        assert_eq!(values["HCOM_WORKER_ROLE"], "reviewer");
        assert_eq!(values["HCOM_PROJECT_ID"], "project-1");
        assert_eq!(values["HCOM_TASK_ID"], "task-2");
        assert!(!values.contains_key("HCOM_AGENT"));
        assert!(!values.contains_key("TERM_PROGRAM"));
    }

    #[test]
    fn exact_environment_requirements_reject_missing_drift_and_ambiguous_order() {
        let names = vec![
            "CODEX_HOME".into(),
            "HOME".into(),
            "PATH".into(),
            "TMPDIR".into(),
            "XDG_RUNTIME_DIR".into(),
        ];
        let lease = ExecutionEnvironmentLease::capture(
            "lease-1",
            "epoch-1",
            &EnvironmentPolicy::new(names.clone(), names).unwrap(),
            vec![
                ("CODEX_HOME".into(), "/isolated/codex".into()),
                ("HOME".into(), "/isolated/home".into()),
                ("PATH".into(), "/usr/bin".into()),
                ("TMPDIR".into(), "/isolated/tmp".into()),
                ("XDG_RUNTIME_DIR".into(), "/run/user/1000".into()),
            ],
        )
        .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    project_id: "project-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        let exact = vec![
            ExactEnvironmentRequirement::new("CODEX_HOME", "/isolated/codex").unwrap(),
            ExactEnvironmentRequirement::new("HOME", "/isolated/home").unwrap(),
            ExactEnvironmentRequirement::new("TMPDIR", "/isolated/tmp").unwrap(),
            ExactEnvironmentRequirement::new("XDG_RUNTIME_DIR", "/run/user/1000").unwrap(),
        ];
        materialized.require_exact(&exact).unwrap();

        let drifted = vec![ExactEnvironmentRequirement::new("HOME", "/different/home").unwrap()];
        assert!(materialized.require_exact(&drifted).is_err());
        let missing = vec![ExactEnvironmentRequirement::new("LC_ALL", "C.UTF-8").unwrap()];
        assert!(materialized.require_exact(&missing).is_err());
        let unordered = vec![
            ExactEnvironmentRequirement::new("HOME", "/isolated/home").unwrap(),
            ExactEnvironmentRequirement::new("CODEX_HOME", "/isolated/codex").unwrap(),
        ];
        assert!(materialized.require_exact(&unordered).is_err());
    }

    #[test]
    fn redactor_removes_raw_values_and_proxy_credentials() {
        let lease = fixture_lease("http://worker-user:worker-pass@proxy.invalid:8080");
        let output = lease.redactor().redact(
            "proxy=http://worker-user:worker-pass@proxy.invalid:8080 user=worker-user password=worker-pass",
        );
        assert!(!output.contains("worker-user"));
        assert!(!output.contains("worker-pass"));
        assert!(output.contains("[REDACTED]"));
    }
}
