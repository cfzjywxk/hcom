use super::validation::{validate_opaque_id, validate_sha256};
use crate::control_api::WorkerRole;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};

const MAX_POLICY_ENTRIES: usize = 64;
const MAX_ENVIRONMENT_VALUE_BYTES: usize = 16 * 1024;
// Substring redaction cannot safely treat low-entropy operational atoms such
// as "1" or "false" as secrets: doing so corrupts otherwise valid structured
// worker output (for example `task1.txt`) and makes every ordinary occurrence
// indistinguishable from an environment leak.
const MIN_ENVIRONMENT_REDACTION_BYTES: usize = 8;

#[derive(Clone)]
pub struct ParentEnvironment {
    values: BTreeMap<OsString, OsString>,
}

impl ParentEnvironment {
    pub fn capture_current() -> Self {
        Self {
            values: std::env::vars_os().collect(),
        }
    }

    pub fn from_unicode(values: BTreeMap<String, String>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_os(values: Vec<(OsString, OsString)>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    pub fn unicode(&self, name: &str) -> Result<Option<&str>> {
        self.values
            .get(OsStr::new(name))
            .map(|value| {
                value
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("parent environment value {name} is not UTF-8"))
            })
            .transpose()
    }

    fn values(&self) -> &BTreeMap<OsString, OsString> {
        &self.values
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&OsStr, &OsStr)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
    }
}

impl From<BTreeMap<String, String>> for ParentEnvironment {
    fn from(values: BTreeMap<String, String>) -> Self {
        Self::from_unicode(values)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPolicy {
    /// Exact names hcom may replace after cloning the complete parent snapshot.
    pub override_names: Vec<String>,
    /// UTF-8 names that must be present and non-empty after those replacements.
    pub required_names: Vec<String>,
}

impl EnvironmentPolicy {
    pub fn baseline() -> Self {
        Self {
            override_names: Vec::new(),
            required_names: vec!["PATH".into()],
        }
    }

    pub fn new(override_names: Vec<String>, required_names: Vec<String>) -> Result<Self> {
        let policy = Self {
            override_names,
            required_names,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<()> {
        validate_environment_names("worker environment overrides", &self.override_names)?;
        validate_environment_names("required environment", &self.required_names)?;
        validate_case_unique("worker environment overrides", &self.override_names)?;
        validate_case_unique("required environment", &self.required_names)?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentLeaseDescriptor {
    pub lease_id: String,
    pub supervisor_epoch: String,
    pub environment_hash: String,
    pub inherited_names: Vec<String>,
    pub required_names: Vec<String>,
}

impl EnvironmentLeaseDescriptor {
    pub fn validate(&self) -> Result<()> {
        validate_opaque_id("environment lease id", &self.lease_id)?;
        validate_opaque_id("environment supervisor epoch", &self.supervisor_epoch)?;
        validate_sha256("environment hash", &self.environment_hash)?;
        validate_environment_names("required environment", &self.required_names)?;
        validate_case_unique("required environment", &self.required_names)?;
        if self.inherited_names.len() > u16::MAX as usize
            || !self
                .inherited_names
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            bail!("inherited environment descriptor is invalid");
        }
        let inherited: BTreeSet<_> = self.inherited_names.iter().map(String::as_str).collect();
        if self
            .required_names
            .iter()
            .map(|name| environment_name_descriptor(OsStr::new(name)))
            .any(|name| !inherited.contains(name.as_str()))
        {
            bail!("lease descriptor required names must be inherited");
        }
        Ok(())
    }

    pub fn require_supervisor_epoch(&self, supervisor_epoch: &str) -> Result<()> {
        validate_opaque_id("current supervisor epoch", supervisor_epoch)?;
        if self.supervisor_epoch != supervisor_epoch {
            bail!("environment lease belongs to a different supervisor epoch");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ExecutionEnvironmentLease {
    descriptor: EnvironmentLeaseDescriptor,
    values: BTreeMap<OsString, OsString>,
    redaction_values: Vec<String>,
}

impl ExecutionEnvironmentLease {
    pub fn capture(
        lease_id: impl Into<String>,
        supervisor_epoch: impl Into<String>,
        policy: &EnvironmentPolicy,
        values: Vec<(String, String)>,
    ) -> Result<Self> {
        Self::capture_complete(
            lease_id,
            supervisor_epoch,
            policy,
            &ParentEnvironment::from_unicode(values.into_iter().collect()),
            Vec::new(),
        )
    }

    pub fn capture_complete(
        lease_id: impl Into<String>,
        supervisor_epoch: impl Into<String>,
        policy: &EnvironmentPolicy,
        parent: &ParentEnvironment,
        overrides: Vec<(String, String)>,
    ) -> Result<Self> {
        policy.validate()?;
        let approved: BTreeSet<_> = policy.override_names.iter().map(String::as_str).collect();
        let mut captured = parent.values().clone();
        for (name, value) in overrides {
            validate_environment_name(&name)?;
            if !approved.contains(name.as_str()) {
                bail!("environment override {name} is outside the role-local policy");
            }
            validate_environment_value(&name, &value)?;
            captured.insert(name.into(), value.into());
        }
        for required in &policy.required_names {
            if captured
                .get(OsStr::new(required))
                .is_none_or(|value| value.is_empty())
            {
                bail!("environment lease is missing a non-empty required name {required}");
            }
        }

        let lease_id = lease_id.into();
        let supervisor_epoch = supervisor_epoch.into();
        validate_opaque_id("environment lease id", &lease_id)?;
        validate_opaque_id("environment supervisor epoch", &supervisor_epoch)?;
        let descriptor = EnvironmentLeaseDescriptor {
            lease_id,
            supervisor_epoch,
            environment_hash: environment_hash(&captured),
            inherited_names: inherited_environment_names(&captured),
            required_names: policy.required_names.clone(),
        };
        descriptor.validate()?;
        Ok(Self {
            descriptor,
            values: captured,
            redaction_values: Vec::new(),
        })
    }

    pub fn descriptor(&self) -> &EnvironmentLeaseDescriptor {
        &self.descriptor
    }

    pub fn materialize(
        &self,
        supervisor_epoch: &str,
        identity: &WorkerEnvironmentIdentity,
    ) -> Result<MaterializedWorkerEnvironment> {
        self.descriptor.require_supervisor_epoch(supervisor_epoch)?;
        identity.validate()?;
        if environment_hash(&self.values) != self.descriptor.environment_hash {
            bail!("in-memory environment lease no longer matches its descriptor");
        }
        let mut values = self.values.clone();
        values.insert(
            OsString::from("HCOM_WORKER_ROLE"),
            match identity.role {
                WorkerRole::Developer => "developer",
                WorkerRole::Reviewer => "reviewer",
            }
            .into(),
        );
        values.insert(
            OsString::from("HCOM_RUN_ID"),
            identity.run_id.clone().into(),
        );
        values.insert(
            OsString::from("HCOM_TASK_ID"),
            identity.task_id.clone().into(),
        );
        Ok(MaterializedWorkerEnvironment { values })
    }

    pub(crate) fn redactor(&self) -> SecretRedactor {
        // Complete inheritance and secret containment are deliberately
        // separate contracts. Operational values such as PWD, SHELL, LANG and
        // PATH are expected to appear in native banners and result summaries;
        // treating every inherited value as a secret rejects otherwise valid
        // turns. Only secret-shaped names contribute their complete value.
        // Credentials embedded in URI userinfo are collected independently so
        // proxy/database URLs remain protected without hiding the whole URL.
        let mut sensitive = BTreeSet::new();
        for (name, value) in &self.values {
            let Some(value) = value.to_str() else {
                continue;
            };
            if is_secret_shaped_environment_name(name) {
                collect_sensitive_value(value, &mut sensitive);
            }
            collect_proxy_credentials(value, &mut sensitive);
        }
        for value in &self.redaction_values {
            collect_sensitive_value(value, &mut sensitive);
        }
        SecretRedactor::from_sensitive_values(sensitive)
    }

    pub(crate) fn with_secret_redaction_values(mut self, values: Vec<String>) -> Result<Self> {
        validate_secret_redaction_values(&values)?;
        let mut unique = BTreeSet::new();
        for value in values {
            unique.insert(value);
        }
        self.redaction_values = unique.into_iter().collect();
        Ok(self)
    }
}

pub(crate) fn validate_secret_redaction_values(values: &[String]) -> Result<()> {
    if values.len() > MAX_POLICY_ENTRIES {
        bail!("secret redaction inventory exceeds its bounded entry count");
    }
    for value in values {
        if value.len() < MIN_ENVIRONMENT_REDACTION_BYTES
            || value.len() > MAX_ENVIRONMENT_VALUE_BYTES
            || value.chars().any(char::is_control)
        {
            bail!("secret redaction value has an unsafe or ineffective shape");
        }
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
pub struct WorkerEnvironmentIdentity {
    pub role: WorkerRole,
    pub run_id: String,
    pub task_id: String,
}

impl WorkerEnvironmentIdentity {
    fn validate(&self) -> Result<()> {
        validate_opaque_id("worker run id", &self.run_id)?;
        validate_opaque_id("worker task id", &self.task_id)
    }
}

pub struct MaterializedWorkerEnvironment {
    values: BTreeMap<OsString, OsString>,
}

impl MaterializedWorkerEnvironment {
    pub fn iter(&self) -> impl Iterator<Item = (&OsStr, &OsStr)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
    }

    pub(crate) fn require_exact(&self, requirements: &[ExactEnvironmentRequirement]) -> Result<()> {
        let mut previous = None;
        for requirement in requirements {
            requirement.validate()?;
            if previous.is_some_and(|name| name >= requirement.name.as_str()) {
                bail!("exact environment requirements must use unique canonical order");
            }
            previous = Some(requirement.name.as_str());
            if self
                .values
                .get(OsStr::new(&requirement.name))
                .map(OsString::as_os_str)
                != Some(OsStr::new(&requirement.value))
            {
                bail!("materialized worker environment does not match its exact path contract");
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get(&self, name: &OsStr) -> Option<&OsStr> {
        self.values.get(name).map(OsString::as_os_str)
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
    fn from_sensitive_values(sensitive: BTreeSet<String>) -> Self {
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
    if names.len() > MAX_POLICY_ENTRIES {
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
    let mut casefolded: BTreeMap<String, String> = BTreeMap::new();
    for name in names {
        let folded = name.to_ascii_uppercase();
        if casefolded.contains_key(&folded) {
            bail!("{label} names must not be case-ambiguous");
        }
        casefolded.insert(folded, name.clone());
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("environment name is forbidden or malformed");
    }
    Ok(())
}

fn validate_environment_value(name: &str, value: &str) -> Result<()> {
    if value.len() > MAX_ENVIRONMENT_VALUE_BYTES
        || value.chars().any(|character| {
            character == '\u{1b}'
                || character == '\r'
                || character == '\n'
                || character == '\t'
                || character.is_control()
                || ('\u{80}'..='\u{9f}').contains(&character)
        })
    {
        bail!("environment value for {name} has an invalid bounded shape");
    }
    Ok(())
}

fn inherited_environment_names(values: &BTreeMap<OsString, OsString>) -> Vec<String> {
    let mut names: Vec<_> = values
        .keys()
        .map(|name| environment_name_descriptor(name))
        .collect();
    names.sort();
    names
}

fn environment_name_descriptor(name: &OsStr) -> String {
    match name.to_str() {
        Some(name) => format!("utf8:{name}"),
        None => format!("bytes:{}", hex_bytes(name.as_encoded_bytes())),
    }
}

fn environment_hash(values: &BTreeMap<OsString, OsString>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"hcom-worker-environment-lease-v2\0");
    for (name, value) in values {
        hash_field(&mut hasher, name.as_encoded_bytes());
        hash_field(&mut hasher, value.as_encoded_bytes());
    }
    hex_bytes(&hasher.finalize())
}

fn is_secret_shaped_environment_name(name: &OsStr) -> bool {
    let upper = name.to_string_lossy().to_ascii_uppercase();
    let tokens: Vec<_> = upper
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    if tokens.iter().any(|token| {
        matches!(
            *token,
            "APIKEY"
                | "ASKPASS"
                | "AUTH"
                | "AUTHTOKEN"
                | "AUTHORIZATION"
                | "BEARER"
                | "COOKIE"
                | "CREDENTIAL"
                | "CREDENTIALS"
                | "JWT"
                | "PASSWORD"
                | "PASSPHRASE"
                | "PASSWD"
                | "SECRET"
                | "TOKEN"
                | "WEBHOOK"
        )
    }) {
        return true;
    }
    tokens.windows(2).any(|pair| {
        matches!(
            pair,
            ["API", "KEY"] | ["ACCESS", "KEY"] | ["PRIVATE", "KEY"] | ["CONNECTION", "STRING"]
        )
    }) || (tokens.last() == Some(&"KEY")
        && tokens.get(tokens.len().saturating_sub(2)) != Some(&"PUBLIC"))
}

fn collect_sensitive_value(value: &str, sensitive: &mut BTreeSet<String>) {
    if value.len() >= MIN_ENVIRONMENT_REDACTION_BYTES {
        sensitive.insert(value.to_owned());
    }
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
    collect_sensitive_value(userinfo, sensitive);
    if let Some((username, password)) = userinfo.split_once(':') {
        collect_sensitive_value(username, sensitive);
        collect_sensitive_value(password, sensitive);
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
    fn complete_parent_environment_accepts_unknown_secret_identity_and_empty_values() {
        let parent = ParentEnvironment::from_unicode(BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("UNAPPROVED".into(), "arbitrary-value".into()),
            ("SERVICE_TOKEN".into(), "secret-shaped-value".into()),
            ("HCOM_AGENT".into(), "parent-agent".into()),
            ("TERM_PROGRAM".into(), "parent-terminal".into()),
            ("EMPTY_VALUE".into(), String::new()),
        ]));
        let lease = ExecutionEnvironmentLease::capture_complete(
            "lease-1",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            &parent,
            Vec::new(),
        )
        .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();

        for (name, value) in [
            ("UNAPPROVED", "arbitrary-value"),
            ("SERVICE_TOKEN", "secret-shaped-value"),
            ("HCOM_AGENT", "parent-agent"),
            ("TERM_PROGRAM", "parent-terminal"),
            ("EMPTY_VALUE", ""),
        ] {
            assert_eq!(
                materialized.get(OsStr::new(name)),
                Some(OsStr::new(value)),
                "{name} did not survive complete inheritance"
            );
        }
    }

    #[test]
    fn complete_parent_environment_is_not_limited_by_override_policy_size() {
        let mut values = BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]);
        for index in 0..128 {
            values.insert(format!("PARENT_VALUE_{index:03}"), format!("value-{index}"));
        }
        let parent = ParentEnvironment::from_unicode(values);
        let lease = ExecutionEnvironmentLease::capture_complete(
            "lease-many-parent-values",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            &parent,
            Vec::new(),
        )
        .unwrap();

        assert_eq!(lease.descriptor().inherited_names.len(), 129);
    }

    #[test]
    fn exact_lower_and_upper_proxy_names_coexist_without_normalization() {
        let lease = ExecutionEnvironmentLease::capture(
            "lease-proxy-pairs",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            vec![
                ("PATH".into(), "/usr/bin".into()),
                ("HTTPS_PROXY".into(), "http://upper.invalid".into()),
                ("https_proxy".into(), "http://lower.invalid".into()),
                ("HTTP_PROXY".into(), "http://upper-http.invalid".into()),
                ("http_proxy".into(), "http://lower-http.invalid".into()),
            ],
        )
        .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        assert_eq!(
            materialized.get(OsStr::new("HTTPS_PROXY")),
            Some(OsStr::new("http://upper.invalid"))
        );
        assert_eq!(
            materialized.get(OsStr::new("https_proxy")),
            Some(OsStr::new("http://lower.invalid"))
        );
        assert_eq!(
            materialized.get(OsStr::new("HTTP_PROXY")),
            Some(OsStr::new("http://upper-http.invalid"))
        );
        assert_eq!(
            materialized.get(OsStr::new("http_proxy")),
            Some(OsStr::new("http://lower-http.invalid"))
        );
        assert_eq!(lease.descriptor().inherited_names.len(), 5);
        assert!(
            lease
                .descriptor()
                .inherited_names
                .contains(&environment_name_descriptor(OsStr::new("PATH")))
        );
    }

    #[test]
    fn role_local_override_policy_rejects_case_ambiguity() {
        assert!(
            EnvironmentPolicy::new(
                vec!["FOO".into(), "PATH".into(), "foo".into()],
                vec!["PATH".into()],
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_generated_roots_override_complete_parent_without_filtering_it() {
        let names = vec![
            "FAKE_CONFIG_ROOT".into(),
            "HOME".into(),
            "PATH".into(),
            "TMPDIR".into(),
            "XDG_RUNTIME_DIR".into(),
        ];
        let policy = EnvironmentPolicy::new(names.clone(), names).unwrap();
        let parent = ParentEnvironment::from_unicode(BTreeMap::from([
            ("HOME".into(), "/parent/home".into()),
            ("PATH".into(), "/usr/bin".into()),
            ("UNLISTED".into(), "must-enter".into()),
        ]));
        let overrides = vec![
            ("FAKE_CONFIG_ROOT".into(), "/private/config".into()),
            ("HOME".into(), "/private/home".into()),
            ("PATH".into(), "/usr/bin".into()),
            ("TMPDIR".into(), "/private/tmp".into()),
            ("XDG_RUNTIME_DIR".into(), "/private/runtime".into()),
        ];
        let lease = ExecutionEnvironmentLease::capture_complete(
            "lease-1", "epoch-1", &policy, &parent, overrides,
        )
        .unwrap();
        assert_eq!(lease.descriptor().required_names, policy.required_names);
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        assert_eq!(
            materialized.get(OsStr::new("UNLISTED")),
            Some(OsStr::new("must-enter"))
        );
        assert_eq!(
            materialized.get(OsStr::new("HOME")),
            Some(OsStr::new("/private/home"))
        );
        assert!(
            ExecutionEnvironmentLease::capture_complete(
                "lease-2",
                "epoch-1",
                &policy,
                &parent,
                vec![("NOT_AN_OVERRIDE".into(), "rejected".into())],
            )
            .is_err()
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
        assert!(
            lease
                .descriptor()
                .require_supervisor_epoch("epoch-2")
                .is_err()
        );
    }

    #[test]
    fn materialization_overrides_only_worker_identity_markers() {
        let lease = fixture_lease("http://proxy.invalid:8080");
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Reviewer,
                    run_id: "run-1".into(),
                    task_id: "task-2".into(),
                },
            )
            .unwrap();
        assert_eq!(
            materialized.get(OsStr::new("HCOM_WORKER_ROLE")),
            Some(OsStr::new("reviewer"))
        );
        assert_eq!(
            materialized.get(OsStr::new("HCOM_RUN_ID")),
            Some(OsStr::new("run-1"))
        );
        assert_eq!(
            materialized.get(OsStr::new("HCOM_TASK_ID")),
            Some(OsStr::new("task-2"))
        );
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
                    run_id: "run-1".into(),
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

    #[test]
    fn operational_environment_values_are_not_substring_secrets() {
        let names = vec![
            "CLAUDE_CODE_DISABLE_FAST_MODE".into(),
            "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION".into(),
            "PATH".into(),
        ];
        let lease = ExecutionEnvironmentLease::capture(
            "lease-short-operational-values",
            "epoch-1",
            &EnvironmentPolicy::new(names.clone(), names).unwrap(),
            vec![
                ("CLAUDE_CODE_DISABLE_FAST_MODE".into(), "1".into()),
                (
                    "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION".into(),
                    "false".into(),
                ),
                ("PATH".into(), "/usr/bin:/bin".into()),
            ],
        )
        .unwrap();
        let redactor = lease.redactor();

        assert_eq!(
            redactor.redact("Round 1 reviewed task1.txt; false is a fixed CLI flag"),
            "Round 1 reviewed task1.txt; false is a fixed CLI flag"
        );
        assert!(!redactor.would_redact("PATH was /usr/bin:/bin"));

        let prompt_redactor = redactor.with_value("fix it");
        assert!(
            !prompt_redactor
                .redact("model echoed: fix it")
                .contains("fix it")
        );
    }

    #[test]
    fn secret_name_detection_uses_tokens_without_matching_operational_author_names() {
        for name in [
            "SERVICE_ACCESS_TOKEN",
            "OPENAI_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "SSH_AUTH_SOCK",
            "GIT_ASKPASS",
            "DATABASE_PASSWORD",
            "CI_JOB_JWT",
            "SLACK_WEBHOOK_URL",
            "APP_SIGNING_KEY",
            "AZURE_STORAGE_CONNECTION_STRING",
        ] {
            assert!(
                is_secret_shaped_environment_name(OsStr::new(name)),
                "{name} should be secret-shaped"
            );
        }
        for name in [
            "PWD",
            "OLDPWD",
            "SHELL",
            "LANG",
            "LC_ALL",
            "PATH",
            "HOME",
            "GIT_AUTHOR_NAME",
            "TERM_PROGRAM",
            "SSH_PUBLIC_KEY",
        ] {
            assert!(
                !is_secret_shaped_environment_name(OsStr::new(name)),
                "{name} should remain operational"
            );
        }
    }

    #[test]
    fn empty_optional_proxy_values_preserve_presence_but_required_path_stays_nonempty() {
        let policy = EnvironmentPolicy::new(
            vec!["HTTPS_PROXY".into(), "PATH".into()],
            vec!["PATH".into()],
        )
        .unwrap();
        let lease = ExecutionEnvironmentLease::capture(
            "lease-empty-proxy",
            "epoch-1",
            &policy,
            vec![
                ("HTTPS_PROXY".into(), String::new()),
                ("PATH".into(), "/usr/bin:/bin".into()),
            ],
        )
        .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        assert!(
            materialized
                .iter()
                .any(|(name, value)| name == OsStr::new("HTTPS_PROXY") && value.is_empty())
        );
        assert!(
            ExecutionEnvironmentLease::capture(
                "lease-empty-path",
                "epoch-1",
                &policy,
                vec![
                    ("HTTPS_PROXY".into(), String::new()),
                    ("PATH".into(), String::new()),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn adapter_private_secrets_are_redacted_without_entering_the_environment() {
        let lease = fixture_lease("http://proxy.invalid:8080")
            .with_secret_redaction_values(vec![
                "session-access-token-v1".into(),
                "session-refresh-token-v1".into(),
            ])
            .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Reviewer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();
        assert!(
            materialized
                .iter()
                .filter_map(|(_, value)| value.to_str())
                .all(|value| !value.contains("session-access-token-v1"))
        );
        let redacted = lease
            .redactor()
            .redact("access=session-access-token-v1 refresh=session-refresh-token-v1");
        assert!(!redacted.contains("session-access-token-v1"));
        assert!(!redacted.contains("session-refresh-token-v1"));
        assert!(redacted.contains("[REDACTED]"));

        assert!(
            fixture_lease("http://proxy.invalid:8080")
                .with_secret_redaction_values(vec!["short".into()])
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn complete_parent_environment_preserves_non_utf8_name_and_value_bytes() {
        use std::os::unix::ffi::OsStringExt;

        let raw_name = OsString::from_vec(b"RAW_\xff_NAME".to_vec());
        let raw_value = OsString::from_vec(b"value-\xfe".to_vec());
        let parent = ParentEnvironment::from_os(vec![
            (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
            (raw_name.clone(), raw_value.clone()),
        ]);
        let lease = ExecutionEnvironmentLease::capture_complete(
            "lease-non-utf8",
            "epoch-1",
            &EnvironmentPolicy::baseline(),
            &parent,
            Vec::new(),
        )
        .unwrap();
        let materialized = lease
            .materialize(
                "epoch-1",
                &WorkerEnvironmentIdentity {
                    role: WorkerRole::Developer,
                    run_id: "run-1".into(),
                    task_id: "task-1".into(),
                },
            )
            .unwrap();

        assert_eq!(
            materialized.get(raw_name.as_os_str()),
            Some(raw_value.as_os_str())
        );
        assert!(
            lease
                .descriptor()
                .inherited_names
                .contains(&environment_name_descriptor(raw_name.as_os_str()))
        );
    }
}
