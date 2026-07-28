use super::contract::{
    AdapterCapabilities, AdapterDescriptor, CommandSpec, ExecutableIdentity, NativeArtifacts,
    NativeObservation, NativeOutputKind, NativeResult, OutputDeclaration, ResultTransport,
    SchemaTransport, TurnControl, WorkerAdapter, WorkerProfile, validate_native_session_id,
};
use super::result::{DeveloperResult, ReviewerResult};
use super::validation::validate_text;
use crate::control_api::{CapabilitySnapshot, NativeSessionMode, WorkerRole};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::PathBuf;

pub struct FakeWorkerAdapter {
    descriptor: AdapterDescriptor,
    executable: ExecutableIdentity,
    workspace_cwd: PathBuf,
}

impl FakeWorkerAdapter {
    pub fn preassigned(executable: ExecutableIdentity, workspace_cwd: PathBuf) -> Result<Self> {
        Self::new(
            "fake-envelope",
            executable,
            workspace_cwd,
            NativeSessionMode::Preassigned,
            ResultTransport::Envelope,
        )
    }

    pub fn discovered(executable: ExecutableIdentity, workspace_cwd: PathBuf) -> Result<Self> {
        Self::new(
            "fake-final-file",
            executable,
            workspace_cwd,
            NativeSessionMode::Discovered,
            ResultTransport::FinalFile,
        )
    }

    fn new(
        name: &str,
        executable: ExecutableIdentity,
        workspace_cwd: PathBuf,
        native_session_mode: NativeSessionMode,
        result_transport: ResultTransport,
    ) -> Result<Self> {
        let descriptor = AdapterDescriptor::new(
            name,
            1,
            "fake-cli-1",
            "fake-model",
            "deterministic",
            "isolated-fake",
            AdapterCapabilities {
                roles: vec![WorkerRole::Developer, WorkerRole::Reviewer],
                native_session_mode,
                result_transport,
                features: vec!["structured-result".into(), "exact-resume".into()],
            },
        )?;
        Ok(Self {
            descriptor,
            executable,
            workspace_cwd,
        })
    }

    pub fn profile(&self, role: WorkerRole) -> WorkerProfile {
        WorkerProfile {
            role,
            adapter: self.descriptor.name.clone(),
            model: self.descriptor.model.clone(),
            reasoning: self.descriptor.reasoning.clone(),
            policy: self.descriptor.policy.clone(),
            executable: self.executable.clone(),
            cli_version: self.descriptor.cli_version.clone(),
            adapter_contract_version: self.descriptor.contract_version,
            native_session_mode: self.descriptor.capabilities.native_session_mode,
            capability: CapabilitySnapshot {
                contract_hash: self.descriptor.capability_contract_hash.clone(),
                features: self.descriptor.capabilities.features.clone(),
            },
        }
    }

    fn command(&self, mode: &str, native_session_id: Option<&str>) -> CommandSpec {
        let mut fixed_argv = vec!["--fake-worker".into(), mode.into(), "--structured".into()];
        if let Some(native_session_id) = native_session_id {
            fixed_argv.extend([
                if mode == "create" {
                    "--session-id"
                } else {
                    "--resume"
                }
                .into(),
                native_session_id.into(),
            ]);
        }
        let (schema_transport, output) = match self.descriptor.capabilities.result_transport {
            ResultTransport::Envelope => (
                SchemaTransport::InlineArgument {
                    flag: "--result-schema".into(),
                    json: r#"{"type":"object","required":["session_id","role","result"]}"#.into(),
                },
                OutputDeclaration {
                    kind: NativeOutputKind::StdoutEnvelope,
                    relative_path: "native.stdout.partial".into(),
                    max_bytes: 256 * 1024,
                },
            ),
            ResultTransport::FinalFile => (
                SchemaTransport::File {
                    argument: "--result-schema-file".into(),
                    relative_path: "result-schema.json".into(),
                    contents: br#"{"type":"object","required":["session_id","role","result"]}"#
                        .to_vec(),
                },
                OutputDeclaration {
                    kind: NativeOutputKind::FinalFile,
                    relative_path: "native-final.partial".into(),
                    max_bytes: 256 * 1024,
                },
            ),
        };
        CommandSpec {
            executable: self.executable.clone(),
            fixed_argv,
            schema_transport,
            expected_outputs: vec![output],
            workspace_cwd: self.workspace_cwd.clone(),
        }
    }
}

impl WorkerAdapter for FakeWorkerAdapter {
    fn descriptor(&self) -> &AdapterDescriptor {
        &self.descriptor
    }

    fn executable_contract(&self) -> &ExecutableIdentity {
        &self.executable
    }

    fn build_create(&self, control: &TurnControl) -> Result<CommandSpec> {
        control.validate()?;
        Ok(self.command("create", control.native_session_id.as_deref()))
    }

    fn build_resume(&self, native_session_id: &str, control: &TurnControl) -> Result<CommandSpec> {
        control.validate()?;
        validate_native_session_id(native_session_id)?;
        Ok(self.command("resume", Some(native_session_id)))
    }

    fn observe_native_record(&self, record: &[u8]) -> Result<Vec<NativeObservation>> {
        if record.is_empty() || record.len() > 64 * 1024 {
            bail!("fake native record exceeds its bound");
        }
        let record: FakeRecord =
            serde_json::from_slice(record).context("fake native record is malformed")?;
        Ok(match record {
            FakeRecord::SessionStarted { session_id } => {
                validate_native_session_id(&session_id)?;
                vec![NativeObservation::SessionStarted {
                    native_session_id: session_id,
                }]
            }
            FakeRecord::Activity { activity, message } => {
                validate_text("fake activity kind", &activity, 128, false)?;
                validate_text("fake activity message", &message, 64 * 1024, true)?;
                vec![NativeObservation::Activity {
                    kind: activity,
                    message,
                }]
            }
        })
    }

    fn extract_result(&self, artifacts: &NativeArtifacts) -> Result<NativeResult> {
        let bytes = match self.descriptor.capabilities.result_transport {
            ResultTransport::Envelope => artifacts.stdout(),
            ResultTransport::FinalFile => artifacts
                .final_output()
                .ok_or_else(|| anyhow::anyhow!("fake final-file result is missing"))?,
        };
        match artifacts.role() {
            WorkerRole::Developer => {
                let envelope: FakeResultEnvelope<DeveloperResult> =
                    serde_json::from_slice(bytes)
                        .context("fake developer result envelope is malformed")?;
                validate_fake_result_envelope(&envelope, WorkerRole::Developer)?;
                envelope.result.validate()?;
                Ok(NativeResult::Developer {
                    native_session_id: envelope.session_id,
                    result: envelope.result,
                })
            }
            WorkerRole::Reviewer => {
                let envelope: FakeResultEnvelope<ReviewerResult> = serde_json::from_slice(bytes)
                    .context("fake reviewer result envelope is malformed")?;
                validate_fake_result_envelope(&envelope, WorkerRole::Reviewer)?;
                envelope.result.validate()?;
                Ok(NativeResult::Reviewer {
                    native_session_id: envelope.session_id,
                    result: envelope.result,
                })
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum FakeRecord {
    SessionStarted { session_id: String },
    Activity { activity: String, message: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FakeResultEnvelope<T> {
    session_id: String,
    role: WorkerRole,
    result: T,
}

fn validate_fake_result_envelope<T>(
    envelope: &FakeResultEnvelope<T>,
    expected_role: WorkerRole,
) -> Result<()> {
    if envelope.role != expected_role {
        bail!("fake result role does not match the turn role");
    }
    validate_native_session_id(&envelope.session_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{prepare_create_turn, prepare_resume_turn};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn executable(temp: &tempfile::TempDir) -> ExecutableIdentity {
        let path = temp.path().join("fake-worker");
        fs::write(&path, b"fake executable").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableIdentity::capture(&path).unwrap()
    }

    #[test]
    fn preassigned_envelope_and_discovered_final_file_extract_typed_results() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        let preassigned = FakeWorkerAdapter::preassigned(executable(&temp), cwd.clone()).unwrap();
        let developer = serde_json::json!({
            "session_id": "native-preassigned",
            "role": "developer",
            "result": {
                "decision": "blocked",
                "summary": "bounded fake failure",
                "head_revision": null,
                "commits": [],
                "checks": [],
                "questions": [],
                "risks": [],
                "changed_paths": []
            }
        });
        let artifacts = NativeArtifacts::new(
            WorkerRole::Developer,
            serde_json::to_vec(&developer).unwrap(),
            vec![],
            None,
        )
        .unwrap();
        let result = preassigned.extract_result(&artifacts).unwrap();
        assert_eq!(result.native_session_id(), "native-preassigned");

        let discovered = FakeWorkerAdapter::discovered(executable(&temp), cwd).unwrap();
        let reviewer = serde_json::json!({
            "session_id": "native-discovered",
            "role": "reviewer",
            "result": {
                "decision": "lgtm",
                "summary": "no blocking finding",
                "findings": [],
                "checks": []
            }
        });
        let artifacts = NativeArtifacts::new(
            WorkerRole::Reviewer,
            vec![],
            vec![],
            Some(serde_json::to_vec(&reviewer).unwrap()),
        )
        .unwrap();
        let result = discovered.extract_result(&artifacts).unwrap();
        assert_eq!(result.native_session_id(), "native-discovered");
    }

    #[test]
    fn fake_result_transport_rejects_wrong_role_missing_file_and_unknown_fields() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        let envelope = FakeWorkerAdapter::preassigned(executable(&temp), cwd.clone()).unwrap();
        let wrong_role = serde_json::json!({
            "session_id": "native-1",
            "role": "reviewer",
            "result": {
                "decision": "lgtm",
                "summary": "bounded",
                "findings": [],
                "checks": []
            }
        });
        let artifacts = NativeArtifacts::new(
            WorkerRole::Developer,
            serde_json::to_vec(&wrong_role).unwrap(),
            vec![],
            None,
        )
        .unwrap();
        assert!(envelope.extract_result(&artifacts).is_err());

        let final_file = FakeWorkerAdapter::discovered(executable(&temp), cwd).unwrap();
        let missing = NativeArtifacts::new(WorkerRole::Reviewer, vec![], vec![], None).unwrap();
        assert!(final_file.extract_result(&missing).is_err());

        let unknown = br#"{"type":"session_started","session_id":"native-1","extra":true}"#;
        assert!(final_file.observe_native_record(unknown).is_err());
        let unsafe_id = br#"{"type":"session_started","session_id":"../../outside"}"#;
        assert!(final_file.observe_native_record(unsafe_id).is_err());
        let unsafe_activity =
            b"{\"type\":\"activity\",\"activity\":\"progress\",\"message\":\"bad\\u001btitle\"}";
        assert!(final_file.observe_native_record(unsafe_activity).is_err());

        let duplicate_result_field = br#"{
            "session_id":"native-1",
            "role":"developer",
            "result":{
                "decision":"blocked",
                "decision":"needs_input",
                "summary":"bounded",
                "head_revision":null,
                "commits":[],
                "checks":[],
                "questions":["question"],
                "risks":[],
                "changed_paths":[]
            }
        }"#;
        let duplicate = NativeArtifacts::new(
            WorkerRole::Developer,
            duplicate_result_field.to_vec(),
            vec![],
            None,
        )
        .unwrap();
        assert!(envelope.extract_result(&duplicate).is_err());
    }

    #[test]
    fn fake_create_and_resume_keep_prompt_private_and_session_exact() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = fs::canonicalize(temp.path()).unwrap();
        for adapter in [
            FakeWorkerAdapter::preassigned(executable(&temp), cwd.clone()).unwrap(),
            FakeWorkerAdapter::discovered(executable(&temp), cwd.clone()).unwrap(),
        ] {
            let role = WorkerRole::Developer;
            let profile = adapter.profile(role);
            let control = TurnControl {
                project_id: "project-1".into(),
                task_id: "task-1".into(),
                role,
                logical_session_id: "logical-1".into(),
                native_session_id: if profile.native_session_mode == NativeSessionMode::Preassigned
                {
                    Some("native-preassigned-1".into())
                } else {
                    None
                },
                turn_sequence: 1,
                attempt: 1,
                task_version: 1,
                review_round: 0,
                base_revision: std::iter::repeat_n('a', 40).collect(),
                head_revision: None,
                artifact_dir: "project-1/task-1/developer/logical-1/turn-1/attempt-1".into(),
            };
            let prompt = b"private fake task sentinel 180c4b55".to_vec();
            let mut invalid_create_control = control.clone();
            invalid_create_control.native_session_id =
                if profile.native_session_mode == NativeSessionMode::Preassigned {
                    None
                } else {
                    Some("native-unexpected".into())
                };
            assert!(
                prepare_create_turn(&adapter, &profile, &invalid_create_control, prompt.clone(),)
                    .is_err()
            );
            let create = prepare_create_turn(&adapter, &profile, &control, prompt.clone()).unwrap();
            assert!(
                !create
                    .command()
                    .materialized_control_argv()
                    .join("\0")
                    .contains("private fake task")
            );
            if profile.native_session_mode == NativeSessionMode::Preassigned {
                let argv = create.command().materialized_control_argv();
                let session_position = argv
                    .iter()
                    .position(|argument| argument == "--session-id")
                    .unwrap();
                assert_eq!(argv[session_position + 1], "native-preassigned-1");
            }

            let mut resume_control = control.clone();
            resume_control.turn_sequence = 2;
            resume_control.native_session_id = Some("native-exact-1".into());
            let resume = prepare_resume_turn(
                &adapter,
                &profile,
                &resume_control,
                "native-exact-1",
                prompt,
            )
            .unwrap();
            let argv = resume.command().materialized_control_argv();
            let resume_position = argv
                .iter()
                .position(|argument| argument == "--resume")
                .unwrap();
            assert_eq!(argv[resume_position + 1], "native-exact-1");
            assert!(
                prepare_resume_turn(
                    &adapter,
                    &profile,
                    &resume_control,
                    "native-other",
                    b"bounded prompt body".to_vec(),
                )
                .is_err()
            );
            assert!(
                prepare_resume_turn(
                    &adapter,
                    &profile,
                    &resume_control,
                    "../../wrong",
                    b"bounded prompt".to_vec(),
                )
                .is_err()
            );
        }
    }
}
