#![cfg(target_os = "linux")]

use hcom::worker::claude_exec_runtime::{ClaudeExecRuntimeConfig, ClaudeExecTaskWorkerRuntime};
use hcom::worker::environment::{
    CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS, CLAUDE_DISABLE_BACKGROUND_TASKS, CLAUDE_PROXY_VALUE,
};
use hcom::worker::guardian::{CleanupRegistryInterlock, GuardianCleanupRegistry};
use hcom::worker::{
    DeveloperOutcomeStatus, EnvironmentPolicy, ExecutionEnvironmentLease, OutcomeContract,
    ParentEnvironment, ReviewerVerdict, RoleSessionSpec, RuntimeApprovalPolicy,
    RuntimeClaudePermissions, RuntimeFailureClass, RuntimeOutcome, RuntimeProfile, RuntimeProvider,
    RuntimeSandbox, RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnPurpose,
    RuntimeTurnSpec, TaskWorkerRuntime,
};
use std::collections::BTreeMap;
use std::ffi::{OsString, OsString as StdOsString};
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn hcom_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hcom")
}

fn profile() -> RuntimeProfile {
    RuntimeProfile {
        provider: RuntimeProvider::ClaudeExec,
        model: "haiku".into(),
        reasoning_effort: "medium".into(),
        sandbox: RuntimeSandbox::DangerFullAccess,
        approval_policy: RuntimeApprovalPolicy::Never,
        claude_permissions: Some(RuntimeClaudePermissions {
            dangerously_skip_permissions: true,
        }),
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    project: PathBuf,
    repository: PathBuf,
    artifacts: PathBuf,
    capture: PathBuf,
    registry: GuardianCleanupRegistry,
    runtime: ClaudeExecTaskWorkerRuntime,
}

impl Fixture {
    fn new(body: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let project = root.join("project");
        let repository = root.join("repository");
        let artifacts = root.join("artifacts");
        let capture = root.join("capture");
        for directory in [&project, &repository, &artifacts, &capture] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let claude = root.join("claude");
        fs::write(
            &claude,
            format!(
                r#"#!/bin/sh
set -eu
SESSION=""
MODE=""
NAME=""
ADD_DIR=""
PREVIOUS=""
for ARGUMENT in "$@"; do
  case "$PREVIOUS" in
    --session-id) SESSION="$ARGUMENT"; MODE="create" ;;
    --resume) SESSION="$ARGUMENT"; MODE="resume" ;;
    --name) NAME="$ARGUMENT" ;;
    --add-dir) ADD_DIR="$ARGUMENT" ;;
  esac
  PREVIOUS="$ARGUMENT"
done
[ -n "$SESSION" ]
[ "$http_proxy" = "{proxy}" ]
[ "$https_proxy" = "{proxy}" ]
[ "$HTTP_PROXY" = "{proxy}" ]
[ "$HTTPS_PROXY" = "{proxy}" ]
[ "$CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD" = "1" ]
[ "$CLAUDE_CODE_DISABLE_BACKGROUND_TASKS" = "1" ]
COUNT_FILE="$CAPTURE/count"
COUNT=0
if [ -f "$COUNT_FILE" ]; then COUNT=$(cat "$COUNT_FILE"); fi
COUNT=$((COUNT + 1))
printf '%s' "$COUNT" > "$COUNT_FILE"
{{ printf '===INVOCATION===\n'; printf '%s\n' "$@"; }} >> "$CAPTURE/args.log"
printf '%s\t%s\t%s\t%s\n' "$COUNT" "$MODE" "$SESSION" "$NAME" >> "$CAPTURE/sessions.log"
printf '%s\t%s\n' "$COUNT" "$ADD_DIR" >> "$CAPTURE/add-dir.log"
pwd >> "$CAPTURE/cwd.log"
cat > "$CAPTURE/prompt-$COUNT"
{body}
"#,
                proxy = CLAUDE_PROXY_VALUE,
            ),
        )
        .unwrap();
        fs::set_permissions(&claude, fs::Permissions::from_mode(0o700)).unwrap();

        let mut values = BTreeMap::from([
            (
                "PATH".to_string(),
                format!(
                    "{}:{}",
                    root.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            ),
            ("CAPTURE".to_string(), capture.display().to_string()),
            (
                "FAKE_SECRET_TOKEN".to_string(),
                "fake-secret-value-123456".into(),
            ),
        ]);
        for name in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"] {
            values.insert(name.into(), CLAUDE_PROXY_VALUE.into());
        }
        let parent = ParentEnvironment::from_unicode(values.clone());
        let materialized = parent.materialize_claude().unwrap();
        let environment = materialized
            .iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        let lease = ExecutionEnvironmentLease::capture(
            "claude-exec-test-lease",
            "claude-exec-test-epoch",
            &EnvironmentPolicy::new(Vec::new(), Vec::new()).unwrap(),
            values.into_iter().collect(),
        )
        .unwrap();
        let registry = GuardianCleanupRegistry::default();
        let runtime = ClaudeExecTaskWorkerRuntime::open(ClaudeExecRuntimeConfig {
            claude: "claude".into(),
            guardian_executable: PathBuf::from(hcom_binary()),
            environment,
            lease,
            artifact_root_path: artifacts.clone(),
            run_id: "run-claude-exec".into(),
            task_id: "task-claude-exec".into(),
            reviewer_id: None,
            cleanup_registry: registry.clone(),
        })
        .unwrap();
        Self {
            _temp: temp,
            root,
            project,
            repository,
            artifacts,
            capture,
            registry,
            runtime,
        }
    }

    fn open_session(&mut self, role: hcom::control_api::WorkerRole) -> RuntimeSessionKey {
        self.runtime
            .open_session(RoleSessionSpec {
                role,
                task_key: "task-claude-exec".into(),
                cwd: self.project.clone(),
                task_repository: self.repository.clone(),
                profile: profile(),
                developer_instructions: "fixed role instructions".into(),
            })
            .unwrap()
    }

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        role: hcom::control_api::WorkerRole,
        purpose: RuntimeTurnPurpose,
        timeout: Duration,
    ) -> RuntimeTurnKey {
        self.runtime
            .start_turn(
                session,
                RuntimeTurnSpec {
                    role,
                    task_key: "task-claude-exec".into(),
                    purpose,
                    cwd: self.project.clone(),
                    task_repository: self.repository.clone(),
                    prompt: format!("pointer-only {} prompt", purpose.as_str()),
                    profile: profile(),
                    outcome_contract: match role {
                        hcom::control_api::WorkerRole::Developer => OutcomeContract::DeveloperV1,
                        hcom::control_api::WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
                    },
                    timeout,
                },
            )
            .unwrap()
    }

    fn poll_terminal(&mut self, turn: RuntimeTurnKey) -> RuntimeTurnPoll {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let poll = self.runtime.poll_turn(turn).unwrap();
            if poll.is_terminal() {
                return poll;
            }
            assert!(Instant::now() < deadline, "Claude fake turn did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn emit_init_and_result(result_json_string: &str) -> String {
    format!(
        r#"printf '{{"type":"system","subtype":"init","session_id":"%s"}}\n' "$SESSION"
printf '{{"type":"assistant","message":{{"content":[]}}}}\n'
printf '{{"type":"result","subtype":"success","is_error":false,"session_id":"%s","result":"{result_json_string}"}}\n' "$SESSION"
"#,
    )
}

#[test]
fn native_claude_developer_create_and_correction_exact_resume() {
    let body = r#"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$SESSION"
if [ "$COUNT" = "1" ]; then
  printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","result":"STATUS: READY\\n原样 fake-secret-value-123456"}\n' "$SESSION"
else
  printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","result":"STATUS: READY\\ncorrected"}\n' "$SESSION"
fi
"#;
    let mut fixture = Fixture::new(body);
    let session = fixture.open_session(hcom::control_api::WorkerRole::Developer);
    let initial = fixture.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::InitialDevelopment,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Completed {
        outcome,
        final_message_path,
        ..
    } = fixture.poll_terminal(initial)
    else {
        panic!("initial Claude Developer turn did not complete");
    };
    let RuntimeOutcome::Developer(outcome) = outcome else {
        panic!("wrong outcome role");
    };
    assert_eq!(outcome.status, DeveloperOutcomeStatus::Ready);
    assert_eq!(
        fs::read_to_string(&final_message_path).unwrap(),
        "STATUS: READY\n原样 fake-secret-value-123456"
    );
    assert!(
        !fs::read_to_string(&final_message_path)
            .unwrap()
            .contains("[REDACTED]")
    );

    let correction = fixture.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::DeveloperCorrection,
        Duration::from_secs(10),
    );
    assert!(matches!(
        fixture.poll_terminal(correction),
        RuntimeTurnPoll::Completed { .. }
    ));

    let sessions = fs::read_to_string(fixture.capture.join("sessions.log")).unwrap();
    let records: Vec<Vec<&str>> = sessions
        .lines()
        .map(|line| line.split('\t').collect())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0][1], "create");
    assert_eq!(records[1][1], "resume");
    assert_eq!(records[0][2], records[1][2]);
    assert_eq!(records[0][3], "hcom-task-developer");
    assert_eq!(records[1][3], "hcom-task-developer");
    let args = fs::read_to_string(fixture.capture.join("args.log")).unwrap();
    for required in [
        "-p",
        "--output-format\nstream-json",
        "--verbose",
        "--model\nhaiku",
        "--effort\nmedium",
        "--prompt-suggestions\nfalse",
        "--dangerously-skip-permissions",
        "--add-dir",
    ] {
        assert!(args.contains(required), "missing {required:?} in {args}");
    }
    let add_dirs = fs::read_to_string(fixture.capture.join("add-dir.log")).unwrap();
    assert!(
        add_dirs
            .lines()
            .all(|line| line.ends_with(fixture.repository.to_str().unwrap()))
    );
    let cwds = fs::read_to_string(fixture.capture.join("cwd.log")).unwrap();
    assert!(
        cwds.lines()
            .all(|cwd| cwd == fixture.project.to_str().unwrap())
    );
    assert!(
        fs::read_to_string(fixture.capture.join("prompt-1"))
            .unwrap()
            .contains("fixed role instructions")
    );
    assert!(
        !fs::read_to_string(fixture.capture.join("prompt-2"))
            .unwrap()
            .contains("fixed role instructions")
    );
    fixture.runtime.shutdown().unwrap();
    assert_eq!(
        fixture.registry.interlock(),
        CleanupRegistryInterlock::Ready
    );
}

#[test]
fn native_claude_reviewer_clarification_and_rereview_exact_resume() {
    let body = r#"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$SESSION"
case "$COUNT" in
  1) RESULT='ambiguous review body' ;;
  2) RESULT='VERDICT: LGTM\nafter clarification' ;;
  *) RESULT='VERDICT: REQUEST_CHANGES\nnew finding' ;;
esac
printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","result":"%s"}\n' "$SESSION" "$RESULT"
"#;
    let mut fixture = Fixture::new(body);
    let session = fixture.open_session(hcom::control_api::WorkerRole::Reviewer);
    let initial = fixture.start_turn(
        session,
        hcom::control_api::WorkerRole::Reviewer,
        RuntimeTurnPurpose::InitialReview,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Completed {
        outcome,
        final_message_path,
        ..
    } = fixture.poll_terminal(initial)
    else {
        panic!("review clarification did not complete");
    };
    let RuntimeOutcome::Reviewer(outcome) = outcome else {
        panic!("wrong outcome role");
    };
    assert_eq!(outcome.verdict, ReviewerVerdict::Lgtm);
    assert_eq!(outcome.preceding_final_message_paths.len(), 1);
    assert_eq!(
        fs::read_to_string(&outcome.preceding_final_message_paths[0]).unwrap(),
        "ambiguous review body"
    );
    assert_eq!(
        fs::read_to_string(&final_message_path).unwrap(),
        "VERDICT: LGTM\nafter clarification"
    );
    let clarification_prompt = fs::read_to_string(fixture.capture.join("prompt-2")).unwrap();
    assert!(clarification_prompt.contains("did not contain a usable verdict"));
    assert!(
        clarification_prompt.contains(outcome.preceding_final_message_paths[0].to_str().unwrap())
    );
    assert!(!clarification_prompt.contains("ambiguous review body"));

    let rereview = fixture.start_turn(
        session,
        hcom::control_api::WorkerRole::Reviewer,
        RuntimeTurnPurpose::ReviewerRereview,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Completed { outcome, .. } = fixture.poll_terminal(rereview) else {
        panic!("rereview did not complete");
    };
    let RuntimeOutcome::Reviewer(outcome) = outcome else {
        panic!("wrong outcome role");
    };
    assert_eq!(outcome.verdict, ReviewerVerdict::RequestChanges);
    let sessions = fs::read_to_string(fixture.capture.join("sessions.log")).unwrap();
    let records: Vec<Vec<&str>> = sessions
        .lines()
        .map(|line| line.split('\t').collect())
        .collect();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0][1], "create");
    assert!(records[1..].iter().all(|record| record[1] == "resume"));
    assert!(records.iter().all(|record| record[2] == records[0][2]));
    assert!(
        records
            .iter()
            .all(|record| record[3] == "hcom-task-reviewer")
    );
}

#[test]
fn large_final_is_preserved_and_nonzero_or_residual_processes_never_route() {
    let large_body = r#"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$SESSION"
printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","result":"STATUS: READY\\n' "$SESSION"
head -c 3145728 /dev/zero | tr '\0' 'x'
printf 'TAIL"}\n'
"#;
    let mut large = Fixture::new(large_body);
    let session = large.open_session(hcom::control_api::WorkerRole::Developer);
    let turn = large.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::InitialDevelopment,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Completed {
        final_message_path, ..
    } = large.poll_terminal(turn)
    else {
        panic!("large Claude final did not complete");
    };
    let final_message = fs::read_to_string(final_message_path).unwrap();
    assert!(final_message.len() > 3 * 1024 * 1024);
    assert!(final_message.ends_with("TAIL"));

    let nonzero_body = format!(
        "{}\nprintf 'fake failure\\n' >&2\nexit 7",
        emit_init_and_result("STATUS: READY\\nplausible")
    );
    let mut nonzero = Fixture::new(&nonzero_body);
    let session = nonzero.open_session(hcom::control_api::WorkerRole::Developer);
    let turn = nonzero.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::InitialDevelopment,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Failed { failure, .. } = nonzero.poll_terminal(turn) else {
        panic!("nonzero Claude fake incorrectly routed a final");
    };
    assert_eq!(failure.class, RuntimeFailureClass::Process);
    assert!(failure.detail.contains("status 7"), "{}", failure.detail);
    assert!(!nonzero
        .artifacts
        .join("run-claude-exec/task-claude-exec/developer/session-1/turn-1/attempt-1/native-final.partial")
        .exists());

    let residual_body = format!(
        "{}\n/usr/bin/setsid /bin/sh -c 'sleep 30' </dev/null >/dev/null 2>&1 &",
        emit_init_and_result("STATUS: READY\\nplausible")
    );
    let mut residual = Fixture::new(&residual_body);
    let session = residual.open_session(hcom::control_api::WorkerRole::Developer);
    let turn = residual.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::InitialDevelopment,
        Duration::from_secs(10),
    );
    let RuntimeTurnPoll::Failed { failure, .. } = residual.poll_terminal(turn) else {
        panic!("residual Claude descendant incorrectly routed a final");
    };
    assert_eq!(failure.class, RuntimeFailureClass::Process);
    assert!(
        failure.detail.contains("residual descendants"),
        "{}",
        failure.detail
    );
    assert_eq!(
        residual.registry.interlock(),
        CleanupRegistryInterlock::Ready
    );
}

#[test]
fn timeout_uses_guardian_cleanup_and_invalid_proxy_never_spawns_fake_claude() {
    let timeout_body = r#"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$SESSION"
sleep 30
"#;
    let mut timeout = Fixture::new(timeout_body);
    let session = timeout.open_session(hcom::control_api::WorkerRole::Developer);
    let turn = timeout.start_turn(
        session,
        hcom::control_api::WorkerRole::Developer,
        RuntimeTurnPurpose::InitialDevelopment,
        Duration::from_millis(150),
    );
    std::thread::sleep(Duration::from_millis(200));
    let RuntimeTurnPoll::Failed { failure, .. } = timeout.poll_terminal(turn) else {
        panic!("timed out Claude fake did not fail");
    };
    assert_eq!(failure.class, RuntimeFailureClass::Timeout);
    assert_eq!(
        timeout.registry.interlock(),
        CleanupRegistryInterlock::Ready
    );

    let marker = timeout.root.join("must-not-spawn");
    let invalid_environment = vec![
        (OsString::from("PATH"), timeout.root.as_os_str().to_owned()),
        (
            OsString::from("http_proxy"),
            OsString::from("http://127.0.0.1:1"),
        ),
        (
            OsString::from("https_proxy"),
            OsString::from(CLAUDE_PROXY_VALUE),
        ),
        (
            OsString::from("HTTP_PROXY"),
            OsString::from(CLAUDE_PROXY_VALUE),
        ),
        (
            OsString::from("HTTPS_PROXY"),
            OsString::from(CLAUDE_PROXY_VALUE),
        ),
        (
            OsString::from(CLAUDE_ADDITIONAL_DIRECTORIES_INSTRUCTIONS),
            OsString::from("1"),
        ),
        (
            OsString::from(CLAUDE_DISABLE_BACKGROUND_TASKS),
            OsString::from("1"),
        ),
    ];
    let lease = ExecutionEnvironmentLease::capture(
        "invalid-claude-lease",
        "invalid-claude-epoch",
        &EnvironmentPolicy::new(Vec::new(), Vec::new()).unwrap(),
        vec![("PATH".into(), "/usr/bin:/bin".into())],
    )
    .unwrap();
    let result = ClaudeExecTaskWorkerRuntime::open(ClaudeExecRuntimeConfig {
        claude: StdOsString::from_vec(
            format!("/bin/sh -c 'touch {}'", marker.display()).into_bytes(),
        ),
        guardian_executable: PathBuf::from(hcom_binary()),
        environment: invalid_environment,
        lease,
        artifact_root_path: timeout.artifacts.clone(),
        run_id: "run-invalid-proxy".into(),
        task_id: "task-invalid-proxy".into(),
        reviewer_id: None,
        cleanup_registry: GuardianCleanupRegistry::default(),
    });
    assert!(result.is_err());
    assert!(!marker.exists());
}
