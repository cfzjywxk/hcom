use crate::control_api::{ActionName, ControlAction};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};

pub(crate) fn tool_definitions() -> Vec<Value> {
    ActionName::ALL
        .into_iter()
        .map(|action| {
            json!({
                "name": action.as_str(),
                "description": tool_description(action),
                "inputSchema": action_schema(action),
            })
        })
        .collect()
}

pub(crate) fn control_action(name: &str, arguments: Value) -> Result<ControlAction> {
    let action = ActionName::ALL
        .into_iter()
        .find(|action| action.as_str() == name)
        .ok_or_else(|| anyhow::anyhow!("unknown architect tool"))?;
    let mut object = match arguments {
        Value::Object(object) => object,
        _ => bail!("architect tool arguments must be an object"),
    };
    object.insert("action".into(), Value::String(action.as_str().into()));
    serde_json::from_value(Value::Object(object))
        .context("architect tool arguments do not match the typed control protocol")
}

fn tool_description(action: ActionName) -> &'static str {
    match action {
        ActionName::ProjectCreate => "Create a draft durable project for this bound repository.",
        ActionName::ProjectGet => "Read the immutable and current state of the bound project.",
        ActionName::ProjectPlanReplace => {
            "Replace the draft typed task plan at an exact project version."
        }
        ActionName::ProjectApprove => "Approve an exact immutable plan version and hash.",
        ActionName::ProjectRun => "Request execution of an exact approved plan.",
        ActionName::ProjectWait => "Wait for a bounded durable project state change.",
        ActionName::ProjectStatus => "Read a sanitized durable project status snapshot.",
        ActionName::ProjectLogs => "Read bounded sanitized worker activity.",
        ActionName::ProjectPause => "Pause a project at an exact version.",
        ActionName::ProjectResume => "Resume a paused project at an exact version.",
        ActionName::ProjectCancel => "Cancel a project at an exact version.",
        ActionName::ProjectAnswer => "Answer one exact needs-input task question.",
        ActionName::ProjectAbandonForReplan => {
            "Abandon one exact task attempt after its archive manifest is fixed."
        }
    }
}

fn action_schema(action: ActionName) -> Value {
    match action {
        ActionName::ProjectCreate => object_schema(
            &["repo_root", "target_ref"],
            [
                ("repo_root", path_schema()),
                ("target_ref", string_schema(1, 1024)),
            ],
        ),
        ActionName::ProjectGet | ActionName::ProjectStatus => {
            object_schema(&["project_id"], [("project_id", id_schema())])
        }
        ActionName::ProjectPlanReplace => object_schema(
            &[
                "project_id",
                "expected_project_version",
                "base_checkpoint_sha",
                "developer_profile",
                "reviewer_profile",
                "tasks",
                "automatic_through_ordinal",
            ],
            [
                ("project_id", id_schema()),
                ("expected_project_version", uint_schema()),
                ("base_checkpoint_sha", git_oid_schema()),
                ("developer_profile", profile_schema()),
                ("reviewer_profile", profile_schema()),
                (
                    "tasks",
                    json!({"type":"array","minItems":1,"maxItems":256,"items":task_schema()}),
                ),
                (
                    "automatic_through_ordinal",
                    nullable(json!({"type":"integer","minimum":0,"maximum":255})),
                ),
            ],
        ),
        ActionName::ProjectApprove | ActionName::ProjectRun => object_schema(
            &[
                "project_id",
                "expected_project_version",
                "plan_version",
                "plan_hash",
            ],
            [
                ("project_id", id_schema()),
                ("expected_project_version", uint_schema()),
                ("plan_version", positive_uint_schema()),
                ("plan_hash", sha256_schema()),
            ],
        ),
        ActionName::ProjectWait => object_schema(
            &["project_id", "after_project_version", "max_wait_ms"],
            [
                ("project_id", id_schema()),
                ("after_project_version", uint_schema()),
                (
                    "max_wait_ms",
                    json!({"type":"integer","minimum":1,"maximum":300000}),
                ),
            ],
        ),
        ActionName::ProjectLogs => object_schema(
            &[
                "project_id",
                "task_id",
                "role",
                "turn_sequence",
                "after_activity_sequence",
                "limit",
                "follow",
            ],
            [
                ("project_id", id_schema()),
                ("task_id", nullable(id_schema())),
                (
                    "role",
                    nullable(json!({"type":"string","enum":["developer","reviewer"]})),
                ),
                (
                    "turn_sequence",
                    nullable(json!({"type":"integer","minimum":1,"maximum":4294967295u64})),
                ),
                ("after_activity_sequence", nullable(uint_schema())),
                (
                    "limit",
                    json!({"type":"integer","minimum":1,"maximum":1000}),
                ),
                ("follow", json!({"type":"boolean"})),
            ],
        ),
        ActionName::ProjectPause | ActionName::ProjectCancel => object_schema(
            &["project_id", "expected_project_version", "reason"],
            [
                ("project_id", id_schema()),
                ("expected_project_version", uint_schema()),
                ("reason", string_schema(1, 4096)),
            ],
        ),
        ActionName::ProjectResume => object_schema(
            &["project_id", "expected_project_version"],
            [
                ("project_id", id_schema()),
                ("expected_project_version", uint_schema()),
            ],
        ),
        ActionName::ProjectAnswer => object_schema(
            &[
                "project_id",
                "task_id",
                "expected_project_version",
                "expected_task_version",
                "answer",
            ],
            [
                ("project_id", id_schema()),
                ("task_id", id_schema()),
                ("expected_project_version", uint_schema()),
                ("expected_task_version", uint_schema()),
                ("answer", string_schema(1, 65_536)),
            ],
        ),
        ActionName::ProjectAbandonForReplan => object_schema(
            &[
                "project_id",
                "task_id",
                "expected_project_version",
                "expected_task_version",
                "archive_manifest_hash",
            ],
            [
                ("project_id", id_schema()),
                ("task_id", id_schema()),
                ("expected_project_version", uint_schema()),
                ("expected_task_version", uint_schema()),
                ("archive_manifest_hash", sha256_schema()),
            ],
        ),
    }
}

fn profile_schema() -> Value {
    object_schema(
        &[
            "adapter",
            "model",
            "reasoning",
            "policy",
            "cli_path",
            "cli_version",
            "adapter_contract_version",
            "native_session_mode",
            "capability",
        ],
        [
            ("adapter", string_schema(1, 64)),
            ("model", string_schema(1, 256)),
            ("reasoning", string_schema(1, 64)),
            ("policy", string_schema(1, 2048)),
            ("cli_path", path_schema()),
            ("cli_version", string_schema(1, 128)),
            ("adapter_contract_version", positive_uint_schema()),
            (
                "native_session_mode",
                json!({"type":"string","enum":["preassigned","discovered"]}),
            ),
            (
                "capability",
                object_schema(
                    &["contract_hash", "features"],
                    [
                        ("contract_hash", sha256_schema()),
                        (
                            "features",
                            json!({
                                "type":"array",
                                "maxItems":256,
                                "uniqueItems":true,
                                "items":string_schema(1, 128)
                            }),
                        ),
                    ],
                ),
            ),
        ],
    )
}

fn task_schema() -> Value {
    object_schema(
        &[
            "task_key",
            "title",
            "objective",
            "dependencies",
            "acceptance_criteria",
            "required_checks",
            "allowed_paths",
            "forbidden_actions",
            "max_review_rounds",
            "context_refs",
        ],
        [
            ("task_key", id_schema()),
            ("title", string_schema(1, 512)),
            ("objective", string_schema(1, 65_536)),
            ("dependencies", unique_array(id_schema())),
            (
                "acceptance_criteria",
                unique_array(string_schema(1, 65_536)),
            ),
            ("required_checks", unique_array(string_schema(1, 4096))),
            ("allowed_paths", unique_array(string_schema(1, 4096))),
            ("forbidden_actions", unique_array(string_schema(1, 4096))),
            (
                "max_review_rounds",
                json!({"type":"integer","minimum":1,"maximum":20}),
            ),
            (
                "context_refs",
                json!({
                    "type":"array",
                    "maxItems":256,
                    "items":object_schema(
                        &["kind","task_id","digest"],
                        [
                            ("kind", json!({"type":"string","enum":["task_result"]})),
                            ("task_id", id_schema()),
                            ("digest", sha256_schema()),
                        ],
                    )
                }),
            ),
        ],
    )
}

fn object_schema<const N: usize>(required: &[&str], properties: [(&str, Value); N]) -> Value {
    let properties: Map<String, Value> = properties
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), schema))
        .collect();
    json!({
        "type":"object",
        "additionalProperties":false,
        "required":required,
        "properties":properties,
    })
}

fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}

fn unique_array(item: Value) -> Value {
    json!({"type":"array","maxItems":256,"uniqueItems":true,"items":item})
}

fn id_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":128,
        "pattern":"^[A-Za-z0-9_.:-]+$"
    })
}

fn path_schema() -> Value {
    json!({"type":"string","minLength":1,"maxLength":4096,"pattern":"^/"})
}

fn sha256_schema() -> Value {
    json!({"type":"string","pattern":"^[0-9a-f]{64}$"})
}

fn git_oid_schema() -> Value {
    json!({"type":"string","pattern":"^([0-9a-f]{40}|[0-9a-f]{64})$"})
}

fn string_schema(minimum: usize, maximum: usize) -> Value {
    json!({"type":"string","minLength":minimum,"maxLength":maximum})
}

fn uint_schema() -> Value {
    json!({"type":"integer","minimum":0,"maximum":u64::MAX})
}

fn positive_uint_schema() -> Value {
    json!({"type":"integer","minimum":1,"maximum":u64::MAX})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn tool_inventory_is_exact_and_contains_no_generic_authority() {
        let tools = tool_definitions();
        let names: BTreeSet<_> = tools
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        let expected: BTreeSet<_> = ActionName::ALL
            .iter()
            .map(|action| action.as_str())
            .collect();
        assert_eq!(names, expected);
        for forbidden in [
            "shell",
            "exec",
            "filesystem",
            "project_apply",
            "service_stop",
            "install",
        ] {
            assert!(!names.contains(forbidden));
        }
        for tool in tools {
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert!(tool["inputSchema"]["required"].is_array());
        }
    }

    #[test]
    fn call_translation_is_strict_and_protocol_typed() {
        let action = control_action(
            "project_create",
            json!({"repo_root":"/repo","target_ref":"refs/heads/master"}),
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::ProjectCreate { repo_root, target_ref }
                if repo_root == "/repo" && target_ref == "refs/heads/master"
        ));
        assert!(
            control_action(
                "project_create",
                json!({"repo_root":"/repo","target_ref":"refs/heads/master","shell":"id"})
            )
            .is_err()
        );
        assert!(control_action("shell", json!({})).is_err());
    }
}
