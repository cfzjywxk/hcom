use crate::control_api::{ActionName, ControlAction};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};

pub(crate) fn tool_definitions() -> Vec<Value> {
    ActionName::ARCHITECT
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
    let action = ActionName::ARCHITECT
        .into_iter()
        .find(|action| action.as_str() == name)
        .ok_or_else(|| anyhow::anyhow!("unknown architect tool"))?;
    let mut object = match arguments {
        Value::Object(object) => object,
        _ => bail!("architect tool arguments must be an object"),
    };
    object.insert("action".into(), Value::String(action.as_str().into()));
    let action: ControlAction = serde_json::from_value(Value::Object(object))
        .context("architect tool arguments do not match the typed session protocol")?;
    action
        .validate_for_tool()
        .context("architect tool arguments failed protocol validation")?;
    Ok(action)
}

fn tool_description(action: ActionName) -> &'static str {
    match action {
        ActionName::SessionPlanReplace => {
            "Draft or replace the bounded ordered task plan. This never starts a worker. Present the returned exact plan version and hash to the human before requesting approval."
        }
        ActionName::SessionApproveAndStart => {
            "Start the exact draft only after the human explicitly approved its returned plan version and hash in this architect conversation. Never infer approval."
        }
        ActionName::SessionStatus => {
            "Read the sanitized in-memory status of this foreground architect run."
        }
        ActionName::SessionCancel => {
            "Cancel this foreground run at an exact version only after the human requests cancellation."
        }
    }
}

fn action_schema(action: ActionName) -> Value {
    match action {
        ActionName::SessionPlanReplace => object_schema(
            &[
                "expected_session_version",
                "developer_adapter",
                "reviewer_adapter",
                "tasks",
            ],
            [
                ("expected_session_version", uint_schema()),
                (
                    "developer_adapter",
                    json!({
                        "type":"string",
                        "enum":["codex-developer-0.145.0"]
                    }),
                ),
                (
                    "reviewer_adapter",
                    json!({
                        "type":"string",
                        "enum":[
                            "codex-reviewer-0.145.0",
                            "claude-reviewer-2.1.220"
                        ]
                    }),
                ),
                (
                    "tasks",
                    json!({
                        "type":"array",
                        "minItems":1,
                        "maxItems":64,
                        "items":task_schema()
                    }),
                ),
            ],
        ),
        ActionName::SessionApproveAndStart => object_schema(
            &[
                "expected_session_version",
                "plan_version",
                "plan_hash",
                "approval_confirmed",
            ],
            [
                ("expected_session_version", uint_schema()),
                ("plan_version", positive_uint_schema()),
                ("plan_hash", sha256_schema()),
                ("approval_confirmed", json!({"type":"boolean","const":true})),
            ],
        ),
        ActionName::SessionStatus => object_schema(&[], []),
        ActionName::SessionCancel => object_schema(
            &["expected_session_version", "reason"],
            [
                ("expected_session_version", uint_schema()),
                ("reason", string_schema(1, 4096)),
            ],
        ),
    }
}

fn task_schema() -> Value {
    object_schema(
        &[
            "task_key",
            "title",
            "objective",
            "acceptance_criteria",
            "required_checks",
            "allowed_paths",
            "forbidden_actions",
            "max_review_rounds",
        ],
        [
            ("task_key", id_schema()),
            ("title", string_schema(1, 512)),
            ("objective", string_schema(1, 65_536)),
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

fn sha256_schema() -> Value {
    json!({"type":"string","pattern":"^[0-9a-f]{64}$"})
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
    fn tool_inventory_is_exact_and_contains_no_project_or_generic_authority() {
        let tools = tool_definitions();
        let names: BTreeSet<_> = tools
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        let expected: BTreeSet<_> = ActionName::ARCHITECT
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
        assert!(names.iter().all(|name| !name.contains("project")));
        for tool in tools {
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert!(tool["inputSchema"]["required"].is_array());
        }
    }

    #[test]
    fn call_translation_requires_explicit_start_confirmation() {
        let action = control_action(
            "session_approve_and_start",
            json!({
                "expected_session_version":1,
                "plan_version":1,
                "plan_hash":"a".repeat(64),
                "approval_confirmed":true
            }),
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionApproveAndStart {
                approval_confirmed: true,
                ..
            }
        ));
        assert!(
            control_action(
                "session_approve_and_start",
                json!({
                    "expected_session_version":1,
                    "plan_version":1,
                    "plan_hash":"a".repeat(64),
                    "approval_confirmed":false
                })
            )
            .is_err()
        );
        assert!(control_action("shell", json!({})).is_err());
    }

    #[test]
    fn plan_translation_accepts_real_multiline_objective_and_rejects_hidden_controls() {
        let arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer-0.145.0",
            "reviewer_adapter":"claude-reviewer-2.1.220",
            "tasks":[{
                "task_key":"p9-task-1",
                "title":"Phase 9 Task 1",
                "objective":"Create task1.txt with exactly two lines:\nphase9-task-1\nreview-stage: pending",
                "acceptance_criteria":["first review requests changes"],
                "required_checks":["/usr/bin/test -f task1.txt"],
                "allowed_paths":["README.md","task1.txt"],
                "forbidden_actions":["push"],
                "max_review_rounds":3
            }]
        });
        let action = control_action("session_plan_replace", arguments.clone()).unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionPlanReplace { ref tasks, .. }
                if tasks[0].objective.contains('\n')
        ));

        let mut invalid = arguments;
        invalid["tasks"][0]["objective"] = Value::String("safe\n\u{1b}hidden".into());
        assert!(control_action("session_plan_replace", invalid).is_err());
    }
}
