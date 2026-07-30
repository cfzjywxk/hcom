use crate::control_api::{ActionName, ControlAction};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};

pub(crate) fn tool_definitions(developer_adapter: &str, reviewer_adapter: &str) -> Vec<Value> {
    ActionName::ARCHITECT
        .into_iter()
        .map(|action| {
            json!({
                "name": action.as_str(),
                "description": tool_description(action),
                "inputSchema": action_schema(action, developer_adapter, reviewer_adapter),
            })
        })
        .collect()
}

pub(crate) fn control_action(
    name: &str,
    arguments: Value,
    developer_adapter: &str,
    reviewer_adapter: &str,
) -> Result<ControlAction> {
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
    if let ControlAction::SessionPlanReplace {
        developer_adapter: requested_developer,
        reviewer_adapter: requested_reviewer,
        ..
    } = &action
        && (requested_developer != developer_adapter || requested_reviewer != reviewer_adapter)
    {
        bail!("architect plan adapters differ from the session-frozen profiles");
    }
    Ok(action)
}

fn tool_description(action: ActionName) -> &'static str {
    match action {
        ActionName::SessionPlanReplace => {
            "Draft or replace the bounded ordered task plan. Read the current project documentation first and set each task's repository_root to that task's real absolute canonical Git top level; it need not equal or live under the project directory. You must faithfully select the repository identified by the named source plan: hcom validates its Git identity and state but has no configured host-path allowlist. You may create or update architecture plans, current_todo, and discussion records before this call. If such an artifact is inside a task repository, leave that repository clean and committed before binding it; after this call, do not modify a bound task repository yourself. That exclusion is enforced by drift checks, not a second filesystem sandbox: an in-scope concurrent write could be included in a developer commit, while other drift may stop only after the developer has committed and leave that partial commit for the human. This call never starts a worker. Enumerate every returned task binding to the human with ordinal, task_key, repository_root, branch, and start HEAD (base_revision), then present the exact plan version and exact plan hash. Do not abbreviate or omit a writable repository binding. If the human's current message explicitly directs you to follow, implement, execute, proceed with, or complete a named existing detailed plan, specification, or current_todo (including an instruction meaning \"按照 current_todo\" or \"按照 <named plan> 推进完成开发\"), that message authorizes starting the faithfully derived plan in this same turn after you present these bindings; otherwise wait for a later explicit approval. An explicit instruction not to start always wins."
        }
        ActionName::SessionApproveAndStart => {
            "Start the exact draft only with explicit human execution authorization in this architect conversation. Authorization is valid either when the human approves the complete displayed task binding list (ordinal, task_key, repository_root, branch, and start HEAD/base_revision) with its plan version and plan hash, or when the human's current message explicitly directs you to follow, implement, execute, proceed with, or complete a named existing detailed plan, specification, or current_todo (including an instruction meaning \"按照 current_todo\" or \"按照 <named plan> 推进完成开发\") and this draft faithfully derives from that source. In the latter case, present the complete returned bindings, plan version, and plan hash, then call this tool in the same turn without requiring a second human reply. A request only to read, analyze, discuss, summarize, draft, or update a plan is not execution authorization. An explicit instruction not to start always wins. Never infer authorization from vague continuation language or from the existence of a plan."
        }
        ActionName::SessionStatus => {
            "Read the sanitized in-memory status of this foreground architect run."
        }
        ActionName::SessionCancel => {
            "Cancel this foreground run at an exact version only after the human requests cancellation."
        }
    }
}

fn action_schema(action: ActionName, developer_adapter: &str, reviewer_adapter: &str) -> Value {
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
                        "enum":[developer_adapter]
                    }),
                ),
                (
                    "reviewer_adapter",
                    json!({
                        "type":"string",
                        "enum":[reviewer_adapter]
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
                (
                    "approval_confirmed",
                    json!({
                        "type":"boolean",
                        "const":true,
                        "description":"Attest that the human explicitly authorized execution either by approving this exact displayed draft or by directing the Architect to follow/implement/execute the named existing detailed plan/specification/current_todo from which this draft was faithfully derived."
                    }),
                ),
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
            "repository_root",
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
            ("repository_root", absolute_path_schema()),
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

fn absolute_path_schema() -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":4096,
        "pattern":"^/[^\\r\\n]*$",
        "description":"Real absolute canonical Git top level for this task, discovered from the current project's documentation; it may be outside or nested under the project directory."
    })
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
        let tools = tool_definitions("codex-developer-0.145.0", "claude-reviewer-2.1.220");
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
            "codex-developer-0.145.0",
            "claude-reviewer-2.1.220",
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
                }),
                "codex-developer-0.145.0",
                "claude-reviewer-2.1.220",
            )
            .is_err()
        );
        assert!(
            control_action(
                "shell",
                json!({}),
                "codex-developer-0.145.0",
                "claude-reviewer-2.1.220",
            )
            .is_err()
        );
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
                "repository_root":"/source/example",
                "acceptance_criteria":["first review requests changes"],
                "required_checks":["/usr/bin/test -f task1.txt"],
                "allowed_paths":["README.md","task1.txt"],
                "forbidden_actions":["push"],
                "max_review_rounds":3
            }]
        });
        let action = control_action(
            "session_plan_replace",
            arguments.clone(),
            "codex-developer-0.145.0",
            "claude-reviewer-2.1.220",
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionPlanReplace { ref tasks, .. }
                if tasks[0].objective.contains('\n')
        ));

        let mut invalid = arguments;
        invalid["tasks"][0]["objective"] = Value::String("safe\n\u{1b}hidden".into());
        assert!(
            control_action(
                "session_plan_replace",
                invalid,
                "codex-developer-0.145.0",
                "claude-reviewer-2.1.220",
            )
            .is_err()
        );
    }

    #[test]
    fn configured_adapter_is_both_schema_visible_and_enforced() {
        let tools = tool_definitions("codex-developer-0.145.0", "codex-reviewer-0.145.0");
        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        assert_eq!(
            plan["inputSchema"]["properties"]["reviewer_adapter"]["enum"],
            json!(["codex-reviewer-0.145.0"])
        );
        let arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer-0.145.0",
            "reviewer_adapter":"claude-reviewer-2.1.220",
            "tasks":[{
                "task_key":"one",
                "title":"one",
                "objective":"one",
                "repository_root":"/source/example",
                "acceptance_criteria":["one"],
                "required_checks":[],
                "allowed_paths":["one.txt"],
                "forbidden_actions":["push"],
                "max_review_rounds":1
            }]
        });
        assert!(
            control_action(
                "session_plan_replace",
                arguments,
                "codex-developer-0.145.0",
                "codex-reviewer-0.145.0",
            )
            .is_err()
        );
    }

    #[test]
    fn execution_authorization_contract_supports_exact_or_named_plan_start() {
        let tools = tool_definitions("codex-developer-0.145.0", "claude-reviewer-2.1.220");
        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        for required in [
            "ordinal",
            "task_key",
            "repository_root",
            "branch",
            "base_revision",
            "plan version",
            "plan hash",
        ] {
            assert!(
                plan.contains(required),
                "plan description omitted {required}"
            );
        }
        assert!(plan.contains("Do not abbreviate or omit"));

        let approve_tool = tools
            .iter()
            .find(|tool| tool["name"] == "session_approve_and_start")
            .unwrap();
        let approve = approve_tool["description"].as_str().unwrap();
        for required in [
            "task binding list",
            "repository_root",
            "branch",
            "base_revision",
            "plan version",
            "plan hash",
            "named existing detailed plan",
            "same turn",
            "current_todo",
            "follow",
        ] {
            assert!(
                approve.contains(required),
                "approval description omitted {required}"
            );
        }
        for required in [
            "current_todo",
            "clean and committed",
            "do not modify a bound task repository",
            "按照 current_todo",
            "An explicit instruction not to start always wins",
            "no configured host-path allowlist",
            "included in a developer commit",
            "leave that partial commit for the human",
        ] {
            assert!(
                plan.contains(required),
                "plan description omitted {required}"
            );
        }
        for non_authorizing in ["analyze", "summarize", "draft", "update a plan"] {
            assert!(
                approve.contains(non_authorizing),
                "approval description omitted non-authorizing verb {non_authorizing}"
            );
        }
        assert_eq!(
            approve_tool["inputSchema"]["properties"]["approval_confirmed"]["const"],
            true
        );
    }
}
