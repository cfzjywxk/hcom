use crate::control_api::{ActionName, ControlAction};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};

pub(crate) const ARCHITECT_INSTRUCTIONS: &str = "\
You are the foreground hcom Architect. Unless the human explicitly says that \
this current Architect session itself must implement the code, do not develop, \
edit the task repository, run the task's implementation checks as its \
Developer, or complete implementation work yourself. Generic requests such as \
\"implement\", \"proceed\", or \"finish\" mean: analyze and bind concrete tasks, \
then delegate implementation and review through the hcom Developer/Reviewer \
loop.

Analyze every human request before dispatch. If it is already concrete, bind it \
faithfully with no gratuitous rewriting. If its intended behavior and scope are \
clear but task decomposition or implementation guidance is incomplete, inspect \
the project materials, make the plan concrete, and record important assumptions \
and tradeoffs in durable task/design documents. If a decision cannot be derived \
from existing materials and different answers would materially change behavior, \
acceptance, scope, or the task set, ask the human before approval. Prefer \
showing inferable assumptions with the complete draft so the human can confirm \
them in the normal approval step. If you introduce a material assumption not \
already authorized by the human's current execution request, display it and \
wait for explicit approval instead of auto-starting in the same turn.

After a plan is approved and until that run is terminal, do not modify its bound \
task/design sources or task repository. The only task-related file you may \
create is a new clarification document at the exact clarification_output_path \
supplied by hcom. Never edit or reuse an earlier clarification. hcom only \
transports these paths; it does not interpret task Markdown, `ASSUMPTION:`, \
`REQUIREMENT_AMBIGUITY:`, or implementation quality. A terminal run and its \
artifacts remain immutable, but terminal does not end this foreground \
Architect. After completing the terminal evidence handoff, a later human \
request may update planning sources and begin a fresh empty run in this same \
Architect session; the new run still requires an exact plan binding and \
explicit execution authorization.";

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
        bail!("architect plan adapters differ from the profiles loaded for this run");
    }
    Ok(action)
}

fn tool_description(action: ActionName) -> &'static str {
    match action {
        ActionName::SessionRunBegin => {
            "Begin a fresh empty run inside this same foreground Architect after the current run is terminal. The old run, its terminal snapshot, and its durable artifacts remain immutable; this action creates a new run_id, preserves the frozen project/profile binding, advances the session version, and resets run-local task and worker identity. Call it only after reading and reporting every required terminal Reviewer/clarification artifact from the old run and only when the human has supplied a later request that needs a new delegated plan. Bind terminal_run_id and expected_session_version to the exact terminal snapshot. This action does not bind a plan, approve execution, or start a worker. After success, use the returned new run_id and version for plan binding. Never use it to skip, abandon, reorder, or replace a non-terminal run."
        }
        ActionName::SessionPlanReplace => {
            "The current Architect session plans and delegates; it does not implement or complete the task unless the human explicitly assigns implementation to this Architect session. Generic requests to implement, proceed, or finish mean analyze, bind, and delegate through the Developer/Reviewer loop. Draft or replace the bounded ordered task plan using file pointers after analyzing the human request and current project documentation. Bind a clear task faithfully; refine or split it only when needed to make execution concrete. Record important inferable assumptions in durable task/design documents. Ask the human before approval only when a decision cannot be derived and different answers materially change behavior, acceptance, scope, or the task set. For every task, set repository_root to the real absolute source directory, task_document_path to the absolute objective/acceptance/checks/scope/actions file, design_document_paths to required design files, task_selector to the exact task section, max_review_rounds to the review budget, and max_clarification_rounds to the maximum Architect-autonomous clarification submissions. There is no configured host-path allowlist. hcom preserves these exact strings; it does not read, canonicalize, hash, lock, drift-check, check document existence, or inspect Git. repository_root must be an existing directory. Before this call you may update planning documents. After it and until the run is terminal, do not modify any bound repository or task/design source: a concurrent Architect write can be silently swept into the Developer's commit. This call never starts a worker. Display every ordinal, task_key, repository_root, task_document_path, design_document_paths entry, task_selector, both round limits, all material assumptions, plan version, and plan hash; do not abbreviate or omit them. A current human instruction to follow or execute a named detailed plan, specification, or current_todo permits same-turn start only when the draft is faithful and introduces no new material decision; examples include \"按照 current_todo\" and \"按照 <named plan> 推进完成开发\". Otherwise wait for explicit approval. An explicit instruction not to start always wins."
        }
        ActionName::SessionApproveAndStart => {
            "The current Architect session starts the delegated Developer/Reviewer workflow; it does not implement or complete the task unless the human explicitly assigns implementation to this Architect session. Generic requests to implement, proceed, or finish authorize planning and delegation, not Architect-side development. Start the exact draft only with explicit human execution authorization in this Architect conversation. Authorization is either approval of the complete displayed task binding list (ordinal, task_key, repository_root, task_document_path, design_document_paths, task_selector, and both round limits) with material assumptions, plan version, and plan hash, or the human's current explicit direction to follow or execute a named existing detailed plan, specification, or current_todo from which the draft is faithfully derived without a new material decision. Examples include \"按照 current_todo\" and \"按照 <named plan> 推进完成开发\". Read, analyze, discuss, summarize, draft, or update-a-plan requests are not execution authorization; an instruction not to start always wins. A faithful named-plan instruction may start in the same turn. When this returns Running, immediately call session_wait with run_id equal to the returned session.run_id and after_session_version equal to the returned session.version. Do not poll."
        }
        ActionName::SessionClarificationSubmit => {
            "Submit one new clarification document for the exact latched Developer request. Read the Developer request file and relevant approved sources first. Create only the exact clarification_document_path supplied as clarification_output_path by session_wait or session_status; never edit task/design sources, the repository, or an older clarification. The document should answer the specific issue without expanding approved scope. Set human_decision_confirmed=false only for an Architect-derived answer while autonomous budget remains. Set it true only after hcom says human_decision_required and the human has actually decided. This boolean is an Architect attestation; hcom cannot independently verify the keyboard source of the decision. On success, immediately call session_wait again with run_id equal to the returned session.run_id and after_session_version equal to the returned session.version."
        }
        ActionName::SessionClarificationRequireHuman => {
            "Escalate the exact latched Developer request when the Architect cannot derive a defensible answer or a material human decision is needed before the autonomous budget is exhausted. After this call, explain the decision, alternatives, consequences, Developer-reported repository state, and that the foreground run is only in memory; ask the human and END the turn. Do not call session_wait while awaiting the human. When the human answers, create the exact pending clarification_output_path and call session_clarification_submit with human_decision_confirmed=true. On that successful response, immediately re-arm session_wait with run_id equal to the returned session.run_id and after_session_version equal to the returned session.version. If the human decides the task cannot continue, the only exit is explicit session_cancel followed by a separately approved new plan/run; do not invent skip, abandon, reorder, or in-place plan replacement."
        }
        ActionName::SessionClarificationsList => {
            "Read a bounded page of durable clarification records for one exact task in the exact current run_id. Status snapshots expose only clarification_record_count so control responses stay bounded. Start with after_sequence=0 and a limit from 1 through 8; if next_after_sequence is present, pass it as the next after_sequence until it is absent. Finish reading the terminal run before session_run_begin, because an earlier run is immutable durable evidence rather than the current in-memory control target. This read-only action does not consume a clarification round and is not a wait or polling mechanism."
        }
        ActionName::SessionWait => {
            "Passively wait within the exact current run_id for either a terminal session or a latched pending_architect_action. This is event-driven, not polling: normal Developer and Reviewer transitions do not return. A pending action carries published_version and is retained across interruption or reconnect: a wait from an older version in the same run re-delivers it, while a repeated wait at or after published_version is rejected until you resolve the action. On an action response, read the exact Developer request path. If you can derive a bounded answer, create the exact clarification_output_path, submit it, and immediately wait again in the same turn. If hcom already requires a human decision, or you choose to escalate with session_clarification_require_human, ask the human and end the turn without waiting. After a terminal response, read all listed Reviewer final files; when clarification_record_count is nonzero, use session_clarifications_list with that run_id to read the bounded record pages before reporting the original outcomes. A later human request may then use session_run_begin to create a fresh run without restarting this Architect. Cancellation or interruption of this tool never cancels the run."
        }
        ActionName::SessionStatus => {
            "Read the in-memory status only when the human asks. It includes any latched pending Architect action, clarification record counts, round budgets, and whether a worker turn is active. Use session_clarifications_list for bounded record pages. It is not a keepalive or polling tool. If the run is active with no human question pending, use session_wait."
        }
        ActionName::SessionCancel => {
            "Cancel this foreground run at an exact version only after the human requests cancellation."
        }
    }
}

fn action_schema(action: ActionName, developer_adapter: &str, reviewer_adapter: &str) -> Value {
    match action {
        ActionName::SessionRunBegin => object_schema(
            &["expected_session_version", "terminal_run_id"],
            [
                ("expected_session_version", uint_schema()),
                ("terminal_run_id", id_schema()),
            ],
        ),
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
        ActionName::SessionClarificationSubmit => object_schema(
            &[
                "expected_session_version",
                "task_ordinal",
                "task_key",
                "action_sequence",
                "developer_request_path",
                "clarification_document_path",
                "human_decision_confirmed",
            ],
            [
                ("expected_session_version", uint_schema()),
                ("task_ordinal", uint32_schema()),
                ("task_key", id_schema()),
                ("action_sequence", positive_uint32_schema()),
                ("developer_request_path", absolute_path_schema()),
                ("clarification_document_path", absolute_path_schema()),
                ("human_decision_confirmed", json!({"type":"boolean"})),
            ],
        ),
        ActionName::SessionClarificationRequireHuman => object_schema(
            &[
                "expected_session_version",
                "task_ordinal",
                "task_key",
                "action_sequence",
                "developer_request_path",
            ],
            [
                ("expected_session_version", uint_schema()),
                ("task_ordinal", uint32_schema()),
                ("task_key", id_schema()),
                ("action_sequence", positive_uint32_schema()),
                ("developer_request_path", absolute_path_schema()),
            ],
        ),
        ActionName::SessionClarificationsList => object_schema(
            &[
                "run_id",
                "task_ordinal",
                "task_key",
                "after_sequence",
                "limit",
            ],
            [
                ("run_id", id_schema()),
                ("task_ordinal", uint32_schema()),
                ("task_key", id_schema()),
                ("after_sequence", uint32_schema()),
                ("limit", json!({"type":"integer","minimum":1,"maximum":8})),
            ],
        ),
        ActionName::SessionWait => object_schema(
            &["run_id", "after_session_version"],
            [
                ("run_id", id_schema()),
                ("after_session_version", uint_schema()),
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
            "repository_root",
            "task_document_path",
            "design_document_paths",
            "task_selector",
            "max_review_rounds",
            "max_clarification_rounds",
        ],
        [
            ("task_key", id_schema()),
            ("title", string_schema(1, 512)),
            ("repository_root", absolute_path_schema()),
            (
                "task_document_path",
                document_path_schema("Task file containing the selected work contract."),
            ),
            (
                "design_document_paths",
                unique_array(document_path_schema(
                    "Design file the selected task requires the workers to read.",
                )),
            ),
            ("task_selector", string_schema(1, 4096)),
            (
                "max_review_rounds",
                json!({"type":"integer","minimum":1,"maximum":20}),
            ),
            (
                "max_clarification_rounds",
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
        "description":"Real absolute path of this task's source directory, discovered from the current project's documentation; it may be outside or nested under the project directory. hcom only requires that it exists and never inspects its Git state."
    })
}

fn document_path_schema(description: &str) -> Value {
    json!({
        "type":"string",
        "minLength":1,
        "maxLength":4096,
        "pattern":"^/[^\\r\\n]*$",
        "description":format!("{description} hcom preserves this absolute path string without reading, canonicalizing, hashing, snapshotting, or checking the file.")
    })
}

fn uint_schema() -> Value {
    json!({"type":"integer","minimum":0,"maximum":u64::MAX})
}

fn positive_uint_schema() -> Value {
    json!({"type":"integer","minimum":1,"maximum":u64::MAX})
}

fn uint32_schema() -> Value {
    json!({"type":"integer","minimum":0,"maximum":u32::MAX})
}

fn positive_uint32_schema() -> Value {
    json!({"type":"integer","minimum":1,"maximum":u32::MAX})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn tool_inventory_is_exact_and_contains_no_project_or_generic_authority() {
        let tools = tool_definitions("codex-developer", "claude-reviewer-2.1.220");
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
            "codex-developer",
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
                "codex-developer",
                "claude-reviewer-2.1.220",
            )
            .is_err()
        );
        assert!(
            control_action(
                "shell",
                json!({}),
                "codex-developer",
                "claude-reviewer-2.1.220",
            )
            .is_err()
        );
    }

    #[test]
    fn next_run_tool_preserves_terminal_evidence_and_does_not_start_work() {
        let tools = tool_definitions("codex-developer", "codex-reviewer");
        let begin = tools
            .iter()
            .find(|tool| tool["name"] == "session_run_begin")
            .unwrap();
        let description = begin["description"].as_str().unwrap();
        for required in [
            "same foreground Architect",
            "old run",
            "immutable",
            "new run_id",
            "does not bind a plan",
            "does not",
            "start a worker",
            "terminal_run_id",
        ] {
            assert!(
                description.contains(required),
                "next-run description omitted {required}"
            );
        }
        assert_eq!(
            begin["inputSchema"]["required"],
            json!(["expected_session_version", "terminal_run_id"])
        );
        let action = control_action(
            "session_run_begin",
            json!({
                "expected_session_version":12,
                "terminal_run_id":"run-completed"
            }),
            "codex-developer",
            "codex-reviewer",
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionRunBegin {
                expected_session_version: 12,
                ref terminal_run_id,
            } if terminal_run_id == "run-completed"
        ));
    }

    #[test]
    fn plan_translation_accepts_file_pointers_and_rejects_inline_task_fields() {
        let arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer",
            "reviewer_adapter":"claude-reviewer-2.1.220",
            "tasks":[{
                "task_key":"p9-task-1",
                "title":"Phase 9 Task 1",
                "repository_root":"/source/example",
                "task_document_path":"/project/current_todo.md",
                "design_document_paths":["/project/architecture.md","/project/design.md"],
                "task_selector":"FBTC-01",
                "max_review_rounds":3,
                "max_clarification_rounds":2
            }]
        });
        let action = control_action(
            "session_plan_replace",
            arguments.clone(),
            "codex-developer",
            "claude-reviewer-2.1.220",
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionPlanReplace { ref tasks, .. }
                if tasks[0].task_document_path == "/project/current_todo.md"
                    && tasks[0].design_document_paths.len() == 2
                    && tasks[0].task_selector == "FBTC-01"
        ));

        for inline_field in [
            "objective",
            "acceptance_criteria",
            "required_checks",
            "allowed_paths",
            "forbidden_actions",
        ] {
            let mut invalid = arguments.clone();
            invalid["tasks"][0][inline_field] = Value::String("inline task body".into());
            assert!(
                control_action(
                    "session_plan_replace",
                    invalid,
                    "codex-developer",
                    "claude-reviewer-2.1.220",
                )
                .is_err(),
                "legacy inline field {inline_field} was accepted"
            );
        }

        let tools = tool_definitions("codex-developer", "claude-reviewer-2.1.220");
        let properties = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap()["inputSchema"]["properties"]["tasks"]["items"]["properties"]
            .as_object()
            .unwrap();
        for required in [
            "task_document_path",
            "design_document_paths",
            "task_selector",
        ] {
            assert!(properties.contains_key(required));
        }
        for removed in [
            "objective",
            "acceptance_criteria",
            "required_checks",
            "allowed_paths",
            "forbidden_actions",
        ] {
            assert!(!properties.contains_key(removed));
        }
    }

    #[test]
    fn configured_adapter_is_both_schema_visible_and_enforced() {
        let tools = tool_definitions("codex-developer", "codex-reviewer");
        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        assert_eq!(
            plan["inputSchema"]["properties"]["reviewer_adapter"]["enum"],
            json!(["codex-reviewer"])
        );
        let arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer",
            "reviewer_adapter":"claude-reviewer-2.1.220",
            "tasks":[{
                "task_key":"one",
                "title":"one",
                "repository_root":"/source/example",
                "task_document_path":"/project/current_todo.md",
                "design_document_paths":[],
                "task_selector":"one",
                "max_review_rounds":1,
                "max_clarification_rounds":2
            }]
        });
        assert!(
            control_action(
                "session_plan_replace",
                arguments,
                "codex-developer",
                "codex-reviewer",
            )
            .is_err()
        );
    }

    #[test]
    fn execution_authorization_contract_supports_exact_or_named_plan_start() {
        let tools = tool_definitions("codex-developer", "claude-reviewer-2.1.220");
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
            "task_document_path",
            "design_document_paths",
            "task_selector",
            "max_clarification_rounds",
            "material assumptions",
            "plan version",
            "plan hash",
        ] {
            assert!(
                plan.contains(required),
                "plan description omitted {required}"
            );
        }
        assert!(plan.contains("do not abbreviate or omit"));
        assert!(plan.contains("current Architect session plans and delegates"));
        assert!(plan.contains("Developer/Reviewer loop"));
        assert!(plan.contains("no configured host-path allowlist"));
        assert!(plan.contains("silently swept into the Developer's commit"));
        assert!(plan.contains("按照 current_todo"));
        assert!(plan.contains("按照 <named plan> 推进完成开发"));

        let approve_tool = tools
            .iter()
            .find(|tool| tool["name"] == "session_approve_and_start")
            .unwrap();
        let approve = approve_tool["description"].as_str().unwrap();
        for required in [
            "task binding list",
            "task_key",
            "repository_root",
            "task_document_path",
            "design_document_paths",
            "task_selector",
            "plan version",
            "plan hash",
            "named existing detailed plan",
            "same turn",
            "current_todo",
            "follow",
            "new material decision",
        ] {
            assert!(
                approve.contains(required),
                "approval description omitted {required}"
            );
        }
        for non_authorizing in ["analyze", "summarize", "draft", "update-a-plan"] {
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

    #[test]
    fn architect_tool_descriptions_never_promise_git_inspection() {
        // hcom takes only the task's source directory path. Any description
        // that still promised branch/revision evidence or a drift check would
        // make the Architect report guarantees the supervisor cannot give.
        let tools = tool_definitions("codex-developer", "claude-reviewer-2.1.220");
        for tool in &tools {
            let description = tool["description"].as_str().unwrap();
            for forbidden in [
                "branch",
                "base_revision",
                "HEAD",
                "Git top level",
                "clean and committed",
                "validates its Git identity",
            ] {
                assert!(
                    !description.contains(forbidden),
                    "{} description still claims Git observation: {forbidden}",
                    tool["name"]
                );
            }
        }

        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        let description = plan["description"].as_str().unwrap();
        assert!(description.contains("inspect Git"));
        assert!(description.contains("drift-check"));
        assert!(description.contains("check document existence"));
        assert!(description.contains("existing directory"));
        assert!(
            plan["inputSchema"]["properties"]["tasks"]["items"]["properties"]["repository_root"]
                ["description"]
                .as_str()
                .unwrap()
                .contains("never inspects its Git state")
        );
    }

    #[test]
    fn worker_status_contract_uses_latched_action_wait_without_model_polling() {
        let tools = tool_definitions("codex-developer", "claude-reviewer-2.1.220");
        let approve = tools
            .iter()
            .find(|tool| tool["name"] == "session_approve_and_start")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        let wait_tool = tools
            .iter()
            .find(|tool| tool["name"] == "session_wait")
            .unwrap();
        let wait = wait_tool["description"].as_str().unwrap();
        let status = tools
            .iter()
            .find(|tool| tool["name"] == "session_status")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(approve.contains("immediately call session_wait"));
        assert!(approve.contains("returned session.run_id"));
        assert!(approve.contains("returned session.version"));
        assert!(approve.contains("Do not poll"));
        assert!(wait.contains("terminal session"));
        assert!(wait.contains("pending_architect_action"));
        assert!(wait.contains("published_version"));
        assert!(wait.contains("older version in the same run re-delivers"));
        assert!(wait.contains("repeated wait"));
        assert!(wait.contains("normal Developer and Reviewer transitions"));
        assert!(wait.contains("never cancels the run"));
        assert!(wait.contains("immediately wait again"));
        assert!(wait.contains("end the turn without waiting"));
        assert!(wait.contains("session_clarifications_list"));
        assert_eq!(
            wait_tool["inputSchema"]["required"],
            json!(["run_id", "after_session_version"])
        );
        assert!(status.contains("only when the human asks"));
        assert!(status.contains("not a keepalive"));
        assert!(status.contains("pending Architect action"));
        assert!(status.contains("clarification record counts"));
        for description in [approve, wait, status] {
            assert!(!description.contains("180 to 300 seconds"));
            assert!(!description.contains("30-second cadence"));
        }

        let action = control_action(
            "session_wait",
            json!({"run_id":"run-one","after_session_version":7}),
            "codex-developer",
            "claude-reviewer-2.1.220",
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionWait {
                ref run_id,
                after_session_version: 7
            } if run_id == "run-one"
        ));
    }

    #[test]
    fn architect_instructions_and_clarification_tools_keep_role_and_human_boundaries() {
        for required in [
            "current Architect session itself",
            "Developer/Reviewer loop",
            "Analyze every human request",
            "materially change behavior",
            "wait for explicit approval",
            "clarification_output_path",
            "does not interpret",
            "fresh empty run",
        ] {
            assert!(
                ARCHITECT_INSTRUCTIONS.contains(required),
                "Architect instructions omitted {required}"
            );
        }
        let tools = tool_definitions("codex-developer", "codex-reviewer");
        let submit = tools
            .iter()
            .find(|tool| tool["name"] == "session_clarification_submit")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(submit.contains("exact clarification_document_path"));
        assert!(submit.contains("human_decision_confirmed=false"));
        assert!(submit.contains("Architect attestation"));
        assert!(submit.contains("immediately call session_wait again"));
        assert!(submit.contains("returned session.run_id"));
        assert!(submit.contains("returned session.version"));

        let require_human = tools
            .iter()
            .find(|tool| tool["name"] == "session_clarification_require_human")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(require_human.contains("END the turn"));
        assert!(require_human.contains("Do not call session_wait"));
        assert!(require_human.contains("foreground run is only in memory"));
        assert!(require_human.contains("do not invent skip"));
        assert!(require_human.contains("returned session.run_id"));
        assert!(require_human.contains("returned session.version"));

        let list = tools
            .iter()
            .find(|tool| tool["name"] == "session_clarifications_list")
            .unwrap();
        assert!(
            list["description"]
                .as_str()
                .unwrap()
                .contains("bounded page")
        );
        assert_eq!(
            list["inputSchema"]["properties"]["limit"]["maximum"],
            json!(8)
        );
    }
}
