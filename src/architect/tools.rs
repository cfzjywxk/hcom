use crate::control_api::protocol::minimum_review_rounds;
use crate::control_api::{ActionName, ControlAction, ReviewerAdapterBinding};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

// Codex 0.145/0.146 starts compacting a normalized MCP input schema at 5,000
// bytes. Keep an explicit margin so hcom's control schemas never depend on that
// lossy compatibility path.
const MAX_CODEX_INPUT_SCHEMA_BYTES: usize = 4_500;

pub(crate) const ARCHITECT_INSTRUCTIONS: &str = "\
You are the foreground hcom Architect. Unless the human explicitly says that \
this current Architect session itself must implement the code, do not develop, \
edit the task repository, run the task's implementation checks as its \
Developer, or complete implementation work yourself. Generic requests such as \
\"implement\", \"proceed\", \"finish\", or \"drive\" mean: analyze and bind concrete \
tasks, then delegate implementation and review through the hcom \
Developer/Reviewer loop. These generic requests select delegation rather than \
Architect-side implementation; standing alone, they do not authorize starting \
the delegated loop. Execution authorization is limited to the forms below.

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

An explicit human request to plan or define the solution and then implement, \
proceed, finish, or drive the work authorizes same-turn start of the faithful \
plan you derive, even when that detailed plan did not exist when the human \
spoke. Display the complete typed binding first, but do not ask the human to \
repeat the authorization merely because the exact plan was produced afterward. \
A direction to follow or execute a named existing detailed plan has the same \
effect. Read, analyze, discuss, summarize, draft, or update-a-plan requests \
without an execution directive do not authorize start. An explicit instruction \
not to start always wins.

The standard delegated loop requires each Developer to create exactly one \
signed-off local candidate commit for its task before review and to amend only \
that same task commit for corrections. Approval of the run includes this local \
candidate-commit topology; it does not authorize push, install, release, or an \
extra commit after LGTM. Do not write or bind a task/design source that forbids \
the required local candidate commit. A general rule that local commits require \
human authorization is satisfied by the human's execution authorization for \
this run. If the human explicitly requires this run to remain uncommitted, \
explain that the standard lane is incompatible and ask before binding or \
starting it. If a Developer reports this authority conflict after start, it is \
never an Architect-derivable clarification: call \
session_clarification_require_human even while autonomous clarification budget \
remains. Do not submit an autonomous clarification or reinterpret run approval \
as overriding the explicit no-commit instruction.

Every review is one strict synchronous generation across the ordered active \
Reviewer bindings: Reviewer1 in single-review mode, or Reviewer1 and Reviewer2 \
in dual-review mode. `review_round` counts only generations whose active logical \
responses have joined; `review_generation` identifies the current or last \
allocated generation. Display Reviewer identity and generation on every review \
progress update, and display `responses_received`/`responses_expected` for a \
`review_responded` event. A response is partial progress while \
`responses_received` is less than `responses_expected`: do not report that the \
review cycle finished, do not start or imply a Developer correction, and \
immediately re-arm `session_wait` for the remaining response. Never read or \
summarize a Reviewer final path merely to display progress. Only after terminal, \
read every listed active Reviewer's current-generation evidence and report the \
original verdicts/findings. A task is LGTM only when every active Reviewer \
returned LGTM for that same generation; do not create or request a post-LGTM \
commit.

hcom session-control tools are direct MCP tools. Call them directly; never \
wrap `session_wait` in `functions.exec`/`functions.wait`, add a timer, or emit a \
heartbeat while it is pending. A pending wait consumes no Architect model turn \
until hcom returns a normal Developer/Reviewer result, a Developer \
clarification/blocker action, or a terminal result. Internal task-completion \
bookkeeping, status publication, supervisor polling, timers, and transport \
yields never release the wait. A terminal result supersedes queued progress so \
the final Reviewer result and derived task completion cause only one wakeup.

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
explicit execution authorization. For an LGTM task, the final Developer task \
commit has already been reviewed at its exact candidate range. Report that \
local reviewed commit and the absence of any separately authorized push or \
install; do not ask whether to retain or revert it merely because commit was \
not separately authorized, and do not create another post-LGTM commit.";

const LOCAL_ARCHITECT_COMMIT_CONTRACT: &str = "The standard delegated loop requires each Developer to create exactly one signed-off local candidate commit for its task before review and to amend only that same task commit for corrections. Approval of the run includes this local candidate-commit topology; it does not authorize push, install, release, or an extra commit after LGTM. Do not write or bind a task/design source that forbids the required local candidate commit. A general rule that local commits require human authorization is satisfied by the human's execution authorization for this run. If the human explicitly requires this run to remain uncommitted, explain that the standard lane is incompatible and ask before binding or starting it. If a Developer reports this authority conflict after start, it is never an Architect-derivable clarification: call session_clarification_require_human even while autonomous clarification budget remains. Do not submit an autonomous clarification or reinterpret run approval as overriding the explicit no-commit instruction.";

const GITHUB_ARCHITECT_COMMIT_CONTRACT: &str = "The GitHub Pull Request delegated loop requires every Developer turn to create exactly one new signed-off child commit on the generated run branch. Corrections and later-task initial turns append commits; they never amend, rebase, squash, reword, or force-push published run history. The foreground supervisor publishes each successful Developer final and each Reviewer final byte-for-byte, without redaction or secret scanning, inside a generated GitHub body whose UTF-8 hard cap is 60 KiB; disclose that external publication to the selected private repository in the complete plan. Approval of the exact displayed plan includes this append-only commit and bounded one-PR push topology; it does not authorize install, release, deployment, or a further commit for a task after that task reaches LGTM. Do not write or bind a task/design source that forbids the required commits. A general rule that commits and the bound GitHub writes require human authorization is satisfied by the human's execution authorization for this exact plan. If the human explicitly requires this run to remain uncommitted or unpushed, explain that the GitHub Pull Request lane is incompatible and ask before binding or starting it. If a Developer reports this authority conflict after start, it is never an Architect-derivable clarification: call session_clarification_require_human even while autonomous clarification budget remains. Do not submit an autonomous clarification or reinterpret run approval as overriding the explicit restriction.";

const LOCAL_ARCHITECT_LGTM_HANDOFF: &str = "For an LGTM task, the final Developer task commit has already been reviewed at its exact candidate range. Report that local reviewed commit and the absence of any separately authorized push or install; do not ask whether to retain or revert it merely because commit was not separately authorized, and do not create another post-LGTM commit.";

const GITHUB_ARCHITECT_LGTM_HANDOFF: &str = "For an LGTM task, the final Developer task range has already been reviewed at its exact published head. Report its commit range and Pull Request evidence; do not create or request another commit for that task. A later task's initial turn still appends its own required commit to the same run branch. Install and release remain separately authorized actions.";

const GITHUB_ARCHITECT_INSTRUCTIONS_SUFFIX: &str = "\
\n\nThis foreground session uses the explicit GitHub Pull Request delivery lane. \
Before binding a plan, call session_github_delivery_inspect and use only its \
newest inspection_id. Display the complete frozen GitHub delivery and run \
binding: private owner/repository, canonical local repository root, base \
branch and inspected SHA, explicit delivery policy, policy-applicable ruleset \
attestation, generated run branch, active App IDs/slugs with ordered Reviewer \
mapping, publication/check policy, and policy-applicable merge, drift, cleanup, \
or preservation behavior. For manual delivery, disclose that hcom cannot prove \
server-side PR/direct-push enforcement for a private GitHub Free repository, \
cannot prevent authorized external direct push or early merge, never requests \
merge/deletion/finalization, and completes all-LGTM as \
review_complete_unmerged with the PR/refs/worktree preserved for human \
disposition. Every task repository_root must equal \
the frozen local repository root. Explicitly disclose that each Developer and \
Reviewer native final is opaque payload published byte-for-byte without \
redaction or secret scanning to the selected private repository, and that its \
generated PR/review/comment body has a 60 KiB UTF-8 hard cap. The read-only \
inspection does not authorize any GitHub write. Approval of the exact displayed \
plan authorizes only the bounded one-PR workflow encoded by that binding; it \
still does not authorize installation, release, package publication, deployment, \
or unrelated repository/branch mutation.\n\nFor progress, report the exact PR URL, \
generation, and published head from the typed event without reading native \
finals. At terminal, read only the listed current-generation final paths and \
report the PR number/URL; run base/final head; every task base..final-head range \
and outcome; ordered Reviewer App verdict/review URLs; final hcom/review Check; \
approved and final policy-applicable ruleset attestations; delivery outcome; \
merge SHA when confirmed; and any preserved branch, worktree, or PR. Distinguish \
review_complete_unmerged, delivered, unmerged_review_exhausted, pre-merge \
operational failure, and a confirmed merge \
whose required finalization failed. Never retry or imply retry of a confirmed \
merge, and never imply that a fresh run adopts a preserved PR/worktree.";

pub(crate) fn architect_instructions_for_delivery(github_pr: bool) -> String {
    if github_pr {
        let instructions = ARCHITECT_INSTRUCTIONS
            .replace(
                LOCAL_ARCHITECT_COMMIT_CONTRACT,
                GITHUB_ARCHITECT_COMMIT_CONTRACT,
            )
            .replace(LOCAL_ARCHITECT_LGTM_HANDOFF, GITHUB_ARCHITECT_LGTM_HANDOFF);
        debug_assert!(!instructions.contains(LOCAL_ARCHITECT_COMMIT_CONTRACT));
        debug_assert!(!instructions.contains(LOCAL_ARCHITECT_LGTM_HANDOFF));
        format!("{instructions}{GITHUB_ARCHITECT_INSTRUCTIONS_SUFFIX}")
    } else {
        ARCHITECT_INSTRUCTIONS.to_owned()
    }
}

#[cfg(test)]
pub(crate) fn tool_definitions(
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
) -> Vec<Value> {
    tool_definitions_for_delivery(developer_adapter, reviewer_adapters, false)
}

pub(crate) fn tool_definitions_for_delivery(
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
    github_pr: bool,
) -> Vec<Value> {
    ActionName::architect(github_pr)
        .iter()
        .copied()
        .map(|action| {
            json!({
                "name": action.as_str(),
                "description": tool_description(action, github_pr),
                "inputSchema": action_schema(action, developer_adapter, reviewer_adapters, github_pr),
            })
        })
        .collect()
}

pub(crate) fn validate_codex_tool_definitions(tools: &[Value]) -> Result<()> {
    for tool in tools {
        let name = tool
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Codex Architect tool has no string name"))?;
        let schema = tool
            .get("inputSchema")
            .ok_or_else(|| anyhow::anyhow!("Codex Architect tool {name} has no inputSchema"))?;
        let schema_len = serde_json::to_vec(schema)
            .context("Codex Architect tool inputSchema is not serializable")?
            .len();
        if schema_len > MAX_CODEX_INPUT_SCHEMA_BYTES {
            bail!(
                "Codex Architect tool {name} inputSchema is {schema_len} bytes; the local compatibility limit is {MAX_CODEX_INPUT_SCHEMA_BYTES}"
            );
        }
        validate_codex_schema_node(schema, &format!("{name}.inputSchema"))?;
    }
    Ok(())
}

fn validate_codex_schema_node(schema: &Value, path: &str) -> Result<()> {
    let object = schema
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{path} must be a JSON Schema object"))?;
    let schema_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{path}.type must be one primitive string"))?;
    if !matches!(
        schema_type,
        "array" | "boolean" | "integer" | "null" | "number" | "object" | "string"
    ) {
        bail!("{path}.type uses an unsupported Codex schema type");
    }
    for keyword in object.keys() {
        let common = matches!(keyword.as_str(), "const" | "description" | "enum" | "type");
        let type_specific = match schema_type {
            "array" => matches!(
                keyword.as_str(),
                "items" | "maxItems" | "minItems" | "uniqueItems"
            ),
            "integer" | "number" => matches!(keyword.as_str(), "maximum" | "minimum"),
            "object" => matches!(
                keyword.as_str(),
                "additionalProperties" | "properties" | "required"
            ),
            "string" => matches!(keyword.as_str(), "maxLength" | "minLength" | "pattern"),
            "boolean" | "null" => false,
            _ => unreachable!("schema type was validated"),
        };
        if !(common || type_specific) {
            bail!("{path} uses unsupported Codex schema keyword {keyword}");
        }
    }

    if object.contains_key("const") && object.contains_key("enum") {
        bail!("{path} cannot combine const and enum");
    }
    if let Some(value) = object.get("const") {
        validate_codex_scalar_constraint(value, schema_type, path, "const")?;
    }
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("{path}.enum must be an array"))?;
        if values.is_empty() {
            bail!("{path}.enum must not be empty");
        }
        for value in values {
            validate_codex_scalar_constraint(value, schema_type, path, "enum")?;
        }
    }

    match schema_type {
        "array" => {
            let items = object
                .get("items")
                .ok_or_else(|| anyhow::anyhow!("{path} array schema must define items"))?;
            validate_codex_schema_node(items, &format!("{path}.items"))?;
        }
        "object" => {
            if object.get("additionalProperties") != Some(&Value::Bool(false)) {
                bail!("{path} object schema must set additionalProperties=false");
            }
            let properties = object
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("{path}.properties must be an object"))?;
            let required = object
                .get("required")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("{path}.required must be an array"))?;
            let required_names: BTreeSet<_> = required
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("{path}.required must contain strings"))
                })
                .collect::<Result<_>>()?;
            let property_names: BTreeSet<_> = properties.keys().map(String::as_str).collect();
            if required_names.len() != required.len() || required_names != property_names {
                bail!("{path}.required must contain every property exactly once");
            }
            for (name, property) in properties {
                validate_codex_schema_node(property, &format!("{path}.properties.{name}"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_codex_scalar_constraint(
    value: &Value,
    schema_type: &str,
    path: &str,
    keyword: &str,
) -> Result<()> {
    let compatible = match schema_type {
        "boolean" => value.is_boolean(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "null" => value.is_null(),
        "number" => value.is_number(),
        "string" => value.is_string(),
        "array" | "object" => false,
        _ => unreachable!("schema type was validated"),
    };
    if !compatible {
        bail!(
            "{path}.{keyword} must be a scalar value compatible with type {schema_type}; complex array/object constraints are forbidden"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn control_action(
    name: &str,
    arguments: Value,
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
) -> Result<ControlAction> {
    control_action_for_delivery(name, arguments, developer_adapter, reviewer_adapters, false)
}

pub(crate) fn control_action_for_delivery(
    name: &str,
    arguments: Value,
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
    github_pr: bool,
) -> Result<ControlAction> {
    let action = ActionName::architect(github_pr)
        .iter()
        .copied()
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
        reviewer_adapters: requested_reviewers,
        ..
    } = &action
        && (requested_developer != developer_adapter || requested_reviewers != reviewer_adapters)
    {
        bail!("architect plan adapters differ from the profiles loaded for this run");
    }
    if let ControlAction::SessionPlanReplace {
        github_inspection_id,
        ..
    } = &action
        && github_pr != github_inspection_id.is_some()
    {
        bail!("architect plan inspection binding differs from the selected delivery mode");
    }
    Ok(action)
}

fn tool_description(action: ActionName, github_pr: bool) -> &'static str {
    match action {
        ActionName::SessionRunBegin => {
            "Begin a fresh empty run inside this same foreground Architect after the current run is terminal. The old run, its terminal snapshot, and its durable artifacts remain immutable; this action creates a new run_id, preserves the frozen project/profile binding, advances the session version, and resets run-local task and worker identity. Call it only after reading and reporting every required terminal Reviewer/clarification artifact from the old run and only when the human has supplied a later request that needs a new delegated plan. Bind terminal_run_id and expected_session_version to the exact terminal snapshot. This action does not bind a plan, approve execution, or start a worker. After success, use the returned new run_id and version for plan binding. Never use it to skip, abandon, reorder, or replace a non-terminal run."
        }
        ActionName::SessionGitHubDeliveryInspect => {
            "Refresh the exact read-only private repository, base ref/SHA, complete active-App permission maps, identities, and installations for the current GitHub Pull Request run. Protected auto-merge additionally refreshes the canonical hcom-critical ruleset attestation; manual delivery does not call, require, hash, freeze, or revalidate repository rules. Bind run_id and expected_session_version to the current snapshot. This operation creates no ref, worktree, branch, Pull Request, Check, comment, or token-bearing artifact and does not advance session.version. Only its newest inspection_id can be consumed by session_plan_replace; a later inspection supersedes an earlier one even when the observed SHA is unchanged. It is available only in explicit --github-pr mode."
        }
        ActionName::SessionPlanReplace if github_pr => {
            "Bind the bounded ordered GitHub Pull Request task plan using file pointers and the exact newest github_inspection_id. Every task repository_root must equal the frozen canonical local repository root. Display the complete delivery/run binding, explicit delivery policy, policy-applicable rules evidence and terminal disposition, every existing task field, material assumption, plan version, and plan hash before approval. Manual plans must disclose missing server-side enforcement, no merge/delete/finalization authority, and review_complete_unmerged preservation. The plan hash binds the frozen delivery profile and policy, inspected base/ref and policy-applicable rules evidence, inspection ID, and deterministic generated branch. This call creates no branch, worktree, Pull Request, Check, comment, worker, or other GitHub write. Every Developer turn must append exactly one new signed-off commit; corrections and later-task initial turns never amend, rebase, squash, reword, or force-push published history. Approval does not authorize install, release, deployment, or unrelated repository mutation."
        }
        ActionName::SessionPlanReplace => {
            "The current Architect session plans and delegates; it does not implement or complete the task unless the human explicitly assigns implementation to this Architect session. Generic requests to implement, proceed, finish, or drive mean analyze, bind, and delegate through the Developer/Reviewer loop. Draft or replace the bounded ordered task plan using file pointers after analyzing the human request and current project documentation. Bind a clear task faithfully; refine or split it only when needed to make execution concrete. Record important inferable assumptions in durable task/design documents. Ask the human before approval only when a decision cannot be derived and different answers materially change behavior, acceptance, scope, or the task set. For every task, set repository_root to the real absolute source directory, task_document_path to the absolute objective/acceptance/checks/scope/actions file, design_document_paths to required design files, task_selector to the exact task section, max_review_rounds to the synchronized active-Reviewer generation budget, and max_clarification_rounds to the maximum Architect-autonomous clarification submissions. One generation routes to every ordered active Reviewer binding and consumes one round only after every logical response joins. There is no configured host-path allowlist. hcom preserves these exact strings; it does not read, canonicalize, hash, lock, drift-check, check document existence, or inspect Git. repository_root must be an existing directory. Before this call you may update planning documents. After it and until the run is terminal, do not modify any bound repository or task/design source: a concurrent Architect write can be silently swept into the Developer's commit. This call never starts a worker. Every standard task must permit the fixed local candidate-commit topology: one signed-off Developer task commit before review, amended in place for corrections, with no extra commit after LGTM. Execution approval for the run includes that local commit topology but never push, install, or release. Do not bind a task/design source that forbids the candidate commit. A general rule requiring human authorization for commits is satisfied by execution authorization for this standard lane; an explicit human no-commit requirement is incompatible and must be resolved before this call. Display every ordinal, task_key, repository_root, task_document_path, design_document_paths entry, task_selector, both round limits, all material assumptions, plan version, and plan hash; do not abbreviate or omit them. A current human instruction either to follow or execute a named detailed plan, specification, or current_todo, or to plan or define the solution and then implement, proceed, finish, or drive the requested work, permits same-turn start when the draft is faithful and introduces no new material decision; examples include \"按照 current_todo\", \"按照 <named plan> 推进完成开发\", and \"先明确技术方案，然后 drive 开发完成\". Do not ask again merely because the faithful detailed plan was created after that execution instruction. Read/analyze/discuss/summarize/draft/update-a-plan alone does not authorize start, and an explicit instruction not to start always wins."
        }
        ActionName::SessionApproveAndStart if github_pr => {
            "Start the exact displayed GitHub Pull Request plan only with valid human execution authorization. Bind expected_session_version, plan_version, and plan_hash exactly. Approval authorizes the frozen one-PR workflow for this run—base fetch, one managed worktree/branch, append-only task commits and publication, and ordered active-App review/check publication. Only a protected_auto_merge binding additionally authorizes its ruleset-attested exact-head merge and generated-ref finalization; manual delivery never authorizes merge or deletion. Neither policy authorizes install, release, package publication, deployment, another repository/branch, force-push, or any unrelated mutation. On Running, immediately use session_wait; do not poll."
        }
        ActionName::SessionApproveAndStart => {
            "The current Architect session starts the delegated Developer/Reviewer workflow; it does not implement or complete the task unless the human explicitly assigns implementation to this Architect session. Generic requests to implement, proceed, finish, or drive select Architect planning and later delegation rather than Architect-side development, but standing alone they do not authorize starting the delegated loop. Start the exact draft only with explicit human execution authorization in this Architect conversation. Authorization is any of: approval of the complete displayed task binding list (ordinal, task_key, repository_root, task_document_path, design_document_paths, task_selector, and both round limits) with material assumptions, plan version, and plan hash; an explicit direction to follow or execute a named existing detailed plan, specification, or current_todo; or an explicit direction to plan or define the solution and then implement, proceed, finish, or drive the requested work. The latter prospective authorization remains valid after you derive and display the faithful detailed plan even though it did not exist when the human spoke; do not ask for a duplicate confirmation solely for that reason. The draft may start in the same turn only when it faithfully implements the authorized request and introduces no unresolved new material decision. Examples include \"按照 current_todo\", \"按照 <named plan> 推进完成开发\", and \"先明确技术方案，然后 drive 开发完成\". Read, analyze, discuss, summarize, draft, or update-a-plan requests without an execution directive are not authorization; an instruction not to start always wins. Starting this standard lane also authorizes one signed-off local Developer candidate commit per task and amendments of that same commit during correction. It does not authorize push, install, release, or a new commit after LGTM. When this returns Running, immediately call session_wait with run_id equal to the returned session.run_id, after_session_version equal to the returned session.version, and after_progress_sequence=0. Do not poll."
        }
        ActionName::SessionClarificationSubmit if github_pr => {
            "Submit one new clarification document for the exact latched Developer request. Read the Developer request file and relevant approved sources first. Create only the exact clarification_document_path supplied as clarification_output_path by session_wait or session_status; never edit task/design sources, the repository, or an older clarification. The document should answer the specific issue without expanding approved scope. Set human_decision_confirmed=false only for an Architect-derived answer while autonomous budget remains. Never use false for a conflict between an explicit no-commit/no-push instruction and the GitHub lane's required append-only commit/publication topology; that authority conflict must first go through session_clarification_require_human. Set human_decision_confirmed=true only after hcom says human_decision_required and the human has actually resolved the conflict or other decision. This boolean is an Architect attestation; hcom cannot independently verify the keyboard source of the decision. On success, immediately call session_wait again with run_id equal to the returned session.run_id, after_session_version equal to the returned session.version, and after_progress_sequence equal to the last progress event sequence already displayed in this run (or 0 when none has been displayed)."
        }
        ActionName::SessionClarificationSubmit => {
            "Submit one new clarification document for the exact latched Developer request. Read the Developer request file and relevant approved sources first. Create only the exact clarification_document_path supplied as clarification_output_path by session_wait or session_status; never edit task/design sources, the repository, or an older clarification. The document should answer the specific issue without expanding approved scope. Set human_decision_confirmed=false only for an Architect-derived answer while autonomous budget remains. Never use false for a conflict between an explicit no-commit instruction and the standard lane's required candidate commit; that authority conflict must first go through session_clarification_require_human. Set human_decision_confirmed=true only after hcom says human_decision_required and the human has actually resolved the conflict or other decision. This boolean is an Architect attestation; hcom cannot independently verify the keyboard source of the decision. On success, immediately call session_wait again with run_id equal to the returned session.run_id, after_session_version equal to the returned session.version, and after_progress_sequence equal to the last progress event sequence already displayed in this run (or 0 when none has been displayed)."
        }
        ActionName::SessionClarificationRequireHuman if github_pr => {
            "Escalate the exact latched Developer request when the Architect cannot derive a defensible answer or a material human decision is needed before the autonomous budget is exhausted. This call is mandatory when the request reports a conflict between an explicit no-commit/no-push instruction and the GitHub lane's required append-only commit/publication topology, regardless of remaining autonomous budget; run approval must not be used to override that explicit instruction. After this call, explain the decision, alternatives, consequences, Developer-reported repository state, and that the foreground run is only in memory; ask the human and END the turn. Do not call session_wait while awaiting the human. When the human answers, create the exact pending clarification_output_path and call session_clarification_submit with human_decision_confirmed=true. On that successful response, immediately re-arm session_wait with run_id equal to the returned session.run_id, after_session_version equal to the returned session.version, and after_progress_sequence equal to the last progress event sequence already displayed in this run (or 0 when none has been displayed). If the human decides the task cannot continue, the only exit is explicit session_cancel followed by a separately approved new plan/run; do not invent skip, abandon, reorder, or in-place plan replacement."
        }
        ActionName::SessionClarificationRequireHuman => {
            "Escalate the exact latched Developer request when the Architect cannot derive a defensible answer or a material human decision is needed before the autonomous budget is exhausted. This call is mandatory when the request reports a conflict between an explicit no-commit instruction and the standard lane's required candidate commit, regardless of remaining autonomous budget; run approval must not be used to override that explicit instruction. After this call, explain the decision, alternatives, consequences, Developer-reported repository state, and that the foreground run is only in memory; ask the human and END the turn. Do not call session_wait while awaiting the human. When the human answers, create the exact pending clarification_output_path and call session_clarification_submit with human_decision_confirmed=true. On that successful response, immediately re-arm session_wait with run_id equal to the returned session.run_id, after_session_version equal to the returned session.version, and after_progress_sequence equal to the last progress event sequence already displayed in this run (or 0 when none has been displayed). If the human decides the task cannot continue, the only exit is explicit session_cancel followed by a separately approved new plan/run; do not invent skip, abandon, reorder, or in-place plan replacement."
        }
        ActionName::SessionClarificationsList => {
            "Read a bounded page of durable clarification records for one exact task in the exact current run_id. Status snapshots expose only clarification_record_count so control responses stay bounded. Start with after_sequence=0 and a limit from 1 through 8; if next_after_sequence is present, pass it as the next after_sequence until it is absent. Finish reading the terminal run before session_run_begin, because an earlier run is immutable durable evidence rather than the current in-memory control target. This read-only action does not consume a clarification round and is not a wait or polling mechanism."
        }
        ActionName::SessionWait if github_pr => {
            "Call this direct MCP tool directly; never wrap it in functions.exec/functions.wait or add a heartbeat around it. Passively wait within the exact current run_id. This is event-driven, not polling, and it returns only for a normal Developer/Reviewer result, a latched pending_architect_action from a Developer clarification/blocker, or a terminal session. A nonterminal progress result is one exact review_requested event after a Developer final or review_responded event after a Reviewer final. Internal candidate_published, task_completed, merge_waiting, and run_finalizing publication/delivery bookkeeping remains retained as bounded evidence but never completes this call or wakes the Architect in manual or protected auto-merge delivery. Pass after_progress_sequence as the last worker-result progress sequence already displayed in this run, or 0 before the first event; returned sequence numbers may skip that internal bookkeeping. Display each worker result once with its delivery policy, exact PR URL, published/final head SHA, task position and generation, and Reviewer identity/verdict/response counts when review-scoped; a partial dual response must immediately re-arm this wait. Do not read or summarize native final files merely for progress. Preserve the existing clarification escalation contract. A terminal snapshot supersedes queued progress and contains final worker and delivery evidence, so a final Reviewer result, derived task completion, final Check/delivery bookkeeping, and successful terminal transition produce one terminal wakeup rather than several; an abnormal terminal transition also returns immediately. At terminal, read the listed current-generation native finals and report the exact policy, PR, per-task ranges/outcomes, ordered Reviewer review URLs/verdicts, hcom/review Check, policy-applicable rules evidence, delivery outcome, merge SHA only when delivered, and preserved branch/worktree/PR for manual or other unmerged outcomes. Cancellation or interruption of this tool never cancels the run."
        }
        ActionName::SessionWait => {
            "Call this direct MCP tool directly; never wrap it in functions.exec/functions.wait or add a heartbeat around it. Passively wait within the exact current run_id. This is event-driven, not polling, and it returns only for a normal Developer/Reviewer result, a latched pending_architect_action from a Developer clarification/blocker, or a terminal session. Internal task_completed bookkeeping, status publication, supervisor poll/timer ticks, and transport yields never complete this call or wake the Architect. Pass after_progress_sequence as the last worker-result progress sequence already displayed in this run, or 0 before the first event. A nonterminal progress result is one exact review_requested event after a Developer final or review_responded event after a Reviewer final, plus durable input or response paths. Display one concise human-visible update for that result before doing anything else: include task position/total, task_key, completed review_round and in-flight/current review_generation, the exact developer_final_path, and the Reviewer identity, verdict, and every reviewer_final_message_path when present. A review_requested event carries the ordered active Reviewer bindings. A review_responded event carries reviewer_id and responses_received/responses_expected; when fewer responses have arrived than are expected, say that another response is pending and immediately wait again. Do not read or summarize Reviewer final files merely for a progress update. Then immediately call session_wait again using the returned session_version as after_session_version and the displayed event.sequence as after_progress_sequence. Worker execution continues while progress is displayed; normal worker-result events produced before the next wait are retained. Returned progress sequence numbers may skip internal bookkeeping events that never wake the Architect. A pending Architect action takes priority over worker progress. It carries published_version and is retained across interruption or reconnect: a wait from an older version in the same run re-delivers it, while a repeated wait at or after published_version is rejected until you resolve the action. On an action response, read the exact Developer request path. If it reports a conflict between an explicit no-commit instruction and the standard lane's required candidate commit, do not answer autonomously even when clarification budget remains: call session_clarification_require_human, ask the human, and end the turn without waiting. For other requests, if you can derive a bounded answer, create the exact clarification_output_path, submit it, and immediately wait again in the same turn using the last displayed progress sequence. If hcom already requires a human decision, or you choose to escalate with session_clarification_require_human, ask the human and end the turn without waiting. A terminal snapshot supersedes queued progress and contains the final worker evidence, so a final Reviewer result, derived task completion, and successful terminal transition produce one terminal wakeup rather than several; an abnormal terminal transition also returns immediately. Only after a terminal response, read every active Reviewer's listed current-generation final files; when clarification_record_count is nonzero, use session_clarifications_list with that run_id to read the bounded record pages before reporting the original outcomes. A task is LGTM only after every active same-generation Reviewer returns LGTM. For an LGTM task, the final Developer candidate commit was already reviewed at the reported exact range: report it as the local reviewed result, do not ask whether to retain or revert it merely for lack of separate commit authorization, and do not create another post-LGTM commit. Push, install, and release remain separately authorized actions. A later human request may then use session_run_begin to create a fresh run without restarting this Architect. Cancellation or interruption of this tool never cancels the run."
        }
        ActionName::SessionStatus if github_pr => {
            "Read the in-memory GitHub Pull Request delivery status only when the human asks. It includes the complete frozen delivery binding, latest inspection and run binding, PR/worktree/branch/head, delivery phase/outcome, merge/finalization/preservation state, active workers, ordered Reviewer bindings, and per-task ranges, native results, GitHub reviews, and hcom/review Check. It is not a keepalive or polling tool; use session_wait while the run is active with no human question pending."
        }
        ActionName::SessionStatus => {
            "Read the in-memory status only when the human asks. It includes any latched pending Architect action, clarification record counts, round budgets, the bounded active_workers list, session-level ordered Reviewer bindings, and each task's current-generation typed Reviewer results. `review_round` is the completed joined-generation count; `review_generation` is the current or last allocated generation and may be one greater while review is in flight. Use session_clarifications_list for bounded record pages. It is not a keepalive or polling tool. If the run is active with no human question pending, use session_wait."
        }
        ActionName::SessionCancel => {
            "Cancel this foreground run at an exact version only after the human requests cancellation."
        }
    }
}

fn action_schema(
    action: ActionName,
    developer_adapter: &str,
    reviewer_adapters: &[ReviewerAdapterBinding],
    github_pr: bool,
) -> Value {
    match action {
        ActionName::SessionRunBegin => object_schema(
            &["expected_session_version", "terminal_run_id"],
            [
                ("expected_session_version", uint_schema()),
                ("terminal_run_id", id_schema()),
            ],
        ),
        ActionName::SessionGitHubDeliveryInspect => object_schema(
            &["expected_session_version", "run_id"],
            [
                ("expected_session_version", uint_schema()),
                ("run_id", id_schema()),
            ],
        ),
        ActionName::SessionPlanReplace if github_pr => object_schema(
            &[
                "expected_session_version",
                "developer_adapter",
                "reviewer_adapters",
                "github_inspection_id",
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
                    "reviewer_adapters",
                    reviewer_adapter_schema(reviewer_adapters),
                ),
                (
                    "github_inspection_id",
                    json!({
                        "type":"string",
                        "minLength":1,
                        "maxLength":128,
                        "pattern":"^[A-Za-z0-9_.:-]+$",
                        "description":"Exact newest inspection_id returned for this run/version by session_github_delivery_inspect (or the initial startup inspection)."
                    }),
                ),
                (
                    "tasks",
                    json!({
                        "type":"array",
                        "description":"One through 64 ordered task bindings. hcom's typed protocol enforces this range before approval or worker start.",
                        "minItems":1,
                        "maxItems":64,
                        "items":task_schema(minimum_review_rounds(reviewer_adapters.len()).expect("validated active Reviewer topology"))
                    }),
                ),
            ],
        ),
        ActionName::SessionPlanReplace => object_schema(
            &[
                "expected_session_version",
                "developer_adapter",
                "reviewer_adapters",
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
                    "reviewer_adapters",
                    reviewer_adapter_schema(reviewer_adapters),
                ),
                (
                    "tasks",
                    json!({
                        "type":"array",
                        "description":"One through 64 ordered task bindings. hcom's typed protocol enforces this range before approval or worker start.",
                        "minItems":1,
                        "maxItems":64,
                        "items":task_schema(minimum_review_rounds(reviewer_adapters.len()).expect("validated active Reviewer topology"))
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
                        "description":approval_confirmation_description(github_pr)
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
                (
                    "human_decision_confirmed",
                    json!({
                        "type":"boolean",
                        "description":clarification_confirmation_description(github_pr)
                    }),
                ),
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
                (
                    "limit",
                    json!({
                        "type":"integer",
                        "description":"Page size from 1 through 8. hcom's typed protocol enforces this range.",
                        "minimum":1,
                        "maximum":8
                    }),
                ),
            ],
        ),
        ActionName::SessionWait => object_schema(
            &["run_id", "after_session_version", "after_progress_sequence"],
            [
                ("run_id", id_schema()),
                ("after_session_version", uint_schema()),
                ("after_progress_sequence", uint32_schema()),
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

fn approval_confirmation_description(github_pr: bool) -> &'static str {
    if github_pr {
        "Attest that the human explicitly authorized execution by approving this exact displayed draft, by directing the Architect to follow/implement/execute a named existing detailed plan/specification/current_todo, or by directing the Architect to plan/define the solution and then implement/proceed/finish/drive the requested work. A faithful derived plan may use the last form in the same turn when it adds no unresolved material decision. GitHub-lane execution includes one new signed-off commit per Developer turn, append-only correction and later-task history, and the bounded one-PR push/review/check workflow. Only an exact protected_auto_merge plan also authorizes its ruleset-attested exact-head merge and generated-ref finalization; manual policy never authorizes merge or deletion. Neither policy authorizes install/release/deployment or unrelated mutation."
    } else {
        "Attest that the human explicitly authorized execution by approving this exact displayed draft, by directing the Architect to follow/implement/execute a named existing detailed plan/specification/current_todo, or by directing the Architect to plan/define the solution and then implement/proceed/finish/drive the requested work. A faithful derived plan may use the last form in the same turn when it adds no unresolved material decision. Standard-lane execution includes one local signed-off candidate commit per task and same-commit amendments, but not push/install/release."
    }
}

fn clarification_confirmation_description(github_pr: bool) -> &'static str {
    if github_pr {
        "False only for a permitted Architect-derived clarification. A conflict between an explicit no-commit/no-push instruction and the GitHub lane's required append-only commit/publication topology must first use session_clarification_require_human, then submit true only after the human actually resolves it."
    } else {
        "False only for a permitted Architect-derived clarification. A conflict between an explicit no-commit instruction and the standard lane's required candidate commit must first use session_clarification_require_human, then submit true only after the human actually resolves it."
    }
}

fn task_schema(minimum_review_rounds: u8) -> Value {
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
                json!({
                    "type":"integer",
                    "description":format!(
                        "Completed review-generation budget from {minimum_review_rounds} through 20 for the active Reviewer topology. hcom's typed protocol enforces this range before approval or worker start."
                    ),
                    "minimum":minimum_review_rounds,
                    "maximum":20
                }),
            ),
            (
                "max_clarification_rounds",
                json!({
                    "type":"integer",
                    "description":"Architect-autonomous clarification budget from 1 through 20. hcom's typed protocol enforces this range before approval or worker start.",
                    "minimum":1,
                    "maximum":20
                }),
            ),
        ],
    )
}

fn reviewer_adapter_schema(reviewer_adapters: &[ReviewerAdapterBinding]) -> Value {
    // Keep exact binding enforcement in control_action instead of expressing the
    // entire object array as const. Codex normalizes const to enum and supplies
    // a default string items schema when an array omits items, which makes that
    // complex enum impossible before the model can call the tool.
    let reviewer_ids: BTreeSet<_> = reviewer_adapters
        .iter()
        .map(|binding| binding.reviewer_id)
        .collect();
    let adapters: BTreeSet<_> = reviewer_adapters
        .iter()
        .map(|binding| binding.adapter.as_str())
        .collect();
    let exact_bindings =
        serde_json::to_string(reviewer_adapters).expect("Reviewer adapter bindings serialize");
    json!({
        "type":"array",
        "description":format!(
            "Must exactly equal the ordered active Reviewer adapter bindings loaded for this run: {exact_bindings}"
        ),
        "minItems":reviewer_adapters.len(),
        "maxItems":reviewer_adapters.len(),
        "uniqueItems":true,
        "items":object_schema(
            &["reviewer_id", "adapter"],
            [
                (
                    "reviewer_id",
                    json!({"type":"string","enum":reviewer_ids}),
                ),
                (
                    "adapter",
                    json!({"type":"string","enum":adapters}),
                ),
            ],
        ),
    })
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
    use crate::worker::profile::ReviewerId;
    use std::collections::BTreeSet;

    fn reviewer_adapters(reviewer1: &str, reviewer2: &str) -> Vec<ReviewerAdapterBinding> {
        vec![
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer1,
                adapter: reviewer1.into(),
            },
            ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer2,
                adapter: reviewer2.into(),
            },
        ]
    }

    // Codex 0.145/0.146 deserializes MCP schemas into a smaller internal
    // representation before sending Responses API tools. This fixture models
    // the transformations relevant to hcom's deliberately narrow schema
    // policy: scalar const becomes enum and guidance-only range/pattern
    // keywords disappear. It is a compatibility oracle for generated schemas,
    // not production argument parsing.
    fn codex_0145_0146_projection(schema: &Value) -> Value {
        let source = schema.as_object().unwrap();
        let mut projected = Map::new();
        for preserved in [
            "additionalProperties",
            "description",
            "enum",
            "required",
            "type",
        ] {
            if let Some(value) = source.get(preserved) {
                projected.insert(preserved.into(), value.clone());
            }
        }
        if let Some(value) = source.get("const") {
            projected.insert("enum".into(), json!([value]));
        }
        if let Some(items) = source.get("items") {
            projected.insert("items".into(), codex_0145_0146_projection(items));
        } else if source.get("type") == Some(&Value::String("array".into())) {
            projected.insert("items".into(), json!({"type":"string"}));
        }
        if let Some(properties) = source.get("properties").and_then(Value::as_object) {
            projected.insert(
                "properties".into(),
                Value::Object(
                    properties
                        .iter()
                        .map(|(name, property)| {
                            (name.clone(), codex_0145_0146_projection(property))
                        })
                        .collect(),
                ),
            );
        }
        Value::Object(projected)
    }

    #[test]
    fn tool_inventory_is_exact_and_contains_no_project_or_generic_authority() {
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
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
    fn github_tool_inventory_requires_inspection_without_changing_local_schema() {
        let reviewers = vec![ReviewerAdapterBinding {
            reviewer_id: ReviewerId::Reviewer1,
            adapter: "codex-reviewer".into(),
        }];
        let local = tool_definitions("codex-developer", &reviewers);
        let github = tool_definitions_for_delivery("codex-developer", &reviewers, true);
        validate_codex_tool_definitions(&github).unwrap();
        for tool in &github {
            validate_codex_schema_node(
                &codex_0145_0146_projection(&tool["inputSchema"]),
                &format!("{}.github_projected", tool["name"].as_str().unwrap()),
            )
            .unwrap();
        }

        assert!(
            local
                .iter()
                .all(|tool| tool["name"] != "session_github_delivery_inspect")
        );
        assert!(
            github
                .iter()
                .any(|tool| tool["name"] == "session_github_delivery_inspect")
        );
        let local_plan = local
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        let github_plan = github
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        assert!(
            local_plan["inputSchema"]["properties"]
                .get("github_inspection_id")
                .is_none()
        );
        assert_eq!(
            github_plan["inputSchema"]["properties"]["github_inspection_id"]["type"],
            "string"
        );
        assert!(
            github_plan["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("github_inspection_id"))
        );
        let github_instructions = architect_instructions_for_delivery(true);
        assert!(github_instructions.contains("session_github_delivery_inspect"));
        assert!(
            github_instructions
                .contains("every Developer turn to create exactly one new signed-off child commit")
        );
        assert!(
            github_instructions.contains("Corrections and later-task initial turns append commits")
        );
        assert!(
            github_instructions
                .contains("A later task's initial turn still appends its own required commit")
        );
        for required in [
            "published byte-for-byte without redaction or secret scanning",
            "60 KiB UTF-8 hard cap",
            "every task base..final-head range",
            "review_complete_unmerged",
            "unmerged_review_exhausted",
            "confirmed merge whose required finalization failed",
            "Never retry or imply retry of a confirmed merge",
            "never imply that a fresh run adopts a preserved PR/worktree",
        ] {
            assert!(
                github_instructions.contains(required),
                "GitHub Architect handoff omitted {required}"
            );
        }
        assert!(!github_instructions.contains("to amend only that same task commit"));
        assert!(!github_instructions.contains("absence of any separately authorized push"));
        assert_eq!(
            architect_instructions_for_delivery(false),
            ARCHITECT_INSTRUCTIONS
        );
        let github_plan_description = github_plan["description"].as_str().unwrap();
        assert!(github_plan_description.contains("Every Developer turn"));
        assert!(github_plan_description.contains("never amend"));
        let github_approve = github
            .iter()
            .find(|tool| tool["name"] == "session_approve_and_start")
            .unwrap();
        let github_attestation = github_approve["inputSchema"]["properties"]["approval_confirmed"]
            ["description"]
            .as_str()
            .unwrap();
        assert!(github_attestation.contains("one new signed-off commit per Developer turn"));
        assert!(github_attestation.contains("bounded one-PR push"));
        assert!(!github_attestation.contains("same-commit amendments"));
        let github_wait = github
            .iter()
            .find(|tool| tool["name"] == "session_wait")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        for event in [
            "candidate_published",
            "review_requested",
            "review_responded",
            "merge_waiting",
            "run_finalizing",
        ] {
            assert!(github_wait.contains(event));
        }
        for required in [
            "direct MCP tool directly",
            "never wrap it in functions.exec/functions.wait",
            "never completes this call or wakes the Architect",
            "manual or protected auto-merge delivery",
            "terminal snapshot supersedes queued progress",
            "one terminal wakeup rather than several",
        ] {
            assert!(
                github_wait.contains(required),
                "GitHub wait contract omitted {required}"
            );
        }
        assert!(!github_wait.contains("Queued progress must drain before terminal"));

        let dual_reviewers = reviewer_adapters("codex-reviewer", "claude-reviewer");
        for github_pr in [false, true] {
            let single_wait =
                tool_definitions_for_delivery("codex-developer", &reviewers, github_pr)
                    .into_iter()
                    .find(|tool| tool["name"] == "session_wait")
                    .unwrap();
            let dual_wait =
                tool_definitions_for_delivery("codex-developer", &dual_reviewers, github_pr)
                    .into_iter()
                    .find(|tool| tool["name"] == "session_wait")
                    .unwrap();
            assert_eq!(
                single_wait["description"], dual_wait["description"],
                "wait wakeup semantics must not vary with single/dual review topology"
            );
        }
        let github_status = github
            .iter()
            .find(|tool| tool["name"] == "session_status")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(github_status.contains("complete frozen delivery binding"));
        assert!(github_status.contains("hcom/review Check"));

        let inspection = control_action_for_delivery(
            "session_github_delivery_inspect",
            json!({"expected_session_version":3,"run_id":"run-one"}),
            "codex-developer",
            &reviewers,
            true,
        )
        .unwrap();
        assert!(matches!(
            inspection,
            ControlAction::SessionGitHubDeliveryInspect {
                expected_session_version: 3,
                ref run_id,
            } if run_id == "run-one"
        ));
        let github_plan_arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer",
            "reviewer_adapters":[{"reviewer_id":"reviewer1","adapter":"codex-reviewer"}],
            "github_inspection_id":"inspection-one",
            "tasks":[{
                "task_key":"one",
                "title":"one",
                "repository_root":"/source/example",
                "task_document_path":"/project/task.md",
                "design_document_paths":[],
                "task_selector":"one",
                "max_review_rounds":5,
                "max_clarification_rounds":1
            }]
        });
        let action = control_action_for_delivery(
            "session_plan_replace",
            github_plan_arguments.clone(),
            "codex-developer",
            &reviewers,
            true,
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionPlanReplace {
                github_inspection_id: Some(ref inspection_id),
                ..
            } if inspection_id == "inspection-one"
        ));
        assert!(
            control_action_for_delivery(
                "session_plan_replace",
                github_plan_arguments,
                "codex-developer",
                &reviewers,
                false,
            )
            .is_err()
        );
        assert!(
            control_action_for_delivery(
                "session_github_delivery_inspect",
                json!({"expected_session_version":3,"run_id":"run-one"}),
                "codex-developer",
                &reviewers,
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn generated_tool_schemas_stay_inside_the_codex_compatibility_policy() {
        for reviewers in [
            vec![ReviewerAdapterBinding {
                reviewer_id: ReviewerId::Reviewer1,
                adapter: "codex-reviewer".into(),
            }],
            reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        ] {
            for github_pr in [false, true] {
                let tools = tool_definitions_for_delivery("codex-developer", &reviewers, github_pr);
                validate_codex_tool_definitions(&tools).unwrap();
                for tool in &tools {
                    let projected = codex_0145_0146_projection(&tool["inputSchema"]);
                    validate_codex_schema_node(
                        &projected,
                        &format!(
                            "{}.{}.projected",
                            tool["name"].as_str().unwrap(),
                            if github_pr { "github" } else { "local" }
                        ),
                    )
                    .unwrap();
                }

                let plan = tools
                    .iter()
                    .find(|tool| tool["name"] == "session_plan_replace")
                    .unwrap();
                let projected_plan = codex_0145_0146_projection(&plan["inputSchema"]);
                let reviewer_schema = &projected_plan["properties"]["reviewer_adapters"];
                assert_eq!(reviewer_schema["type"], "array");
                assert_eq!(reviewer_schema["items"]["type"], "object");
                assert!(reviewer_schema.get("enum").is_none());
                let rounds = &projected_plan["properties"]["tasks"]["items"]["properties"]["max_review_rounds"];
                assert!(rounds.get("minimum").is_none());
                assert!(rounds.get("maximum").is_none());
                assert!(
                    rounds["description"]
                        .as_str()
                        .unwrap()
                        .contains("hcom's typed protocol enforces this range")
                );
                assert_eq!(
                    projected_plan["properties"]
                        .get("github_inspection_id")
                        .is_some(),
                    github_pr
                );

                let approve = tools
                    .iter()
                    .find(|tool| tool["name"] == "session_approve_and_start")
                    .unwrap();
                let projected_approve = codex_0145_0146_projection(&approve["inputSchema"]);
                assert_eq!(
                    projected_approve["properties"]["approval_confirmed"]["enum"],
                    json!([true])
                );
            }
        }
    }

    #[test]
    fn codex_schema_policy_rejects_lossy_or_ambiguous_shapes_before_launch() {
        let mut tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        let plan = tools
            .iter_mut()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        plan["inputSchema"]["properties"]["reviewer_adapters"] = json!({
            "type":"array",
            "const":[
                {"reviewer_id":"reviewer1","adapter":"codex-reviewer"},
                {"reviewer_id":"reviewer2","adapter":"claude-reviewer-2.1.220"}
            ]
        });
        let error = validate_codex_tool_definitions(&tools)
            .unwrap_err()
            .to_string();
        assert!(error.contains("complex array/object constraints are forbidden"));

        let mut tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        let plan = tools
            .iter_mut()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        plan["inputSchema"]["properties"]["reviewer_adapters"]
            .as_object_mut()
            .unwrap()
            .remove("items");
        let error = validate_codex_tool_definitions(&tools)
            .unwrap_err()
            .to_string();
        assert!(error.contains("array schema must define items"));

        let mut tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        tools[0]["inputSchema"]["required"] = json!([]);
        let error = validate_codex_tool_definitions(&tools)
            .unwrap_err()
            .to_string();
        assert!(error.contains("required must contain every property exactly once"));

        let mut tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        tools[0]["inputSchema"]["anyOf"] = json!([]);
        let error = validate_codex_tool_definitions(&tools)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported Codex schema keyword anyOf"));

        let mut tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        tools[0]["inputSchema"]["description"] =
            Value::String("x".repeat(MAX_CODEX_INPUT_SCHEMA_BYTES));
        let error = validate_codex_tool_definitions(&tools)
            .unwrap_err()
            .to_string();
        assert!(error.contains("local compatibility limit"));
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
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
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
                &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
            )
            .is_err()
        );
        assert!(
            control_action(
                "shell",
                json!({}),
                "codex-developer",
                &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
            )
            .is_err()
        );
    }

    #[test]
    fn next_run_tool_preserves_terminal_evidence_and_does_not_start_work() {
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "codex-reviewer"),
        );
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
            &reviewer_adapters("codex-reviewer", "codex-reviewer"),
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
            "reviewer_adapters":[
                {"reviewer_id":"reviewer1","adapter":"codex-reviewer"},
                {"reviewer_id":"reviewer2","adapter":"claude-reviewer-2.1.220"}
            ],
            "tasks":[{
                "task_key":"p9-task-1",
                "title":"Phase 9 Task 1",
                "repository_root":"/source/example",
                "task_document_path":"/project/current_todo.md",
                "design_document_paths":["/project/architecture.md","/project/design.md"],
                "task_selector":"FBTC-01",
                "max_review_rounds":7,
                "max_clarification_rounds":2
            }]
        });
        let action = control_action(
            "session_plan_replace",
            arguments.clone(),
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
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
                    &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
                )
                .is_err(),
                "legacy inline field {inline_field} was accepted"
            );
        }

        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
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
        let expected_reviewers = reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220");
        let tools = tool_definitions("codex-developer", &expected_reviewers);
        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        let reviewer_schema = &plan["inputSchema"]["properties"]["reviewer_adapters"];
        assert!(reviewer_schema.get("const").is_none());
        assert_eq!(reviewer_schema["minItems"], 2);
        assert_eq!(reviewer_schema["maxItems"], 2);
        assert_eq!(reviewer_schema["uniqueItems"], true);
        assert_eq!(reviewer_schema["items"]["type"], "object");
        assert_eq!(reviewer_schema["items"]["additionalProperties"], false);
        assert_eq!(
            reviewer_schema["items"]["required"],
            json!(["reviewer_id", "adapter"])
        );
        assert_eq!(
            reviewer_schema["items"]["properties"]["reviewer_id"]["enum"],
            json!(["reviewer1", "reviewer2"])
        );
        assert_eq!(
            reviewer_schema["items"]["properties"]["adapter"]["enum"],
            json!(["claude-reviewer-2.1.220", "codex-reviewer"])
        );
        assert!(
            reviewer_schema["description"]
                .as_str()
                .unwrap()
                .contains(&serde_json::to_string(&expected_reviewers).unwrap())
        );
        let arguments = json!({
            "expected_session_version":0,
            "developer_adapter":"codex-developer",
            "reviewer_adapters":[
                {"reviewer_id":"reviewer1","adapter":"claude-reviewer-2.1.220"},
                {"reviewer_id":"reviewer2","adapter":"codex-reviewer"}
            ],
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
                &expected_reviewers,
            )
            .is_err()
        );
    }

    #[test]
    fn plan_schema_uses_single_review_minimum_for_one_reviewer_binding() {
        let reviewers = vec![ReviewerAdapterBinding {
            reviewer_id: ReviewerId::Reviewer1,
            adapter: "codex-reviewer".into(),
        }];
        let tools = tool_definitions("codex-developer", &reviewers);
        let plan = tools
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        let reviewer_schema = &plan["inputSchema"]["properties"]["reviewer_adapters"];
        assert!(reviewer_schema.get("const").is_none());
        assert_eq!(reviewer_schema["minItems"], 1);
        assert_eq!(reviewer_schema["maxItems"], 1);
        assert_eq!(
            reviewer_schema["items"]["properties"]["reviewer_id"]["enum"],
            json!(["reviewer1"])
        );
        assert_eq!(
            plan["inputSchema"]["properties"]["tasks"]["items"]["properties"]["max_review_rounds"]
                ["minimum"],
            5
        );

        let dual = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
        let dual_plan = dual
            .iter()
            .find(|tool| tool["name"] == "session_plan_replace")
            .unwrap();
        assert_eq!(
            dual_plan["inputSchema"]["properties"]["tasks"]["items"]["properties"]["max_review_rounds"]
                ["minimum"],
            7
        );
    }

    #[test]
    fn execution_authorization_contract_supports_exact_named_or_prospective_start() {
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
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
        assert!(plan.contains("先明确技术方案，然后 drive 开发完成"));
        assert!(plan.contains("Do not ask again"));
        assert!(plan.contains("local candidate-commit topology"));
        assert!(plan.contains("no extra commit after LGTM"));
        assert!(plan.contains("never push, install, or release"));
        assert!(plan.contains("synchronized active-Reviewer generation budget"));
        assert!(plan.contains("every ordered active Reviewer binding"));

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
            "plan or define the solution",
            "prospective authorization",
            "duplicate confirmation",
            "local Developer candidate commit",
            "new commit after LGTM",
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
        let attestation =
            approve_tool["inputSchema"]["properties"]["approval_confirmed"]["description"]
                .as_str()
                .unwrap();
        assert!(attestation.contains("plan/define the solution"));
        assert!(attestation.contains("same-commit amendments"));
        assert!(attestation.contains("not push/install/release"));
        assert!(approve.contains("standing alone they do not authorize starting"));
    }

    #[test]
    fn architect_tool_descriptions_never_promise_git_inspection() {
        // hcom takes only the task's source directory path. Any description
        // that still promised branch/revision evidence or a drift check would
        // make the Architect report guarantees the supervisor cannot give.
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
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
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        );
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
        assert!(wait.contains("direct MCP tool directly"));
        assert!(wait.contains("never wrap it in functions.exec/functions.wait"));
        assert!(wait.contains("heartbeat"));
        assert!(wait.contains("pending_architect_action"));
        assert!(wait.contains("published_version"));
        assert!(wait.contains("older version in the same run re-delivers"));
        assert!(wait.contains("repeated wait"));
        assert!(wait.contains("review_requested"));
        assert!(wait.contains("review_responded"));
        assert!(wait.contains("Internal task_completed bookkeeping"));
        assert!(wait.contains("never complete this call or wake the Architect"));
        assert!(wait.contains("normal Developer/Reviewer result"));
        assert!(wait.contains("terminal snapshot supersedes queued progress"));
        assert!(wait.contains("one terminal wakeup rather than several"));
        assert!(wait.contains("abnormal terminal transition also returns immediately"));
        assert!(wait.contains("review_generation"));
        assert!(wait.contains("reviewer_id"));
        assert!(wait.contains("responses_received/responses_expected"));
        assert!(wait.contains("fewer responses have arrived than are expected"));
        assert!(wait.contains("another response is pending"));
        assert!(wait.contains("every active Reviewer's listed current-generation final files"));
        assert!(wait.contains("every active same-generation Reviewer returns LGTM"));
        assert!(wait.contains("developer_final_path"));
        assert!(wait.contains("reviewer_final_message_path"));
        assert!(wait.contains("worker-result events produced before the next wait are retained"));
        assert!(wait.contains("may skip internal bookkeeping events"));
        assert!(wait.contains("never cancels the run"));
        assert!(wait.contains("immediately wait again"));
        assert!(wait.contains("end the turn without waiting"));
        assert!(wait.contains("session_clarifications_list"));
        assert_eq!(
            wait_tool["inputSchema"]["required"],
            json!(["run_id", "after_session_version", "after_progress_sequence"])
        );
        assert!(status.contains("only when the human asks"));
        assert!(status.contains("not a keepalive"));
        assert!(status.contains("pending Architect action"));
        assert!(status.contains("clarification record counts"));
        assert!(status.contains("active_workers"));
        assert!(status.contains("session-level ordered Reviewer bindings"));
        assert!(status.contains("current-generation typed Reviewer results"));
        assert!(status.contains("review_round"));
        assert!(status.contains("review_generation"));
        for description in [approve, wait, status] {
            assert!(!description.contains("180 to 300 seconds"));
            assert!(!description.contains("30-second cadence"));
        }

        let action = control_action(
            "session_wait",
            json!({
                "run_id":"run-one",
                "after_session_version":7,
                "after_progress_sequence":3
            }),
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "claude-reviewer-2.1.220"),
        )
        .unwrap();
        assert!(matches!(
            action,
            ControlAction::SessionWait {
                ref run_id,
                after_session_version: 7,
                after_progress_sequence: 3
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
            "plan or define the solution and then implement",
            "do not ask the human to repeat the authorization",
            "standing alone, they do not authorize starting",
            "exactly one signed-off local candidate commit",
            "does not authorize push, install, release",
            "never an Architect-derivable clarification",
            "session_clarification_require_human",
            "do not ask whether to retain or revert it",
            "clarification_output_path",
            "does not interpret",
            "fresh empty run",
            "strict synchronous generation",
            "review_round",
            "review_generation",
            "responses_received",
            "responses_received` is less than `responses_expected",
            "Only after terminal",
            "every active Reviewer returned LGTM",
            "direct MCP tools",
            "never wrap `session_wait`",
            "consumes no Architect model turn",
            "Internal task-completion bookkeeping",
            "transport yields never release the wait",
            "final Reviewer result and derived task completion cause only one wakeup",
        ] {
            assert!(
                ARCHITECT_INSTRUCTIONS.contains(required),
                "Architect instructions omitted {required}"
            );
        }
        let tools = tool_definitions(
            "codex-developer",
            &reviewer_adapters("codex-reviewer", "codex-reviewer"),
        );
        let submit = tools
            .iter()
            .find(|tool| tool["name"] == "session_clarification_submit")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(submit.contains("exact clarification_document_path"));
        assert!(submit.contains("human_decision_confirmed=false"));
        assert!(submit.contains("Never use false"));
        assert!(submit.contains("explicit no-commit instruction"));
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
        assert!(require_human.contains("This call is mandatory"));
        assert!(require_human.contains("regardless of remaining autonomous budget"));
        assert!(require_human.contains("Do not call session_wait"));
        assert!(require_human.contains("foreground run is only in memory"));
        assert!(require_human.contains("do not invent skip"));
        assert!(require_human.contains("returned session.run_id"));
        assert!(require_human.contains("returned session.version"));

        let wait = tools
            .iter()
            .find(|tool| tool["name"] == "session_wait")
            .unwrap()["description"]
            .as_str()
            .unwrap();
        assert!(wait.contains("do not answer autonomously even when clarification budget remains"));

        let human_attestation = tools
            .iter()
            .find(|tool| tool["name"] == "session_clarification_submit")
            .unwrap()["inputSchema"]["properties"]["human_decision_confirmed"]["description"]
            .as_str()
            .unwrap();
        assert!(human_attestation.contains("must first use session_clarification_require_human"));

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
