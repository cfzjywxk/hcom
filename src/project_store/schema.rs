pub(crate) const APPLICATION_ID: i32 = 0x4843_5031; // "HCP1"
pub(crate) const SCHEMA_VERSION: i32 = 1;
pub(crate) const PRODUCT_ID: &str = "hcom-project-control";
pub(crate) const COMPONENT_VERSION: &str =
    concat!("hcom/", env!("CARGO_PKG_VERSION"), ";store=1;control=1");

pub(crate) const SCHEMA_SQL: &str = r#"
CREATE TABLE store_meta (
    singleton             INTEGER PRIMARY KEY CHECK (singleton = 1),
    product_id            TEXT NOT NULL CHECK (product_id = 'hcom-project-control'),
    schema_version        INTEGER NOT NULL CHECK (schema_version = 1),
    installation_id       TEXT NOT NULL UNIQUE,
    created_by_version    TEXT NOT NULL,
    schema_digest         TEXT NOT NULL,
    created_at            INTEGER NOT NULL,
    CHECK (length(CAST(installation_id AS BLOB)) BETWEEN 16 AND 128),
    CHECK (length(CAST(created_by_version AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(schema_digest AS BLOB)) = 64),
    CHECK (created_at > 0)
);

CREATE TABLE daemon_epochs (
    id                 TEXT PRIMARY KEY,
    boot_id            TEXT NOT NULL,
    daemon_pid         INTEGER NOT NULL CHECK (daemon_pid > 1),
    process_birth      TEXT NOT NULL,
    state              TEXT NOT NULL CHECK (state IN ('active', 'retired', 'crashed')),
    started_at         INTEGER NOT NULL,
    stopped_at         INTEGER,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(boot_id AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(process_birth AS BLOB)) BETWEEN 1 AND 256),
    CHECK ((state = 'active' AND stopped_at IS NULL)
           OR (state IN ('retired', 'crashed') AND stopped_at IS NOT NULL))
);

CREATE UNIQUE INDEX daemon_epochs_one_active
    ON daemon_epochs(state) WHERE state = 'active';

CREATE TABLE project_runs (
    id                       TEXT PRIMARY KEY,
    state                    TEXT NOT NULL CHECK (state IN (
                                 'draft', 'needs_approval', 'approved', 'running',
                                 'paused', 'needs_input', 'replanning', 'completed',
                                 'failed', 'needs_recovery', 'canceled'
                             )),
    version                  INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    pause_reason             TEXT,
    source_repo_root         TEXT NOT NULL,
    source_git_dir_identity  TEXT NOT NULL,
    target_ref               TEXT NOT NULL,
    target_expected_sha      TEXT NOT NULL,
    worktree_root            TEXT NOT NULL,
    worktree_branch          TEXT NOT NULL,
    checkpoint_sha           TEXT NOT NULL,
    applied_target_sha       TEXT,
    approved_plan_version    INTEGER,
    approved_plan_hash       TEXT,
    run_requested_at         INTEGER,
    active_daemon_epoch      TEXT,
    created_at               INTEGER NOT NULL,
    updated_at               INTEGER NOT NULL,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (substr(source_repo_root, 1, 1) = '/'
           AND length(CAST(source_repo_root AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (length(CAST(source_git_dir_identity AS BLOB)) BETWEEN 1 AND 1024),
    CHECK (substr(target_ref, 1, 5) = 'refs/'
           AND length(CAST(target_ref AS BLOB)) BETWEEN 6 AND 1024),
    CHECK (length(target_expected_sha) IN (40, 64)
           AND target_expected_sha NOT GLOB '*[^0-9a-f]*'),
    CHECK (substr(worktree_root, 1, 1) = '/'
           AND length(CAST(worktree_root AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (substr(worktree_branch, 1, 11) = 'refs/heads/'
           AND length(CAST(worktree_branch AS BLOB)) BETWEEN 12 AND 1024),
    CHECK (length(checkpoint_sha) IN (40, 64)
           AND checkpoint_sha NOT GLOB '*[^0-9a-f]*'),
    CHECK (applied_target_sha IS NULL OR
           (length(applied_target_sha) IN (40, 64)
            AND applied_target_sha NOT GLOB '*[^0-9a-f]*')),
    CHECK ((approved_plan_version IS NULL) = (approved_plan_hash IS NULL)),
    CHECK (approved_plan_version IS NULL OR approved_plan_version > 0),
    CHECK (approved_plan_hash IS NULL OR
           (length(approved_plan_hash) = 64
            AND approved_plan_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK (pause_reason IS NULL OR
           length(CAST(pause_reason AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (active_daemon_epoch IS NULL OR
           length(CAST(active_daemon_epoch AS BLOB)) BETWEEN 1 AND 128),
    CHECK (updated_at >= created_at)
);

CREATE TABLE worker_profiles (
    id                    TEXT PRIMARY KEY,
    project_id            TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    role                  TEXT NOT NULL CHECK (role IN ('developer', 'reviewer')),
    adapter               TEXT NOT NULL,
    model                 TEXT NOT NULL,
    reasoning             TEXT NOT NULL,
    policy                TEXT NOT NULL,
    cli_path              TEXT NOT NULL,
    executable_identity_json TEXT NOT NULL CHECK (
                              json_valid(executable_identity_json)
                              AND json_type(executable_identity_json) = 'object'
                          ),
    cli_version           TEXT NOT NULL,
    adapter_contract_ver  INTEGER NOT NULL CHECK (adapter_contract_ver > 0),
    native_session_mode   TEXT NOT NULL CHECK (
                              native_session_mode IN ('preassigned', 'discovered')
                          ),
    capability_json       TEXT NOT NULL CHECK (
                              json_valid(capability_json)
                              AND json_type(capability_json) = 'object'
                          ),
    created_at            INTEGER NOT NULL,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(adapter AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(model AS BLOB)) BETWEEN 1 AND 256),
    CHECK (length(CAST(reasoning AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(policy AS BLOB)) BETWEEN 1 AND 2048),
    CHECK (substr(cli_path, 1, 1) = '/'
           AND length(CAST(cli_path AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (length(CAST(executable_identity_json AS BLOB)) BETWEEN 2 AND 65536),
    CHECK (length(CAST(cli_version AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(capability_json AS BLOB)) BETWEEN 2 AND 65536),
    UNIQUE (project_id, id)
);

CREATE TABLE execution_environment_leases (
    project_id           TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    lease_id             TEXT NOT NULL,
    daemon_epoch         TEXT NOT NULL REFERENCES daemon_epochs(id) ON DELETE RESTRICT,
    environment_hash     TEXT NOT NULL,
    inherited_names_json TEXT NOT NULL CHECK (
                             json_valid(inherited_names_json)
                             AND json_type(inherited_names_json) = 'array'
                         ),
    required_names_json  TEXT NOT NULL CHECK (
                             json_valid(required_names_json)
                             AND json_type(required_names_json) = 'array'
                         ),
    state                TEXT NOT NULL CHECK (state IN ('active', 'lost')),
    created_at           INTEGER NOT NULL,
    lost_at              INTEGER,
    PRIMARY KEY (project_id, lease_id),
    CHECK (length(CAST(lease_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(environment_hash) = 64
           AND environment_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(CAST(inherited_names_json AS BLOB)) BETWEEN 2 AND 8192),
    CHECK (length(CAST(required_names_json AS BLOB)) BETWEEN 2 AND 8192),
    CHECK ((state = 'active' AND lost_at IS NULL)
           OR (state = 'lost' AND lost_at IS NOT NULL))
);

CREATE UNIQUE INDEX execution_environment_one_active
    ON execution_environment_leases(project_id) WHERE state = 'active';

CREATE TABLE project_plans (
    id                         TEXT PRIMARY KEY,
    project_id                 TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    version                    INTEGER NOT NULL CHECK (version > 0),
    state                      TEXT NOT NULL CHECK (
                                   state IN ('draft', 'approved', 'superseded')
                               ),
    base_checkpoint_sha        TEXT NOT NULL,
    plan_hash                  TEXT NOT NULL,
    developer_profile_id       TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE RESTRICT,
    reviewer_profile_id        TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE RESTRICT,
    automatic_through_ordinal  INTEGER CHECK (automatic_through_ordinal >= 0),
    created_by_binding         TEXT REFERENCES architect_bindings(id) ON DELETE RESTRICT,
    created_at                 INTEGER NOT NULL,
    approved_at                INTEGER,
    superseded_at              INTEGER,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(base_checkpoint_sha) IN (40, 64)
           AND base_checkpoint_sha NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(plan_hash) = 64 AND plan_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK ((state = 'draft' AND approved_at IS NULL AND superseded_at IS NULL)
           OR (state = 'approved' AND approved_at IS NOT NULL AND superseded_at IS NULL)
           OR (state = 'superseded' AND superseded_at IS NOT NULL)),
    UNIQUE (project_id, version),
    UNIQUE (project_id, plan_hash),
    UNIQUE (id, project_id)
);

CREATE UNIQUE INDEX project_plans_one_draft
    ON project_plans(project_id) WHERE state = 'draft';
CREATE UNIQUE INDEX project_plans_one_approved
    ON project_plans(project_id) WHERE state = 'approved';

CREATE TABLE project_tasks (
    id                    TEXT PRIMARY KEY,
    project_id            TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    plan_id               TEXT NOT NULL REFERENCES project_plans(id) ON DELETE CASCADE,
    task_key              TEXT NOT NULL,
    ordinal               INTEGER NOT NULL CHECK (ordinal >= 0),
    spec_json             TEXT NOT NULL CHECK (
                              json_valid(spec_json) AND json_type(spec_json) = 'object'
                          ),
    spec_hash             TEXT NOT NULL,
    state                 TEXT NOT NULL CHECK (state IN (
                              'draft', 'queued', 'developing', 'awaiting_review',
                              'changes_requested', 'finalizing', 'completed',
                              'needs_input', 'failed', 'indeterminate', 'stale',
                              'superseded', 'canceled'
                          )),
    version               INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    base_revision         TEXT,
    head_revision         TEXT,
    review_round          INTEGER NOT NULL DEFAULT 0 CHECK (review_round >= 0),
    max_review_rounds     INTEGER NOT NULL CHECK (
                              max_review_rounds BETWEEN 1 AND 20
                          ),
    developer_session_id  TEXT REFERENCES worker_sessions(id) ON DELETE RESTRICT,
    reviewer_session_id   TEXT REFERENCES worker_sessions(id) ON DELETE RESTRICT,
    result_json           TEXT CHECK (
                              result_json IS NULL
                              OR (json_valid(result_json) AND json_type(result_json) = 'object')
                          ),
    result_hash           TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    completed_at          INTEGER,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(task_key AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(spec_json AS BLOB)) BETWEEN 2 AND 262144),
    CHECK (length(spec_hash) = 64 AND spec_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (base_revision IS NULL OR
           (length(base_revision) IN (40, 64)
            AND base_revision NOT GLOB '*[^0-9a-f]*')),
    CHECK (head_revision IS NULL OR
           (length(head_revision) IN (40, 64)
            AND head_revision NOT GLOB '*[^0-9a-f]*')),
    CHECK ((result_json IS NULL) = (result_hash IS NULL)),
    CHECK (result_hash IS NULL OR
           (length(result_hash) = 64 AND result_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK ((state = 'completed' AND completed_at IS NOT NULL)
           OR (state != 'completed' AND completed_at IS NULL)),
    CHECK (updated_at >= created_at),
    UNIQUE (plan_id, task_key),
    UNIQUE (plan_id, ordinal),
    UNIQUE (id, project_id, plan_id)
);

CREATE TABLE task_dependencies (
    task_id             TEXT NOT NULL REFERENCES project_tasks(id) ON DELETE CASCADE,
    depends_on_task_id  TEXT NOT NULL REFERENCES project_tasks(id) ON DELETE RESTRICT,
    PRIMARY KEY (task_id, depends_on_task_id),
    CHECK (task_id != depends_on_task_id)
);

CREATE TABLE worker_sessions (
    id                 TEXT PRIMARY KEY,
    project_id         TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    task_id            TEXT NOT NULL REFERENCES project_tasks(id) ON DELETE CASCADE,
    role               TEXT NOT NULL CHECK (role IN ('developer', 'reviewer')),
    profile_id         TEXT NOT NULL REFERENCES worker_profiles(id) ON DELETE RESTRICT,
    adapter            TEXT NOT NULL,
    native_session_id  TEXT,
    state              TEXT NOT NULL CHECK (
                           state IN ('creating', 'active', 'closed', 'failed', 'indeterminate')
                       ),
    created_at         INTEGER NOT NULL,
    closed_at          INTEGER,
    updated_at         INTEGER NOT NULL,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(adapter AS BLOB)) BETWEEN 1 AND 64),
    CHECK (native_session_id IS NULL OR
           length(CAST(native_session_id AS BLOB)) BETWEEN 1 AND 256),
    CHECK ((state = 'closed' AND closed_at IS NOT NULL)
           OR (state != 'closed' AND closed_at IS NULL)),
    CHECK (updated_at >= created_at),
    UNIQUE (task_id, role)
);

CREATE UNIQUE INDEX worker_sessions_native_exact
    ON worker_sessions(adapter, native_session_id)
    WHERE native_session_id IS NOT NULL;

CREATE TABLE worker_turns (
    id                  TEXT PRIMARY KEY,
    session_id          TEXT NOT NULL REFERENCES worker_sessions(id) ON DELETE CASCADE,
    sequence            INTEGER NOT NULL CHECK (sequence > 0),
    kind                TEXT NOT NULL CHECK (kind IN ('create', 'resume')),
    task_version        INTEGER NOT NULL CHECK (task_version >= 0),
    review_round        INTEGER NOT NULL CHECK (review_round >= 0),
    request_hash        TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN (
                            'queued', 'claimed', 'running', 'result_ready', 'applied',
                            'failed', 'indeterminate', 'stale', 'canceled'
                        )),
    attempt             INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_owner         TEXT,
    expires_at          INTEGER,
    worker_pid          INTEGER,
    process_birth       TEXT,
    progress_phase      TEXT NOT NULL CHECK (
                            progress_phase IN (
                                'queued', 'spawn', 'running', 'validating', 'applying', 'done'
                            )
                        ),
    last_progress_at    INTEGER,
    activity_truncated  INTEGER NOT NULL DEFAULT 0 CHECK (activity_truncated IN (0, 1)),
    artifact_dir        TEXT NOT NULL,
    review_snapshot_digest TEXT,
    result_json         TEXT CHECK (
                            result_json IS NULL
                            OR (json_valid(result_json) AND json_type(result_json) = 'object')
                        ),
    result_hash         TEXT,
    error_kind          TEXT,
    error_message       TEXT,
    created_at          INTEGER NOT NULL,
    started_at          INTEGER,
    result_at           INTEGER,
    applied_at          INTEGER,
    updated_at          INTEGER NOT NULL,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK ((lease_owner IS NULL) = (expires_at IS NULL)),
    CHECK (lease_owner IS NULL OR length(CAST(lease_owner AS BLOB)) BETWEEN 1 AND 128),
    CHECK ((worker_pid IS NULL) = (process_birth IS NULL)),
    CHECK (worker_pid IS NULL OR worker_pid > 1),
    CHECK (process_birth IS NULL OR
           length(CAST(process_birth AS BLOB)) BETWEEN 1 AND 256),
    CHECK (length(CAST(artifact_dir AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (substr(artifact_dir, 1, 1) != '/'),
    CHECK (instr('/' || artifact_dir || '/', '/../') = 0),
    CHECK (instr('/' || artifact_dir || '/', '/./') = 0),
    CHECK (review_snapshot_digest IS NULL OR
           (length(review_snapshot_digest) = 64
            AND review_snapshot_digest NOT GLOB '*[^0-9a-f]*')),
    CHECK ((result_json IS NULL) = (result_hash IS NULL)),
    CHECK (result_hash IS NULL OR
           (length(result_hash) = 64 AND result_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK (error_kind IS NULL OR length(CAST(error_kind AS BLOB)) BETWEEN 1 AND 64),
    CHECK (error_message IS NULL OR length(CAST(error_message AS BLOB)) <= 4096),
    CHECK (status NOT IN ('result_ready', 'applied')
           OR result_json IS NOT NULL),
    CHECK (status != 'queued'
           OR (lease_owner IS NULL AND worker_pid IS NULL
               AND result_json IS NULL AND progress_phase = 'queued')),
    CHECK (status != 'claimed'
           OR (attempt > 0 AND lease_owner IS NOT NULL AND worker_pid IS NULL
               AND result_json IS NULL AND progress_phase = 'spawn')),
    CHECK (status != 'running'
           OR (attempt > 0 AND lease_owner IS NOT NULL AND worker_pid IS NOT NULL
               AND progress_phase IN ('spawn', 'running', 'validating'))),
    CHECK (status != 'result_ready' OR progress_phase = 'applying'),
    CHECK (status != 'applied'
           OR (progress_phase = 'done' AND applied_at IS NOT NULL)),
    CHECK (updated_at >= created_at),
    UNIQUE (session_id, sequence)
);

CREATE TABLE state_transitions (
    id              INTEGER PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    scope_kind      TEXT NOT NULL CHECK (scope_kind IN ('project', 'task')),
    scope_id         TEXT NOT NULL,
    from_version     INTEGER NOT NULL CHECK (from_version >= 0),
    to_version       INTEGER NOT NULL CHECK (to_version = from_version + 1),
    from_state       TEXT NOT NULL,
    to_state         TEXT NOT NULL,
    action           TEXT NOT NULL,
    actor_kind       TEXT NOT NULL CHECK (actor_kind IN (
                         'human', 'architect', 'scheduler', 'developer_result',
                         'reviewer_result', 'recovery'
                     )),
    actor_identity   TEXT NOT NULL,
    payload_hash     TEXT NOT NULL,
    turn_id          TEXT REFERENCES worker_turns(id) ON DELETE RESTRICT,
    result_hash      TEXT,
    created_at       INTEGER NOT NULL,
    CHECK (length(CAST(scope_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(from_state AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(to_state AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(action AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(CAST(actor_identity AS BLOB)) BETWEEN 1 AND 256),
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (result_hash IS NULL OR
           (length(result_hash) = 64 AND result_hash NOT GLOB '*[^0-9a-f]*')),
    UNIQUE (scope_kind, scope_id, from_version)
);

CREATE TABLE architect_bindings (
    id                              TEXT PRIMARY KEY,
    version                         INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    repo_root                       TEXT NOT NULL,
    project_id                      TEXT REFERENCES project_runs(id) ON DELETE RESTRICT,
    architect_name                  TEXT NOT NULL,
    architect_adapter               TEXT NOT NULL,
    architect_pid                   INTEGER,
    architect_process_birth         TEXT,
    bridge_pid                      INTEGER,
    bridge_process_birth            TEXT,
    relay_executable_contract_hash  TEXT,
    relay_runtime_scope_hash        TEXT,
    launch_nonce_hash               TEXT NOT NULL,
    architect_native_session_id     TEXT,
    binding_state                   TEXT NOT NULL CHECK (
                                        binding_state IN ('pending', 'bound', 'closed')
                                    ),
    control_capability_hash         TEXT NOT NULL,
    action_set_json                 TEXT NOT NULL CHECK (
                                        json_valid(action_set_json)
                                        AND json_type(action_set_json) = 'array'
                                    ),
    action_set_hash                 TEXT NOT NULL,
    created_at                      INTEGER NOT NULL,
    updated_at                      INTEGER NOT NULL,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (substr(repo_root, 1, 1) = '/'
           AND length(CAST(repo_root AS BLOB)) BETWEEN 1 AND 4096),
    CHECK (length(CAST(architect_name AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(architect_adapter AS BLOB)) BETWEEN 1 AND 64),
    CHECK (architect_pid IS NULL OR architect_pid > 1),
    CHECK (bridge_pid IS NULL OR bridge_pid > 1),
    CHECK (architect_process_birth IS NULL OR
           length(CAST(architect_process_birth AS BLOB)) BETWEEN 1 AND 256),
    CHECK (bridge_process_birth IS NULL OR
           length(CAST(bridge_process_birth AS BLOB)) BETWEEN 1 AND 256),
    CHECK (relay_executable_contract_hash IS NULL OR
           (length(relay_executable_contract_hash) = 64
            AND relay_executable_contract_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK (relay_runtime_scope_hash IS NULL OR
           (length(relay_runtime_scope_hash) = 64
            AND relay_runtime_scope_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK (length(launch_nonce_hash) = 64
           AND launch_nonce_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (architect_native_session_id IS NULL OR
           length(CAST(architect_native_session_id AS BLOB)) BETWEEN 1 AND 256),
    CHECK (length(control_capability_hash) = 64
           AND control_capability_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(CAST(action_set_json AS BLOB)) BETWEEN 2 AND 4096),
    CHECK (length(action_set_hash) = 64
           AND action_set_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK ((binding_state IN ('pending', 'closed')
            AND architect_pid IS NULL AND architect_process_birth IS NULL
            AND bridge_pid IS NULL AND bridge_process_birth IS NULL
            AND relay_executable_contract_hash IS NULL
            AND relay_runtime_scope_hash IS NULL)
           OR (binding_state IN ('bound', 'closed')
               AND architect_pid IS NOT NULL AND architect_process_birth IS NOT NULL
               AND bridge_pid IS NOT NULL AND bridge_process_birth IS NOT NULL
               AND relay_executable_contract_hash IS NOT NULL
               AND relay_runtime_scope_hash IS NOT NULL)),
    CHECK (updated_at >= created_at)
);

CREATE UNIQUE INDEX architect_bindings_live_process
    ON architect_bindings(architect_name, architect_pid, architect_process_birth)
    WHERE architect_pid IS NOT NULL;
CREATE UNIQUE INDEX architect_bindings_native_session
    ON architect_bindings(architect_adapter, architect_native_session_id)
    WHERE architect_native_session_id IS NOT NULL;

CREATE TABLE project_apply_ops (
    id                        TEXT PRIMARY KEY,
    project_id                TEXT NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    expected_project_version  INTEGER NOT NULL CHECK (expected_project_version >= 0),
    expected_target_sha       TEXT NOT NULL,
    new_target_sha            TEXT NOT NULL,
    state                     TEXT NOT NULL CHECK (
                                  state IN ('intent', 'ref_updated', 'applied', 'conflict')
                              ),
    observed_target_sha       TEXT,
    created_at                INTEGER NOT NULL,
    ref_updated_at            INTEGER,
    applied_at                INTEGER,
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(expected_target_sha) IN (40, 64)
           AND expected_target_sha NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(new_target_sha) IN (40, 64)
           AND new_target_sha NOT GLOB '*[^0-9a-f]*'),
    CHECK (observed_target_sha IS NULL OR
           (length(observed_target_sha) IN (40, 64)
            AND observed_target_sha NOT GLOB '*[^0-9a-f]*')),
    CHECK ((state = 'intent' AND ref_updated_at IS NULL AND applied_at IS NULL)
           OR (state = 'ref_updated' AND ref_updated_at IS NOT NULL AND applied_at IS NULL)
           OR (state = 'applied' AND ref_updated_at IS NOT NULL AND applied_at IS NOT NULL)
           OR state = 'conflict'),
    UNIQUE (project_id, expected_project_version)
);

CREATE TABLE control_requests (
    caller_key_hash  TEXT NOT NULL,
    request_id       TEXT NOT NULL,
    daemon_epoch     TEXT NOT NULL REFERENCES daemon_epochs(id) ON DELETE RESTRICT,
    action           TEXT NOT NULL,
    payload_hash     TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('accepted', 'completed')),
    response_json    TEXT,
    response_hash    TEXT,
    created_at       INTEGER NOT NULL,
    completed_at     INTEGER,
    PRIMARY KEY (caller_key_hash, request_id),
    CHECK (length(caller_key_hash) = 64
           AND caller_key_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(CAST(request_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(action AS BLOB)) BETWEEN 1 AND 64),
    CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (response_json IS NULL OR
           (json_valid(response_json)
            AND length(CAST(response_json AS BLOB)) BETWEEN 2 AND 262144)),
    CHECK ((state = 'accepted'
            AND response_json IS NULL AND response_hash IS NULL AND completed_at IS NULL)
           OR (state = 'completed'
               AND response_json IS NOT NULL AND response_hash IS NOT NULL
               AND completed_at IS NOT NULL)),
    CHECK (response_hash IS NULL OR
           (length(response_hash) = 64 AND response_hash NOT GLOB '*[^0-9a-f]*'))
);

CREATE TRIGGER store_meta_immutable_update
BEFORE UPDATE ON store_meta
BEGIN
    SELECT RAISE(ABORT, 'store metadata is immutable');
END;
CREATE TRIGGER store_meta_immutable_delete
BEFORE DELETE ON store_meta
BEGIN
    SELECT RAISE(ABORT, 'store metadata is immutable');
END;

CREATE TRIGGER daemon_epochs_immutable_identity
BEFORE UPDATE OF id, boot_id, daemon_pid, process_birth, started_at
ON daemon_epochs
BEGIN
    SELECT RAISE(ABORT, 'daemon epoch identity is immutable');
END;
CREATE TRIGGER daemon_epochs_legal_state
BEFORE UPDATE OF state, stopped_at ON daemon_epochs
WHEN NOT (
    OLD.state = 'active' AND NEW.state IN ('retired', 'crashed')
    AND OLD.stopped_at IS NULL AND NEW.stopped_at IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'daemon epoch may close exactly once');
END;
CREATE TRIGGER daemon_epochs_immutable_delete
BEFORE DELETE ON daemon_epochs
BEGIN
    SELECT RAISE(ABORT, 'daemon epoch history is append-only');
END;

CREATE TRIGGER project_runs_immutable_identity
BEFORE UPDATE OF id, source_repo_root, source_git_dir_identity, target_ref,
                 target_expected_sha, worktree_root, worktree_branch, created_at
ON project_runs
BEGIN
    SELECT RAISE(ABORT, 'project identity is immutable');
END;
CREATE TRIGGER project_runs_version_cas
BEFORE UPDATE ON project_runs
WHEN NEW.version != OLD.version + 1
BEGIN
    SELECT RAISE(ABORT, 'project version must advance exactly once');
END;
CREATE TRIGGER project_runs_legal_state
BEFORE UPDATE OF state ON project_runs
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'draft' AND NEW.state IN ('needs_approval', 'canceled', 'failed'))
    OR (OLD.state = 'needs_approval' AND NEW.state IN ('approved', 'canceled', 'failed'))
    OR (OLD.state = 'approved' AND NEW.state IN ('running', 'paused', 'canceled', 'failed'))
    OR (OLD.state = 'running' AND NEW.state IN (
        'paused', 'needs_input', 'replanning', 'completed', 'failed',
        'needs_recovery', 'canceled'
    ))
    OR (OLD.state = 'paused' AND NEW.state IN (
        'running', 'replanning', 'failed', 'needs_recovery', 'canceled'
    ))
    OR (OLD.state = 'needs_input' AND NEW.state IN (
        'running', 'replanning', 'failed', 'needs_recovery', 'canceled'
    ))
    OR (OLD.state = 'replanning' AND NEW.state IN (
        'needs_approval', 'failed', 'needs_recovery', 'canceled'
    ))
    OR (OLD.state = 'needs_recovery' AND NEW.state IN (
        'paused', 'running', 'failed', 'canceled'
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'illegal project state transition');
END;

CREATE TRIGGER worker_profiles_immutable_update
BEFORE UPDATE ON worker_profiles
BEGIN
    SELECT RAISE(ABORT, 'worker profile is immutable');
END;

CREATE TRIGGER execution_environment_immutable_identity
BEFORE UPDATE OF project_id, lease_id, daemon_epoch, environment_hash,
                 inherited_names_json, required_names_json, created_at
ON execution_environment_leases
BEGIN
    SELECT RAISE(ABORT, 'execution environment lease identity is immutable');
END;
CREATE TRIGGER execution_environment_legal_state
BEFORE UPDATE OF state, lost_at ON execution_environment_leases
WHEN NOT (
    OLD.state = 'active' AND NEW.state = 'lost'
    AND OLD.lost_at IS NULL AND NEW.lost_at IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'execution environment lease may be lost exactly once');
END;
CREATE TRIGGER execution_environment_immutable_delete
BEFORE DELETE ON execution_environment_leases
BEGIN
    SELECT RAISE(ABORT, 'execution environment lease history cannot be deleted');
END;

CREATE TRIGGER project_plans_profile_shape_insert
BEFORE INSERT ON project_plans
WHEN NOT EXISTS (
    SELECT 1 FROM worker_profiles d
    WHERE d.id = NEW.developer_profile_id
      AND d.project_id = NEW.project_id
      AND d.role = 'developer'
) OR NOT EXISTS (
    SELECT 1 FROM worker_profiles r
    WHERE r.id = NEW.reviewer_profile_id
      AND r.project_id = NEW.project_id
      AND r.role = 'reviewer'
)
BEGIN
    SELECT RAISE(ABORT, 'plan profiles do not match project roles');
END;
CREATE TRIGGER project_plans_immutable_snapshot
BEFORE UPDATE OF id, project_id, version, base_checkpoint_sha, plan_hash,
                 developer_profile_id, reviewer_profile_id,
                 automatic_through_ordinal, created_by_binding, created_at
ON project_plans
BEGIN
    SELECT RAISE(ABORT, 'plan snapshot is immutable');
END;
CREATE TRIGGER project_plans_legal_state
BEFORE UPDATE OF state ON project_plans
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'draft' AND NEW.state IN ('approved', 'superseded'))
    OR (OLD.state = 'approved' AND NEW.state = 'superseded')
)
BEGIN
    SELECT RAISE(ABORT, 'illegal plan state transition');
END;

CREATE TRIGGER project_tasks_plan_shape_insert
BEFORE INSERT ON project_tasks
WHEN NOT EXISTS (
    SELECT 1 FROM project_plans p
    WHERE p.id = NEW.plan_id AND p.project_id = NEW.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'task plan does not match project');
END;
CREATE TRIGGER project_tasks_immutable_snapshot
BEFORE UPDATE OF id, project_id, plan_id, task_key, ordinal, spec_json,
                 spec_hash, max_review_rounds, created_at
ON project_tasks
BEGIN
    SELECT RAISE(ABORT, 'task snapshot is immutable');
END;
CREATE TRIGGER project_tasks_version_cas
BEFORE UPDATE ON project_tasks
WHEN NEW.version != OLD.version + 1
BEGIN
    SELECT RAISE(ABORT, 'task version must advance exactly once');
END;
CREATE TRIGGER project_tasks_legal_state
BEFORE UPDATE OF state ON project_tasks
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'draft' AND NEW.state IN ('queued', 'superseded', 'canceled'))
    OR (OLD.state = 'queued' AND NEW.state IN (
        'developing', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.state = 'developing' AND NEW.state IN (
        'awaiting_review', 'needs_input', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.state = 'awaiting_review' AND NEW.state IN (
        'changes_requested', 'finalizing', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.state = 'changes_requested' AND NEW.state IN (
        'developing', 'needs_input', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.state = 'finalizing' AND NEW.state IN (
        'completed', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.state = 'needs_input' AND NEW.state IN (
        'developing', 'superseded', 'failed', 'canceled'
    ))
    OR (OLD.state = 'indeterminate' AND NEW.state IN (
        'developing', 'failed', 'canceled'
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'illegal task state transition');
END;
CREATE TRIGGER project_tasks_developer_session_bind
BEFORE UPDATE OF developer_session_id ON project_tasks
WHEN OLD.developer_session_id IS NOT NULL
  OR NEW.developer_session_id IS NULL
  OR NOT EXISTS (
      SELECT 1 FROM worker_sessions s
      WHERE s.id = NEW.developer_session_id
        AND s.task_id = OLD.id AND s.project_id = OLD.project_id
        AND s.role = 'developer'
  )
BEGIN
    SELECT RAISE(ABORT, 'developer session may be bound once to the exact task');
END;
CREATE TRIGGER project_tasks_reviewer_session_bind
BEFORE UPDATE OF reviewer_session_id ON project_tasks
WHEN OLD.reviewer_session_id IS NOT NULL
  OR NEW.reviewer_session_id IS NULL
  OR NOT EXISTS (
      SELECT 1 FROM worker_sessions s
      WHERE s.id = NEW.reviewer_session_id
        AND s.task_id = OLD.id AND s.project_id = OLD.project_id
        AND s.role = 'reviewer'
  )
BEGIN
    SELECT RAISE(ABORT, 'reviewer session may be bound once to the exact task');
END;

CREATE TRIGGER task_dependencies_same_plan_insert
BEFORE INSERT ON task_dependencies
WHEN NOT EXISTS (
    SELECT 1
    FROM project_tasks t
    JOIN project_tasks d ON d.id = NEW.depends_on_task_id
    WHERE t.id = NEW.task_id AND t.plan_id = d.plan_id
)
BEGIN
    SELECT RAISE(ABORT, 'task dependency must stay in one plan');
END;
CREATE TRIGGER task_dependencies_immutable_update
BEFORE UPDATE ON task_dependencies
BEGIN
    SELECT RAISE(ABORT, 'task dependency is immutable');
END;
CREATE TRIGGER task_dependencies_immutable_delete
BEFORE DELETE ON task_dependencies
BEGIN
    SELECT RAISE(ABORT, 'task dependency is immutable');
END;

CREATE TRIGGER worker_sessions_shape_insert
BEFORE INSERT ON worker_sessions
WHEN NOT EXISTS (
    SELECT 1
    FROM project_tasks t
    JOIN worker_profiles p ON p.id = NEW.profile_id
    WHERE t.id = NEW.task_id
      AND t.project_id = NEW.project_id
      AND p.project_id = NEW.project_id
      AND p.role = NEW.role
      AND p.adapter = NEW.adapter
      AND ((p.native_session_mode = 'preassigned' AND NEW.native_session_id IS NOT NULL)
           OR (p.native_session_mode = 'discovered' AND NEW.native_session_id IS NULL))
)
BEGIN
    SELECT RAISE(ABORT, 'worker session task/profile shape is inconsistent');
END;
CREATE TRIGGER worker_sessions_immutable_identity
BEFORE UPDATE OF id, project_id, task_id, role, profile_id, adapter, created_at
ON worker_sessions
BEGIN
    SELECT RAISE(ABORT, 'worker session identity is immutable');
END;
CREATE TRIGGER worker_sessions_native_once
BEFORE UPDATE OF native_session_id ON worker_sessions
WHEN OLD.state != 'creating' OR NEW.state NOT IN ('creating', 'active')
  OR OLD.native_session_id IS NOT NULL OR NEW.native_session_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'native session may be bound exactly once');
END;
CREATE TRIGGER worker_sessions_active_requires_native
BEFORE UPDATE OF state ON worker_sessions
WHEN NEW.state = 'active' AND NEW.native_session_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'active worker session requires an exact native session');
END;
CREATE TRIGGER worker_sessions_legal_state
BEFORE UPDATE OF state ON worker_sessions
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'creating' AND NEW.state IN ('active', 'closed', 'failed', 'indeterminate'))
    OR (OLD.state = 'active' AND NEW.state IN ('closed', 'failed', 'indeterminate'))
    OR (OLD.state = 'indeterminate' AND NEW.state IN ('closed', 'failed'))
)
BEGIN
    SELECT RAISE(ABORT, 'illegal worker session state transition');
END;

CREATE TRIGGER worker_turns_shape_insert
BEFORE INSERT ON worker_turns
WHEN NOT EXISTS (
    SELECT 1 FROM worker_sessions s
    JOIN project_tasks t ON t.id = s.task_id
    WHERE s.id = NEW.session_id
      AND ((NEW.sequence = 1 AND NEW.kind = 'create' AND s.state = 'creating')
           OR (NEW.sequence > 1 AND NEW.kind = 'resume' AND s.state = 'active'
               AND EXISTS (
                   SELECT 1 FROM worker_turns previous
                   WHERE previous.session_id = NEW.session_id
                     AND previous.sequence = NEW.sequence - 1
                     AND previous.status = 'applied'
               )))
      AND t.state NOT IN ('completed', 'superseded', 'canceled')
      AND NEW.task_version = t.version
      AND NEW.review_round = t.review_round
) OR NEW.sequence != COALESCE((
    SELECT MAX(existing.sequence) + 1
    FROM worker_turns existing
    WHERE existing.session_id = NEW.session_id
), 1)
BEGIN
    SELECT RAISE(ABORT, 'worker turn session or sequence shape is invalid');
END;
CREATE TRIGGER worker_turns_immutable_request
BEFORE UPDATE OF id, session_id, sequence, kind, task_version, review_round,
                 request_hash, artifact_dir, created_at
ON worker_turns
BEGIN
    SELECT RAISE(ABORT, 'worker turn request is immutable');
END;
CREATE TRIGGER worker_turns_attempt_cas
BEFORE UPDATE OF attempt ON worker_turns
WHEN NEW.attempt != OLD.attempt AND NOT (
    NEW.attempt = OLD.attempt + 1
    AND NEW.status = 'claimed'
    AND ((OLD.status = 'queued' AND OLD.attempt = 0)
         OR OLD.status IN ('failed', 'indeterminate'))
    AND NEW.lease_owner IS NOT NULL AND NEW.expires_at IS NOT NULL
    AND NEW.worker_pid IS NULL AND NEW.process_birth IS NULL
    AND NEW.progress_phase = 'spawn' AND NEW.last_progress_at IS NOT NULL
    AND NEW.result_json IS NULL AND NEW.result_hash IS NULL
    AND NEW.error_kind IS NULL AND NEW.error_message IS NULL
    AND NEW.started_at IS NULL AND NEW.result_at IS NULL AND NEW.applied_at IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'worker turn attempt must advance through a clean queued CAS');
END;
CREATE TRIGGER worker_turns_review_snapshot_once
BEFORE UPDATE OF review_snapshot_digest ON worker_turns
WHEN NOT (
    NEW.review_snapshot_digest IS OLD.review_snapshot_digest
    OR (
        OLD.review_snapshot_digest IS NULL
        AND NEW.review_snapshot_digest IS NOT NULL
        AND OLD.status = 'claimed' AND NEW.status = 'claimed'
        AND OLD.attempt = NEW.attempt AND NEW.attempt > 0
        AND EXISTS (
            SELECT 1 FROM worker_sessions s
            WHERE s.id = NEW.session_id AND s.role = 'reviewer'
        )
    )
    OR (
        OLD.review_snapshot_digest IS NOT NULL
        AND NEW.review_snapshot_digest IS NULL
        AND OLD.status IN ('failed', 'indeterminate')
        AND NEW.status = 'claimed'
        AND NEW.attempt = OLD.attempt + 1
    )
)
BEGIN
    SELECT RAISE(ABORT, 'review snapshot may bind once per exact reviewer attempt');
END;
CREATE TRIGGER worker_turns_reviewer_result_requires_snapshot
BEFORE UPDATE OF status ON worker_turns
WHEN NEW.status IN ('result_ready', 'applied')
  AND EXISTS (
      SELECT 1 FROM worker_sessions s
      WHERE s.id = NEW.session_id AND s.role = 'reviewer'
  )
  AND NEW.review_snapshot_digest IS NULL
BEGIN
    SELECT RAISE(ABORT, 'reviewer result requires an exact snapshot binding');
END;
CREATE TRIGGER worker_turns_legal_state
BEFORE UPDATE OF status ON worker_turns
WHEN NEW.status != OLD.status AND NOT (
    (OLD.status = 'queued' AND NEW.status IN (
        'claimed', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.status = 'claimed' AND NEW.status IN (
        'running', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.status = 'running' AND NEW.status IN (
        'result_ready', 'failed', 'indeterminate', 'stale', 'canceled'
    ))
    OR (OLD.status = 'result_ready' AND NEW.status IN ('applied', 'stale', 'canceled'))
    OR (OLD.status IN ('failed', 'indeterminate') AND NEW.status = 'claimed'
        AND NEW.attempt = OLD.attempt + 1)
)
BEGIN
    SELECT RAISE(ABORT, 'illegal worker turn state transition');
END;
CREATE TRIGGER worker_turns_process_bind_once
BEFORE UPDATE OF worker_pid, process_birth ON worker_turns
WHEN NOT (
    (NEW.worker_pid IS OLD.worker_pid AND NEW.process_birth IS OLD.process_birth)
    OR (OLD.worker_pid IS NULL AND OLD.process_birth IS NULL
        AND NEW.worker_pid IS NOT NULL AND NEW.process_birth IS NOT NULL
        AND OLD.status = 'claimed' AND NEW.status = 'running'
        AND NEW.attempt = OLD.attempt AND NEW.attempt > 0)
    OR (OLD.worker_pid IS NOT NULL AND OLD.process_birth IS NOT NULL
        AND NEW.worker_pid IS NULL AND NEW.process_birth IS NULL
        AND OLD.status IN ('failed', 'indeterminate') AND NEW.status = 'claimed'
        AND NEW.attempt = OLD.attempt + 1)
)
BEGIN
    SELECT RAISE(ABORT, 'worker process identity may be bound once per exact attempt');
END;

CREATE TRIGGER state_transitions_append_only_update
BEFORE UPDATE ON state_transitions
BEGIN
    SELECT RAISE(ABORT, 'state transition audit is append-only');
END;
CREATE TRIGGER state_transitions_append_only_delete
BEFORE DELETE ON state_transitions
BEGIN
    SELECT RAISE(ABORT, 'state transition audit is append-only');
END;
CREATE TRIGGER state_transitions_scope_insert
BEFORE INSERT ON state_transitions
WHEN (NEW.scope_kind = 'project' AND
      (NEW.scope_id != NEW.project_id OR NOT EXISTS (
          SELECT 1 FROM project_runs p WHERE p.id = NEW.scope_id
      )))
   OR (NEW.scope_kind = 'task' AND NOT EXISTS (
       SELECT 1 FROM project_tasks t
       WHERE t.id = NEW.scope_id AND t.project_id = NEW.project_id
   ))
BEGIN
    SELECT RAISE(ABORT, 'transition scope does not match project');
END;

CREATE TRIGGER architect_bindings_immutable_scope
BEFORE UPDATE OF id, repo_root, architect_name, architect_adapter,
                 launch_nonce_hash, control_capability_hash, action_set_json,
                 action_set_hash, created_at
ON architect_bindings
BEGIN
    SELECT RAISE(ABORT, 'architect binding scope is immutable');
END;
CREATE TRIGGER architect_bindings_version_cas
BEFORE UPDATE ON architect_bindings
WHEN NEW.version != OLD.version + 1
BEGIN
    SELECT RAISE(ABORT, 'architect binding version must advance exactly once');
END;
CREATE TRIGGER architect_bindings_bind_once
BEFORE UPDATE OF architect_pid, architect_process_birth, bridge_pid,
                 bridge_process_birth, relay_executable_contract_hash,
                 relay_runtime_scope_hash, binding_state
ON architect_bindings
WHEN NOT (
    (OLD.binding_state = 'pending' AND NEW.binding_state = 'bound'
     AND OLD.architect_pid IS NULL AND NEW.architect_pid IS NOT NULL
     AND OLD.architect_process_birth IS NULL AND NEW.architect_process_birth IS NOT NULL
     AND OLD.bridge_pid IS NULL AND NEW.bridge_pid IS NOT NULL
     AND OLD.bridge_process_birth IS NULL AND NEW.bridge_process_birth IS NOT NULL
     AND OLD.relay_executable_contract_hash IS NULL
     AND NEW.relay_executable_contract_hash IS NOT NULL
     AND OLD.relay_runtime_scope_hash IS NULL
     AND NEW.relay_runtime_scope_hash IS NOT NULL)
    OR (OLD.binding_state = 'pending' AND NEW.binding_state = 'closed'
        AND NEW.architect_pid IS OLD.architect_pid
        AND NEW.architect_process_birth IS OLD.architect_process_birth
        AND NEW.bridge_pid IS OLD.bridge_pid
        AND NEW.bridge_process_birth IS OLD.bridge_process_birth
        AND NEW.relay_executable_contract_hash IS OLD.relay_executable_contract_hash
        AND NEW.relay_runtime_scope_hash IS OLD.relay_runtime_scope_hash)
    OR (OLD.binding_state = 'bound' AND NEW.binding_state = 'closed'
        AND NEW.architect_pid IS OLD.architect_pid
        AND NEW.architect_process_birth IS OLD.architect_process_birth
        AND NEW.bridge_pid IS OLD.bridge_pid
        AND NEW.bridge_process_birth IS OLD.bridge_process_birth
        AND NEW.relay_executable_contract_hash IS OLD.relay_executable_contract_hash
        AND NEW.relay_runtime_scope_hash IS OLD.relay_runtime_scope_hash)
)
BEGIN
    SELECT RAISE(ABORT, 'architect process binding is one-shot');
END;
CREATE TRIGGER architect_bindings_native_once
BEFORE UPDATE OF architect_native_session_id ON architect_bindings
WHEN OLD.binding_state != 'bound'
  OR OLD.architect_native_session_id IS NOT NULL
  OR NEW.architect_native_session_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'architect native session may be bound exactly once');
END;
CREATE TRIGGER architect_bindings_project_once
BEFORE UPDATE OF project_id ON architect_bindings
WHEN OLD.binding_state != 'bound'
  OR OLD.project_id IS NOT NULL
  OR NEW.project_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'architect project may be bound exactly once');
END;

CREATE TRIGGER project_apply_ops_immutable_intent
BEFORE UPDATE OF id, project_id, expected_project_version,
                 expected_target_sha, new_target_sha, created_at
ON project_apply_ops
BEGIN
    SELECT RAISE(ABORT, 'project apply intent is immutable');
END;
CREATE TRIGGER project_apply_ops_legal_state
BEFORE UPDATE OF state ON project_apply_ops
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'intent' AND NEW.state IN ('ref_updated', 'conflict'))
    OR (OLD.state = 'ref_updated' AND NEW.state IN ('applied', 'conflict'))
)
BEGIN
    SELECT RAISE(ABORT, 'illegal project apply state transition');
END;

CREATE TRIGGER control_requests_immutable_identity
BEFORE UPDATE OF caller_key_hash, request_id, daemon_epoch, action, payload_hash, created_at
ON control_requests
BEGIN
    SELECT RAISE(ABORT, 'control request identity is immutable');
END;
CREATE TRIGGER control_requests_complete_once
BEFORE UPDATE OF state, response_json, response_hash, completed_at
ON control_requests
WHEN NOT (
    OLD.state = 'accepted' AND NEW.state = 'completed'
    AND OLD.response_json IS NULL AND NEW.response_json IS NOT NULL
    AND OLD.response_hash IS NULL AND NEW.response_hash IS NOT NULL
    AND OLD.completed_at IS NULL AND NEW.completed_at IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'control request may complete exactly once');
END;

CREATE TRIGGER worker_sessions_immutable_delete
BEFORE DELETE ON worker_sessions
BEGIN
    SELECT RAISE(ABORT, 'worker session history cannot be deleted');
END;
CREATE TRIGGER worker_turns_immutable_delete
BEFORE DELETE ON worker_turns
BEGIN
    SELECT RAISE(ABORT, 'worker turn history cannot be deleted');
END;
CREATE TRIGGER control_requests_immutable_delete
BEFORE DELETE ON control_requests
BEGIN
    SELECT RAISE(ABORT, 'control request history cannot be deleted');
END;
"#;
