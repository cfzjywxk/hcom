//! SQLite database access for hcom
//!
//! Three loosely-coupled state planes live in a single DB:
//! - `instances`: live per-agent state (TUI display, gating, delivery cursors)
//! - `events`: append-only history / message log / relay replication source
//! - `process_bindings`, `session_bindings`, `notify_endpoints`, `kv`: routing
//!   and control-plane state
//!
//! Callers typically write an event, advance per-instance cursors separately,
//! and touch bindings/endpoints/kv for delivery, identity resolution, relay
//! cursors, request-watch bookkeeping, and other control-plane state.
//!
//! Includes:
//! - Reading unread messages from `events`
//! - Updating cursor position (instances.last_event_id)
//! - Reading instance status
//! - Registering notify endpoints

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::shared::time::now_epoch_f64;

mod events;
mod instances;
mod kv;
mod notify;
pub(crate) mod reqwatch_policy;
mod sessions;
pub(crate) mod subscriptions;

pub use events::Message;
pub use instances::InstanceRow;
#[allow(unused_imports)]
pub use instances::InstanceStatus;

/// Schema version - bump on any schema change.
const SCHEMA_VERSION: i32 = 23;
pub const DEV_ROOT_KV_KEY: &str = "config:dev_root";
const REVIEW_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS review_runs (
        id                    TEXT PRIMARY KEY,
        task                  TEXT NOT NULL,
        workspace             TEXT NOT NULL,
        thread                TEXT NOT NULL UNIQUE,
        developer_name        TEXT NOT NULL,
        developer_session_id  TEXT NOT NULL,
        reviewer_name         TEXT NOT NULL,
        reviewer_session_id   TEXT NOT NULL,
        state                 TEXT NOT NULL CHECK (state IN (
                                  'awaiting_review',
                                  'awaiting_developer',
                                  'max_rounds',
                                  'approved',
                                  'canceled'
                              )),
        round                 INTEGER NOT NULL,
        max_rounds            INTEGER NOT NULL,
        version               INTEGER NOT NULL DEFAULT 0,
        last_message_event_id INTEGER,
        created_at            REAL NOT NULL,
        updated_at            REAL NOT NULL,
        CHECK (round >= 1 AND round <= max_rounds AND max_rounds <= 20),
        FOREIGN KEY (last_message_event_id) REFERENCES events(id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_review_runs_active_pair
        ON review_runs(developer_session_id, reviewer_session_id, state);

    CREATE TABLE IF NOT EXISTS review_transitions (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        workflow_id      TEXT NOT NULL,
        from_version     INTEGER NOT NULL,
        to_version       INTEGER NOT NULL,
        round            INTEGER NOT NULL,
        actor_name       TEXT NOT NULL,
        actor_session_id TEXT NOT NULL,
        actor_role       TEXT NOT NULL CHECK (actor_role IN ('developer', 'reviewer')),
        action           TEXT NOT NULL CHECK (action IN (
                             'start', 'request_changes', 'lgtm',
                             'fixed', 'rebut', 'extend', 'cancel'
                         )),
        from_state       TEXT,
        to_state         TEXT NOT NULL,
        summary          TEXT NOT NULL DEFAULT '',
        payload_hash     TEXT NOT NULL,
        message_event_id INTEGER,
        created_at       REAL NOT NULL,
        UNIQUE (workflow_id, from_version),
        FOREIGN KEY (workflow_id) REFERENCES review_runs(id) ON DELETE CASCADE,
        FOREIGN KEY (message_event_id) REFERENCES events(id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_review_transitions_workflow
        ON review_transitions(workflow_id, to_version);
";
const HANDOFF_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS terminal_chains (
        id                                TEXT PRIMARY KEY,
        workspace                         TEXT NOT NULL,
        tool                              TEXT NOT NULL CHECK (tool = 'codex'),
        tag                               TEXT NOT NULL DEFAULT '',
        model_ref                         TEXT NOT NULL,
        reasoning_ref                     TEXT NOT NULL,
        permission_policy_ref             TEXT NOT NULL,
        policy_ref                        TEXT NOT NULL,
        supervisor_process_id             TEXT NOT NULL,
        supervisor_process_birth_identity TEXT NOT NULL,
        supervisor_pid                    INTEGER,
        supervisor_pgid                   INTEGER,
        outer_foreground_pgid             INTEGER,
        outer_tty_device                  INTEGER,
        outer_tty_inode                   INTEGER,
        current_generation                INTEGER NOT NULL CHECK (current_generation >= 1),
        state                             TEXT NOT NULL CHECK (state IN (
                                              'active',
                                              'prepared',
                                              'committed',
                                              'stop_observed',
                                              'quiescing_source',
                                              'launching_target',
                                              'awaiting_acceptance',
                                              'needs_recovery'
                                          )),
        version                           INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
        created_at                        REAL NOT NULL,
        updated_at                        REAL NOT NULL,
        CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(workspace AS BLOB)) BETWEEN 1 AND 4096),
        CHECK (length(CAST(tag AS BLOB)) <= 64),
        CHECK (tag NOT GLOB '*[^a-zA-Z0-9-]*'),
        CHECK (length(CAST(model_ref AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(reasoning_ref AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(permission_policy_ref AS BLOB)) BETWEEN 1 AND 512),
        CHECK (length(CAST(policy_ref AS BLOB)) BETWEEN 1 AND 512),
        CHECK (length(CAST(supervisor_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(supervisor_process_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (
            (supervisor_pid IS NULL
             AND supervisor_pgid IS NULL
             AND outer_foreground_pgid IS NULL
             AND outer_tty_device IS NULL
             AND outer_tty_inode IS NULL)
            OR
            (supervisor_pid > 0
             AND supervisor_pgid > 0
             AND outer_foreground_pgid > 0
             AND outer_tty_device > 0
             AND outer_tty_inode > 0)
        ),
        FOREIGN KEY (id, current_generation)
            REFERENCES terminal_generations(chain_id, generation)
            DEFERRABLE INITIALLY DEFERRED
    );

    CREATE TABLE IF NOT EXISTS terminal_generations (
        chain_id              TEXT NOT NULL,
        generation            INTEGER NOT NULL CHECK (generation >= 1),
        launch_nonce          TEXT NOT NULL,
        wrapper_process_id    TEXT,
        process_birth_identity TEXT,
        instance_name         TEXT,
        hcom_session_id       TEXT,
        native_session_id     TEXT,
        state                 TEXT NOT NULL CHECK (state IN (
                                  'active',
                                  'handoff_prepared',
                                  'handoff_committed',
                                  'stop_observed',
                                  'quiescing',
                                  'retired',
                                  'reserved',
                                  'launching',
                                  'awaiting_acceptance',
                                  'needs_recovery'
                              )),
        version               INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
        created_at            REAL NOT NULL,
        updated_at            REAL NOT NULL,
        PRIMARY KEY (chain_id, generation),
        CHECK (length(CAST(launch_nonce AS BLOB)) BETWEEN 1 AND 64),
        CHECK (wrapper_process_id IS NULL OR
               length(CAST(wrapper_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (process_birth_identity IS NULL OR
               length(CAST(process_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (instance_name IS NULL OR
               length(CAST(instance_name AS BLOB)) BETWEEN 1 AND 128),
        CHECK (hcom_session_id IS NULL OR
               length(CAST(hcom_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (native_session_id IS NULL OR
               length(CAST(native_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (
            (wrapper_process_id IS NULL AND process_birth_identity IS NULL
             AND instance_name IS NULL AND hcom_session_id IS NULL
             AND native_session_id IS NULL)
            OR
            (wrapper_process_id IS NOT NULL AND process_birth_identity IS NOT NULL
             AND instance_name IS NOT NULL AND hcom_session_id IS NOT NULL)
        ),
        FOREIGN KEY (chain_id) REFERENCES terminal_chains(id) ON DELETE RESTRICT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_generation_launch_nonce
        ON terminal_generations(launch_nonce);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_generation_process
        ON terminal_generations(wrapper_process_id)
        WHERE wrapper_process_id IS NOT NULL;

    CREATE TRIGGER IF NOT EXISTS terminal_chains_immutable_identity_policy
    BEFORE UPDATE OF workspace, tool, tag, model_ref, reasoning_ref,
                     permission_policy_ref, policy_ref, supervisor_process_id,
                     supervisor_process_birth_identity, supervisor_pid,
                     supervisor_pgid, outer_foreground_pgid, outer_tty_device,
                     outer_tty_inode
    ON terminal_chains
    WHEN NEW.workspace IS NOT OLD.workspace
      OR NEW.tool IS NOT OLD.tool
      OR NEW.tag IS NOT OLD.tag
      OR NEW.model_ref IS NOT OLD.model_ref
      OR NEW.reasoning_ref IS NOT OLD.reasoning_ref
      OR NEW.permission_policy_ref IS NOT OLD.permission_policy_ref
      OR NEW.policy_ref IS NOT OLD.policy_ref
      OR NEW.supervisor_process_id IS NOT OLD.supervisor_process_id
      OR NEW.supervisor_process_birth_identity
             IS NOT OLD.supervisor_process_birth_identity
      OR NEW.supervisor_pid IS NOT OLD.supervisor_pid
      OR NEW.supervisor_pgid IS NOT OLD.supervisor_pgid
      OR NEW.outer_foreground_pgid IS NOT OLD.outer_foreground_pgid
      OR NEW.outer_tty_device IS NOT OLD.outer_tty_device
      OR NEW.outer_tty_inode IS NOT OLD.outer_tty_inode
    BEGIN
        SELECT RAISE(ABORT, 'terminal chain identity and policy are immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_chains_outer_evidence_insert
    BEFORE INSERT ON terminal_chains
    WHEN NOT (
        (NEW.supervisor_pid IS NULL
         AND NEW.supervisor_pgid IS NULL
         AND NEW.outer_foreground_pgid IS NULL
         AND NEW.outer_tty_device IS NULL
         AND NEW.outer_tty_inode IS NULL)
        OR
        (NEW.supervisor_pid > 0
         AND NEW.supervisor_pgid > 0
         AND NEW.outer_foreground_pgid > 0
         AND NEW.outer_tty_device > 0
         AND NEW.outer_tty_inode > 0)
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal chain outer evidence must be complete');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_chains_outer_evidence_update
    BEFORE UPDATE OF supervisor_pid, supervisor_pgid, outer_foreground_pgid,
                     outer_tty_device, outer_tty_inode
    ON terminal_chains
    WHEN NOT (
        (NEW.supervisor_pid IS NULL
         AND NEW.supervisor_pgid IS NULL
         AND NEW.outer_foreground_pgid IS NULL
         AND NEW.outer_tty_device IS NULL
         AND NEW.outer_tty_inode IS NULL)
        OR
        (NEW.supervisor_pid > 0
         AND NEW.supervisor_pgid > 0
         AND NEW.outer_foreground_pgid > 0
         AND NEW.outer_tty_device > 0
         AND NEW.outer_tty_inode > 0)
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal chain outer evidence must be complete');
    END;

    CREATE TABLE IF NOT EXISTS terminal_handoffs (
        id                                  TEXT PRIMARY KEY,
        chain_id                            TEXT NOT NULL,
        source_generation                   INTEGER NOT NULL CHECK (source_generation >= 1),
        target_generation                   INTEGER NOT NULL CHECK (target_generation >= 1),
        source_launch_nonce                 TEXT NOT NULL,
        source_instance_name                TEXT NOT NULL,
        source_hcom_session_id              TEXT NOT NULL,
        source_native_session_id            TEXT NOT NULL,
        source_wrapper_process_id            TEXT NOT NULL,
        source_process_birth_identity        TEXT NOT NULL,
        bundle_event_id                     INTEGER NOT NULL,
        bundle_digest                       TEXT NOT NULL,
        bundle_size_bytes                   INTEGER NOT NULL
                                             CHECK (bundle_size_bytes BETWEEN 0 AND 1048576),
        workspace                            TEXT NOT NULL,
        revision                             TEXT NOT NULL,
        branch                               TEXT NOT NULL,
        dirty_summary                        TEXT NOT NULL,
        policy_ref                           TEXT NOT NULL,
        state                                TEXT NOT NULL CHECK (state IN (
                                                 'prepared',
                                                 'committed',
                                                 'stop_observed',
                                                 'quiescing_source',
                                                 'launching_target',
                                                 'awaiting_acceptance',
                                                 'accepted',
                                                 'aborted',
                                                 'needs_recovery'
                                             )),
        version                              INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
        quiesce_token                        TEXT,
        quiesce_generation                   INTEGER,
        quiesce_native_session_id            TEXT,
        quiesce_process_id                   TEXT,
        quiesce_process_birth_identity       TEXT,
        quiesce_committed_version            INTEGER,
        stop_observed_at                     REAL,
        stop_turn_id                         TEXT,
        sigterm_requested_wall_at             REAL,
        sigterm_requested_monotonic_ns        INTEGER,
        sigterm_request_result                TEXT NOT NULL DEFAULT ''
                                               CHECK (sigterm_request_result IN (
                                                   '', 'sent', 'not_found',
                                                   'permission_denied', 'error'
                                               )),
        child_exit_observed_wall_at           REAL,
        child_exit_observed_monotonic_ns      INTEGER,
        child_exit_code                       INTEGER,
        child_exit_signal                     INTEGER,
        sigterm_to_exit_ms                    INTEGER,
        delivery_exit_context                 TEXT NOT NULL DEFAULT ''
                                               CHECK (delivery_exit_context IN (
                                                   '', 'exit:closed', 'exit:killed'
                                               )),
        waitpid_reaped                        INTEGER CHECK (waitpid_reaped IN (0, 1)),
        inject_cleanup_succeeded              INTEGER
                                               CHECK (inject_cleanup_succeeded IN (0, 1)),
        delivery_cleanup_succeeded            INTEGER
                                               CHECK (delivery_cleanup_succeeded IN (0, 1)),
        pty_cleanup_succeeded                 INTEGER
                                               CHECK (pty_cleanup_succeeded IN (0, 1)),
        screen_cleanup_succeeded              INTEGER
                                               CHECK (screen_cleanup_succeeded IN (0, 1)),
        write_queue_cleanup_succeeded         INTEGER
                                               CHECK (write_queue_cleanup_succeeded IN (0, 1)),
        cleanup_completed_at                  REAL,
        target_validation_token              TEXT,
        target_instructions_digest           TEXT,
        target_validated_at                   REAL,
        failure_kind                         TEXT NOT NULL DEFAULT '',
        failure_reason                       TEXT NOT NULL DEFAULT '',
        created_at                           REAL NOT NULL,
        updated_at                           REAL NOT NULL,
        committed_at                         REAL,
        accepted_at                          REAL,
        CHECK (target_generation = source_generation + 1),
        CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(source_launch_nonce AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(source_instance_name AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(source_hcom_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(source_native_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(source_wrapper_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(source_process_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(bundle_digest AS BLOB)) = 64),
        CHECK (length(CAST(workspace AS BLOB)) BETWEEN 1 AND 4096),
        CHECK (length(CAST(revision AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(branch AS BLOB)) BETWEEN 1 AND 1024),
        CHECK (length(CAST(dirty_summary AS BLOB)) BETWEEN 1 AND 512),
        CHECK (length(CAST(policy_ref AS BLOB)) BETWEEN 1 AND 512),
        CHECK (quiesce_token IS NULL OR
               length(CAST(quiesce_token AS BLOB)) BETWEEN 1 AND 64),
        CHECK (quiesce_native_session_id IS NULL OR
               length(CAST(quiesce_native_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (stop_turn_id IS NULL OR
               length(CAST(stop_turn_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (quiesce_process_id IS NULL OR
               length(CAST(quiesce_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (quiesce_process_birth_identity IS NULL OR
               length(CAST(quiesce_process_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(failure_kind AS BLOB)) <= 64),
        CHECK (length(CAST(failure_reason AS BLOB)) <= 1024),
        CHECK (target_validation_token IS NULL OR
               length(CAST(target_validation_token AS BLOB)) BETWEEN 1 AND 64),
        CHECK (target_instructions_digest IS NULL OR
               length(CAST(target_instructions_digest AS BLOB)) = 64),
        CHECK (
            (target_validation_token IS NULL
             AND target_instructions_digest IS NULL
             AND target_validated_at IS NULL)
            OR
            (target_validation_token IS NOT NULL
             AND target_instructions_digest IS NOT NULL
             AND target_validated_at IS NOT NULL)
        ),
        CHECK (sigterm_requested_monotonic_ns IS NULL
               OR sigterm_requested_monotonic_ns >= 0),
        CHECK (child_exit_observed_monotonic_ns IS NULL
               OR child_exit_observed_monotonic_ns >= 0),
        CHECK (sigterm_to_exit_ms IS NULL
               OR sigterm_to_exit_ms BETWEEN 0 AND 86400000),
        CHECK (
            (sigterm_request_result = ''
             AND sigterm_requested_wall_at IS NULL
             AND sigterm_requested_monotonic_ns IS NULL)
            OR
            (sigterm_request_result != ''
             AND sigterm_requested_wall_at IS NOT NULL
             AND sigterm_requested_monotonic_ns IS NOT NULL)
        ),
        CHECK (
            (child_exit_observed_wall_at IS NULL
             AND child_exit_observed_monotonic_ns IS NULL
             AND child_exit_code IS NULL
             AND child_exit_signal IS NULL
             AND sigterm_to_exit_ms IS NULL
             AND delivery_exit_context = '')
            OR
            (child_exit_observed_wall_at IS NOT NULL
             AND child_exit_observed_monotonic_ns IS NOT NULL
             AND ((child_exit_code IS NOT NULL AND child_exit_signal IS NULL)
                  OR (child_exit_code IS NULL AND child_exit_signal IS NOT NULL))
             AND (
                 (sigterm_request_result = 'sent'
                  AND sigterm_to_exit_ms IS NOT NULL)
                 OR
                 (sigterm_request_result = ''
                  AND sigterm_to_exit_ms IS NULL)
             )
             AND delivery_exit_context != '')
        ),
        CHECK (
            (waitpid_reaped IS NULL
             AND inject_cleanup_succeeded IS NULL
             AND delivery_cleanup_succeeded IS NULL
             AND pty_cleanup_succeeded IS NULL
             AND screen_cleanup_succeeded IS NULL
             AND write_queue_cleanup_succeeded IS NULL
             AND cleanup_completed_at IS NULL)
            OR
            (waitpid_reaped IS NOT NULL
             AND inject_cleanup_succeeded IS NOT NULL
             AND delivery_cleanup_succeeded IS NOT NULL
             AND pty_cleanup_succeeded IS NOT NULL
             AND screen_cleanup_succeeded IS NOT NULL
             AND write_queue_cleanup_succeeded IS NOT NULL
             AND cleanup_completed_at IS NOT NULL)
        ),
        CHECK (
            (quiesce_token IS NULL
             AND quiesce_generation IS NULL
             AND quiesce_native_session_id IS NULL
             AND quiesce_process_id IS NULL
             AND quiesce_process_birth_identity IS NULL
             AND quiesce_committed_version IS NULL)
            OR
            (quiesce_token IS NOT NULL
             AND quiesce_generation = source_generation
             AND quiesce_native_session_id = source_native_session_id
             AND quiesce_process_id = source_wrapper_process_id
             AND quiesce_process_birth_identity = source_process_birth_identity
             AND quiesce_committed_version >= 1)
        ),
        FOREIGN KEY (chain_id) REFERENCES terminal_chains(id) ON DELETE RESTRICT,
        FOREIGN KEY (chain_id, source_generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT,
        FOREIGN KEY (chain_id, target_generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT,
        FOREIGN KEY (bundle_event_id) REFERENCES events(id) ON DELETE RESTRICT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_handoffs_one_non_final
        ON terminal_handoffs(chain_id)
        WHERE state NOT IN ('accepted', 'aborted');
    CREATE INDEX IF NOT EXISTS idx_terminal_handoffs_chain
        ON terminal_handoffs(chain_id, source_generation, target_generation);

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_immutable_snapshot
    BEFORE UPDATE OF chain_id, source_generation, target_generation,
                     source_launch_nonce, source_instance_name,
                     source_hcom_session_id, source_native_session_id,
                     source_wrapper_process_id, source_process_birth_identity,
                     bundle_event_id, bundle_digest, bundle_size_bytes,
                     workspace, revision, branch, dirty_summary, policy_ref
    ON terminal_handoffs
    WHEN NEW.chain_id IS NOT OLD.chain_id
      OR NEW.source_generation IS NOT OLD.source_generation
      OR NEW.target_generation IS NOT OLD.target_generation
      OR NEW.source_launch_nonce IS NOT OLD.source_launch_nonce
      OR NEW.source_instance_name IS NOT OLD.source_instance_name
      OR NEW.source_hcom_session_id IS NOT OLD.source_hcom_session_id
      OR NEW.source_native_session_id IS NOT OLD.source_native_session_id
      OR NEW.source_wrapper_process_id IS NOT OLD.source_wrapper_process_id
      OR NEW.source_process_birth_identity IS NOT OLD.source_process_birth_identity
      OR NEW.bundle_event_id IS NOT OLD.bundle_event_id
      OR NEW.bundle_digest IS NOT OLD.bundle_digest
      OR NEW.bundle_size_bytes IS NOT OLD.bundle_size_bytes
      OR NEW.workspace IS NOT OLD.workspace
      OR NEW.revision IS NOT OLD.revision
      OR NEW.branch IS NOT OLD.branch
      OR NEW.dirty_summary IS NOT OLD.dirty_summary
      OR NEW.policy_ref IS NOT OLD.policy_ref
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff snapshot is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_quiesce_once
    BEFORE UPDATE OF quiesce_token, quiesce_generation,
                     quiesce_native_session_id, quiesce_process_id,
                     quiesce_process_birth_identity, quiesce_committed_version
    ON terminal_handoffs
    WHEN OLD.quiesce_token IS NOT NULL
      AND (
          NEW.quiesce_token IS NOT OLD.quiesce_token
          OR NEW.quiesce_generation IS NOT OLD.quiesce_generation
          OR NEW.quiesce_native_session_id IS NOT OLD.quiesce_native_session_id
          OR NEW.quiesce_process_id IS NOT OLD.quiesce_process_id
          OR NEW.quiesce_process_birth_identity
                 IS NOT OLD.quiesce_process_birth_identity
          OR NEW.quiesce_committed_version IS NOT OLD.quiesce_committed_version
      )
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff quiesce authorization is immutable once committed');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_process_evidence_once
    BEFORE UPDATE OF sigterm_requested_wall_at, sigterm_requested_monotonic_ns,
                     sigterm_request_result, child_exit_observed_wall_at,
                     child_exit_observed_monotonic_ns, child_exit_code,
                     child_exit_signal, sigterm_to_exit_ms,
                     delivery_exit_context, waitpid_reaped,
                     inject_cleanup_succeeded, delivery_cleanup_succeeded,
                     pty_cleanup_succeeded, screen_cleanup_succeeded,
                     write_queue_cleanup_succeeded, cleanup_completed_at
    ON terminal_handoffs
    WHEN OLD.sigterm_request_result != ''
      AND (
          NEW.sigterm_requested_wall_at IS NOT OLD.sigterm_requested_wall_at
          OR NEW.sigterm_requested_monotonic_ns
                 IS NOT OLD.sigterm_requested_monotonic_ns
          OR NEW.sigterm_request_result IS NOT OLD.sigterm_request_result
      )
      OR OLD.cleanup_completed_at IS NOT NULL
      AND (
          NEW.child_exit_observed_wall_at
                 IS NOT OLD.child_exit_observed_wall_at
          OR NEW.child_exit_observed_monotonic_ns
                 IS NOT OLD.child_exit_observed_monotonic_ns
          OR NEW.child_exit_code IS NOT OLD.child_exit_code
          OR NEW.child_exit_signal IS NOT OLD.child_exit_signal
          OR NEW.sigterm_to_exit_ms IS NOT OLD.sigterm_to_exit_ms
          OR NEW.delivery_exit_context IS NOT OLD.delivery_exit_context
          OR NEW.waitpid_reaped IS NOT OLD.waitpid_reaped
          OR NEW.inject_cleanup_succeeded IS NOT OLD.inject_cleanup_succeeded
          OR NEW.delivery_cleanup_succeeded IS NOT OLD.delivery_cleanup_succeeded
          OR NEW.pty_cleanup_succeeded IS NOT OLD.pty_cleanup_succeeded
          OR NEW.screen_cleanup_succeeded IS NOT OLD.screen_cleanup_succeeded
          OR NEW.write_queue_cleanup_succeeded
                 IS NOT OLD.write_queue_cleanup_succeeded
          OR NEW.cleanup_completed_at IS NOT OLD.cleanup_completed_at
      )
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff process evidence is immutable once recorded');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_process_evidence_insert
    BEFORE INSERT ON terminal_handoffs
    WHEN NOT (
        (
            (NEW.sigterm_request_result = ''
             AND NEW.sigterm_requested_wall_at IS NULL
             AND NEW.sigterm_requested_monotonic_ns IS NULL)
            OR
            (NEW.sigterm_request_result != ''
             AND NEW.sigterm_requested_wall_at IS NOT NULL
             AND NEW.sigterm_requested_monotonic_ns IS NOT NULL)
        )
        AND
        (
            (NEW.child_exit_observed_wall_at IS NULL
             AND NEW.child_exit_observed_monotonic_ns IS NULL
             AND NEW.child_exit_code IS NULL
             AND NEW.child_exit_signal IS NULL
             AND NEW.sigterm_to_exit_ms IS NULL
             AND NEW.delivery_exit_context = '')
            OR
            (NEW.child_exit_observed_wall_at IS NOT NULL
             AND NEW.child_exit_observed_monotonic_ns IS NOT NULL
             AND ((NEW.child_exit_code IS NOT NULL
                   AND NEW.child_exit_signal IS NULL)
                  OR
                  (NEW.child_exit_code IS NULL
                   AND NEW.child_exit_signal IS NOT NULL))
             AND (
                 (NEW.sigterm_request_result = 'sent'
                  AND NEW.sigterm_to_exit_ms IS NOT NULL)
                 OR
                 (NEW.sigterm_request_result = ''
                  AND NEW.sigterm_to_exit_ms IS NULL)
             )
             AND NEW.delivery_exit_context != '')
        )
        AND
        (
            (NEW.waitpid_reaped IS NULL
             AND NEW.inject_cleanup_succeeded IS NULL
             AND NEW.delivery_cleanup_succeeded IS NULL
             AND NEW.pty_cleanup_succeeded IS NULL
             AND NEW.screen_cleanup_succeeded IS NULL
             AND NEW.write_queue_cleanup_succeeded IS NULL
             AND NEW.cleanup_completed_at IS NULL)
            OR
            (NEW.waitpid_reaped IS NOT NULL
             AND NEW.inject_cleanup_succeeded IS NOT NULL
             AND NEW.delivery_cleanup_succeeded IS NOT NULL
             AND NEW.pty_cleanup_succeeded IS NOT NULL
             AND NEW.screen_cleanup_succeeded IS NOT NULL
             AND NEW.write_queue_cleanup_succeeded IS NOT NULL
             AND NEW.cleanup_completed_at IS NOT NULL)
        )
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff process evidence is inconsistent');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_process_evidence_update
    BEFORE UPDATE OF sigterm_requested_wall_at, sigterm_requested_monotonic_ns,
                     sigterm_request_result, child_exit_observed_wall_at,
                     child_exit_observed_monotonic_ns, child_exit_code,
                     child_exit_signal, sigterm_to_exit_ms,
                     delivery_exit_context, waitpid_reaped,
                     inject_cleanup_succeeded, delivery_cleanup_succeeded,
                     pty_cleanup_succeeded, screen_cleanup_succeeded,
                     write_queue_cleanup_succeeded, cleanup_completed_at
    ON terminal_handoffs
    WHEN NOT (
        (
            (NEW.sigterm_request_result = ''
             AND NEW.sigterm_requested_wall_at IS NULL
             AND NEW.sigterm_requested_monotonic_ns IS NULL)
            OR
            (NEW.sigterm_request_result != ''
             AND NEW.sigterm_requested_wall_at IS NOT NULL
             AND NEW.sigterm_requested_monotonic_ns IS NOT NULL)
        )
        AND
        (
            (NEW.child_exit_observed_wall_at IS NULL
             AND NEW.child_exit_observed_monotonic_ns IS NULL
             AND NEW.child_exit_code IS NULL
             AND NEW.child_exit_signal IS NULL
             AND NEW.sigterm_to_exit_ms IS NULL
             AND NEW.delivery_exit_context = '')
            OR
            (NEW.child_exit_observed_wall_at IS NOT NULL
             AND NEW.child_exit_observed_monotonic_ns IS NOT NULL
             AND ((NEW.child_exit_code IS NOT NULL
                   AND NEW.child_exit_signal IS NULL)
                  OR
                  (NEW.child_exit_code IS NULL
                   AND NEW.child_exit_signal IS NOT NULL))
             AND (
                 (NEW.sigterm_request_result = 'sent'
                  AND NEW.sigterm_to_exit_ms IS NOT NULL)
                 OR
                 (NEW.sigterm_request_result = ''
                  AND NEW.sigterm_to_exit_ms IS NULL)
             )
             AND NEW.delivery_exit_context != '')
        )
        AND
        (
            (NEW.waitpid_reaped IS NULL
             AND NEW.inject_cleanup_succeeded IS NULL
             AND NEW.delivery_cleanup_succeeded IS NULL
             AND NEW.pty_cleanup_succeeded IS NULL
             AND NEW.screen_cleanup_succeeded IS NULL
             AND NEW.write_queue_cleanup_succeeded IS NULL
             AND NEW.cleanup_completed_at IS NULL)
            OR
            (NEW.waitpid_reaped IS NOT NULL
             AND NEW.inject_cleanup_succeeded IS NOT NULL
             AND NEW.delivery_cleanup_succeeded IS NOT NULL
             AND NEW.pty_cleanup_succeeded IS NOT NULL
             AND NEW.screen_cleanup_succeeded IS NOT NULL
             AND NEW.write_queue_cleanup_succeeded IS NOT NULL
             AND NEW.cleanup_completed_at IS NOT NULL)
        )
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff process evidence is inconsistent');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_exit_requires_cleanup_insert
    BEFORE INSERT ON terminal_handoffs
    WHEN NEW.child_exit_observed_wall_at IS NOT NULL
      AND NEW.cleanup_completed_at IS NULL
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff exit evidence requires atomic cleanup evidence');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_exit_requires_cleanup_update
    BEFORE UPDATE OF child_exit_observed_wall_at, child_exit_observed_monotonic_ns,
                     child_exit_code, child_exit_signal, sigterm_to_exit_ms,
                     delivery_exit_context, waitpid_reaped,
                     inject_cleanup_succeeded, delivery_cleanup_succeeded,
                     pty_cleanup_succeeded, screen_cleanup_succeeded,
                     write_queue_cleanup_succeeded, cleanup_completed_at
    ON terminal_handoffs
    WHEN NEW.child_exit_observed_wall_at IS NOT NULL
      AND NEW.cleanup_completed_at IS NULL
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff exit evidence requires atomic cleanup evidence');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_stop_identity_insert
    BEFORE INSERT ON terminal_handoffs
    WHEN NEW.stop_observed_at IS NOT NULL OR NEW.stop_turn_id IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff Stop identity must be recorded atomically');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_stop_identity_once
    BEFORE UPDATE OF stop_observed_at, stop_turn_id
    ON terminal_handoffs
    WHEN (NEW.stop_observed_at IS NULL) != (NEW.stop_turn_id IS NULL)
      OR (OLD.stop_observed_at IS NOT NULL
          AND NEW.stop_observed_at IS NOT OLD.stop_observed_at)
      OR (OLD.stop_turn_id IS NOT NULL
          AND NEW.stop_turn_id IS NOT OLD.stop_turn_id)
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff Stop identity is incomplete or mutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_handoffs_target_validation_once
    BEFORE UPDATE OF target_validation_token, target_instructions_digest,
                     target_validated_at
    ON terminal_handoffs
    WHEN (NEW.target_validation_token IS NULL) !=
             (NEW.target_instructions_digest IS NULL)
      OR (NEW.target_validation_token IS NULL) !=
             (NEW.target_validated_at IS NULL)
      OR OLD.target_validation_token IS NOT NULL
         AND (
             NEW.target_validation_token IS NOT OLD.target_validation_token
             OR NEW.target_instructions_digest IS NOT OLD.target_instructions_digest
             OR NEW.target_validated_at IS NOT OLD.target_validated_at
         )
    BEGIN
        SELECT RAISE(ABORT, 'terminal handoff target validation is incomplete or immutable');
    END;

    CREATE TABLE IF NOT EXISTS terminal_chain_claims (
        chain_id          TEXT PRIMARY KEY,
        workspace         TEXT NOT NULL,
        outer_tty_device  INTEGER NOT NULL CHECK (outer_tty_device > 0),
        outer_tty_inode   INTEGER NOT NULL CHECK (outer_tty_inode > 0),
        state             TEXT NOT NULL CHECK (state IN ('active', 'released')),
        version           INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
        created_at        REAL NOT NULL,
        updated_at        REAL NOT NULL,
        released_at       REAL,
        CHECK (length(CAST(workspace AS BLOB)) BETWEEN 1 AND 4096),
        CHECK ((state = 'active' AND released_at IS NULL)
               OR (state = 'released' AND released_at IS NOT NULL)),
        FOREIGN KEY (chain_id) REFERENCES terminal_chains(id) ON DELETE RESTRICT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_chain_claim_workspace
        ON terminal_chain_claims(workspace) WHERE state = 'active';
    CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_chain_claim_tty
        ON terminal_chain_claims(outer_tty_device, outer_tty_inode)
        WHERE state = 'active';

    CREATE TABLE IF NOT EXISTS terminal_generation_processes (
        chain_id                    TEXT NOT NULL,
        generation                  INTEGER NOT NULL CHECK (generation >= 1),
        wrapper_pid                 INTEGER NOT NULL CHECK (wrapper_pid > 1),
        wrapper_pgid                INTEGER NOT NULL CHECK (wrapper_pgid > 0),
        wrapper_birth_identity      TEXT NOT NULL,
        child_pid                   INTEGER NOT NULL CHECK (child_pid > 1),
        child_pgid                  INTEGER NOT NULL CHECK (child_pgid > 1),
        child_birth_identity        TEXT NOT NULL,
        materialized_at             REAL NOT NULL,
        PRIMARY KEY (chain_id, generation),
        CHECK (wrapper_pid != child_pid),
        CHECK (child_pid = child_pgid),
        CHECK (length(CAST(wrapper_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(child_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        FOREIGN KEY (chain_id, generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT
    );

    CREATE TABLE IF NOT EXISTS terminal_generation_prepare_intents (
        chain_id                          TEXT NOT NULL,
        generation                        INTEGER NOT NULL CHECK (generation >= 1),
        launch_nonce                      TEXT NOT NULL,
        supervisor_process_id             TEXT NOT NULL,
        supervisor_process_birth_identity TEXT NOT NULL,
        control_object_kind                TEXT NOT NULL
                                               CHECK (control_object_kind IN ('chain', 'handoff')),
        control_object_id                  TEXT NOT NULL,
        control_version                    INTEGER NOT NULL CHECK (control_version >= 0),
        generation_version                 INTEGER NOT NULL CHECK (generation_version >= 0),
        started_at                         REAL NOT NULL,
        PRIMARY KEY (chain_id, generation),
        CHECK (length(CAST(launch_nonce AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(supervisor_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(supervisor_process_birth_identity AS BLOB))
               BETWEEN 1 AND 256),
        CHECK (length(CAST(control_object_id AS BLOB)) BETWEEN 1 AND 64),
        FOREIGN KEY (chain_id, generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT
    );

    CREATE TABLE IF NOT EXISTS terminal_recovery_attempts (
        id                                TEXT PRIMARY KEY,
        chain_id                          TEXT NOT NULL,
        sequence                          INTEGER NOT NULL CHECK (sequence >= 1),
        requested_chain_version           INTEGER NOT NULL
                                             CHECK (requested_chain_version >= 0),
        handoff_id                        TEXT,
        replaced_generation               INTEGER NOT NULL
                                             CHECK (replaced_generation >= 1),
        target_generation                 INTEGER
                                             CHECK (target_generation >= 1),
        plan_code                         TEXT NOT NULL,
        state                             TEXT NOT NULL CHECK (state IN (
                                              'intent',
                                              'authorized',
                                              'materialized',
                                              'awaiting_acceptance',
                                              'active',
                                              'manual',
                                              'failed'
                                          )),
        version                           INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
        supervisor_process_id             TEXT NOT NULL,
        supervisor_process_birth_identity TEXT NOT NULL,
        supervisor_pid                    INTEGER NOT NULL CHECK (supervisor_pid > 1),
        supervisor_pgid                   INTEGER NOT NULL CHECK (supervisor_pgid > 0),
        outer_foreground_pgid             INTEGER NOT NULL
                                             CHECK (outer_foreground_pgid > 0),
        outer_tty_device                  INTEGER NOT NULL CHECK (outer_tty_device > 0),
        outer_tty_inode                   INTEGER NOT NULL CHECK (outer_tty_inode > 0),
        created_at                        REAL NOT NULL,
        updated_at                        REAL NOT NULL,
        CHECK (supervisor_pgid = outer_foreground_pgid),
        CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(plan_code AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(supervisor_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(supervisor_process_birth_identity AS BLOB))
               BETWEEN 1 AND 256),
        CHECK ((state = 'manual' AND target_generation IS NULL)
               OR (state != 'manual' AND target_generation IS NOT NULL)),
        UNIQUE (chain_id, sequence),
        UNIQUE (chain_id, requested_chain_version),
        UNIQUE (chain_id, target_generation),
        FOREIGN KEY (chain_id) REFERENCES terminal_chains(id) ON DELETE RESTRICT,
        FOREIGN KEY (handoff_id) REFERENCES terminal_handoffs(id) ON DELETE RESTRICT,
        FOREIGN KEY (chain_id, replaced_generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT,
        FOREIGN KEY (chain_id, target_generation)
            REFERENCES terminal_generations(chain_id, generation) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_terminal_recovery_attempts_chain
        ON terminal_recovery_attempts(chain_id, sequence);

    CREATE TABLE IF NOT EXISTS terminal_recovery_absence_evidence (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        recovery_attempt_id    TEXT NOT NULL,
        subject                TEXT NOT NULL CHECK (subject IN (
                                   'supervisor', 'supervisor_process_group',
                                   'wrapper', 'child', 'child_process_group'
                               )),
        generation             INTEGER,
        pid                    INTEGER,
        pgid                   INTEGER,
        process_birth_identity TEXT,
        observation            TEXT NOT NULL CHECK (observation = 'absent'),
        method                 TEXT NOT NULL CHECK (method IN (
                                   'proc_birth_missing', 'process_group_missing'
                               )),
        observed_at            REAL NOT NULL,
        CHECK ((subject IN ('supervisor_process_group', 'child_process_group')
                AND pid IS NULL AND pgid > 1
                AND process_birth_identity IS NULL)
               OR
               (subject NOT IN ('supervisor_process_group', 'child_process_group')
                AND pid > 1 AND pgid IS NULL
                AND length(CAST(process_birth_identity AS BLOB))
                    BETWEEN 1 AND 256)),
        UNIQUE (recovery_attempt_id, subject, generation),
        FOREIGN KEY (recovery_attempt_id)
            REFERENCES terminal_recovery_attempts(id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_terminal_recovery_absence_attempt
        ON terminal_recovery_absence_evidence(recovery_attempt_id, id);

    CREATE TABLE IF NOT EXISTS terminal_target_validations (
        handoff_id          TEXT NOT NULL,
        target_generation   INTEGER NOT NULL CHECK (target_generation >= 1),
        validation_token    TEXT NOT NULL,
        instructions_digest TEXT NOT NULL,
        validated_at        REAL NOT NULL,
        PRIMARY KEY (handoff_id, target_generation),
        CHECK (length(CAST(validation_token AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(instructions_digest AS BLOB)) = 64),
        FOREIGN KEY (handoff_id) REFERENCES terminal_handoffs(id) ON DELETE RESTRICT
    );

    CREATE TRIGGER IF NOT EXISTS terminal_chain_claims_immutable_identity
    BEFORE UPDATE OF chain_id, workspace, outer_tty_device, outer_tty_inode
    ON terminal_chain_claims
    BEGIN
        SELECT RAISE(ABORT, 'terminal chain public claim identity is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_chain_claims_release_once
    BEFORE UPDATE OF state, version, released_at
    ON terminal_chain_claims
    WHEN NOT (
        OLD.state = 'active' AND NEW.state = 'released'
        AND NEW.version = OLD.version + 1
        AND OLD.released_at IS NULL AND NEW.released_at IS NOT NULL
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal chain public claim release is one-shot');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_generation_processes_no_update
    BEFORE UPDATE ON terminal_generation_processes
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation process evidence is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_generation_processes_no_delete
    BEFORE DELETE ON terminal_generation_processes
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation process evidence is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_generation_prepare_intents_no_update
    BEFORE UPDATE ON terminal_generation_prepare_intents
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation prepare intent is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_generation_prepare_intents_no_delete
    BEFORE DELETE ON terminal_generation_prepare_intents
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation prepare intent is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_recovery_attempts_immutable_identity
    BEFORE UPDATE OF id, chain_id, sequence, requested_chain_version, handoff_id,
                     replaced_generation, target_generation, plan_code,
                     supervisor_process_id, supervisor_process_birth_identity,
                     supervisor_pid, supervisor_pgid, outer_foreground_pgid,
                     outer_tty_device, outer_tty_inode, created_at
    ON terminal_recovery_attempts
    BEGIN
        SELECT RAISE(ABORT, 'terminal recovery attempt identity is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_recovery_absence_no_update
    BEFORE UPDATE ON terminal_recovery_absence_evidence
    BEGIN
        SELECT RAISE(ABORT, 'terminal recovery absence evidence is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_recovery_absence_no_delete
    BEFORE DELETE ON terminal_recovery_absence_evidence
    BEGIN
        SELECT RAISE(ABORT, 'terminal recovery absence evidence is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_target_validations_no_update
    BEFORE UPDATE ON terminal_target_validations
    BEGIN
        SELECT RAISE(ABORT, 'terminal target validation is immutable');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_target_validations_no_delete
    BEFORE DELETE ON terminal_target_validations
    BEGIN
        SELECT RAISE(ABORT, 'terminal target validation is immutable');
    END;

    CREATE TABLE IF NOT EXISTS terminal_transition_audit (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        chain_id               TEXT NOT NULL,
        object_kind            TEXT NOT NULL CHECK (object_kind IN (
                                     'chain', 'generation', 'handoff'
                                 )),
        object_id              TEXT NOT NULL,
        from_version           INTEGER NOT NULL CHECK (from_version >= -1),
        to_version             INTEGER NOT NULL CHECK (to_version = from_version + 1),
        from_state             TEXT,
        to_state               TEXT NOT NULL,
        actor_instance_name    TEXT NOT NULL,
        actor_hcom_session_id  TEXT NOT NULL,
        actor_process_id       TEXT NOT NULL,
        actor_process_birth_identity TEXT NOT NULL,
        actor_generation       INTEGER NOT NULL CHECK (actor_generation >= 1),
        actor_role             TEXT NOT NULL CHECK (actor_role IN (
                                     'source', 'target', 'supervisor'
                                 )),
        action                 TEXT NOT NULL,
        request_hash           TEXT NOT NULL,
        created_at             REAL NOT NULL,
        UNIQUE (object_kind, object_id, from_version),
        CHECK (length(CAST(object_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(from_state AS BLOB)) <= 64),
        CHECK (length(CAST(to_state AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(actor_instance_name AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(actor_hcom_session_id AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(actor_process_id AS BLOB)) BETWEEN 1 AND 128),
        CHECK (length(CAST(actor_process_birth_identity AS BLOB)) BETWEEN 1 AND 256),
        CHECK (length(CAST(action AS BLOB)) BETWEEN 1 AND 64),
        CHECK (length(CAST(request_hash AS BLOB)) = 64),
        FOREIGN KEY (chain_id) REFERENCES terminal_chains(id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_terminal_transition_audit_chain
        ON terminal_transition_audit(chain_id, id);

    CREATE TRIGGER IF NOT EXISTS terminal_generations_monotonic_insert
    BEFORE INSERT ON terminal_generations
    WHEN NEW.generation != COALESCE(
        (SELECT MAX(generation) + 1
         FROM terminal_generations
         WHERE chain_id = NEW.chain_id),
        1
    )
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation must be next monotonic integer');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_generations_immutable_identity
    BEFORE UPDATE OF launch_nonce, wrapper_process_id, process_birth_identity,
                     instance_name, hcom_session_id, native_session_id
    ON terminal_generations
    WHEN NEW.launch_nonce != OLD.launch_nonce
      OR (OLD.wrapper_process_id IS NOT NULL
          AND NEW.wrapper_process_id IS NOT OLD.wrapper_process_id)
      OR (OLD.process_birth_identity IS NOT NULL
          AND NEW.process_birth_identity IS NOT OLD.process_birth_identity)
      OR (OLD.instance_name IS NOT NULL
          AND NEW.instance_name IS NOT OLD.instance_name)
      OR (OLD.hcom_session_id IS NOT NULL
          AND NEW.hcom_session_id IS NOT OLD.hcom_session_id)
      OR (OLD.native_session_id IS NOT NULL
          AND NEW.native_session_id IS NOT OLD.native_session_id)
    BEGIN
        SELECT RAISE(ABORT, 'terminal generation identity is immutable once pinned');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_transition_audit_no_update
    BEFORE UPDATE ON terminal_transition_audit
    BEGIN
        SELECT RAISE(ABORT, 'terminal transition audit is append-only');
    END;

    CREATE TRIGGER IF NOT EXISTS terminal_transition_audit_no_delete
    BEFORE DELETE ON terminal_transition_audit
    BEGIN
        SELECT RAISE(ABORT, 'terminal transition audit is append-only');
    END;
";
const HANDOFF_V23_SCHEMA_SQL: &str = "
    CREATE TRIGGER IF NOT EXISTS terminal_chain_instance_tag_immutable
    BEFORE UPDATE OF tag ON instances
    WHEN EXISTS (
        SELECT 1
        FROM terminal_generations g
        JOIN terminal_chains c ON c.id = g.chain_id
        WHERE g.instance_name = OLD.name
          AND g.hcom_session_id = OLD.session_id
          AND COALESCE(NEW.tag, '') != c.tag
    )
    BEGIN
        SELECT RAISE(ABORT, 'managed chain tag is immutable');
    END;
";
const MIGRATIONS: &[(i32, &str)] = &[
    (
        17,
        "ALTER TABLE instances ADD COLUMN terminal_preset_requested TEXT DEFAULT '';
     ALTER TABLE instances ADD COLUMN terminal_preset_effective TEXT DEFAULT '';
     UPDATE instances
     SET terminal_preset_effective = json_extract(launch_context, '$.terminal_preset')
     WHERE launch_context != '' AND json_valid(launch_context) AND json_extract(launch_context, '$.terminal_preset') IS NOT NULL;",
    ),
    (18, REVIEW_SCHEMA_SQL),
    (19, HANDOFF_SCHEMA_SQL),
    // v20 is applied by `ensure_handoff_v20_columns` so stamped partial
    // migrations can repair individual missing columns without recreating or
    // archiving any typed control-plane table.
    (20, ""),
    // v21 is additive and repaired by `ensure_handoff_v21_columns`.
    (21, ""),
    // v22 adds public ownership, process, recovery, absence, and per-target
    // validation evidence without rewriting any Phase 3 lifecycle row.
    (22, HANDOFF_SCHEMA_SQL),
    // v23 pins one bounded group tag across every generation and recovery.
    (23, ""),
];

const HANDOFF_V23_COLUMNS: &[(&str, &str, &str)] = &[(
    "terminal_chains",
    "tag",
    "ALTER TABLE terminal_chains ADD COLUMN tag TEXT NOT NULL DEFAULT ''
     CHECK (length(CAST(tag AS BLOB)) <= 64)
     CHECK (tag NOT GLOB '*[^a-zA-Z0-9-]*')",
)];

const HANDOFF_V21_COLUMNS: &[(&str, &str, &str)] = &[
    (
        "terminal_handoffs",
        "stop_turn_id",
        "ALTER TABLE terminal_handoffs ADD COLUMN stop_turn_id TEXT
         CHECK (stop_turn_id IS NULL OR
                length(CAST(stop_turn_id AS BLOB)) BETWEEN 1 AND 256)",
    ),
    (
        "terminal_handoffs",
        "target_validation_token",
        "ALTER TABLE terminal_handoffs ADD COLUMN target_validation_token TEXT
         CHECK (target_validation_token IS NULL OR
                length(CAST(target_validation_token AS BLOB)) BETWEEN 1 AND 64)",
    ),
    (
        "terminal_handoffs",
        "target_instructions_digest",
        "ALTER TABLE terminal_handoffs ADD COLUMN target_instructions_digest TEXT
         CHECK (target_instructions_digest IS NULL OR
                length(CAST(target_instructions_digest AS BLOB)) = 64)",
    ),
    (
        "terminal_handoffs",
        "target_validated_at",
        "ALTER TABLE terminal_handoffs ADD COLUMN target_validated_at REAL",
    ),
];

const HANDOFF_V20_COLUMNS: &[(&str, &str, &str)] = &[
    (
        "terminal_chains",
        "supervisor_pid",
        "ALTER TABLE terminal_chains ADD COLUMN supervisor_pid INTEGER
         CHECK (supervisor_pid IS NULL OR supervisor_pid > 0)",
    ),
    (
        "terminal_chains",
        "supervisor_pgid",
        "ALTER TABLE terminal_chains ADD COLUMN supervisor_pgid INTEGER
         CHECK (supervisor_pgid IS NULL OR supervisor_pgid > 0)",
    ),
    (
        "terminal_chains",
        "outer_foreground_pgid",
        "ALTER TABLE terminal_chains ADD COLUMN outer_foreground_pgid INTEGER
         CHECK (outer_foreground_pgid IS NULL OR outer_foreground_pgid > 0)",
    ),
    (
        "terminal_chains",
        "outer_tty_device",
        "ALTER TABLE terminal_chains ADD COLUMN outer_tty_device INTEGER
         CHECK (outer_tty_device IS NULL OR outer_tty_device > 0)",
    ),
    (
        "terminal_chains",
        "outer_tty_inode",
        "ALTER TABLE terminal_chains ADD COLUMN outer_tty_inode INTEGER
         CHECK (outer_tty_inode IS NULL OR outer_tty_inode > 0)",
    ),
    (
        "terminal_handoffs",
        "sigterm_requested_wall_at",
        "ALTER TABLE terminal_handoffs ADD COLUMN sigterm_requested_wall_at REAL",
    ),
    (
        "terminal_handoffs",
        "sigterm_requested_monotonic_ns",
        "ALTER TABLE terminal_handoffs ADD COLUMN sigterm_requested_monotonic_ns INTEGER
         CHECK (sigterm_requested_monotonic_ns IS NULL
                OR sigterm_requested_monotonic_ns >= 0)",
    ),
    (
        "terminal_handoffs",
        "sigterm_request_result",
        "ALTER TABLE terminal_handoffs ADD COLUMN sigterm_request_result TEXT NOT NULL DEFAULT ''
         CHECK (sigterm_request_result IN (
             '', 'sent', 'not_found', 'permission_denied', 'error'
         ))",
    ),
    (
        "terminal_handoffs",
        "child_exit_observed_wall_at",
        "ALTER TABLE terminal_handoffs ADD COLUMN child_exit_observed_wall_at REAL",
    ),
    (
        "terminal_handoffs",
        "child_exit_observed_monotonic_ns",
        "ALTER TABLE terminal_handoffs ADD COLUMN child_exit_observed_monotonic_ns INTEGER
         CHECK (child_exit_observed_monotonic_ns IS NULL
                OR child_exit_observed_monotonic_ns >= 0)",
    ),
    (
        "terminal_handoffs",
        "child_exit_code",
        "ALTER TABLE terminal_handoffs ADD COLUMN child_exit_code INTEGER",
    ),
    (
        "terminal_handoffs",
        "child_exit_signal",
        "ALTER TABLE terminal_handoffs ADD COLUMN child_exit_signal INTEGER",
    ),
    (
        "terminal_handoffs",
        "sigterm_to_exit_ms",
        "ALTER TABLE terminal_handoffs ADD COLUMN sigterm_to_exit_ms INTEGER
         CHECK (sigterm_to_exit_ms IS NULL
                OR sigterm_to_exit_ms BETWEEN 0 AND 86400000)",
    ),
    (
        "terminal_handoffs",
        "delivery_exit_context",
        "ALTER TABLE terminal_handoffs ADD COLUMN delivery_exit_context TEXT NOT NULL DEFAULT ''
         CHECK (delivery_exit_context IN ('', 'exit:closed', 'exit:killed'))",
    ),
    (
        "terminal_handoffs",
        "waitpid_reaped",
        "ALTER TABLE terminal_handoffs ADD COLUMN waitpid_reaped INTEGER
         CHECK (waitpid_reaped IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "inject_cleanup_succeeded",
        "ALTER TABLE terminal_handoffs ADD COLUMN inject_cleanup_succeeded INTEGER
         CHECK (inject_cleanup_succeeded IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "delivery_cleanup_succeeded",
        "ALTER TABLE terminal_handoffs ADD COLUMN delivery_cleanup_succeeded INTEGER
         CHECK (delivery_cleanup_succeeded IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "pty_cleanup_succeeded",
        "ALTER TABLE terminal_handoffs ADD COLUMN pty_cleanup_succeeded INTEGER
         CHECK (pty_cleanup_succeeded IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "screen_cleanup_succeeded",
        "ALTER TABLE terminal_handoffs ADD COLUMN screen_cleanup_succeeded INTEGER
         CHECK (screen_cleanup_succeeded IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "write_queue_cleanup_succeeded",
        "ALTER TABLE terminal_handoffs ADD COLUMN write_queue_cleanup_succeeded INTEGER
         CHECK (write_queue_cleanup_succeeded IN (0, 1))",
    ),
    (
        "terminal_handoffs",
        "cleanup_completed_at",
        "ALTER TABLE terminal_handoffs ADD COLUMN cleanup_completed_at REAL",
    ),
];

const HANDOFF_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    (
        "terminal_chains",
        &[
            "id",
            "workspace",
            "tool",
            "tag",
            "model_ref",
            "reasoning_ref",
            "permission_policy_ref",
            "policy_ref",
            "supervisor_process_id",
            "supervisor_process_birth_identity",
            "supervisor_pid",
            "supervisor_pgid",
            "outer_foreground_pgid",
            "outer_tty_device",
            "outer_tty_inode",
            "current_generation",
            "state",
            "version",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "terminal_generations",
        &[
            "chain_id",
            "generation",
            "launch_nonce",
            "wrapper_process_id",
            "process_birth_identity",
            "instance_name",
            "hcom_session_id",
            "native_session_id",
            "state",
            "version",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "terminal_handoffs",
        &[
            "id",
            "chain_id",
            "source_generation",
            "target_generation",
            "source_launch_nonce",
            "source_instance_name",
            "source_hcom_session_id",
            "source_native_session_id",
            "source_wrapper_process_id",
            "source_process_birth_identity",
            "bundle_event_id",
            "bundle_digest",
            "bundle_size_bytes",
            "workspace",
            "revision",
            "branch",
            "dirty_summary",
            "policy_ref",
            "state",
            "version",
            "quiesce_token",
            "quiesce_generation",
            "quiesce_native_session_id",
            "quiesce_process_id",
            "quiesce_process_birth_identity",
            "quiesce_committed_version",
            "stop_observed_at",
            "stop_turn_id",
            "sigterm_requested_wall_at",
            "sigterm_requested_monotonic_ns",
            "sigterm_request_result",
            "child_exit_observed_wall_at",
            "child_exit_observed_monotonic_ns",
            "child_exit_code",
            "child_exit_signal",
            "sigterm_to_exit_ms",
            "delivery_exit_context",
            "waitpid_reaped",
            "inject_cleanup_succeeded",
            "delivery_cleanup_succeeded",
            "pty_cleanup_succeeded",
            "screen_cleanup_succeeded",
            "write_queue_cleanup_succeeded",
            "cleanup_completed_at",
            "target_validation_token",
            "target_instructions_digest",
            "target_validated_at",
            "failure_kind",
            "failure_reason",
            "created_at",
            "updated_at",
            "committed_at",
            "accepted_at",
        ],
    ),
    (
        "terminal_transition_audit",
        &[
            "id",
            "chain_id",
            "object_kind",
            "object_id",
            "from_version",
            "to_version",
            "from_state",
            "to_state",
            "actor_instance_name",
            "actor_hcom_session_id",
            "actor_process_id",
            "actor_process_birth_identity",
            "actor_generation",
            "actor_role",
            "action",
            "request_hash",
            "created_at",
        ],
    ),
    (
        "terminal_chain_claims",
        &[
            "chain_id",
            "workspace",
            "outer_tty_device",
            "outer_tty_inode",
            "state",
            "version",
            "created_at",
            "updated_at",
            "released_at",
        ],
    ),
    (
        "terminal_generation_processes",
        &[
            "chain_id",
            "generation",
            "wrapper_pid",
            "wrapper_pgid",
            "wrapper_birth_identity",
            "child_pid",
            "child_pgid",
            "child_birth_identity",
            "materialized_at",
        ],
    ),
    (
        "terminal_generation_prepare_intents",
        &[
            "chain_id",
            "generation",
            "launch_nonce",
            "supervisor_process_id",
            "supervisor_process_birth_identity",
            "control_object_kind",
            "control_object_id",
            "control_version",
            "generation_version",
            "started_at",
        ],
    ),
    (
        "terminal_recovery_attempts",
        &[
            "id",
            "chain_id",
            "sequence",
            "requested_chain_version",
            "handoff_id",
            "replaced_generation",
            "target_generation",
            "plan_code",
            "state",
            "version",
            "supervisor_process_id",
            "supervisor_process_birth_identity",
            "supervisor_pid",
            "supervisor_pgid",
            "outer_foreground_pgid",
            "outer_tty_device",
            "outer_tty_inode",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "terminal_recovery_absence_evidence",
        &[
            "id",
            "recovery_attempt_id",
            "subject",
            "generation",
            "pid",
            "pgid",
            "process_birth_identity",
            "observation",
            "method",
            "observed_at",
        ],
    ),
    (
        "terminal_target_validations",
        &[
            "handoff_id",
            "target_generation",
            "validation_token",
            "instructions_digest",
            "validated_at",
        ],
    ),
];

const HANDOFF_SCHEMA_OBJECTS: &[(&str, &str)] = &[
    ("index", "idx_terminal_generation_launch_nonce"),
    ("index", "idx_terminal_generation_process"),
    ("index", "idx_terminal_handoffs_one_non_final"),
    ("index", "idx_terminal_handoffs_chain"),
    ("index", "idx_terminal_transition_audit_chain"),
    ("index", "idx_terminal_chain_claim_workspace"),
    ("index", "idx_terminal_chain_claim_tty"),
    ("index", "idx_terminal_recovery_attempts_chain"),
    ("index", "idx_terminal_recovery_absence_attempt"),
    ("trigger", "terminal_chains_immutable_identity_policy"),
    ("trigger", "terminal_chains_outer_evidence_insert"),
    ("trigger", "terminal_chains_outer_evidence_update"),
    ("trigger", "terminal_chain_instance_tag_immutable"),
    ("trigger", "terminal_generations_monotonic_insert"),
    ("trigger", "terminal_generations_immutable_identity"),
    ("trigger", "terminal_handoffs_immutable_snapshot"),
    ("trigger", "terminal_handoffs_quiesce_once"),
    ("trigger", "terminal_handoffs_process_evidence_once"),
    ("trigger", "terminal_handoffs_process_evidence_insert"),
    ("trigger", "terminal_handoffs_process_evidence_update"),
    ("trigger", "terminal_handoffs_exit_requires_cleanup_insert"),
    ("trigger", "terminal_handoffs_exit_requires_cleanup_update"),
    ("trigger", "terminal_handoffs_stop_identity_insert"),
    ("trigger", "terminal_handoffs_stop_identity_once"),
    ("trigger", "terminal_handoffs_target_validation_once"),
    ("trigger", "terminal_transition_audit_no_update"),
    ("trigger", "terminal_transition_audit_no_delete"),
    ("trigger", "terminal_chain_claims_immutable_identity"),
    ("trigger", "terminal_chain_claims_release_once"),
    ("trigger", "terminal_generation_processes_no_update"),
    ("trigger", "terminal_generation_processes_no_delete"),
    ("trigger", "terminal_generation_prepare_intents_no_update"),
    ("trigger", "terminal_generation_prepare_intents_no_delete"),
    ("trigger", "terminal_recovery_attempts_immutable_identity"),
    ("trigger", "terminal_recovery_absence_no_update"),
    ("trigger", "terminal_recovery_absence_no_delete"),
    ("trigger", "terminal_target_validations_no_update"),
    ("trigger", "terminal_target_validations_no_delete"),
];

const REVIEW_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    (
        "review_runs",
        &[
            "id",
            "task",
            "workspace",
            "thread",
            "developer_name",
            "developer_session_id",
            "reviewer_name",
            "reviewer_session_id",
            "state",
            "round",
            "max_rounds",
            "version",
            "last_message_event_id",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "review_transitions",
        &[
            "id",
            "workflow_id",
            "from_version",
            "to_version",
            "round",
            "actor_name",
            "actor_session_id",
            "actor_role",
            "action",
            "from_state",
            "to_state",
            "summary",
            "payload_hash",
            "message_event_id",
            "created_at",
        ],
    ),
];

const REVIEW_SCHEMA_OBJECTS: &[(&str, &str)] = &[
    ("index", "idx_review_runs_active_pair"),
    ("index", "idx_review_transitions_workflow"),
];

fn schema_objects_are_complete(
    conn: &Connection,
    tables: &[(&str, &[&str])],
    objects: &[(&str, &str)],
) -> Result<bool> {
    for (table, expected_columns) in tables {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let actual: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<_>>()?;
        if !expected_columns
            .iter()
            .all(|column| actual.contains(*column))
        {
            return Ok(false);
        }
    }
    for (kind, name) in objects {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2
             )",
            rusqlite::params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
    }
    Ok(true)
}

fn review_schema_is_complete(conn: &Connection) -> Result<bool> {
    schema_objects_are_complete(conn, REVIEW_TABLE_COLUMNS, REVIEW_SCHEMA_OBJECTS)
}

fn handoff_schema_is_complete(conn: &Connection) -> Result<bool> {
    schema_objects_are_complete(conn, HANDOFF_TABLE_COLUMNS, HANDOFF_SCHEMA_OBJECTS)
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let present = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|row| row.ok())
        .any(|name| name == column);
    Ok(present)
}

/// Add only missing v20 evidence columns, then refresh the triggers whose
/// definitions reference them. This is safe for both a real v19 migration and
/// a v20 database that was stamped after only part of the ALTER sequence.
fn ensure_handoff_v20_columns(conn: &Connection) -> Result<()> {
    let have_chain: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'terminal_chains'
         )",
        [],
        |row| row.get(0),
    )?;
    let have_handoff: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'terminal_handoffs'
         )",
        [],
        |row| row.get(0),
    )?;
    if !have_chain || !have_handoff {
        conn.execute_batch(HANDOFF_SCHEMA_SQL)?;
    }
    for (table, column, sql) in HANDOFF_V20_COLUMNS {
        if !table_has_column(conn, table, column)? {
            conn.execute_batch(sql)?;
        }
    }
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS terminal_chains_immutable_identity_policy;
         DROP TRIGGER IF EXISTS terminal_chains_outer_evidence_insert;
         DROP TRIGGER IF EXISTS terminal_chains_outer_evidence_update;
         DROP TRIGGER IF EXISTS terminal_handoffs_process_evidence_once;
         DROP TRIGGER IF EXISTS terminal_handoffs_process_evidence_insert;
         DROP TRIGGER IF EXISTS terminal_handoffs_process_evidence_update;",
    )?;
    conn.execute_batch(HANDOFF_SCHEMA_SQL)?;
    Ok(())
}

/// Add the Phase 3 Stop/validation columns and refresh the fail-closed
/// evidence triggers. This remains safe if a prior open was interrupted
/// between individual ALTER statements.
fn ensure_handoff_v21_columns(conn: &Connection) -> Result<()> {
    ensure_handoff_v20_columns(conn)?;
    for (table, column, sql) in HANDOFF_V21_COLUMNS {
        if !table_has_column(conn, table, column)? {
            conn.execute_batch(sql)?;
        }
    }
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS terminal_handoffs_exit_requires_cleanup_insert;
         DROP TRIGGER IF EXISTS terminal_handoffs_exit_requires_cleanup_update;
         DROP TRIGGER IF EXISTS terminal_handoffs_stop_identity_insert;
         DROP TRIGGER IF EXISTS terminal_handoffs_stop_identity_once;
         DROP TRIGGER IF EXISTS terminal_handoffs_target_validation_once;",
    )?;
    conn.execute_batch(HANDOFF_SCHEMA_SQL)?;
    Ok(())
}

/// Add the immutable chain tag without rewriting any existing handoff row.
/// Existing v22 chains migrate to the untagged empty value.
fn ensure_handoff_v23_columns(conn: &Connection) -> Result<()> {
    ensure_handoff_v21_columns(conn)?;
    for (table, column, sql) in HANDOFF_V23_COLUMNS {
        if !table_has_column(conn, table, column)? {
            conn.execute_batch(sql)?;
        }
    }
    conn.execute_batch("DROP TRIGGER IF EXISTS terminal_chains_immutable_identity_policy;")?;
    conn.execute_batch(HANDOFF_SCHEMA_SQL)?;
    conn.execute_batch(HANDOFF_V23_SCHEMA_SQL)?;
    Ok(())
}

/// Schema compatibility check result
enum SchemaCompat {
    /// Schema is compatible (or fresh DB) — proceed with init_db
    Ok,
    /// Schema is incompatible — archive, reconnect, reinit
    NeedsArchive(String, Option<i32>),
    /// DB is newer than code — stale process, work with existing schema
    StaleProcess,
}

/// Database handle for hcom operations
pub struct HcomDb {
    conn: Connection,
    db_path: std::path::PathBuf,
    db_inode: u64,
}

fn get_inode(path: &std::path::Path) -> u64 {
    crate::sys::fs::file_id(path)
}

impl HcomDb {
    /// Access the underlying SQLite connection (for direct queries).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Access the filesystem path backing this DB handle.
    pub fn path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Open the hcom database at ~/.hcom/hcom.db with schema migration/compat.
    pub fn open() -> Result<Self> {
        let db_path = crate::paths::db_path();
        Self::open_at(&db_path)
    }

    /// Open the hcom database at a specific path with schema migration/compat.
    pub fn open_at(db_path: &std::path::Path) -> Result<Self> {
        let mut db = Self::open_raw(db_path)?;
        db.ensure_schema()?;
        Ok(db)
    }

    /// Open DB connection without schema checks (for testing only).
    pub fn open_raw(db_path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create db directory: {}", parent.display()))?;
        }
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

        // Enable WAL mode for concurrent access + foreign keys for CASCADE
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
        )?;

        let inode = get_inode(db_path);

        Ok(Self {
            conn,
            db_path: db_path.to_path_buf(),
            db_inode: inode,
        })
    }

    /// Reconnect if the DB file was replaced (e.g., by hcom reset / schema bump).
    /// Long-lived threads (PTY delivery, listeners) hold an open connection to the
    /// old inode; this moves them onto the new DB file.
    /// Returns true if reconnection happened.
    pub fn reconnect_if_stale(&mut self) -> bool {
        let current_inode = get_inode(&self.db_path);
        if current_inode == 0 || current_inode == self.db_inode {
            return false;
        }
        // DB file replaced — reconnect
        use crate::log::{log_error, log_info};
        match Connection::open(&self.db_path) {
            Ok(new_conn) => {
                if let Err(e) = new_conn.execute_batch(
                    "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
                ) {
                    use crate::log::log_warn;
                    log_warn(
                        "native",
                        "db.pragma_fail",
                        &format!("PRAGMA setup failed after reconnect: {}", e),
                    );
                }
                log_info(
                    "native",
                    "db.reconnect",
                    &format!(
                        "DB file replaced (inode {} -> {}), reconnected",
                        self.db_inode, current_inode
                    ),
                );
                self.conn = new_conn;
                self.db_inode = current_inode;
                true
            }
            Err(e) => {
                log_error(
                    "native",
                    "db.reconnect_fail",
                    &format!("Failed to reconnect: {}", e),
                );
                false
            }
        }
    }

    /// Initialize database schema. Idempotent (IF NOT EXISTS).
    /// Creates all tables, indexes, events_v view, FTS5 virtual table + trigger,
    /// and sets PRAGMA user_version.
    pub fn init_db(&self) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        // Re-read after taking the writer lock so concurrent fresh opens cannot
        // interleave schema creation or stamp a partial database.
        let current: i32 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if current == SCHEMA_VERSION
            && review_schema_is_complete(&tx)?
            && handoff_schema_is_complete(&tx)?
        {
            tx.commit()?;
            return Ok(());
        }

        tx.execute_batch(
            "
            -- Events table
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                instance TEXT NOT NULL,
                data TEXT NOT NULL
            );

            -- Notify endpoints
            CREATE TABLE IF NOT EXISTS notify_endpoints (
                instance TEXT NOT NULL,
                kind TEXT NOT NULL,
                port INTEGER NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (instance, kind)
            );
            CREATE INDEX IF NOT EXISTS idx_notify_endpoints_instance ON notify_endpoints(instance);
            CREATE INDEX IF NOT EXISTS idx_notify_endpoints_port ON notify_endpoints(port);

            -- Process bindings
            CREATE TABLE IF NOT EXISTS process_bindings (
                process_id TEXT PRIMARY KEY,
                session_id TEXT,
                instance_name TEXT,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_process_bindings_instance ON process_bindings(instance_name);
            CREATE INDEX IF NOT EXISTS idx_process_bindings_session ON process_bindings(session_id);

            -- Session bindings
            CREATE TABLE IF NOT EXISTS session_bindings (
                session_id TEXT PRIMARY KEY,
                instance_name TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_session_bindings_instance ON session_bindings(instance_name);

            -- Instances table
            CREATE TABLE IF NOT EXISTS instances (
                name TEXT PRIMARY KEY,
                session_id TEXT UNIQUE,
                parent_session_id TEXT,
                parent_name TEXT,
                tag TEXT,
                last_event_id INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                status_time INTEGER DEFAULT 0,
                status_context TEXT DEFAULT '',
                status_detail TEXT DEFAULT '',
                last_stop INTEGER DEFAULT 0,
                directory TEXT,
                created_at REAL NOT NULL,
                transcript_path TEXT DEFAULT '',
                tcp_mode INTEGER DEFAULT 0,
                wait_timeout INTEGER,
                background INTEGER DEFAULT 0,
                background_log_file TEXT DEFAULT '',
                name_announced INTEGER DEFAULT 0,
                agent_id TEXT UNIQUE,
                running_tasks TEXT DEFAULT '',
                origin_device_id TEXT DEFAULT '',
                hints TEXT DEFAULT '',
                subagent_timeout INTEGER,
                tool TEXT DEFAULT 'claude',
                launch_args TEXT DEFAULT '',
                terminal_preset_requested TEXT DEFAULT '',
                terminal_preset_effective TEXT DEFAULT '',
                idle_since TEXT DEFAULT '',
                pid INTEGER DEFAULT NULL,
                launch_context TEXT DEFAULT '',
                FOREIGN KEY (parent_session_id) REFERENCES instances(session_id) ON DELETE SET NULL
            );

            -- KV table
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);

            -- Event indexes
            CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_type ON events(type);
            CREATE INDEX IF NOT EXISTS idx_instance ON events(instance);
            CREATE INDEX IF NOT EXISTS idx_type_instance ON events(type, instance);

            -- Instance indexes
            CREATE INDEX IF NOT EXISTS idx_session_id ON instances(session_id);
            CREATE INDEX IF NOT EXISTS idx_parent_session_id ON instances(parent_session_id);
            CREATE INDEX IF NOT EXISTS idx_parent_name ON instances(parent_name);
            CREATE INDEX IF NOT EXISTS idx_created_at ON instances(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_status ON instances(status);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_id_unique ON instances(agent_id) WHERE agent_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_instances_origin ON instances(origin_device_id);

            -- Flattened events view (DROP first to apply schema changes)
            DROP VIEW IF EXISTS events_v;
            CREATE VIEW IF NOT EXISTS events_v AS
            SELECT
                id, timestamp, type, instance, data,
                json_extract(data, '$.from') as msg_from,
                json_extract(data, '$.text') as msg_text,
                json_extract(data, '$.scope') as msg_scope,
                json_extract(data, '$.sender_kind') as msg_sender_kind,
                json_extract(data, '$.delivered_to') as msg_delivered_to,
                json_extract(data, '$.mentions') as msg_mentions,
                json_extract(data, '$.intent') as msg_intent,
                json_extract(data, '$.thread') as msg_thread,
                json_extract(data, '$.reply_to') as msg_reply_to,
                json_extract(data, '$.reply_to_local') as msg_reply_to_local,
                json_extract(data, '$.bundle_id') as bundle_id,
                json_extract(data, '$.title') as bundle_title,
                json_extract(data, '$.description') as bundle_description,
                json_extract(data, '$.extends') as bundle_extends,
                json_extract(data, '$.refs.events') as bundle_events,
                json_extract(data, '$.refs.files') as bundle_files,
                json_extract(data, '$.refs.transcript') as bundle_transcript,
                json_extract(data, '$.created_by') as bundle_created_by,
                json_extract(data, '$.status') as status_val,
                json_extract(data, '$.context') as status_context,
                json_extract(data, '$.detail') as status_detail,
                json_extract(data, '$.action') as life_action,
                json_extract(data, '$.by') as life_by,
                json_extract(data, '$.batch_id') as life_batch_id,
                json_extract(data, '$.reason') as life_reason
            FROM events;

            -- FTS5 full-text search index
            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                searchable,
                tokenize='unicode61'
            );
            CREATE TRIGGER IF NOT EXISTS events_fts_insert
            AFTER INSERT ON events BEGIN
                INSERT INTO events_fts(rowid, searchable) VALUES (
                    new.id,
                    COALESCE(json_extract(new.data, '$.text'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.from'), '') || ' ' ||
                    COALESCE(new.instance, '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.context'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.detail'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.action'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.reason'), '')
                );
            END;
            ",
        )?;

        tx.execute_batch(REVIEW_SCHEMA_SQL)?;
        let has_handoff_tables: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'terminal_handoffs'
             )",
            [],
            |row| row.get(0),
        )?;
        if has_handoff_tables {
            ensure_handoff_v23_columns(&tx)?;
        }
        tx.execute_batch(HANDOFF_SCHEMA_SQL)?;
        tx.execute_batch(HANDOFF_V23_SCHEMA_SQL)?;
        if !review_schema_is_complete(&tx)? || !handoff_schema_is_complete(&tx)? {
            anyhow::bail!("migrated control-plane schema is incomplete");
        }

        // Set schema version
        tx.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION))?;
        tx.commit()?;

        Ok(())
    }

    /// Full schema bootstrap: check version, archive if mismatched, reconnect, init.
    ///
    /// Checks schema version, archives DB if mismatched, reconnects, and reinitializes.
    /// Call after open() for production use.
    pub fn ensure_schema(&mut self) -> Result<()> {
        match self.check_schema_compat()? {
            SchemaCompat::Ok => {
                // The compatibility check above is deliberately read-only. A
                // complete current database is the overwhelmingly common
                // case, so return without taking a writer lock. Fresh,
                // partially initialized, or repairable databases still enter
                // init_db(), which re-checks version/completeness after
                // acquiring BEGIN IMMEDIATE.
                let current: i32 = self
                    .conn
                    .query_row("PRAGMA user_version", [], |row| row.get(0))?;
                if current != SCHEMA_VERSION
                    || !review_schema_is_complete(&self.conn)?
                    || !handoff_schema_is_complete(&self.conn)?
                {
                    self.init_db()?;
                }
                Ok(())
            }
            SchemaCompat::NeedsArchive(reason, old_version) => {
                if let Some(version) = old_version {
                    // If version matches but columns are missing (stamped without migration),
                    // repair by running migrations from version-1.
                    let migrate_from = if version == SCHEMA_VERSION {
                        version - 1
                    } else {
                        version
                    };
                    match self.try_apply_migrations(migrate_from) {
                        Ok(true) => {
                            if matches!(self.check_schema_compat()?, SchemaCompat::Ok) {
                                return Ok(());
                            }
                        }
                        Ok(false) => {}
                        Err(e) => {
                            crate::log::log_warn(
                                "db",
                                "schema.migration_failed",
                                &format!("v{} -> v{} failed: {}", migrate_from, SCHEMA_VERSION, e),
                            );
                        }
                    }
                    // v18 is the data-preserving handoff migration boundary.
                    // Never archive a v18+ database to recover a missing or
                    // malformed handoff schema: returning an error preserves
                    // all existing events, instances, bindings, and reviews.
                    if version >= 18 {
                        anyhow::bail!(
                            "Failed to repair migrated control-plane schema in place; database was left unchanged"
                        );
                    }
                }
                eprintln!("hcom: {}, archiving...", reason);

                // Snapshot running instances to pidtrack before archive so orphan
                // recovery can re-register them into the fresh DB.
                self.snapshot_running_to_pidtrack();

                // Release our handle to the old DB file before archiving. Windows
                // refuses to delete a file that still has an open handle; Unix
                // unlinks an open file fine, so this is a no-op there.
                //
                // This only releases *our own* connection. If any other hcom
                // process — another agent instance, a relay worker, a hook
                // invocation — has the same DB file open at this moment, the
                // `remove_file` inside `archive_db_at` below can still fail on
                // Windows; see the doc comment there for why closing our own
                // handle isn't sufficient in general.
                self.conn = Connection::open_in_memory()?;

                // Archive the old DB
                let archive_path = Self::archive_db_at(&self.db_path)?;
                if let Some(ref path) = archive_path {
                    eprintln!("hcom: Archived to {}", path);
                    eprintln!("       Query with: hcom archive 1");
                }

                // Reconnect to fresh DB file
                let new_conn = Connection::open(&self.db_path).with_context(|| {
                    format!(
                        "Failed to reopen DB after archive: {}",
                        self.db_path.display()
                    )
                })?;
                new_conn.execute_batch(
                    "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
                )?;
                self.conn = new_conn;
                self.db_inode = get_inode(&self.db_path);

                // Init fresh schema
                self.init_db()?;

                // Log reset event to fresh DB
                self.log_reset_event()?;

                Ok(())
            }
            SchemaCompat::StaleProcess => {
                // DB is newer than our code — work with it, don't archive
                Ok(())
            }
        }
    }

    /// Internal: check schema compatibility without taking action.
    fn check_schema_compat(&self) -> Result<SchemaCompat> {
        let version: i32 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap_or(0);

        // Check what tables exist
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();

        let required: std::collections::HashSet<&str> = [
            "events",
            "instances",
            "kv",
            "notify_endpoints",
            "session_bindings",
        ]
        .into_iter()
        .collect();

        if version == 0 {
            // Race handling: another process may be initializing
            if !tables.is_empty() && required.iter().any(|t| tables.contains(*t)) {
                let mut resolved_version = 0i32;
                for _ in 0..20 {
                    let v2: i32 = self
                        .conn
                        .query_row("PRAGMA user_version", [], |row| row.get(0))
                        .unwrap_or(0);
                    if v2 != 0 {
                        resolved_version = v2;
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                if resolved_version == SCHEMA_VERSION {
                    return Ok(SchemaCompat::Ok);
                }
                if resolved_version > SCHEMA_VERSION {
                    crate::log::log_warn(
                        "db",
                        "schema.stale_process",
                        &format!(
                            "DB v{} > code v{}, working with newer schema",
                            resolved_version, SCHEMA_VERSION
                        ),
                    );
                    return Ok(SchemaCompat::StaleProcess);
                }
                // Timeout exhausted — another process is still initializing.
                // Return Ok rather than falling through to NeedsArchive which
                // would incorrectly archive a valid in-progress DB.
                if resolved_version == 0 {
                    crate::log::log_warn(
                        "db",
                        "schema.init_timeout",
                        "Concurrent init poll timed out, assuming OK",
                    );
                    return Ok(SchemaCompat::Ok);
                }
            }
            // Fresh DB (no tables) - safe to initialize
            if tables.is_empty() {
                return Ok(SchemaCompat::Ok);
            }
            // Pre-versioned DB with our tables - needs archive
            if required.iter().any(|t| tables.contains(*t)) {
                return Ok(SchemaCompat::NeedsArchive(
                    "Pre-versioned DB found".to_string(),
                    None,
                ));
            }
            // Has tables but not ours - fresh enough
            return Ok(SchemaCompat::Ok);
        }

        if version != SCHEMA_VERSION {
            if version > SCHEMA_VERSION {
                // DB newer than code - stale process, work with it
                crate::log::log_warn(
                    "db",
                    "schema.stale_process",
                    &format!(
                        "DB v{} > code v{}, working with newer schema",
                        version, SCHEMA_VERSION
                    ),
                );
                return Ok(SchemaCompat::StaleProcess);
            }
            // DB older - needs archive
            return Ok(SchemaCompat::NeedsArchive(
                format!(
                    "DB version mismatch (DB v{}, code v{})",
                    version, SCHEMA_VERSION
                ),
                Some(version),
            ));
        }

        // Verify the long-standing core tables first. Corruption here follows
        // the pre-existing archive/recreate recovery path; the Phase 1
        // data-preserving migration path below is only for review/handoff
        // schema introduced by explicit migrations.
        let have_all = required.iter().all(|t| tables.contains(*t));
        if !have_all {
            let missing: Vec<&&str> = required.iter().filter(|t| !tables.contains(**t)).collect();
            return Ok(SchemaCompat::NeedsArchive(
                format!("DB missing tables {:?}", missing),
                None,
            ));
        }

        // Column guard: verify all expected columns exist (catches partial schema from
        // version bump before migration was written)
        let missing_col: Option<String> = self
            .conn
            .prepare("PRAGMA table_info(instances)")
            .and_then(|mut s| {
                let cols: Vec<String> = s
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|r| r.ok())
                    .collect();
                let required = [
                    "tool",
                    "terminal_preset_requested",
                    "terminal_preset_effective",
                ];
                Ok(required
                    .iter()
                    .find(|c| !cols.contains(&c.to_string()))
                    .map(|s| s.to_string()))
            })
            .unwrap_or(None);
        if let Some(col) = missing_col {
            return Ok(SchemaCompat::NeedsArchive(
                format!("DB schema missing instances.{}", col),
                None,
            ));
        }
        let migrated_tables = [
            "review_runs",
            "review_transitions",
            "terminal_chains",
            "terminal_generations",
            "terminal_handoffs",
            "terminal_transition_audit",
            "terminal_chain_claims",
            "terminal_generation_processes",
            "terminal_generation_prepare_intents",
            "terminal_recovery_attempts",
            "terminal_recovery_absence_evidence",
            "terminal_target_validations",
        ];
        let missing_migrated: Vec<&str> = migrated_tables
            .iter()
            .copied()
            .filter(|table| !tables.contains(*table))
            .collect();
        if !missing_migrated.is_empty() {
            return Ok(SchemaCompat::NeedsArchive(
                format!("DB missing migrated tables {:?}", missing_migrated),
                Some(version),
            ));
        }
        if !handoff_schema_is_complete(&self.conn)? {
            return Ok(SchemaCompat::NeedsArchive(
                "DB terminal handoff schema is incomplete".to_string(),
                Some(version),
            ));
        }
        if !review_schema_is_complete(&self.conn)? {
            return Ok(SchemaCompat::NeedsArchive(
                "DB review schema is incomplete".to_string(),
                Some(version),
            ));
        }

        Ok(SchemaCompat::Ok)
    }

    /// Try in-place migration for consecutive schema versions.
    ///
    /// Returns `Ok(false)` if a step is missing or the result is incomplete.
    /// `ensure_schema()` preserves v18+ databases and fails closed in that
    /// case; only older legacy versions retain the archive/recreate fallback.
    fn try_apply_migrations(&self, old_version: i32) -> Result<bool> {
        if old_version <= 0 || old_version >= SCHEMA_VERSION {
            return Ok(false);
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let current: i32 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if current == SCHEMA_VERSION {
            // Handles a DB stamped at the current version with missing typed
            // tables/indexes/triggers or only part of the v20 ALTER sequence.
            // Repairs are additive and happen under this writer transaction;
            // malformed existing tables are detected before commit and fail
            // closed.
            tx.execute_batch(REVIEW_SCHEMA_SQL)?;
            let has_handoff_tables: bool = tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                     WHERE type = 'table' AND name = 'terminal_handoffs'
                 )",
                [],
                |row| row.get(0),
            )?;
            if has_handoff_tables {
                ensure_handoff_v23_columns(&tx)?;
            }
            tx.execute_batch(HANDOFF_SCHEMA_SQL)?;
            tx.execute_batch(HANDOFF_V23_SCHEMA_SQL)?;
            if !review_schema_is_complete(&tx)? || !handoff_schema_is_complete(&tx)? {
                return Ok(false);
            }
            tx.commit()?;
            return Ok(true);
        }
        // v17 was briefly stamped onto v16-shaped databases before its two
        // terminal columns had actually been added (issue #16). Once code
        // advances beyond v17 this repair still has to run before later
        // migrations, otherwise the old data-preserving guard is bypassed.
        if old_version == 17 {
            let has_v17_columns = tx
                .prepare("PRAGMA table_info(instances)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .filter_map(|row| row.ok())
                .any(|column| column == "terminal_preset_requested");
            if !has_v17_columns {
                let (_, migration) = MIGRATIONS
                    .iter()
                    .find(|(version, _)| *version == 17)
                    .expect("v17 migration must exist");
                tx.execute_batch(migration)?;
            }
        }
        // A v18 database may have been stamped while one or more review
        // objects were still missing. Re-run the idempotent v18 DDL under the
        // same writer transaction before applying v19; malformed existing
        // tables remain detectable by the completeness check and roll back.
        if old_version >= 18 {
            tx.execute_batch(REVIEW_SCHEMA_SQL)?;
        }
        for next_version in (old_version + 1)..=SCHEMA_VERSION {
            if next_version == 17 {
                let has_launch_context = tx
                    .prepare("PRAGMA table_info(instances)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|r| r.ok())
                    .any(|col| col == "launch_context");
                if !has_launch_context {
                    tx.execute(
                        "ALTER TABLE instances ADD COLUMN launch_context TEXT DEFAULT ''",
                        [],
                    )?;
                }
            }
            let Some((_, sql)) = MIGRATIONS.iter().find(|(v, _)| *v == next_version) else {
                return Ok(false);
            };
            if next_version == 20 {
                ensure_handoff_v20_columns(&tx)?;
            } else if next_version == 21 {
                ensure_handoff_v21_columns(&tx)?;
            } else if next_version == 23 {
                ensure_handoff_v23_columns(&tx)?;
            } else {
                tx.execute_batch(sql)?;
            }
            tx.execute_batch(&format!("PRAGMA user_version = {}", next_version))?;
        }
        if !review_schema_is_complete(&tx)? || !handoff_schema_is_complete(&tx)? {
            return Ok(false);
        }
        tx.commit()?;
        Ok(true)
    }

    /// Archive current database at a given path.
    /// WAL checkpoint, copy to archive dir (sibling archive/ directory), delete original.
    ///
    /// Known, deliberately deferred limitation on Windows: the `remove_file`
    /// below can fail even though the caller already released its own
    /// connection (see `ensure_schema`). Windows only allows deleting a file
    /// while other handles remain open if *every* one of those handles was
    /// opened with `FILE_SHARE_DELETE` — and SQLite's Windows VFS (and thus
    /// rusqlite's default `Connection::open`) does not request that flag.
    /// Unix has no equivalent restriction; `unlink` on an open file always
    /// succeeds there, which is why this asymmetry doesn't show up in the
    /// Unix path at all.
    ///
    /// In practice this only bites when a schema-version mismatch forces an
    /// archive-and-reset (rare) while some other hcom process — another agent
    /// instance, a relay worker, a hook invocation — still has the same DB
    /// file open anywhere on the machine. When that happens, this call
    /// returns a real, un-recoverable-in-place `Err`; there is no retry that
    /// helps within this function. A proper fix would need a different
    /// strategy entirely — e.g. copying the live file's contents into a fresh
    /// DB and resetting schema in place, rather than deleting the original —
    /// so no cross-process handle-closing coordination is required. That is a
    /// larger change than this narrow Windows-support pass and is deferred
    /// given how rare schema mismatches are in practice.
    fn archive_db_at(db_path: &std::path::Path) -> Result<Option<String>> {
        if !db_path.exists() {
            return Ok(None);
        }

        let db_wal = db_path.with_extension("db-wal");
        let db_shm = db_path.with_extension("db-shm");

        // WAL checkpoint before archive
        if let Ok(temp_conn) = Connection::open(db_path) {
            let _ = temp_conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        }

        // Create archive directory next to the DB file
        let parent = db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S").to_string();
        let archive_dir = parent
            .join("archive")
            .join(format!("session-{}", timestamp));
        std::fs::create_dir_all(&archive_dir)?;

        // Copy DB files to archive
        let db_name = db_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("hcom.db"));
        std::fs::copy(db_path, archive_dir.join(db_name))?;
        if db_wal.exists() {
            let wal_name = format!("{}-wal", db_name.to_string_lossy());
            let _ = std::fs::copy(&db_wal, archive_dir.join(wal_name));
        }
        if db_shm.exists() {
            let shm_name = format!("{}-shm", db_name.to_string_lossy());
            let _ = std::fs::copy(&db_shm, archive_dir.join(shm_name));
        }

        // Delete original
        std::fs::remove_file(db_path)?;
        let _ = std::fs::remove_file(&db_wal);
        let _ = std::fs::remove_file(&db_shm);

        Ok(Some(archive_dir.to_string_lossy().to_string()))
    }

    /// Snapshot running instances to pidtrack before DB archive.
    ///
    /// Writes live instances (with their PIDs) to ~/.hcom/.tmp/launched_pids.json
    /// so orphan recovery can re-register them into the fresh DB after schema bump.
    fn snapshot_running_to_pidtrack(&self) {
        let Ok(mut stmt) = self.conn.prepare(
            "SELECT i.name, i.pid, i.tool, i.directory, i.session_id, p.process_id, \
                    n_pty.port AS notify_port, n_inj.port AS inject_port \
             FROM instances i \
             LEFT JOIN process_bindings p ON i.name = p.instance_name \
             LEFT JOIN notify_endpoints n_pty ON i.name = n_pty.instance AND n_pty.kind = 'pty' \
             LEFT JOIN notify_endpoints n_inj ON i.name = n_inj.instance AND n_inj.kind = 'inject' \
             WHERE i.pid IS NOT NULL",
        ) else {
            return;
        };

        let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,         // name
                row.get::<_, i64>(1)?,            // pid
                row.get::<_, Option<String>>(2)?, // tool
                row.get::<_, Option<String>>(3)?, // directory
                row.get::<_, Option<String>>(4)?, // session_id
                row.get::<_, Option<String>>(5)?, // process_id
                row.get::<_, Option<i64>>(6)?,    // notify_port
                row.get::<_, Option<i64>>(7)?,    // inject_port
            ))
        }) else {
            return;
        };

        let pidfile_path = self
            .db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(".tmp")
            .join("launched_pids.json");

        // Read existing pidfile
        let mut piddata: serde_json::Map<String, serde_json::Value> =
            std::fs::read_to_string(&pidfile_path)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();

        for row in rows.flatten() {
            let (name, pid, tool, directory, session_id, process_id, notify_port, inject_port) =
                row;
            let alive = crate::pidtrack::is_alive(pid as u32);
            if !alive {
                continue;
            }

            piddata.insert(
                pid.to_string(),
                serde_json::json!({
                    "tool": tool.unwrap_or_else(|| "claude".to_string()),
                    "names": [name],
                    "directory": directory.unwrap_or_default(),
                    "process_id": process_id.unwrap_or_default(),
                    "session_id": session_id.unwrap_or_default(),
                    "notify_port": notify_port.unwrap_or(0),
                    "inject_port": inject_port.unwrap_or(0),
                    "launched_at": now_epoch_f64(),
                }),
            );
        }

        if let Ok(json) = serde_json::to_string(&piddata) {
            let _ = std::fs::write(&pidfile_path, json);
        }
    }

    /// Log _device reset event + set relay timestamp. Call after any DB archive/reset.
    pub fn log_reset_event(&self) -> Result<()> {
        // Derive hcom_dir from db_path (db is at hcom_dir/hcom.db)
        let hcom_dir = self
            .db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let device_id = std::fs::read_to_string(hcom_dir.join(".tmp").join("device_uuid"))
            .unwrap_or_else(|_| "unknown".to_string())
            .trim()
            .to_string();

        self.log_event(
            "life",
            "_device",
            &serde_json::json!({"action": "reset", "device": device_id}),
        )?;

        self.kv_set("relay_local_reset_ts", Some(&now_epoch_f64().to_string()))?;

        Ok(())
    }

    /// Remove all event subscriptions owned by an instance.
    ///
    /// Subscriptions are stored as kv entries with key 'events_sub:sub-{hash}'
    /// and a JSON value containing a "caller" field.
    pub fn cleanup_subscriptions(&self, name: &str) -> Result<u32> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::cleanup_subscriptions(self, name)
    }

    /// Remove delivery-only thread memberships for an instance.
    ///
    /// This is used when a stopped name is being reused by a fresh instance:
    /// normal stop/resume should preserve memberships, but identity replacement
    /// must not inherit old thread state.
    pub fn cleanup_thread_memberships_for_name_reuse(&self, name: &str) -> Result<u32> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::cleanup_thread_memberships_for_name_reuse(self, name)
    }

    /// Return active members of a thread in join order.
    pub fn get_thread_members(&self, thread: &str) -> Vec<String> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::get_thread_members(self, thread)
    }

    /// Upsert memberships for recipients of a thread message.
    pub fn add_thread_memberships(
        &self,
        thread: &str,
        sender: Option<&str>,
        recipients: &[String],
    ) {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::add_thread_memberships(self, thread, sender, recipients);
    }

    /// Send a system notification message (simplified inline version).
    /// Parses @mentions, computes scope, inserts message event.
    pub fn send_system_message(&self, sender_name: &str, message: &str) -> Result<Vec<String>> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::send_system_message(self, sender_name, message)
    }

    /// Like `send_system_message` but lets the caller specify `sender_kind`
    /// ("instance" | "external" | "system"). Used by subscription on-hit to
    /// preserve the sub caller's real identity on the event.
    pub fn send_message_as(
        &self,
        sender_name: &str,
        sender_kind: &str,
        message: &str,
    ) -> Result<Vec<String>> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::send_message_as(self, sender_name, sender_kind, message)
    }
}

/// Generate ISO timestamp for current time.
pub(super) fn chrono_now_iso() -> String {
    crate::shared::time::now_iso()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use rusqlite::{Connection, params};
    use std::path::PathBuf;

    /// Clean up test database
    pub(super) fn cleanup_test_db(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(format!("{}-wal", path.display())));
        let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", path.display())));
    }

    #[test]
    fn test_all_methods_return_ok_none_when_not_found() {
        let (db, db_path) = setup_full_test_db();

        // All these should return Ok(None) for non-existent data
        assert!(db.get_instance_status("nonexistent").unwrap().is_none());
        assert!(db.get_status("nonexistent").unwrap().is_none());
        assert!(db.get_process_binding("nonexistent").unwrap().is_none());
        assert!(db.get_transcript_path("nonexistent").unwrap().is_none());
        assert!(db.get_instance_snapshot("nonexistent").unwrap().is_none());

        cleanup_test_db(db_path);
    }

    /// Create a test DB with full init_db() schema
    pub(super) fn setup_full_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_full_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path)
    }

    fn rebuild_handoff_tables_with_v19_columns(conn: &Connection, stamped_version: i32) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             DROP TRIGGER IF EXISTS terminal_chain_instance_tag_immutable;
             CREATE TABLE terminal_chains_v19 AS
             SELECT id, workspace, tool, model_ref, reasoning_ref,
                    permission_policy_ref, policy_ref, supervisor_process_id,
                    supervisor_process_birth_identity, current_generation,
                    state, version, created_at, updated_at
             FROM terminal_chains;
             CREATE TABLE terminal_handoffs_v19 AS
             SELECT id, chain_id, source_generation, target_generation,
                    source_launch_nonce, source_instance_name,
                    source_hcom_session_id, source_native_session_id,
                    source_wrapper_process_id, source_process_birth_identity,
                    bundle_event_id, bundle_digest, bundle_size_bytes,
                    workspace, revision, branch, dirty_summary, policy_ref,
                    state, version, quiesce_token, quiesce_generation,
                    quiesce_native_session_id, quiesce_process_id,
                    quiesce_process_birth_identity, quiesce_committed_version,
                    stop_observed_at, failure_kind, failure_reason,
                    created_at, updated_at, committed_at, accepted_at
             FROM terminal_handoffs;
             DROP TABLE terminal_handoffs;
             DROP TABLE terminal_chains;
             ALTER TABLE terminal_chains_v19 RENAME TO terminal_chains;
             ALTER TABLE terminal_handoffs_v19 RENAME TO terminal_handoffs;",
        )
        .unwrap();
        conn.execute_batch(&format!(
            "PRAGMA user_version = {stamped_version};
             PRAGMA foreign_keys = ON;"
        ))
        .unwrap();
    }

    fn rebuild_terminal_chains_without_v23_tag(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             DROP TRIGGER IF EXISTS terminal_chain_instance_tag_immutable;
             CREATE TABLE terminal_chains_v22 AS
             SELECT id, workspace, tool, model_ref, reasoning_ref,
                    permission_policy_ref, policy_ref, supervisor_process_id,
                    supervisor_process_birth_identity, supervisor_pid,
                    supervisor_pgid, outer_foreground_pgid, outer_tty_device,
                    outer_tty_inode, current_generation, state, version,
                    created_at, updated_at
             FROM terminal_chains;
             DROP TABLE terminal_chains;
             ALTER TABLE terminal_chains_v22 RENAME TO terminal_chains;
             PRAGMA user_version = 22;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    fn insert_preserved_phase1_handoff(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             INSERT INTO events (timestamp, type, instance, data)
             VALUES (
                 '2026-07-27T00:00:00Z', 'bundle', 'source',
                 '{\"bundle_id\":\"bundle:migrate\",\"created_by\":\"source\"}'
             );
             INSERT INTO terminal_chains (
                 id, workspace, tool, model_ref, reasoning_ref,
                 permission_policy_ref, policy_ref, supervisor_process_id,
                 supervisor_process_birth_identity, current_generation,
                 state, version, created_at, updated_at
             ) VALUES (
                 'tc-migrate', '/tmp', 'codex', 'model', 'reasoning',
                 'permission', 'policy', 'supervisor', 'supervisor-birth',
                 1, 'prepared', 0, 1.0, 1.0
             );
             INSERT INTO terminal_generations (
                 chain_id, generation, launch_nonce, wrapper_process_id,
                 process_birth_identity, instance_name, hcom_session_id,
                 native_session_id, state, version, created_at, updated_at
             ) VALUES (
                 'tc-migrate', 1, 'nonce-1', 'process-1', 'birth-1',
                 'source', 'hcom-source', 'native-source',
                 'handoff_prepared', 0, 1.0, 1.0
             );
             INSERT INTO terminal_generations (
                 chain_id, generation, launch_nonce, state, version,
                 created_at, updated_at
             ) VALUES (
                 'tc-migrate', 2, 'nonce-2', 'reserved', 0, 1.0, 1.0
             );
             INSERT INTO terminal_handoffs (
                 id, chain_id, source_generation, target_generation,
                 source_launch_nonce, source_instance_name,
                 source_hcom_session_id, source_native_session_id,
                 source_wrapper_process_id, source_process_birth_identity,
                 bundle_event_id, bundle_digest, bundle_size_bytes,
                 workspace, revision, branch, dirty_summary, policy_ref,
                 state, version, created_at, updated_at
             ) VALUES (
                 'ho-migrate', 'tc-migrate', 1, 2, 'nonce-1', 'source',
                 'hcom-source', 'native-source', 'process-1', 'birth-1',
                 (SELECT id FROM events
                  WHERE instance = 'source' AND type = 'bundle'
                  ORDER BY id DESC LIMIT 1),
                 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 17, '/tmp', 'revision', 'main',
                 'staged=0,unstaged=0,untracked=0,conflicted=0',
                 'policy', 'prepared', 0, 1.0, 1.0
             );
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_init_db_creates_all_tables() {
        let (db, db_path) = setup_full_test_db();

        let tables: Vec<String> = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"events".to_string()));
        assert!(tables.contains(&"instances".to_string()));
        assert!(tables.contains(&"kv".to_string()));
        assert!(tables.contains(&"notify_endpoints".to_string()));
        assert!(tables.contains(&"process_bindings".to_string()));
        assert!(tables.contains(&"session_bindings".to_string()));
        assert!(tables.contains(&"review_runs".to_string()));
        assert!(tables.contains(&"review_transitions".to_string()));
        assert!(tables.contains(&"terminal_chains".to_string()));
        assert!(tables.contains(&"terminal_generations".to_string()));
        assert!(tables.contains(&"terminal_handoffs".to_string()));
        assert!(tables.contains(&"terminal_transition_audit".to_string()));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_sets_schema_version() {
        let (db, db_path) = setup_full_test_db();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_idempotent() {
        let (db, db_path) = setup_full_test_db();

        // Call init_db again - should be no-op
        db.init_db().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_current_complete_open_does_not_wait_for_writer_lock() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (db, db_path) = setup_full_test_db();
        drop(db);

        let writer = Connection::open(&db_path).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;")
            .unwrap();

        let (tx, rx) = mpsc::sync_channel(1);
        let open_path = db_path.clone();
        let handle = std::thread::spawn(move || {
            tx.send(HcomDb::open_at(&open_path).map(|_| ())).unwrap();
        });
        let result = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("current complete open waited for the held writer transaction");
        result.unwrap();

        writer.execute_batch("ROLLBACK").unwrap();
        handle.join().unwrap();
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_creates_events_v_view() {
        let (db, db_path) = setup_full_test_db();

        // Check view exists
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='events_v'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_creates_fts5_table() {
        let (db, db_path) = setup_full_test_db();

        // FTS5 tables show up as 'table' in sqlite_master
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='events_fts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0, "events_fts should exist");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_fts_trigger_indexes_on_insert() {
        let (db, db_path) = setup_full_test_db();

        // Insert an event
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES ('2026-01-01T00:00:00Z', 'message', 'luna', ?)",
                params![serde_json::json!({"from": "luna", "text": "hello world"}).to_string()],
            )
            .unwrap();

        // Search FTS
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events_fts WHERE searchable MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_check_schema_compat_fresh_db() {
        let (db, db_path) = setup_full_test_db();
        match db.check_schema_compat().unwrap() {
            SchemaCompat::Ok => {} // expected
            other => panic!(
                "Expected SchemaCompat::Ok, got {:?}",
                match other {
                    SchemaCompat::NeedsArchive(r, v) => format!("NeedsArchive({}, {:?})", r, v),
                    SchemaCompat::StaleProcess => "StaleProcess".to_string(),
                    SchemaCompat::Ok => unreachable!(),
                }
            ),
        }
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_fresh_db() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_ensure_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should have full schema
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_archives_old_version() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(2000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_archive_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Create a DB with old schema version
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (name TEXT PRIMARY KEY, created_at REAL NOT NULL);
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = 5;",
            )
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should have been archived and recreated at current version
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Archive directory should exist
        let archive_dir = temp_dir.join("archive");
        if archive_dir.exists() {
            let _ = std::fs::remove_dir_all(&archive_dir);
        }

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_migrates_v16_to_v17_in_place() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(2500);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_migrate_{}_{}.db",
            std::process::id(),
            test_id
        ));

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (
                     name TEXT PRIMARY KEY,
                     tool TEXT DEFAULT 'claude',
                     created_at REAL NOT NULL,
                     launch_context TEXT DEFAULT ''
                 );
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = 16;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO instances (name, tool, created_at, launch_context) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    "luna",
                    "claude",
                    1.0f64,
                    r#"{"terminal_preset":"ghostty-tab"}"#
                ],
            )
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        let preset: String = db
            .conn
            .query_row(
                "SELECT terminal_preset_effective FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preset, "ghostty-tab");
        let launch_context: String = db
            .conn
            .query_row(
                "SELECT launch_context FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(launch_context, r#"{"terminal_preset":"ghostty-tab"}"#);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_migrates_v17_to_v18_in_place() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data)
                 VALUES ('2026-01-01T00:00:00Z', 'message', 'luna', '{}')",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, session_id, tool, created_at)
                 VALUES ('luna', 'session-luna', 'claude', 1.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO kv (key, value) VALUES ('preserved', 'yes')",
                [],
            )
            .unwrap();
        db.conn
            .execute_batch(
                "DROP TABLE review_transitions;
                 DROP TABLE review_runs;
                 PRAGMA user_version = 17;",
            )
            .unwrap();
        drop(db);

        let db = HcomDb::open_at(&db_path).unwrap();
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let preserved: (i64, i64, String) = db
            .conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM events WHERE instance = 'luna'),
                     (SELECT COUNT(*) FROM instances WHERE name = 'luna'),
                     (SELECT value FROM kv WHERE key = 'preserved')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(preserved, (1, 1, "yes".to_string()));
        for table in ["review_runs", "review_transitions"] {
            let exists: bool = db
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    params![table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing table {table}");
        }

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_column_guard() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(3000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_colguard_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Create a DB at current version but missing 'tool' column
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(&format!(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (name TEXT PRIMARY KEY, created_at REAL NOT NULL);
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = {};",
                SCHEMA_VERSION
            ))
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();

        // check_schema_compat should detect missing column
        match db.check_schema_compat().unwrap() {
            SchemaCompat::NeedsArchive(reason, _) => {
                assert!(reason.contains("instances.tool"), "reason: {}", reason);
            }
            _ => panic!("Expected NeedsArchive for missing tool column"),
        }

        // ensure_schema should fix it
        db.ensure_schema().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    /// Regression test for issue #16: init_db() stamped user_version=17 without
    /// actually adding the terminal_preset_* columns. ensure_schema must repair
    /// this via migration instead of archiving (which would lose data).
    #[test]
    fn test_ensure_schema_repairs_stamped_but_not_migrated_db() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(4000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_repair_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Simulate the bug: create a v16-style DB but stamp it as v17
        // (this is what init_db() did — CREATE IF NOT EXISTS is a no-op on
        // existing tables, then it unconditionally set user_version = 17)
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, type TEXT NOT NULL, instance TEXT NOT NULL, data TEXT NOT NULL);
                 CREATE TABLE instances (
                     name TEXT PRIMARY KEY,
                     session_id TEXT UNIQUE,
                     parent_session_id TEXT,
                     parent_name TEXT,
                     tag TEXT,
                     last_event_id INTEGER DEFAULT 0,
                     status TEXT DEFAULT 'active',
                     status_time INTEGER DEFAULT 0,
                     status_context TEXT DEFAULT '',
                     status_detail TEXT DEFAULT '',
                     last_stop INTEGER DEFAULT 0,
                     directory TEXT,
                     created_at REAL NOT NULL,
                     transcript_path TEXT DEFAULT '',
                     tcp_mode INTEGER DEFAULT 0,
                     wait_timeout INTEGER DEFAULT 86400,
                     background INTEGER DEFAULT 0,
                     background_log_file TEXT DEFAULT '',
                     name_announced INTEGER DEFAULT 0,
                     agent_id TEXT UNIQUE,
                     running_tasks TEXT DEFAULT '',
                     origin_device_id TEXT DEFAULT '',
                     hints TEXT DEFAULT '',
                     subagent_timeout INTEGER,
                     tool TEXT DEFAULT 'claude',
                     launch_args TEXT DEFAULT '',
                     idle_since TEXT DEFAULT '',
                     pid INTEGER DEFAULT NULL,
                     launch_context TEXT DEFAULT ''
                 );
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT NOT NULL, kind TEXT NOT NULL, port INTEGER NOT NULL, updated_at REAL NOT NULL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 CREATE TABLE process_bindings (process_id TEXT PRIMARY KEY, session_id TEXT, instance_name TEXT, updated_at REAL NOT NULL);
                 PRAGMA user_version = 17;",
            )
            .unwrap();
            // Insert test data that should survive the repair
            conn.execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('luna', 'claude', 1.0)",
                [],
            )
            .unwrap();
        }

        // Verify columns are missing before repair
        {
            let conn = Connection::open(&db_path).unwrap();
            let cols: Vec<String> = conn
                .prepare("PRAGMA table_info(instances)")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
            assert!(
                !cols.contains(&"terminal_preset_requested".to_string()),
                "column should be missing before repair"
            );
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should be at current version
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Columns should now exist
        let cols: Vec<String> = db
            .conn
            .prepare("PRAGMA table_info(instances)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            cols.contains(&"terminal_preset_requested".to_string()),
            "terminal_preset_requested column should exist after repair"
        );
        assert!(
            cols.contains(&"terminal_preset_effective".to_string()),
            "terminal_preset_effective column should exist after repair"
        );

        // Test data should have survived (not archived)
        let name: String = db
            .conn
            .query_row(
                "SELECT name FROM instances WHERE name = 'luna'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "luna");

        cleanup_test_db(db_path);
    }

    fn terminal_schema_objects(conn: &Connection) -> Vec<(String, String, String)> {
        conn.prepare(
            "SELECT type, name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE name LIKE 'terminal_%' OR name LIKE 'idx_terminal_%'
             ORDER BY type, name",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    }

    #[test]
    fn test_v18_to_v19_migration_preserves_all_existing_state_and_matches_fresh_schema() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data)
                 VALUES ('2026-07-27T00:00:00Z', 'message', 'source', '{}')",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO instances (name, session_id, tool, created_at)
                 VALUES ('source', 'session-source', 'codex', 1.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO process_bindings (
                     process_id, session_id, instance_name, updated_at
                 ) VALUES ('process-source', 'session-source', 'source', 1.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO session_bindings (session_id, instance_name, created_at)
                 VALUES ('session-source', 'source', 1.0)",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO review_runs (
                     id, task, workspace, thread, developer_name,
                     developer_session_id, reviewer_name, reviewer_session_id,
                     state, round, max_rounds, version, created_at, updated_at
                 ) VALUES (
                     'rv-preserved', 'task', '/workspace', 'review-rv-preserved',
                     'source', 'session-source', 'reviewer', 'session-reviewer',
                     'awaiting_review', 1, 3, 0, 1.0, 1.0
                 )",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO review_transitions (
                     workflow_id, from_version, to_version, round,
                     actor_name, actor_session_id, actor_role, action,
                     from_state, to_state, summary, payload_hash, created_at
                 ) VALUES (
                     'rv-preserved', -1, 0, 1, 'source', 'session-source',
                     'developer', 'start', NULL, 'awaiting_review',
                     'task', 'hash', 1.0
                 )",
                [],
            )
            .unwrap();
        db.conn
            .execute_batch(
                "DROP INDEX idx_review_runs_active_pair;
             DROP TRIGGER terminal_transition_audit_no_delete;
             DROP TRIGGER terminal_transition_audit_no_update;
             DROP TRIGGER terminal_generations_immutable_identity;
             DROP TRIGGER terminal_generations_monotonic_insert;
             DROP TABLE terminal_transition_audit;
             DROP TABLE terminal_handoffs;
             DROP TABLE terminal_generations;
             DROP TABLE terminal_chains;
             PRAGMA user_version = 18;",
            )
            .unwrap();
        drop(db);

        let migrated = HcomDb::open_at(&db_path).unwrap();
        assert!(review_schema_is_complete(migrated.conn()).unwrap());
        let preserved: (i64, i64, i64, i64, i64, i64) = migrated
            .conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM events WHERE instance = 'source'),
                     (SELECT COUNT(*) FROM instances WHERE name = 'source'),
                     (SELECT COUNT(*) FROM process_bindings
                       WHERE process_id = 'process-source'),
                     (SELECT COUNT(*) FROM session_bindings
                       WHERE session_id = 'session-source'),
                     (SELECT COUNT(*) FROM review_runs
                       WHERE id = 'rv-preserved'),
                     (SELECT COUNT(*) FROM review_transitions
                       WHERE workflow_id = 'rv-preserved')",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(preserved, (1, 1, 1, 1, 1, 1));

        let (fresh, fresh_path) = setup_full_test_db();
        assert_eq!(
            terminal_schema_objects(migrated.conn()),
            terminal_schema_objects(fresh.conn())
        );
        cleanup_test_db(fresh_path);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_real_v19_shape_migrates_to_current_without_losing_handoff_state() {
        let (db, db_path) = setup_full_test_db();
        insert_preserved_phase1_handoff(db.conn());
        rebuild_handoff_tables_with_v19_columns(db.conn(), 19);
        drop(db);

        let migrated = HcomDb::open_at(&db_path).unwrap();
        assert!(handoff_schema_is_complete(migrated.conn()).unwrap());
        let version: i32 = migrated
            .conn()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let preserved: (String, i64, String, i64, Option<i64>, String) = migrated
            .conn()
            .query_row(
                "SELECT chain_id, target_generation, bundle_digest,
                        bundle_size_bytes, child_exit_signal,
                        sigterm_request_result
                 FROM terminal_handoffs WHERE id = 'ho-migrate'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            preserved,
            (
                "tc-migrate".to_string(),
                2,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                17,
                None,
                String::new(),
            )
        );
        type OsEvidence = (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        );
        let os_evidence: OsEvidence = migrated
            .conn()
            .query_row(
                "SELECT supervisor_pid, supervisor_pgid,
                            outer_foreground_pgid, outer_tty_device,
                            outer_tty_inode
                     FROM terminal_chains WHERE id = 'tc-migrate'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(os_evidence, (None, None, None, None, None));
        assert!(
            migrated
                .conn()
                .execute(
                    "INSERT INTO terminal_chains (
                         id, workspace, tool, model_ref, reasoning_ref,
                         permission_policy_ref, policy_ref,
                         supervisor_process_id,
                         supervisor_process_birth_identity,
                         supervisor_pid, current_generation, state, version,
                         created_at, updated_at
                     ) VALUES (
                         'tc-partial-outer', '/tmp', 'codex', 'model',
                         'reasoning', 'permission', 'policy', 'supervisor-2',
                         'supervisor-birth-2', 42, 1, 'active', 0, 1.0, 1.0
                     )",
                    [],
                )
                .is_err(),
            "migrated schema must reject partial outer-terminal evidence"
        );
        assert!(
            migrated
                .conn()
                .execute(
                    "UPDATE terminal_handoffs
                     SET sigterm_request_result = 'sent'
                     WHERE id = 'ho-migrate'",
                    [],
                )
                .is_err(),
            "migrated schema must reject a SIGTERM result without timestamps"
        );
        assert!(
            migrated
                .conn()
                .execute(
                    "UPDATE terminal_handoffs
                     SET waitpid_reaped = 1
                     WHERE id = 'ho-migrate'",
                    [],
                )
                .is_err(),
            "migrated schema must reject partial cleanup evidence"
        );
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_v21_to_v22_migration_preserves_handoff_state_and_adds_recovery_schema() {
        let (db, db_path) = setup_full_test_db();
        insert_preserved_phase1_handoff(db.conn());
        db.conn()
            .execute_batch(
                "DROP TABLE terminal_target_validations;
                 DROP TABLE terminal_recovery_absence_evidence;
                 DROP TABLE terminal_recovery_attempts;
                 DROP TABLE terminal_generation_prepare_intents;
                 DROP TABLE terminal_generation_processes;
                 DROP TABLE terminal_chain_claims;
                 PRAGMA user_version = 21;",
            )
            .unwrap();
        drop(db);

        let migrated = HcomDb::open_at(&db_path).unwrap();
        assert_eq!(
            migrated
                .conn()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert!(handoff_schema_is_complete(migrated.conn()).unwrap());
        assert_eq!(
            migrated
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM terminal_handoffs
                     WHERE id = 'ho-migrate' AND bundle_size_bytes = 17",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );

        let (fresh, fresh_path) = setup_full_test_db();
        assert_eq!(
            terminal_schema_objects(migrated.conn()),
            terminal_schema_objects(fresh.conn())
        );
        cleanup_test_db(fresh_path);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_v22_to_v23_migration_preserves_chain_and_pins_empty_tag() {
        let (db, db_path) = setup_full_test_db();
        insert_preserved_phase1_handoff(db.conn());
        rebuild_terminal_chains_without_v23_tag(db.conn());
        drop(db);

        let migrated = HcomDb::open_at(&db_path).unwrap();
        assert_eq!(
            migrated
                .conn()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let preserved: (String, String, i64) = migrated
            .conn()
            .query_row(
                "SELECT c.tag, c.state,
                        (SELECT COUNT(*) FROM terminal_handoffs
                         WHERE id = 'ho-migrate' AND bundle_size_bytes = 17)
                 FROM terminal_chains c WHERE c.id = 'tc-migrate'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(preserved, (String::new(), "prepared".to_string(), 1));
        assert!(
            migrated
                .conn()
                .execute(
                    "UPDATE terminal_chains SET tag = 'changed'
                     WHERE id = 'tc-migrate'",
                    [],
                )
                .is_err(),
            "migrated chain tag must be immutable"
        );
        assert!(handoff_schema_is_complete(migrated.conn()).unwrap());
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_stamped_partial_v20_adds_only_missing_evidence_columns() {
        let (db, db_path) = setup_full_test_db();
        insert_preserved_phase1_handoff(db.conn());
        rebuild_handoff_tables_with_v19_columns(db.conn(), 20);
        drop(db);

        let repaired = HcomDb::open_at(&db_path).unwrap();
        assert!(handoff_schema_is_complete(repaired.conn()).unwrap());
        let preserved: i64 = repaired
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_handoffs
                 WHERE id = 'ho-migrate' AND bundle_size_bytes = 17",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, 1);
        let phase3_fields: (Option<String>, Option<String>, Option<String>, Option<f64>) = repaired
            .conn()
            .query_row(
                "SELECT stop_turn_id, target_validation_token,
                        target_instructions_digest, target_validated_at
                 FROM terminal_handoffs WHERE id = 'ho-migrate'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(phase3_fields, (None, None, None, None));
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_concurrent_v19_to_current_migration_is_atomic_and_idempotent() {
        use std::sync::{Arc, Barrier};

        let (db, db_path) = setup_full_test_db();
        insert_preserved_phase1_handoff(db.conn());
        rebuild_handoff_tables_with_v19_columns(db.conn(), 19);
        drop(db);

        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let db_path = db_path.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    HcomDb::open_at(&db_path).map(|db| {
                        db.conn()
                            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                            .unwrap()
                    })
                })
            })
            .collect();
        for result in handles.into_iter().map(|handle| handle.join().unwrap()) {
            assert_eq!(result.unwrap(), SCHEMA_VERSION);
        }
        let db = HcomDb::open_at(&db_path).unwrap();
        assert!(handoff_schema_is_complete(db.conn()).unwrap());
        let preserved: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM terminal_handoffs WHERE id = 'ho-migrate'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, 1);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_stamped_v19_missing_handoff_objects_repairs_without_archive() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data)
                 VALUES ('2026-07-27T00:00:00Z', 'message', 'preserved', '{}')",
                [],
            )
            .unwrap();
        db.conn
            .execute_batch(
                "DROP TRIGGER terminal_transition_audit_no_delete;
             DROP TRIGGER terminal_transition_audit_no_update;
             DROP TRIGGER terminal_generations_immutable_identity;
             DROP TRIGGER terminal_generations_monotonic_insert;
             DROP TABLE terminal_transition_audit;
             DROP TABLE terminal_handoffs;
             DROP TABLE terminal_generations;
             DROP TABLE terminal_chains;
             PRAGMA user_version = 19;",
            )
            .unwrap();
        drop(db);

        let repaired = HcomDb::open_at(&db_path).unwrap();
        assert!(handoff_schema_is_complete(repaired.conn()).unwrap());
        let preserved: i64 = repaired
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE instance = 'preserved'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, 1);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_stamped_v19_missing_review_object_repairs_and_preserves_review_data() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO review_runs (
                     id, task, workspace, thread, developer_name,
                     developer_session_id, reviewer_name, reviewer_session_id,
                     state, round, max_rounds, version, created_at, updated_at
                 ) VALUES (
                     'rv-preserved', 'task', '/workspace', 'review-rv-preserved',
                     'source', 'session-source', 'reviewer', 'session-reviewer',
                     'awaiting_review', 1, 3, 0, 1.0, 1.0
                 )",
                [],
            )
            .unwrap();
        db.conn
            .execute_batch(
                "DROP INDEX idx_review_runs_active_pair;
                 PRAGMA user_version = 19;",
            )
            .unwrap();
        drop(db);

        let repaired = HcomDb::open_at(&db_path).unwrap();
        assert!(review_schema_is_complete(repaired.conn()).unwrap());
        let preserved: i64 = repaired
            .conn
            .query_row(
                "SELECT COUNT(*) FROM review_runs WHERE id = 'rv-preserved'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, 1);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_malformed_stamped_v19_fails_closed_without_archive_or_data_loss() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data)
                 VALUES ('2026-07-27T00:00:00Z', 'message', 'preserved', '{}')",
                [],
            )
            .unwrap();
        db.conn
            .execute_batch(
                "DROP TRIGGER terminal_transition_audit_no_delete;
             DROP TRIGGER terminal_transition_audit_no_update;
             DROP TRIGGER terminal_generations_immutable_identity;
             DROP TRIGGER terminal_generations_monotonic_insert;
             DROP TABLE terminal_transition_audit;
             DROP TABLE terminal_handoffs;
             DROP TABLE terminal_generations;
             PRAGMA foreign_keys = OFF;
             DROP TABLE terminal_chains;
             CREATE TABLE terminal_chains (id TEXT PRIMARY KEY);
             PRAGMA foreign_keys = ON;
             PRAGMA user_version = 19;",
            )
            .unwrap();
        drop(db);

        let result = HcomDb::open_at(&db_path);
        assert!(result.is_err());
        let conn = Connection::open(&db_path).unwrap();
        let preserved: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE instance = 'preserved'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, 1);
        let reset_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE type = 'life' AND instance = '_device'
                   AND json_extract(data, '$.action') = 'reset'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reset_events, 0);
        drop(conn);
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_concurrent_v18_migration_is_atomic_and_idempotent() {
        use std::sync::{Arc, Barrier};

        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute_batch(
                "DROP TRIGGER terminal_transition_audit_no_delete;
             DROP TRIGGER terminal_transition_audit_no_update;
             DROP TRIGGER terminal_generations_immutable_identity;
             DROP TRIGGER terminal_generations_monotonic_insert;
             DROP TABLE terminal_transition_audit;
             DROP TABLE terminal_handoffs;
             DROP TABLE terminal_generations;
             DROP TABLE terminal_chains;
             PRAGMA user_version = 18;",
            )
            .unwrap();
        drop(db);

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let db_path = db_path.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    HcomDb::open_at(&db_path).map(|db| {
                        db.conn
                            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                            .unwrap()
                    })
                })
            })
            .collect();
        for result in handles.into_iter().map(|handle| handle.join().unwrap()) {
            assert_eq!(result.unwrap(), SCHEMA_VERSION);
        }
        let db = HcomDb::open_at(&db_path).unwrap();
        assert!(handoff_schema_is_complete(db.conn()).unwrap());
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_handoff_schema_constraints_and_append_only_audit() {
        let (db, db_path) = setup_full_test_db();
        db.conn
            .execute_batch(
                "BEGIN IMMEDIATE;
                 INSERT INTO events (timestamp, type, instance, data)
                 VALUES (
                     '2026-07-27T00:00:00Z', 'bundle', 'source',
                     '{\"bundle_id\":\"bundle:test\",\"created_by\":\"source\"}'
                 );
                 INSERT INTO terminal_chains (
                     id, workspace, tool, model_ref, reasoning_ref,
                     permission_policy_ref, policy_ref, supervisor_process_id,
                     supervisor_process_birth_identity, current_generation,
                     state, version, created_at, updated_at
                 ) VALUES (
                     'tc-valid', '/tmp', 'codex', 'm', 'r', 'p', 'policy',
                     'supervisor', 'supervisor-birth', 1, 'prepared', 0, 1.0, 1.0
                 );
                 INSERT INTO terminal_generations (
                     chain_id, generation, launch_nonce, wrapper_process_id,
                     process_birth_identity, instance_name, hcom_session_id,
                     native_session_id, state, version, created_at, updated_at
                 ) VALUES (
                     'tc-valid', 1, 'nonce-1', 'process-1', 'birth-1',
                     'source', 'hcom-source', 'native-source',
                     'handoff_prepared', 0, 1.0, 1.0
                 );
                 INSERT INTO terminal_generations (
                     chain_id, generation, launch_nonce, state, version,
                     created_at, updated_at
                 ) VALUES (
                     'tc-valid', 2, 'nonce-2', 'reserved', 0, 1.0, 1.0
                 );
                 INSERT INTO terminal_handoffs (
                     id, chain_id, source_generation, target_generation,
                     source_launch_nonce, source_instance_name,
                     source_hcom_session_id, source_native_session_id,
                     source_wrapper_process_id, source_process_birth_identity,
                     bundle_event_id, bundle_digest, bundle_size_bytes,
                     workspace, revision, branch, dirty_summary, policy_ref,
                     state, version, created_at, updated_at
                 ) VALUES (
                     'ho-valid', 'tc-valid', 1, 2, 'nonce-1', 'source',
                     'hcom-source', 'native-source', 'process-1', 'birth-1',
                     1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                     0, '/tmp', 'revision', 'main',
                     'staged=0,unstaged=0,untracked=0,conflicted=0',
                     'policy', 'prepared', 0, 1.0, 1.0
                 );
                 INSERT INTO terminal_transition_audit (
                     chain_id, object_kind, object_id, from_version, to_version,
                     from_state, to_state, actor_instance_name,
                     actor_hcom_session_id, actor_process_id,
                     actor_process_birth_identity, actor_generation,
                     actor_role, action, request_hash, created_at
                 ) VALUES (
                     'tc-valid', 'handoff', 'ho-valid', -1, 0, NULL,
                     'prepared', 'source', 'hcom-source', 'process-1',
                     'birth-1', 1, 'source', 'prepare',
                     'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                     1.0
                 );
                 COMMIT;",
            )
            .unwrap();

        let rejects = [
            "INSERT INTO terminal_generations (
                 chain_id, generation, launch_nonce, state, version,
                 created_at, updated_at
             ) VALUES ('tc-valid', 4, 'nonce-4', 'reserved', 0, 1.0, 1.0)",
            "UPDATE terminal_chains SET state = 'unknown' WHERE id = 'tc-valid'",
            "UPDATE terminal_chains SET version = -1 WHERE id = 'tc-valid'",
            "UPDATE terminal_generations SET state = 'unknown'
             WHERE chain_id = 'tc-valid' AND generation = 2",
            "UPDATE terminal_generations SET version = -1
             WHERE chain_id = 'tc-valid' AND generation = 2",
            "UPDATE terminal_chains SET policy_ref = 'changed'
             WHERE id = 'tc-valid'",
            "UPDATE terminal_chains SET tag = 'changed'
             WHERE id = 'tc-valid'",
            "UPDATE terminal_generations SET native_session_id = 'changed'
             WHERE chain_id = 'tc-valid' AND generation = 1",
            "UPDATE terminal_handoffs SET workspace = '/changed'
             WHERE id = 'ho-valid'",
            "UPDATE terminal_handoffs SET state = 'unknown'
             WHERE id = 'ho-valid'",
            "UPDATE terminal_handoffs SET version = -1
             WHERE id = 'ho-valid'",
            "INSERT INTO terminal_handoffs (
                 id, chain_id, source_generation, target_generation,
                 source_launch_nonce, source_instance_name,
                 source_hcom_session_id, source_native_session_id,
                 source_wrapper_process_id, source_process_birth_identity,
                 bundle_event_id, bundle_digest, bundle_size_bytes,
                 workspace, revision, branch, dirty_summary, policy_ref,
                 state, version, created_at, updated_at
             ) VALUES (
                 'ho-second', 'tc-valid', 1, 2, 'nonce-1', 'source',
                 'hcom-source', 'native-source', 'process-1', 'birth-1',
                 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 0, '/tmp', 'revision', 'main',
                 'staged=0,unstaged=0,untracked=0,conflicted=0',
                 'policy', 'prepared', 0, 1.0, 1.0
             )",
        ];
        for sql in rejects {
            assert!(db.conn.execute(sql, []).is_err(), "must reject: {sql}");
        }

        db.conn
            .execute(
                "UPDATE terminal_handoffs SET
                     quiesce_token = 'qa-token',
                     quiesce_generation = 1,
                     quiesce_native_session_id = 'native-source',
                     quiesce_process_id = 'process-1',
                     quiesce_process_birth_identity = 'birth-1',
                     quiesce_committed_version = 1
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap();
        assert!(
            db.conn
                .execute(
                    "UPDATE terminal_handoffs
                     SET quiesce_token = 'qa-different'
                     WHERE id = 'ho-valid'",
                    [],
                )
                .is_err()
        );
        let split_stop = db
            .conn
            .execute(
                "UPDATE terminal_handoffs
                 SET stop_observed_at = 9.0
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap_err();
        assert!(split_stop.to_string().contains("Stop identity"));
        db.conn
            .execute(
                "UPDATE terminal_handoffs
                 SET stop_observed_at = 9.0, stop_turn_id = 'turn-valid'
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap();
        assert!(
            db.conn
                .execute(
                    "UPDATE terminal_handoffs
                     SET stop_observed_at = 9.1
                     WHERE id = 'ho-valid'",
                    [],
                )
                .is_err(),
            "recorded Stop identity must be immutable"
        );
        db.conn
            .execute(
                "UPDATE terminal_handoffs
                 SET sigterm_request_result = 'sent',
                     sigterm_requested_wall_at = 10.0,
                     sigterm_requested_monotonic_ns = 1000000000
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap();
        let split_exit = db
            .conn
            .execute(
                "UPDATE terminal_handoffs
                 SET child_exit_observed_wall_at = 10.1,
                     child_exit_observed_monotonic_ns = 1100000000,
                     child_exit_signal = 15,
                     sigterm_to_exit_ms = 100,
                     delivery_exit_context = 'exit:killed'
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap_err();
        assert!(
            split_exit
                .to_string()
                .contains("exit evidence requires atomic cleanup evidence")
        );
        let exit_fields: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM terminal_handoffs
                 WHERE id = 'ho-valid'
                   AND child_exit_observed_wall_at IS NULL
                   AND cleanup_completed_at IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(exit_fields, 1);
        db.conn
            .execute(
                "UPDATE terminal_handoffs
                 SET child_exit_observed_wall_at = 10.1,
                     child_exit_observed_monotonic_ns = 1100000000,
                     child_exit_signal = 15,
                     sigterm_to_exit_ms = 100,
                     delivery_exit_context = 'exit:killed',
                     waitpid_reaped = 1,
                     inject_cleanup_succeeded = 1,
                     delivery_cleanup_succeeded = 1,
                     pty_cleanup_succeeded = 1,
                     screen_cleanup_succeeded = 1,
                     write_queue_cleanup_succeeded = 1,
                     cleanup_completed_at = 10.2
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap();
        assert!(
            db.conn
                .execute(
                    "UPDATE terminal_transition_audit
                     SET action = 'changed' WHERE object_id = 'ho-valid'",
                    [],
                )
                .is_err()
        );
        assert!(
            db.conn
                .execute(
                    "DELETE FROM terminal_transition_audit
                     WHERE object_id = 'ho-valid'",
                    [],
                )
                .is_err()
        );

        // Once the first handoff is final, the uniqueness guard no longer
        // masks the exact N -> N+1 constraint.
        db.conn
            .execute(
                "UPDATE terminal_handoffs SET state = 'accepted'
                 WHERE id = 'ho-valid'",
                [],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO terminal_generations (
                     chain_id, generation, launch_nonce, state, version,
                     created_at, updated_at
                 ) VALUES ('tc-valid', 3, 'nonce-3', 'reserved', 0, 1.0, 1.0)",
                [],
            )
            .unwrap();
        assert!(
            db.conn
                .execute(
                    "INSERT INTO terminal_handoffs (
                         id, chain_id, source_generation, target_generation,
                         source_launch_nonce, source_instance_name,
                         source_hcom_session_id, source_native_session_id,
                         source_wrapper_process_id, source_process_birth_identity,
                         bundle_event_id, bundle_digest, bundle_size_bytes,
                         workspace, revision, branch, dirty_summary, policy_ref,
                         state, version, created_at, updated_at
                     ) VALUES (
                         'ho-skips-generation', 'tc-valid', 1, 3, 'nonce-1',
                         'source', 'hcom-source', 'native-source', 'process-1',
                         'birth-1', 1,
                         'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                         0, '/tmp', 'revision', 'main',
                         'staged=0,unstaged=0,untracked=0,conflicted=0',
                         'policy', 'prepared', 0, 1.0, 1.0
                     )",
                    [],
                )
                .is_err()
        );

        cleanup_test_db(db_path);
    }
}
