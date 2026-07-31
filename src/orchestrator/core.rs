//! Pure domain boundary for the deterministic session supervisor.
//!
//! Phase P0 freezes these provider-neutral event/effect types. The complete
//! reducer and exhaustive transition matrix are implemented in P1.

use crate::control_api::{SessionState, TaskDraft, TaskState, WorkerRole};
use crate::worker::runtime::{
    RuntimeFailureClass, RuntimeOutcome, RuntimeSessionKey, RuntimeTurnKey, RuntimeTurnPurpose,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorCore {
    run_id: String,
    project_root: PathBuf,
    session_state: SessionState,
    version: u64,
    tasks: Vec<CoreTask>,
    current_task: Option<usize>,
}

impl SupervisorCore {
    pub fn skeleton(run_id: String, project_root: PathBuf) -> Self {
        Self {
            run_id,
            project_root,
            session_state: SessionState::AwaitingPlan,
            version: 0,
            tasks: Vec::new(),
            current_task: None,
        }
    }

    pub fn session_state(&self) -> SessionState {
        self.session_state
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn project_root(&self) -> &PathBuf {
        &self.project_root
    }

    pub fn tasks(&self) -> &[CoreTask] {
        &self.tasks
    }

    pub fn current_task(&self) -> Option<usize> {
        self.current_task
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTask {
    pub spec: TaskDraft,
    pub state: TaskState,
    pub review_round: u32,
    pub base_revision: Option<String>,
    pub head_revision: Option<String>,
    pub developer_session: Option<RuntimeSessionKey>,
    pub reviewer_session: Option<RuntimeSessionKey>,
    pub recovery_used: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RepositoryObservation {
    pub repository_root: String,
    pub identity_hash: String,
    pub branch: String,
    pub head: String,
    pub tracked_diff_hash: String,
    pub index_diff_hash: String,
    pub untracked_status_hash: String,
    pub clean: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEvent {
    PlanBound {
        plan_version: u64,
        plan_hash: String,
        tasks: Vec<TaskDraft>,
    },
    ExecutionAuthorized {
        plan_version: u64,
        plan_hash: String,
    },
    TaskRuntimeOpened {
        task_ordinal: usize,
    },
    RoleSessionOpened {
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
    },
    TurnStarted {
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
    },
    TurnCompleted {
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
        outcome: RuntimeOutcome,
    },
    TurnFailed {
        task_ordinal: usize,
        role: WorkerRole,
        session: RuntimeSessionKey,
        turn: RuntimeTurnKey,
        completion_token: String,
        class: RuntimeFailureClass,
        detail: String,
    },
    RepositoryObserved {
        task_ordinal: usize,
        checkpoint: String,
        observation: RepositoryObservation,
    },
    RuntimeClosed {
        task_ordinal: usize,
    },
    Timeout {
        task_ordinal: usize,
        role: WorkerRole,
    },
    CancelRequested {
        reason: String,
    },
    ParentStopping,
    StatusRequested,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEffect {
    OpenTaskRuntime {
        task_ordinal: usize,
    },
    OpenRoleSession {
        task_ordinal: usize,
        role: WorkerRole,
    },
    StartTurn {
        task_ordinal: usize,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
    },
    ObserveRepository {
        task_ordinal: usize,
        checkpoint: String,
    },
    InterruptTurn {
        task_ordinal: usize,
        role: WorkerRole,
    },
    CloseTaskRuntime {
        task_ordinal: usize,
    },
    PublishStatus,
    FinishSession {
        state: SessionState,
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p0_core_skeleton_is_pure_and_empty() {
        let core = SupervisorCore::skeleton("run-1".into(), PathBuf::from("/project"));
        assert_eq!(core.session_state(), SessionState::AwaitingPlan);
        assert_eq!(core.version(), 0);
        assert_eq!(core.run_id(), "run-1");
        assert_eq!(core.project_root(), &PathBuf::from("/project"));
        assert!(core.tasks().is_empty());
        assert_eq!(core.current_task(), None);
    }

    #[test]
    fn provider_transport_types_do_not_leak_into_the_core_source() {
        let source = include_str!("core.rs");
        let implementation = source
            .split("#[cfg(test)]")
            .next()
            .expect("core source contains its implementation");
        for forbidden in [
            "serde_json::Value",
            "JSON-RPC",
            "threadId",
            "turnId",
            "std::process::Child",
            "std::process::Stdio",
            "ClaudeInvocationProfile",
            "CodexInvocationProfile",
        ] {
            assert!(
                !implementation.contains(forbidden),
                "provider/process type leaked into SupervisorCore: {forbidden}"
            );
        }
    }
}
