//! Deterministic scripted implementation of [`TaskWorkerRuntime`].

use crate::control_api::WorkerRole;
use crate::worker::runtime::{
    RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeSessionKey, RuntimeTurnKey,
    RuntimeTurnPoll, RuntimeTurnPurpose, RuntimeTurnSpec, TaskWorkerRuntime,
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeRuntimeRecord {
    SessionOpened {
        session: RuntimeSessionKey,
        role: WorkerRole,
        task_key: String,
    },
    TurnStarted {
        turn: RuntimeTurnKey,
        session: RuntimeSessionKey,
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
    },
    TurnPolled {
        turn: RuntimeTurnKey,
        terminal: bool,
    },
    TurnCanceled {
        turn: RuntimeTurnKey,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct FakeTurnScript {
    pub role: WorkerRole,
    pub purpose: RuntimeTurnPurpose,
    pub polls: VecDeque<RuntimeTurnPoll>,
}

impl FakeTurnScript {
    pub fn new(
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        polls: impl IntoIterator<Item = RuntimeTurnPoll>,
    ) -> Self {
        Self {
            role,
            purpose,
            polls: polls.into_iter().collect(),
        }
    }
}

struct FakeSession {
    spec: RoleSessionSpec,
}

struct FakeActiveTurn {
    script: FakeTurnScript,
}

pub struct FakeTaskWorkerRuntime {
    contract: RuntimeContractIdentity,
    scripts: VecDeque<FakeTurnScript>,
    sessions: BTreeMap<RuntimeSessionKey, FakeSession>,
    active: BTreeMap<RuntimeTurnKey, FakeActiveTurn>,
    next_session: u64,
    next_turn: u64,
    records: Vec<FakeRuntimeRecord>,
    shutdown: bool,
}

impl FakeTaskWorkerRuntime {
    pub fn new(scripts: impl IntoIterator<Item = FakeTurnScript>) -> Self {
        Self::with_contract(RuntimeContractIdentity::codex_exec(), scripts)
    }

    pub fn with_contract(
        contract: RuntimeContractIdentity,
        scripts: impl IntoIterator<Item = FakeTurnScript>,
    ) -> Self {
        Self {
            contract,
            scripts: scripts.into_iter().collect(),
            sessions: BTreeMap::new(),
            active: BTreeMap::new(),
            next_session: 1,
            next_turn: 1,
            records: Vec::new(),
            shutdown: false,
        }
    }

    pub fn records(&self) -> &[FakeRuntimeRecord] {
        &self.records
    }

    pub fn remaining_scripts(&self) -> usize {
        self.scripts.len()
    }

    fn require_open(&self) -> Result<(), RuntimeError> {
        if self.shutdown {
            return Err(RuntimeError::invalid_transition(
                "task runtime is already shut down",
            ));
        }
        Ok(())
    }

    fn allocate_session_key(&mut self) -> Result<RuntimeSessionKey, RuntimeError> {
        let value = self.next_session;
        self.next_session = self
            .next_session
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("runtime session key overflow"))?;
        RuntimeSessionKey::from_counter(value)
    }

    fn allocate_turn_key(&mut self) -> Result<RuntimeTurnKey, RuntimeError> {
        let value = self.next_turn;
        self.next_turn = self
            .next_turn
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("runtime turn key overflow"))?;
        RuntimeTurnKey::from_counter(value)
    }
}

impl TaskWorkerRuntime for FakeTaskWorkerRuntime {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        let role = spec.role;
        let task_key = spec.task_key.clone();
        let key = self.allocate_session_key()?;
        self.sessions.insert(key, FakeSession { spec });
        self.records.push(FakeRuntimeRecord::SessionOpened {
            session: key,
            role,
            task_key,
        });
        Ok(key)
    }

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        let role_session = self
            .sessions
            .get(&session)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown runtime session key"))?;
        if role_session.spec.role != spec.role
            || role_session.spec.task_key != spec.task_key
            || role_session.spec.cwd != spec.cwd
            || role_session.spec.profile != spec.profile
        {
            return Err(RuntimeError::invalid_identity(
                "runtime turn does not match its bound role session",
            ));
        }
        let script = self
            .scripts
            .pop_front()
            .ok_or_else(|| RuntimeError::invalid_transition("fake runtime script is exhausted"))?;
        if script.role != spec.role || script.purpose != spec.purpose {
            return Err(RuntimeError::invalid_transition(
                "fake runtime script does not match the requested role/purpose",
            ));
        }
        if script.polls.is_empty() {
            return Err(RuntimeError::invalid_contract(
                "fake runtime turn script must contain at least one poll",
            ));
        }
        for poll in &script.polls {
            poll.validate()?;
            if let RuntimeTurnPoll::Completed { outcome, .. } = poll
                && outcome.role() != spec.role
            {
                return Err(RuntimeError::invalid_contract(
                    "fake completed outcome has the wrong role",
                ));
            }
        }
        let key = self.allocate_turn_key()?;
        self.active.insert(key, FakeActiveTurn { script });
        self.records.push(FakeRuntimeRecord::TurnStarted {
            turn: key,
            session,
            role: spec.role,
            purpose: spec.purpose,
        });
        Ok(key)
    }

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
        self.require_open()?;
        let active = self
            .active
            .get_mut(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown or terminal runtime turn"))?;
        let poll = active
            .script
            .polls
            .pop_front()
            .ok_or_else(|| RuntimeError::internal("fake runtime poll script disappeared"))?;
        let terminal = poll.is_terminal();
        if !terminal && active.script.polls.is_empty() {
            return Err(RuntimeError::invalid_contract(
                "fake pending turn has no later scripted poll",
            ));
        }
        self.records
            .push(FakeRuntimeRecord::TurnPolled { turn, terminal });
        if terminal {
            self.active.remove(&turn);
        }
        Ok(poll)
    }

    fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
        self.require_open()?;
        self.active
            .remove(&turn)
            .ok_or_else(|| RuntimeError::invalid_identity("unknown or terminal runtime turn"))?;
        self.records.push(FakeRuntimeRecord::TurnCanceled { turn });
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.shutdown {
            return Ok(());
        }
        self.active.clear();
        self.sessions.clear();
        self.shutdown = true;
        self.records.push(FakeRuntimeRecord::Shutdown);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::runtime::{
        DeveloperOutcomeStatus, DeveloperOutcomeV1, OutcomeContract, RuntimeOutcome,
        RuntimeProfile, RuntimeTelemetry,
    };
    use std::path::PathBuf;
    use std::time::Duration;

    fn session_spec(role: WorkerRole) -> RoleSessionSpec {
        RoleSessionSpec {
            role,
            task_key: "task-1".into(),
            cwd: PathBuf::from("/repo"),
            task_repository: PathBuf::from("/repo"),
            profile: RuntimeProfile::codex_exec_default(),
            developer_instructions: "closed role instructions".into(),
        }
    }

    fn turn_spec(role: WorkerRole, purpose: RuntimeTurnPurpose) -> RuntimeTurnSpec {
        RuntimeTurnSpec {
            role,
            task_key: "task-1".into(),
            purpose,
            cwd: PathBuf::from("/repo"),
            task_repository: PathBuf::from("/repo"),
            prompt: "bounded turn prompt".into(),
            profile: RuntimeProfile::codex_exec_default(),
            outcome_contract: match role {
                WorkerRole::Developer => OutcomeContract::DeveloperV1,
                WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
            },
            timeout: Duration::from_secs(30),
        }
    }

    fn ready() -> RuntimeTurnPoll {
        RuntimeTurnPoll::Completed {
            outcome: RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                status: DeveloperOutcomeStatus::Ready,
            }),
            final_message_path: PathBuf::from("/artifacts/developer/native-final.partial"),
            telemetry: RuntimeTelemetry::default(),
        }
    }

    #[test]
    fn fake_runtime_opens_fresh_sessions_and_follows_up_on_the_exact_session() {
        let mut runtime = FakeTaskWorkerRuntime::new([
            FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::InitialDevelopment,
                [
                    RuntimeTurnPoll::Pending {
                        telemetry: RuntimeTelemetry::default(),
                    },
                    ready(),
                ],
            ),
            FakeTurnScript::new(
                WorkerRole::Developer,
                RuntimeTurnPurpose::DeveloperCorrection,
                [ready()],
            ),
        ]);
        let developer = runtime
            .open_session(session_spec(WorkerRole::Developer))
            .unwrap();
        let reviewer = runtime
            .open_session(session_spec(WorkerRole::Reviewer))
            .unwrap();
        assert_ne!(developer, reviewer);

        let first = runtime
            .start_turn(
                developer,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                ),
            )
            .unwrap();
        assert!(!runtime.poll_turn(first).unwrap().is_terminal());
        assert!(runtime.poll_turn(first).unwrap().is_terminal());

        let follow_up = runtime
            .start_turn(
                developer,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                ),
            )
            .unwrap();
        assert_ne!(first, follow_up);
        assert!(runtime.poll_turn(follow_up).unwrap().is_terminal());
        assert_eq!(runtime.remaining_scripts(), 0);
    }

    #[test]
    fn fake_runtime_enforces_one_turn_cancel_and_shutdown() {
        let mut runtime = FakeTaskWorkerRuntime::new([FakeTurnScript::new(
            WorkerRole::Developer,
            RuntimeTurnPurpose::InitialDevelopment,
            [
                RuntimeTurnPoll::Pending {
                    telemetry: RuntimeTelemetry::default(),
                },
                ready(),
            ],
        )]);
        let session = runtime
            .open_session(session_spec(WorkerRole::Developer))
            .unwrap();
        let turn = runtime
            .start_turn(
                session,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                ),
            )
            .unwrap();
        assert!(
            runtime
                .start_turn(
                    session,
                    turn_spec(
                        WorkerRole::Developer,
                        RuntimeTurnPurpose::InitialDevelopment
                    )
                )
                .is_err()
        );
        runtime.cancel_turn(turn).unwrap();
        runtime.shutdown().unwrap();
        runtime.shutdown().unwrap();
        assert!(
            runtime
                .open_session(session_spec(WorkerRole::Developer))
                .is_err()
        );
        assert_eq!(runtime.records().last(), Some(&FakeRuntimeRecord::Shutdown));
    }
}
