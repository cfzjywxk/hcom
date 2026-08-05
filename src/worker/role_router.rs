//! Lane-scoped ownership for task-local Developer and Reviewer runtimes.
//!
//! The bundle owns one provider runtime instance per worker lane. Provider-local
//! keys never escape it, so a session or turn cannot be resumed, polled, or
//! canceled through another lane even when two children allocate the same
//! numeric key.

use crate::worker::runtime::{
    RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeSessionKey, RuntimeTurnKey,
    RuntimeTurnPoll, RuntimeTurnSpec, TaskWorkerProfiles, TaskWorkerRuntime, WorkerLane,
};
use std::collections::BTreeMap;

pub(crate) struct LaneRuntimeSlot {
    lane: WorkerLane,
    contract: RuntimeContractIdentity,
    runtime: Option<Box<dyn TaskWorkerRuntime>>,
    unavailable_detail: Option<String>,
}

impl LaneRuntimeSlot {
    pub(crate) fn available(
        lane: WorkerLane,
        runtime: Box<dyn TaskWorkerRuntime>,
    ) -> Result<Self, RuntimeError> {
        let contract = runtime.contract().clone();
        contract.validate()?;
        Ok(Self {
            lane,
            contract,
            runtime: Some(runtime),
            unavailable_detail: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn unavailable(
        lane: WorkerLane,
        contract: RuntimeContractIdentity,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            lane,
            contract,
            runtime: None,
            unavailable_detail: Some(detail.into()),
        }
    }

    fn runtime_mut(&mut self) -> Result<&mut Box<dyn TaskWorkerRuntime>, RuntimeError> {
        self.runtime.as_mut().ok_or_else(|| {
            RuntimeError::unsupported(
                self.unavailable_detail
                    .clone()
                    .unwrap_or_else(|| "selected task worker provider is unavailable".into()),
            )
        })
    }
}

#[derive(Clone, Copy)]
struct RoutedSession {
    lane: WorkerLane,
    provider_session: RuntimeSessionKey,
}

#[derive(Clone, Copy)]
struct RoutedTurn {
    lane: WorkerLane,
    provider_turn: RuntimeTurnKey,
}

#[derive(Debug)]
pub(crate) struct LaneRuntimeTurnPoll {
    pub(crate) lane: WorkerLane,
    pub(crate) turn: RuntimeTurnKey,
    pub(crate) poll: RuntimeTurnPoll,
}

pub(crate) trait LaneTaskWorkerRuntime: Send {
    fn contract(&self) -> &RuntimeContractIdentity;

    fn open_session(
        &mut self,
        lane: WorkerLane,
        spec: RoleSessionSpec,
    ) -> Result<RuntimeSessionKey, RuntimeError>;

    fn start_turn(
        &mut self,
        lane: WorkerLane,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError>;

    fn poll_turn(
        &mut self,
        lane: WorkerLane,
        turn: RuntimeTurnKey,
    ) -> Result<LaneRuntimeTurnPoll, RuntimeError>;

    fn cancel_turn(&mut self, lane: WorkerLane, turn: RuntimeTurnKey) -> Result<(), RuntimeError>;

    fn cancel_all(&mut self) -> Result<(), RuntimeError>;

    fn shutdown(&mut self) -> Result<(), RuntimeError>;
}

pub(crate) struct TaskRuntimeBundle {
    contract: RuntimeContractIdentity,
    profiles: TaskWorkerProfiles,
    lanes: BTreeMap<WorkerLane, LaneRuntimeSlot>,
    sessions: BTreeMap<RuntimeSessionKey, RoutedSession>,
    turns: BTreeMap<RuntimeTurnKey, RoutedTurn>,
    next_session: u64,
    next_turn: u64,
    active_turns: BTreeMap<WorkerLane, RuntimeTurnKey>,
    shut_down: bool,
}

impl TaskRuntimeBundle {
    pub(crate) fn new(
        profiles: &TaskWorkerProfiles,
        slots: impl IntoIterator<Item = LaneRuntimeSlot>,
    ) -> Result<Self, RuntimeError> {
        profiles.validate()?;
        let mut lanes = BTreeMap::new();
        for slot in slots {
            slot.contract.validate()?;
            let profile = profiles.profile_for_lane(slot.lane)?;
            if slot.contract != profile.provider.contract_identity() {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} runtime contract differs from its frozen provider identity",
                    slot.lane.as_str()
                )));
            }
            let lane = slot.lane;
            if lanes.insert(lane, slot).is_some() {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} task runtime lane was registered twice",
                    lane.as_str()
                )));
            }
        }
        let expected_lanes = profiles.lanes().collect::<Vec<_>>();
        if lanes.len() != expected_lanes.len() {
            return Err(RuntimeError::invalid_contract(
                "task runtime lane collection differs from the frozen worker profiles",
            ));
        }
        for lane in expected_lanes {
            if !lanes.contains_key(&lane) {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} task runtime lane is missing",
                    lane.as_str()
                )));
            }
        }
        Ok(Self {
            contract: profiles.contract_identity(),
            profiles: profiles.clone(),
            lanes,
            sessions: BTreeMap::new(),
            turns: BTreeMap::new(),
            next_session: 1,
            next_turn: 1,
            active_turns: BTreeMap::new(),
            shut_down: false,
        })
    }

    fn require_open(&self) -> Result<(), RuntimeError> {
        if self.shut_down {
            return Err(RuntimeError::invalid_transition(
                "role-routed task runtime is shut down",
            ));
        }
        Ok(())
    }

    fn lane_mut(&mut self, lane: WorkerLane) -> Result<&mut LaneRuntimeSlot, RuntimeError> {
        self.lanes.get_mut(&lane).ok_or_else(|| {
            RuntimeError::invalid_contract(format!(
                "{} task runtime lane disappeared",
                lane.as_str()
            ))
        })
    }

    fn allocate_session_key(&mut self) -> Result<RuntimeSessionKey, RuntimeError> {
        let value = self.next_session;
        self.next_session = self
            .next_session
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("role router session key overflow"))?;
        RuntimeSessionKey::from_counter(value)
    }

    fn allocate_turn_key(&mut self) -> Result<RuntimeTurnKey, RuntimeError> {
        let value = self.next_turn;
        self.next_turn = self
            .next_turn
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("role router turn key overflow"))?;
        RuntimeTurnKey::from_counter(value)
    }

    fn require_lane_role(
        lane: WorkerLane,
        role: crate::control_api::WorkerRole,
    ) -> Result<(), RuntimeError> {
        if lane.role() != role {
            return Err(RuntimeError::invalid_identity(
                "worker lane belongs to another worker role",
            ));
        }
        Ok(())
    }

    fn require_turn_start_allowed(&self, lane: WorkerLane) -> Result<(), RuntimeError> {
        if self.active_turns.contains_key(&lane) {
            return Err(RuntimeError::invalid_transition(format!(
                "{} already has an active task turn",
                lane.as_str()
            )));
        }
        match lane {
            WorkerLane::Developer if !self.active_turns.is_empty() => {
                Err(RuntimeError::invalid_transition(
                    "Developer cannot start while a Reviewer turn is active",
                ))
            }
            WorkerLane::Reviewer(_) if self.active_turns.contains_key(&WorkerLane::Developer) => {
                Err(RuntimeError::invalid_transition(
                    "Reviewer cannot start while the Developer turn is active",
                ))
            }
            _ => Ok(()),
        }
    }

    fn require_active_turn(
        &self,
        lane: WorkerLane,
        turn: RuntimeTurnKey,
    ) -> Result<RoutedTurn, RuntimeError> {
        let routed = self
            .turns
            .get(&turn)
            .copied()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown lane-scoped turn"))?;
        if routed.lane != lane {
            return Err(RuntimeError::invalid_identity(
                "lane-scoped turn belongs to another worker lane",
            ));
        }
        if self.active_turns.get(&lane) != Some(&turn) {
            return Err(RuntimeError::invalid_identity(
                "lane-scoped turn is not the exact active turn",
            ));
        }
        Ok(routed)
    }
}

impl LaneTaskWorkerRuntime for TaskRuntimeBundle {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(
        &mut self,
        lane: WorkerLane,
        spec: RoleSessionSpec,
    ) -> Result<RuntimeSessionKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        Self::require_lane_role(lane, spec.role)?;
        if spec.profile != *self.profiles.profile_for_lane(lane)? {
            return Err(RuntimeError::invalid_profile(
                "lane session profile differs from the frozen lane route",
            ));
        }
        let session = self.allocate_session_key()?;
        let provider_session = self.lane_mut(lane)?.runtime_mut()?.open_session(spec)?;
        self.sessions.insert(
            session,
            RoutedSession {
                lane,
                provider_session,
            },
        );
        Ok(session)
    }

    fn start_turn(
        &mut self,
        lane: WorkerLane,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        Self::require_lane_role(lane, spec.role)?;
        self.require_turn_start_allowed(lane)?;
        let routed = self
            .sessions
            .get(&session)
            .copied()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown lane-scoped session"))?;
        if routed.lane != lane {
            return Err(RuntimeError::invalid_identity(
                "lane-scoped session belongs to another worker lane",
            ));
        }
        if spec.profile != *self.profiles.profile_for_lane(lane)? {
            return Err(RuntimeError::invalid_identity(
                "lane-scoped session belongs to another frozen lane profile",
            ));
        }
        // Allocate the supervisor-facing key before the child starts. Once a
        // lane turn exists, every later operation must have an exact route
        // even at the theoretical counter-overflow boundary.
        let turn = self.allocate_turn_key()?;
        let provider_turn = self
            .lane_mut(lane)?
            .runtime_mut()?
            .start_turn(routed.provider_session, spec)?;
        self.turns.insert(
            turn,
            RoutedTurn {
                lane,
                provider_turn,
            },
        );
        self.active_turns.insert(lane, turn);
        Ok(turn)
    }

    fn poll_turn(
        &mut self,
        lane: WorkerLane,
        turn: RuntimeTurnKey,
    ) -> Result<LaneRuntimeTurnPoll, RuntimeError> {
        self.require_open()?;
        let routed = self.require_active_turn(lane, turn)?;
        let poll = self
            .lane_mut(lane)?
            .runtime_mut()?
            .poll_turn(routed.provider_turn)?;
        poll.validate()?;
        if let RuntimeTurnPoll::Completed { outcome, .. } = &poll
            && outcome.role() != lane.role()
        {
            return Err(RuntimeError::invalid_contract(
                "lane child returned an outcome for another worker role",
            ));
        }
        if poll.is_terminal() {
            self.active_turns.remove(&lane);
        }
        Ok(LaneRuntimeTurnPoll { lane, turn, poll })
    }

    fn cancel_turn(&mut self, lane: WorkerLane, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
        self.require_open()?;
        let routed = self.require_active_turn(lane, turn)?;
        self.lane_mut(lane)?
            .runtime_mut()?
            .cancel_turn(routed.provider_turn)?;
        self.active_turns.remove(&lane);
        Ok(())
    }

    fn cancel_all(&mut self) -> Result<(), RuntimeError> {
        self.require_open()?;
        let mut first_error = None;
        let active_turns = self
            .active_turns
            .iter()
            .map(|(lane, turn)| (*lane, *turn))
            .collect::<Vec<_>>();
        for (lane, turn) in active_turns {
            if let Err(error) = self.cancel_turn(lane, turn)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.shut_down {
            return Ok(());
        }
        let mut first_error = self.cancel_all().err();
        for slot in self.lanes.values_mut() {
            if let Some(runtime) = slot.runtime.as_mut()
                && let Err(error) = runtime.shutdown()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.active_turns.clear();
        self.shut_down = true;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

impl Drop for TaskRuntimeBundle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::WorkerRole;
    use crate::worker::fake_runtime::{FakeTaskWorkerRuntime, FakeTurnScript};
    use crate::worker::profile::{ClaudeInvocationProfile, ReviewerId};
    use crate::worker::runtime::{
        DeveloperOutcomeStatus, DeveloperOutcomeV1, OutcomeContract, ReviewerOutcomeV1,
        ReviewerRuntimeProfile, ReviewerVerdict, RuntimeErrorCode, RuntimeOutcome, RuntimeProfile,
        RuntimeProvider, RuntimeTelemetry, RuntimeTurnPurpose,
    };
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const DEVELOPER: WorkerLane = WorkerLane::Developer;
    const REVIEWER1: WorkerLane = WorkerLane::Reviewer(ReviewerId::Reviewer1);
    const REVIEWER2: WorkerLane = WorkerLane::Reviewer(ReviewerId::Reviewer2);

    fn profile(provider: RuntimeProvider) -> RuntimeProfile {
        match provider {
            RuntimeProvider::CodexExec => RuntimeProfile::codex_exec_default(),
            RuntimeProvider::ClaudeExec => RuntimeProfile::from_claude(
                "fake Claude worker",
                &ClaudeInvocationProfile {
                    model: "haiku".into(),
                    effort: "medium".into(),
                    dangerously_skip_permissions: true,
                },
            )
            .unwrap(),
        }
    }

    fn profiles(developer: RuntimeProvider, reviewer: RuntimeProvider) -> TaskWorkerProfiles {
        TaskWorkerProfiles {
            developer: profile(developer),
            reviewers: vec![
                ReviewerRuntimeProfile {
                    reviewer_id: ReviewerId::Reviewer1,
                    profile: profile(reviewer),
                },
                ReviewerRuntimeProfile {
                    reviewer_id: ReviewerId::Reviewer2,
                    profile: profile(reviewer),
                },
            ],
        }
    }

    fn session_spec(role: WorkerRole, profile: RuntimeProfile) -> RoleSessionSpec {
        RoleSessionSpec {
            role,
            task_key: "task-1".into(),
            cwd: PathBuf::from("/project"),
            task_repository: PathBuf::from("/repository"),
            profile,
            developer_instructions: "role contract".into(),
        }
    }

    fn turn_spec(
        role: WorkerRole,
        purpose: RuntimeTurnPurpose,
        profile: RuntimeProfile,
    ) -> RuntimeTurnSpec {
        RuntimeTurnSpec {
            role,
            task_key: "task-1".into(),
            purpose,
            cwd: PathBuf::from("/project"),
            task_repository: PathBuf::from("/repository"),
            prompt: "bounded pointer-only prompt".into(),
            profile,
            outcome_contract: match role {
                WorkerRole::Developer => OutcomeContract::DeveloperV1,
                WorkerRole::Reviewer => OutcomeContract::ReviewerV1,
            },
            timeout: Duration::from_secs(30),
        }
    }

    fn completed(role: WorkerRole) -> RuntimeTurnPoll {
        match role {
            WorkerRole::Developer => RuntimeTurnPoll::Completed {
                outcome: RuntimeOutcome::Developer(DeveloperOutcomeV1 {
                    status: DeveloperOutcomeStatus::Ready,
                }),
                final_message_path: PathBuf::from("/artifacts/developer/native-final.partial"),
                telemetry: RuntimeTelemetry::default(),
            },
            WorkerRole::Reviewer => RuntimeTurnPoll::Completed {
                outcome: RuntimeOutcome::Reviewer(ReviewerOutcomeV1 {
                    verdict: ReviewerVerdict::Lgtm,
                    preceding_final_message_paths: Vec::new(),
                }),
                final_message_path: PathBuf::from("/artifacts/reviewer/native-final.partial"),
                telemetry: RuntimeTelemetry::default(),
            },
        }
    }

    fn available_slot(
        lane: WorkerLane,
        provider: RuntimeProvider,
        scripts: Vec<FakeTurnScript>,
    ) -> LaneRuntimeSlot {
        LaneRuntimeSlot::available(
            lane,
            Box::new(FakeTaskWorkerRuntime::with_contract(
                provider.contract_identity(),
                scripts,
            )),
        )
        .unwrap()
    }

    fn bundle(
        profiles: &TaskWorkerProfiles,
        developer_scripts: Vec<FakeTurnScript>,
        reviewer_scripts: Vec<FakeTurnScript>,
    ) -> TaskRuntimeBundle {
        TaskRuntimeBundle::new(
            profiles,
            [
                available_slot(DEVELOPER, profiles.developer.provider, developer_scripts),
                available_slot(
                    REVIEWER1,
                    profiles.reviewer1().provider,
                    reviewer_scripts.clone(),
                ),
                available_slot(REVIEWER2, profiles.reviewer2().provider, reviewer_scripts),
            ],
        )
        .unwrap()
    }

    #[test]
    fn all_four_role_provider_bindings_use_lane_owned_children() {
        for developer in [RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec] {
            for reviewer in [RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec] {
                let profiles = profiles(developer, reviewer);
                let developer_script = FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [completed(WorkerRole::Developer)],
                );
                let reviewer_script = FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [completed(WorkerRole::Reviewer)],
                );
                let mut runtime = bundle(&profiles, vec![developer_script], vec![reviewer_script]);
                assert_eq!(runtime.contract(), &profiles.contract_identity());

                let developer_session = runtime
                    .open_session(
                        DEVELOPER,
                        session_spec(WorkerRole::Developer, profiles.developer.clone()),
                    )
                    .unwrap();
                let developer_turn = runtime
                    .start_turn(
                        DEVELOPER,
                        developer_session,
                        turn_spec(
                            WorkerRole::Developer,
                            RuntimeTurnPurpose::InitialDevelopment,
                            profiles.developer.clone(),
                        ),
                    )
                    .unwrap();
                let developer_poll = runtime.poll_turn(DEVELOPER, developer_turn).unwrap();
                assert_eq!(developer_poll.lane, DEVELOPER);
                assert_eq!(developer_poll.turn, developer_turn);
                assert!(developer_poll.poll.is_terminal());

                let reviewer_session = runtime
                    .open_session(
                        REVIEWER1,
                        session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
                    )
                    .unwrap();
                assert_ne!(developer_session, reviewer_session);
                let reviewer_turn = runtime
                    .start_turn(
                        REVIEWER1,
                        reviewer_session,
                        turn_spec(
                            WorkerRole::Reviewer,
                            RuntimeTurnPurpose::InitialReview,
                            profiles.reviewer1().clone(),
                        ),
                    )
                    .unwrap();
                let reviewer_poll = runtime.poll_turn(REVIEWER1, reviewer_turn).unwrap();
                assert_eq!(
                    reviewer_poll.lane.reviewer_id(),
                    Some(ReviewerId::Reviewer1)
                );
                assert_eq!(reviewer_poll.turn, reviewer_turn);
                assert!(reviewer_poll.poll.is_terminal());
                runtime.shutdown().unwrap();
            }
        }
    }

    #[test]
    fn single_reviewer_profiles_create_no_reviewer2_runtime_slot() {
        let mut profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::CodexExec);
        profiles.reviewers.pop();
        profiles.validate().unwrap();
        let reviewer_script = FakeTurnScript::new(
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
            [completed(WorkerRole::Reviewer)],
        );
        let mut runtime = TaskRuntimeBundle::new(
            &profiles,
            [
                available_slot(DEVELOPER, profiles.developer.provider, Vec::new()),
                available_slot(
                    REVIEWER1,
                    profiles.reviewer1().provider,
                    vec![reviewer_script],
                ),
            ],
        )
        .unwrap();
        let reviewer1_session = runtime
            .open_session(
                REVIEWER1,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap();
        assert!(
            runtime
                .open_session(
                    REVIEWER2,
                    session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
                )
                .is_err()
        );
        let reviewer1_turn = runtime
            .start_turn(
                REVIEWER1,
                reviewer1_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer1().clone(),
                ),
            )
            .unwrap();
        assert!(
            runtime
                .poll_turn(REVIEWER1, reviewer1_turn)
                .unwrap()
                .poll
                .is_terminal()
        );
    }

    #[test]
    fn unavailable_lane_fails_closed_before_a_child_turn_exists() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec);
        let mut runtime = TaskRuntimeBundle::new(
            &profiles,
            [
                available_slot(DEVELOPER, RuntimeProvider::CodexExec, Vec::new()),
                LaneRuntimeSlot::unavailable(
                    REVIEWER1,
                    RuntimeProvider::ClaudeExec.contract_identity(),
                    "selected Claude task worker executable is unavailable",
                ),
                available_slot(REVIEWER2, RuntimeProvider::ClaudeExec, Vec::new()),
            ],
        )
        .unwrap();
        let error = runtime
            .open_session(
                REVIEWER1,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::Unsupported);
        assert_eq!(
            error.detail,
            "selected Claude task worker executable is unavailable"
        );
    }

    #[test]
    fn same_provider_roles_cannot_exchange_frozen_profiles() {
        let mut profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::CodexExec);
        profiles.reviewers[0].profile.model = "reviewer-only-model".into();
        profiles.validate().unwrap();
        let mut runtime = bundle(&profiles, Vec::new(), Vec::new());

        let error = runtime
            .open_session(
                DEVELOPER,
                session_spec(WorkerRole::Developer, profiles.reviewer1().clone()),
            )
            .unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::InvalidProfile);
        assert_eq!(
            error.detail,
            "lane session profile differs from the frozen lane route"
        );

        let developer = runtime
            .open_session(
                DEVELOPER,
                session_spec(WorkerRole::Developer, profiles.developer.clone()),
            )
            .unwrap();
        let reviewer = runtime
            .open_session(
                REVIEWER1,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap();
        assert_ne!(developer, reviewer);
    }

    #[test]
    fn same_provider_reviewers_own_independent_concurrent_runtime_slots() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::CodexExec);
        let reviewer_script = FakeTurnScript::new(
            WorkerRole::Reviewer,
            RuntimeTurnPurpose::InitialReview,
            [
                RuntimeTurnPoll::Pending {
                    telemetry: RuntimeTelemetry::default(),
                },
                completed(WorkerRole::Reviewer),
            ],
        );
        let mut runtime = bundle(&profiles, Vec::new(), vec![reviewer_script]);
        let reviewer1 = runtime
            .open_session(
                REVIEWER1,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap();
        let reviewer2 = runtime
            .open_session(
                REVIEWER2,
                session_spec(WorkerRole::Reviewer, profiles.reviewer2().clone()),
            )
            .unwrap();
        let turn1 = runtime
            .start_turn(
                REVIEWER1,
                reviewer1,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer1().clone(),
                ),
            )
            .unwrap();
        let turn2 = runtime
            .start_turn(
                REVIEWER2,
                reviewer2,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer2().clone(),
                ),
            )
            .unwrap();
        assert_ne!(reviewer1, reviewer2);
        assert_ne!(turn1, turn2);
        assert!(
            !runtime
                .poll_turn(REVIEWER2, turn2)
                .unwrap()
                .poll
                .is_terminal()
        );
        assert!(
            !runtime
                .poll_turn(REVIEWER1, turn1)
                .unwrap()
                .poll
                .is_terminal()
        );
        assert!(
            runtime
                .poll_turn(REVIEWER1, turn1)
                .unwrap()
                .poll
                .is_terminal()
        );
        assert!(
            runtime
                .poll_turn(REVIEWER2, turn2)
                .unwrap()
                .poll
                .is_terminal()
        );
        runtime.shutdown().unwrap();
    }

    #[test]
    fn session_turn_poll_and_cancel_are_exactly_lane_scoped() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec);
        let mut runtime = bundle(
            &profiles,
            vec![
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [
                        RuntimeTurnPoll::Pending {
                            telemetry: RuntimeTelemetry::default(),
                        },
                        completed(WorkerRole::Developer),
                    ],
                ),
                FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    [completed(WorkerRole::Developer)],
                ),
            ],
            vec![FakeTurnScript::new(
                WorkerRole::Reviewer,
                RuntimeTurnPurpose::InitialReview,
                [completed(WorkerRole::Reviewer)],
            )],
        );

        let wrong_role = runtime
            .open_session(
                DEVELOPER,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap_err();
        assert_eq!(wrong_role.code, RuntimeErrorCode::InvalidIdentity);

        let developer_session = runtime
            .open_session(
                DEVELOPER,
                session_spec(WorkerRole::Developer, profiles.developer.clone()),
            )
            .unwrap();
        let reviewer_session = runtime
            .open_session(
                REVIEWER1,
                session_spec(WorkerRole::Reviewer, profiles.reviewer1().clone()),
            )
            .unwrap();
        let wrong_session_lane = runtime
            .start_turn(
                REVIEWER1,
                developer_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer1().clone(),
                ),
            )
            .unwrap_err();
        assert_eq!(wrong_session_lane.code, RuntimeErrorCode::InvalidIdentity);

        let developer_turn = runtime
            .start_turn(
                DEVELOPER,
                developer_session,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    profiles.developer.clone(),
                ),
            )
            .unwrap();
        let second_live = runtime
            .start_turn(
                REVIEWER1,
                reviewer_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer1().clone(),
                ),
            )
            .unwrap_err();
        assert_eq!(second_live.code, RuntimeErrorCode::InvalidTransition);
        assert_eq!(
            runtime
                .poll_turn(REVIEWER1, developer_turn)
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
        assert_eq!(
            runtime
                .cancel_turn(REVIEWER1, developer_turn)
                .unwrap_err()
                .code,
            RuntimeErrorCode::InvalidIdentity
        );
        assert!(
            !runtime
                .poll_turn(DEVELOPER, developer_turn)
                .unwrap()
                .poll
                .is_terminal()
        );
        assert!(
            runtime
                .poll_turn(DEVELOPER, developer_turn)
                .unwrap()
                .poll
                .is_terminal()
        );

        let reviewer_turn = runtime
            .start_turn(
                REVIEWER1,
                reviewer_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer1().clone(),
                ),
            )
            .unwrap();
        assert!(
            runtime
                .poll_turn(REVIEWER1, reviewer_turn)
                .unwrap()
                .poll
                .is_terminal()
        );

        let correction = runtime
            .start_turn(
                DEVELOPER,
                developer_session,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    profiles.developer.clone(),
                ),
            )
            .unwrap();
        assert!(
            runtime
                .poll_turn(DEVELOPER, correction)
                .unwrap()
                .poll
                .is_terminal()
        );
    }

    struct ShutdownRuntime {
        contract: RuntimeContractIdentity,
        lane: WorkerLane,
        fail: bool,
        shutdowns: Arc<Mutex<Vec<WorkerLane>>>,
    }

    impl TaskWorkerRuntime for ShutdownRuntime {
        fn contract(&self) -> &RuntimeContractIdentity {
            &self.contract
        }

        fn open_session(
            &mut self,
            _spec: RoleSessionSpec,
        ) -> Result<RuntimeSessionKey, RuntimeError> {
            Err(RuntimeError::unsupported("unused shutdown test operation"))
        }

        fn start_turn(
            &mut self,
            _session: RuntimeSessionKey,
            _spec: RuntimeTurnSpec,
        ) -> Result<RuntimeTurnKey, RuntimeError> {
            Err(RuntimeError::unsupported("unused shutdown test operation"))
        }

        fn poll_turn(&mut self, _turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
            Err(RuntimeError::unsupported("unused shutdown test operation"))
        }

        fn cancel_turn(&mut self, _turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
            Err(RuntimeError::unsupported("unused shutdown test operation"))
        }

        fn shutdown(&mut self) -> Result<(), RuntimeError> {
            self.shutdowns.lock().unwrap().push(self.lane);
            if self.fail {
                return Err(RuntimeError::internal("first lane cleanup failed"));
            }
            Ok(())
        }
    }

    #[test]
    fn shutdown_attempts_every_owned_lane_and_returns_the_first_failure() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec);
        let shutdowns = Arc::new(Mutex::new(Vec::new()));
        let slot = |lane: WorkerLane, provider: RuntimeProvider, fail: bool| {
            LaneRuntimeSlot::available(
                lane,
                Box::new(ShutdownRuntime {
                    contract: provider.contract_identity(),
                    lane,
                    fail,
                    shutdowns: Arc::clone(&shutdowns),
                }),
            )
            .unwrap()
        };
        let mut runtime = TaskRuntimeBundle::new(
            &profiles,
            [
                slot(DEVELOPER, RuntimeProvider::CodexExec, true),
                slot(REVIEWER1, RuntimeProvider::ClaudeExec, false),
                slot(REVIEWER2, RuntimeProvider::ClaudeExec, false),
            ],
        )
        .unwrap();
        let error = runtime.shutdown().unwrap_err();
        assert_eq!(error.detail, "first lane cleanup failed");
        assert_eq!(
            *shutdowns.lock().unwrap(),
            [DEVELOPER, REVIEWER1, REVIEWER2]
        );
    }
}
