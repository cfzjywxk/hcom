//! Provider-neutral routing for task-local Developer and Reviewer runtimes.
//!
//! The router owns the logical keys exposed to the supervisor. Provider-local
//! keys never escape it, so a role session cannot be resumed through another
//! role or provider even when two children allocate the same numeric key.

use crate::control_api::WorkerRole;
use crate::worker::runtime::{
    RoleSessionSpec, RuntimeContractIdentity, RuntimeError, RuntimeProvider, RuntimeSessionKey,
    RuntimeTurnKey, RuntimeTurnPoll, RuntimeTurnSpec, TaskWorkerProfiles, TaskWorkerRuntime,
};
use std::collections::BTreeMap;

pub(crate) struct ProviderRuntimeSlot {
    provider: RuntimeProvider,
    contract: RuntimeContractIdentity,
    runtime: Option<Box<dyn TaskWorkerRuntime>>,
    unavailable_detail: Option<String>,
}

impl ProviderRuntimeSlot {
    pub(crate) fn available(
        provider: RuntimeProvider,
        runtime: Box<dyn TaskWorkerRuntime>,
    ) -> Result<Self, RuntimeError> {
        let contract = provider.contract_identity();
        if runtime.contract() != &contract {
            return Err(RuntimeError::invalid_contract(format!(
                "{} child runtime contract differs from its provider identity",
                provider.as_str()
            )));
        }
        Ok(Self {
            provider,
            contract,
            runtime: Some(runtime),
            unavailable_detail: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn unavailable(provider: RuntimeProvider, detail: impl Into<String>) -> Self {
        Self {
            provider,
            contract: provider.contract_identity(),
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
    role: WorkerRole,
    provider: RuntimeProvider,
    provider_session: RuntimeSessionKey,
}

#[derive(Clone, Copy)]
struct RoutedTurn {
    role: WorkerRole,
    provider: RuntimeProvider,
    provider_turn: RuntimeTurnKey,
}

pub(crate) struct RoleRoutedTaskWorkerRuntime {
    contract: RuntimeContractIdentity,
    developer_provider: RuntimeProvider,
    reviewer_provider: RuntimeProvider,
    providers: BTreeMap<RuntimeProvider, ProviderRuntimeSlot>,
    sessions: BTreeMap<RuntimeSessionKey, RoutedSession>,
    turns: BTreeMap<RuntimeTurnKey, RoutedTurn>,
    next_session: u64,
    next_turn: u64,
    active_turn: Option<RuntimeTurnKey>,
    shut_down: bool,
}

impl RoleRoutedTaskWorkerRuntime {
    pub(crate) fn new(
        profiles: &TaskWorkerProfiles,
        slots: impl IntoIterator<Item = ProviderRuntimeSlot>,
    ) -> Result<Self, RuntimeError> {
        profiles.validate()?;
        let mut providers = BTreeMap::new();
        for slot in slots {
            slot.contract.validate()?;
            if slot.contract != slot.provider.contract_identity() {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} route carries the wrong provider contract",
                    slot.provider.as_str()
                )));
            }
            let provider = slot.provider;
            if providers.insert(provider, slot).is_some() {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} task runtime route was registered twice",
                    provider.as_str()
                )));
            }
        }
        for provider in [profiles.developer.provider, profiles.reviewer.provider] {
            if !providers.contains_key(&provider) {
                return Err(RuntimeError::invalid_contract(format!(
                    "{} task runtime route is missing",
                    provider.as_str()
                )));
            }
        }
        Ok(Self {
            contract: profiles.contract_identity(),
            developer_provider: profiles.developer.provider,
            reviewer_provider: profiles.reviewer.provider,
            providers,
            sessions: BTreeMap::new(),
            turns: BTreeMap::new(),
            next_session: 1,
            next_turn: 1,
            active_turn: None,
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

    fn provider_for_role(&self, role: WorkerRole) -> RuntimeProvider {
        match role {
            WorkerRole::Developer => self.developer_provider,
            WorkerRole::Reviewer => self.reviewer_provider,
        }
    }

    fn provider_mut(
        &mut self,
        provider: RuntimeProvider,
    ) -> Result<&mut ProviderRuntimeSlot, RuntimeError> {
        self.providers.get_mut(&provider).ok_or_else(|| {
            RuntimeError::invalid_contract(format!(
                "{} task runtime route disappeared",
                provider.as_str()
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
}

impl TaskWorkerRuntime for RoleRoutedTaskWorkerRuntime {
    fn contract(&self) -> &RuntimeContractIdentity {
        &self.contract
    }

    fn open_session(&mut self, spec: RoleSessionSpec) -> Result<RuntimeSessionKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        let role = spec.role;
        let provider = self.provider_for_role(role);
        if spec.profile.provider != provider {
            return Err(RuntimeError::invalid_profile(
                "role session profile provider differs from the frozen role route",
            ));
        }
        let session = self.allocate_session_key()?;
        let provider_session = self
            .provider_mut(provider)?
            .runtime_mut()?
            .open_session(spec)?;
        self.sessions.insert(
            session,
            RoutedSession {
                role,
                provider,
                provider_session,
            },
        );
        Ok(session)
    }

    fn start_turn(
        &mut self,
        session: RuntimeSessionKey,
        spec: RuntimeTurnSpec,
    ) -> Result<RuntimeTurnKey, RuntimeError> {
        self.require_open()?;
        spec.validate()?;
        if self.active_turn.is_some() {
            return Err(RuntimeError::invalid_transition(
                "another role-routed task turn is still active",
            ));
        }
        let routed = self
            .sessions
            .get(&session)
            .copied()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown role-routed session"))?;
        if routed.role != spec.role {
            return Err(RuntimeError::invalid_identity(
                "role-routed session belongs to another worker role",
            ));
        }
        if routed.provider != spec.profile.provider
            || routed.provider != self.provider_for_role(spec.role)
        {
            return Err(RuntimeError::invalid_identity(
                "role-routed session belongs to another provider",
            ));
        }
        // Allocate the supervisor-facing key before the child starts. Once a
        // provider turn exists, every later operation must have an exact route
        // even at the theoretical counter-overflow boundary.
        let turn = self.allocate_turn_key()?;
        let provider_turn = self
            .provider_mut(routed.provider)?
            .runtime_mut()?
            .start_turn(routed.provider_session, spec)?;
        self.turns.insert(
            turn,
            RoutedTurn {
                role: routed.role,
                provider: routed.provider,
                provider_turn,
            },
        );
        self.active_turn = Some(turn);
        Ok(turn)
    }

    fn poll_turn(&mut self, turn: RuntimeTurnKey) -> Result<RuntimeTurnPoll, RuntimeError> {
        self.require_open()?;
        let routed = self
            .turns
            .get(&turn)
            .copied()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown role-routed turn"))?;
        let poll = self
            .provider_mut(routed.provider)?
            .runtime_mut()?
            .poll_turn(routed.provider_turn)?;
        poll.validate()?;
        if let RuntimeTurnPoll::Completed { outcome, .. } = &poll
            && outcome.role() != routed.role
        {
            return Err(RuntimeError::invalid_contract(
                "provider child returned an outcome for another worker role",
            ));
        }
        if poll.is_terminal() {
            self.active_turn = None;
        }
        Ok(poll)
    }

    fn cancel_turn(&mut self, turn: RuntimeTurnKey) -> Result<(), RuntimeError> {
        self.require_open()?;
        let routed = self
            .turns
            .get(&turn)
            .copied()
            .ok_or_else(|| RuntimeError::invalid_identity("unknown role-routed turn"))?;
        self.provider_mut(routed.provider)?
            .runtime_mut()?
            .cancel_turn(routed.provider_turn)?;
        self.active_turn = None;
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.shut_down {
            return Ok(());
        }
        let mut first_error = None;
        if let Some(turn) = self.active_turn
            && let Err(error) = self.cancel_turn(turn)
        {
            first_error = Some(error);
        }
        for slot in self.providers.values_mut() {
            if let Some(runtime) = slot.runtime.as_mut()
                && let Err(error) = runtime.shutdown()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.shut_down = true;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

impl Drop for RoleRoutedTaskWorkerRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::fake_runtime::{FakeTaskWorkerRuntime, FakeTurnScript};
    use crate::worker::profile::ClaudeInvocationProfile;
    use crate::worker::runtime::{
        DeveloperOutcomeStatus, DeveloperOutcomeV1, OutcomeContract, ReviewerOutcomeV1,
        ReviewerVerdict, RuntimeErrorCode, RuntimeOutcome, RuntimeProfile, RuntimeTelemetry,
        RuntimeTurnPurpose,
    };
    use std::path::PathBuf;
    use std::time::Duration;

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
            reviewer: profile(reviewer),
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
        provider: RuntimeProvider,
        scripts: Vec<FakeTurnScript>,
    ) -> ProviderRuntimeSlot {
        ProviderRuntimeSlot::available(
            provider,
            Box::new(FakeTaskWorkerRuntime::with_contract(
                provider.contract_identity(),
                scripts,
            )),
        )
        .unwrap()
    }

    #[test]
    fn all_four_role_provider_bindings_route_to_the_selected_children() {
        for developer in [RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec] {
            for reviewer in [RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec] {
                let profiles = profiles(developer, reviewer);
                let mut codex_scripts = Vec::new();
                let mut claude_scripts = Vec::new();
                let developer_script = FakeTurnScript::new(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    [completed(WorkerRole::Developer)],
                );
                if developer == RuntimeProvider::CodexExec {
                    codex_scripts.push(developer_script);
                } else {
                    claude_scripts.push(developer_script);
                }
                let reviewer_script = FakeTurnScript::new(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    [completed(WorkerRole::Reviewer)],
                );
                if reviewer == RuntimeProvider::CodexExec {
                    codex_scripts.push(reviewer_script);
                } else {
                    claude_scripts.push(reviewer_script);
                }
                let mut slots = Vec::new();
                if !codex_scripts.is_empty() {
                    slots.push(available_slot(RuntimeProvider::CodexExec, codex_scripts));
                }
                if !claude_scripts.is_empty() {
                    slots.push(available_slot(RuntimeProvider::ClaudeExec, claude_scripts));
                }
                let mut router = RoleRoutedTaskWorkerRuntime::new(&profiles, slots).unwrap();
                assert_eq!(router.contract(), &profiles.contract_identity());

                let developer_session = router
                    .open_session(session_spec(
                        WorkerRole::Developer,
                        profiles.developer.clone(),
                    ))
                    .unwrap();
                let developer_turn = router
                    .start_turn(
                        developer_session,
                        turn_spec(
                            WorkerRole::Developer,
                            RuntimeTurnPurpose::InitialDevelopment,
                            profiles.developer.clone(),
                        ),
                    )
                    .unwrap();
                assert!(router.poll_turn(developer_turn).unwrap().is_terminal());

                let reviewer_session = router
                    .open_session(session_spec(
                        WorkerRole::Reviewer,
                        profiles.reviewer.clone(),
                    ))
                    .unwrap();
                assert_ne!(developer_session, reviewer_session);
                let reviewer_turn = router
                    .start_turn(
                        reviewer_session,
                        turn_spec(
                            WorkerRole::Reviewer,
                            RuntimeTurnPurpose::InitialReview,
                            profiles.reviewer.clone(),
                        ),
                    )
                    .unwrap();
                assert!(router.poll_turn(reviewer_turn).unwrap().is_terminal());
                router.shutdown().unwrap();
            }
        }
    }

    #[test]
    fn unavailable_claude_route_fails_closed_before_a_child_turn_exists() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec);
        let mut router = RoleRoutedTaskWorkerRuntime::new(
            &profiles,
            [
                available_slot(RuntimeProvider::CodexExec, Vec::new()),
                ProviderRuntimeSlot::unavailable(
                    RuntimeProvider::ClaudeExec,
                    "selected Claude task worker executable is unavailable",
                ),
            ],
        )
        .unwrap();
        let error = router
            .open_session(session_spec(
                WorkerRole::Reviewer,
                profiles.reviewer.clone(),
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::Unsupported);
        assert_eq!(
            error.detail,
            "selected Claude task worker executable is unavailable"
        );
    }

    #[test]
    fn logical_keys_cannot_cross_role_or_provider_routes() {
        let profiles = profiles(RuntimeProvider::CodexExec, RuntimeProvider::ClaudeExec);
        let mut router = RoleRoutedTaskWorkerRuntime::new(
            &profiles,
            [
                available_slot(
                    RuntimeProvider::CodexExec,
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
                ),
                available_slot(
                    RuntimeProvider::ClaudeExec,
                    vec![FakeTurnScript::new(
                        WorkerRole::Reviewer,
                        RuntimeTurnPurpose::InitialReview,
                        [completed(WorkerRole::Reviewer)],
                    )],
                ),
            ],
        )
        .unwrap();

        let wrong_profile = router
            .open_session(session_spec(
                WorkerRole::Developer,
                profiles.reviewer.clone(),
            ))
            .unwrap_err();
        assert_eq!(wrong_profile.code, RuntimeErrorCode::InvalidProfile);

        let developer_session = router
            .open_session(session_spec(
                WorkerRole::Developer,
                profiles.developer.clone(),
            ))
            .unwrap();
        let reviewer_session = router
            .open_session(session_spec(
                WorkerRole::Reviewer,
                profiles.reviewer.clone(),
            ))
            .unwrap();
        let wrong_role = router
            .start_turn(
                developer_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer.clone(),
                ),
            )
            .unwrap_err();
        assert_eq!(wrong_role.code, RuntimeErrorCode::InvalidIdentity);

        let developer_turn = router
            .start_turn(
                developer_session,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::InitialDevelopment,
                    profiles.developer.clone(),
                ),
            )
            .unwrap();
        let second_live = router
            .start_turn(
                reviewer_session,
                turn_spec(
                    WorkerRole::Reviewer,
                    RuntimeTurnPurpose::InitialReview,
                    profiles.reviewer.clone(),
                ),
            )
            .unwrap_err();
        assert_eq!(second_live.code, RuntimeErrorCode::InvalidTransition);
        assert!(!router.poll_turn(developer_turn).unwrap().is_terminal());
        assert!(router.poll_turn(developer_turn).unwrap().is_terminal());

        let correction = router
            .start_turn(
                developer_session,
                turn_spec(
                    WorkerRole::Developer,
                    RuntimeTurnPurpose::DeveloperCorrection,
                    profiles.developer.clone(),
                ),
            )
            .unwrap();
        assert!(router.poll_turn(correction).unwrap().is_terminal());
    }
}
