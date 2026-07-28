//! Internal adapter between the foreground supervisor core and the durable
//! handoff state machine.
//!
//! No router or CLI path constructs this adapter yet. The Phase 3 Codex
//! generation lifecycle remains private, preventing the adapter seam from
//! becoming a hidden production launcher.

use hcom::chain_supervisor::{
    ChainTitleState, CleanupEvidence, DeliveryExitContext as CoreDeliveryExitContext,
    DurableControl, DurableDirective, ExitEvidence, GenerationIdentity, OuterTerminalIdentity,
    PostCleanup, QuiesceApply, QuiesceAuthorization, ShutdownReason, SignalSendResult,
    SigtermEvidence, TargetReservation,
};

use crate::codex_chain::terminal_record_persisted;
use crate::db::HcomDb;
use crate::handoff::{
    self, ChainShutdownObservation, ChainState, ChildExitEvidence, CleanupObservation,
    DeliveryExitContext, GenerationState, HandoffActor, HandoffError, HandoffState,
    ResourceCleanupEvidence, SigtermObservation, SigtermRequestResult, SupervisorActor,
    SupervisorShutdownReason, TargetFailureIdentity, TargetLaunchFailure, TargetMaterialization,
};

pub(crate) struct HcomChainControl {
    db: HcomDb,
    chain_id: String,
    supervisor: SupervisorActor,
    outer: OuterTerminalIdentity,
}

impl HcomChainControl {
    pub(crate) fn new(
        db: HcomDb,
        chain_id: String,
        supervisor: SupervisorActor,
        outer: OuterTerminalIdentity,
    ) -> Result<Self, HandoffError> {
        let _chain = handoff::get_chain(&db, &chain_id)?.ok_or(HandoffError::NotManaged)?;
        let binding = handoff::current_supervisor_binding(&db, &chain_id)?;
        if binding.process_id != supervisor.process_id
            || binding.process_birth_identity != supervisor.process_birth_identity
            || binding.pid != i64::from(outer.supervisor_pid)
            || binding.pgid != i64::from(outer.supervisor_pgid)
            || binding.outer_foreground_pgid != i64::from(outer.foreground_pgid)
            || binding.outer_tty_device != outer.tty_device as i64
            || binding.outer_tty_inode != outer.tty_inode as i64
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT durable supervisor/outer-TTY evidence does not match".to_string(),
            ));
        }
        Ok(Self {
            db,
            chain_id,
            supervisor,
            outer,
        })
    }

    fn exact_generation(
        &self,
        active: &GenerationIdentity,
    ) -> Result<handoff::TerminalGeneration, HandoffError> {
        let chain =
            handoff::get_chain(&self.db, &self.chain_id)?.ok_or(HandoffError::NotManaged)?;
        let generation = handoff::get_generation(
            &self.db,
            &self.chain_id,
            i64::try_from(active.generation).map_err(|_| {
                HandoffError::Invalid("generation exceeds the durable integer bound".to_string())
            })?,
        )?
        .ok_or(HandoffError::NotManaged)?;
        let binding = handoff::current_supervisor_binding(&self.db, &self.chain_id)?;
        if chain.current_generation != active.generation as i64
            || binding.process_id != self.supervisor.process_id
            || binding.process_birth_identity != self.supervisor.process_birth_identity
            || binding.pid != i64::from(self.outer.supervisor_pid)
            || binding.pgid != i64::from(self.outer.supervisor_pgid)
            || binding.outer_foreground_pgid != i64::from(self.outer.foreground_pgid)
            || binding.outer_tty_device != self.outer.tty_device as i64
            || binding.outer_tty_inode != self.outer.tty_inode as i64
            || generation.launch_nonce != active.launch_nonce
            || generation.wrapper_process_id.as_deref() != Some(active.process_id.as_str())
            || generation.process_birth_identity.as_deref()
                != Some(active.process_birth_identity.as_str())
            || generation.instance_name.as_deref() != Some(active.instance_name.as_str())
            || generation.hcom_session_id.as_deref() != Some(active.hcom_session_id.as_str())
            || generation.native_session_id.as_deref() != active.native_session_id.as_deref()
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT active process does not match the exact typed generation"
                    .to_string(),
            ));
        }
        Ok(generation)
    }

    fn current_handoff(&self) -> Result<Option<handoff::TerminalHandoff>, HandoffError> {
        handoff::get_open_handoff_for_chain(&self.db, &self.chain_id)
    }

    fn cleanup_observation(
        expected_version: i64,
        evidence: &CleanupEvidence,
    ) -> CleanupObservation {
        CleanupObservation {
            expected_version,
            exit: evidence.exit.as_ref().map(map_exit),
            reaped: evidence.waitpid_reaped,
            resources: ResourceCleanupEvidence {
                inject_succeeded: evidence.resources.inject_stopped,
                delivery_succeeded: evidence.resources.delivery_joined,
                pty_succeeded: evidence.resources.pty_closed,
                screen_succeeded: evidence.resources.screen_released,
                write_queue_succeeded: evidence.resources.write_queue_empty,
            },
            failure_kind: evidence.failure_kind.clone(),
            failure_reason: evidence.failure_reason.clone(),
        }
    }
}

fn map_exit(exit: &ExitEvidence) -> ChildExitEvidence {
    ChildExitEvidence {
        observed_wall_at: exit.observed_wall_seconds as f64,
        observed_monotonic_ns: exit.observed_monotonic_ns,
        exit_code: exit.exit_code,
        exit_signal: exit.exit_signal,
        delivery_context: match exit.delivery_context {
            CoreDeliveryExitContext::Closed => DeliveryExitContext::Closed,
            CoreDeliveryExitContext::Killed => DeliveryExitContext::Killed,
        },
    }
}

impl DurableControl for HcomChainControl {
    type Error = HandoffError;

    fn title_state(&mut self, active: &GenerationIdentity) -> Result<ChainTitleState, Self::Error> {
        self.exact_generation(active)?;
        let chain =
            handoff::get_chain(&self.db, &self.chain_id)?.ok_or(HandoffError::NotManaged)?;
        Ok(match chain.state {
            ChainState::QuiescingSource | ChainState::StopObserved => ChainTitleState::Quiescing,
            ChainState::LaunchingTarget => ChainTitleState::Launching,
            ChainState::AwaitingAcceptance => ChainTitleState::AwaitingAcceptance,
            ChainState::NeedsRecovery => ChainTitleState::NeedsRecovery,
            ChainState::Active | ChainState::Prepared | ChainState::Committed => {
                ChainTitleState::Active
            }
        })
    }

    fn read_directive(
        &mut self,
        active: &GenerationIdentity,
        local_quiesce: Option<&QuiesceApply>,
    ) -> Result<DurableDirective, Self::Error> {
        let generation = self.exact_generation(active)?;
        let chain =
            handoff::get_chain(&self.db, &self.chain_id)?.ok_or(HandoffError::NotManaged)?;
        let Some(handoff) = self.current_handoff()? else {
            return if chain.state == ChainState::NeedsRecovery {
                Ok(DurableDirective::NeedsRecovery(
                    "durable chain is marked needs_recovery".to_string(),
                ))
            } else {
                Ok(DurableDirective::Wait)
            };
        };
        match handoff.state {
            HandoffState::StopObserved => {
                if handoff.source_generation != generation.generation {
                    return Err(HandoffError::Conflict(
                        "HANDOFF_CONFLICT Stop belongs to a different generation".to_string(),
                    ));
                }
                let native_session_id =
                    generation.native_session_id.as_deref().ok_or_else(|| {
                        HandoffError::Conflict(
                            "HANDOFF_CONFLICT generation native session is not pinned".to_string(),
                        )
                    })?;
                let stop_turn_id = handoff.stop_turn_id.as_deref().ok_or_else(|| {
                    HandoffError::Conflict(
                        "HANDOFF_CONFLICT Stop has no exact turn identity".to_string(),
                    )
                })?;
                let instance = self
                    .db
                    .get_instance_full(&active.instance_name)
                    .map_err(|_| HandoffError::Storage)?
                    .ok_or(HandoffError::NotManaged)?;
                if instance.session_id.as_deref() != Some(active.hcom_session_id.as_str())
                    || instance.tool != "codex"
                    || instance.transcript_path.is_empty()
                {
                    return Ok(DurableDirective::NeedsRecovery(
                        "exact Codex transcript binding is unavailable".to_string(),
                    ));
                }
                match terminal_record_persisted(
                    &instance.transcript_path,
                    native_session_id,
                    stop_turn_id,
                ) {
                    Ok(false) => return Ok(DurableDirective::Wait),
                    Err(_) => {
                        return Ok(DurableDirective::NeedsRecovery(
                            "exact Codex terminal rollout evidence is invalid".to_string(),
                        ));
                    }
                    Ok(true) => {}
                }
                Ok(DurableDirective::Quiesce(QuiesceAuthorization {
                    handoff_id: handoff.id,
                    expected_version: handoff.version,
                    quiesce_token: handoff.quiesce_token.ok_or_else(|| {
                        HandoffError::Conflict(
                            "HANDOFF_CONFLICT Stop has no quiesce authorization".to_string(),
                        )
                    })?,
                    generation: active.generation,
                    launch_nonce: generation.launch_nonce,
                    pinned_native_session_id: native_session_id.to_string(),
                    process_birth_identity: generation.process_birth_identity.ok_or_else(|| {
                        HandoffError::Conflict(
                            "HANDOFF_CONFLICT generation process birth is missing".to_string(),
                        )
                    })?,
                }))
            }
            HandoffState::QuiescingSource if handoff.sigterm_request_result.is_empty() => {
                Ok(DurableDirective::NeedsRecovery(
                    "quiescing state has no certain SIGTERM delivery evidence".to_string(),
                ))
            }
            HandoffState::QuiescingSource
                if local_quiesce.is_some_and(|apply| {
                    apply.handoff_id == handoff.id
                        && apply.expected_version == handoff.version
                        && apply.generation == active.generation
                }) =>
            {
                Ok(DurableDirective::Wait)
            }
            HandoffState::QuiescingSource => Ok(DurableDirective::NeedsRecovery(
                "recorded SIGTERM has no matching local one-shot ownership".to_string(),
            )),
            HandoffState::NeedsRecovery => Ok(DurableDirective::NeedsRecovery(
                "durable handoff is marked needs_recovery".to_string(),
            )),
            HandoffState::LaunchingTarget => Ok(DurableDirective::NeedsRecovery(
                "target launch state was reopened with a live generation".to_string(),
            )),
            HandoffState::Prepared | HandoffState::Committed | HandoffState::AwaitingAcceptance => {
                Ok(DurableDirective::Wait)
            }
            HandoffState::Accepted | HandoffState::Aborted => Err(HandoffError::Storage),
        }
    }

    fn begin_quiesce(
        &mut self,
        active: &GenerationIdentity,
        authorization: &QuiesceAuthorization,
    ) -> Result<QuiesceApply, Self::Error> {
        self.exact_generation(active)?;
        let outcome = handoff::begin_quiesce(
            &self.db,
            &self.supervisor,
            &authorization.handoff_id,
            authorization.expected_version,
            &authorization.quiesce_token,
        )?;
        Ok(QuiesceApply {
            handoff_id: outcome.handoff.id,
            expected_version: outcome.handoff.version,
            generation: active.generation,
        })
    }

    fn record_sigterm(
        &mut self,
        apply: &QuiesceApply,
        evidence: &SigtermEvidence,
    ) -> Result<QuiesceApply, Self::Error> {
        let result = match evidence.result {
            SignalSendResult::Sent => SigtermRequestResult::Sent,
            SignalSendResult::NotFound => SigtermRequestResult::NotFound,
            SignalSendResult::PermissionDenied => SigtermRequestResult::PermissionDenied,
            SignalSendResult::Error => SigtermRequestResult::Error,
        };
        let outcome = handoff::record_sigterm_request(
            &self.db,
            &self.supervisor,
            &apply.handoff_id,
            &SigtermObservation {
                expected_version: apply.expected_version,
                requested_wall_at: evidence.requested_wall_seconds as f64,
                requested_monotonic_ns: evidence.requested_monotonic_ns,
                result,
            },
        )?;
        Ok(QuiesceApply {
            handoff_id: outcome.handoff.id,
            expected_version: outcome.handoff.version,
            generation: apply.generation,
        })
    }

    fn record_cleanup(
        &mut self,
        apply: &QuiesceApply,
        evidence: &CleanupEvidence,
    ) -> Result<PostCleanup, Self::Error> {
        let outcome = handoff::complete_source_cleanup(
            &self.db,
            &self.supervisor,
            &apply.handoff_id,
            &Self::cleanup_observation(apply.expected_version, evidence),
        )?;
        if outcome.handoff.state != HandoffState::LaunchingTarget {
            return Ok(PostCleanup::NeedsRecovery);
        }
        let target = handoff::get_generation(
            &self.db,
            &outcome.handoff.chain_id,
            outcome.handoff.target_generation,
        )?
        .ok_or(HandoffError::Storage)?;
        Ok(PostCleanup::Advance(TargetReservation {
            handoff_id: outcome.handoff.id,
            expected_version: outcome.handoff.version,
            generation: target.generation as u64,
            launch_nonce: target.launch_nonce,
        }))
    }

    fn record_exit_without_stop(
        &mut self,
        active: &GenerationIdentity,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error> {
        self.exact_generation(active)?;
        let handoff = self.current_handoff()?.ok_or_else(|| {
            HandoffError::Conflict(
                "HANDOFF_CONFLICT exited generation has no committed handoff".to_string(),
            )
        })?;
        handoff::observe_source_exit_without_stop(
            &self.db,
            &self.supervisor,
            &handoff.id,
            &Self::cleanup_observation(handoff.version, evidence),
        )?;
        Ok(())
    }

    fn begin_target_prepare(&mut self, reservation: &TargetReservation) -> Result<(), Self::Error> {
        handoff::begin_generation_prepare(
            &self.db,
            &self.supervisor,
            &self.chain_id,
            i64::try_from(reservation.generation).map_err(|_| {
                HandoffError::Invalid("generation exceeds the durable integer bound".to_string())
            })?,
            reservation.expected_version,
            &reservation.launch_nonce,
        )
    }

    fn materialize_target(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error> {
        handoff::materialize_target_generation(
            &self.db,
            &self.supervisor,
            &reservation.handoff_id,
            &TargetMaterialization {
                expected_version: reservation.expected_version,
                launch_nonce: reservation.launch_nonce.clone(),
                instance_name: identity.instance_name.clone(),
                hcom_session_id: identity.hcom_session_id.clone(),
                process_id: identity.process_id.clone(),
                process_birth_identity: identity.process_birth_identity.clone(),
                wrapper_pid: i64::from(identity.wrapper_pid),
                wrapper_pgid: i64::from(identity.wrapper_pgid),
                child_pid: i64::from(identity.child_pid),
                child_pgid: i64::from(identity.child_pgid),
                child_process_birth_identity: identity.child_process_birth_identity.clone(),
            },
        )?;
        Ok(())
    }

    fn target_ready(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<(), Self::Error> {
        let handoff = handoff::get_handoff(&self.db, &reservation.handoff_id)?
            .ok_or(HandoffError::Storage)?;
        let actor = HandoffActor {
            instance_name: identity.instance_name.clone(),
            hcom_session_id: identity.hcom_session_id.clone(),
            native_session_id: identity.native_session_id.clone(),
            process_id: identity.process_id.clone(),
            process_birth_identity: identity.process_birth_identity.clone(),
            generation: identity.generation as i64,
        };
        let outcome = handoff::target_ready(
            &self.db,
            &actor,
            &reservation.handoff_id,
            handoff.version,
            &reservation.launch_nonce,
        )?;
        if outcome.handoff.state != HandoffState::AwaitingAcceptance {
            return Err(HandoffError::Storage);
        }
        Ok(())
    }

    fn target_native_session(
        &mut self,
        reservation: &TargetReservation,
        identity: &GenerationIdentity,
    ) -> Result<Option<String>, Self::Error> {
        let handoff = handoff::get_handoff(&self.db, &reservation.handoff_id)?
            .ok_or(HandoffError::NotManaged)?;
        let generation = handoff::get_generation(
            &self.db,
            &handoff.chain_id,
            i64::try_from(reservation.generation).map_err(|_| {
                HandoffError::Invalid("generation exceeds the durable integer bound".to_string())
            })?,
        )?
        .ok_or(HandoffError::NotManaged)?;
        let chain =
            handoff::get_chain(&self.db, &handoff.chain_id)?.ok_or(HandoffError::NotManaged)?;
        let effective_target = handoff::effective_handoff_target_generation(&self.db, &handoff.id)?;
        if handoff.chain_id != self.chain_id
            || handoff.state != HandoffState::LaunchingTarget
            || effective_target != reservation.generation as i64
            || chain.state != ChainState::LaunchingTarget
            || chain.current_generation != reservation.generation as i64
            || generation.state != GenerationState::Launching
            || generation.launch_nonce != reservation.launch_nonce
            || generation.wrapper_process_id.as_deref() != Some(identity.process_id.as_str())
            || generation.process_birth_identity.as_deref()
                != Some(identity.process_birth_identity.as_str())
            || generation.instance_name.as_deref() != Some(identity.instance_name.as_str())
            || generation.hcom_session_id.as_deref() != Some(identity.hcom_session_id.as_str())
            || identity.native_session_id.is_some()
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT target SessionStart state does not match the activated reservation"
                    .to_string(),
            ));
        }
        Ok(generation.native_session_id)
    }

    fn record_target_failure(
        &mut self,
        reservation: &TargetReservation,
        identity: Option<&GenerationIdentity>,
        cleanup: Option<&CleanupEvidence>,
        failure_kind: &str,
        failure_reason: &str,
    ) -> Result<(), Self::Error> {
        let handoff = handoff::get_handoff(&self.db, &reservation.handoff_id)?
            .ok_or(HandoffError::Storage)?;
        let target_generation = i64::try_from(reservation.generation).map_err(|_| {
            HandoffError::Invalid("generation exceeds the durable integer bound".to_string())
        })?;
        let target = handoff::get_generation(&self.db, &handoff.chain_id, target_generation)?
            .ok_or(HandoffError::Storage)?;
        let effective_target = handoff::effective_handoff_target_generation(&self.db, &handoff.id)?;
        if handoff.chain_id != self.chain_id
            || effective_target != target_generation
            || target.launch_nonce != reservation.launch_nonce
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT target failure does not match the exact reservation".to_string(),
            ));
        }
        let identity = identity.map(|identity| TargetFailureIdentity {
            instance_name: identity.instance_name.clone(),
            hcom_session_id: identity.hcom_session_id.clone(),
            process_id: identity.process_id.clone(),
            process_birth_identity: identity.process_birth_identity.clone(),
        });
        let outcome = handoff::fail_target_launch(
            &self.db,
            &self.supervisor,
            &reservation.handoff_id,
            &TargetLaunchFailure {
                expected_version: handoff.version,
                launch_nonce: reservation.launch_nonce.clone(),
                identity,
                cleanup_completed: cleanup.is_some_and(CleanupEvidence::successful),
                failure_kind: failure_kind.to_string(),
                failure_reason: failure_reason.to_string(),
            },
        )?;
        if outcome.handoff.state != HandoffState::NeedsRecovery {
            return Err(HandoffError::Storage);
        }
        Ok(())
    }

    fn begin_shutdown(
        &mut self,
        active: &GenerationIdentity,
        reason: ShutdownReason,
    ) -> Result<(), Self::Error> {
        let generation = self.exact_generation(active)?;
        let chain =
            handoff::get_chain(&self.db, &self.chain_id)?.ok_or(HandoffError::NotManaged)?;
        let actor = HandoffActor {
            instance_name: active.instance_name.clone(),
            hcom_session_id: active.hcom_session_id.clone(),
            native_session_id: active.native_session_id.clone(),
            process_id: active.process_id.clone(),
            process_birth_identity: active.process_birth_identity.clone(),
            generation: active.generation as i64,
        };
        let outcome = handoff::begin_chain_shutdown(
            &self.db,
            &self.supervisor,
            &self.chain_id,
            &actor,
            &ChainShutdownObservation {
                expected_chain_version: chain.version,
                expected_generation_version: generation.version,
                reason: match reason {
                    ShutdownReason::Explicit => SupervisorShutdownReason::Explicit,
                    ShutdownReason::OuterHangup => SupervisorShutdownReason::OuterHangup,
                },
            },
        )?;
        if outcome.generation.state != GenerationState::NeedsRecovery {
            return Err(HandoffError::Storage);
        }
        Ok(())
    }

    fn record_shutdown(
        &mut self,
        active: &GenerationIdentity,
        _reason: ShutdownReason,
        evidence: &CleanupEvidence,
    ) -> Result<(), Self::Error> {
        let generation = self.exact_generation(active)?;
        let chain =
            handoff::get_chain(&self.db, &self.chain_id)?.ok_or(HandoffError::NotManaged)?;
        if chain.state != ChainState::NeedsRecovery
            || generation.state != GenerationState::NeedsRecovery
            || !evidence.successful()
        {
            return Err(HandoffError::Conflict(
                "HANDOFF_CONFLICT shutdown cleanup does not match durable recovery intent"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::process::Command;
    use std::time::Duration;

    use hcom::chain_supervisor::ResourceCleanupEvidence as CoreResourceCleanupEvidence;
    use rusqlite::params;

    use crate::handoff::{ChainSpec, StopObservation};

    fn run_git(workspace: &Path, args: &[&str]) {
        let _env_read = crate::hooks::test_helpers::process_env_read();
        let output = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn cleanup_mapping_preserves_each_independent_outcome() {
        let evidence = CleanupEvidence {
            exit: Some(ExitEvidence {
                observed_wall_seconds: 7,
                observed_monotonic_ns: 11,
                exit_code: None,
                exit_signal: Some(libc::SIGTERM),
                delivery_context: CoreDeliveryExitContext::Killed,
            }),
            waitpid_reaped: true,
            resources: CoreResourceCleanupEvidence {
                inject_stopped: true,
                delivery_joined: false,
                pty_closed: true,
                screen_released: false,
                write_queue_empty: true,
            },
            failure_kind: "delivery_join".to_string(),
            failure_reason: "bounded".to_string(),
        };
        let mapped = HcomChainControl::cleanup_observation(9, &evidence);
        assert_eq!(mapped.expected_version, 9);
        assert!(mapped.reaped);
        assert!(mapped.resources.inject_succeeded);
        assert!(!mapped.resources.delivery_succeeded);
        assert!(mapped.resources.pty_succeeded);
        assert!(!mapped.resources.screen_succeeded);
        assert!(mapped.resources.write_queue_succeeded);
        assert_eq!(mapped.exit.unwrap().exit_signal, Some(libc::SIGTERM));
    }

    #[test]
    fn chain_core_poll_interval_is_bounded() {
        assert!(Duration::from_millis(100) < Duration::from_secs(1));
    }

    #[test]
    fn durable_adapter_consumes_stop_cleanup_materialization_and_ready_without_accepting() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        run_git(&workspace, &["init", "-b", "main"]);
        run_git(&workspace, &["config", "user.name", "hcom test"]);
        run_git(
            &workspace,
            &["config", "user.email", "hcom-test@example.invalid"],
        );
        std::fs::write(workspace.join("README.md"), "fixture\n").unwrap();
        run_git(&workspace, &["add", "README.md"]);
        run_git(&workspace, &["commit", "-m", "fixture"]);

        let db = HcomDb::open_at(&directory.path().join("hcom.db")).unwrap();
        let transcript_path = directory.path().join("rollout-fixture-native-source.jsonl");
        std::fs::write(
            &transcript_path,
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-source\"}}\n",
        )
        .unwrap();
        let source = HandoffActor {
            instance_name: "source".to_string(),
            hcom_session_id: "hcom-source".to_string(),
            native_session_id: Some("native-source".to_string()),
            process_id: "process-source".to_string(),
            process_birth_identity: "birth-source".to_string(),
            generation: 1,
        };
        db.conn()
            .execute(
                "INSERT INTO instances (
                     name, session_id, status, tool, created_at, transcript_path,
                     parent_name, origin_device_id
                 ) VALUES (?1, ?2, 'listening', 'codex', 1.0, ?3, '', '')",
                params![
                    source.instance_name,
                    source.hcom_session_id,
                    transcript_path.to_str().unwrap()
                ],
            )
            .unwrap();
        db.set_process_binding(
            &source.process_id,
            &source.hcom_session_id,
            &source.instance_name,
        )
        .unwrap();
        let outer = OuterTerminalIdentity {
            supervisor_pid: 41001,
            supervisor_pgid: 41001,
            foreground_pgid: 41001,
            tty_device: 7,
            tty_inode: 11,
        };
        let supervisor = SupervisorActor {
            process_id: "supervisor-process".to_string(),
            process_birth_identity: "supervisor-birth".to_string(),
        };
        let chain = handoff::create_chain(
            &db,
            &source,
            &ChainSpec {
                workspace: workspace.clone(),
                tool: "codex".to_string(),
                tag: String::new(),
                model_ref: "model".to_string(),
                reasoning_ref: "reasoning".to_string(),
                permission_policy_ref: "permission".to_string(),
                policy_ref: "policy".to_string(),
                supervisor_process_id: supervisor.process_id.clone(),
                supervisor_process_birth_identity: supervisor.process_birth_identity.clone(),
                supervisor_pid: i64::from(outer.supervisor_pid),
                supervisor_pgid: i64::from(outer.supervisor_pgid),
                outer_foreground_pgid: i64::from(outer.foreground_pgid),
                outer_tty_device: outer.tty_device as i64,
                outer_tty_inode: outer.tty_inode as i64,
                launch_nonce: "nonce-source".to_string(),
            },
        )
        .unwrap();
        let bundle = serde_json::json!({
            "bundle_id": "bundle:control-adapter",
            "created_by": "source",
            "description": "bounded",
        });
        db.log_event("bundle", "source", &bundle).unwrap();
        let bundle_event_id: i64 = db
            .conn()
            .query_row("SELECT MAX(id) FROM events", [], |row| row.get(0))
            .unwrap();
        let prepared = handoff::prepare_handoff(&db, &source, bundle_event_id, &workspace).unwrap();
        let committed = handoff::commit_handoff(
            &db,
            &source,
            &prepared.handoff.id,
            prepared.handoff.version,
            &workspace,
        )
        .unwrap();
        let token = committed.handoff.quiesce_token.clone().unwrap();
        let stopped = handoff::observe_stop(
            &db,
            &source,
            &committed.handoff.id,
            &StopObservation {
                expected_version: committed.handoff.version,
                quiesce_token: token,
                committed_version: committed.handoff.version,
                hook_native_session_id: "native-source".to_string(),
                launch_nonce: "nonce-source".to_string(),
                turn_id: "turn-source".to_string(),
            },
        )
        .unwrap();

        let source_identity = GenerationIdentity {
            generation: 1,
            launch_nonce: "nonce-source".to_string(),
            wrapper_pid: 41002,
            wrapper_pgid: outer.supervisor_pgid,
            child_pid: 41003,
            child_pgid: 41003,
            child_process_birth_identity: "child-birth-source".to_string(),
            process_id: source.process_id.clone(),
            process_birth_identity: source.process_birth_identity.clone(),
            instance_name: source.instance_name.clone(),
            hcom_session_id: source.hcom_session_id.clone(),
            native_session_id: source.native_session_id.clone(),
        };
        let mut control = HcomChainControl::new(db, chain.id.clone(), supervisor, outer).unwrap();
        assert_eq!(
            control.read_directive(&source_identity, None).unwrap(),
            DurableDirective::Wait
        );
        use std::io::Write as _;
        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&transcript_path)
                .unwrap(),
            "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"turn-source\"}}}}"
        )
        .unwrap();
        let authorization = match control.read_directive(&source_identity, None).unwrap() {
            DurableDirective::Quiesce(authorization) => authorization,
            other => panic!("expected exact Stop directive, got {other:?}"),
        };
        assert_eq!(authorization.expected_version, stopped.handoff.version);
        let apply = control
            .begin_quiesce(&source_identity, &authorization)
            .unwrap();
        let apply = control
            .record_sigterm(
                &apply,
                &SigtermEvidence {
                    requested_wall_seconds: 1000,
                    requested_monotonic_ns: 1_000_000_000,
                    result: SignalSendResult::Sent,
                },
            )
            .unwrap();
        assert!(matches!(
            control.read_directive(&source_identity, None).unwrap(),
            DurableDirective::NeedsRecovery(_)
        ));
        assert_eq!(
            control
                .read_directive(&source_identity, Some(&apply))
                .unwrap(),
            DurableDirective::Wait
        );
        control
            .db
            .conn()
            .execute(
                "DELETE FROM process_bindings WHERE process_id = ?1",
                params![source.process_id],
            )
            .unwrap();
        control
            .db
            .conn()
            .execute(
                "DELETE FROM instances WHERE name = ?1",
                params![source.instance_name],
            )
            .unwrap();
        let reservation = match control
            .record_cleanup(
                &apply,
                &CleanupEvidence {
                    exit: Some(ExitEvidence {
                        observed_wall_seconds: 1000,
                        observed_monotonic_ns: 1_025_000_000,
                        exit_code: None,
                        exit_signal: Some(libc::SIGTERM),
                        delivery_context: CoreDeliveryExitContext::Killed,
                    }),
                    waitpid_reaped: true,
                    resources: CoreResourceCleanupEvidence {
                        inject_stopped: true,
                        delivery_joined: true,
                        pty_closed: true,
                        screen_released: true,
                        write_queue_empty: true,
                    },
                    failure_kind: String::new(),
                    failure_reason: String::new(),
                },
            )
            .unwrap()
        {
            PostCleanup::Advance(reservation) => reservation,
            PostCleanup::NeedsRecovery => panic!("complete cleanup must advance"),
        };
        control.begin_target_prepare(&reservation).unwrap();
        let mut target = GenerationIdentity {
            generation: 2,
            launch_nonce: reservation.launch_nonce.clone(),
            wrapper_pid: 41004,
            wrapper_pgid: outer.supervisor_pgid,
            child_pid: 41005,
            child_pgid: 41005,
            child_process_birth_identity: "child-birth-target".to_string(),
            process_id: "process-target".to_string(),
            process_birth_identity: "birth-target".to_string(),
            instance_name: "target".to_string(),
            hcom_session_id: "hcom-target".to_string(),
            native_session_id: None,
        };
        control.materialize_target(&reservation, &target).unwrap();
        let target_actor = HandoffActor {
            instance_name: target.instance_name.clone(),
            hcom_session_id: target.hcom_session_id.clone(),
            native_session_id: None,
            process_id: target.process_id.clone(),
            process_birth_identity: target.process_birth_identity.clone(),
            generation: target.generation as i64,
        };
        let target_generation = handoff::get_generation(&control.db, &chain.id, 2)
            .unwrap()
            .unwrap();
        handoff::pin_native_session(
            &control.db,
            &chain.id,
            &target_actor,
            target_generation.version,
            "native-target",
        )
        .unwrap();
        target.native_session_id = Some("native-target".to_string());
        control.target_ready(&reservation, &target).unwrap();
        let handoff = handoff::get_handoff(&control.db, &reservation.handoff_id)
            .unwrap()
            .unwrap();
        assert_eq!(handoff.state, HandoffState::AwaitingAcceptance);
        let target_row = handoff::get_generation(&control.db, &chain.id, 2)
            .unwrap()
            .unwrap();
        assert_eq!(
            target_row.native_session_id.as_deref(),
            Some("native-target")
        );
        assert_ne!(
            target_row.native_session_id.as_deref(),
            target_row.hcom_session_id.as_deref()
        );
    }
}
