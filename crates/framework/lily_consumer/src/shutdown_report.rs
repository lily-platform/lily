//! Bounded, payload-free evidence for the application shutdown attempt.

use std::{sync::OnceLock, time::Duration};

use lily_shutdown::{
    FrameworkComponentStatus, FrameworkForcedCleanupStatus, FrameworkShutdownReport,
};
use serde::Serialize;

/// Result of an individual coordinator action, independent of resource joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerShutdownActionOutcome {
    /// The action was not required.
    NotAttempted,
    /// The action returned successfully.
    Completed,
    /// The action returned an error; error text is deliberately excluded.
    Failed,
    /// The action panicked; panic text is deliberately excluded.
    Panicked,
    /// The action exceeded its remaining deadline.
    TimedOut,
    /// Force interrupted the graceful action.
    CancelledByForce,
    /// An explicit force handle was unavailable.
    Unavailable,
}

/// One application component's original graceful and force outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConsumerShutdownActionReport {
    /// Framework phase, in execution order.
    pub phase: &'static str,
    /// Framework component name, never a queue, event or application identity.
    pub component: String,
    /// Original graceful outcome, retained even if force later completes.
    pub graceful: ConsumerShutdownActionOutcome,
    /// Outcome of the separate force operation.
    pub forced: ConsumerShutdownActionOutcome,
}

/// Ownership and receipt evidence for an optional application dependency.
///
/// `terminal` describes actual owner/worker termination, while `succeeded`
/// additionally requires successful disposal. A failed but joined disposer
/// can therefore be terminal. For caller-owned or absent resources all four
/// fields are false: Lily makes no claim that it disposed them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConsumerResourceReport {
    /// Whether Lily owns this resource's cleanup.
    pub owned: bool,
    /// Whether its original cleanup receipt was created.
    pub started: bool,
    /// Whether its actual termination prerequisites and receipts were observed.
    pub terminal: bool,
    /// Whether the original cleanup succeeded and termination was confirmed.
    pub succeeded: bool,
}

impl ConsumerResourceReport {
    fn reconciled(self) -> bool {
        !self.owned || self.terminal
    }

    fn successful(self) -> bool {
        !self.owned || self.succeeded
    }
}

/// Evidence for optional dependencies, sampled after final reconciliation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConsumerDependencyReport {
    /// Owned application DI, including its outstanding scope/cleanup barrier.
    pub di: ConsumerResourceReport,
    /// Owned telemetry, requiring both the shutdown owner and worker joins.
    pub telemetry: ConsumerResourceReport,
    /// Installed signal monitor; an abort request alone is not terminal.
    pub signal_monitor: ConsumerResourceReport,
}

impl ConsumerDependencyReport {
    fn reconciled(self) -> bool {
        self.di.reconciled() && self.telemetry.reconciled() && self.signal_monitor.reconciled()
    }

    fn successful(self) -> bool {
        self.di.successful() && self.telemetry.successful() && self.signal_monitor.successful()
    }
}

/// Aggregate outcome; known failures remain distinct from outstanding work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerShutdownCompletion {
    /// All owned barriers and the managed runtime join succeeded gracefully.
    GracefulCompleted,
    /// All owned barriers and the managed runtime join succeeded using force.
    ForcedCompleted,
    /// Resources are terminal, but at least one lifecycle operation failed.
    Failed,
    /// At least one required termination or coordinator receipt is unconfirmed.
    Incomplete,
}

/// Immutable final evidence for one managed Consumer shutdown attempt.
///
/// Available through [`crate::ManagedConsumer::shutdown_report`] after the
/// actual runtime task join, including error returns. It retains a bounded
/// list of application components, never per-delivery history or error/panic
/// text. Queue delivery counters remain in [`crate::ManagedConsumer::snapshot`].
/// Late cleanup cannot rewrite the attempt's original outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConsumerShutdownReport {
    /// Classified from termination evidence and the original runtime result.
    pub completion: ConsumerShutdownCompletion,
    /// Whether the attempt used explicit or deadline-triggered force cleanup.
    pub forced: bool,
    /// Configured total budget, shared by all phases.
    pub budget: Duration,
    /// Time from the first shutdown initiation through dependency reconciliation.
    pub elapsed: Duration,
    /// Original coordinator actions account for every registered component.
    pub coordinator_accounted: bool,
    /// Every coordinator action proved graceful or forced cleanup.
    pub coordinator_complete: bool,
    /// Actual managed runtime join, which follows its supervisor/cleanup joins.
    /// This does not by itself prove resource termination.
    pub runtime_joined: bool,
    /// Delivery, scope, transaction, relay and subscription task barriers.
    pub queue_drain_reconciled: bool,
    /// Confirmed broker close in addition to the queue drain barrier.
    pub queue_close_reconciled: bool,
    /// Receipt evidence for dependencies Lily owns.
    pub dependencies: ConsumerDependencyReport,
    /// One entry per registered application action, in execution order.
    pub actions: Vec<ConsumerShutdownActionReport>,
    /// Original managed result's stable failure code; no error text or payload.
    pub failure_code: Option<&'static str>,
}

impl ConsumerShutdownReport {
    /// True only for fully reconciled, successful graceful or forced shutdown.
    pub fn is_success(&self) -> bool {
        matches!(
            self.completion,
            ConsumerShutdownCompletion::GracefulCompleted
                | ConsumerShutdownCompletion::ForcedCompleted
        )
    }

    pub(crate) fn capture(
        coordinator: &FrameworkShutdownReport,
        elapsed: Duration,
        queue_drain_reconciled: bool,
        queue_close_reconciled: bool,
        dependencies: ConsumerDependencyReport,
    ) -> Self {
        Self {
            completion: ConsumerShutdownCompletion::Incomplete,
            forced: coordinator.forced,
            budget: coordinator.deadline,
            elapsed,
            coordinator_accounted: coordinator.reconciles(),
            coordinator_complete: coordinator.is_terminal_complete(),
            runtime_joined: false,
            queue_drain_reconciled,
            queue_close_reconciled,
            dependencies,
            actions: coordinator
                .phases
                .iter()
                .flat_map(|phase| {
                    phase
                        .components
                        .iter()
                        .map(|component| ConsumerShutdownActionReport {
                            phase: phase.phase.description(),
                            component: component.name.clone(),
                            graceful: match component.status {
                                FrameworkComponentStatus::Completed => {
                                    ConsumerShutdownActionOutcome::Completed
                                }
                                FrameworkComponentStatus::Failed { .. } => {
                                    ConsumerShutdownActionOutcome::Failed
                                }
                                FrameworkComponentStatus::Panicked { .. } => {
                                    ConsumerShutdownActionOutcome::Panicked
                                }
                                FrameworkComponentStatus::TimedOut => {
                                    ConsumerShutdownActionOutcome::TimedOut
                                }
                                FrameworkComponentStatus::CancelledByForce => {
                                    ConsumerShutdownActionOutcome::CancelledByForce
                                }
                            },
                            forced: match component.forced_cleanup {
                                FrameworkForcedCleanupStatus::NotAttempted => {
                                    ConsumerShutdownActionOutcome::NotAttempted
                                }
                                FrameworkForcedCleanupStatus::Completed => {
                                    ConsumerShutdownActionOutcome::Completed
                                }
                                FrameworkForcedCleanupStatus::Failed { .. } => {
                                    ConsumerShutdownActionOutcome::Failed
                                }
                                FrameworkForcedCleanupStatus::Panicked { .. } => {
                                    ConsumerShutdownActionOutcome::Panicked
                                }
                                FrameworkForcedCleanupStatus::TimedOut => {
                                    ConsumerShutdownActionOutcome::TimedOut
                                }
                                FrameworkForcedCleanupStatus::Unavailable => {
                                    ConsumerShutdownActionOutcome::Unavailable
                                }
                            },
                        })
                })
                .collect(),
            failure_code: None,
        }
    }

    pub(crate) fn after_runtime_join(mut self, failure_code: Option<&'static str>) -> Self {
        self.runtime_joined = true;
        self.failure_code = failure_code;
        let resources_terminal = self.queue_drain_reconciled
            && self.queue_close_reconciled
            && self.dependencies.reconciled();
        // A joined failing disposer is terminal; an expired force observer is
        // not a completion receipt. Late resource joins cannot rewrite that
        // original missing action evidence as a mere known failure.
        let unconfirmed_action = self.actions.iter().any(|action| {
            use ConsumerShutdownActionOutcome::*;
            matches!(action.forced, TimedOut | Unavailable)
                || (action.forced == NotAttempted
                    && matches!(action.graceful, TimedOut | CancelledByForce))
        });
        self.completion =
            if !resources_terminal || !self.coordinator_accounted || unconfirmed_action {
                ConsumerShutdownCompletion::Incomplete
            } else if failure_code.is_some() || !self.dependencies.successful() {
                ConsumerShutdownCompletion::Failed
            } else if !self.coordinator_complete {
                ConsumerShutdownCompletion::Incomplete
            } else if self.forced {
                ConsumerShutdownCompletion::ForcedCompleted
            } else {
                ConsumerShutdownCompletion::GracefulCompleted
            };
        self
    }
}

/// One write, independent of public waiter lifetime. No user code in publication.
pub(crate) type ConsumerShutdownObservation = OnceLock<ConsumerShutdownReport>;

#[cfg(test)]
mod tests {
    use super::*;
    use lily_shutdown::{
        FrameworkShutdownCoordinator, FrameworkShutdownPhase, ShutdownAction, ShutdownError,
        ShutdownSignal, ShutdownState,
    };
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn timed_out_force_receipt_is_not_rewritten_by_later_resource_termination() {
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_millis(40),
        );
        coordinator.register(
            ShutdownAction::new(
                "consumer-queue-drain",
                FrameworkShutdownPhase::DrainInFlight,
                Duration::from_secs(1),
                || std::future::pending(),
            )
            .with_force(|| std::future::pending()),
        );
        let original = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(!original.is_terminal_complete());
        // Resource owners can join after the coordinator observer expires.
        // This does not establish success for that original force invocation.
        let report = ConsumerShutdownReport::capture(
            &original,
            original.elapsed,
            true,
            true,
            ConsumerDependencyReport::default(),
        )
        .after_runtime_join(Some("CONSUMER_SHUTDOWN_INCOMPLETE"));
        assert!(report.queue_close_reconciled && report.runtime_joined);
        assert_eq!(
            report.actions[0].forced,
            ConsumerShutdownActionOutcome::TimedOut
        );
        assert_eq!(report.completion, ConsumerShutdownCompletion::Incomplete);
        assert!(!report.is_success());
    }

    #[tokio::test]
    async fn joined_failed_actions_remain_known_failure_without_claiming_success() {
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(1),
        );
        coordinator.register(
            ShutdownAction::new(
                "consumer-di-container",
                FrameworkShutdownPhase::DisposeDependencies,
                Duration::from_secs(1),
                || async { Err(ShutdownError::Component("private disposal data".into())) },
            )
            .with_force(|| async { Err(ShutdownError::Component("private disposal data".into())) }),
        );
        let original = coordinator.execute_report(ShutdownSignal::Manual).await;
        let report = ConsumerShutdownReport::capture(
            &original,
            original.elapsed,
            true,
            true,
            ConsumerDependencyReport {
                di: ConsumerResourceReport {
                    owned: true,
                    started: true,
                    terminal: true,
                    succeeded: false,
                },
                ..Default::default()
            },
        )
        .after_runtime_join(Some("INJECTION_SERVICE_DISPOSAL_FAILED"));
        assert_eq!(report.completion, ConsumerShutdownCompletion::Failed);
        assert!(!report.is_success());
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("private disposal data")
        );
    }
}
