//! One immutable shutdown attempt, plus compact lifetime owner evidence.
//! Root-body completion cannot prove its own Tokio join: await_terminal records
//! that separately, after awaiting the retained handle.

use super::*;
use crate::reporting::DependencyState;
use lily_shutdown::{
    FrameworkShutdownCompletion, FrameworkShutdownMetricSnapshot, FrameworkShutdownReport,
};

/// A subscriber is application code too. Diagnostic failure must not prevent
/// terminal publication or interrupt dependency disposal.
pub(super) fn diagnostic(emit: impl FnOnce()) {
    let _ = catch_unwind(AssertUnwindSafe(emit));
}

type DisconnectRecord = Arc<StdMutex<crate::lifecycle::LifecycleInvocation>>;
type ArmedDisconnect = (WebSocketLifecycleHandler, Duration, DisconnectRecord);

#[derive(Default)]
pub(super) struct DisconnectLedger {
    handlers: Vec<ArmedDisconnect>,
    records: Vec<DisconnectRecord>,
}

impl DisconnectLedger {
    pub(super) fn arm(&mut self, handler: WebSocketLifecycleHandler, timeout: Duration) {
        let record = Arc::new(StdMutex::new(
            crate::lifecycle::LifecycleInvocation::default(),
        ));
        self.handlers.push((handler, timeout, record.clone()));
        self.records.push(record);
    }
    pub(super) fn take_handlers(&mut self) -> Vec<ArmedDisconnect> {
        std::mem::take(&mut self.handlers)
    }
    pub(super) fn obligations(&self) -> usize {
        self.records.len()
    }
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.handlers.len()
    }
    pub(super) fn accounting(&self) -> crate::reporting::InvocationCounts {
        let mut counts = crate::reporting::InvocationCounts::default();
        for record in &self.records {
            counts.record(
                *record
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        counts
    }
}

#[cfg(test)]
impl From<Vec<(WebSocketLifecycleHandler, Duration)>> for DisconnectLedger {
    fn from(handlers: Vec<(WebSocketLifecycleHandler, Duration)>) -> Self {
        let mut ledger = Self::default();
        for (handler, timeout) in handlers {
            ledger.arm(handler, timeout);
        }
        ledger
    }
}

/// Lives outside the scoped callback future. Dropping that future must release
/// its borrows before this observation can record interrupted invocation.
pub(super) struct DisconnectObservation(DisconnectRecord);
impl DisconnectObservation {
    pub(super) fn new(record: DisconnectRecord) -> Self {
        assert!(
            record
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .claim()
        );
        Self(record)
    }
    pub(super) fn start(&self) {
        assert!(
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .start()
        );
    }
    pub(super) fn finish(&self, outcome: crate::lifecycle::LifecycleOutcome) {
        let mut record = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            record.state,
            crate::lifecycle::LifecycleInvocationState::Terminal { .. }
        ) {
            assert!(record.finish(outcome));
        }
    }
}
impl Drop for DisconnectObservation {
    fn drop(&mut self) {
        self.finish(if std::thread::panicking() {
            crate::lifecycle::LifecycleOutcome::Panicked
        } else {
            crate::lifecycle::LifecycleOutcome::Interrupted(
                crate::lifecycle::LifecycleInterruption::Cancelled,
            )
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CoordinatorSummary {
    completion: FrameworkShutdownCompletion,
    forced: bool,
    component_count: usize,
    reconciles: bool,
    metrics: FrameworkShutdownMetricSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ShutdownEvidence {
    pub(super) background: lily_background_service::BackgroundServiceSnapshot,
    pub(super) background_monitor: crate::tasks::TaskSnapshot,
    pub(super) messages: ownership::MessageAccounting,
    pub(super) connections: crate::middleware::ConnectionAccounting,
    // Scope receipts prove exact-generation worker termination, not successful
    // disposal of every resource. DI owns the individual disposer outcomes.
    pub(super) scope_receipts_observed: usize,
    pub(super) scope_receipts_outstanding: usize,
    pub(super) scope_deadline_failures: usize,
    pub(super) tasks: [(&'static str, crate::tasks::TaskSnapshot); 9],
    pub(super) dispatches_outstanding: usize,
    pub(super) backplane: DependencyState,
    pub(super) container: DependencyState,
    pub(super) container_quiescent: bool,
    pub(super) telemetry: DependencyState,
    pub(super) shutdown_message_cleanup_failures: usize,
    pub(super) shutdown_reconciliation_failed: bool,
}

impl ShutdownEvidence {
    pub(super) fn quiescent(&self) -> bool {
        self.background.is_terminal()
            && self.background_monitor.outstanding == 0
            && self.tasks.iter().all(|(_, tasks)| tasks.outstanding == 0)
            && self.messages.outstanding == 0
            && self.messages.output.outstanding == 0
            && self.connections.workers.outstanding == 0
            && self.scope_receipts_outstanding == 0
            && self.dispatches_outstanding == 0
            && self.backplane.terminal()
            && self.container.terminal()
            && self.container_quiescent
            && self.telemetry.terminal()
    }

    pub(super) fn reconciles(&self) -> bool {
        let messages = self.messages;
        self.background.reconciles()
            && messages.reconciles()
            && self.connections.owners == self.connections.workers.total
            && self.connections.workers.reconciles()
            && self.connections.disconnected.reconciles()
            && self.connections.middleware.reconciles()
            && self.connections.owners
                == self.connections.stages.connections + self.connections.stages_unobserved
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ShutdownReport {
    pub(super) completion: FrameworkShutdownCompletion,
    pub(super) forced: bool,
    pub(super) elapsed: Duration,
    pub(super) deadline_elapsed: bool,
    pub(super) coordinator: Option<CoordinatorSummary>,
    pub(super) evidence: ShutdownEvidence,
}

impl WsApp {
    pub(super) fn begin_shutdown_reporting(&self) {
        self.lifecycle.reporting_started.get_or_init(Instant::now);
    }

    pub(super) fn record_coordinator(&self, report: &FrameworkShutdownReport) {
        self.lifecycle.health.record_shutdown_report(report);
        let _ = self.lifecycle.coordinator_report.set(CoordinatorSummary {
            completion: report.completion,
            forced: report.forced,
            component_count: report.component_count(),
            reconciles: report.reconciles(),
            metrics: report.metrics,
        });
    }

    pub(super) fn shutdown_evidence(&self) -> ShutdownEvidence {
        let (messages, message_drivers) = self.message_dispatch_registry.accounting();
        let (connections, connection_drivers) = self.connection_cleanup_registry.accounting();
        let (
            scope_receipts_observed,
            scope_receipts_outstanding,
            scope_deadline_failures,
            scope_drivers,
        ) = self.scope_cleanup_registry.accounting();
        let (backplane, ingress, backplane_drivers, dispatches_outstanding) =
            self.dispatcher.shutdown_accounting();
        let container = if !self.owns_container {
            DependencyState::NotOwned
        } else if let Some(succeeded) =
            lily_injection::__private::container_shutdown_succeeded(&self.container)
        {
            if succeeded {
                DependencyState::Completed
            } else {
                DependencyState::Failed
            }
        } else if lily_injection::__private::container_shutdown_started(&self.container) {
            DependencyState::Unconfirmed
        } else {
            DependencyState::NotStarted
        };
        let telemetry = match self.lifecycle.tracing_task.get() {
            Some(task) => match task.clone().now_or_never() {
                None => DependencyState::Unconfirmed,
                Some(Ok(result)) if result.is_success() => DependencyState::Completed,
                Some(Ok(_)) => DependencyState::Failed,
                Some(Err(error)) if error.is_cancelled() => DependencyState::Cancelled,
                Some(Err(_)) => DependencyState::Panicked,
            },
            None if self.lifecycle.owns_tracing => DependencyState::NotStarted,
            None => DependencyState::NotOwned,
        };
        ShutdownEvidence {
            background: self.background_snapshot(),
            background_monitor: self
                .lifecycle
                .background
                .as_ref()
                .map_or_else(Default::default, |background| {
                    background.monitors.snapshot()
                }),
            messages,
            connections,
            scope_receipts_observed,
            scope_receipts_outstanding,
            scope_deadline_failures,
            tasks: [
                ("server", self.lifecycle.server_tasks.snapshot()),
                ("connection", self.lifecycle.connection_tasks.snapshot()),
                ("maintenance", self.lifecycle.maintenance_tasks.snapshot()),
                ("signal", self.lifecycle.signal_tasks.snapshot()),
                ("message_receipt", message_drivers),
                ("connection_receipt", connection_drivers),
                ("scope_receipt", scope_drivers),
                ("ingress", ingress),
                ("backplane_receipt", backplane_drivers),
            ],
            dispatches_outstanding,
            backplane,
            container,
            telemetry,
            container_quiescent: !self.owns_container
                || lily_injection::__private::container_shutdown_quiescent(&self.container),
            shutdown_message_cleanup_failures: self.message_dispatch_registry.cleanup_failures(),
            shutdown_reconciliation_failed: self
                .lifecycle
                .reconciliation_failed
                .load(Ordering::Acquire),
        }
    }

    pub(super) async fn finish_shutdown(&self, result: Result<(), String>) {
        let evidence = self.shutdown_evidence();
        let coordinator = self.lifecycle.coordinator_report.get().cloned();
        let forced = self.lifecycle.shutdown_state.is_force_requested()
            || evidence.background.abort_requested != 0
            || self.scope_cleanup_registry.budget.is_forced()
            || coordinator.as_ref().is_some_and(|report| report.forced);
        let complete = result.is_ok()
            && evidence.background.succeeded()
            && evidence.background_monitor.panicked == 0
            && evidence.quiescent()
            && evidence.reconciles()
            && !evidence.shutdown_reconciliation_failed
            && evidence.shutdown_message_cleanup_failures == 0
            && [evidence.backplane, evidence.container, evidence.telemetry]
                .iter()
                .all(|state| {
                    matches!(
                        state,
                        DependencyState::Completed | DependencyState::NotOwned
                    )
                });
        // Preserve a more specific existing lifecycle failure (for example
        // bind_failed). Only a newly discovered aggregate failure needs this
        // fallback reason, including post-coordinator signal/join failures.
        if !complete
            && self
                .lifecycle
                .health
                .snapshot()
                .map_or(true, |snapshot| snapshot.live)
        {
            let _ = self
                .lifecycle
                .health
                .record_lifecycle_failure(WS_LISTENER_HEALTH_CHECK, "shutdown_incomplete");
        }
        if evidence.background.failed > 0
            || evidence.background.panicked > 0
            || evidence.background.supervisor_failed
        {
            let _ = self
                .lifecycle
                .health
                .record_lifecycle_failure(background::HEALTH_CHECK, "execution_failed");
        }
        if evidence.quiescent()
            && let Some(background) = &self.lifecycle.background
        {
            background.release();
        }
        let report = ShutdownReport {
            completion: if !complete {
                FrameworkShutdownCompletion::Incomplete
            } else if forced {
                FrameworkShutdownCompletion::ForcedCompleted
            } else {
                FrameworkShutdownCompletion::GracefulCompleted
            },
            forced,
            elapsed: self
                .lifecycle
                .reporting_started
                .get()
                .map_or(Duration::ZERO, Instant::elapsed),
            deadline_elapsed: self
                .lifecycle
                .root_budget
                .hard_deadline()
                .is_some_and(|deadline| Instant::now() >= deadline),
            coordinator,
            evidence,
        };
        // This event contains only fixed fields/counters. Export through an
        // application subscriber is best effort after owned telemetry closes;
        // the pre-flush checkpoint is emitted while Lily telemetry is live.
        diagnostic(|| {
            tracing::info!(target: "lily_websocket::shutdown", ?report,
            root_join_observed = false, "WebSocket shutdown outcome recorded")
        });
        assert!(self.lifecycle.shutdown_report.set(report).is_ok());
        let result = if result.is_ok() && !complete {
            Err("WebSocket shutdown evidence is incomplete or does not reconcile".into())
        } else {
            result
        };
        Self::complete_lifecycle(&self.lifecycle, result).await;
    }
}

#[cfg(test)]
#[path = "reporting_tests.rs"]
mod tests;
