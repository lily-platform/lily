//! Application-owned tracing adapter for the framework shutdown coordinator.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{future::BoxFuture, future::Shared, FutureExt};
use lily_shutdown::{
    FrameworkForceShutdownFuture, FrameworkShutdownComponent, FrameworkShutdownPhase, ShutdownError,
};

#[doc(hidden)]
pub use crate::runtime::tasks::TracingWorkerSnapshot;
use crate::{TraceShutdownReport, TracingRuntimeOwner};

/// Cloneable evidence handle retained by the composition root after the
/// shutdown component itself is moved into the coordinator.
#[derive(Clone, Default)]
pub struct TracingShutdownEvidence {
    report: Arc<Mutex<Option<TraceShutdownReport>>>,
    receipt: Arc<OnceLock<TracingShutdownReceipt>>,
}

/// Actual adapter task join disposition, independent of exporter report success.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TracingShutdownOwnerOutcome {
    /// The adapter returned a report; that report may still describe failure.
    Returned,
    /// The actual task join reported cancellation.
    Cancelled,
    /// The actual task join reported a panic.
    Panicked,
}

impl TracingShutdownEvidence {
    /// True only after the actual adapter task receipt has been published.
    /// Constructing an adapter/evidence handle alone registers no task.
    #[doc(hidden)]
    pub fn owner_registered(&self) -> bool {
        self.receipt.get().is_some()
    }

    /// Returns the terminal shutdown report after the coordinator has run.
    pub fn report(&self) -> Option<TraceShutdownReport> {
        self.report
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// A returned report does not prove even the owner task joined.
    #[doc(hidden)]
    pub fn owner_joined(&self) -> bool {
        self.owner_outcome().is_some()
    }

    /// Polls the original retained JoinHandle, never an abort request.
    #[doc(hidden)]
    pub fn owner_outcome(&self) -> Option<TracingShutdownOwnerOutcome> {
        self.receipt
            .get()?
            .clone()
            .now_or_never()
            .map(|result| match result {
                TracingShutdownTerminal::Report(_) => TracingShutdownOwnerOutcome::Returned,
                TracingShutdownTerminal::Panicked(_) => TracingShutdownOwnerOutcome::Panicked,
                TracingShutdownTerminal::Interrupted(_) => TracingShutdownOwnerOutcome::Cancelled,
            })
    }

    /// Process-owned worker joins, including the actual file thread and any
    /// started provider blocking task. Failure is distinct from outstanding.
    #[doc(hidden)]
    pub fn workers(&self) -> TracingWorkerSnapshot {
        crate::runtime::tasks::snapshot()
    }

    /// Retain and reconcile the same joins under the caller's final cutoff.
    /// Late completion never changes the original shutdown report.
    #[doc(hidden)]
    pub async fn reconcile_before(&self, deadline: tokio::time::Instant) -> bool {
        let Some(receipt) = self.receipt.get() else {
            return false;
        };
        let joined = crate::runtime::tasks::observe_before(deadline, receipt.clone())
            .await
            .is_some();
        joined && crate::runtime::reconcile_shutdown_before(deadline).await
    }
}

type TracingShutdownReceipt = Shared<BoxFuture<'static, TracingShutdownTerminal>>;

#[derive(Clone)]
enum TracingShutdownTerminal {
    Report(Box<TraceShutdownReport>),
    Panicked(String),
    Interrupted(String),
}

impl TracingShutdownTerminal {
    fn result(self) -> Result<(), ShutdownError> {
        match self {
            Self::Report(report) if report.is_success() => Ok(()),
            Self::Report(report) => Err(ShutdownError::Component(format!(
                "tracing shutdown incomplete: {report:?}"
            ))),
            Self::Panicked(message) => Err(ShutdownError::ActionPanicked {
                name: "tracing-runtime".to_owned(),
                message,
            }),
            Self::Interrupted(error) => Err(ShutdownError::Component(format!(
                "tracing shutdown owner task was interrupted: {error}"
            ))),
        }
    }
}

fn spawn_shutdown_owner<F>(shutdown: F, evidence: TracingShutdownEvidence) -> TracingShutdownReceipt
where
    F: Future<Output = TraceShutdownReport> + Send + 'static,
{
    // Record the outcome in the worker itself. There is no receipt-driver task
    // to detach when an outer coordinator stops waiting.
    let report_slot = evidence.report.clone();
    let (published, registered) = tokio::sync::oneshot::channel();
    let owner = crate::spawn(async move {
        registered.await.expect("tracing receipt publication");
        let report = shutdown.await;
        *report_slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(report.clone());
        report
    });
    let receipt = async move {
        match owner.await {
            Ok(report) => TracingShutdownTerminal::Report(Box::new(report)),
            Err(error) if error.is_panic() => TracingShutdownTerminal::Panicked(error.to_string()),
            Err(error) => TracingShutdownTerminal::Interrupted(error.to_string()),
        }
    }
    .boxed()
    .shared();

    assert!(
        evidence.receipt.set(receipt.clone()).is_ok(),
        "one tracing owner receipt"
    );
    let _ = published.send(());
    receipt
}

/// One-shot owner for the configured tracing runtime, including OTLP workers
/// or the bounded JSONL file worker.
pub struct TracingShutdownHandle {
    owner: Option<TracingRuntimeOwner>,
    receipt: Option<TracingShutdownReceipt>,
    timeout: Duration,
    deadline: tokio::time::Instant,
    evidence: TracingShutdownEvidence,
}

impl TracingShutdownHandle {
    /// Wraps an owner for the framework shutdown coordinator and returns a
    /// cloneable evidence handle for observing the terminal report.
    pub fn new(owner: TracingRuntimeOwner, timeout: Duration) -> (Self, TracingShutdownEvidence) {
        let started = tokio::time::Instant::now();
        Self::before(
            owner,
            timeout,
            started.checked_add(timeout).unwrap_or(started),
        )
    }

    /// Wrap using an absolute composition-root shutdown deadline.
    ///
    /// Framework adapters use this seam so a retained tracing shutdown task
    /// cannot start a fresh relative budget after earlier lifecycle phases.
    #[doc(hidden)]
    pub fn before(
        owner: TracingRuntimeOwner,
        timeout: Duration,
        deadline: tokio::time::Instant,
    ) -> (Self, TracingShutdownEvidence) {
        let evidence = TracingShutdownEvidence::default();
        (
            Self {
                owner: Some(owner),
                receipt: None,
                timeout,
                deadline,
                evidence: evidence.clone(),
            },
            evidence,
        )
    }

    fn shutdown_receipt(&mut self) -> TracingShutdownReceipt {
        if let Some(receipt) = &self.receipt {
            return receipt.clone();
        }

        let owner = self
            .owner
            .take()
            .expect("tracing shutdown owner and receipt invariant violated");
        let timeout =
            tracing_shutdown_budget(self.timeout, self.deadline, tokio::time::Instant::now());
        let deadline = tokio::time::Instant::now() + timeout;
        let receipt = spawn_shutdown_owner(
            owner.shutdown_before(deadline.min(self.deadline)),
            self.evidence.clone(),
        );
        self.receipt = Some(receipt.clone());
        receipt
    }
}

fn tracing_shutdown_budget(
    configured_timeout: Duration,
    deadline: tokio::time::Instant,
    now: tokio::time::Instant,
) -> Duration {
    configured_timeout.min(deadline.saturating_duration_since(now))
}

#[async_trait]
impl FrameworkShutdownComponent for TracingShutdownHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.shutdown_receipt().await.result()
    }

    fn name(&self) -> &str {
        "tracing-runtime"
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::FlushTelemetry
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
        let receipt = self.shutdown_receipt();
        Some(Box::pin(async move { receipt.await.result() }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;

    use futures_util::future::poll_fn;
    use tokio::sync::oneshot;

    use super::*;
    use crate::{ExportTaskShutdownStatus, FileExportShutdownStatus, SpanExportTaskShutdownStatus};

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn shutdown_report(provider_timed_out: bool) -> TraceShutdownReport {
        TraceShutdownReport {
            was_initialized: true,
            already_shutdown: false,
            log_export: ExportTaskShutdownStatus::NotConfigured,
            log_metrics: None,
            span_export: SpanExportTaskShutdownStatus::NotConfigured,
            span_metrics: None,
            file_export: FileExportShutdownStatus::NotConfigured,
            tracer_flush_errors: Vec::new(),
            meter_flush_error: None,
            meter_shutdown_error: None,
            provider_timed_out,
        }
    }

    fn handle_with_receipt(
        receipt: TracingShutdownReceipt,
        evidence: TracingShutdownEvidence,
    ) -> TracingShutdownHandle {
        TracingShutdownHandle {
            owner: None,
            receipt: Some(receipt),
            timeout: Duration::from_secs(1),
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            evidence,
        }
    }

    #[test]
    fn tracing_owner_uses_only_the_remaining_composition_root_budget() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(10);

        assert_eq!(
            tracing_shutdown_budget(Duration::from_secs(10), deadline, started),
            Duration::from_secs(10)
        );
        assert_eq!(
            tracing_shutdown_budget(
                Duration::from_secs(10),
                deadline,
                started + Duration::from_secs(7),
            ),
            Duration::from_secs(3)
        );
        assert_eq!(
            tracing_shutdown_budget(
                Duration::from_secs(10),
                deadline,
                started + Duration::from_secs(11),
            ),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_cancel_owner_and_force_replays_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let evidence = TracingShutdownEvidence::default();
        let owner_calls = Arc::clone(&calls);
        let owner_dropped = Arc::clone(&dropped);
        let receipt = spawn_shutdown_owner(
            async move {
                owner_calls.fetch_add(1, Ordering::SeqCst);
                let _drop_probe = DropProbe(owner_dropped);
                started_tx.send(()).expect("owner start receiver");
                release_rx.await.expect("owner release sender");
                shutdown_report(false)
            },
            evidence.clone(),
        );
        let mut handle = handle_with_receipt(receipt, evidence.clone());
        started_rx.await.expect("owned shutdown must start");

        let mut graceful = Box::pin(handle.shutdown());
        poll_fn(|context| {
            assert!(graceful.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(graceful);

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!dropped.load(Ordering::SeqCst));
        let force = handle
            .force_shutdown()
            .expect("tracing must expose force reconciliation");
        release_tx
            .send(())
            .expect("owned shutdown release receiver");
        force.await.expect("force waiter must replay owner success");
        assert!(dropped.load(Ordering::SeqCst));
        assert!(evidence.report().expect("terminal evidence").is_success());
        handle
            .shutdown()
            .await
            .expect("later graceful waiter must replay success");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn incomplete_report_is_recorded_and_replayed_to_force_waiter() {
        let expected = shutdown_report(true);
        let evidence = TracingShutdownEvidence::default();
        let receipt = spawn_shutdown_owner(
            {
                let expected = expected.clone();
                async move { expected }
            },
            evidence.clone(),
        );
        let mut handle = handle_with_receipt(receipt, evidence.clone());

        let graceful = handle.shutdown().await.expect_err("incomplete report");
        let force = handle
            .force_shutdown()
            .expect("force reconciliation")
            .await
            .expect_err("force must not turn an incomplete report into success");

        assert_eq!(graceful.to_string(), force.to_string());
        assert_eq!(evidence.report(), Some(expected));
    }

    #[tokio::test]
    async fn evidence_alone_retains_the_join_after_every_component_waiter_is_dropped() {
        let evidence = TracingShutdownEvidence::default();
        assert!(!evidence.owner_registered());
        let (release, wait) = oneshot::channel();
        let receipt = spawn_shutdown_owner(
            async move {
                wait.await.unwrap();
                shutdown_report(true)
            },
            evidence.clone(),
        );
        let handle = handle_with_receipt(receipt, evidence.clone());
        assert!(evidence.owner_registered());
        drop(handle);
        assert!(!evidence.owner_joined());
        assert!(evidence.report().is_none());
        assert!(!evidence.reconcile_before(tokio::time::Instant::now()).await);
        release.send(()).unwrap();
        // No driver task is necessary to materialize a report. Reconciliation
        // still has to consume the actual owner's join separately.
        while evidence.report().is_none() {
            tokio::task::yield_now().await;
        }
        assert!(evidence.owner_joined());
        assert_eq!(
            evidence.owner_outcome(),
            Some(TracingShutdownOwnerOutcome::Returned)
        );
        assert!(!evidence.report().unwrap().is_success());
        evidence
            .receipt
            .get()
            .unwrap()
            .clone()
            .await
            .result()
            .unwrap_err();
        assert!(!evidence.report().unwrap().is_success());
    }

    #[tokio::test]
    async fn owner_panic_is_replayed_without_false_success() {
        let evidence = TracingShutdownEvidence::default();
        let receipt = spawn_shutdown_owner(
            async move {
                panic!("controlled tracing shutdown panic");
                #[allow(unreachable_code)]
                shutdown_report(false)
            },
            evidence.clone(),
        );
        let mut handle = handle_with_receipt(receipt, evidence.clone());

        let graceful = handle.shutdown().await.expect_err("owner panic");
        let force = handle
            .force_shutdown()
            .expect("force reconciliation")
            .await
            .expect_err("panic must be replayed");

        assert!(matches!(graceful, ShutdownError::ActionPanicked { .. }));
        assert!(matches!(force, ShutdownError::ActionPanicked { .. }));
        assert!(evidence.report().is_none());
        assert_eq!(
            evidence.owner_outcome(),
            Some(TracingShutdownOwnerOutcome::Panicked)
        );
    }
}
