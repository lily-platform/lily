//! Dependency barriers and retained receipts shared by normal and fallback shutdown.

use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use lily_error::{
    application::consumer::{ConsumerError, ConsumerSignalFailureStage},
    injection::InjectionError,
};
use lily_injection::{ApplicationContainer, ContainerShutdownReport};
use lily_queue::__private::{QueueRuntimeHandle, QueueShutdownDeadlines};
use lily_shutdown::{FrameworkShutdownComponent, ShutdownError, SignalMonitor};
use lily_trace::{
    lifecycle::{TracingShutdownEvidence, TracingShutdownHandle},
    TracingRuntimeOwner,
};
use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::time::Instant;

use super::owned_tasks::OwnedTask;

type DiReceipt = Shared<BoxFuture<'static, Result<ContainerShutdownReport, InjectionError>>>;

pub(super) struct ConsumerDependencies {
    provider: Option<QueueRuntimeHandle>,
    container: Option<Arc<ApplicationContainer>>,
    di: OnceLock<DiReceipt>,
    tracing_owner: tokio::sync::Mutex<Option<TracingRuntimeOwner>>,
    tracing_handle: tokio::sync::Mutex<Option<TracingShutdownHandle>>,
    trace_evidence: OnceLock<TracingShutdownEvidence>,
    owns_trace: bool,
    owns_signal: bool,
    signal_monitor: Mutex<Option<SignalMonitor>>,
    signal: OnceLock<OwnedTask<Result<(), ConsumerError>>>,
    deadlines: QueueShutdownDeadlines,
    di_deadline: Instant,
    trace_deadline: Instant,
    retained: Mutex<Option<Arc<Self>>>,
}

impl ConsumerDependencies {
    pub(super) fn new(
        provider: Option<QueueRuntimeHandle>,
        container: Option<Arc<ApplicationContainer>>,
        tracing_owner: Option<TracingRuntimeOwner>,
        signal_monitor: Option<SignalMonitor>,
        deadlines: QueueShutdownDeadlines,
    ) -> Arc<Self> {
        let remaining = deadlines.hard().saturating_duration_since(Instant::now());
        // Forced delivery owners may use three quarters of the force reserve.
        // Reserve DI/telemetry from its remaining quarter, not from the total
        // duration, or a one-second root would expire DI before force finishes.
        let dependency_tail = (deadlines
            .hard()
            .saturating_duration_since(deadlines.graceful())
            / 4)
        .min(remaining);
        let owner = Arc::new(Self {
            provider,
            container,
            di: OnceLock::new(),
            owns_trace: tracing_owner.is_some(),
            owns_signal: signal_monitor.is_some(),
            tracing_owner: tokio::sync::Mutex::new(tracing_owner),
            tracing_handle: tokio::sync::Mutex::new(None),
            trace_evidence: OnceLock::new(),
            signal_monitor: Mutex::new(signal_monitor),
            signal: OnceLock::new(),
            deadlines,
            di_deadline: deadlines.hard() - (dependency_tail / 2).min(Duration::from_millis(250)),
            trace_deadline: deadlines.hard() - (dependency_tail / 4).min(Duration::from_millis(50)),
            retained: Mutex::new(None),
        });
        *owner.retained.lock().unwrap_or_else(|p| p.into_inner()) = Some(owner.clone());
        owner
    }

    fn queue_terminal(&self) -> bool {
        self.provider.as_ref().is_none_or(|p| p.close_reconciled())
    }
    fn di_terminal(&self) -> bool {
        self.container
            .as_ref()
            .is_none_or(|c| lily_injection::__private::container_shutdown_quiescent(c))
    }

    pub(super) async fn close_di(&self) -> Result<(), ShutdownError> {
        let Some(container) = &self.container else {
            return Ok(());
        };
        if !self.queue_terminal() {
            return Err(failure(
                "Consumer DI blocked by unconfirmed queue/connection termination",
            ));
        }
        if self.di.get().is_none() && Instant::now() >= self.di_deadline {
            return Err(failure(
                "Consumer DI not started: dependency deadline elapsed",
            ));
        }
        let receipt = self.di.get_or_init(|| {
            let container = container.clone();
            let deadline = self.di_deadline;
            async move { container.close_before(deadline).await }
                .boxed()
                .shared()
        });
        observe_before(self.di_deadline, receipt.clone())
            .await
            .ok_or_else(|| {
                failure("Consumer DI observation incomplete; original receipt retained")
            })?
            .map_err(|_| failure("Consumer DI disposal failed"))?;
        if !self.di_terminal() {
            return Err(failure(
                "Consumer DI owner returned with outstanding cleanup",
            ));
        }
        Ok(())
    }

    pub(super) fn di_error(&self) -> Option<InjectionError> {
        self.di.get()?.peek()?.as_ref().err().cloned()
    }

    pub(super) fn report(&self) -> crate::ConsumerDependencyReport {
        use crate::ConsumerResourceReport;
        let di_owned = self.container.is_some();
        let di_terminal = di_owned && self.di_terminal();
        let di = ConsumerResourceReport {
            owned: di_owned,
            started: self.di.get().is_some(),
            terminal: di_terminal,
            succeeded: di_terminal
                && self
                    .di
                    .get()
                    .and_then(|r| r.peek())
                    .is_some_and(Result::is_ok),
        };
        let trace = self.trace_evidence();
        let telemetry = ConsumerResourceReport {
            owned: self.owns_trace,
            started: trace.is_some_and(TracingShutdownEvidence::owner_registered),
            terminal: self.owns_trace
                && trace.is_some_and(|e| e.owner_joined() && e.workers().is_terminal()),
            succeeded: self.owns_trace && self.tracing_succeeded(),
        };
        let owns_signal = self.owns_signal || self.signal.get().is_some();
        let signal_terminal = owns_signal && self.signal_terminal();
        let signal_monitor = ConsumerResourceReport {
            owned: owns_signal,
            started: self.signal.get().is_some(),
            terminal: signal_terminal,
            succeeded: signal_terminal && self.signal_error().is_none(),
        };
        crate::ConsumerDependencyReport {
            di,
            telemetry,
            signal_monitor,
        }
    }

    pub(super) fn trace_evidence(&self) -> Option<&TracingShutdownEvidence> {
        self.trace_evidence.get()
    }

    pub(super) fn tracing_succeeded(&self) -> bool {
        !self.owns_trace
            || self.trace_evidence().is_some_and(|evidence| {
                evidence.owner_joined()
                    && evidence.report().is_some_and(|report| report.is_success())
                    && evidence.workers().is_terminal()
                    && evidence.workers().failed == 0
            })
    }

    pub(super) fn signal_error(&self) -> Option<ConsumerError> {
        match self.signal.get()?.clone().now_or_never()? {
            Ok(Err(error)) => Some(error),
            Err(error) if !error.is_cancelled() => Some(ConsumerError::signal(
                ConsumerSignalFailureStage::Cleanup,
                error,
            )),
            _ => None,
        }
    }

    /// A failed signal task is still terminal once its original join is
    /// observed. Keep that failure in `signal_error`, independently of this
    /// dependency barrier.
    fn signal_terminal(&self) -> bool {
        self.signal_monitor
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_none()
            && self
                .signal
                .get()
                .is_none_or(|task| task.clone().now_or_never().is_some())
    }

    async fn stop_signal(&self) -> Result<(), ShutdownError> {
        if self.signal.get().is_none() {
            let mut monitor = self
                .signal_monitor
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(task) = monitor.take().and_then(SignalMonitor::into_stop_task) {
                let task = OwnedTask::map(task, |result| {
                    result
                        .map_err(|e| ConsumerError::signal(ConsumerSignalFailureStage::Cleanup, e))
                });
                let _ = self.signal.set(task);
            }
        }
        if let Some(task) = self.signal.get() {
            match observe_before(self.deadlines.hard(), task.clone()).await {
                Some(Ok(Ok(()))) => {}
                // Cancellation is expected after into_stop_task; the real join
                // is still required before this is terminal.
                Some(Err(error)) if error.is_cancelled() => {}
                _ => {
                    return Err(failure(
                        "Consumer signal monitor termination unconfirmed or failed",
                    ))
                }
            }
        }
        Ok(())
    }

    pub(super) async fn close_trace(&self) -> Result<(), ShutdownError> {
        let signal_result = self.stop_signal().await;
        if !self.signal_terminal() {
            signal_result?;
            return Err(failure("Consumer signal monitor termination unconfirmed"));
        }
        if let Some(receipt) = self.di.get() {
            let _ = observe_before(self.trace_deadline, receipt.clone()).await;
        }
        if !self.owns_trace {
            return Ok(());
        }
        if !self.queue_terminal() || !self.di_terminal() {
            return Err(failure(
                "Consumer telemetry blocked by outstanding framework/dependency users",
            ));
        }
        let mut handle = self.tracing_handle.lock().await;
        if handle.is_none() {
            if Instant::now() >= self.trace_deadline {
                return Err(failure("Consumer telemetry not started: deadline elapsed"));
            }
            let owner = self
                .tracing_owner
                .lock()
                .await
                .take()
                .expect("retained tracing owner");
            let (adapter, evidence) = TracingShutdownHandle::before(
                owner,
                self.trace_deadline
                    .saturating_duration_since(Instant::now()),
                self.trace_deadline,
            );
            let _ = self.trace_evidence.set(evidence);
            *handle = Some(adapter);
        }
        observe_before(self.trace_deadline, handle.as_mut().unwrap().shutdown())
            .await
            .ok_or_else(|| failure("Consumer telemetry observation incomplete; owner retained"))?
    }

    pub(super) async fn reconcile(self: &Arc<Self>) -> bool {
        let _ = self.stop_signal().await;
        let signal_terminal = self.signal_terminal();
        if let Some(receipt) = self.di.get() {
            let _ = observe_before(self.deadlines.hard(), receipt.clone()).await;
        }
        let trace_terminal = if self.owns_trace {
            match self.trace_evidence.get() {
                Some(evidence) => evidence.reconcile_before(self.deadlines.hard()).await,
                None => false,
            }
        } else {
            true
        };
        let terminal =
            self.queue_terminal() && self.di_terminal() && signal_terminal && trace_terminal;
        if terminal {
            self.retained
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
        }
        terminal
    }
}

pub(super) async fn observe_before<F: std::future::Future>(
    deadline: Instant,
    future: F,
) -> Option<F::Output> {
    // These are existing receipts: observing an already ready join at the
    // deadline is allowed; this helper must never start new resource I/O.
    tokio::time::timeout_at(deadline, future).await.ok()
}

fn failure(message: &str) -> ShutdownError {
    ShutdownError::Component(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn joined_signal_failure_releases_barriers_without_erasing_failure() {
        let root = QueueShutdownDeadlines::starting_at(Instant::now(), Duration::from_secs(1));
        let owner = ConsumerDependencies::new(None, None, None, None, root);
        let task = OwnedTask::from(tokio::spawn(async {
            Err(ConsumerError::signal(
                ConsumerSignalFailureStage::Cleanup,
                std::io::Error::other("signal monitor read failed"),
            ))
        }));
        assert!(task.clone().await.unwrap().is_err());
        assert!(owner.signal.set(task).is_ok());

        assert!(owner.stop_signal().await.is_err());
        assert!(owner.signal_terminal());
        owner.close_trace().await.unwrap();
        assert!(owner.reconcile().await);
        assert!(owner.retained.lock().unwrap().is_none());
        let failure = owner
            .signal_error()
            .expect("joined failure must survive reconciliation");
        assert_eq!(failure.error_code(), "CONSUMER_SIGNAL_CLEANUP");
        assert_eq!(
            std::error::Error::source(&failure).unwrap().to_string(),
            "signal monitor read failed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn joined_signal_panic_is_terminal_and_remains_a_primary_error() {
        let root = QueueShutdownDeadlines::starting_at(Instant::now(), Duration::from_secs(1));
        let owner = ConsumerDependencies::new(None, None, None, None, root);
        let task: OwnedTask<Result<(), ConsumerError>> = OwnedTask::from(tokio::spawn(async {
            panic!("signal monitor qualification panic");
        }));
        assert!(task.clone().await.unwrap_err().is_panic());
        assert!(owner.signal.set(task).is_ok());

        assert!(owner.stop_signal().await.is_err());
        assert!(owner.signal_terminal());
        owner.close_trace().await.unwrap();
        assert!(owner.reconcile().await);
        assert!(owner.retained.lock().unwrap().is_none());
        assert_eq!(
            owner
                .signal_error()
                .expect("panic remains reportable")
                .error_code(),
            "CONSUMER_SIGNAL_CLEANUP"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pending_signal_and_abort_request_cannot_release_dependency_retention() {
        use tokio_util::sync::CancellationToken;

        let started = Instant::now();
        let root = QueueShutdownDeadlines::starting_at(started, Duration::from_millis(100));
        let owner = ConsumerDependencies::new(None, None, None, None, root);
        let entered = CancellationToken::new();
        let terminated = CancellationToken::new();
        let task_entered = entered.clone();
        let task_terminated = terminated.clone();
        let task: OwnedTask<Result<(), ConsumerError>> =
            OwnedTask::from(tokio::spawn(async move {
                let _termination = task_terminated.drop_guard();
                task_entered.cancel();
                std::future::pending().await
            }));
        assert!(owner.signal.set(task.clone()).is_ok());
        entered.cancelled().await;

        assert!(!owner.signal_terminal());
        assert!(owner.close_trace().await.is_err());
        assert_eq!(Instant::now(), root.hard());
        assert!(!owner.reconcile().await);
        assert!(owner.retained.lock().unwrap().is_some());
        assert!(!terminated.is_cancelled());
        task.abort_handle().abort();
        assert!(
            !owner.signal_terminal(),
            "abort request is not the original join"
        );
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(terminated.is_cancelled());
        assert!(owner.signal_terminal());
        assert!(owner.reconcile().await);
        assert!(owner.retained.lock().unwrap().is_none());
        assert!(
            owner.signal_error().is_none(),
            "requested cancellation is expected"
        );
        assert_eq!(
            Instant::now(),
            root.hard(),
            "late join observation cannot renew the budget"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dependency_reserve_follows_the_forced_owner_cutoff_even_for_short_roots() {
        let started = Instant::now();
        let root = QueueShutdownDeadlines::starting_at(started, Duration::from_secs(1));
        let owner = ConsumerDependencies::new(None, None, None, None, root);
        let forced_owner_cutoff = root.graceful() + (root.hard() - root.graceful()) * 3 / 4;
        assert!(forced_owner_cutoff < owner.di_deadline);
        assert!(owner.di_deadline < owner.trace_deadline);
        assert!(owner.trace_deadline < root.hard());
        assert!(owner.reconcile().await);
    }

    #[test]
    fn telemetry_requires_queue_barrier_and_actual_owner_and_worker_joins() {
        const CASE: &str = "LILY_CONSUMER_STAGE5_TRACE_CHILD";
        if std::env::var_os(CASE).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "consumer::dependencies::tests::telemetry_requires_queue_barrier_and_actual_owner_and_worker_joins", "--nocapture", "--test-threads=1"])
                .env(CASE, "1").output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("STAGE5_TRACE_RECEIPTS_CONFIRMED")
            );
            assert!(!String::from_utf8_lossy(&output.stderr)
                .contains("tracing runtime owner dropped without awaited shutdown"));
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let config = lily_trace::TraceConfig {
                enabled: true,
                service_name: "consumer-stage5-joins".into(),
                ..Default::default()
            };
            let lily_trace::TraceInstallOutcome::Owned(tracing) =
                TracingRuntimeOwner::install(&config).unwrap()
            else {
                panic!("owned trace required");
            };
            let (queue, _) = lily_queue::__private::queue_service_test_seed(Arc::new(
                lily_config::ConfigService::development("/tmp/consumer-stage5-trace.toml"),
            ));
            let provider = lily_queue::__private::queue_runtime(&queue).unwrap();
            let now = Instant::now();
            let root = QueueShutdownDeadlines::starting_at(now, Duration::from_secs(3));
            let owner =
                ConsumerDependencies::new(Some(provider.clone()), None, Some(tracing), None, root);
            let failed_signal = OwnedTask::from(tokio::spawn(async {
                Err(ConsumerError::signal(
                    ConsumerSignalFailureStage::Cleanup,
                    std::io::Error::other("terminal signal failure before telemetry"),
                ))
            }));
            assert!(failed_signal.clone().await.unwrap().is_err());
            assert!(owner.signal.set(failed_signal).is_ok());
            assert!(owner.close_trace().await.is_err());
            assert!(
                owner.trace_evidence().is_none(),
                "failed prerequisite must not invoke telemetry"
            );
            let mut coordinator = lily_shutdown::FrameworkShutdownCoordinator::before(
                Arc::new(lily_shutdown::ShutdownState::new()),
                Duration::from_secs(3),
                root.graceful(),
                root.hard(),
            );
            lily_queue::__private::register_queue_lifecycle(
                &mut coordinator,
                provider.clone(),
                Duration::from_secs(3),
            );
            assert!(coordinator
                .execute_report(lily_shutdown::ShutdownSignal::Manual)
                .await
                .is_terminal_complete());
            assert!(provider.close_reconciled());
            owner.close_trace().await.unwrap();
            assert!(owner.reconcile().await);
            let evidence = owner.trace_evidence().unwrap();
            assert!(evidence.owner_joined());
            assert!(evidence.workers().is_terminal());
            assert!(evidence.report().unwrap().is_success());
            assert!(owner.retained.lock().unwrap().is_none());
            assert_eq!(
                owner
                    .signal_error()
                    .expect("signal failure is still primary")
                    .error_code(),
                "CONSUMER_SIGNAL_CLEANUP"
            );
            println!("STAGE5_TRACE_RECEIPTS_CONFIRMED");
        });
    }
}
