//! One dependency obligation and its actual receipts across every root path.

use super::*;
use crate::shutdown_report::{
    DependencyDisposition, DependencyInventory, DependencySnapshot, TelemetrySnapshot,
};
use lily_trace::lifecycle::{TracingShutdownEvidence, TracingShutdownHandle};
use std::sync::Mutex;

pub(super) struct HttpDependencies {
    pub(super) background: OnceLock<Arc<lily_background_service::BackgroundServiceRuntime>>,
    pub(super) background_force_bridge: OnceLock<TaskReceipt<io::Result<()>>>,
    container: Option<Arc<ApplicationContainer>>,
    owns_container: bool,
    owns_tracing: bool,
    di_wait_failed: AtomicBool,
    trace_wait_failed: AtomicBool,
    trace_terminal: AtomicBool,
    di: OnceLock<
        TaskReceipt<
            Result<lily_injection::ContainerShutdownReport, lily_injection::InjectionError>,
        >,
    >,
    tasks: TaskRegistry,
    tracing_owner: tokio::sync::Mutex<Option<TracingRuntimeOwner>>,
    tracing: tokio::sync::Mutex<Option<TracingShutdownHandle>>,
    pub(super) trace_evidence: OnceLock<TracingShutdownEvidence>,
    // If a prerequisite or deadline prevents disposal, dropping every App
    // waiter must not drop the owner and start an unrelated fallback timeout.
    retained: Mutex<Option<Arc<Self>>>,
}

impl HttpDependencies {
    pub(super) fn new(
        container: Arc<ApplicationContainer>,
        owns_container: bool,
        tracing_owner: Option<TracingRuntimeOwner>,
    ) -> Arc<Self> {
        Self::with_container(Some(container), owns_container, tracing_owner)
    }

    pub(super) fn with_container(
        container: Option<Arc<ApplicationContainer>>,
        owns_container: bool,
        tracing_owner: Option<TracingRuntimeOwner>,
    ) -> Arc<Self> {
        Arc::new(Self {
            background: OnceLock::new(),
            background_force_bridge: OnceLock::new(),
            container,
            owns_container,
            owns_tracing: tracing_owner.is_some(),
            di_wait_failed: AtomicBool::new(false),
            trace_wait_failed: AtomicBool::new(false),
            trace_terminal: AtomicBool::new(false),
            di: OnceLock::new(),
            tasks: TaskRegistry::default(),
            tracing_owner: tokio::sync::Mutex::new(tracing_owner),
            tracing: tokio::sync::Mutex::new(None),
            trace_evidence: OnceLock::new(),
            retained: Mutex::new(None),
        })
    }

    pub(super) fn retain(self: &Arc<Self>) {
        self.retained
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert_with(|| self.clone());
    }

    pub(super) fn attach_background(
        &self,
        background: Arc<lily_background_service::BackgroundServiceRuntime>,
    ) {
        assert!(self.background.set(background).is_ok());
    }

    pub(super) fn start_background(
        &self,
    ) -> Result<(), lily_background_service::BackgroundServiceError> {
        self.background
            .get()
            .map_or(Ok(()), |runtime| runtime.start())
    }

    pub(super) async fn wait_for_background_failure(&self) {
        match self.background.get() {
            Some(runtime) => runtime.wait_for_failure().await,
            None => futures::future::pending().await,
        }
    }

    pub(super) fn begin_background_shutdown(&self, budget: &ShutdownBudget) {
        if let Some(runtime) = self.background.get() {
            let deadlines = budget.begin();
            runtime.begin_shutdown(lily_background_service::BackgroundShutdownDeadlines {
                cooperative: deadlines.at(ShutdownStage::Cooperative),
                execution_stop: deadlines.at(ShutdownStage::ExecutionStop),
                cleanup: deadlines.at(ShutdownStage::Cleanup),
                reconcile: deadlines.at(ShutdownStage::Reconcile),
            });
            if budget.force_requested() {
                runtime.force_stop();
            }
        }
    }

    fn background_snapshot(&self) -> lily_background_service::BackgroundServiceSnapshot {
        self.background.get().map_or_else(
            lily_background_service::BackgroundServiceSnapshot::empty,
            |runtime| runtime.snapshot(),
        )
    }

    pub(super) async fn close_background(
        self: &Arc<Self>,
        budget: &ShutdownBudget,
    ) -> Result<(), ShutdownError> {
        self.retain();
        self.begin_background_shutdown(budget);
        if let Some(runtime) = self.background.get() {
            if !runtime
                .wait_stopped_before(budget.begin().at(ShutdownStage::Reconcile))
                .await
            {
                return Err(failure(
                    "HTTP background tasks/scopes outstanding; owners retained",
                ));
            }
            if !runtime.snapshot().cleanup_succeeded() {
                return Err(failure(
                    "HTTP background scope cleanup or task reconciliation failed",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn close_di(
        self: &Arc<Self>,
        budget: &ShutdownBudget,
    ) -> Result<(), ShutdownError> {
        self.retain();
        let _ = self.close_background(budget).await;
        if !self.background_snapshot().is_terminal() {
            return Err(failure("HTTP DI blocked by outstanding background users"));
        }
        if !self.owns_container {
            return Ok(());
        }
        let deadline = budget.begin().at(ShutdownStage::Dependencies);
        if self.di.get().is_none() && tokio::time::Instant::now() >= deadline {
            return Err(failure(
                "HTTP owned DI not started: dependency deadline elapsed",
            ));
        }
        let receipt = self
            .di
            .get_or_init(|| {
                let container = self.container.as_ref().expect("owned DI container").clone();
                self.tasks
                    .spawn(async move { container.close_before(deadline).await })
            })
            .clone();
        match budget
            .wait_for_receipt(ShutdownStage::Dependencies, receipt)
            .await
        {
            Ok(Ok(result)) => result
                .as_ref()
                .as_ref()
                .map(|_| ())
                .map_err(|e| failure(&e.to_string())),
            Ok(Err(error)) => Err(failure(&format!("HTTP DI owner join failed: {error}"))),
            Err(()) => {
                self.di_wait_failed.store(true, Ordering::Release);
                Err(failure(
                    "HTTP DI shutdown join outstanding; receipt retained",
                ))
            }
        }
    }

    fn di_terminal(&self) -> bool {
        !self.owns_container
            || (self.tasks.snapshot().is_terminal()
                && self.container.as_ref().is_some_and(|container| {
                    lily_injection::__private::container_shutdown_quiescent(container)
                }))
    }

    pub(super) async fn close_tracing(
        self: &Arc<Self>,
        budget: &ShutdownBudget,
    ) -> Result<(), ShutdownError> {
        self.retain();
        // Observe late DI owner completion, without retrying disposal or
        // extending its D cutoff. Telemetry may only use the remaining T.
        if let Some(receipt) = self.di.get() {
            let _ = budget
                .wait_for_receipt(ShutdownStage::Telemetry, receipt.clone())
                .await;
        }
        if !self.di_terminal() || !self.background_snapshot().is_terminal() {
            return Err(failure("HTTP telemetry blocked by outstanding DI users"));
        }
        let mut handle = self.tracing.lock().await;
        if handle.is_none() {
            let mut owner = self.tracing_owner.lock().await;
            if owner.is_none() {
                return Ok(());
            }
            let deadline = budget.begin().at(ShutdownStage::Telemetry);
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(
                    "HTTP telemetry not started: telemetry deadline elapsed",
                ));
            }
            let (adapter, evidence) = TracingShutdownHandle::before(
                owner.take().unwrap(),
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                deadline,
            );
            assert!(self.trace_evidence.set(evidence).is_ok());
            *handle = Some(adapter);
        }
        let receipt = handle.as_mut().unwrap().shutdown();
        // Adapter owns the actual task; this borrowed future is only a waiter.
        match budget
            .wait_for_receipt(ShutdownStage::Telemetry, receipt)
            .await
        {
            Ok(result) => result,
            Err(()) => {
                self.trace_wait_failed.store(true, Ordering::Release);
                Err(failure(
                    "HTTP telemetry shutdown outstanding; receipt retained",
                ))
            }
        }
    }

    pub(super) async fn reconcile(
        self: &Arc<Self>,
        budget: &ShutdownBudget,
    ) -> Result<(), ShutdownError> {
        if let Some(receipt) = self.di.get() {
            let _ = budget
                .wait_for_receipt(ShutdownStage::Final, receipt.clone())
                .await;
        }
        let tracing_terminal = match self.trace_evidence.get() {
            Some(evidence) => {
                evidence
                    .reconcile_before(budget.begin().at(ShutdownStage::Final))
                    .await
            }
            None => self.tracing_owner.lock().await.is_none(),
        };
        if tracing_terminal {
            self.trace_terminal.store(true, Ordering::Release);
        }
        if self.di_terminal() && tracing_terminal && self.background_snapshot().is_terminal() {
            self.retained
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            Ok(())
        } else {
            self.retain();
            Err(failure(
                "HTTP dependency/task termination unconfirmed; owners retained",
            ))
        }
    }

    /// Synchronous receipt observation only. This never starts dependency work
    /// or queries a caller-owned container's unrelated scopes/workers.
    pub(super) fn snapshot(&self) -> DependencyInventory {
        use DependencyDisposition as D;
        let di_disposition = if !self.owns_container {
            D::NotOwned
        } else {
            match self.di.get().map(|receipt| receipt.clone().now_or_never()) {
                None => D::NotStarted,
                Some(None) => D::Outstanding,
                Some(Some(Ok(result))) if result.is_ok() => D::Succeeded,
                Some(Some(Ok(_))) => D::Failed,
                Some(Some(Err(error))) if error.is_cancelled() => D::Cancelled,
                Some(Some(Err(_))) => D::Panicked,
            }
        };
        let evidence = self.trace_evidence.get();
        let trace_report = evidence.and_then(|evidence| evidence.report());
        let trace_owner = evidence.and_then(|evidence| evidence.owner_outcome());
        let trace_owner_joined = trace_owner.is_some();
        let trace_owner_registered = evidence.is_some_and(|evidence| evidence.owner_registered());
        use lily_trace::lifecycle::TracingShutdownOwnerOutcome as TraceJoin;
        let disposition = if !self.owns_tracing {
            D::NotOwned
        } else if let Some(report) = &trace_report {
            if report.is_success() {
                D::Succeeded
            } else {
                D::Failed
            }
        } else if trace_owner == Some(TraceJoin::Panicked) {
            D::Panicked
        } else if trace_owner == Some(TraceJoin::Cancelled) {
            D::Cancelled
        } else if trace_owner_registered {
            D::Outstanding
        } else {
            D::NotStarted
        };
        let workers = evidence
            .map(|evidence| evidence.workers())
            .unwrap_or_default();
        let (dropped, rejected) = trace_report.map_or((0, 0), |report| {
            let log = report.log_metrics;
            let span = report.span_metrics;
            let file = report.file_metrics().map_or(0, |metrics| {
                u64::try_from(metrics.total_dropped()).unwrap_or(u64::MAX)
            });
            (
                log.map_or(0, |m| m.dropped)
                    .saturating_add(span.map_or(0, |m| m.dropped))
                    .saturating_add(file),
                log.map_or(0, |m| m.rejected)
                    .saturating_add(span.map_or(0, |m| m.rejected)),
            )
        });
        DependencyInventory {
            background: self.background_snapshot(),
            di: DependencySnapshot {
                disposition: di_disposition,
                terminal: self.di_terminal(),
                wait_failed: self.di_wait_failed.load(Ordering::Acquire),
                tasks: self.tasks.snapshot(),
            },
            telemetry: TelemetrySnapshot {
                dependency: DependencySnapshot {
                    disposition,
                    terminal: !self.owns_tracing || self.trace_terminal.load(Ordering::Acquire),
                    wait_failed: self.trace_wait_failed.load(Ordering::Acquire),
                    // Adapter joins and runtime worker joins have distinct receipts.
                    tasks: crate::tasks::TaskSnapshot {
                        registered: usize::from(trace_owner_registered),
                        completed: usize::from(trace_owner == Some(TraceJoin::Returned)),
                        cancelled: usize::from(trace_owner == Some(TraceJoin::Cancelled)),
                        panicked: usize::from(trace_owner == Some(TraceJoin::Panicked)),
                        outstanding: usize::from(trace_owner_registered && !trace_owner_joined),
                        ..Default::default()
                    },
                },
                owner_joined: trace_owner_joined,
                workers,
                dropped,
                rejected,
            },
        }
    }
}

fn failure(message: &str) -> ShutdownError {
    ShutdownError::Component(message.to_owned())
}

pub(super) struct HttpDependencyHandle {
    pub(super) lifecycle: Arc<AppLifecycleState>,
    pub(super) phase: FrameworkShutdownPhase,
    pub(super) timeout: Duration,
}

impl HttpDependencyHandle {
    async fn run(&self) -> Result<(), ShutdownError> {
        let lifecycle = &self.lifecycle;
        lifecycle.dependencies.retain();
        if !lifecycle.tasks.transport_is_terminal()
            || !lifecycle.tasks.monitors.snapshot().is_terminal()
            || !lifecycle.requests.snapshot().is_terminal()
        {
            return Err(failure("HTTP dependency users have not terminated"));
        }
        match self.phase {
            FrameworkShutdownPhase::DisposeDependencies => {
                lifecycle.dependencies.close_di(&lifecycle.budget).await
            }
            FrameworkShutdownPhase::FlushTelemetry => {
                lifecycle
                    .observe_report(lifecycle.root_tasks.snapshot(), false)
                    .emit("before_telemetry");
                lifecycle
                    .dependencies
                    .close_tracing(&lifecycle.budget)
                    .await
            }
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for HttpDependencyHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.run().await
    }
    fn name(&self) -> &str {
        match self.phase {
            FrameworkShutdownPhase::DisposeDependencies => "http-di-container",
            _ => "tracing-runtime",
        }
    }
    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }
    fn timeout(&self) -> Duration {
        self.timeout
    }
    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        // Both paths use the SAME retained close. Force never skips DI and
        // never grants an additional timeout or repeats a user disposer.
        Some(Box::pin(self.run()))
    }
    fn force_deadline(&self) -> Option<tokio::time::Instant> {
        let stage = match self.phase {
            FrameworkShutdownPhase::DisposeDependencies => ShutdownStage::Dependencies,
            FrameworkShutdownPhase::FlushTelemetry => ShutdownStage::Telemetry,
            _ => unreachable!(),
        };
        Some(self.lifecycle.budget.begin().at(stage))
    }
}

pub(super) struct HttpReconciliationHandle(pub(super) App);

#[async_trait::async_trait]
impl FrameworkShutdownComponent for HttpReconciliationHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        let transport = self.0.reconcile_transport().await;
        let background = self
            .0
            .lifecycle
            .dependencies
            .close_background(&self.0.lifecycle.budget)
            .await;
        let monitors = self.0.reconcile_monitors().await;
        transport
            .and(monitors)
            .map_err(|e| failure(&e.to_string()))
            .and(background)
    }
    fn name(&self) -> &str {
        "http-resource-reconciliation"
    }
    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DrainInFlight
    }
    fn timeout(&self) -> Duration {
        self.0.shutdown_timeout
    }
    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        Some(Box::pin(self.shutdown()))
    }
    fn force_deadline(&self) -> Option<tokio::time::Instant> {
        Some(self.0.lifecycle.budget.begin().at(ShutdownStage::Reconcile))
    }
}
