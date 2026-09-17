use super::{ApplicationScopeFactory, BackgroundServiceError, Registration};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use lily_cancellation::__private::ExecutionCancellationSource;
use lily_injection::__private::ServiceScopeSnapshot;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, OnceLock},
    time::Instant as Measurement,
};
use tokio::{
    sync::{oneshot, Notify},
    task::{JoinError, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Absolute cutoffs projected from the host's one shutdown budget. They must
/// be ordered: cooperative <= execution_stop <= cleanup <= reconcile.
/// Repeated shutdown calls keep the first set; they never renew the budget.
#[derive(Clone, Copy, Debug)]
pub struct BackgroundShutdownDeadlines {
    pub cooperative: Instant,
    pub execution_stop: Instant,
    pub cleanup: Instant,
    pub reconcile: Instant,
}

/// Actual task joins and exact factory scope generations, not timer success.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackgroundServiceSnapshot {
    pub registered: usize,
    pub spawned: usize,
    pub started: usize,
    pub joined: usize,
    pub completed: usize,
    pub failed: usize,
    pub panicked: usize,
    pub aborted: usize,
    pub stopped_before_start: usize,
    pub abort_requested: usize,
    pub outstanding: usize,
    pub supervisor_joined: bool,
    pub supervisor_failed: bool,
    pub execution_deadline_missed: bool,
    pub scopes: ServiceScopeSnapshot,
}

impl BackgroundServiceSnapshot {
    /// Evidence for a host with no registered background services.
    pub fn empty() -> Self {
        Self {
            supervisor_joined: true,
            scopes: ServiceScopeSnapshot {
                sealed: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    pub fn reconciles(self) -> bool {
        self.started <= self.spawned
            && self.spawned <= self.registered
            && self.spawned == self.joined + self.outstanding
            && self.joined
                == self.completed
                    + self.failed
                    + self.panicked
                    + self.aborted
                    + self.stopped_before_start
            && self.scopes.reconciles()
    }
    pub fn is_terminal(self) -> bool {
        self.reconciles()
            && self.supervisor_joined
            && self.outstanding == 0
            && self.scopes.is_terminal()
    }
    pub fn succeeded(self) -> bool {
        self.cleanup_succeeded() && self.failed == 0 && self.panicked == 0
    }
    /// Resource shutdown can succeed after an application/constructor error.
    /// This lets a failed build report its original failure without falsely
    /// labelling successful resource cleanup a second rollback failure.
    pub fn cleanup_succeeded(self) -> bool {
        self.is_terminal()
            && !self.supervisor_failed
            && !self.execution_deadline_missed
            && self.scopes.failed == 0
    }
}

type Owner = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

struct State {
    registrations: Option<Vec<Registration>>,
    initialized: bool,
    initialization_error: Option<BackgroundServiceError>,
    snapshot: BackgroundServiceSnapshot,
}

/// Retains all constructor/execution joins, even if build/start/close waiters
/// are dropped. It never shuts down its caller-owned DI container.
pub struct BackgroundServiceRuntime {
    scopes: Arc<ApplicationScopeFactory>,
    state: Mutex<State>,
    owner: Mutex<Option<Owner>>,
    initialized: Notify,
    failure: Notify,
    start: CancellationToken,
    stopping: ExecutionCancellationSource,
    force: CancellationToken,
    deadlines: OnceLock<BackgroundShutdownDeadlines>,
}

impl BackgroundServiceRuntime {
    pub(crate) fn new(
        registrations: Vec<Registration>,
        scopes: Arc<ApplicationScopeFactory>,
    ) -> Arc<Self> {
        Arc::new(Self {
            scopes,
            state: Mutex::new(State {
                snapshot: BackgroundServiceSnapshot {
                    registered: registrations.len(),
                    ..Default::default()
                },
                registrations: Some(registrations),
                initialized: false,
                initialization_error: None,
            }),
            owner: Mutex::new(None),
            initialized: Notify::new(),
            failure: Notify::new(),
            start: CancellationToken::new(),
            stopping: ExecutionCancellationSource::default(),
            force: CancellationToken::new(),
            deadlines: OnceLock::new(),
        })
    }

    fn ensure_owner(self: &Arc<Self>) -> Owner {
        let mut owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
        owner
            .get_or_insert_with(|| {
                let registrations = self
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .registrations
                    .take()
                    .unwrap();
                let runtime = Arc::clone(self);
                let handle = tokio::spawn(async move { runtime.supervise(registrations).await });
                async move { handle.await.map_err(Arc::new) }
                    .boxed()
                    .shared()
            })
            .clone()
    }

    /// Construct each worker once, in registration order, without executing it.
    pub async fn initialize(self: &Arc<Self>) -> Result<(), BackgroundServiceError> {
        let owner = self.ensure_owner();
        loop {
            let changed = self.initialized.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(error) = state.initialization_error {
                    return Err(error);
                }
                if self.stopping.view().is_cancelled() {
                    return Err(BackgroundServiceError::Stopping);
                }
                if state.initialized {
                    return Ok(());
                }
            }
            let stopping = self.stopping.view();
            tokio::select! {
                biased;
                _ = stopping.cancelled() => return Err(BackgroundServiceError::Stopping),
                _ = changed => {},
                _ = owner.clone() => return Err(BackgroundServiceError::SupervisorFailed),
            }
        }
    }

    /// Release prepared workers exactly once after the host's successful bind.
    pub fn start(&self) -> Result<(), BackgroundServiceError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.stopping.view().is_cancelled() {
            return Err(BackgroundServiceError::Stopping);
        }
        if let Some(error) = state.initialization_error {
            return Err(error);
        }
        if !state.initialized {
            return Err(BackgroundServiceError::NotInitialized);
        }
        self.start.cancel();
        Ok(())
    }

    /// Cancel the worker token promptly and start retained shutdown ownership.
    pub fn begin_shutdown(self: &Arc<Self>, deadlines: BackgroundShutdownDeadlines) {
        // Same lock as start: shutdown cannot race a fresh start publication.
        {
            let _state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            self.deadlines.get_or_init(|| deadlines);
            self.stopping.cancel();
        }
        drop(self.ensure_owner());
    }

    /// Escalate an already-started shutdown without changing its deadlines.
    pub fn force_stop(&self) {
        if self.stopping.view().is_cancelled() {
            self.scopes.seal();
            self.force.cancel();
        }
    }

    /// Durable failure signal. Hosts must initiate their normal shutdown path.
    pub async fn wait_for_failure(&self) {
        loop {
            let changed = self.failure.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self.snapshot();
            if snapshot.failed != 0 || snapshot.panicked != 0 || snapshot.supervisor_failed {
                return;
            }
            // Poll the real supervisor receipt too, so a supervisor panic is
            // not hidden behind a notification it never got to send.
            let owner = self.owner.lock().unwrap_or_else(|p| p.into_inner()).clone();
            tokio::select! {
                _ = changed => {},
                _ = async { match owner { Some(owner) => { let _ = owner.await; }, None => futures::future::pending().await } } => {
                    let snapshot = self.snapshot();
                    if snapshot.failed == 0 && snapshot.panicked == 0 && !snapshot.supervisor_failed {
                        // Cleanup failures are reported by the shutdown owner.
                        // A joined supervisor with no unhandled worker failure
                        // must not busy-poll this already-ready receipt.
                        futures::future::pending::<()>().await;
                    }
                },
            }
        }
    }

    /// Await actual supervisor termination within the host's original cutoff.
    /// Expiry leaves the supervisor and every remaining join owned, not detached.
    pub async fn wait_stopped_before(self: &Arc<Self>, deadline: Instant) -> bool {
        let owner = self.ensure_owner();
        tokio::select! {
            biased;
            _ = owner => self.snapshot().is_terminal(),
            _ = tokio::time::sleep_until(deadline) => false,
        }
    }

    pub fn snapshot(&self) -> BackgroundServiceSnapshot {
        let joined = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|owner| owner.clone().now_or_never());
        let mut snapshot = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot;
        snapshot.supervisor_joined = joined.is_some();
        snapshot.supervisor_failed = joined.is_some_and(|result| result.is_err());
        snapshot.outstanding = snapshot.spawned - snapshot.joined;
        snapshot.scopes = self.scopes.snapshot();
        snapshot
    }

    async fn supervise(self: Arc<Self>, registrations: Vec<Registration>) {
        let mut tasks = JoinSet::new();
        let stopping = self.stopping.view();
        for registration in registrations {
            if stopping.is_cancelled() {
                break;
            }
            let (ready_tx, ready_rx) = oneshot::channel();
            let runtime = Arc::clone(&self);
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .snapshot
                .spawned += 1;
            tasks.spawn(runtime.run_worker(registration, ready_tx));
            let initialized = tokio::select! {
                biased;
                _ = stopping.cancelled() => break,
                result = ready_rx => result.unwrap_or(Err(BackgroundServiceError::SupervisorFailed)),
            };
            if let Err(error) = initialized {
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .initialization_error = Some(error);
                break;
            }
        }
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .initialized = true;
        self.initialized.notify_waiters();

        // Even after every one-shot worker returns, keep the scope owner until
        // host shutdown. Normal completion must not stop a healthy host.
        loop {
            tokio::select! {
                biased;
                _ = stopping.cancelled() => break,
                result = tasks.join_next(), if !tasks.is_empty() => self.record_join(result.unwrap()),
            }
        }
        let deadlines = *self
            .deadlines
            .get()
            .expect("stopping has absolute host deadlines");
        while !tasks.is_empty() {
            tokio::select! {
                biased;
                result = tasks.join_next() => self.record_join(result.unwrap()),
                _ = self.force.cancelled() => break,
                _ = tokio::time::sleep_until(deadlines.cooperative) => break,
            }
        }
        self.scopes.seal();
        if !tasks.is_empty() {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .snapshot
                .abort_requested += tasks.len();
            tasks.abort_all();
            let mut deadline_passed = false;
            while !tasks.is_empty() {
                tokio::select! {
                    biased;
                    result = tasks.join_next() => self.record_join(result.unwrap()),
                    _ = tokio::time::sleep_until(deadlines.execution_stop), if !deadline_passed => {
                        deadline_passed = true;
                        self.state.lock().unwrap_or_else(|p| p.into_inner()).snapshot.execution_deadline_missed = true;
                    }
                }
            }
        }
        self.scopes.drain_before(deadlines.cleanup).await;
    }

    async fn run_worker(
        self: Arc<Self>,
        registration: Registration,
        ready: oneshot::Sender<Result<(), BackgroundServiceError>>,
    ) -> Completion {
        let name = registration.name;
        let constructed =
            AssertUnwindSafe(async { (registration.construct)(Arc::clone(&self.scopes)).await })
                .catch_unwind()
                .await;
        let worker = match constructed {
            Ok(Ok(worker)) => {
                let _ = ready.send(Ok(()));
                worker
            }
            Ok(Err(error)) => {
                let _ = ready.send(Err(error));
                return Completion::Failed;
            }
            Err(_) => {
                let _ = ready.send(Err(BackgroundServiceError::InitializationPanicked {
                    service: name,
                }));
                return Completion::Panicked;
            }
        };
        let token = self.stopping.view();
        tokio::select! {
            biased;
            _ = token.cancelled() => return Completion::NotStarted,
            _ = self.start.cancelled() => {},
        }
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot
            .started += 1;
        let mut event = ExecutionEvent::new(name);
        match AssertUnwindSafe(async { worker.execute(token).await })
            .catch_unwind()
            .await
        {
            Ok(true) => {
                event.outcome = "completed";
                Completion::Completed
            }
            Ok(false) => {
                event.outcome = "error";
                Completion::Failed
            }
            Err(_) => {
                event.outcome = "panicked";
                Completion::Panicked
            }
        }
    }

    fn record_join(&self, result: Result<Completion, JoinError>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if self
            .deadlines
            .get()
            .is_some_and(|limits| Instant::now() > limits.execution_stop)
        {
            // A ready join polled late must not erase a missed execution
            // cutoff merely because select's biased join arm beat its timer.
            state.snapshot.execution_deadline_missed = true;
        }
        state.snapshot.joined += 1;
        match result {
            Ok(Completion::Completed) => state.snapshot.completed += 1,
            Ok(Completion::NotStarted) => state.snapshot.stopped_before_start += 1,
            Ok(Completion::Failed) => state.snapshot.failed += 1,
            Ok(Completion::Panicked) => state.snapshot.panicked += 1,
            Err(error) if error.is_cancelled() => state.snapshot.aborted += 1,
            Err(_) => state.snapshot.panicked += 1,
        }
        self.failure.notify_waiters();
    }
}

enum Completion {
    Completed,
    NotStarted,
    Failed,
    Panicked,
}

struct ExecutionEvent {
    service: &'static str,
    started: Measurement,
    outcome: &'static str,
}
impl ExecutionEvent {
    fn new(service: &'static str) -> Self {
        tracing::info!(target: "lily_background_service", service, lifecycle = "started", "Background service started");
        Self {
            service,
            started: Measurement::now(),
            outcome: "dropped",
        }
    }
}
impl Drop for ExecutionEvent {
    fn drop(&mut self) {
        let duration_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        match self.outcome {
            "error" | "panicked" => {
                tracing::error!(target: "lily_background_service", service = self.service, lifecycle = self.outcome, duration_ms, "Background service failed")
            }
            "dropped" => {
                tracing::warn!(target: "lily_background_service", service = self.service, lifecycle = self.outcome, duration_ms, "Background service execution dropped")
            }
            _ => {
                tracing::info!(target: "lily_background_service", service = self.service, lifecycle = self.outcome, duration_ms, "Background service completed")
            }
        }
    }
}
