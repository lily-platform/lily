//! Request owners outlive service waiters and their replaceable execution slot.
//!
//! Response sources, input transfers and file helper joins also precede scope
//! disposal. Middleware obligations survive execution and unwind separately.
//! Never use this inventory as body-delivery proof.

mod body;
pub(crate) mod deadline;
mod report;
pub(crate) use report::RequestOwnerSnapshot;
pub(crate) mod middleware;
pub(crate) use body::BodyBridge;

use std::{
    collections::HashMap,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use lily_injection::{ApplicationContainer, ApplicationScope, InjectionError, ProcessContext};
use lily_web_core::{Request, Response};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    app::App,
    lifecycle::{
        CleanupOutcome, ExecutionEvidence, ExecutionStopReason, ResponseEvidence,
        ScopeCleanupEvidence, SlotTermination,
    },
    shutdown::{ShutdownBudget, ShutdownStage},
    tasks::{TaskReceipt, TaskRegistry},
    HttpApiError,
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

type ScopeClose = Shared<BoxFuture<'static, ScopeCloseOutcome>>;
type ScopeTerminal = Shared<BoxFuture<'static, ()>>;

#[derive(Clone)]
struct ScopeCloseOutcome {
    // false means another DI owner had already claimed cleanup: its disposal
    // result is not available through this close ticket, even after termination.
    result: Result<bool, InjectionError>,
    deadline_failure: Option<InjectionError>,
}

struct ScopeReceipt {
    observation: lily_injection::__private::ScopeCleanupObservation,
    terminal: ScopeTerminal,
    close: Option<ScopeClose>,
}

impl ScopeReceipt {
    fn evidence(&self) -> ScopeCleanupEvidence {
        // These observers poll DI-owned receipts, never user disposal inline.
        let result = self
            .close
            .as_ref()
            .and_then(|close| close.clone().now_or_never());
        if self.terminal.clone().now_or_never().is_none() {
            return ScopeCleanupEvidence::Outstanding;
        }
        ScopeCleanupEvidence::Terminated(match result {
            Some(ScopeCloseOutcome {
                result: Ok(true),
                deadline_failure: None,
            }) => CleanupOutcome::Succeeded,
            Some(ScopeCloseOutcome {
                result: Err(InjectionError::ScopeCleanupTimedOut { .. }),
                ..
            })
            | Some(ScopeCloseOutcome {
                deadline_failure: Some(InjectionError::ScopeCleanupTimedOut { .. }),
                ..
            }) => CleanupOutcome::TimedOut,
            Some(ScopeCloseOutcome {
                result: Ok(false),
                deadline_failure: None,
            }) => CleanupOutcome::Unknown,
            Some(_) => CleanupOutcome::Failed,
            None => CleanupOutcome::Unknown,
        })
    }
}

pub(crate) struct RequestResources {
    pub(crate) request: Option<Request>,
    pub(crate) response: Option<Response>,
    pub(crate) scope: Option<ApplicationScope>,
}

#[derive(Default)]
struct RequestEvidence {
    admitted: bool,
    execution_created: bool,
    execution: ExecutionEvidence,
    scope_created: bool,
    scope: Option<ScopeReceipt>,
    resource_release_failed: bool,
    cleanup_wait_failed: bool,
    owner_panicked: bool,
    body_created: bool,
    body_streaming: bool,
    body_suppressed: bool,
    response: ResponseEvidence,
}

struct RequestState {
    context: ProcessContext,
    resources: tokio::sync::Mutex<Option<RequestResources>>,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    evidence: Mutex<RequestEvidence>,
    deadline: deadline::RequestDeadline,
    execution_changed: Arc<Notify>,
    force: CancellationToken,
    budget: ShutdownBudget,
    cleanup_deadline: OnceLock<Instant>,
    cleanup_cap: OnceLock<Duration>,
    body_cleanup_deadline: OnceLock<Instant>,
    producer: Mutex<Option<body::ResponseProducer>>,
    children: lily_web_core::__private::HttpResources,
    middleware: middleware::MiddlewareLedger,
    extensions: Arc<lily_injection::Extensions>,
    span: OnceLock<tracing::Span>,
    response_transport: OnceLock<crate::server::response_control::ResponseTransportControl>,
}

/// Internal owner capability. This is not a public execution/cleanup token.
#[derive(Clone)]
pub(crate) struct RequestExecutionContext(Arc<RequestState>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionInterrupted {
    TimedOut,
    Stopped,
    Panicked,
    DropUnconfirmed,
}

// Initial internal policy for local timeout, peer loss and explicit force.
// This is one window per request, always shortened by the root C cutoff.
pub(crate) const COOPERATIVE_CANCELLATION_CAP: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestAdmissionError {
    Shutdown,
    Capacity,
}

impl RequestExecutionContext {
    fn new(app: &App, execution_changed: Arc<Notify>) -> Self {
        Self(Arc::new(RequestState {
            context: ProcessContext::new().with_metadata("transport".into(), "http".into()),
            resources: tokio::sync::Mutex::new(None),
            permit: Mutex::new(None),
            evidence: Mutex::new(RequestEvidence::default()),
            deadline: deadline::RequestDeadline::new(
                app.shutdown_budget().clone(),
                app.transport_force(),
            ),
            execution_changed,
            force: app.transport_force(),
            budget: app.shutdown_budget().clone(),
            cleanup_deadline: OnceLock::new(),
            cleanup_cap: OnceLock::new(),
            body_cleanup_deadline: OnceLock::new(),
            producer: Mutex::new(None),
            children: lily_web_core::__private::HttpResources::default(),
            middleware: middleware::MiddlewareLedger::default(),
            extensions: app.extensions(),
            span: OnceLock::new(),
            response_transport: OnceLock::new(),
        }))
    }

    fn stop(&self, reason: ExecutionStopReason) {
        self.0.deadline.cancel(reason);
        let mut evidence = lock(&self.0.evidence);
        evidence
            .execution
            .request_cancellation(self.cancellation_reason().expect("stop reason published"));
    }

    pub(crate) fn cancellation(&self) -> lily_cancellation::ExecutionCancellation {
        self.0.deadline.cancellation()
    }

    pub(crate) fn cancellation_reason(&self) -> Option<ExecutionStopReason> {
        self.0.deadline.reason()
    }

    pub(crate) fn bind_response_transport(
        &self,
        control: crate::server::response_control::ResponseTransportControl,
    ) {
        control.bind_deadline(self.0.deadline.clone());
        assert!(
            self.0.response_transport.set(control).is_ok(),
            "one transport receipt per request owner"
        );
    }

    pub(crate) fn response_transport(
        &self,
    ) -> Option<&crate::server::response_control::ResponseTransportControl> {
        self.0.response_transport.get()
    }

    fn start_deadline(&self, timeout: Duration) {
        self.0.deadline.start(timeout);
        assert!(self.0.cleanup_cap.set(timeout).is_ok());
    }

    /// The admitted request's deadline, retained across execution phases.
    /// Looking it up never starts another timeout.
    pub(crate) fn execution_deadline(&self) -> Instant {
        self.0.deadline.at()
    }

    /// A control waiter only. Execution and body slots remain owned by their
    /// callers and share the first signal's window, including later root caps.
    async fn cooperative_cutoff(&self) {
        self.0.deadline.cooperative_cutoff().await;
    }

    pub(crate) fn track_input(
        &self,
        input: Box<dyn lily_web_core::RequestBodyStream>,
    ) -> Box<dyn lily_web_core::RequestBodyStream> {
        self.0.children.track_input(input)
    }

    /// Linearize accepted identity and capacity under the admission gate.
    /// A candidate owner exists already, but no user work has been polled.
    pub(crate) fn admit(
        &self,
        app: &App,
        capacity: Arc<Semaphore>,
        timeout: Duration,
    ) -> Result<(), RequestAdmissionError> {
        let mut registry = lock(&app.request_registry().state);
        if registry.admission_closed || app.request_admission_stopping() {
            registry.close_admission();
            registry.reject();
            return Err(RequestAdmissionError::Shutdown);
        }
        let mut evidence = lock(&self.0.evidence);
        assert!(!evidence.admitted, "one admission decision per request");
        let permit = capacity.try_acquire_owned().map_err(|_| {
            registry.reject();
            RequestAdmissionError::Capacity
        })?;
        self.retain_permit(permit);
        self.start_deadline(timeout);
        evidence.admitted = true;
        Ok(())
    }

    pub(crate) fn retain_permit(&self, permit: OwnedSemaphorePermit) {
        *lock(&self.0.permit) = Some(permit);
    }

    /// Publish resources and their creation-bound DI observation before user
    /// work can be polled. The mutex guard is only a borrow of owner-held state.
    pub(crate) async fn prepare_scope(
        &self,
        container: &ApplicationContainer,
        mut request: Request,
        response: Response,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<RequestResources>>, HttpApiError> {
        let mut resources = self.0.resources.lock().await;
        if resources.is_some() || lock(&self.0.evidence).scope_created {
            return Err(HttpApiError::StateError(
                "HTTP request scope already registered".into(),
            ));
        }
        lily_web_core::__private::bind_request_execution(&mut request, self.cancellation());
        self.0
            .middleware
            .initialize(self.0.context.process_id, &request);
        let context = self
            .0
            .context
            .clone()
            .with_metadata("transport".into(), "http".into())
            .with_metadata("method".into(), request.method().to_string())
            .with_metadata("path".into(), request.path().to_string());
        let scope = container.create_scope(context)?;
        let observation = lily_injection::__private::application_scope_cleanup_observation(&scope);
        let terminal =
            lily_injection::__private::wait_for_observed_scope_cleanup(observation.clone())
                .boxed()
                .shared();
        {
            let mut evidence = lock(&self.0.evidence);
            evidence.scope_created = true;
            evidence.scope = Some(ScopeReceipt {
                observation,
                terminal,
                close: None,
            });
        }
        *resources = Some(RequestResources {
            request: Some(request),
            response: Some(response),
            scope: Some(scope),
        });
        Ok(resources)
    }

    /// Poll the same dispatch slot after signalling cancellation. Only after
    /// its cooperative cutoff can a pending slot be destroyed; this owner and
    /// cleanup authority remain outside that replaceable execution future.
    pub(crate) async fn execute<T>(
        &self,
        future: impl Future<Output = Result<T, HttpApiError>>,
    ) -> Result<Result<T, HttpApiError>, ExecutionInterrupted> {
        {
            let mut evidence = lock(&self.0.evidence);
            assert!(!evidence.execution_created, "one dispatch slot per request");
            evidence.execution_created = true;
        }
        let deadline = self.execution_deadline();
        let timeout = *self
            .0
            .cleanup_cap
            .get()
            .expect("admission publishes cleanup cap");
        let _ = self.0.span.set(tracing::Span::current());
        let mut slot = Box::pin(self.0.middleware.scope(self.0.children.scope(future)));
        let result = {
            let polled = futures::future::poll_fn(|cx| {
                lock(&self.0.evidence).execution.observe_first_poll();
                slot.as_mut().poll(cx)
            });
            let invocation = AssertUnwindSafe(polled).catch_unwind();
            tokio::pin!(invocation);
            // Do not first-poll unstarted execution after its stop/deadline.
            self.0.deadline.refresh();
            if let Some(reason) = self.cancellation_reason() {
                self.stop(reason);
            }
            if self.0.deadline.is_cancelled() {
                Err(self.interruption())
            } else {
                let stopped = async {
                    self.0.deadline.wait_for_stop().await;
                    self.stop(self.cancellation_reason().expect("stop reason published"));
                };
                tokio::pin!(stopped);
                tokio::select! {
                    biased;
                    _ = &mut stopped => {
                        // The control timer never owns invocation. Keep polling
                        // this exact pinned future until it returns or C expires.
                        tokio::select! {
                            biased;
                            _ = self.cooperative_cutoff() => Err(self.interruption()),
                            result = &mut invocation => result.map_err(|_| ExecutionInterrupted::Panicked),
                        }
                    },
                    result = &mut invocation => result.map_err(|_| ExecutionInterrupted::Panicked),
                }
            }
        };
        // Release the poll adapter's borrow, then destroy the actual future.
        // A panic or blocked destructor cannot manufacture terminal evidence.
        if catch_unwind(AssertUnwindSafe(|| drop(slot))).is_err() {
            lock(&self.0.evidence).resource_release_failed = true;
            return Err(ExecutionInterrupted::DropUnconfirmed);
        }
        let termination = match &result {
            Ok(Ok(_)) => SlotTermination::Returned,
            Ok(Err(_)) => SlotTermination::ReturnedError,
            Err(ExecutionInterrupted::Panicked) => SlotTermination::Panicked,
            Err(_) => SlotTermination::Dropped,
        };
        lock(&self.0.evidence)
            .execution
            .observe_termination(termination);
        self.0.execution_changed.notify_waiters();
        // Normal DI close remains inside the normal request deadline. An
        // interrupted slot needs a separate local cleanup allowance; reusing
        // its expired execution deadline would immediately kill disposal.
        // This is a request-local cap, always clamped by the existing U/R.
        let cleanup_deadline = if result.is_err() || self.0.deadline.is_cancelled() {
            let now = Instant::now();
            now.checked_add(timeout).unwrap_or(now)
        } else {
            deadline
        };
        let _ = self.0.cleanup_deadline.set(cleanup_deadline);
        result
    }

    /// Owner qualification can bypass server admission; production cannot.
    #[cfg(test)]
    pub(crate) async fn execute_for_test<T>(
        &self,
        timeout: Duration,
        future: impl Future<Output = Result<T, HttpApiError>>,
    ) -> Result<Result<T, HttpApiError>, ExecutionInterrupted> {
        self.start_deadline(timeout);
        self.execute(future).await
    }

    fn interruption(&self) -> ExecutionInterrupted {
        if self.cancellation_reason() == Some(ExecutionStopReason::RequestTimeout) {
            ExecutionInterrupted::TimedOut
        } else {
            ExecutionInterrupted::Stopped
        }
    }

    /// Begin cleanup only after slot destruction/return is confirmed. The same
    /// retained close future is reused after waiter expiry, never a second close.
    pub(crate) async fn close_scope(&self) -> Result<(), HttpApiError> {
        let permitted = {
            let evidence = lock(&self.0.evidence);
            !evidence.resource_release_failed
                && (!evidence.execution_created || evidence.execution.is_terminal())
                && (!evidence.body_created || evidence.response.resources_terminal())
        };
        if !permitted {
            return Err(HttpApiError::StateError(
                "HTTP request execution termination is unconfirmed".into(),
            ));
        }
        let mut resources = self.0.resources.lock().await;
        if let Some(resources) = resources.as_mut() {
            if let Some(scope) = resources.scope.as_ref() {
                let context = scope.context().clone();
                let released = ProcessContext::scope(context, async {
                    let request = resources.request.take();
                    let response = resources.response.take();
                    let request = catch_unwind(AssertUnwindSafe(|| drop(request)));
                    let response = catch_unwind(AssertUnwindSafe(|| drop(response)));
                    request.is_ok() && response.is_ok()
                })
                .await;
                if !released {
                    lock(&self.0.evidence).resource_release_failed = true;
                    return Err(HttpApiError::StateError(
                        "HTTP request resources did not release cleanly".into(),
                    ));
                }
            }
        }
        let local = self
            .0
            .body_cleanup_deadline
            .get()
            .or_else(|| self.0.cleanup_deadline.get())
            .copied()
            .unwrap_or_else(Instant::now);
        let context = resources
            .as_ref()
            .and_then(|resources| resources.scope.as_ref())
            .map(|scope| scope.context().clone())
            .unwrap_or_else(|| self.0.context.clone());
        let reason = match self.cancellation_reason() {
            Some(ExecutionStopReason::GracefulDeadline) => {
                lily_middleware::HttpRequestInterruption::GracefulDeadline
            }
            Some(ExecutionStopReason::ForcedShutdown) => {
                lily_middleware::HttpRequestInterruption::ForcedShutdown
            }
            Some(ExecutionStopReason::RequestTimeout) => {
                lily_middleware::HttpRequestInterruption::RequestTimeout
            }
            Some(ExecutionStopReason::ResponseFinalizationTimeout) => {
                lily_middleware::HttpRequestInterruption::ResponseFinalizationTimeout
            }
            Some(ExecutionStopReason::PeerDisconnect) => {
                lily_middleware::HttpRequestInterruption::PeerDisconnect
            }
            Some(ExecutionStopReason::TransportFailure) => {
                lily_middleware::HttpRequestInterruption::TransportFailure
            }
            Some(ExecutionStopReason::ServiceWaiterDropped) => {
                lily_middleware::HttpRequestInterruption::ServiceWaiterDropped
            }
            None if lock(&self.0.evidence).execution.termination()
                == Some(SlotTermination::Panicked) =>
            {
                lily_middleware::HttpRequestInterruption::ExecutionPanicked
            }
            None => lily_middleware::HttpRequestInterruption::ExecutionInterrupted,
        };
        let finalization = self
            .0
            .middleware
            .finalize(
                context,
                self.0.extensions.clone(),
                reason,
                self.0.budget.clone(),
                local,
                self.0.children.clone(),
                self.0.deadline.is_cancelled(),
            )
            .await;
        if finalization.failed {
            lock(&self.0.evidence).cleanup_wait_failed = true;
        }
        if !finalization.safe_to_close {
            return Err(HttpApiError::StateError(
                "HTTP request middleware/input/helper termination remains unconfirmed".into(),
            ));
        }
        if let Some(resources) = resources.as_mut() {
            if resources.scope.is_some() {
                let mut evidence = lock(&self.0.evidence);
                let Some(receipt) = evidence.scope.as_mut() else {
                    return Err(HttpApiError::StateError(
                        "HTTP request scope receipt is missing".into(),
                    ));
                };
                let mut scope = resources.scope.take().unwrap();
                let observation = receipt.observation.clone();
                let budget = self.0.budget.clone();
                receipt.close = Some(async move {
                    let close = lily_injection::__private::close_application_scope_before(&mut scope, local);
                    tokio::pin!(close);
                    match budget.wait_for_receipt(ShutdownStage::Cleanup, close.as_mut()).await {
                        Ok(result) => ScopeCloseOutcome { result, deadline_failure: None },
                        Err(()) => {
                            // Stop only this generation. The enclosing retained
                            // receipt remains pending until DI proves actual joins.
                            let cutoff = budget.deadlines().expect("installed root deadline")
                                .at(ShutdownStage::Cleanup);
                            let stopped = lily_injection::__private::wait_for_observed_scope_cleanup_before(
                                observation, cutoff,
                            ).await;
                            ScopeCloseOutcome { result: close.await, deadline_failure: stopped.err() }
                        }
                    }
                }.instrument(tracing::Span::current()).boxed().shared());
            }
        }
        drop(resources);
        let receipt = lock(&self.0.evidence).scope.as_ref().and_then(|receipt| {
            receipt
                .close
                .clone()
                .map(|close| (close, receipt.terminal.clone()))
        });
        let Some((close, terminal)) = receipt else {
            return if lock(&self.0.evidence).scope_created {
                Err(HttpApiError::StateError(
                    "HTTP request scope close receipt is missing".into(),
                ))
            } else {
                Ok(())
            };
        };
        match self
            .0
            .budget
            .wait_for_receipt(ShutdownStage::Reconcile, close)
            .await
        {
            Ok(outcome) => {
                if self
                    .0
                    .budget
                    .wait_for_receipt(ShutdownStage::Reconcile, terminal)
                    .await
                    .is_err()
                {
                    lock(&self.0.evidence).cleanup_wait_failed = true;
                    return Err(HttpApiError::StateError(
                        "HTTP scope disposal join remains outstanding".into(),
                    ));
                }
                match outcome.deadline_failure {
                    Some(error) => Err(HttpApiError::from(error)),
                    None => match outcome.result {
                        Ok(true) if finalization.failed => Err(HttpApiError::StateError(
                            "HTTP middleware/resource cleanup did not complete successfully".into(),
                        )),
                        Ok(true) => Ok(()),
                        Ok(false) => Err(HttpApiError::StateError(
                            "HTTP scope disposal result was claimed by another owner".into(),
                        )),
                        Err(error) => Err(HttpApiError::from(error)),
                    },
                }
            }
            Err(()) => {
                lock(&self.0.evidence).cleanup_wait_failed = true;
                Err(HttpApiError::StateError(
                    "HTTP request scope cleanup remains outstanding".into(),
                ))
            }
        }
    }
}

struct RequestEntry {
    in_attempt: bool,
    context: RequestExecutionContext,
    join: TaskReceipt<()>,
    // Retain dependencies when this owner/receipt cannot prove termination.
    _application: Arc<App>,
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<u64, RequestEntry>,
    retired: RequestOwnerSnapshot,
    attempt: Option<RequestOwnerSnapshot>,
    sealed: bool,
    admission_closed: bool,
}

#[derive(Clone, Default)]
pub(crate) struct RequestRegistry {
    state: Arc<Mutex<RegistryState>>,
    tasks: TaskRegistry,
    execution_changed: Arc<Notify>,
}

#[derive(Debug)]
pub(crate) enum RequestOwnerError {
    AdmissionClosed,
    Failed,
}

struct HttpRequestLifecycleOwner {
    context: RequestExecutionContext,
}

impl HttpRequestLifecycleOwner {
    async fn run<T, F>(
        self,
        factory: impl FnOnce(RequestExecutionContext) -> F,
        sender: oneshot::Sender<Result<T, RequestOwnerError>>,
    ) where
        F: Future<Output = T>,
    {
        let owner = self.context;
        // Response selection/suppression also destroys user-owned sources.
        // Keep request identity active outside the inner application dispatch.
        let mut job = Box::pin(ProcessContext::scope(owner.0.context.clone(), async {
            factory(owner.clone()).await
        }));
        let result = AssertUnwindSafe(job.as_mut()).catch_unwind().await;
        let released = catch_unwind(AssertUnwindSafe(|| drop(job))).is_ok();
        if !released {
            lock(&owner.0.evidence).resource_release_failed = true;
        }
        if result.is_err() || !released {
            let mut evidence = lock(&owner.0.evidence);
            evidence.owner_panicked = true;
            // Slot poll panics are contained inside execute. An outer panic
            // may be a response destructor: its release is unconfirmed.
            evidence.resource_release_failed = true;
        }
        // The service can hand the head to Hyper while this retained task
        // continues to own body production and cleanup. Losing the waiter only
        // drops the bridge; the source and DI receipt remain here.
        let _ = sender.send(result.map_err(|_| RequestOwnerError::Failed));
        async {
            owner.finish_response().await;
            if let Err(error) = owner.close_scope().await {
                tracing::error!(
                    lily.event = "http.request.cleanup_incomplete",
                    lily.error_code = error.error_code(),
                    "HTTP request cleanup did not complete successfully"
                );
            }
        }
        .instrument(
            owner
                .0
                .span
                .get()
                .cloned()
                .unwrap_or_else(tracing::Span::none),
        )
        .await;
    }
}

pub(crate) struct RequestWaiter<T> {
    receiver: oneshot::Receiver<Result<T, RequestOwnerError>>,
    context: RequestExecutionContext,
    #[cfg(test)]
    join: TaskReceipt<()>,
    registry: RequestRegistry,
    armed: bool,
}

impl<T> RequestWaiter<T> {
    pub(crate) async fn handoff(mut self) -> Result<T, RequestOwnerError> {
        let result = (&mut self.receiver)
            .await
            .map_err(|_| RequestOwnerError::Failed)?;
        self.armed = false;
        if result.is_ok() {
            lock(&self.context.0.evidence)
                .response
                .observe_service_handoff();
        }
        self.registry.snapshot();
        result
    }

    #[cfg(test)]
    pub(crate) async fn wait(mut self) -> Result<T, RequestOwnerError> {
        let result = (&mut self.receiver)
            .await
            .map_err(|_| RequestOwnerError::Failed)?;
        self.join
            .clone()
            .await
            .map_err(|_| RequestOwnerError::Failed)?;
        self.armed = false;
        self.registry.snapshot();
        result
    }
}

impl<T> Drop for RequestWaiter<T> {
    fn drop(&mut self) {
        if self.armed {
            let reason = self
                .context
                .response_transport()
                .and_then(|control| control.snapshot().stop_requested)
                .unwrap_or_else(|| {
                    if self.context.0.force.is_cancelled() {
                        ExecutionStopReason::ForcedShutdown
                    } else {
                        ExecutionStopReason::ServiceWaiterDropped
                    }
                });
            self.context.stop(reason);
        }
    }
}

impl RequestRegistry {
    #[cfg(test)]
    pub(crate) fn response_transports(
        &self,
    ) -> Vec<crate::server::response_control::ResponseTransportControl> {
        lock(&self.state)
            .entries
            .values()
            .filter_map(|entry| entry.context.response_transport().cloned())
            .collect()
    }

    pub(crate) fn spawn<T, F, Factory>(
        &self,
        app: Arc<App>,
        factory: Factory,
    ) -> Result<RequestWaiter<T>, RequestOwnerError>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        Factory: FnOnce(RequestExecutionContext) -> F + Send + 'static,
    {
        let mut state = lock(&self.state);
        if state.sealed || state.admission_closed || app.request_admission_stopping() {
            state.close_admission();
            state.reject();
            return Err(RequestOwnerError::AdmissionClosed);
        }
        state.reap();
        let context = RequestExecutionContext::new(&app, self.execution_changed.clone());
        let id = context.0.context.process_id;
        let owner = context.clone();
        let (sender, receiver) = oneshot::channel();
        let (publish, published) = oneshot::channel();
        let join = self.tasks.spawn(async move {
            // TaskRegistry publishes the actual join first; this second gate
            // publishes its request identity/state before any factory/user work.
            if published.await.is_err() {
                return;
            }
            HttpRequestLifecycleOwner { context: owner }
                .run(factory, sender)
                .await;
        });
        state.entries.insert(
            id,
            RequestEntry {
                in_attempt: false,
                context: context.clone(),
                join: join.clone(),
                _application: app,
            },
        );
        drop(state);
        let _ = publish.send(());
        Ok(RequestWaiter {
            receiver,
            context,
            #[cfg(test)]
            join,
            registry: self.clone(),
            armed: true,
        })
    }

    pub(crate) fn close_admission(&self) {
        lock(&self.state).close_admission();
    }

    pub(crate) fn admission_closed(&self) -> bool {
        lock(&self.state).admission_closed
    }

    pub(crate) fn cancel_executions(&self, reason: ExecutionStopReason) {
        let mut state = lock(&self.state);
        state.close_admission();
        for entry in state.entries.values() {
            entry.context.stop(reason);
        }
    }

    pub(crate) async fn wait_for_executions(&self) {
        loop {
            let changed = self.execution_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let terminal = lock(&self.state).entries.values().all(|entry| {
                let evidence = lock(&entry.context.0.evidence);
                (!evidence.execution_created || evidence.execution.is_terminal())
                    && (!evidence.body_created || evidence.response.resources_terminal())
            });
            if terminal {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn seal(&self) {
        lock(&self.state).sealed = true;
        self.tasks.seal();
    }

    /// Frozen-cohort accounting; live receipts are observed without restarting work.
    pub(crate) fn attempt_snapshot(&self) -> RequestOwnerSnapshot {
        lock(&self.state).snapshot_for(true)
    }

    pub(crate) fn snapshot(&self) -> RequestOwnerSnapshot {
        lock(&self.state).snapshot()
    }

    pub(crate) async fn wait(&self) -> RequestOwnerSnapshot {
        let _ = self.tasks.wait().await;
        // An owner can return with a retained middleware/helper barrier. Resume
        // that same finalizer after the child joins; its settled hook outcomes
        // and original U/R cutoffs cannot be restarted by this observer.
        let owners = lock(&self.state)
            .entries
            .values()
            .map(|entry| entry.context.clone())
            .collect::<Vec<_>>();
        for owner in owners {
            let _ = owner.close_scope().await;
        }
        let receipts = lock(&self.state)
            .entries
            .values()
            .filter_map(|entry| {
                lock(&entry.context.0.evidence)
                    .scope
                    .as_ref()
                    .map(|scope| (scope.close.clone(), scope.terminal.clone()))
            })
            .collect::<Vec<_>>();
        for (close, terminal) in receipts {
            if let Some(close) = close {
                let _ = close.await;
            }
            terminal.await;
        }
        self.snapshot()
    }
}

#[cfg(test)]
mod tests;
