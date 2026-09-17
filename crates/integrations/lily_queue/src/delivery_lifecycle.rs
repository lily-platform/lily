//! Delivery state outlives the replaceable user execution future. Abandoned
//! owners transfer their resources to tracked cleanup, never to detached work.

use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    FutureExt,
    future::{AbortHandle, AbortRegistration, Abortable, BoxFuture, Shared},
};
use lily_error::{
    application::{MessageBrokerError, QueueHandlerError, message_broker::RabbitMQError},
    injection::InjectionError,
};
use lily_injection::{ApplicationScope, ProcessContext};
use tokio::{sync::Notify, time::Instant};
use tracing::Instrument;

use crate::{
    DeliveryCancellationReason, DeliveryInvocation, DeliveryNormalExit, DeliveryTerminationReason,
    cancellation::DeliveryCancellationSource,
    pipeline::CompiledQueuePipeline,
    shutdown_budget::{QueueShutdownBudget, QueueShutdownDeadlines},
};

const ABANDONED_SCOPE_CLEANUP_CAP: Duration = Duration::from_millis(100);

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MiddlewareExitState {
    NotStarted,
    Running,
    Completed,
    Failed,
    Interrupted,
    Panicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminationState {
    NotStarted,
    Running,
    Completed,
    Failed,
    Panicked,
    TimedOut,
    Interrupted,
    BudgetExhausted,
}

struct MiddlewareEntry {
    normal: MiddlewareExitState,
    termination: TerminationState,
}

/// One slot per successfully entered middleware, in actual entry order.
/// The owner retains this ledger; a pending callback only borrows it.
#[derive(Clone, Default)]
pub(crate) struct DeliveryMiddlewareLedger(Arc<Mutex<Vec<MiddlewareEntry>>>);

impl DeliveryMiddlewareLedger {
    pub(crate) fn entered(&self) -> usize {
        lock(&self.0).len()
    }

    pub(crate) fn record_enter(&mut self, index: usize) {
        let mut entries = lock(&self.0);
        assert_eq!(
            entries.len(),
            index,
            "middleware enter must commit one prefix"
        );
        entries.push(MiddlewareEntry {
            normal: MiddlewareExitState::NotStarted,
            termination: TerminationState::NotStarted,
        });
    }

    pub(crate) fn begin_exit(&mut self, index: usize) -> bool {
        let mut entries = lock(&self.0);
        let Some(entry) = entries.get_mut(index) else {
            return false;
        };
        let state = &mut entry.normal;
        if *state != MiddlewareExitState::NotStarted {
            return false;
        }
        *state = MiddlewareExitState::Running;
        true
    }

    pub(crate) fn finish_exit(&mut self, index: usize, succeeded: bool) {
        let mut entries = lock(&self.0);
        assert_eq!(entries[index].normal, MiddlewareExitState::Running);
        entries[index].normal = if succeeded {
            MiddlewareExitState::Completed
        } else {
            MiddlewareExitState::Failed
        };
    }

    pub(crate) fn interrupt_exit(&mut self) {
        for entry in lock(&self.0).iter_mut() {
            let state = &mut entry.normal;
            if *state == MiddlewareExitState::Running {
                *state = MiddlewareExitState::Interrupted;
            }
            if entry.termination == TerminationState::Running {
                entry.termination = TerminationState::Interrupted;
            }
        }
    }

    pub(crate) fn panicked_exit(&mut self, index: usize) {
        lock(&self.0)[index].normal = MiddlewareExitState::Panicked;
    }

    pub(crate) fn eligible_termination(&self, index: usize) -> Option<DeliveryNormalExit> {
        let entries = lock(&self.0);
        let entry = entries.get(index)?;
        if entry.termination != TerminationState::NotStarted {
            return None;
        }
        match entry.normal {
            MiddlewareExitState::NotStarted => Some(DeliveryNormalExit::NotStarted),
            MiddlewareExitState::Interrupted => Some(DeliveryNormalExit::Interrupted),
            MiddlewareExitState::Panicked => Some(DeliveryNormalExit::Panicked),
            _ => None,
        }
    }

    pub(crate) fn termination_state(&mut self, index: usize, state: TerminationState) {
        lock(&self.0)[index].termination = state;
    }

    pub(crate) fn termination_failed(&self) -> bool {
        lock(&self.0).iter().any(|entry| {
            matches!(
                entry.termination,
                TerminationState::Failed
                    | TerminationState::Panicked
                    | TerminationState::TimedOut
                    | TerminationState::Interrupted
                    | TerminationState::BudgetExhausted
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn states(&self) -> Vec<MiddlewareExitState> {
        lock(&self.0).iter().map(|entry| entry.normal).collect()
    }

    #[cfg(all(test, feature = "test-support"))]
    pub(crate) fn termination_states(&self) -> Vec<TerminationState> {
        lock(&self.0)
            .iter()
            .map(|entry| entry.termination)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionTermination {
    NotStarted,
    Returned,
    Dropped,
}

#[derive(Debug)]
struct ExecutionEvidence {
    started: bool,
    termination: Option<ExecutionTermination>,
    release_failed: bool,
}

impl Default for ExecutionEvidence {
    fn default() -> Self {
        Self {
            started: false,
            termination: Some(ExecutionTermination::NotStarted),
            release_failed: false,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ExecutionReceipt(Arc<Mutex<ExecutionEvidence>>);

impl ExecutionReceipt {
    pub(crate) fn terminal(&self) -> bool {
        let evidence = lock(&self.0);
        evidence.termination.is_some() && !evidence.release_failed
    }
}

/// This guard publishes termination only after the owned future is dropped.
/// It also runs when the enclosing broker or transaction task is aborted.
struct OwnedExecution<F> {
    future: Option<Pin<Box<F>>>,
    receipt: ExecutionReceipt,
    started: bool,
}

impl<F> OwnedExecution<F> {
    fn release(&mut self, outcome: ExecutionTermination) {
        let Some(future) = self.future.take() else {
            return;
        };
        let released = catch_unwind(AssertUnwindSafe(|| drop(future))).is_ok();
        let mut evidence = lock(&self.receipt.0);
        if released {
            evidence.termination = Some(outcome);
        } else {
            evidence.release_failed = true;
        }
    }
}

impl<F: Future> Future for OwnedExecution<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.started {
            this.started = true;
            lock(&this.receipt.0).started = true;
        }
        let result = this
            .future
            .as_mut()
            .expect("execution polled after termination")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            this.release(ExecutionTermination::Returned);
        }
        result
    }
}

impl<F> Drop for OwnedExecution<F> {
    fn drop(&mut self) {
        self.release(ExecutionTermination::Dropped);
    }
}

#[derive(Debug)]
pub(crate) enum DeliveryExecutionExit<T> {
    Completed(T),
    Cancelled,
    Panicked,
    Interrupted(DeliveryCancellationReason),
}

pub(crate) struct DeliveryExecutionSlot {
    registration: AbortRegistration,
    receipt: ExecutionReceipt,
    #[cfg(test)]
    abort: AbortHandle,
}

impl DeliveryExecutionSlot {
    pub(crate) fn receipt(&self) -> ExecutionReceipt {
        self.receipt.clone()
    }
    /// Construct synchronously so even an unpolled run future drops execution
    /// before publishing its receipt. No cancellation policy lives in this slot.
    pub(crate) fn run<F: Future>(
        self,
        future: F,
    ) -> impl Future<Output = DeliveryExecutionExit<F::Output>> {
        lock(&self.receipt.0).termination = None;
        let execution = OwnedExecution {
            future: Some(Box::pin(future)),
            receipt: self.receipt.clone(),
            started: false,
        };
        let mut execution = Box::pin(Abortable::new(execution, self.registration));
        async move {
            let result = AssertUnwindSafe(execution.as_mut()).catch_unwind().await;
            drop(execution);
            if !self.receipt.terminal() {
                return DeliveryExecutionExit::Panicked;
            }
            match result {
                Ok(Ok(output)) => DeliveryExecutionExit::Completed(output),
                Ok(Err(_)) => DeliveryExecutionExit::Cancelled,
                Err(_) => DeliveryExecutionExit::Panicked,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }
}

#[derive(Clone, Copy)]
enum CleanupJoin {
    Completed,
    Lost,
}
type CleanupTaskReceipt = Shared<BoxFuture<'static, CleanupJoin>>;

/// Retains exact scope ownership and the real joins of transferred cleanup.
/// Completed receipts are reaped during admission, observation and drain.
#[derive(Default)]
pub(crate) struct DeliveryScopeTracker {
    active: AtomicUsize,
    failed: AtomicBool,
    ownership_lost: AtomicBool,
    changed: Notify,
    cleanup_tasks: Mutex<Vec<CleanupTaskReceipt>>,
    budget: QueueShutdownBudget,
}

impl DeliveryScopeTracker {
    pub(crate) fn begin(self: &Arc<Self>) {
        let mut tasks = lock(&self.cleanup_tasks);
        self.reap(&mut tasks);
        self.active.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn finish(&self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "delivery scope tracker underflow");
        self.changed.notify_waiters();
    }

    pub(crate) fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn mark_ownership_lost(&self) {
        self.ownership_lost.store(true, Ordering::Release);
        self.mark_failed();
    }

    pub(crate) fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
        self.budget.install(deadlines);
    }

    pub(crate) fn shutdown_budget(&self) -> QueueShutdownBudget {
        self.budget.clone()
    }

    fn cap(&self, deadline: Instant) -> Instant {
        self.budget
            .deadlines()
            .map_or(deadline, |root| root.cap(deadline))
    }

    async fn close_resources(
        &self,
        resources: &mut DeliveryLifecycleResources,
        local_deadline: Instant,
    ) -> Result<(), QueueHandlerError> {
        resources.ledger.interrupt_exit();
        let deadline = *resources.cleanup_deadline.get_or_insert(local_deadline);
        let hook_deadline = *resources.termination_deadline.get_or_insert_with(|| {
            let hard = self.cap(deadline);
            let remaining = hard.saturating_duration_since(Instant::now());
            hard.checked_sub((remaining / 4).min(crate::shutdown_budget::DELIVERY_COOPERATIVE_CAP))
                .unwrap_or(hard)
        });
        if let Some(invocation) = resources.invocation.as_mut() {
            resources
                .pipeline
                .terminate_recorded(
                    invocation,
                    &mut resources.ledger,
                    resources
                        .interruption
                        .unwrap_or(DeliveryTerminationReason::Panicked),
                    hook_deadline,
                    &resources.cleanup_authority,
                    &self.budget,
                )
                .await;
        }
        loop {
            let revision = self.budget.revision();
            // Retained close/result and terminal receipts make cancellation
            // of this waiter safe when a newly published root is shorter.
            let deadline = self.cap(deadline);
            tokio::select! {
                biased;
                result = resources.close_scope(deadline) => {
                    result?;
                    return if resources.ledger.termination_failed() { Err(QueueHandlerError::retryable("QUEUE_DELIVERY_TERMINATION_CLEANUP_FAILED")) } else { Ok(()) };
                },
                _ = self.budget.changed_since(revision) => {}
            }
        }
    }

    fn reap(&self, tasks: &mut Vec<CleanupTaskReceipt>) {
        tasks.retain(|receipt| match receipt.clone().now_or_never() {
            None => true,
            Some(CleanupJoin::Completed) => false,
            Some(CleanupJoin::Lost) => {
                self.mark_ownership_lost();
                false
            }
        });
    }

    fn adopt_cleanup(
        &self,
        runtime: &tokio::runtime::Handle,
        cleanup: impl Future<Output = ()> + Send + 'static,
    ) {
        // No observer can see active == 0 without also seeing this retained
        // join, even if another worker finishes cleanup immediately on spawn.
        let mut tasks = lock(&self.cleanup_tasks);
        self.reap(&mut tasks);
        let task = runtime.spawn(cleanup);
        tasks.push(
            async move {
                match task.await {
                    Ok(()) => CleanupJoin::Completed,
                    Err(_) => CleanupJoin::Lost,
                }
            }
            .boxed()
            .shared(),
        );
        self.changed.notify_waiters();
    }

    pub(crate) async fn drain(&self) -> Result<(), MessageBrokerError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (pending, active) = {
                let mut tasks = lock(&self.cleanup_tasks);
                self.reap(&mut tasks);
                (tasks.first().cloned(), self.active.load(Ordering::Acquire))
            };
            if active == 0 && pending.is_none() {
                return if self.failed.load(Ordering::Acquire) {
                    Err(MessageBrokerError::RabbitMQError(
                        RabbitMQError::Configuration("delivery scope cleanup failed".into()),
                    ))
                } else {
                    Ok(())
                };
            }
            if let Some(pending) = pending {
                tokio::select! { _ = pending => {}, _ = changed.as_mut() => {} }
            } else {
                changed.as_mut().await;
            }
        }
    }

    pub(crate) fn reconciled(&self) -> bool {
        let mut tasks = lock(&self.cleanup_tasks);
        self.reap(&mut tasks);
        self.active.load(Ordering::Acquire) == 0
            && tasks.is_empty()
            && !self.ownership_lost.load(Ordering::Acquire)
    }
}

/// A task-local completion guard is not a task join. The tracker requires both.
pub(crate) struct DeliveryScopeCleanupObserver {
    tracker: Arc<DeliveryScopeTracker>,
    terminal_observed: bool,
}

impl DeliveryScopeCleanupObserver {
    pub(crate) fn new(tracker: Arc<DeliveryScopeTracker>) -> Self {
        Self {
            tracker,
            terminal_observed: false,
        }
    }
    pub(crate) fn complete(&mut self, succeeded: bool) {
        self.terminal_observed = true;
        if !succeeded {
            self.tracker.mark_failed();
        }
    }
}

impl Drop for DeliveryScopeCleanupObserver {
    fn drop(&mut self) {
        if !self.terminal_observed {
            self.tracker.mark_ownership_lost();
        }
        self.tracker.finish();
    }
}

type ScopeClose = Shared<BoxFuture<'static, Result<bool, QueueHandlerError>>>;
type ScopeTerminal = Shared<BoxFuture<'static, ()>>;

/// Captured from the scope handle itself, never looked up again by reusable ID.
struct DeliveryScopeReceipt {
    observation: lily_injection::__private::ScopeCleanupObservation,
    terminal: ScopeTerminal,
    close: Option<ScopeClose>,
}

pub(crate) struct DeliveryLifecycleResources {
    pub(crate) invocation: Option<DeliveryInvocation>,
    pub(crate) ledger: DeliveryMiddlewareLedger,
    pub(crate) pipeline: CompiledQueuePipeline,
    scope: Option<ApplicationScope>,
    scope_receipt: DeliveryScopeReceipt,
    execution: ExecutionReceipt,
    context: ProcessContext,
    deadline: Instant,
    cancellation: DeliveryCancellationSource,
    invocation_release_failed: bool,
    interruption: Option<DeliveryTerminationReason>,
    cleanup_deadline: Option<Instant>,
    termination_deadline: Option<Instant>,
    cleanup_authority: tokio_util::sync::CancellationToken,
}

fn scope_error(error: InjectionError) -> QueueHandlerError {
    let code = if matches!(error, InjectionError::ScopeCleanupTimedOut { .. }) {
        "QUEUE_DELIVERY_SCOPE_CLEANUP_TIMED_OUT"
    } else {
        "QUEUE_DELIVERY_SCOPE_CLEANUP_FAILED"
    };
    QueueHandlerError::retryable_with_source(code, error)
}

impl DeliveryLifecycleResources {
    fn cleanup_cutoff(&self, budget: &QueueShutdownBudget) -> Instant {
        let deadline = match (self.cancellation.reason(), self.cancellation.requested_at()) {
            (Some(reason), Some(at)) if reason != DeliveryCancellationReason::DeliveryTimeout => {
                self.deadline.min(budget.forced_delivery_deadline(at))
            }
            _ => self.deadline,
        };
        budget.cap(deadline)
    }

    async fn close_scope(&mut self, deadline: Instant) -> Result<(), QueueHandlerError> {
        self.ledger.interrupt_exit();
        // Scope-sensitive delivery locals are released before DI disposal,
        // after execution and the existing normal middleware unwind.
        if catch_unwind(AssertUnwindSafe(|| drop(self.invocation.take()))).is_err() {
            self.invocation_release_failed = true;
        }
        if self.scope_receipt.close.is_none() {
            let mut scope = self.scope.take().expect("scope close has one owner");
            self.scope_receipt.close = Some(
                async move {
                    AssertUnwindSafe(lily_injection::__private::close_application_scope_before(
                        &mut scope, deadline,
                    ))
                    .catch_unwind()
                    .await
                    .map_err(|_| {
                        QueueHandlerError::retryable("QUEUE_DELIVERY_SCOPE_CLEANUP_PANICKED")
                    })?
                    .map_err(scope_error)
                }
                .boxed()
                .shared(),
            );
        }
        // If a close waiter is cancelled, its original result future stays
        // here. A later, shorter observer can stop DI without restarting close.
        let (close, observation) = tokio::join!(
            self.scope_receipt
                .close
                .as_ref()
                .expect("close receipt installed")
                .clone(),
            lily_injection::__private::wait_for_observed_scope_cleanup_before(
                self.scope_receipt.observation.clone(),
                deadline
            ),
        );
        self.scope_receipt.terminal.clone().await;
        if self.invocation_release_failed {
            return Err(QueueHandlerError::retryable(
                "QUEUE_DELIVERY_RESOURCE_RELEASE_PANICKED",
            ));
        }
        observation.map_err(scope_error)?;
        match close {
            Ok(true) => Ok(()),
            Ok(false) => Err(QueueHandlerError::retryable(
                "QUEUE_DELIVERY_SCOPE_RESULT_UNOBSERVED",
            )),
            Err(error) => Err(error),
        }
    }
}

/// Survives execution stop, with an explicit transfer to retained cleanup if
/// an enclosing broker/transaction future is dropped. The mutable invocation,
/// middleware plan/ledger and exact scope receipt move together.
pub(crate) struct DeliveryLifecycleOwner {
    resources: Option<DeliveryLifecycleResources>,
    tracker: Arc<DeliveryScopeTracker>,
}

impl DeliveryLifecycleOwner {
    pub(crate) fn new(
        scope: ApplicationScope,
        invocation: DeliveryInvocation,
        pipeline: CompiledQueuePipeline,
        tracker: Arc<DeliveryScopeTracker>,
        deadline: Instant,
        cancellation: DeliveryCancellationSource,
    ) -> (Self, DeliveryExecutionSlot) {
        let context = scope.context().clone();
        let observation = lily_injection::__private::application_scope_cleanup_observation(&scope);
        let terminal =
            lily_injection::__private::wait_for_observed_scope_cleanup(observation.clone())
                .boxed()
                .shared();
        let receipt = ExecutionReceipt::default();
        let (_abort, registration) = AbortHandle::new_pair();
        tracker.begin();
        (
            Self {
                resources: Some(DeliveryLifecycleResources {
                    scope: Some(scope),
                    invocation: Some(invocation),
                    pipeline,
                    ledger: DeliveryMiddlewareLedger::default(),
                    scope_receipt: DeliveryScopeReceipt {
                        observation,
                        terminal,
                        close: None,
                    },
                    execution: receipt.clone(),
                    context,
                    deadline,
                    cancellation,
                    invocation_release_failed: false,
                    interruption: None,
                    cleanup_deadline: None,
                    termination_deadline: None,
                    cleanup_authority: tokio_util::sync::CancellationToken::new(),
                }),
                tracker,
            },
            DeliveryExecutionSlot {
                registration,
                receipt,
                #[cfg(test)]
                abort: _abort,
            },
        )
    }

    pub(crate) fn resources(&mut self) -> &mut DeliveryLifecycleResources {
        self.resources
            .as_mut()
            .expect("delivery owner already closed")
    }

    pub(crate) fn context(&self) -> ProcessContext {
        self.resources
            .as_ref()
            .expect("delivery owner already closed")
            .context
            .clone()
    }

    pub(crate) fn record_interruption(&mut self, reason: DeliveryTerminationReason) {
        self.resources().interruption = Some(reason);
    }

    pub(crate) async fn close(&mut self) -> Result<(), QueueHandlerError> {
        let resources = self.resources.as_mut().expect("delivery owner closes once");
        let execution_terminal = resources.execution.terminal();
        if !execution_terminal {
            self.tracker.mark_ownership_lost();
        }
        let context = resources.context.clone();
        let deadline = resources.cleanup_cutoff(&self.tracker.budget);
        let result =
            ProcessContext::scope(context, self.tracker.close_resources(resources, deadline)).await;
        let resources = self.resources.take();
        let released = catch_unwind(AssertUnwindSafe(|| drop(resources))).is_ok();
        if result.is_err() || !released {
            self.tracker.mark_failed();
        }
        self.tracker.finish();
        if !execution_terminal {
            return Err(QueueHandlerError::retryable(
                "QUEUE_DELIVERY_EXECUTION_TERMINATION_UNCONFIRMED",
            ));
        }
        if !released {
            return Err(QueueHandlerError::retryable(
                "QUEUE_DELIVERY_RESOURCE_RELEASE_PANICKED",
            ));
        }
        result
    }
}

impl Drop for DeliveryLifecycleOwner {
    fn drop(&mut self) {
        let Some(mut resources) = self.resources.take() else {
            return;
        };
        resources.ledger.interrupt_exit();
        resources.interruption.get_or_insert_with(|| {
            resources.cancellation.reason().map_or(
                DeliveryTerminationReason::OwnerDropped,
                DeliveryTerminationReason::ExecutionCancelled,
            )
        });
        if !resources.execution.terminal() {
            self.tracker.mark_ownership_lost();
        }
        let deadline = self.tracker.cap(if resources.cancellation.is_cancelled() {
            resources
                .deadline
                .min(Instant::now() + ABANDONED_SCOPE_CLEANUP_CAP)
        } else {
            resources.deadline
        });
        // A cancelled close waiter can shorten, but never restart, its tail.
        resources.cleanup_deadline = Some(
            resources
                .cleanup_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
        // Construct the guard before spawning, so even runtime rejection or a
        // never-polled cleanup task cannot be reported as reconciled cleanup.
        let mut observer = DeliveryScopeCleanupObserver::new(Arc::clone(&self.tracker));
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let context = resources.context.clone();
        let component = lily_trace::current_component();
        let tracker = Arc::clone(&self.tracker);
        let cleanup = async move {
            let result = tracker.close_resources(&mut resources, deadline).await;
            if let Err(error) = &result {
                tracing::warn!(
                    error_code = error.code(),
                    "Transferred delivery scope cleanup failed"
                );
            }
            let released = catch_unwind(AssertUnwindSafe(|| drop(resources))).is_ok();
            observer.complete(result.is_ok() && released);
        };
        self.tracker.adopt_cleanup(
            &runtime,
            lily_trace::scope_component(component, ProcessContext::scope(context, cleanup))
                .instrument(tracing::Span::current()),
        );
    }
}

#[cfg(test)]
#[path = "delivery_lifecycle_tests.rs"]
mod tests;
