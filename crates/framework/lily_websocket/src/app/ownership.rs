//! Execution can be replaced without dropping its lifecycle owner. Receipts
//! belong to registries; dropping a connection's receiver cannot detach work.

use super::*;
use crate::lifecycle::{
    LifecycleInterruption, LifecycleInvocation, LifecycleInvocationState, LifecycleOutcome,
    LifecycleStopRequest,
};
use futures_util::future::{AbortHandle, AbortRegistration, Abortable, BoxFuture, Shared};
use lily_injection::{ApplicationScope, InjectionError};

fn lock<T>(value: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Owns the admission/open/connected execution and both transport halves.
/// Cleanup keeps the receipt outside this future. A ready return or task abort
/// cannot publish session termination until the owned future has been dropped.
pub(super) struct ConnectionSession<F> {
    future: Option<std::pin::Pin<Box<F>>>,
    termination: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<F> ConnectionSession<F> {
    pub(super) fn new(future: F, termination: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            future: Some(Box::pin(future)),
            termination: Some(termination),
        }
    }

    fn finish(&mut self) {
        // A destructor panic closes the sender without a success value. The
        // receiver reports that as unconfirmed termination, never completion.
        let termination = self.termination.take();
        drop(self.future.take());
        if let Some(termination) = termination {
            let _ = termination.send(());
        }
    }
}

impl<F: Future> Future for ConnectionSession<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let result = this
            .future
            .as_mut()
            .expect("session polled after completion")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            this.finish();
        }
        result
    }
}

impl<F> Drop for ConnectionSession<F> {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Clone)]
pub(super) struct ExecutionControl {
    #[cfg(test)]
    abort: AbortHandle,
    cancellation: crate::ExecutionCancellation,
    record: Arc<StdMutex<LifecycleInvocation>>,
}

pub(super) struct ExecutionSlot {
    registration: Option<AbortRegistration>,
    cancellation: crate::ExecutionCancellation,
    cleanup: MessageCleanupObservation,
    record: Arc<StdMutex<LifecycleInvocation>>,
}

#[derive(Clone)]
pub(super) struct MessageCleanupObservation {
    budget: crate::shutdown::ShutdownBudget,
    failures: Arc<AtomicUsize>,
    ledger: Arc<std::sync::OnceLock<crate::reporting::LedgerCounts>>,
    pipeline: Arc<std::sync::OnceLock<Option<WsMessageOutcome>>>,
    pub(super) output: Option<super::message_reporting::OutputObservation>,
}

impl MessageCleanupObservation {
    pub(super) fn pipeline_result(&self, outcome: Option<WsMessageOutcome>) {
        assert!(
            self.pipeline.set(outcome).is_ok(),
            "one observed pipeline result"
        );
    }
    pub(super) fn record(&self, ledger: crate::reporting::LedgerCounts) {
        assert!(
            self.ledger.set(ledger).is_ok(),
            "one message ledger observation per owner"
        );
    }

    pub(super) fn failed(&self, count: usize) {
        // Retain shutdown failures even when the owner joins and its registry
        // entry disappears. Earlier ordinary request failures are not replayed
        // as failures of a later, unrelated shutdown.
        if self.budget.hard_deadline().is_some() {
            self.failures.fetch_add(count, Ordering::AcqRel);
        }
    }
}

pub(super) enum ExecutionExit<T> {
    Completed(T),
    /// The normal pipeline did not return before its local cooperative cutoff.
    /// This is termination evidence, not a cancellation request or user result.
    TimedOut,
    Aborted,
    Panicked,
}

impl ExecutionControl {
    fn request_cancel(&self) {
        let mut record = lock(&self.record);
        if !matches!(record.state, LifecycleInvocationState::Terminal { .. }) {
            record.request_stop(LifecycleStopRequest::Cancellation);
            drop(record);
            self.cancellation.request_cancel();
        }
    }

    #[cfg(test)]
    fn request_abort(&self) -> bool {
        let mut record = lock(&self.record);
        if matches!(record.state, LifecycleInvocationState::Terminal { .. })
            || record.abort_requested
        {
            return false;
        }
        record.request_stop(LifecycleStopRequest::Abort);
        self.abort.abort();
        true
    }

    fn aborted(&self) -> bool {
        matches!(
            lock(&self.record).state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted),
                ..
            }
        )
    }
}

impl ExecutionSlot {
    #[cfg(test)]
    pub(super) fn new() -> (ExecutionControl, Self) {
        Self::with_cancellation(CancellationToken::new(), Default::default())
    }

    pub(super) fn with_cancellation(
        cancellation: CancellationToken,
        budget: crate::shutdown::ShutdownBudget,
    ) -> (ExecutionControl, Self) {
        let (_abort, registration) = AbortHandle::new_pair();
        let cancellation = crate::ExecutionCancellation::for_message(cancellation, budget.clone());
        let mut invocation = LifecycleInvocation::default();
        assert!(invocation.claim());
        let record = Arc::new(StdMutex::new(invocation));
        (
            ExecutionControl {
                #[cfg(test)]
                abort: _abort,
                cancellation: cancellation.clone(),
                record: Arc::clone(&record),
            },
            Self {
                registration: Some(registration),
                cancellation,
                cleanup: MessageCleanupObservation {
                    budget,
                    failures: Default::default(),
                    ledger: Default::default(),
                    pipeline: Default::default(),
                    output: None,
                },
                record,
            },
        )
    }

    pub(super) fn cleanup_observation(&self) -> MessageCleanupObservation {
        self.cleanup.clone()
    }

    pub(super) fn message_cancellation(&self, deadline: Instant) -> crate::ExecutionCancellation {
        self.cancellation.set_message_deadline(deadline);
        self.cancellation.clone()
    }

    #[cfg(test)]
    pub(super) fn test_shutdown_budget(&self) -> crate::shutdown::ShutdownBudget {
        self.cleanup.budget.clone()
    }

    #[cfg(test)]
    pub(super) async fn run<F: Future>(self, future: F) -> ExecutionExit<F::Output> {
        self.run_with_deadline(None, future).await
    }

    pub(super) async fn run_until<F: Future>(
        self,
        deadline: tokio::time::Instant,
        future: F,
    ) -> ExecutionExit<F::Output> {
        self.cancellation.set_message_deadline(deadline);
        self.run_with_deadline(Some(deadline), future).await
    }

    async fn run_with_deadline<F: Future>(
        mut self,
        deadline: Option<tokio::time::Instant>,
        future: F,
    ) -> ExecutionExit<F::Output> {
        let registration = self.registration.take().expect("execution slot runs once");
        let record = Arc::clone(&self.record);
        // This block must end before publishing termination: Abortable owns
        // and drops only the execution future, including its mutable borrows.
        let result = {
            let invocation = AssertUnwindSafe(async {
                assert!(lock(&record).start());
                future.await
            })
            .catch_unwind();
            let mut invocation = Box::pin(Abortable::new(invocation, registration));
            let result = tokio::select! {
                biased;
                () = async {
                    tokio::select! {
                        biased;
                        () = self.cancellation.cancelled() => {},
                        () = self.cleanup.budget.execution_limit_expired() => self.cancellation.request_cancel(),
                    }
                    lock(&record).request_stop(LifecycleStopRequest::Cancellation);
                    self.cancellation.execution_stopped(deadline).await;
                } => {
                    lock(&record).request_stop(LifecycleStopRequest::Abort);
                    Err(futures_util::future::Aborted)
                }
                result = &mut invocation => result,
            };
            // Cancellation may run application destructors. A panic while
            // dropping the slot must not take its retained ledger with it.
            match catch_unwind(AssertUnwindSafe(|| drop(invocation))) {
                Ok(()) => result,
                Err(panic) => Ok(Err(panic)),
            }
        };
        self.cancellation.finish_message();
        let cancellation = self.cancellation.message_facts().expect("message facts");
        if cancellation.request.is_some() {
            lock(&record).request_stop(LifecycleStopRequest::Cancellation);
        }
        let (outcome, result) = match result {
            Ok(Ok(value)) => (LifecycleOutcome::Completed, ExecutionExit::Completed(value)),
            Ok(Err(_)) => (LifecycleOutcome::Panicked, ExecutionExit::Panicked),
            Err(_)
                if cancellation.request.is_some_and(|request| {
                    request.cause == crate::extractor::MessageCancellationCause::MessageTimeout
                }) =>
            {
                (
                    LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
                    ExecutionExit::TimedOut,
                )
            }
            Err(_) => (
                LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted),
                ExecutionExit::Aborted,
            ),
        };
        assert!(lock(&record).finish(outcome));
        result
    }
}

impl Drop for ExecutionSlot {
    fn drop(&mut self) {
        self.cancellation.finish_message();
        let mut record = lock(&self.record);
        if !matches!(record.state, LifecycleInvocationState::Terminal { .. }) {
            // An owner can fail scope creation before ever polling execution.
            // This is a stopped slot, not a completed invocation or owner join.
            assert!(record.finish(LifecycleOutcome::Interrupted(
                LifecycleInterruption::Cancelled
            )));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerJoinStatus {
    Completed,
    Cancelled,
    Panicked,
}

type OwnerJoinReceipt = Shared<BoxFuture<'static, OwnerJoinStatus>>;

#[derive(Debug)]
pub(super) enum MessageOwnerFailure {
    OutputLost,
    Cancelled,
    Panicked,
}

type MessageDispatchReceipt<T> = BoxFuture<'static, Result<T, MessageOwnerFailure>>;

struct MessageTaskEntry {
    connection_id: Uuid,
    execution: ExecutionControl,
    join: OwnerJoinReceipt,
    ledger: Arc<std::sync::OnceLock<crate::reporting::LedgerCounts>>,
    pipeline: Arc<std::sync::OnceLock<Option<WsMessageOutcome>>>,
}

#[derive(Default)]
struct MessageRegistryInner {
    entries: StdMutex<HashMap<Uuid, Arc<MessageTaskEntry>>>,
    panicked: AtomicUsize,
    cleanup_failures: Arc<AtomicUsize>,
    retired: StdMutex<MessageAccounting>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct MessageAccounting {
    pub(super) owners: usize,
    pub(super) joined: usize,
    pub(super) join_cancelled: usize,
    pub(super) join_panicked: usize,
    pub(super) outstanding: usize,
    pub(super) ledgers_unobserved: usize,
    pub(super) execution: crate::reporting::InvocationCounts,
    pub(super) deadline_exceeded: usize,
    pub(super) completed_after_cancellation: usize,
    pub(super) completed_after_deadline: usize,
    pub(super) timeout_cancellation: usize,
    pub(super) connection_cancellation: usize,
    pub(super) pipeline: super::message_reporting::PipelineCounts,
    pub(super) output: super::message_reporting::OutputCounts,
    pub(super) middleware: crate::reporting::LedgerCounts,
}

impl MessageAccounting {
    pub(super) fn reconciles(self) -> bool {
        self.owners == self.joined + self.join_cancelled + self.join_panicked + self.outstanding
            && self.execution.total == self.owners
            && self.execution.reconciles()
            && self.middleware.reconciles()
            && self.pipeline.total() == self.owners
            && self.pipeline.returned() <= self.execution.completed
            && self.output.total == self.owners
            && self.output.reconciles()
            && self.timeout_cancellation + self.connection_cancellation
                <= self.execution.cancellation_requested
            && self.completed_after_deadline <= self.deadline_exceeded
            && self.completed_after_deadline <= self.completed_after_cancellation
            && self.completed_after_cancellation <= self.execution.completed
            && self.completed_after_cancellation <= self.execution.cancellation_requested
            && self.completed_after_deadline + self.execution.timed_out <= self.deadline_exceeded
            && self.timeout_cancellation <= self.deadline_exceeded
    }
    fn record(&mut self, entry: &MessageTaskEntry, join: Option<OwnerJoinStatus>) {
        self.owners += 1;
        match join {
            Some(OwnerJoinStatus::Completed) => self.joined += 1,
            Some(OwnerJoinStatus::Cancelled) => self.join_cancelled += 1,
            Some(OwnerJoinStatus::Panicked) => self.join_panicked += 1,
            None => self.outstanding += 1,
        }
        let mut record = *lock(&entry.execution.record);
        if let Some(facts) = entry.execution.cancellation.recorded_message_facts() {
            if let Some(request) = facts.request {
                // Callback observation may precede the slot's next poll.
                record.cancellation_requested = true;
                match request.cause {
                    crate::extractor::MessageCancellationCause::MessageTimeout => {
                        self.timeout_cancellation += 1
                    }
                    crate::extractor::MessageCancellationCause::ConnectionCancelled => {
                        self.connection_cancellation += 1
                    }
                }
            }
            self.deadline_exceeded += usize::from(facts.deadline_exceeded);
            let completed = matches!(
                record.state,
                LifecycleInvocationState::Terminal {
                    outcome: LifecycleOutcome::Completed,
                    ..
                }
            );
            self.completed_after_cancellation += usize::from(completed && facts.request.is_some());
            self.completed_after_deadline += usize::from(completed && facts.deadline_exceeded);
        }
        self.execution.record(record);
        self.pipeline.record(entry.pipeline.get().copied());
        if let Some(ledger) = entry.ledger.get() {
            self.middleware.merge(*ledger);
        } else {
            self.ledgers_unobserved += 1;
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct MessageDispatchRegistry {
    pub(super) budget: crate::shutdown::ShutdownBudget,
    inner: Arc<MessageRegistryInner>,
    tracker: TaskTracker,
    driver_joins: TaskRegistry,
    output: super::message_reporting::OutputRegistry,
    #[cfg(test)]
    dispatch_admissions: Arc<AtomicUsize>,
    #[cfg(test)]
    scope_execution_attempts: Arc<AtomicUsize>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MessageDispatchTestSnapshot {
    pub(super) dispatch_admissions: usize,
    pub(super) scope_execution_attempts: usize,
    pub(super) active_tasks: usize,
    pub(super) registered_abort_handles: usize,
}

#[derive(Debug, Default)]
pub(super) struct MessageDispatchDrain {
    pub(super) pending: usize,
    pub(super) abort_requested: usize,
    pub(super) aborted: usize,
    pub(super) panicked: usize,
    pub(super) owner_join_cancelled: usize,
    pub(super) timed_out: bool,
    pub(super) forced: bool,
    pub(super) outstanding: usize,
    pub(super) cleanup_failures: usize,
}

impl MessageDispatchRegistry {
    pub(super) fn with_budget(budget: crate::shutdown::ShutdownBudget) -> Self {
        Self {
            budget,
            ..Default::default()
        }
    }
    #[cfg(test)]
    pub(super) fn spawn_owner<F, T>(
        &self,
        connection_id: Uuid,
        make_owner: impl FnOnce(ExecutionSlot) -> F + Send + 'static,
    ) -> MessageDispatchReceipt<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_owner_with_cancellation(connection_id, CancellationToken::new(), make_owner)
    }

    pub(super) fn spawn_owner_with_cancellation<F, T>(
        &self,
        connection_id: Uuid,
        cancellation: CancellationToken,
        make_owner: impl FnOnce(ExecutionSlot) -> F + Send + 'static,
    ) -> MessageDispatchReceipt<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(test)]
        self.dispatch_admissions.fetch_add(1, Ordering::AcqRel);
        let id = Uuid::new_v4();
        let (execution, mut slot) =
            ExecutionSlot::with_cancellation(cancellation, self.budget.clone());
        slot.cleanup.failures = Arc::clone(&self.inner.cleanup_failures);
        let ledger = slot.cleanup.ledger.clone();
        let pipeline = slot.cleanup.pipeline.clone();
        // Registration and the output count become visible under the same
        // entry lock before first poll; snapshots cannot invent extra outputs.
        let mut entries = lock(&self.inner.entries);
        slot.cleanup.output = Some(self.output.observe());
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let (output_tx, output_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = start_rx.await;
            let output = make_owner(slot).await;
            let _ = output_tx.send(output);
        });
        // The shared receipt owns the real JoinHandle. Sending output or
        // requesting a slot abort does not constitute an observed owner join.
        let join = async move {
            match task.await {
                Ok(()) => OwnerJoinStatus::Completed,
                Err(error) if error.is_cancelled() => OwnerJoinStatus::Cancelled,
                Err(_) => OwnerJoinStatus::Panicked,
            }
        }
        .boxed()
        .shared();
        let entry = Arc::new(MessageTaskEntry {
            connection_id,
            execution,
            join: join.clone(),
            ledger,
            pipeline,
        });
        entries.insert(id, entry);
        drop(entries);
        let inner = Arc::downgrade(&self.inner);
        // Receipt drivers are tracked too, including when every receiver is
        // gone. No driver executes application code or owns an abort authority.
        let driver_join = join.clone();
        self.driver_joins.track(self.tracker.spawn(async move {
            let status = driver_join.await;
            if let Some(inner) = inner.upgrade() {
                if status == OwnerJoinStatus::Panicked {
                    inner.panicked.fetch_add(1, Ordering::AcqRel);
                }
                // Retire evidence under the same lock as removal. A snapshot
                // sees each owner exactly once, including concurrent joins.
                let mut entries = lock(&inner.entries);
                if let Some(entry) = entries.remove(&id) {
                    lock(&inner.retired).record(&entry, Some(status));
                }
            }
        }));
        let _ = start_tx.send(());
        // Output availability is not termination evidence. Keep the exact
        // join receipt in both the registry and receiver, even if either
        // waiter is dropped. A destructor panic cannot publish a usable result.
        async move {
            let output = output_rx.await;
            match join.await {
                OwnerJoinStatus::Completed => output.map_err(|_| MessageOwnerFailure::OutputLost),
                OwnerJoinStatus::Cancelled => Err(MessageOwnerFailure::Cancelled),
                OwnerJoinStatus::Panicked => Err(MessageOwnerFailure::Panicked),
            }
        }
        .boxed()
    }

    pub(super) async fn wait_connection(&self, connection_id: Uuid) {
        let entries = lock(&self.inner.entries)
            .values()
            .filter(|entry| entry.connection_id == connection_id)
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let _ = entry.join.clone().await;
        }
    }

    #[cfg(test)]
    pub(super) fn record_scope_execution_attempt(&self) {
        self.scope_execution_attempts.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn test_snapshot(&self) -> MessageDispatchTestSnapshot {
        MessageDispatchTestSnapshot {
            dispatch_admissions: self.dispatch_admissions.load(Ordering::Acquire),
            scope_execution_attempts: self.scope_execution_attempts.load(Ordering::Acquire),
            active_tasks: self.tracker.len(),
            registered_abort_handles: lock(&self.inner.entries).len(),
        }
    }

    #[cfg(test)]
    pub(super) async fn drain_until(
        &self,
        deadline: Instant,
        force: &CancellationToken,
    ) -> MessageDispatchDrain {
        self.drain_with_force(Some(deadline), force).await
    }

    pub(super) async fn drain_with_force(
        &self,
        deadline: Option<Instant>,
        force: &CancellationToken,
    ) -> MessageDispatchDrain {
        self.tracker.close();
        let entries = lock(&self.inner.entries)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut report = MessageDispatchDrain {
            pending: entries.len(),
            ..Default::default()
        };
        if !entries.is_empty() {
            let deadline_signal = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = force.cancelled() => report.forced = true,
                () = deadline_signal => report.timed_out = true,
                () = self.tracker.wait() => {}
            }
            if report.forced || report.timed_out {
                if !self.budget.is_forced() {
                    // Standalone registry callers also get a bounded policy;
                    // production always publishes the composition-root cap.
                    self.budget.force_before(
                        self.budget
                            .hard_deadline()
                            .unwrap_or_else(|| Instant::now() + Duration::from_millis(500)),
                    );
                }
                for entry in &entries {
                    entry.execution.request_cancel();
                }
            }
            for entry in &entries {
                if let Ok(joined) = self.budget.reconcile(entry.join.clone()).await {
                    report.owner_join_cancelled +=
                        usize::from(joined == OwnerJoinStatus::Cancelled);
                }
                report.abort_requested +=
                    usize::from(lock(&entry.execution.record).abort_requested);
                report.aborted += usize::from(entry.execution.aborted());
            }
        }
        let _ = self.budget.reconcile(self.tracker.wait()).await;
        let _ = self.budget.reconcile(self.driver_joins.wait()).await;
        report.outstanding = lock(&self.inner.entries).len();
        report.panicked = self.inner.panicked.load(Ordering::Acquire);
        report.cleanup_failures = self.inner.cleanup_failures.load(Ordering::Acquire);
        report
    }

    pub(super) async fn reconcile(&self) -> bool {
        self.tracker.close();
        let _ = self.budget.reconcile(self.driver_joins.wait()).await;
        self.is_terminal()
    }

    pub(super) fn is_terminal(&self) -> bool {
        self.driver_joins.snapshot().outstanding == 0
            && lock(&self.inner.entries).is_empty()
            && self.output.snapshot().outstanding == 0
    }

    pub(super) fn cleanup_failures(&self) -> usize {
        self.inner.cleanup_failures.load(Ordering::Acquire)
    }

    pub(super) fn owner_panics(&self) -> usize {
        self.inner.panicked.load(Ordering::Acquire)
    }

    pub(super) fn accounting(&self) -> (MessageAccounting, crate::tasks::TaskSnapshot) {
        let drivers = self.driver_joins.snapshot();
        let entries = lock(&self.inner.entries);
        let mut counts = *lock(&self.inner.retired);
        for entry in entries.values() {
            counts.record(entry, entry.join.peek().copied());
        }
        counts.output = self.output.snapshot();
        (counts, drivers)
    }
}

type ScopeReceipt = Shared<BoxFuture<'static, ()>>;

#[derive(Clone, Default)]
pub(super) struct ScopeCleanupRegistry {
    pub(super) budget: crate::shutdown::ShutdownBudget,
    deadline_failures: Arc<AtomicUsize>,
    entries: Arc<StdMutex<HashMap<Uuid, (Uuid, ScopeReceipt)>>>,
    drivers: TaskTracker,
    driver_joins: TaskRegistry,
    observed: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct ScopeCleanupDrain {
    pub(super) outstanding: usize,
    pub(super) deadline_failures: usize,
}

/// Scope disposal remains owned by DI. This registry retains generation-bound
/// termination evidence even when a handshake/connection execution is dropped.
impl ScopeCleanupRegistry {
    pub(super) fn with_budget(budget: crate::shutdown::ShutdownBudget) -> Self {
        Self {
            budget,
            ..Default::default()
        }
    }
    pub(super) fn create_scope(
        &self,
        connection_id: Uuid,
        container: &ApplicationContainer,
        context: ProcessContext,
    ) -> Result<ApplicationScope, InjectionError> {
        let scope = container.create_scope(context)?;
        let scope_id = scope.context().process_id_string();
        let observation = lily_injection::__private::observe_scope_cleanup(container, &scope_id)
            .ok_or(InjectionError::ScopeClosed { scope_id })?;
        let budget = self.budget.clone();
        let deadline_failures = Arc::clone(&self.deadline_failures);
        let receipt = async move {
            let observed =
                lily_injection::__private::wait_for_observed_scope_cleanup(observation.clone());
            if budget.observe_cleanup(observed).await.is_err() {
                // DI requests disposal abort and observes the exact generation.
                // This receipt remains owned if the root wait itself expires.
                if lily_injection::__private::wait_for_observed_scope_cleanup_before(
                    observation,
                    Instant::now(),
                )
                .await
                .is_err()
                {
                    deadline_failures.fetch_add(1, Ordering::AcqRel);
                }
            }
        }
        .boxed()
        .shared();
        let id = Uuid::new_v4();
        lock(&self.entries).insert(id, (connection_id, receipt.clone()));
        let entries = Arc::downgrade(&self.entries);
        let observed = self.observed.clone();
        self.driver_joins.track(self.drivers.spawn(async move {
            receipt.await;
            if let Some(entries) = entries.upgrade() {
                let mut entries = lock(&entries);
                if entries.remove(&id).is_some() {
                    observed.fetch_add(1, Ordering::AcqRel);
                }
            }
        }));
        Ok(scope)
    }

    pub(super) async fn run_scoped<F: Future>(
        &self,
        connection_id: Uuid,
        container: &ApplicationContainer,
        context: ProcessContext,
        future: F,
    ) -> Result<F::Output, InjectionError> {
        let mut scope = self.create_scope(connection_id, container, context)?;
        let result = scope.run(future).await?;
        let scope_id = scope.context().process_id_string();
        {
            // Keep both the original close future and its result (which owns
            // the message ledger) through DI abort/termination reconciliation.
            // Reopening close() would falsely succeed on an already-closed scope.
            let close = scope.close();
            tokio::pin!(close);
            match self.budget.cleanup(None, close.as_mut()).await {
                Ok(result) => result?,
                Err(()) => {
                    let _ = self.budget.reconcile(close.as_mut()).await;
                    return Err(InjectionError::ScopeCleanupTimedOut { scope_id });
                }
            }
        }
        Ok(result)
    }

    pub(super) async fn wait_connection(&self, connection_id: Uuid) {
        let receipts = lock(&self.entries)
            .values()
            .filter(|(owner, _)| *owner == connection_id)
            .map(|(_, receipt)| receipt.clone())
            .collect::<Vec<_>>();
        for receipt in receipts {
            receipt.await;
        }
    }

    pub(super) async fn drain(&self) -> ScopeCleanupDrain {
        self.drivers.close();
        let _ = self.budget.reconcile(self.drivers.wait()).await;
        let _ = self.budget.reconcile(self.driver_joins.wait()).await;
        ScopeCleanupDrain {
            outstanding: lock(&self.entries).len(),
            deadline_failures: self.deadline_failures.load(Ordering::Acquire),
        }
    }

    pub(super) fn is_terminal(&self) -> bool {
        self.driver_joins.snapshot().outstanding == 0 && lock(&self.entries).is_empty()
    }

    pub(super) fn accounting(&self) -> (usize, usize, usize, crate::tasks::TaskSnapshot) {
        let drivers = self.driver_joins.snapshot();
        let entries = lock(&self.entries);
        (
            self.observed.load(Ordering::Acquire),
            entries.len(),
            self.deadline_failures.load(Ordering::Acquire),
            drivers,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SessionProbe {
        dropped: Arc<std::sync::atomic::AtomicBool>,
        ready: bool,
        panic_on_drop: bool,
    }

    impl Future for SessionProbe {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.ready {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }
    }

    impl Drop for SessionProbe {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "intentional session destructor panic");
        }
    }

    #[tokio::test]
    async fn session_receipt_requires_future_drop_including_ready_and_unpolled_sessions() {
        for ready in [false, true] {
            for polled in [false, true] {
                let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let (tx, mut rx) = tokio::sync::oneshot::channel();
                let mut session = Box::pin(ConnectionSession::new(
                    SessionProbe {
                        dropped: dropped.clone(),
                        ready,
                        panic_on_drop: false,
                    },
                    tx,
                ));
                assert!(matches!(
                    rx.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
                if polled {
                    assert_eq!(futures_util::poll!(session.as_mut()).is_ready(), ready);
                    assert_eq!(dropped.load(Ordering::SeqCst), ready);
                }
                drop(session);
                rx.await.unwrap();
                assert!(dropped.load(Ordering::SeqCst));
            }
        }
    }

    #[tokio::test]
    async fn session_destructor_panic_cannot_publish_a_success_receipt_on_drop_retry() {
        for ready in [false, true] {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let session = ConnectionSession::new(
                SessionProbe {
                    dropped: dropped.clone(),
                    ready,
                    panic_on_drop: true,
                },
                tx,
            );
            if ready {
                assert!(AssertUnwindSafe(session).catch_unwind().await.is_err());
            } else {
                assert!(catch_unwind(AssertUnwindSafe(|| drop(session))).is_err());
            }
            assert!(
                rx.await.is_err(),
                "a panic must not turn into confirmed transport termination"
            );
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn session_abort_request_is_not_a_receipt_or_a_join_result() {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = tokio::spawn(ConnectionSession::new(
            SessionProbe {
                dropped: dropped.clone(),
                ready: false,
                panic_on_drop: false,
            },
            tx,
        ));
        tokio::task::yield_now().await;
        task.abort();
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(task.await.unwrap_err().is_cancelled());
        rx.await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }
    use std::future::pending;
    use std::task::Poll;

    #[tokio::test(start_paused = true)]
    async fn local_deadline_signals_then_confirms_slot_drop_after_cooperation() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let resource = Dropped(dropped.clone());
        let (control, slot) = ExecutionSlot::new();
        let started = Instant::now();
        let exit = slot
            .run_until(Instant::now() + Duration::from_millis(20), async move {
                let _resource = resource;
                pending::<()>().await;
            })
            .await;
        assert!(matches!(exit, ExecutionExit::TimedOut));
        assert!(dropped.load(Ordering::SeqCst));
        let record = *lock(&control.record);
        assert_eq!(
            record.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
                started: true,
            }
        );
        assert!(record.cancellation_requested);
        assert!(record.abort_requested);
        assert_eq!(started.elapsed(), Duration::from_millis(270));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_force_races_keep_the_first_cause_and_never_restart_cooperation() {
        for (force_at, expected_end, local_wins) in
            [(80, 110, false), (100, 125, false), (120, 140, true)]
        {
            for cooperates in [true, false] {
                let budget = crate::shutdown::ShutdownBudget::default();
                let parent = CancellationToken::new();
                let (control, slot) =
                    ExecutionSlot::with_cancellation(parent.clone(), budget.clone());
                let started = Instant::now();
                let signal = slot.message_cancellation(started + Duration::from_millis(100));
                let returned = if force_at == 120 { 30 } else { 5 };
                let mut ledger = vec!["entered"];
                let execution = slot.run_until(started + Duration::from_millis(100), async {
                    signal.cancelled().await;
                    if cooperates {
                        tokio::time::sleep(Duration::from_millis(returned)).await;
                    } else {
                        pending::<()>().await;
                    }
                    ledger.push("normal exit");
                    "actual result"
                });
                let force = async {
                    tokio::time::sleep_until(started + Duration::from_millis(force_at)).await;
                    budget.force_before(started + Duration::from_millis(200));
                    parent.cancel();
                    // A later, more generous publication must not extend the stop.
                    budget.force_before(started + Duration::from_secs(20));
                };
                // Explicitly make both signals observable before the slot's
                // poll in the equal-deadline case; this tests the tie contract.
                let ((), result) = tokio::join!(biased; force, execution);
                let facts = signal.recorded_message_facts().unwrap();
                assert_eq!(
                    facts.request.unwrap().cause,
                    if local_wins {
                        crate::extractor::MessageCancellationCause::MessageTimeout
                    } else {
                        crate::extractor::MessageCancellationCause::ConnectionCancelled
                    }
                );
                assert!(facts.terminal);
                if cooperates {
                    assert!(matches!(result, ExecutionExit::Completed("actual result")));
                    assert!(!lock(&control.record).abort_requested);
                    assert_eq!(ledger, ["entered", "normal exit"]);
                } else {
                    assert!(matches!(result, ExecutionExit::TimedOut) == local_wins);
                    assert_eq!(matches!(result, ExecutionExit::Aborted), !local_wins);
                    assert_eq!(started.elapsed(), Duration::from_millis(expected_end));
                    assert!(lock(&control.record).abort_requested);
                    assert_eq!(ledger, ["entered"]);
                    ledger.push("termination exit");
                }
                assert!(lock(&control.record).cancellation_requested);
                // The owner still has exclusive lifecycle state after slot drop.
                ledger.push("scope cleanup");
                assert_eq!(ledger.last(), Some(&"scope cleanup"));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_cooperative_deadline_does_not_start_fresh_user_execution() {
        let (control, slot) = ExecutionSlot::new();
        let exit = slot
            .run_until(Instant::now() - Duration::from_millis(250), async {
                panic!("expired execution must not be polled");
            })
            .await;
        assert!(matches!(exit, ExecutionExit::TimedOut));
        assert_eq!(
            lock(&control.record).state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
                started: false,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_request_does_not_replace_a_completed_execution_result() {
        let (control, slot) = ExecutionSlot::new();
        control.request_cancel();
        let exit = slot
            .run_until(Instant::now() + Duration::from_secs(1), async {
                Err::<(), _>("application result")
            })
            .await;
        assert!(matches!(
            exit,
            ExecutionExit::Completed(Err("application result"))
        ));
        let record = *lock(&control.record);
        assert!(record.cancellation_requested);
        assert!(!record.abort_requested);
        assert_eq!(
            record.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Completed,
                started: true,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn forced_execution_can_observe_cancellation_and_return_before_slot_abort() {
        let budget = crate::shutdown::ShutdownBudget::default();
        let start = Instant::now();
        budget.configure(
            start + Duration::from_secs(1),
            start + Duration::from_secs(2),
        );
        budget.force_before(start + Duration::from_millis(400));
        let registry = MessageDispatchRegistry::with_budget(budget);
        let source = CancellationToken::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let output = registry.spawn_owner_with_cancellation(
            Uuid::new_v4(),
            source,
            move |slot| async move {
                let mut ledger = Vec::new();
                let signal = slot.message_cancellation(start + Duration::from_secs(1));
                let exit = slot
                    .run(async {
                        ledger.push("enter");
                        started_tx.send(()).unwrap();
                        signal.cancelled().await;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        ledger.push("cooperative return");
                    })
                    .await;
                assert!(matches!(exit, ExecutionExit::Completed(())));
                ledger.push("unwind");
                ledger
            },
        );
        started_rx.await.unwrap();
        let force = CancellationToken::new();
        force.cancel();
        let report = registry.drain_with_force(None, &force).await;
        assert_eq!(
            output.await.unwrap(),
            ["enter", "cooperative return", "unwind"]
        );
        assert_eq!(report.abort_requested, 0);
        assert_eq!(report.aborted, 0);
        assert_eq!(report.outstanding, 0);
        assert_eq!(report.owner_join_cancelled, 0);
        assert!(Instant::now() <= start + Duration::from_millis(21));
    }

    #[tokio::test(start_paused = true)]
    async fn root_expiry_reports_an_outstanding_owner_without_aborting_cleanup() {
        let budget = crate::shutdown::ShutdownBudget::default();
        let now = Instant::now();
        budget.force_before(now + Duration::from_millis(40));
        let registry = MessageDispatchRegistry::with_budget(budget);
        let connection_id = Uuid::new_v4();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let output = registry.spawn_owner(connection_id, |slot| async move {
            assert!(matches!(
                slot.run(async {}).await,
                ExecutionExit::Completed(())
            ));
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
        });
        ready_rx.await.unwrap();
        let force = CancellationToken::new();
        force.cancel();
        let report = registry.drain_with_force(None, &force).await;
        assert_eq!(report.outstanding, 1);
        assert_eq!(report.abort_requested, 0);
        assert_eq!(report.owner_join_cancelled, 0);
        assert!(Instant::now() <= now + Duration::from_millis(41));
        release_tx.send(()).unwrap();
        output.await.unwrap();
        registry.wait_connection(connection_id).await;
        registry.tracker.wait().await;
        assert_eq!(registry.test_snapshot().active_tasks, 0);
    }

    struct DropFlag(Arc<AtomicUsize>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slot_drop_panic_is_contained_before_owner_unwind() {
        struct PanickingDrop;
        impl Drop for PanickingDrop {
            fn drop(&mut self) {
                panic!("execution destructor");
            }
        }
        let (control, slot) = ExecutionSlot::new();
        let mut ledger = Vec::new();
        {
            let execution = slot.run(async {
                let _resource = PanickingDrop;
                ledger.push("entered");
                pending::<()>().await;
            });
            tokio::pin!(execution);
            assert!(matches!(
                futures_util::poll!(execution.as_mut()),
                Poll::Pending
            ));
            control.request_cancel();
            assert!(matches!(
                futures_util::poll!(execution.as_mut()),
                Poll::Pending
            ));
            assert!(matches!(execution.await, ExecutionExit::Panicked));
        }
        ledger.push("owner cleanup");
        assert_eq!(ledger, ["entered", "owner cleanup"]);
        assert!(matches!(
            lock(&control.record).state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Panicked,
                started: true,
            }
        ));
    }

    #[tokio::test]
    async fn abort_request_does_not_confirm_slot_drop_or_invoke_unstarted_execution() {
        let (control, slot) = ExecutionSlot::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        let resource = DropFlag(Arc::clone(&dropped));
        let body = async move {
            let _resource = resource;
            panic!("aborted execution must not start");
        };
        assert!(control.request_abort());
        assert!(!control.aborted());
        assert_eq!(dropped.load(Ordering::Acquire), 0);
        assert!(matches!(slot.run(body).await, ExecutionExit::Aborted));
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert!(control.aborted());
        assert!(matches!(
            lock(&control.record).state,
            LifecycleInvocationState::Terminal { started: false, .. }
        ));
    }

    #[tokio::test]
    async fn slot_abort_releases_borrows_before_owner_cleanup() {
        let (control, slot) = ExecutionSlot::new();
        let mut ledger = Vec::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        {
            let execution = slot.run(async {
                let _resource = DropFlag(Arc::clone(&dropped));
                ledger.push("entered");
                pending::<()>().await;
            });
            tokio::pin!(execution);
            assert!(matches!(
                futures_util::poll!(execution.as_mut()),
                Poll::Pending
            ));
            assert!(control.request_abort());
            assert!(!control.aborted());
            assert_eq!(dropped.load(Ordering::Acquire), 0);
            assert!(matches!(execution.await, ExecutionExit::Aborted));
        }
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        ledger.push("cleanup");
        assert_eq!(ledger, ["entered", "cleanup"]);
        assert!(matches!(
            lock(&control.record).state,
            LifecycleInvocationState::Terminal { started: true, .. }
        ));
    }

    #[tokio::test]
    async fn slot_panic_keeps_owner_state_and_returned_slot_cannot_be_aborted_again() {
        let (control, slot) = ExecutionSlot::new();
        let mut ledger = Vec::new();
        let result = slot
            .run(async {
                ledger.push("entered");
                panic!("execution panic");
            })
            .await;
        assert!(matches!(result, ExecutionExit::Panicked));
        assert_eq!(ledger, ["entered"]);
        assert!(!control.request_abort());
        let (control, slot) = ExecutionSlot::new();
        assert!(matches!(
            slot.run(async { 42 }).await,
            ExecutionExit::Completed(42)
        ));
        assert!(!control.request_abort());
        assert!(!control.aborted());
    }

    #[tokio::test]
    async fn owner_is_registered_before_first_poll_and_scope_failure_can_skip_execution() {
        let registry = MessageDispatchRegistry::default();
        let connection_id = Uuid::new_v4();
        let inspect = registry.clone();
        let output = registry.spawn_owner(connection_id, move |_slot| async move {
            assert_eq!(lock(&inspect.inner.entries).len(), 1);
            // Models an owner whose DI scope could not be opened.
            "scope unavailable"
        });
        assert_eq!(output.await.unwrap(), "scope unavailable");
        registry.wait_connection(connection_id).await;
        let report = registry
            .drain_with_force(None, &CancellationToken::new())
            .await;
        assert_eq!(report.aborted, 0);
        assert_eq!(report.owner_join_cancelled, 0);
        assert_eq!(registry.test_snapshot().active_tasks, 0);
        assert_eq!(registry.test_snapshot().registered_abort_handles, 0);
    }

    #[tokio::test]
    async fn force_aborts_execution_but_waits_for_owner_cleanup_and_confirmed_join() {
        let registry = MessageDispatchRegistry::default();
        let connection_id = Uuid::new_v4();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let output = registry.spawn_owner(connection_id, move |slot| async move {
            let result = slot
                .run(async {
                    let _ = started_tx.send(());
                    pending::<()>().await
                })
                .await;
            assert!(matches!(result, ExecutionExit::Aborted));
            let _ = cleanup_tx.send(());
            release_rx.await.unwrap();
        });
        started_rx.await.unwrap();
        drop(output);
        let force = CancellationToken::new();
        force.cancel();
        let drain = registry.drain_with_force(None, &force);
        tokio::pin!(drain);
        assert!(matches!(futures_util::poll!(drain.as_mut()), Poll::Pending));
        cleanup_rx.await.unwrap();
        assert_eq!(registry.test_snapshot().active_tasks, 1);
        let connection_join = registry.wait_connection(connection_id);
        tokio::pin!(connection_join);
        assert!(matches!(
            futures_util::poll!(connection_join.as_mut()),
            Poll::Pending
        ));
        release_tx.send(()).unwrap();
        let report = drain.await;
        connection_join.await;
        assert_eq!(report.abort_requested, 1);
        assert_eq!(report.aborted, 1);
        assert_eq!(report.owner_join_cancelled, 0);
        assert_eq!(registry.test_snapshot().active_tasks, 0);
        assert_eq!(registry.test_snapshot().registered_abort_handles, 0);
    }

    #[tokio::test]
    async fn force_during_cleanup_cannot_abort_the_owner_or_another_connection() {
        let registry = MessageDispatchRegistry::default();
        let connection_id = Uuid::new_v4();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let output = registry.spawn_owner(connection_id, move |slot| async move {
            assert!(matches!(
                slot.run(async {}).await,
                ExecutionExit::Completed(())
            ));
            started_tx.send(()).unwrap();
            release_rx.await.unwrap();
            "cleanup returned"
        });
        started_rx.await.unwrap();
        // A different connection has no dependency on this cleanup.
        registry.wait_connection(Uuid::new_v4()).await;
        let force = CancellationToken::new();
        force.cancel();
        let drain = registry.drain_with_force(None, &force);
        tokio::pin!(drain);
        assert!(matches!(futures_util::poll!(drain.as_mut()), Poll::Pending));
        release_tx.send(()).unwrap();
        assert_eq!(output.await.unwrap(), "cleanup returned");
        let report = drain.await;
        assert_eq!(report.abort_requested, 0);
        assert_eq!(report.aborted, 0);
        assert_eq!(report.owner_join_cancelled, 0);
    }

    #[tokio::test]
    async fn completed_execution_with_owner_cleanup_panic_cannot_publish_a_successful_receipt() {
        struct CleanupPanic;
        impl Drop for CleanupPanic {
            fn drop(&mut self) {
                panic!("owner cleanup destructor");
            }
        }
        let registry = MessageDispatchRegistry::default();
        let output = registry.spawn_owner(Uuid::new_v4(), |slot| async move {
            let _cleanup = CleanupPanic;
            assert!(matches!(
                slot.run(async { 42 }).await,
                ExecutionExit::Completed(42)
            ));
            42
        });
        assert!(matches!(output.await, Err(MessageOwnerFailure::Panicked)));
        registry.reconcile().await;
        let (messages, tasks) = registry.accounting();
        assert_eq!(messages.execution.completed, 1);
        assert_eq!(messages.joined, 0);
        assert_eq!(messages.join_panicked, 1);
        assert_eq!(messages.outstanding, 0);
        assert_eq!(tasks.outstanding, 0);
    }

    #[tokio::test]
    async fn scope_receipt_survives_execution_drop_and_never_attaches_to_a_reused_id() {
        let container = ApplicationContainer::build().await.unwrap();
        let scopes = ScopeCleanupRegistry::default();
        let connection_id = Uuid::new_v4();
        let context = ProcessContext::new();
        let scope = scopes
            .create_scope(connection_id, &container, context.clone())
            .unwrap();
        let receipt = lock(&scopes.entries).values().next().unwrap().1.clone();
        let retained = receipt.clone();
        tokio::pin!(retained);
        assert!(matches!(
            futures_util::poll!(retained.as_mut()),
            Poll::Pending
        ));
        drop(scope);
        receipt.await;
        // Completion is bound to the old generation, even if the ID is reused.
        let mut replacement = container.create_scope(context).unwrap();
        retained.await;
        assert_eq!(container.active_scope_count(), 1);
        replacement.close().await.unwrap();
        scopes.drain().await;
        assert!(lock(&scopes.entries).is_empty());
        assert!(scopes.drivers.is_empty());
        container.close().await.unwrap();
    }
}
