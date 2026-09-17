//! Provider-private delivery outcome materialization.
//!
//! This module deliberately is not a public provider SPI. It gives Lily one
//! transport-neutral authority for deciding a retry/dead-letter handoff and
//! for consuming the original delivery's settlement capability at most once.

use std::{
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

use crate::{retry_engine_trait::FailureClass, shutdown_budget::QueueShutdownBudget};

/// Normalized result of delivery execution after framework cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionOutcome {
    /// Handler execution and all reverse cleanup completed successfully.
    Success,
    /// Handler or framework execution failed with an explicit safe class/code.
    Failure {
        class: FailureClass,
        code: &'static str,
    },
    /// A framework-owned transactional lease is currently held elsewhere.
    ///
    /// This is neither an application failure nor a retry-policy attempt. The
    /// original broker delivery is released once with a bounded requeue NACK.
    RequeueDeferred { code: &'static str },
    /// Framework force cancellation won before an execution outcome existed.
    FrameworkCancelled,
}

/// Destination selected by Lily's bounded retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandoffDestination {
    Retry,
    DeadLetter,
}

/// Immutable handoff decision supplied to the provider adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HandoffPlan {
    destination: HandoffDestination,
    failure_class: FailureClass,
    failure_code: &'static str,
    current_retry_count: u32,
    next_retry_count: u32,
    delivery_attempt: u32,
}

impl HandoffPlan {
    pub(crate) fn for_failure(
        failure_class: FailureClass,
        failure_code: &'static str,
        current_retry_count: u32,
        retry_attempts: u32,
    ) -> Self {
        let retry =
            failure_class == FailureClass::Retryable && current_retry_count < retry_attempts;
        Self {
            destination: if retry {
                HandoffDestination::Retry
            } else {
                HandoffDestination::DeadLetter
            },
            failure_class,
            failure_code,
            current_retry_count,
            next_retry_count: if retry {
                current_retry_count.saturating_add(1)
            } else {
                current_retry_count
            },
            delivery_attempt: current_retry_count.saturating_add(1),
        }
    }

    pub(crate) const fn destination(self) -> HandoffDestination {
        self.destination
    }

    #[cfg(test)]
    pub(crate) const fn failure_class(self) -> FailureClass {
        self.failure_class
    }

    pub(crate) const fn failure_code(self) -> &'static str {
        self.failure_code
    }

    pub(crate) const fn current_retry_count(self) -> u32 {
        self.current_retry_count
    }

    pub(crate) const fn next_retry_count(self) -> u32 {
        self.next_retry_count
    }

    pub(crate) const fn delivery_attempt(self) -> u32 {
        self.delivery_attempt
    }
}

/// Provider evidence that a handoff reached one exact destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandoffReceipt {
    RetryConfirmed,
    DeadLetterConfirmed,
}

impl HandoffReceipt {
    const fn destination(self) -> HandoffDestination {
        match self {
            Self::RetryConfirmed => HandoffDestination::Retry,
            Self::DeadLetterConfirmed => HandoffDestination::DeadLetter,
        }
    }
}

/// Proven terminal state for the original broker delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettlementTerminal {
    AckedSuccess,
    AckedRetry,
    AckedDeadLetter,
    NackRequeue,
    NackDeadLetter,
    Unresolved,
}

/// One bounded provider operation performed by the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettlementOperation {
    Handoff,
    Ack,
    NackRequeue,
    NackDeadLetter,
}

/// Secret-safe classification for materialization failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettlementFailureKind {
    AdapterError,
    Declined,
    Panicked,
    TimedOut,
    Cancelled,
    ReceiptMismatch,
    AuthorityConsumed,
}

impl SettlementFailureKind {
    pub(crate) const fn stable_code(self) -> &'static str {
        match self {
            Self::AdapterError => "QUEUE_SETTLEMENT_ADAPTER_ERROR",
            Self::Declined => "QUEUE_SETTLEMENT_DECLINED",
            Self::Panicked => "QUEUE_SETTLEMENT_PANICKED",
            Self::TimedOut => "QUEUE_SETTLEMENT_TIMED_OUT",
            Self::Cancelled => "QUEUE_SETTLEMENT_CANCELLED",
            Self::ReceiptMismatch => "QUEUE_HANDOFF_RECEIPT_MISMATCH",
            Self::AuthorityConsumed => "QUEUE_SETTLEMENT_AUTHORITY_CONSUMED",
        }
    }
}

/// A materialization failure which never exposes its provider source through
/// `Debug` or `Display`.
pub(crate) struct SettlementFailure {
    kind: SettlementFailureKind,
    operation: Option<SettlementOperation>,
    source: Option<MessageBrokerError>,
}

impl SettlementFailure {
    fn adapter(operation: SettlementOperation, source: MessageBrokerError) -> Self {
        Self {
            kind: SettlementFailureKind::AdapterError,
            operation: Some(operation),
            source: Some(source),
        }
    }

    const fn classified(
        kind: SettlementFailureKind,
        operation: Option<SettlementOperation>,
    ) -> Self {
        Self {
            kind,
            operation,
            source: None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn classified_for_test(
        kind: SettlementFailureKind,
        operation: Option<SettlementOperation>,
    ) -> Self {
        Self::classified(kind, operation)
    }

    pub(crate) const fn kind(&self) -> SettlementFailureKind {
        self.kind
    }

    pub(crate) const fn operation(&self) -> Option<SettlementOperation> {
        self.operation
    }

    pub(crate) fn into_source(self) -> Option<MessageBrokerError> {
        self.source
    }

    pub(crate) fn stable_code(&self) -> &'static str {
        self.source
            .as_ref()
            .map_or_else(|| self.kind.stable_code(), MessageBrokerError::error_code)
    }
}

impl fmt::Debug for SettlementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SettlementFailure")
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("stable_code", &self.stable_code())
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for SettlementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "queue settlement failed ({})",
            self.stable_code()
        )
    }
}

/// Bounded indication that optional settlement observation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SettlementObserverFailure;

impl SettlementObserverFailure {
    pub(crate) const fn recording_failed() -> Self {
        Self
    }
}

/// Final evidence returned by the private settlement coordinator.
pub(crate) struct SettlementReport {
    terminal: SettlementTerminal,
    failure: Option<SettlementFailure>,
    secondary_failure: Option<SettlementFailure>,
    observer_failure: Option<SettlementObserverFailure>,
    confirmed_handoff: Option<HandoffReceipt>,
}

impl SettlementReport {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) const fn terminal(&self) -> SettlementTerminal {
        self.terminal
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) const fn failure(&self) -> Option<&SettlementFailure> {
        self.failure.as_ref()
    }

    pub(crate) const fn secondary_failure(&self) -> Option<&SettlementFailure> {
        self.secondary_failure.as_ref()
    }

    pub(crate) const fn observer_failure(&self) -> Option<SettlementObserverFailure> {
        self.observer_failure
    }

    #[cfg(test)]
    pub(crate) const fn confirmed_handoff(&self) -> Option<HandoffReceipt> {
        self.confirmed_handoff
    }

    pub(crate) fn into_failure(self) -> Option<SettlementFailure> {
        self.failure
    }
}

impl fmt::Debug for SettlementReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SettlementReport")
            .field("terminal", &self.terminal)
            .field("failure", &self.failure)
            .field("secondary_failure", &self.secondary_failure)
            .field("observer_failure", &self.observer_failure)
            .field("confirmed_handoff", &self.confirmed_handoff)
            .finish()
    }
}

/// Private provider adapter used by the production RabbitMQ engine and fakes.
#[async_trait]
pub(crate) trait SettlementPort: Send {
    async fn handoff(&mut self, plan: HandoffPlan) -> Result<HandoffReceipt, MessageBrokerError>;

    async fn ack(&mut self) -> Result<bool, MessageBrokerError>;

    async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError>;

    async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError>;
}

/// Optional sampling-independent observer. Its failure is always secondary.
pub(crate) trait SettlementObserver: Send {
    fn handoff_confirmed(
        &mut self,
        _receipt: HandoffReceipt,
    ) -> Result<(), SettlementObserverFailure> {
        Ok(())
    }

    fn terminal(&mut self, terminal: SettlementTerminal) -> Result<(), SettlementObserverFailure>;
}

const AUTHORITY_AVAILABLE: u8 = 0;
const AUTHORITY_CONSUMED: u8 = 1;

/// Per-delivery linearization point for the complete materialization flow.
///
/// The claim is intentionally never released. If the caller future is
/// cancelled after an adapter operation started, the broker result is
/// ambiguous and a later caller must not attempt a second settlement.
#[derive(Debug, Default)]
pub(crate) struct SettlementAuthority {
    state: AtomicU8,
}

impl SettlementAuthority {
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU8::new(AUTHORITY_AVAILABLE),
        }
    }

    fn claim(&self) -> Result<(), SettlementFailure> {
        self.state
            .compare_exchange(
                AUTHORITY_AVAILABLE,
                AUTHORITY_CONSUMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| {
                SettlementFailure::classified(SettlementFailureKind::AuthorityConsumed, None)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseState {
    Pending,
    OriginalSettlementStarted(SettlementOperation),
    Terminal(SettlementTerminal),
}

struct SettlementLease<'a> {
    observer: &'a mut dyn SettlementObserver,
    state: LeaseState,
    observer_failure: Option<SettlementObserverFailure>,
    confirmed_handoff: Option<HandoffReceipt>,
}

impl<'a> SettlementLease<'a> {
    fn claim(
        observer: &'a mut dyn SettlementObserver,
        authority: &SettlementAuthority,
    ) -> Result<Self, SettlementFailure> {
        authority.claim()?;
        Ok(Self {
            observer,
            state: LeaseState::Pending,
            observer_failure: None,
            confirmed_handoff: None,
        })
    }

    fn observe_handoff(&mut self, receipt: HandoffReceipt) {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.observer.handoff_confirmed(receipt)
        }));
        if !matches!(result, Ok(Ok(()))) {
            self.observer_failure
                .get_or_insert(SettlementObserverFailure::recording_failed());
        }
    }

    fn observe_terminal(&mut self, terminal: SettlementTerminal) {
        let result =
            std::panic::catch_unwind(AssertUnwindSafe(|| self.observer.terminal(terminal)));
        if !matches!(result, Ok(Ok(()))) {
            self.observer_failure
                .get_or_insert(SettlementObserverFailure::recording_failed());
        }
    }

    fn confirm_handoff(&mut self, receipt: HandoffReceipt) {
        self.confirmed_handoff = Some(receipt);
        self.observe_handoff(receipt);
    }

    fn begin_original(&mut self, operation: SettlementOperation) -> Result<(), SettlementFailure> {
        if self.state != LeaseState::Pending {
            return Err(SettlementFailure::classified(
                SettlementFailureKind::AuthorityConsumed,
                Some(operation),
            ));
        }
        self.state = LeaseState::OriginalSettlementStarted(operation);
        Ok(())
    }

    fn finish(&mut self, terminal: SettlementTerminal) {
        if matches!(self.state, LeaseState::Terminal(_)) {
            return;
        }
        self.state = LeaseState::Terminal(terminal);
        self.observe_terminal(terminal);
    }

    fn report(
        mut self,
        terminal: SettlementTerminal,
        failure: Option<SettlementFailure>,
        secondary_failure: Option<SettlementFailure>,
    ) -> SettlementReport {
        self.finish(terminal);
        SettlementReport {
            terminal,
            failure,
            secondary_failure,
            observer_failure: self.observer_failure,
            confirmed_handoff: self.confirmed_handoff,
        }
    }
}

impl Drop for SettlementLease<'_> {
    fn drop(&mut self) {
        if !matches!(self.state, LeaseState::Terminal(_)) {
            self.state = LeaseState::Terminal(SettlementTerminal::Unresolved);
            self.observe_terminal(SettlementTerminal::Unresolved);
        }
    }
}

async fn bounded_operation<T, F>(
    operation: SettlementOperation,
    timeout: Duration,
    cancellation: &CancellationToken,
    future: F,
    shutdown_budget: &QueueShutdownBudget,
) -> Result<T, SettlementFailure>
where
    F: Future<Output = Result<T, MessageBrokerError>> + Send,
    T: Send,
{
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(SettlementFailure::classified(
            SettlementFailureKind::Cancelled,
            Some(operation),
        )),
        result = shutdown_budget.run_until(
            tokio::time::Instant::now() + timeout,
            AssertUnwindSafe(future).catch_unwind(),
        ) => {
            match result {
                Ok(Ok(Ok(value))) => Ok(value),
                Ok(Ok(Err(error))) => Err(SettlementFailure::adapter(operation, error)),
                Ok(Err(_)) => Err(SettlementFailure::classified(
                    SettlementFailureKind::Panicked,
                    Some(operation),
                )),
                Err(_) => Err(SettlementFailure::classified(
                    SettlementFailureKind::TimedOut,
                    Some(operation),
                )),
            }
        }
    }
}

async fn original_settlement(
    lease: &mut SettlementLease<'_>,
    port: &mut dyn SettlementPort,
    operation: SettlementOperation,
    timeout: Duration,
    cancellation: &CancellationToken,
    shutdown_budget: &QueueShutdownBudget,
) -> Result<(), SettlementFailure> {
    lease.begin_original(operation)?;
    let accepted = match operation {
        SettlementOperation::Ack => {
            bounded_operation(
                operation,
                timeout,
                cancellation,
                port.ack(),
                shutdown_budget,
            )
            .await
        }
        SettlementOperation::NackRequeue => {
            bounded_operation(
                operation,
                timeout,
                cancellation,
                port.nack_requeue(),
                shutdown_budget,
            )
            .await
        }
        SettlementOperation::NackDeadLetter => {
            bounded_operation(
                operation,
                timeout,
                cancellation,
                port.nack_dead_letter(),
                shutdown_budget,
            )
            .await
        }
        SettlementOperation::Handoff => unreachable!("handoff is not an original settlement"),
    }?;
    if accepted {
        Ok(())
    } else {
        Err(SettlementFailure::classified(
            SettlementFailureKind::Declined,
            Some(operation),
        ))
    }
}

/// Materialize one normalized execution outcome through an exact-once lease.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn materialize_delivery(
    port: &mut dyn SettlementPort,
    authority: &SettlementAuthority,
    observer: &mut dyn SettlementObserver,
    outcome: ExecutionOutcome,
    current_retry_count: u32,
    retry_attempts: u32,
    cancellation: &CancellationToken,
    handoff_timeout: Duration,
    settlement_timeout: Duration,
    shutdown_budget: &QueueShutdownBudget,
) -> SettlementReport {
    let mut lease = match SettlementLease::claim(observer, authority) {
        Ok(lease) => lease,
        Err(failure) => {
            return SettlementReport {
                terminal: SettlementTerminal::Unresolved,
                failure: Some(failure),
                secondary_failure: None,
                observer_failure: None,
                confirmed_handoff: None,
            };
        }
    };

    if matches!(outcome, ExecutionOutcome::FrameworkCancelled) || cancellation.is_cancelled() {
        return lease.report(
            SettlementTerminal::Unresolved,
            Some(SettlementFailure::classified(
                SettlementFailureKind::Cancelled,
                None,
            )),
            None,
        );
    }

    if let ExecutionOutcome::RequeueDeferred { code: _ } = outcome {
        let result = original_settlement(
            &mut lease,
            port,
            SettlementOperation::NackRequeue,
            settlement_timeout,
            cancellation,
            shutdown_budget,
        )
        .await;
        return match result {
            Ok(()) => lease.report(SettlementTerminal::NackRequeue, None, None),
            Err(failure) => lease.report(SettlementTerminal::Unresolved, Some(failure), None),
        };
    }

    let ExecutionOutcome::Failure { class, code } = outcome else {
        let result = original_settlement(
            &mut lease,
            port,
            SettlementOperation::Ack,
            settlement_timeout,
            cancellation,
            shutdown_budget,
        )
        .await;
        return match result {
            Ok(()) => lease.report(SettlementTerminal::AckedSuccess, None, None),
            Err(failure) => lease.report(SettlementTerminal::Unresolved, Some(failure), None),
        };
    };

    let plan = HandoffPlan::for_failure(class, code, current_retry_count, retry_attempts);
    let handoff = bounded_operation(
        SettlementOperation::Handoff,
        handoff_timeout,
        cancellation,
        port.handoff(plan),
        shutdown_budget,
    )
    .await;

    let receipt = match handoff {
        Ok(receipt) if receipt.destination() == plan.destination() => receipt,
        Ok(_) => {
            return lease.report(
                SettlementTerminal::Unresolved,
                Some(SettlementFailure::classified(
                    SettlementFailureKind::ReceiptMismatch,
                    Some(SettlementOperation::Handoff),
                )),
                None,
            );
        }
        Err(failure)
            if matches!(
                failure.kind(),
                SettlementFailureKind::AdapterError
                    | SettlementFailureKind::Panicked
                    | SettlementFailureKind::TimedOut
            ) =>
        {
            let nack = original_settlement(
                &mut lease,
                port,
                SettlementOperation::NackRequeue,
                settlement_timeout,
                cancellation,
                shutdown_budget,
            )
            .await;
            return match nack {
                Ok(()) => lease.report(SettlementTerminal::NackRequeue, Some(failure), None),
                Err(nack_failure) => lease.report(
                    SettlementTerminal::Unresolved,
                    Some(failure),
                    Some(nack_failure),
                ),
            };
        }
        Err(failure) => {
            return lease.report(SettlementTerminal::Unresolved, Some(failure), None);
        }
    };

    lease.confirm_handoff(receipt);
    let ack = original_settlement(
        &mut lease,
        port,
        SettlementOperation::Ack,
        settlement_timeout,
        cancellation,
        shutdown_budget,
    )
    .await;
    match ack {
        Ok(()) => lease.report(
            match receipt {
                HandoffReceipt::RetryConfirmed => SettlementTerminal::AckedRetry,
                HandoffReceipt::DeadLetterConfirmed => SettlementTerminal::AckedDeadLetter,
            },
            None,
            None,
        ),
        Err(failure) => lease.report(SettlementTerminal::Unresolved, Some(failure), None),
    }
}

/// Materialize broker-native dead-lettering without copying the remote body.
pub(crate) async fn materialize_broker_dead_letter(
    port: &mut dyn SettlementPort,
    authority: &SettlementAuthority,
    observer: &mut dyn SettlementObserver,
    cancellation: &CancellationToken,
    settlement_timeout: Duration,
    shutdown_budget: &QueueShutdownBudget,
) -> SettlementReport {
    let mut lease = match SettlementLease::claim(observer, authority) {
        Ok(lease) => lease,
        Err(failure) => {
            return SettlementReport {
                terminal: SettlementTerminal::Unresolved,
                failure: Some(failure),
                secondary_failure: None,
                observer_failure: None,
                confirmed_handoff: None,
            };
        }
    };
    if cancellation.is_cancelled() {
        return lease.report(
            SettlementTerminal::Unresolved,
            Some(SettlementFailure::classified(
                SettlementFailureKind::Cancelled,
                None,
            )),
            None,
        );
    }
    match original_settlement(
        &mut lease,
        port,
        SettlementOperation::NackDeadLetter,
        settlement_timeout,
        cancellation,
        shutdown_budget,
    )
    .await
    {
        Ok(()) => lease.report(SettlementTerminal::NackDeadLetter, None, None),
        Err(failure) => lease.report(SettlementTerminal::Unresolved, Some(failure), None),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use lily_error::application::message_broker::RabbitMQError;
    use tokio::sync::Notify;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Call {
        Handoff(HandoffDestination),
        Ack,
        NackRequeue,
        NackDeadLetter,
    }

    enum Step<T> {
        Return(Result<T, MessageBrokerError>),
        Panic,
        Pending,
        Block {
            entered: Arc<Notify>,
            release: Arc<Notify>,
            result: Result<T, MessageBrokerError>,
        },
    }

    impl<T> Step<T> {
        async fn run(self, hits: &AtomicUsize) -> Result<T, MessageBrokerError> {
            hits.fetch_add(1, Ordering::SeqCst);
            match self {
                Self::Return(result) => result,
                Self::Panic => panic!("injected settlement adapter panic"),
                Self::Pending => std::future::pending().await,
                Self::Block {
                    entered,
                    release,
                    result,
                } => {
                    entered.notify_one();
                    release.notified().await;
                    result
                }
            }
        }
    }

    struct FakePort {
        calls: Arc<Mutex<Vec<Call>>>,
        plans: Arc<Mutex<Vec<HandoffPlan>>>,
        handoff: VecDeque<Step<HandoffReceipt>>,
        ack: VecDeque<Step<bool>>,
        nack_requeue: VecDeque<Step<bool>>,
        nack_dead_letter: VecDeque<Step<bool>>,
        handoff_hits: Arc<AtomicUsize>,
        ack_hits: Arc<AtomicUsize>,
        nack_requeue_hits: Arc<AtomicUsize>,
        nack_dead_letter_hits: Arc<AtomicUsize>,
    }

    impl FakePort {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                plans: Arc::new(Mutex::new(Vec::new())),
                handoff: VecDeque::new(),
                ack: VecDeque::new(),
                nack_requeue: VecDeque::new(),
                nack_dead_letter: VecDeque::new(),
                handoff_hits: Arc::new(AtomicUsize::new(0)),
                ack_hits: Arc::new(AtomicUsize::new(0)),
                nack_requeue_hits: Arc::new(AtomicUsize::new(0)),
                nack_dead_letter_hits: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().expect("fake call ledger").clone()
        }

        fn plans(&self) -> Vec<HandoffPlan> {
            self.plans.lock().expect("fake plan ledger").clone()
        }

        fn pop<T>(steps: &mut VecDeque<Step<T>>, operation: &str) -> Step<T> {
            steps
                .pop_front()
                .unwrap_or_else(|| panic!("missing fake {operation} step"))
        }
    }

    #[async_trait]
    impl SettlementPort for FakePort {
        async fn handoff(
            &mut self,
            plan: HandoffPlan,
        ) -> Result<HandoffReceipt, MessageBrokerError> {
            self.plans.lock().expect("fake plan ledger").push(plan);
            self.calls
                .lock()
                .expect("fake call ledger")
                .push(Call::Handoff(plan.destination()));
            Self::pop(&mut self.handoff, "handoff")
                .run(&self.handoff_hits)
                .await
        }

        async fn ack(&mut self) -> Result<bool, MessageBrokerError> {
            self.calls.lock().expect("fake call ledger").push(Call::Ack);
            Self::pop(&mut self.ack, "ack").run(&self.ack_hits).await
        }

        async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError> {
            self.calls
                .lock()
                .expect("fake call ledger")
                .push(Call::NackRequeue);
            Self::pop(&mut self.nack_requeue, "nack requeue")
                .run(&self.nack_requeue_hits)
                .await
        }

        async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError> {
            self.calls
                .lock()
                .expect("fake call ledger")
                .push(Call::NackDeadLetter);
            Self::pop(&mut self.nack_dead_letter, "nack dead-letter")
                .run(&self.nack_dead_letter_hits)
                .await
        }
    }

    #[derive(Default)]
    struct ObserverState {
        handoffs: Mutex<Vec<HandoffReceipt>>,
        terminals: Mutex<Vec<SettlementTerminal>>,
        fail_handoff: bool,
        fail_terminal: bool,
        panic_handoff: bool,
        panic_terminal: bool,
    }

    struct RecordingObserver(Arc<ObserverState>);

    impl SettlementObserver for RecordingObserver {
        fn handoff_confirmed(
            &mut self,
            receipt: HandoffReceipt,
        ) -> Result<(), SettlementObserverFailure> {
            assert!(!self.0.panic_handoff, "injected handoff observer panic");
            self.0
                .handoffs
                .lock()
                .expect("handoff observations")
                .push(receipt);
            if self.0.fail_handoff {
                Err(SettlementObserverFailure::recording_failed())
            } else {
                Ok(())
            }
        }

        fn terminal(
            &mut self,
            terminal: SettlementTerminal,
        ) -> Result<(), SettlementObserverFailure> {
            assert!(!self.0.panic_terminal, "injected terminal observer panic");
            self.0
                .terminals
                .lock()
                .expect("terminal observations")
                .push(terminal);
            if self.0.fail_terminal {
                Err(SettlementObserverFailure::recording_failed())
            } else {
                Ok(())
            }
        }
    }

    fn observer() -> (RecordingObserver, Arc<ObserverState>) {
        let state = Arc::new(ObserverState::default());
        (RecordingObserver(Arc::clone(&state)), state)
    }

    fn adapter_error(message: &str) -> MessageBrokerError {
        MessageBrokerError::RabbitMQError(RabbitMQError::General(message.into()))
    }

    fn retryable(code: &'static str) -> ExecutionOutcome {
        ExecutionOutcome::Failure {
            class: FailureClass::Retryable,
            code,
        }
    }

    fn permanent(code: &'static str) -> ExecutionOutcome {
        ExecutionOutcome::Failure {
            class: FailureClass::Permanent,
            code,
        }
    }

    async fn run(
        port: &mut FakePort,
        observer: &mut RecordingObserver,
        outcome: ExecutionOutcome,
        retry_count: u32,
        retry_attempts: u32,
    ) -> SettlementReport {
        let authority = SettlementAuthority::new();
        materialize_delivery(
            port,
            &authority,
            observer,
            outcome,
            retry_count,
            retry_attempts,
            &CancellationToken::new(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await
    }

    #[tokio::test]
    async fn success_consumes_exactly_one_ack_authority() {
        let mut port = FakePort::new();
        port.ack.push_back(Step::Return(Ok(true)));
        let (mut observer, state) = observer();

        let report = run(&mut port, &mut observer, ExecutionOutcome::Success, 0, 3).await;

        assert_eq!(report.terminal(), SettlementTerminal::AckedSuccess);
        assert!(report.failure().is_none());
        assert_eq!(port.calls(), vec![Call::Ack]);
        assert_eq!(port.ack_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            *state.terminals.lock().unwrap(),
            vec![SettlementTerminal::AckedSuccess]
        );
    }

    #[tokio::test]
    async fn deferred_reasons_nack_once_without_handoff_even_when_retries_are_disabled() {
        for code in [
            "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED",
            "QUEUE_MONGODB_COMMIT_OUTCOME_DEFERRED",
        ] {
            let mut port = FakePort::new();
            port.nack_requeue.push_back(Step::Return(Ok(true)));
            let (mut observer, state) = observer();

            let report = run(
                &mut port,
                &mut observer,
                ExecutionOutcome::RequeueDeferred { code },
                u32::MAX,
                0,
            )
            .await;

            assert_eq!(report.terminal(), SettlementTerminal::NackRequeue);
            assert!(report.failure().is_none());
            assert!(report.confirmed_handoff().is_none());
            assert!(port.plans().is_empty());
            assert_eq!(port.calls(), vec![Call::NackRequeue]);
            assert_eq!(port.handoff_hits.load(Ordering::SeqCst), 0);
            assert_eq!(port.nack_requeue_hits.load(Ordering::SeqCst), 1);
            assert_eq!(
                *state.terminals.lock().expect("terminal observations"),
                vec![SettlementTerminal::NackRequeue]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn contention_deferred_nack_failures_remain_unresolved_without_second_settlement() {
        let cases = [
            (Step::Return(Ok(false)), SettlementFailureKind::Declined),
            (
                Step::Return(Err(adapter_error("deferred nack failed"))),
                SettlementFailureKind::AdapterError,
            ),
            (Step::Panic, SettlementFailureKind::Panicked),
            (Step::Pending, SettlementFailureKind::TimedOut),
        ];

        for (step, expected_kind) in cases {
            let mut port = FakePort::new();
            port.nack_requeue.push_back(step);
            let (mut observer, _) = observer();
            let authority = SettlementAuthority::new();
            let timeout = if expected_kind == SettlementFailureKind::TimedOut {
                Duration::from_millis(10)
            } else {
                Duration::from_secs(1)
            };

            let report = materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                ExecutionOutcome::RequeueDeferred {
                    code: "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED",
                },
                0,
                0,
                &CancellationToken::new(),
                Duration::from_secs(1),
                timeout,
                &QueueShutdownBudget::default(),
            )
            .await;

            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(
                report.failure().expect("failure evidence").kind(),
                expected_kind
            );
            assert!(report.secondary_failure().is_none());
            assert_eq!(port.calls(), vec![Call::NackRequeue]);
            assert_eq!(port.handoff_hits.load(Ordering::SeqCst), 0);
            assert_eq!(port.nack_requeue_hits.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_keeps_contention_deferred_unresolved_without_broker_io() {
        let mut port = FakePort::new();
        let (mut observer, state) = observer();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let report = materialize_delivery(
            &mut port,
            &SettlementAuthority::new(),
            &mut observer,
            ExecutionOutcome::RequeueDeferred {
                code: "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED",
            },
            0,
            0,
            &cancellation,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await;

        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            report.failure().expect("cancelled failure").kind(),
            SettlementFailureKind::Cancelled
        );
        assert!(port.calls().is_empty());
        assert_eq!(
            *state.terminals.lock().expect("terminal observations"),
            vec![SettlementTerminal::Unresolved]
        );
    }

    #[test]
    fn retry_plan_has_exact_zero_n_and_n_plus_one_boundaries() {
        let first = HandoffPlan::for_failure(FailureClass::Retryable, "E", 0, 2);
        assert_eq!(first.destination(), HandoffDestination::Retry);
        assert_eq!(first.next_retry_count(), 1);
        assert_eq!(first.delivery_attempt(), 1);

        let exhausted = HandoffPlan::for_failure(FailureClass::Retryable, "E", 2, 2);
        assert_eq!(exhausted.destination(), HandoffDestination::DeadLetter);
        assert_eq!(exhausted.next_retry_count(), 2);
        assert_eq!(exhausted.delivery_attempt(), 3);

        let beyond = HandoffPlan::for_failure(FailureClass::Retryable, "E", 3, 2);
        assert_eq!(beyond.destination(), HandoffDestination::DeadLetter);
        assert_eq!(beyond.next_retry_count(), 3);
        assert_eq!(beyond.delivery_attempt(), 4);

        let permanent = HandoffPlan::for_failure(FailureClass::Permanent, "E", 0, 2);
        assert_eq!(permanent.destination(), HandoffDestination::DeadLetter);
    }

    #[tokio::test]
    async fn normalized_execution_failures_keep_their_class_and_stable_code() {
        for (class, code, expected_destination) in [
            (
                FailureClass::Retryable,
                "QUEUE_HANDLER_RETRYABLE",
                HandoffDestination::Retry,
            ),
            (
                FailureClass::Retryable,
                "QUEUE_HANDLER_PANICKED",
                HandoffDestination::Retry,
            ),
            (
                FailureClass::Retryable,
                "QUEUE_HANDLER_TIMEOUT",
                HandoffDestination::Retry,
            ),
            (
                FailureClass::Retryable,
                "QUEUE_HANDLER_CANCELLED",
                HandoffDestination::Retry,
            ),
            (
                FailureClass::Permanent,
                "QUEUE_PAYLOAD_INVALID",
                HandoffDestination::DeadLetter,
            ),
        ] {
            let mut port = FakePort::new();
            port.handoff
                .push_back(Step::Return(Ok(match expected_destination {
                    HandoffDestination::Retry => HandoffReceipt::RetryConfirmed,
                    HandoffDestination::DeadLetter => HandoffReceipt::DeadLetterConfirmed,
                })));
            port.ack.push_back(Step::Return(Ok(true)));
            let (mut observer, _) = observer();

            let report = run(
                &mut port,
                &mut observer,
                ExecutionOutcome::Failure { class, code },
                0,
                1,
            )
            .await;

            assert!(matches!(
                report.terminal(),
                SettlementTerminal::AckedRetry | SettlementTerminal::AckedDeadLetter
            ));
            let plans = port.plans();
            assert_eq!(plans.len(), 1);
            assert_eq!(plans[0].failure_class(), class);
            assert_eq!(plans[0].failure_code(), code);
            assert_eq!(plans[0].destination(), expected_destination);
        }
    }

    #[tokio::test]
    async fn retry_and_dead_letter_receipts_are_validated_before_ack() {
        for (outcome, retry_count, receipt, terminal, destination) in [
            (
                retryable("TRANSIENT"),
                0,
                HandoffReceipt::RetryConfirmed,
                SettlementTerminal::AckedRetry,
                HandoffDestination::Retry,
            ),
            (
                retryable("EXHAUSTED"),
                2,
                HandoffReceipt::DeadLetterConfirmed,
                SettlementTerminal::AckedDeadLetter,
                HandoffDestination::DeadLetter,
            ),
            (
                permanent("INVALID"),
                0,
                HandoffReceipt::DeadLetterConfirmed,
                SettlementTerminal::AckedDeadLetter,
                HandoffDestination::DeadLetter,
            ),
        ] {
            let mut port = FakePort::new();
            port.handoff.push_back(Step::Return(Ok(receipt)));
            port.ack.push_back(Step::Return(Ok(true)));
            let (mut observer, _) = observer();

            let report = run(&mut port, &mut observer, outcome, retry_count, 2).await;

            assert_eq!(report.terminal(), terminal);
            assert_eq!(report.confirmed_handoff(), Some(receipt));
            assert_eq!(port.calls(), vec![Call::Handoff(destination), Call::Ack]);
        }
    }

    #[tokio::test]
    async fn failed_handoff_nacks_once_and_preserves_primary_failure() {
        let mut port = FakePort::new();
        port.handoff
            .push_back(Step::Return(Err(adapter_error("handoff failed"))));
        port.nack_requeue.push_back(Step::Return(Ok(true)));
        let (mut observer, _) = observer();

        let report = run(&mut port, &mut observer, retryable("TRANSIENT"), 0, 2).await;

        assert_eq!(report.terminal(), SettlementTerminal::NackRequeue);
        assert_eq!(
            report.failure().unwrap().kind(),
            SettlementFailureKind::AdapterError
        );
        assert!(report.secondary_failure().is_none());
        assert_eq!(
            port.calls(),
            vec![Call::Handoff(HandoffDestination::Retry), Call::NackRequeue]
        );
    }

    #[tokio::test]
    async fn failed_handoff_and_failed_nack_keep_primary_and_secondary_evidence() {
        let mut port = FakePort::new();
        port.handoff
            .push_back(Step::Return(Err(adapter_error("handoff failed"))));
        port.nack_requeue
            .push_back(Step::Return(Err(adapter_error("nack failed"))));
        let (mut observer, _) = observer();

        let report = run(&mut port, &mut observer, permanent("INVALID"), 0, 2).await;

        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            report.failure().unwrap().operation(),
            Some(SettlementOperation::Handoff)
        );
        assert_eq!(
            report.secondary_failure().unwrap().operation(),
            Some(SettlementOperation::NackRequeue)
        );
        assert_eq!(port.nack_requeue_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_nack_decline_error_panic_and_timeout_never_reverse_settle() {
        let cases = [
            Step::Return(Ok(false)),
            Step::Return(Err(adapter_error("nack failed"))),
            Step::Panic,
            Step::Pending,
        ];
        let expected = [
            SettlementFailureKind::Declined,
            SettlementFailureKind::AdapterError,
            SettlementFailureKind::Panicked,
            SettlementFailureKind::TimedOut,
        ];

        for (step, expected_kind) in cases.into_iter().zip(expected) {
            let mut port = FakePort::new();
            port.handoff
                .push_back(Step::Return(Err(adapter_error("handoff failed"))));
            port.nack_requeue.push_back(step);
            let (mut observer, _) = observer();
            let authority = SettlementAuthority::new();
            let settlement_timeout = if expected_kind == SettlementFailureKind::TimedOut {
                Duration::from_millis(10)
            } else {
                Duration::from_secs(1)
            };

            let report = materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                retryable("TRANSIENT"),
                0,
                1,
                &CancellationToken::new(),
                Duration::from_secs(1),
                settlement_timeout,
                &QueueShutdownBudget::default(),
            )
            .await;

            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(
                report.failure().unwrap().operation(),
                Some(SettlementOperation::Handoff)
            );
            assert_eq!(report.secondary_failure().unwrap().kind(), expected_kind);
            assert_eq!(
                port.calls(),
                vec![Call::Handoff(HandoffDestination::Retry), Call::NackRequeue]
            );
            assert_eq!(port.nack_requeue_hits.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn mismatched_receipt_is_unresolved_and_never_settles_original() {
        for (outcome, receipt, expected) in [
            (
                permanent("INVALID"),
                HandoffReceipt::RetryConfirmed,
                HandoffDestination::DeadLetter,
            ),
            (
                retryable("TRANSIENT"),
                HandoffReceipt::DeadLetterConfirmed,
                HandoffDestination::Retry,
            ),
        ] {
            let mut port = FakePort::new();
            port.handoff.push_back(Step::Return(Ok(receipt)));
            let (mut observer, _) = observer();

            let report = run(&mut port, &mut observer, outcome, 0, 2).await;

            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(
                report.failure().unwrap().kind(),
                SettlementFailureKind::ReceiptMismatch
            );
            assert_eq!(port.calls(), vec![Call::Handoff(expected)]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ack_decline_error_panic_and_timeout_are_unresolved_without_replay() {
        let cases = [
            Step::Return(Ok(false)),
            Step::Return(Err(adapter_error("ack failed"))),
            Step::Panic,
            Step::Pending,
        ];
        let expected = [
            SettlementFailureKind::Declined,
            SettlementFailureKind::AdapterError,
            SettlementFailureKind::Panicked,
            SettlementFailureKind::TimedOut,
        ];

        for (step, expected_kind) in cases.into_iter().zip(expected) {
            let mut port = FakePort::new();
            port.ack.push_back(step);
            let (mut observer, _) = observer();
            let authority = SettlementAuthority::new();
            let timeout = if expected_kind == SettlementFailureKind::TimedOut {
                Duration::from_millis(10)
            } else {
                Duration::from_secs(1)
            };

            let report = materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                ExecutionOutcome::Success,
                0,
                1,
                &CancellationToken::new(),
                Duration::from_secs(1),
                timeout,
                &QueueShutdownBudget::default(),
            )
            .await;

            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(report.failure().unwrap().kind(), expected_kind);
            assert_eq!(port.calls(), vec![Call::Ack]);
            assert_eq!(port.ack_hits.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn handoff_panic_and_timeout_nack_once_but_coordinator_cancellation_does_not() {
        for (step, timeout, cancel_after_start, expected) in [
            (
                Step::Panic,
                Duration::from_secs(1),
                false,
                SettlementFailureKind::Panicked,
            ),
            (
                Step::Pending,
                Duration::from_millis(10),
                false,
                SettlementFailureKind::TimedOut,
            ),
        ] {
            let mut port = FakePort::new();
            port.handoff.push_back(step);
            port.nack_requeue.push_back(Step::Return(Ok(true)));
            let (mut observer, _) = observer();
            let cancellation = CancellationToken::new();
            let authority = SettlementAuthority::new();
            if cancel_after_start {
                cancellation.cancel();
            }
            let report = materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                retryable("TRANSIENT"),
                0,
                1,
                &cancellation,
                timeout,
                Duration::from_secs(1),
                &QueueShutdownBudget::default(),
            )
            .await;
            assert_eq!(report.terminal(), SettlementTerminal::NackRequeue);
            assert_eq!(report.failure().unwrap().kind(), expected);
            assert_eq!(
                port.calls(),
                vec![Call::Handoff(HandoffDestination::Retry), Call::NackRequeue]
            );
        }

        let mut port = FakePort::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        port.handoff.push_back(Step::Block {
            entered: Arc::clone(&entered),
            release,
            result: Ok(HandoffReceipt::RetryConfirmed),
        });
        let calls = Arc::clone(&port.calls);
        let (mut observer, _) = observer();
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        let authority = SettlementAuthority::new();
        let task = tokio::spawn(async move {
            materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                retryable("TRANSIENT"),
                0,
                1,
                &cancellation,
                Duration::from_secs(1),
                Duration::from_secs(1),
                &QueueShutdownBudget::default(),
            )
            .await
        });
        entered.notified().await;
        trigger.cancel();
        let report = task.await.unwrap();
        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            report.failure().unwrap().kind(),
            SettlementFailureKind::Cancelled
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![Call::Handoff(HandoffDestination::Retry)]
        );
    }

    #[tokio::test]
    async fn caller_drop_during_ack_marks_unresolved_and_never_replays() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut port = FakePort::new();
        port.ack.push_back(Step::Block {
            entered: Arc::clone(&entered),
            release,
            result: Ok(true),
        });
        let calls = Arc::clone(&port.calls);
        let (mut initial_observer, state) = observer();
        let authority = Arc::new(SettlementAuthority::new());
        let task_authority = Arc::clone(&authority);

        let task = tokio::spawn(async move {
            materialize_delivery(
                &mut port,
                task_authority.as_ref(),
                &mut initial_observer,
                ExecutionOutcome::Success,
                0,
                1,
                &CancellationToken::new(),
                Duration::from_secs(1),
                Duration::from_secs(1),
                &QueueShutdownBudget::default(),
            )
            .await
        });
        entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert_eq!(*calls.lock().unwrap(), vec![Call::Ack]);
        assert_eq!(
            *state.terminals.lock().unwrap(),
            vec![SettlementTerminal::Unresolved]
        );

        let mut replay_port = FakePort::new();
        let (mut replay_observer, _) = observer();
        let replay = materialize_delivery(
            &mut replay_port,
            authority.as_ref(),
            &mut replay_observer,
            ExecutionOutcome::Success,
            0,
            1,
            &CancellationToken::new(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await;
        assert_eq!(replay.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            replay.failure().unwrap().kind(),
            SettlementFailureKind::AuthorityConsumed
        );
        assert!(replay_port.calls().is_empty());
    }

    #[tokio::test]
    async fn concurrent_materializers_share_one_atomic_delivery_authority() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut first_port = FakePort::new();
        first_port.ack.push_back(Step::Block {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            result: Ok(true),
        });
        let first_calls = Arc::clone(&first_port.calls);
        let (mut first_observer, _) = observer();
        let authority = Arc::new(SettlementAuthority::new());
        let first_authority = Arc::clone(&authority);

        let first = tokio::spawn(async move {
            materialize_delivery(
                &mut first_port,
                first_authority.as_ref(),
                &mut first_observer,
                ExecutionOutcome::Success,
                0,
                1,
                &CancellationToken::new(),
                Duration::from_secs(1),
                Duration::from_secs(1),
                &QueueShutdownBudget::default(),
            )
            .await
        });
        entered.notified().await;

        let mut competing_port = FakePort::new();
        let (mut competing_observer, _) = observer();
        let competing = materialize_delivery(
            &mut competing_port,
            authority.as_ref(),
            &mut competing_observer,
            ExecutionOutcome::Success,
            0,
            1,
            &CancellationToken::new(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await;
        assert_eq!(
            competing.failure().unwrap().kind(),
            SettlementFailureKind::AuthorityConsumed
        );
        assert!(competing_port.calls().is_empty());

        release.notify_one();
        let first = first.await.unwrap();
        assert_eq!(first.terminal(), SettlementTerminal::AckedSuccess);
        assert_eq!(*first_calls.lock().unwrap(), vec![Call::Ack]);
    }

    #[tokio::test]
    async fn fake_runtime_accepts_healthy_work_after_adapter_faults() {
        let mut port = FakePort::new();
        port.ack
            .push_back(Step::Return(Err(adapter_error("first ack failed"))));
        port.ack.push_back(Step::Return(Ok(true)));
        let (mut observer, _) = observer();

        let failed = run(&mut port, &mut observer, ExecutionOutcome::Success, 0, 1).await;
        assert_eq!(failed.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            failed.failure().unwrap().kind(),
            SettlementFailureKind::AdapterError
        );

        let healthy = run(&mut port, &mut observer, ExecutionOutcome::Success, 0, 1).await;
        assert_eq!(healthy.terminal(), SettlementTerminal::AckedSuccess);
        assert_eq!(port.calls(), vec![Call::Ack, Call::Ack]);
        assert_eq!(port.ack_hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn framework_and_explicit_handler_cancellation_remain_distinct() {
        let mut framework_port = FakePort::new();
        let (mut framework_observer, _) = observer();
        let framework = run(
            &mut framework_port,
            &mut framework_observer,
            ExecutionOutcome::FrameworkCancelled,
            0,
            2,
        )
        .await;
        assert_eq!(framework.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            framework.failure().unwrap().kind(),
            SettlementFailureKind::Cancelled
        );
        assert!(framework_port.calls().is_empty());

        let mut handler_port = FakePort::new();
        handler_port
            .handoff
            .push_back(Step::Return(Ok(HandoffReceipt::RetryConfirmed)));
        handler_port.ack.push_back(Step::Return(Ok(true)));
        let (mut handler_observer, _) = observer();
        let handler = run(
            &mut handler_port,
            &mut handler_observer,
            retryable("HANDLER_CANCELLED"),
            0,
            2,
        )
        .await;
        assert_eq!(handler.terminal(), SettlementTerminal::AckedRetry);
    }

    #[tokio::test]
    async fn observer_fault_is_secondary_and_does_not_change_settlement() {
        let mut port = FakePort::new();
        port.handoff
            .push_back(Step::Return(Ok(HandoffReceipt::RetryConfirmed)));
        port.ack.push_back(Step::Return(Ok(true)));
        let state = Arc::new(ObserverState {
            fail_handoff: true,
            fail_terminal: true,
            ..ObserverState::default()
        });
        let mut observer = RecordingObserver(state);

        let report = run(&mut port, &mut observer, retryable("TRANSIENT"), 0, 2).await;

        assert_eq!(report.terminal(), SettlementTerminal::AckedRetry);
        assert!(report.failure().is_none());
        assert!(report.observer_failure().is_some());
        assert_eq!(
            port.calls(),
            vec![Call::Handoff(HandoffDestination::Retry), Call::Ack]
        );
    }

    #[tokio::test]
    async fn observer_panic_is_secondary_and_does_not_change_settlement() {
        let mut port = FakePort::new();
        port.handoff
            .push_back(Step::Return(Ok(HandoffReceipt::RetryConfirmed)));
        port.ack.push_back(Step::Return(Ok(true)));
        let state = Arc::new(ObserverState {
            panic_handoff: true,
            panic_terminal: true,
            ..ObserverState::default()
        });
        let mut observer = RecordingObserver(state);

        let report = run(&mut port, &mut observer, retryable("TRANSIENT"), 0, 2).await;

        assert_eq!(report.terminal(), SettlementTerminal::AckedRetry);
        assert!(report.failure().is_none());
        assert!(report.observer_failure().is_some());
        assert_eq!(
            port.calls(),
            vec![Call::Handoff(HandoffDestination::Retry), Call::Ack]
        );
    }

    #[tokio::test]
    async fn broker_native_dead_letter_uses_one_non_requeueing_nack() {
        let mut port = FakePort::new();
        port.nack_dead_letter.push_back(Step::Return(Ok(true)));
        let (mut observer, _) = observer();
        let authority = SettlementAuthority::new();
        let report = materialize_broker_dead_letter(
            &mut port,
            &authority,
            &mut observer,
            &CancellationToken::new(),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await;

        assert_eq!(report.terminal(), SettlementTerminal::NackDeadLetter);
        assert_eq!(port.calls(), vec![Call::NackDeadLetter]);
        assert_eq!(port.nack_dead_letter_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn broker_dead_letter_decline_error_panic_and_timeout_are_unresolved_once() {
        let cases = [
            Step::Return(Ok(false)),
            Step::Return(Err(adapter_error("dead-letter nack failed"))),
            Step::Panic,
            Step::Pending,
        ];
        let expected = [
            SettlementFailureKind::Declined,
            SettlementFailureKind::AdapterError,
            SettlementFailureKind::Panicked,
            SettlementFailureKind::TimedOut,
        ];

        for (step, expected_kind) in cases.into_iter().zip(expected) {
            let mut port = FakePort::new();
            port.nack_dead_letter.push_back(step);
            let (mut observer, _) = observer();
            let authority = SettlementAuthority::new();
            let timeout = if expected_kind == SettlementFailureKind::TimedOut {
                Duration::from_millis(10)
            } else {
                Duration::from_secs(1)
            };
            let report = materialize_broker_dead_letter(
                &mut port,
                &authority,
                &mut observer,
                &CancellationToken::new(),
                timeout,
                &QueueShutdownBudget::default(),
            )
            .await;

            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(report.failure().unwrap().kind(), expected_kind);
            assert_eq!(port.calls(), vec![Call::NackDeadLetter]);
            assert_eq!(port.nack_dead_letter_hits.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_root_starts_no_ack_nack_or_handoff() {
        let budget = QueueShutdownBudget::default();
        let now = tokio::time::Instant::now();
        budget.install(crate::shutdown_budget::QueueShutdownDeadlines::before(
            now, now,
        ));
        for outcome in [
            ExecutionOutcome::Success,
            retryable("RETRY"),
            ExecutionOutcome::RequeueDeferred { code: "CONTENTION" },
        ] {
            let mut port = FakePort::new();
            let (mut observer, state) = observer();
            let authority = SettlementAuthority::new();
            let report = materialize_delivery(
                &mut port,
                &authority,
                &mut observer,
                outcome,
                0,
                1,
                &CancellationToken::new(),
                Duration::from_secs(30),
                Duration::from_secs(30),
                &budget,
            )
            .await;
            assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
            assert_eq!(
                report.failure().unwrap().kind(),
                SettlementFailureKind::TimedOut
            );
            assert!(port.calls().is_empty());
            assert_eq!(
                *state.terminals.lock().unwrap(),
                vec![SettlementTerminal::Unresolved]
            );
        }
        let mut port = FakePort::new();
        let (mut observer, _) = observer();
        let report = materialize_broker_dead_letter(
            &mut port,
            &SettlementAuthority::new(),
            &mut observer,
            &CancellationToken::new(),
            Duration::from_secs(30),
            &budget,
        )
        .await;
        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert!(port.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn handoff_exhausting_root_cannot_start_a_fresh_fallback_nack_budget() {
        let mut port = FakePort::new();
        port.handoff.push_back(Step::Pending);
        let (mut observer, state) = observer();
        let budget = QueueShutdownBudget::default();
        let now = tokio::time::Instant::now();
        budget.install(crate::shutdown_budget::QueueShutdownDeadlines::before(
            now,
            now + Duration::from_millis(40),
        ));
        let report = materialize_delivery(
            &mut port,
            &SettlementAuthority::new(),
            &mut observer,
            retryable("RETRY"),
            0,
            1,
            &CancellationToken::new(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            &budget,
        )
        .await;
        assert_eq!(tokio::time::Instant::now() - now, Duration::from_millis(40));
        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            report.failure().unwrap().operation(),
            Some(SettlementOperation::Handoff)
        );
        assert_eq!(
            report.secondary_failure().unwrap().operation(),
            Some(SettlementOperation::NackRequeue)
        );
        assert_eq!(
            report.secondary_failure().unwrap().kind(),
            SettlementFailureKind::TimedOut
        );
        assert_eq!(port.calls(), vec![Call::Handoff(HandoffDestination::Retry)]);
        assert_eq!(
            *state.terminals.lock().unwrap(),
            vec![SettlementTerminal::Unresolved]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_handoff_is_retained_when_ack_hits_the_same_root_and_is_never_replayed() {
        let mut port = FakePort::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        port.handoff.push_back(Step::Block {
            entered: entered.clone(),
            release: release.clone(),
            result: Ok(HandoffReceipt::RetryConfirmed),
        });
        port.ack.push_back(Step::Pending);
        let (mut observer, state) = observer();
        let budget = QueueShutdownBudget::default();
        let authority = SettlementAuthority::new();
        let now = tokio::time::Instant::now();
        budget.install(crate::shutdown_budget::QueueShutdownDeadlines::before(
            now,
            now + Duration::from_millis(100),
        ));
        let cancel = CancellationToken::new();
        let operation = materialize_delivery(
            &mut port,
            &authority,
            &mut observer,
            retryable("RETRY"),
            0,
            1,
            &cancel,
            Duration::from_secs(30),
            Duration::from_secs(30),
            &budget,
        );
        let drive = async {
            entered.notified().await;
            tokio::time::sleep(Duration::from_millis(80)).await;
            release.notify_one();
        };
        let (report, ()) = tokio::join!(operation, drive);
        assert_eq!(
            tokio::time::Instant::now() - now,
            Duration::from_millis(100)
        );
        assert_eq!(report.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(
            report.confirmed_handoff(),
            Some(HandoffReceipt::RetryConfirmed)
        );
        assert_eq!(
            report.failure().unwrap().kind(),
            SettlementFailureKind::TimedOut
        );
        assert_eq!(
            report.failure().unwrap().operation(),
            Some(SettlementOperation::Ack)
        );
        assert_eq!(
            port.calls(),
            vec![Call::Handoff(HandoffDestination::Retry), Call::Ack]
        );
        let replay = materialize_delivery(
            &mut port,
            &authority,
            &mut observer,
            ExecutionOutcome::Success,
            0,
            1,
            &cancel,
            Duration::from_secs(30),
            Duration::from_secs(30),
            &budget,
        )
        .await;
        assert_eq!(replay.terminal(), SettlementTerminal::Unresolved);
        assert_eq!(port.ack_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            *state.handoffs.lock().unwrap(),
            vec![HandoffReceipt::RetryConfirmed]
        );
        assert_eq!(
            *state.terminals.lock().unwrap(),
            vec![SettlementTerminal::Unresolved]
        );
    }

    #[tokio::test]
    async fn cleanup_finishes_before_settlement_and_runtime_recovers_after_business_fault() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Event {
            Handler,
            CleanupStarted,
            CleanupFinished,
            Settlement,
        }

        let events = Arc::new(Mutex::new(Vec::new()));
        let execution_events = Arc::clone(&events);
        let outcome = async move {
            execution_events.lock().unwrap().push(Event::Handler);
            execution_events.lock().unwrap().push(Event::CleanupStarted);
            tokio::task::yield_now().await;
            execution_events
                .lock()
                .unwrap()
                .push(Event::CleanupFinished);
            retryable("FIRST_FAILURE")
        }
        .await;

        let mut port = FakePort::new();
        port.handoff
            .push_back(Step::Return(Ok(HandoffReceipt::RetryConfirmed)));
        port.ack.push_back(Step::Return(Ok(true)));
        port.ack.push_back(Step::Return(Ok(true)));
        let (mut observer, _) = observer();
        let first = run(&mut port, &mut observer, outcome, 0, 1).await;
        events.lock().unwrap().push(Event::Settlement);
        assert_eq!(first.terminal(), SettlementTerminal::AckedRetry);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                Event::Handler,
                Event::CleanupStarted,
                Event::CleanupFinished,
                Event::Settlement,
            ]
        );

        let healthy = run(&mut port, &mut observer, ExecutionOutcome::Success, 1, 1).await;
        assert_eq!(healthy.terminal(), SettlementTerminal::AckedSuccess);
        assert_eq!(port.ack_hits.load(Ordering::SeqCst), 2);
    }
}
