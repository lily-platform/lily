use super::*;

fn execution_slot() -> (DeliveryExecutionSlot, ExecutionReceipt) {
    let receipt = ExecutionReceipt::default();
    let (abort, registration) = AbortHandle::new_pair();
    (
        DeliveryExecutionSlot {
            registration,
            receipt: receipt.clone(),
            abort,
        },
        receipt,
    )
}

#[tokio::test(start_paused = true)]
async fn cooperative_driver_keeps_polling_until_real_completion_and_preserves_typed_error() {
    use crate::{delivery_execution::run_cooperative, shutdown_budget::DeliveryExecutionBudget};
    let source = DeliveryCancellationSource::new();
    let root = QueueShutdownBudget::default();
    let budget =
        DeliveryExecutionBudget::before(Instant::now() + Duration::from_secs(2), Instant::now());
    let (slot, receipt) = execution_slot();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe(dropped.clone());
    let future = async {
        let _probe = probe;
        source.cancelled().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        Err::<(), _>(QueueHandlerError::permanent("QUEUE_HANDLER_CANCELLED"))
    };
    let mut driven = Box::pin(run_cooperative(slot, future, &source, budget, &root));
    assert!(driven.as_mut().now_or_never().is_none());
    tokio::time::advance(budget.pipeline - Instant::now()).await;
    assert!(driven.as_mut().now_or_never().is_none());
    assert_eq!(
        source.reason(),
        Some(DeliveryCancellationReason::DeliveryTimeout)
    );
    assert_eq!(source.requested_at(), Some(budget.pipeline));
    assert!(!dropped.load(Ordering::Acquire));
    assert!(!receipt.terminal());
    match driven.await {
        DeliveryExecutionExit::Completed(Err(error)) => {
            assert_eq!(error.code(), "QUEUE_HANDLER_CANCELLED");
            assert_eq!(
                error.class(),
                lily_error::application::QueueHandlerFailureClass::Permanent
            );
        }
        _ => panic!("cooperative completion must preserve the application result"),
    }
    assert!(dropped.load(Ordering::Acquire));
    assert!(receipt.terminal());
}

#[tokio::test(start_paused = true)]
async fn late_poll_cannot_renew_expired_cooperation_or_start_new_execution() {
    use crate::{delivery_execution::run_cooperative, shutdown_budget::DeliveryExecutionBudget};
    let source = DeliveryCancellationSource::new();
    let at = Instant::now();
    source.cancel_at(DeliveryCancellationReason::ForcedShutdown, at);
    tokio::time::advance(Duration::from_secs(2)).await;
    source.cancel(DeliveryCancellationReason::RuntimeFailure);
    assert_eq!(source.requested_at(), Some(at));
    let (slot, receipt) = execution_slot();
    let budget =
        DeliveryExecutionBudget::before(Instant::now() + Duration::from_secs(30), Instant::now());
    let result = run_cooperative(
        slot,
        async { panic!("expired cooperative budget cannot start user code") },
        &source,
        budget,
        &QueueShutdownBudget::default(),
    )
    .await;
    assert!(matches!(
        result,
        DeliveryExecutionExit::Interrupted(DeliveryCancellationReason::ForcedShutdown)
    ));
    assert!(receipt.terminal());
    assert!(!lock(&receipt.0).started);
}

#[tokio::test(start_paused = true)]
async fn root_shortening_wakes_pending_execution_and_never_restarts_cooperation() {
    use crate::{delivery_execution::run_cooperative, shutdown_budget::DeliveryExecutionBudget};
    let source = DeliveryCancellationSource::new();
    let root = QueueShutdownBudget::default();
    let at = Instant::now();
    source.cancel(DeliveryCancellationReason::ForcedShutdown);
    let (slot, receipt) = execution_slot();
    let budget = DeliveryExecutionBudget::before(at + Duration::from_secs(30), at);
    let mut driven = Box::pin(run_cooperative(
        slot,
        std::future::pending::<()>(),
        &source,
        budget,
        &root,
    ));
    assert!(driven.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(20)).await;
    root.install(QueueShutdownDeadlines::before(
        at,
        at + Duration::from_millis(40),
    ));
    assert!(matches!(
        driven.await,
        DeliveryExecutionExit::Interrupted(DeliveryCancellationReason::ForcedShutdown)
    ));
    assert_eq!(Instant::now(), at + Duration::from_millis(20));
    assert!(receipt.terminal());
}

#[tokio::test(start_paused = true)]
async fn local_timeout_does_not_cancel_parent_or_concurrent_delivery() {
    use crate::{delivery_execution::run_cooperative, shutdown_budget::DeliveryExecutionBudget};
    let parent = DeliveryCancellationSource::new();
    let first = parent.child();
    let second = parent.child();
    let root = QueueShutdownBudget::default();
    let at = Instant::now();
    let (first_slot, first_receipt) = execution_slot();
    let (second_slot, second_receipt) = execution_slot();
    let (a, b) = tokio::join!(
        run_cooperative(
            first_slot,
            std::future::pending::<()>(),
            &first,
            DeliveryExecutionBudget::before(at + Duration::from_millis(100), at),
            &root
        ),
        run_cooperative(
            second_slot,
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                42
            },
            &second,
            DeliveryExecutionBudget::before(at + Duration::from_secs(1), at),
            &root
        )
    );
    assert!(matches!(
        a,
        DeliveryExecutionExit::Interrupted(DeliveryCancellationReason::DeliveryTimeout)
    ));
    assert!(matches!(b, DeliveryExecutionExit::Completed(42)));
    assert!(!parent.is_cancelled());
    assert!(!second.is_cancelled());
    assert!(first_receipt.terminal() && second_receipt.terminal());
}

#[test]
fn error_codes_and_cancellation_notifications_cannot_fake_framework_stop_evidence() {
    use crate::{
        delivery_execution::FrameworkStopReceipt, queue_engine_trait::QueueExecutionError,
    };
    let receipt = FrameworkStopReceipt::default();
    assert!(matches!(
        receipt.classify_error(QueueHandlerError::retryable("QUEUE_HANDLER_CANCELLED")),
        QueueExecutionError::Handler(_)
    ));
    receipt.interrupted(DeliveryCancellationReason::DeliveryTimeout);
    assert!(matches!(
        receipt.classify_error(QueueHandlerError::retryable(
            "QUEUE_DELIVERY_EXECUTION_TIMED_OUT"
        )),
        QueueExecutionError::Handler(_)
    ));
    receipt.interrupted(DeliveryCancellationReason::ForcedShutdown);
    assert!(matches!(
        receipt.classify_error(QueueHandlerError::retryable("ANY_ERROR")),
        QueueExecutionError::FrameworkCancelled { code: "ANY_ERROR" }
    ));
    receipt.begin_attempt();
    assert!(matches!(
        receipt.classify_error(QueueHandlerError::retryable("QUEUE_HANDLER_CANCELLED")),
        QueueExecutionError::Handler(_)
    ));
}

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn abort_request_is_not_execution_termination_and_unpolled_slot_still_releases() {
    let (slot, receipt) = execution_slot();
    let abort = slot.abort_handle();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe(dropped.clone());
    let execution = slot.run(async move {
        let _probe = probe;
        std::future::pending::<()>().await;
    });
    assert!(!receipt.terminal());
    abort.abort();
    assert!(!receipt.terminal(), "request cannot publish drop evidence");
    assert!(!dropped.load(Ordering::Acquire));
    assert!(matches!(execution.await, DeliveryExecutionExit::Cancelled));
    assert!(dropped.load(Ordering::Acquire));
    assert!(receipt.terminal());
    assert!(
        !lock(&receipt.0).started,
        "abort happened before the first user poll"
    );

    let (slot, receipt) = execution_slot();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe(dropped.clone());
    let execution = slot.run(async move {
        let _probe = probe;
    });
    drop(execution);
    assert!(dropped.load(Ordering::Acquire));
    assert!(receipt.terminal());
    assert_eq!(
        lock(&receipt.0).termination,
        Some(ExecutionTermination::Dropped)
    );
}

#[tokio::test]
async fn execution_poll_and_drop_panics_cannot_claim_success() {
    let (slot, receipt) = execution_slot();
    assert!(matches!(
        slot.run(async { panic!("poll panic") }).await,
        DeliveryExecutionExit::Panicked
    ));
    assert!(
        receipt.terminal(),
        "contained poll panic still releases execution"
    );

    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("drop panic");
        }
    }
    let (slot, receipt) = execution_slot();
    let abort = slot.abort_handle();
    let guard = PanicOnDrop;
    let execution = slot.run(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    abort.abort();
    assert!(matches!(execution.await, DeliveryExecutionExit::Panicked));
    assert!(
        !receipt.terminal(),
        "failed release is not clean termination evidence"
    );
}

#[test]
fn ledger_keeps_completed_and_interrupted_exits_distinct_and_non_repeatable() {
    let mut ledger = DeliveryMiddlewareLedger::default();
    for index in 0..3 {
        ledger.record_enter(index);
    }
    assert!(ledger.begin_exit(2));
    ledger.finish_exit(2, true);
    assert!(ledger.begin_exit(1));
    ledger.interrupt_exit();
    assert_eq!(
        ledger.states(),
        [
            MiddlewareExitState::NotStarted,
            MiddlewareExitState::Interrupted,
            MiddlewareExitState::Completed
        ]
    );
    assert!(!ledger.begin_exit(2));
    assert!(!ledger.begin_exit(1));
    assert!(ledger.begin_exit(0));
    ledger.finish_exit(0, false);
    assert!(!ledger.begin_exit(0));
}

#[tokio::test]
async fn observer_completion_and_cancelled_drain_waiters_do_not_replace_the_real_join() {
    let tracker = Arc::new(DeliveryScopeTracker::default());
    tracker.begin();
    let mut observer = DeliveryScopeCleanupObserver::new(tracker.clone());
    let observed = Arc::new(Notify::new());
    let task_observed = observed.clone();
    let release = Arc::new(Notify::new());
    let task_release = release.clone();
    tracker.adopt_cleanup(&tokio::runtime::Handle::current(), async move {
        observer.complete(true);
        drop(observer);
        task_observed.notify_one();
        task_release.notified().await;
    });
    observed.notified().await;
    assert_eq!(tracker.active.load(Ordering::Acquire), 0);
    assert!(
        !tracker.reconciled(),
        "completed scope observation is not a joined task"
    );
    assert!(tracker.drain().now_or_never().is_none());
    assert_eq!(
        lock(&tracker.cleanup_tasks).len(),
        1,
        "cancelled waiter retains the original join"
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .unwrap()
        .unwrap();
    assert!(tracker.reconciled());
    assert!(lock(&tracker.cleanup_tasks).is_empty());
}

#[tokio::test]
async fn cleanup_task_panic_is_not_hidden_by_a_completed_scope_observer() {
    let tracker = Arc::new(DeliveryScopeTracker::default());
    tracker.begin();
    let mut observer = DeliveryScopeCleanupObserver::new(tracker.clone());
    tracker.adopt_cleanup(&tokio::runtime::Handle::current(), async move {
        observer.complete(true);
        drop(observer);
        panic!("cleanup task panicked after scope observation");
    });
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .unwrap()
        .unwrap_err();
    assert!(!tracker.reconciled());
    assert!(
        lock(&tracker.cleanup_tasks).is_empty(),
        "panic join must still be consumed"
    );
}

#[test]
fn runtime_discarding_unpolled_cleanup_does_not_report_reconciliation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tracker = Arc::new(DeliveryScopeTracker::default());
    tracker.begin();
    let observer = DeliveryScopeCleanupObserver::new(tracker.clone());
    tracker.adopt_cleanup(runtime.handle(), async move {
        let _observer = observer;
        std::future::pending::<()>().await;
    });
    drop(runtime);
    assert_eq!(tracker.active.load(Ordering::Acquire), 0);
    assert!(!tracker.reconciled());
    assert!(lock(&tracker.cleanup_tasks).is_empty());
}

#[tokio::test]
async fn completed_cleanup_history_is_reaped_by_next_admission() {
    let tracker = Arc::new(DeliveryScopeTracker::default());
    for _ in 0..64 {
        tracker.begin();
        assert!(lock(&tracker.cleanup_tasks).is_empty());
        let mut observer = DeliveryScopeCleanupObserver::new(tracker.clone());
        tracker.adopt_cleanup(&tokio::runtime::Handle::current(), async move {
            observer.complete(true);
        });
        let receipt = lock(&tracker.cleanup_tasks)[0].clone();
        assert!(matches!(receipt.await, CleanupJoin::Completed));
        assert_eq!(lock(&tracker.cleanup_tasks).len(), 1);
    }
    tracker.drain().await.unwrap();
    assert!(lock(&tracker.cleanup_tasks).is_empty());
    assert!(tracker.reconciled());
}

#[test]
fn transferred_cleanup_cap_never_renews_or_exceeds_the_root_deadline() {
    let tracker = DeliveryScopeTracker::default();
    let now = Instant::now();
    let expired = now - Duration::from_secs(1);
    tracker.set_shutdown_deadlines(QueueShutdownDeadlines::before(expired, expired));
    tracker.set_shutdown_deadlines(QueueShutdownDeadlines::starting_at(
        now,
        Duration::from_secs(30),
    ));
    assert_eq!(tracker.cap(now + ABANDONED_SCOPE_CLEANUP_CAP), expired);
}
