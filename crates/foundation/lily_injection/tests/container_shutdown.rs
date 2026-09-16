use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ProcessContext, ServiceTrait, ShutdownOutcomeStatus,
    async_trait::async_trait,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static SINGLETON_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static SCOPED_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_DISPOSAL_ORDER: Mutex<Vec<usize>> = Mutex::new(Vec::new());
static BLOCK_SINGLETON_DISPOSE: AtomicBool = AtomicBool::new(false);
static ACTIVE_SINGLETON_DISPOSERS: AtomicUsize = AtomicUsize::new(0);
static DISPOSE_ENTERED: Notify = Notify::const_new();
static ALLOW_DISPOSE: Notify = Notify::const_new();
static ROOT_INIT_STARTED: Notify = Notify::const_new();
static ROOT_PARTIAL_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static SCOPED_INIT_STARTED: Notify = Notify::const_new();
static SCOPED_INIT_RELEASE: Notify = Notify::const_new();
static LATE_SCOPED_DISPOSES: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct SingletonResource;

struct ActiveSingletonDisposer;

impl Drop for ActiveSingletonDisposer {
    fn drop(&mut self) {
        ACTIVE_SINGLETON_DISPOSERS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ServiceTrait for SingletonResource {
    async fn dispose(&self) -> Result<(), InjectionError> {
        if BLOCK_SINGLETON_DISPOSE.load(Ordering::SeqCst) {
            ACTIVE_SINGLETON_DISPOSERS.fetch_add(1, Ordering::SeqCst);
            let _active_disposer = ActiveSingletonDisposer;
            // Register before publishing entry: the observer can run on the
            // other worker and notify_waiters() does not retain a permit.
            let release = ALLOW_DISPOSE.notified();
            tokio::pin!(release);
            release.as_mut().enable();
            DISPOSE_ENTERED.notify_one();
            release.await;
        }
        SINGLETON_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedResource;

#[async_trait]
impl ServiceTrait for ScopedResource {
    async fn dispose(&self) -> Result<(), InjectionError> {
        SCOPED_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct RootTransient {
    sequence: usize,
}

#[async_trait]
impl ServiceTrait for RootTransient {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.sequence = TRANSIENT_SEQUENCE.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        TRANSIENT_DISPOSAL_ORDER.lock().unwrap().push(self.sequence);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct CancelledRootInitialization;

#[async_trait]
impl ServiceTrait for CancelledRootInitialization {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        ROOT_INIT_STARTED.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        ROOT_PARTIAL_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct BlockedScopedInitialization;

#[async_trait]
impl ServiceTrait for BlockedScopedInitialization {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        SCOPED_INIT_STARTED.notify_one();
        SCOPED_INIT_RELEASE.notified().await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        LATE_SCOPED_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn reset() {
    SINGLETON_DISPOSES.store(0, Ordering::SeqCst);
    SCOPED_DISPOSES.store(0, Ordering::SeqCst);
    TRANSIENT_SEQUENCE.store(0, Ordering::SeqCst);
    TRANSIENT_DISPOSAL_ORDER.lock().unwrap().clear();
    BLOCK_SINGLETON_DISPOSE.store(false, Ordering::SeqCst);
    ACTIVE_SINGLETON_DISPOSERS.store(0, Ordering::SeqCst);
    ROOT_PARTIAL_DISPOSES.store(0, Ordering::SeqCst);
    LATE_SCOPED_DISPOSES.store(0, Ordering::SeqCst);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_close_is_idempotent_and_disposes_root_ledger_in_reverse() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let first = container.resolve::<RootTransient>(None).await.unwrap();
    let second = container.resolve::<RootTransient>(None).await.unwrap();
    assert_ne!(first.sequence, second.sequence);

    let calls = (0..32).map(|_| {
        let container = Arc::clone(&container);
        tokio::spawn(async move { container.close().await })
    });
    let reports = futures::future::join_all(calls).await;
    let first_report = reports[0].as_ref().unwrap().as_ref().unwrap().clone();
    for report in reports {
        assert_eq!(report.unwrap().unwrap(), first_report);
    }
    assert!(
        first_report
            .outcomes
            .iter()
            .all(|outcome| outcome.status == ShutdownOutcomeStatus::Completed)
    );
    assert_eq!(first_report.active_scopes_remaining, 0);
    assert_eq!(first_report.cleanup_tasks_remaining, 0);
    assert_eq!(first_report.active_resolutions_remaining, 0);
    assert_eq!(first_report.root_lifecycle_entries_remaining, 0);

    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(*TRANSIENT_DISPOSAL_ORDER.lock().unwrap(), vec![2, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hundred_close_resolve_races_leave_no_unowned_transient() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());

    let resolutions = (0..100).map(|_| {
        let container = Arc::clone(&container);
        tokio::spawn(async move { container.resolve::<RootTransient>(None).await })
    });
    let closing_container = Arc::clone(&container);
    let close = tokio::spawn(async move {
        closing_container
            .close_with_timeout(Duration::from_secs(2))
            .await
    });

    for resolution in futures::future::join_all(resolutions).await {
        match resolution.unwrap() {
            Ok(_) | Err(InjectionError::ContainerClosing | InjectionError::ContainerClosed) => {}
            Err(error) => panic!("unexpected resolution race result: {error}"),
        }
    }
    let report = close.await.unwrap().unwrap();
    assert_eq!(report.active_resolutions_remaining, 0);
    assert_eq!(report.root_lifecycle_entries_remaining, 0);
    assert_eq!(
        TRANSIENT_DISPOSAL_ORDER.lock().unwrap().len(),
        TRANSIENT_SEQUENCE.load(Ordering::SeqCst),
        "every initialized transient must reach exactly one disposer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_root_initialization_is_owned_by_container_rollback_ledger() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let resolving_container = Arc::clone(&container);
    let resolution = tokio::spawn(async move {
        resolving_container
            .resolve::<CancelledRootInitialization>(None)
            .await
    });
    ROOT_INIT_STARTED.notified().await;
    resolution.abort();
    assert!(matches!(resolution.await, Err(error) if error.is_cancelled()));

    let report = container.close().await.unwrap();
    assert_eq!(report.root_lifecycle_entries_remaining, 0);
    assert_eq!(ROOT_PARTIAL_DISPOSES.load(Ordering::SeqCst), 1);
    assert!(report.outcomes.iter().any(|outcome| {
        outcome.component.contains("CancelledRootInitialization")
            && outcome.status == ShutdownOutcomeStatus::Completed
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_resolution_deadline_skips_disposal_instead_of_racing_the_factory() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let resolving_container = Arc::clone(&container);
    let resolution = tokio::spawn(async move {
        resolving_container
            .resolve::<CancelledRootInitialization>(None)
            .await
    });
    ROOT_INIT_STARTED.notified().await;

    let error = container
        .close_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    let InjectionError::ShutdownFailed {
        outcomes,
        remaining,
        ..
    } = error
    else {
        panic!("root resolution deadline must produce a typed shutdown failure")
    };
    assert!(outcomes.iter().any(|outcome| {
        outcome.component == "service-resolution-drain"
            && outcome.status == ShutdownOutcomeStatus::TimedOut
    }));
    assert!(outcomes.iter().any(|outcome| {
        outcome.component == "root-lifecycle" && outcome.status == ShutdownOutcomeStatus::Cancelled
    }));
    let remaining = remaining.expect("unsafe root shutdown must retain work counters");
    assert!(remaining.active_resolutions >= 1);
    assert!(remaining.root_lifecycle_entries >= 1);
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 0);

    resolution.abort();
    assert!(matches!(resolution.await, Err(error) if error.is_cancelled()));
    tokio::task::yield_now().await;
    assert_eq!(ROOT_PARTIAL_DISPOSES.load(Ordering::SeqCst), 0);
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_scope_drains_before_singleton_shutdown_and_new_work_is_rejected() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let mut scope = container
        .create_scope(ProcessContext::with_process_id(8001))
        .unwrap();
    scope
        .run(container.resolve::<ScopedResource>(None))
        .await
        .unwrap()
        .unwrap();

    let closing_container = Arc::clone(&container);
    let close = tokio::spawn(async move {
        closing_container
            .close_with_timeout(Duration::from_secs(1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                container.resolve::<SingletonResource>(None).await,
                Err(InjectionError::ContainerClosing)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown did not stop root admission");
    assert!(matches!(
        container.create_scope(ProcessContext::with_process_id(8002)),
        Err(InjectionError::ContainerClosing)
    ));

    scope.close().await.unwrap();
    let report = close.await.unwrap().unwrap();
    assert_eq!(report.scopes_drained, 1);
    assert!(report.outcomes.iter().any(|outcome| {
        outcome.component == "scope:8001" && outcome.status == ShutdownOutcomeStatus::Completed
    }));
    assert!(report.outcomes.iter().any(|outcome| {
        outcome.component.contains("scope:8001:scoped:")
            && outcome.component.contains("ScopedResource")
            && outcome.status == ShutdownOutcomeStatus::Completed
    }));
    assert_eq!(report.cleanup_tasks_joined, 1);
    assert_eq!(SCOPED_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_a_close_waiter_does_not_cancel_container_owned_shutdown() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    BLOCK_SINGLETON_DISPOSE.store(true, Ordering::SeqCst);
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let waiting_container = Arc::clone(&container);
    let waiter = tokio::spawn(async move {
        waiting_container
            .close_with_timeout(Duration::from_secs(2))
            .await
    });

    DISPOSE_ENTERED.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    ALLOW_DISPOSE.notify_waiters();

    container.close().await.unwrap();
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_forces_scope_removal_and_returns_aggregate_shutdown_error() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = ApplicationContainer::build().await.unwrap();
    let scope = container
        .create_scope(ProcessContext::with_process_id(8003))
        .unwrap();
    scope
        .run(container.resolve::<ScopedResource>(None))
        .await
        .unwrap()
        .unwrap();

    let error = container
        .close_with_timeout(Duration::from_millis(10))
        .await
        .unwrap_err();
    match error {
        InjectionError::ShutdownFailed {
            errors,
            outcomes,
            remaining,
        } => {
            assert!(
                errors
                    .iter()
                    .any(|error| error.contains("ShutdownTimedOut"))
            );
            assert!(outcomes.iter().any(|outcome| {
                outcome.component == "active-scope-drain"
                    && outcome.status == ShutdownOutcomeStatus::TimedOut
            }));
            let remaining = remaining.expect("shutdown counters must be retained");
            assert_eq!(remaining.active_scopes, 0);
            assert_eq!(remaining.cleanup_tasks, 0);
        }
        other => panic!("unexpected shutdown error: {other}"),
    }
    assert_eq!(container.active_scope_count(), 0);
    assert!(matches!(
        container.resolve::<SingletonResource>(None).await,
        Err(InjectionError::ContainerClosed)
    ));
    drop(scope);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_deadline_retains_resolution_drain_and_skips_root_disposal() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let context = ProcessContext::with_process_id(8_004);
    let scope = container.create_scope(context.clone()).unwrap();
    let resolving_container = Arc::clone(&container);
    let resolution_context = context.clone();
    let resolution = tokio::spawn(ProcessContext::scope(resolution_context, async move {
        resolving_container
            .resolve::<BlockedScopedInitialization>(None)
            .await
    }));
    SCOPED_INIT_STARTED.notified().await;

    let started = tokio::time::Instant::now();
    let close_error = container
        .close_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "aggregate shutdown must not unboundedly join an unsafe cleanup task"
    );
    match close_error {
        InjectionError::ShutdownFailed {
            outcomes,
            remaining,
            ..
        } => {
            assert!(outcomes.iter().any(|outcome| {
                outcome.component == "scope-cleanup-tasks"
                    && outcome.status == ShutdownOutcomeStatus::TimedOut
                    && outcome
                        .detail
                        .as_deref()
                        .is_some_and(|detail| detail.contains("retained 1"))
            }));
            assert!(outcomes.iter().any(|outcome| {
                outcome.component == "root-lifecycle"
                    && outcome.status == ShutdownOutcomeStatus::Cancelled
            }));
            let remaining = remaining.expect("failed close must retain work counters");
            assert!(remaining.cleanup_tasks >= 1);
            assert!(remaining.active_resolutions >= 1);
        }
        other => panic!("unexpected shutdown error: {other}"),
    }
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 0);

    let waiting_container = Arc::clone(&container);
    let cleanup_waiter = tokio::spawn(async move {
        lily_injection::__private::wait_for_scope_cleanup(&waiting_container, "8004").await;
    });
    tokio::task::yield_now().await;
    assert!(
        !cleanup_waiter.is_finished(),
        "closing scope reservation must outlive the aggregate close result"
    );

    SCOPED_INIT_RELEASE.notify_one();
    let escaped = resolution.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), cleanup_waiter)
        .await
        .expect("late resolution was not reconciled by retained cleanup")
        .unwrap();
    assert_eq!(LATE_SCOPED_DISPOSES.load(Ordering::SeqCst), 0);
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 0);
    drop(escaped);
    drop(scope);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_disposal_timeout_is_structured_and_does_not_detach_work() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    BLOCK_SINGLETON_DISPOSE.store(true, Ordering::SeqCst);
    let container = ApplicationContainer::build().await.unwrap();

    let error = container
        .close_with_timeout(Duration::from_millis(10))
        .await
        .unwrap_err();
    // Consume the notification permit left by the cancelled disposer so it
    // cannot affect another serialized test in this binary.
    DISPOSE_ENTERED.notified().await;
    match error {
        InjectionError::ShutdownFailed { outcomes, .. } => {
            assert!(outcomes.iter().any(|outcome| {
                outcome.component.contains("SingletonResource")
                    && outcome.status == ShutdownOutcomeStatus::TimedOut
            }));
        }
        other => panic!("unexpected shutdown error: {other}"),
    }
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn close_before_preserves_the_callers_absolute_deadline_and_cancels_the_disposer() {
    let _lock = TEST_LOCK.lock().await;
    reset();
    BLOCK_SINGLETON_DISPOSE.store(true, Ordering::SeqCst);
    let container = ApplicationContainer::build().await.unwrap();

    let aggregate_budget = Duration::from_millis(100);
    let earlier_shutdown_work = Duration::from_millis(75);
    let retained_task_start_delay = Duration::from_millis(10);
    let deadline = tokio::time::Instant::now() + aggregate_budget;
    tokio::time::advance(earlier_shutdown_work).await;

    let close = container.close_before(deadline);
    tokio::pin!(close);
    assert!(futures::poll!(close.as_mut()).is_pending());

    // `close_before` has captured the caller's deadline and queued its owned
    // shutdown task, but that task cannot run until this current-thread test
    // yields. Move the paused clock first so recomputing `now + timeout` in
    // that task would incorrectly extend the aggregate budget.
    let delayed_task_start = tokio::time::advance(retained_task_start_delay);
    tokio::pin!(delayed_task_start);
    assert!(futures::poll!(delayed_task_start.as_mut()).is_pending());

    DISPOSE_ENTERED.notified().await;
    assert_eq!(ACTIVE_SINGLETON_DISPOSERS.load(Ordering::SeqCst), 1);

    let remaining = aggregate_budget - earlier_shutdown_work - retained_task_start_delay;
    tokio::time::advance(remaining - Duration::from_millis(1)).await;
    tokio::select! {
        biased;
        result = &mut close => panic!("close finished before the caller's deadline: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(Duration::from_millis(1)).await;
    let error = tokio::select! {
        biased;
        result = &mut close => result.unwrap_err(),
        () = tokio::task::yield_now() => {
            panic!("the retained shutdown task rebased the caller's absolute deadline")
        }
    };
    assert_eq!(tokio::time::Instant::now(), deadline);
    assert_eq!(ACTIVE_SINGLETON_DISPOSERS.load(Ordering::SeqCst), 0);
    assert!(matches!(
        error,
        InjectionError::ShutdownFailed { ref outcomes, .. }
            if outcomes.iter().any(|outcome| {
                outcome.component.contains("SingletonResource")
                    && outcome.status == ShutdownOutcomeStatus::TimedOut
            })
    ));

    ALLOW_DISPOSE.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(
        SINGLETON_DISPOSES.load(Ordering::SeqCst),
        0,
        "the timed-out disposer must be cancelled before close_before returns"
    );
}
