use super::*;
use futures::stream::{FuturesUnordered, StreamExt};

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Singleton")]
struct DependencyProbe {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    mode: std::sync::atomic::AtomicUsize,
    started: tokio::sync::Notify,
    release: CancellationToken,
    disposed: CancellationToken,
}

static BUILD_ENTERED: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Arc<DependencyProbe>>>> =
    std::sync::Mutex::new(None);

struct PendingBuildMiddleware;
#[async_trait::async_trait]
impl HttpMiddleware for PendingBuildMiddleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        let probe = extensions
            .get_service::<DependencyProbe>(None)
            .await
            .unwrap();
        BUILD_ENTERED
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(probe)
            .ok();
        std::future::pending().await
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "phase7-pending-constructor",
            lily_middleware::MiddlewareKind::Custom,
        )
    }
    async fn handle(
        &self,
        _: &mut HttpExchange<'_>,
        _: lily_middleware::HttpNext<'_>,
        _: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        unreachable!()
    }
}

#[tokio::test]
async fn dropping_pending_http_build_preserves_owned_di_rollback() {
    let (started, ready) = tokio::sync::oneshot::channel();
    *BUILD_ENTERED.lock().unwrap() = Some(started);
    let mut build = Box::pin(
        AppBuilder::new("127.0.0.1:0")
            .middleware::<PendingBuildMiddleware>()
            .build(),
    );
    let probe = tokio::select! {
        result = &mut build => panic!("pending constructor returned: {:?}", result.err()),
        probe = ready => probe.unwrap(),
    };
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    drop(build);
    tokio::time::timeout(Duration::from_secs(2), probe.started.notified())
        .await
        .unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    // Consume the process inventory's actual joins, including the root created
    // by Drop. Merely observing the disposer body above was insufficient.
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        BUILD_ROLLBACK_TASKS.get().unwrap().wait()
    )
    .await
    .unwrap()
    .is_terminal());
}

#[async_trait::async_trait]
impl lily_injection::ServiceTrait for DependencyProbe {
    async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
        if self.mode.load(Ordering::Acquire) == 3 {
            self.release.cancel();
            std::future::pending().await
        } else {
            Ok(())
        }
    }
    async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.disposed.cancel();
        self.started.notify_one();
        match self.mode.load(Ordering::Acquire) {
            1 => self.release.cancelled().await,
            2 => {
                return Err(lily_error::injection::InjectionError::DisposeError(
                    "phase7 probe".into(),
                ))
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn cancelled_build_guard_retains_the_exact_pending_di_transaction() {
    let probe = DependencyProbe::default();
    probe.mode.store(3, Ordering::Release);
    let entered = probe.release.clone();
    let disposed = probe.disposed.clone();
    let calls = probe.calls.clone();
    let mut guard = HttpBuildGuard::new(None, true);
    let mut build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder().seed_singleton(probe),
    );
    build.reserve_rollback_tail(5);
    guard.di_build = Some(build);
    tokio::select! {
        result = guard.di_build.as_mut().unwrap().wait() => panic!("unexpected build result: {result:?}"),
        () = entered.cancelled() => {}
    }
    assert!(!disposed.is_cancelled());
    drop(guard);
    tokio::time::timeout(Duration::from_secs(2), disposed.cancelled())
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        BUILD_ROLLBACK_TASKS.get().unwrap().wait()
    )
    .await
    .unwrap()
    .is_terminal());
    assert_eq!(calls.load(Ordering::Acquire), 1);
}

async fn short_owned_app() -> App {
    let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let lifecycle = Arc::get_mut(&mut app.lifecycle).unwrap();
    lifecycle.budget =
        ShutdownBudget::for_state(Duration::from_secs(1), lifecycle.shutdown_state.clone());
    app
}

#[tokio::test(start_paused = true)]
async fn root_panic_recovery_preserves_cooperation_and_reserves_actual_transport_joins() {
    for cooperative in [true, false] {
        let app = short_owned_app().await;
        let force = app.transport_force();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let task = app.lifecycle.tasks.protocol.spawn(async move {
            entered.send(()).unwrap();
            force.cancelled().await;
            if cooperative {
                tokio::time::sleep(Duration::from_millis(40)).await;
            } else {
                std::future::pending::<()>().await;
            }
        });
        ready.await.unwrap();
        let started = tokio::time::Instant::now();
        let recovery = app.recover_transport_after_root_panic();
        tokio::pin!(recovery);
        assert!(recovery.as_mut().now_or_never().is_none());
        assert!(app.transport_force().is_cancelled());
        let deadlines = app.shutdown_budget().deadlines().unwrap();
        let before = if cooperative { 39 } else { 879 };
        tokio::time::sleep(Duration::from_millis(before)).await;
        assert!(recovery.as_mut().now_or_never().is_none());
        assert_eq!(task.snapshot().abort_requested, 0);
        assert_eq!(task.snapshot().outstanding, 1);
        assert!(!lily_injection::__private::container_shutdown_started(
            app.container()
        ));
        recovery.await.unwrap();
        assert_eq!(
            tokio::time::Instant::now(),
            if cooperative {
                started + Duration::from_millis(40)
            } else {
                deadlines.at(ShutdownStage::TransportStop)
            }
        );
        let outcome = task.await;
        assert_eq!(outcome.is_ok(), cooperative);
        let evidence = app.lifecycle.tasks.protocol.snapshot();
        assert_eq!(evidence.abort_requested, usize::from(!cooperative));
        assert_eq!(evidence.cancelled, usize::from(!cooperative));
        assert_eq!(evidence.completed, usize::from(cooperative));
        assert_eq!(evidence.outstanding, 0);
        assert!(tokio::time::Instant::now() < deadlines.at(ShutdownStage::Reconcile));
        assert!(!lily_injection::__private::container_shutdown_started(
            app.container()
        ));
        app.close().await.unwrap();
        assert!(lily_injection::__private::container_shutdown_quiescent(
            app.container()
        ));
    }
}

#[tokio::test]
async fn force_before_any_component_still_closes_owned_di_once() {
    let app = short_owned_app().await;
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    app.lifecycle.shutdown_state.request_force();
    app.close().await.unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    app.close().await.unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert_joined(&app);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_abort_pending_destructor_blocks_dependencies_and_preserves_incomplete_report() {
    struct PendingProtocolDrop {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Drop for PendingProtocolDrop {
        fn drop(&mut self) {
            self.entered.take().unwrap().send(()).unwrap();
            self.release.recv().unwrap();
        }
    }
    let app = short_owned_app().await;
    let (release, wait) = std::sync::mpsc::channel();
    let emergency = release.clone();
    let (watchdog_done, watchdog_wait) = std::sync::mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        if watchdog_wait.recv_timeout(Duration::from_secs(3)).is_err() {
            let _ = emergency.send(());
            true
        } else {
            false
        }
    });
    let (dropping, dropped) = tokio::sync::oneshot::channel();
    let (entered, started) = tokio::sync::oneshot::channel();
    let capture = PendingProtocolDrop {
        entered: Some(dropping),
        release: wait,
    };
    let protocol = app.lifecycle.tasks.protocol.clone();
    let (published, receipt) = tokio::sync::oneshot::channel();
    // Give the root executor independent progress while deliberately blocking
    // a worker inside Drop. This is a real framework-registered Tokio task and
    // JoinHandle, not injected termination evidence or a simulated counter.
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let task = protocol.spawn(async move {
                let _capture = capture;
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            published.send(task.clone()).ok().unwrap();
            let _ = task.await;
        });
    });
    let task = receipt.await.unwrap();
    started.await.unwrap();
    let recovery_app = app.clone();
    let recovery =
        tokio::spawn(async move { recovery_app.recover_transport_after_root_panic().await });
    dropped.await.unwrap();
    let recovery_result = recovery.await.unwrap();
    let close_result = app.close().await;
    let before = app.lifecycle.tasks.protocol.snapshot();
    let started_di = lily_injection::__private::container_shutdown_started(app.container());
    let frozen = app.shutdown_report().unwrap();
    // Always release the actual destructor before an assertion can panic.
    let _ = release.send(());
    let _ = watchdog_done.send(());
    let emergency_released = watchdog.join().unwrap();
    let joined = task.await;
    worker.join().unwrap();
    assert!(
        !emergency_released,
        "qualification watchdog had to release a stuck destructor"
    );
    assert!(joined.unwrap_err().is_cancelled());
    assert!(recovery_result.is_err());
    assert!(close_result.is_err());
    assert_eq!(before.abort_requested, 1);
    assert_eq!(before.cancelled, 0);
    assert_eq!(before.outstanding, 1);
    assert!(!started_di);
    assert!(!frozen.terminal());
    assert_eq!(frozen.protocol.outstanding, 1);
    assert_eq!(frozen.protocol.cancelled, 0);
    assert!(app.close().await.is_err());
    assert_eq!(app.shutdown_report().unwrap(), frozen);
    assert_eq!(app.lifecycle.tasks.protocol.snapshot().cancelled, 1);
    assert_eq!(app.lifecycle.tasks.protocol.snapshot().outstanding, 0);
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    // Test teardown owns the explicit recovery; a failed HTTP attempt cannot
    // secretly start this disposal or rewrite its earlier incomplete report.
    app.container.close().await.unwrap();
    app.lifecycle
        .dependencies
        .reconcile(&app.lifecycle.budget)
        .await
        .unwrap();
}

#[tokio::test]
async fn transport_receipt_is_a_barrier_before_owned_di_close() {
    let app = short_owned_app().await;
    let (release, wait) = tokio::sync::oneshot::channel();
    let receipt = app.lifecycle.tasks.protocol.spawn(async move {
        wait.await.unwrap();
    });
    let observer = app.clone();
    let shutdown = tokio::spawn(async move { observer.close().await });
    while app.lifecycle.budget.deadlines().is_none() {
        tokio::task::yield_now().await;
    }
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    assert_eq!(app.lifecycle.tasks.protocol.snapshot().outstanding, 1);
    release.send(()).unwrap();
    receipt.await.unwrap();
    shutdown.await.unwrap().unwrap();
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
}

#[tokio::test(start_paused = true)]
async fn outstanding_transport_blocks_di_and_replay_cannot_start_it_after_h() {
    let app = short_owned_app().await;
    let receipt = app
        .lifecycle
        .tasks
        .protocol
        .spawn(std::future::pending::<()>());
    let start = tokio::time::Instant::now();
    let error = app.close().await.unwrap_err();
    assert!(tokio::time::Instant::now() <= start + Duration::from_secs(1));
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    receipt.abort();
    assert!(receipt.await.unwrap_err().is_cancelled());
    assert_eq!(
        app.close().await.unwrap_err().to_string(),
        error.to_string()
    );
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    // Explicit test teardown is outside the frozen HTTP attempt.
    app.container.close().await.unwrap();
    app.lifecycle
        .dependencies
        .reconcile(&app.lifecycle.budget)
        .await
        .unwrap();
}

#[tokio::test]
async fn joined_monitor_panic_is_failure_but_allows_dependency_cleanup() {
    let app = short_owned_app().await;
    let receipt = app.lifecycle.tasks.monitors.spawn(async {
        panic!("phase7 monitor panic");
    });
    assert!(receipt.await.unwrap_err().is_panic());
    assert!(app.close().await.is_err());
    assert!(app.lifecycle.tasks.monitors.snapshot().is_terminal());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
}

#[tokio::test]
async fn monitor_future_drops_before_di_begins() {
    struct MonitorDrop(Arc<ApplicationContainer>, Arc<AtomicBool>);
    impl Drop for MonitorDrop {
        fn drop(&mut self) {
            assert!(!lily_injection::__private::container_shutdown_started(
                &self.0
            ));
            self.1.store(true, Ordering::Release);
        }
    }
    let app = short_owned_app().await;
    let dropped = Arc::new(AtomicBool::new(false));
    let capture = MonitorDrop(app.container.clone(), dropped.clone());
    let (started, ready) = tokio::sync::oneshot::channel();
    app.lifecycle.tasks.monitors.spawn(async move {
        let _capture = capture;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    app.close().await.unwrap();
    assert!(dropped.load(Ordering::Acquire));
    assert_joined(&app);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_request_cannot_authorize_di_while_monitor_destruction_is_pending() {
    struct PendingDrop(
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    );
    impl Drop for PendingDrop {
        fn drop(&mut self) {
            // Move the sender without making destruction itself recursive.
            let (placeholder, _) = tokio::sync::oneshot::channel();
            let _ = std::mem::replace(&mut self.0, placeholder).send(());
            self.1.recv().unwrap();
        }
    }
    let app = short_owned_app().await;
    let (release, wait) = std::sync::mpsc::channel();
    let (dropping, dropped) = tokio::sync::oneshot::channel();
    let capture = PendingDrop(dropping, wait);
    let (started, ready) = tokio::sync::oneshot::channel();
    let receipt = app.lifecycle.tasks.monitors.spawn(async move {
        let _capture = capture;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    dropped.await.unwrap();
    let result = close.await.unwrap();
    // Always release the real blocked destructor before assertions can panic.
    let started_di = lily_injection::__private::container_shutdown_started(&app.container);
    let before_join = app.lifecycle.tasks.monitors.snapshot();
    release.send(()).unwrap();
    assert!(receipt.await.unwrap_err().is_cancelled());
    assert!(result.is_err());
    assert!(!started_di);
    assert_eq!(before_join.abort_requested, 1);
    assert_eq!(before_join.outstanding, 1);
    assert_eq!(before_join.cancelled, 0);
    app.container.close().await.unwrap();
    app.lifecycle
        .dependencies
        .reconcile(&app.lifecycle.budget)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn pending_root_disposer_uses_d_and_never_replays_as_success() {
    let app = short_owned_app().await;
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    probe.mode.store(1, Ordering::Release);
    let start = tokio::time::Instant::now();
    let error = app.close().await.unwrap_err();
    assert!(tokio::time::Instant::now() <= start + Duration::from_secs(1));
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    probe.release.cancel();
    assert_eq!(
        app.close().await.unwrap_err().to_string(),
        error.to_string()
    );
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
}

#[tokio::test]
async fn cancelled_di_waiter_and_force_share_the_original_close_receipt() {
    let app = short_owned_app().await;
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    probe.mode.store(1, Ordering::Release);
    app.lifecycle.begin_shutdown();
    let mut handle = HttpDependencyHandle {
        lifecycle: app.lifecycle.clone(),
        phase: FrameworkShutdownPhase::DisposeDependencies,
        timeout: Duration::from_secs(1),
    };
    let mut waiter = Box::pin(handle.shutdown());
    assert!(waiter.as_mut().now_or_never().is_none());
    probe.started.notified().await;
    drop(waiter);
    assert!(!lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    probe.release.cancel();
    handle.force_shutdown().unwrap().await.unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    app.close().await.unwrap();
}

#[tokio::test]
async fn returned_disposal_error_is_terminal_and_stays_failed_on_replay() {
    let app = short_owned_app().await;
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    probe.mode.store(2, Ordering::Release);
    let error = app.close().await.unwrap_err();
    assert!(error.to_string().contains("phase7 probe"));
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        app.close().await.unwrap_err().to_string(),
        error.to_string()
    );
}

#[tokio::test]
async fn dropped_build_rollback_waiter_keeps_the_dependency_receipt() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let probe = container.resolve::<DependencyProbe>(None).await.unwrap();
    probe.mode.store(1, Ordering::Release);
    let budget = ShutdownBudget::from_started(Duration::from_secs(2), tokio::time::Instant::now());
    let dependencies = HttpDependencies::new(container.clone(), true, None);
    let mut rollback = Box::pin(run_build_rollback(
        AppBuildError::CsrfInitialization,
        dependencies.clone(),
        budget.clone(),
    ));
    assert!(rollback.as_mut().now_or_never().is_none());
    probe.started.notified().await;
    drop(rollback);
    assert!(!lily_injection::__private::container_shutdown_quiescent(
        &container
    ));
    probe.release.cancel();
    dependencies.close_di(&budget).await.unwrap();
    dependencies.reconcile(&budget).await.unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
}

#[tokio::test(start_paused = true)]
async fn failed_build_rollback_cannot_multiply_the_total_budget() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let probe = container.resolve::<DependencyProbe>(None).await.unwrap();
    probe.mode.store(1, Ordering::Release);
    let start = tokio::time::Instant::now();
    let error = rollback_failed_build(
        AppBuildError::CsrfInitialization,
        &container,
        true,
        Duration::from_secs(1),
        None,
    )
    .await;
    assert!(matches!(error, AppBuildError::StartupRollback { .. }));
    assert!(tokio::time::Instant::now() <= start + Duration::from_secs(1));
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
}

async fn wait_ready(app: &App) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !app.lifecycle.health.snapshot().unwrap().ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed root must publish readiness");
}

fn assert_joined(app: &App) {
    assert!(app.lifecycle.root_join_observed.load(Ordering::Acquire));
    let root = app.lifecycle.root_tasks.snapshot();
    assert_eq!(root.registered, 1);
    assert_eq!(root.completed, 1);
    for registry in [
        &app.lifecycle.tasks.listener,
        &app.lifecycle.tasks.connections,
        &app.lifecycle.tasks.protocol,
        &app.lifecycle.tasks.monitors,
    ] {
        assert!(registry.snapshot().is_terminal());
    }
}

#[tokio::test(start_paused = true)]
async fn native_signal_bounds_a_waiter_even_when_the_root_cannot_observe_it() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let mut app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    let source = app.lifecycle.shutdown_state.clone();
    Arc::get_mut(&mut app.lifecycle).unwrap().budget =
        ShutdownBudget::for_state(Duration::from_secs(1), source.clone());
    let root = app
        .lifecycle
        .root_tasks
        .spawn(std::future::pending::<HttpLifecycleOutcome>());
    *app.lifecycle.root.lock().unwrap() = Some(root.clone());
    let observer = app.clone();
    let receipt = root.clone();
    let waiter = tokio::spawn(async move { observer.await_root(receipt).await });
    tokio::task::yield_now().await;
    let started = tokio::time::Instant::now();
    source.initiate_shutdown(ShutdownSignal::Interrupt).unwrap();
    let error = waiter.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        tokio::time::Instant::now(),
        started + Duration::from_secs(1)
    );
    assert_eq!(app.lifecycle.root_tasks.snapshot().outstanding, 1);
    root.abort();
    assert!(root.await.unwrap_err().is_cancelled());
    container
        .close_with_timeout(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn concurrent_close_without_start_uses_one_root_and_never_binds() {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let app = AppBuilder::new(&reserved.local_addr().unwrap().to_string())
        .build()
        .await
        .unwrap();
    let mut waiters = FuturesUnordered::new();
    for _ in 0..16 {
        let observer = app.clone();
        waiters.push(tokio::spawn(async move { observer.close().await }));
    }
    while let Some(result) = waiters.next().await {
        result.unwrap().unwrap();
    }
    assert_eq!(app.bound_address(), None);
    assert_eq!(app.lifecycle.tasks.listener.snapshot().registered, 0);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_joined(&app);
    let deadline = app
        .lifecycle
        .budget
        .deadlines()
        .unwrap()
        .at(ShutdownStage::Final);
    app.close().await.unwrap();
    assert_eq!(
        app.lifecycle
            .budget
            .deadlines()
            .unwrap()
            .at(ShutdownStage::Final),
        deadline
    );
    assert_eq!(
        app.clone()
            .start_with_cancellation(CancellationToken::new())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

#[tokio::test]
async fn dropped_start_waiter_requests_shutdown_and_preserves_all_root_joins() {
    let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let runtime = app.clone();
    let cancellation = CancellationToken::new();
    let input = cancellation.clone();
    let waiter = tokio::spawn(async move { runtime.start_with_cancellation(input).await });
    wait_ready(&app).await;
    let connection = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    // Wait until the accept task has registered the connection's actual join.
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.lifecycle.tasks.connections.snapshot().registered == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), app.close())
        .await
        .unwrap()
        .unwrap();
    assert!(!cancellation.is_cancelled());
    assert_eq!(app.lifecycle.tasks.listener.snapshot().registered, 1);
    assert_eq!(app.lifecycle.tasks.monitors.snapshot().registered, 1);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_joined(&app);
    drop(connection);
}

#[tokio::test]
async fn dropped_close_waiter_keeps_pending_work_owned_and_preserves_caller_di() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    // Exercise root reconciliation with a still-owned listener receipt without
    // introducing a request/body owner from a later phase.
    let release = CancellationToken::new();
    let token = release.clone();
    app.lifecycle
        .tasks
        .listener
        .spawn(async move { token.cancelled().await });
    let observer = app.clone();
    let waiter = tokio::spawn(async move { observer.close().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.lifecycle.root_tasks.snapshot().registered == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(app.lifecycle.tasks.listener.snapshot().outstanding, 1);
    assert_eq!(app.lifecycle.root_tasks.snapshot().outstanding, 1);
    release.cancel();
    app.close().await.unwrap();
    assert!(!lily_injection::__private::container_shutdown_started(
        &container
    ));
    assert_joined(&app);
    container
        .close_with_timeout(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_bind_result_is_replayed_after_confirmed_root_join() {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let app = AppBuilder::new(&reserved.local_addr().unwrap().to_string())
        .build()
        .await
        .unwrap();
    let error = app
        .clone()
        .start_with_cancellation(CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    let replay = app.close().await.unwrap_err();
    assert_eq!(replay.kind(), error.kind());
    assert_eq!(replay.to_string(), error.to_string());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_joined(&app);
}

#[tokio::test]
async fn simultaneous_starts_share_one_claim_without_cancelling_the_winner() {
    let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let stop = CancellationToken::new();
    stop.cancel();
    let (first, second) = tokio::join!(
        app.clone().start_with_cancellation(stop.clone()),
        app.clone().start_with_cancellation(stop),
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let error = results.into_iter().find_map(Result::err).unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    app.close().await.unwrap();
    assert_joined(&app);
}

#[tokio::test(start_paused = true)]
async fn late_root_join_cannot_rewrite_an_already_frozen_timeout() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let mut app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    Arc::get_mut(&mut app.lifecycle).unwrap().budget = ShutdownBudget::new(Duration::from_secs(1));
    let release = CancellationToken::new();
    let token = release.clone();
    let root = app.lifecycle.root_tasks.spawn(async move {
        token.cancelled().await;
        HttpLifecycleOutcome::Completed
    });
    *app.lifecycle.root.lock().unwrap() = Some(root.clone());
    let timeout = app.close().await.unwrap_err();
    assert_eq!(timeout.kind(), io::ErrorKind::TimedOut);
    assert!(!app.lifecycle.root_join_observed.load(Ordering::Acquire));
    assert_eq!(app.lifecycle.root_tasks.snapshot().outstanding, 1);
    let frozen = app.shutdown_report().unwrap();
    assert_eq!(frozen.root.outstanding, 1);
    assert_eq!(
        frozen.completion,
        crate::shutdown_report::HttpShutdownCompletion::Incomplete
    );
    assert!(frozen.reconciles());
    let deadline = app
        .lifecycle
        .budget
        .deadlines()
        .unwrap()
        .at(ShutdownStage::Final);
    release.cancel();
    root.await.unwrap();
    let replay = app.close().await.unwrap_err();
    assert_eq!(replay.to_string(), timeout.to_string());
    assert_eq!(app.shutdown_report().unwrap(), frozen);
    assert_eq!(
        app.lifecycle
            .health
            .snapshot()
            .unwrap()
            .checks
            .iter()
            .find(|check| check.name == "http.shutdown")
            .unwrap()
            .reason_code,
        "incomplete"
    );
    assert_eq!(
        app.lifecycle
            .budget
            .deadlines()
            .unwrap()
            .at(ShutdownStage::Final),
        deadline
    );
    assert_joined(&app);
    container
        .close_with_timeout(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn http_report_distinguishes_root_abort_request_from_its_confirmed_join() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    let root = app
        .lifecycle
        .root_tasks
        .spawn(std::future::pending::<HttpLifecycleOutcome>());
    *app.lifecycle.root.lock().unwrap() = Some(root.clone());
    app.lifecycle.begin_shutdown();
    root.abort();
    let pending = app.lifecycle.observe_report(root.snapshot(), false);
    assert_eq!(pending.root.abort_requested, 1);
    assert_eq!(pending.root.cancelled, 0);
    assert_eq!(
        pending.completion,
        crate::shutdown_report::HttpShutdownCompletion::Incomplete
    );
    assert!(root.clone().await.unwrap_err().is_cancelled());
    app.close().await.unwrap_err();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.root.abort_requested, 1);
    assert_eq!(report.root.cancelled, 1);
    assert!(report.terminal() && report.reconciles());
    assert_eq!(
        report.completion,
        crate::shutdown_report::HttpShutdownCompletion::TerminalFailed
    );
    container.close().await.unwrap();
}

#[tokio::test]
async fn health_observer_finalizes_a_join_after_the_close_waiter_and_app_are_gone() {
    let app = short_owned_app().await;
    let health = app.lifecycle.health.clone();
    let weak = Arc::downgrade(&app.lifecycle);
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    probe.mode.store(1, Ordering::Release);
    let mut close = Box::pin(app.close());
    assert!(close.as_mut().now_or_never().is_none());
    probe.started.notified().await;
    drop(close);
    let root = app.lifecycle.root.lock().unwrap().clone().unwrap();
    probe.release.cancel();
    root.await.unwrap();
    assert!(app.shutdown_report().is_none(), "no final observer has run");
    drop(app);
    assert!(
        weak.upgrade().is_none(),
        "health must not retain the App/DI graph"
    );
    let snapshot = health.snapshot().unwrap();
    assert!(snapshot.live && !snapshot.ready);
    assert_eq!(
        snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.shutdown")
            .unwrap()
            .reason_code,
        "graceful_completed"
    );
    assert_eq!(
        health.snapshot().unwrap(),
        snapshot,
        "replay cannot change generation"
    );
}

#[test]
fn completed_root_health_receipt_can_be_observed_without_a_tokio_context() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let health = runtime.block_on(async {
        let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let health = app.lifecycle.health.clone();
        app.lifecycle.begin_shutdown();
        app.install_root(HttpRootMode::Close, false)
            .unwrap()
            .await
            .unwrap();
        assert!(app.shutdown_report().is_none());
        health
    });
    let snapshot = health.snapshot().unwrap();
    assert_eq!(
        snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.shutdown")
            .unwrap()
            .reason_code,
        "graceful_completed"
    );
    assert!(snapshot.live);
}

#[tokio::test]
async fn report_and_health_keep_failed_disposal_separate_from_terminal_resources() {
    let app = short_owned_app().await;
    let probe = app
        .container
        .resolve::<DependencyProbe>(None)
        .await
        .unwrap();
    probe.mode.store(2, Ordering::Release);
    app.close().await.unwrap_err();
    let report = app.shutdown_report().unwrap();
    assert_eq!(
        report.dependencies.di.disposition,
        crate::shutdown_report::DependencyDisposition::Failed
    );
    assert_eq!(report.dependencies.di.tasks.completed, 1);
    assert!(report.terminal() && report.reconciles());
    assert_eq!(
        report.completion,
        crate::shutdown_report::HttpShutdownCompletion::TerminalFailed
    );
    let health = app.lifecycle.health.snapshot().unwrap();
    assert!(!health.live && !health.ready);
    assert_eq!(
        health
            .checks
            .iter()
            .find(|check| check.name == "http.shutdown")
            .unwrap()
            .reason_code,
        "terminal_failed"
    );
    let text = format!("{health:?} {report:?}");
    assert!(
        !text.contains("phase7 probe"),
        "provider error text is not a health/report label"
    );
    let _ = app.close().await;
    assert_eq!(app.shutdown_report().unwrap(), report);
    assert_eq!(app.lifecycle.health.snapshot().unwrap(), health);
}

#[tokio::test(start_paused = true)]
async fn a_first_late_health_observation_cannot_claim_a_join_within_the_deadline() {
    let app = short_owned_app().await;
    app.lifecycle.begin_shutdown();
    app.install_root(HttpRootMode::Close, false)
        .unwrap()
        .await
        .unwrap();
    assert!(app.shutdown_report().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let health = app.lifecycle.health.snapshot().unwrap();
    assert!(!health.live);
    let report = app.shutdown_report().unwrap();
    assert!(report.terminal() && report.deadline_expired);
    assert_eq!(
        report.completion,
        crate::shutdown_report::HttpShutdownCompletion::TerminalFailed
    );
    assert_eq!(
        app.close().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(app.shutdown_report().unwrap(), report);
}
