use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Default)]
struct CloseProbe {
    calls: AtomicUsize,
    started: Notify,
    release: Notify,
    dropped: AtomicBool,
    fail: AtomicBool,
}

struct ClosedFuture<'a>(&'a AtomicBool);
impl Drop for ClosedFuture<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[async_trait::async_trait]
impl crate::WebSocketBackplane for CloseProbe {
    async fn new(
        _: Arc<lily_injection::Extensions>,
    ) -> Result<Self, crate::WebSocketBackplaneInitError> {
        Ok(Self::default())
    }
    async fn publish(
        &self,
        _: crate::WebSocketBackplaneFrame,
    ) -> Result<crate::WebSocketBackplanePublishReceipt, crate::WebSocketBackplaneError> {
        Ok(crate::WebSocketBackplanePublishReceipt::accepted())
    }
    async fn receive(
        &self,
        _: crate::WebSocketBackplaneInboundAdmission,
    ) -> Result<crate::WebSocketBackplaneEvent, crate::WebSocketBackplaneError> {
        std::future::pending().await
    }
    async fn close(&self) -> Result<(), crate::WebSocketBackplaneError> {
        let _drop = ClosedFuture(&self.dropped);
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.started.notify_one();
        self.release.notified().await;
        if self.fail.load(Ordering::Acquire) {
            Err(crate::WebSocketBackplaneError::new(
                crate::WebSocketBackplaneErrorKind::Shutdown,
            ))
        } else {
            Ok(())
        }
    }
}

async fn runtime(probe: &Arc<CloseProbe>, timeout: Duration) -> Arc<WsApp> {
    let mut app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    app.shutdown_timeout = timeout;
    for name in [
        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    ] {
        app.lifecycle
            .health
            .register(
                name,
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
            .unwrap();
    }
    app.dispatcher = Arc::new(WebSocketDispatcher::active(
        app.connection_manager.clone(),
        BackplaneRequirement::Required,
        probe.clone(),
        Duration::from_secs(1),
        app.lifecycle.health.clone(),
    ));
    Arc::new(app)
}

#[tokio::test]
async fn managed_server_drop_requests_cooperation_and_retains_the_actual_join() {
    let registry = TaskRegistry::default();
    let admission = CancellationToken::new();
    let message_admission = CancellationToken::new();
    let force = CancellationToken::new();
    let task_force = force.clone();
    let (observed, cancellation_observed) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = ManagedWsServer {
        admission: admission.clone(),
        message_admission: message_admission.clone(),
        force,
        task: registry.track(tokio::spawn(async move {
            task_force.cancelled().await;
            let _ = observed.send(());
            let _ = released.await;
            Ok(())
        })),
        outcome: None,
        runtime_result_observed: false,
    };
    drop(server);
    cancellation_observed.await.unwrap();
    assert!(admission.is_cancelled() && message_admission.is_cancelled());
    assert_eq!(registry.snapshot().outstanding, 1);
    assert_eq!(registry.snapshot().abort_requested, 0);
    release.send(()).unwrap();
    registry.wait().await;
    assert_eq!(registry.snapshot().completed, 1);
    assert_eq!(registry.snapshot().cancelled, 0);
}

#[tokio::test]
async fn publishing_a_terminal_result_is_not_root_task_termination() {
    let app = Arc::new(WsAppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let task_app = app.clone();
    let (release, released) = tokio::sync::oneshot::channel();
    let root = tokio::spawn(async move {
        task_app.container.close().await.unwrap();
        WsApp::complete_lifecycle(&task_app.lifecycle, Ok(())).await;
        let _ = released.await;
    });
    assert!(app.lifecycle.root_task.set(TaskReceipt::from(root)).is_ok());
    assert!(
        timeout(Duration::from_millis(10), app.await_terminal())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    app.await_terminal().await.unwrap();
    assert!(
        app.lifecycle
            .root_task
            .get()
            .unwrap()
            .clone()
            .now_or_never()
            .is_some()
    );
}

#[tokio::test]
async fn dropping_close_waiters_preserves_one_root_and_joins_provider_receipt_drivers() {
    let probe = Arc::new(CloseProbe::default());
    let app = runtime(&probe, Duration::from_secs(2)).await;
    let closing = app.clone();
    let waiter = tokio::spawn(async move { closing.close().await });
    probe.started.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(!probe.dropped.load(Ordering::Acquire));
    probe.release.notify_one();
    let (one, two) = tokio::join!(app.close(), app.close());
    one.unwrap();
    two.unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(probe.dropped.load(Ordering::Acquire));
    assert!(app.framework_tasks_terminal());
    assert!(app.dispatcher.close_terminal());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert!(
        app.lifecycle
            .root_task
            .get()
            .unwrap()
            .clone()
            .now_or_never()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn never_started_pending_provider_uses_one_deadline_and_disposes_di_only_after_join() {
    let probe = Arc::new(CloseProbe::default());
    let total = Duration::from_millis(100);
    let app = runtime(&probe, total).await;
    let started = Instant::now();
    let error = app.close().await.unwrap_err().to_string();
    assert!(error.contains("incomplete"), "{error}");
    assert!(Instant::now().duration_since(started) <= total);
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(probe.dropped.load(Ordering::Acquire));
    let report = app.lifecycle.shutdown_report.get().unwrap();
    assert_eq!(
        report.completion,
        lily_shutdown::FrameworkShutdownCompletion::Incomplete
    );
    assert!(report.forced);
    // Either the provider task observes its cutoff or final abort wins the
    // scheduler race. Both require an actual terminal receipt, never success.
    assert!(matches!(
        report.evidence.backplane,
        crate::reporting::DependencyState::TimedOut | crate::reporting::DependencyState::Cancelled
    ));
    assert!(report.evidence.quiescent());
    assert!(app.dispatcher.close_terminal());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_eq!(app.close().await.unwrap_err().to_string(), error);
}

#[tokio::test]
async fn unjoined_lifecycle_owner_blocks_backplane_di_and_telemetry_cleanup() {
    let probe = Arc::new(CloseProbe::default());
    let app = runtime(&probe, Duration::from_secs(2)).await;
    let now = Instant::now();
    app.lifecycle
        .root_budget
        .configure(now + Duration::from_secs(1), now + Duration::from_secs(2));
    let (release, released) = tokio::sync::oneshot::channel();
    let output = app
        .message_dispatch_registry
        .spawn_owner(Uuid::new_v4(), |slot| async move {
            let _ = slot.run(async {}).await;
            let _ = released.await;
        });
    for dependency in [
        Dependency::Backplane,
        Dependency::Container,
        Dependency::Tracing,
    ] {
        let error = app
            .close_dependency(dependency)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("not terminal"), "{error}");
    }
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    assert!(
        app.container
            .resolve::<lily_config::ConfigService>(None)
            .await
            .is_ok()
    );
    release.send(()).unwrap();
    output.await.unwrap();
    assert!(app.message_dispatch_registry.reconcile().await);
    probe.release.notify_one();
    app.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn crashed_server_children_retain_transport_tail_after_execution_cancellation() {
    let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let now = Instant::now();
    app.scope_cleanup_registry
        .budget
        .configure(now, now + Duration::from_secs(1));
    let force = CancellationToken::new();
    assert!(app.lifecycle.execution_force.set(force.clone()).is_ok());
    let acknowledged = Arc::new(AtomicBool::new(false));
    let observed = acknowledged.clone();
    let mut tasks = OwnedTaskSet::with_registry(app.lifecycle.connection_tasks.clone());
    tasks.spawn(async move {
        force.cancelled().await;
        // Execution can end before its connection's retained cleanup/output.
        // This tail exceeds the 250 ms execution cutoff but fits transport.
        tokio::time::sleep(Duration::from_millis(350)).await;
        observed.store(true, Ordering::Release);
    });
    drop(tasks);
    let server = app
        .lifecycle
        .server_tasks
        .track(tokio::spawn(async { panic!("server crash probe") }));
    assert!(server.await.unwrap_err().is_panic());
    assert!(
        app.reconcile_framework_tasks()
            .await
            .unwrap_err()
            .to_string()
            .contains("panic")
    );
    assert!(acknowledged.load(Ordering::Acquire));
    assert_eq!(now.elapsed(), Duration::from_millis(350));
    assert_eq!(app.lifecycle.connection_tasks.snapshot().completed, 1);
    assert_eq!(app.lifecycle.connection_tasks.snapshot().abort_requested, 0);
    assert!(app.framework_tasks_terminal());
    app.container.close().await.unwrap();
}

#[tokio::test]
async fn force_retry_joins_the_same_provider_after_a_graceful_waiter_is_dropped() {
    let probe = Arc::new(CloseProbe::default());
    let app = runtime(&probe, Duration::from_secs(2)).await;
    let mut action = DependencyHandle {
        runtime: app.runtime_clone(),
        dependency: Dependency::Backplane,
    };
    let now = Instant::now();
    action.set_shutdown_deadlines(now + Duration::from_secs(1), now + Duration::from_secs(2));
    let mut graceful = Box::pin(action.shutdown());
    tokio::select! {
        () = probe.started.notified() => {},
        result = &mut graceful => panic!("provider completed unexpectedly: {result:?}"),
    }
    drop(graceful);
    assert!(!probe.dropped.load(Ordering::Acquire));
    let error = app
        .close_dependency(Dependency::Container)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("not terminal"), "{error}");
    assert!(
        app.container
            .resolve::<lily_config::ConfigService>(None)
            .await
            .is_ok()
    );
    action.set_force_deadline(now + Duration::from_secs(2));
    probe.release.notify_one();
    action.force_shutdown().unwrap().await.unwrap();
    app.close_dependency(Dependency::Container).await.unwrap();
    assert!(app.dispatcher.close_terminal());
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
}

#[tokio::test]
async fn terminal_provider_failure_allows_di_disposal_but_remains_a_failure() {
    let probe = Arc::new(CloseProbe::default());
    probe.fail.store(true, Ordering::Release);
    probe.release.notify_one();
    let app = runtime(&probe, Duration::from_secs(2)).await;
    let error = app.close().await.unwrap_err().to_string();
    assert!(error.contains("incomplete"), "{error}");
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert!(app.dispatcher.close_terminal());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &app.container
    ));
    assert_eq!(app.close().await.unwrap_err().to_string(), error);
    let report = app.lifecycle.shutdown_report.get().unwrap();
    assert_eq!(
        report.completion,
        lily_shutdown::FrameworkShutdownCompletion::Incomplete
    );
    assert_eq!(
        report.evidence.backplane,
        crate::reporting::DependencyState::Failed
    );
    assert_eq!(
        report.evidence.container,
        crate::reporting::DependencyState::Completed
    );
    assert!(report.evidence.quiescent());
    assert!(report.evidence.reconciles());
}

#[tokio::test]
async fn expired_root_does_not_spawn_a_new_provider_cleanup_task() {
    let probe = Arc::new(CloseProbe::default());
    let app = runtime(&probe, Duration::from_secs(1)).await;
    let now = Instant::now();
    app.lifecycle.root_budget.configure(now, now);
    app.dispatcher.set_shutdown_deadlines(now, now);
    let error = app
        .close_dependency(Dependency::Backplane)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("root deadline"), "{error}");
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    assert!(!app.dispatcher.close_terminal());
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    app.container.close().await.unwrap();
}

#[tokio::test]
async fn a_joined_message_owner_panic_is_not_erased_by_reconciliation_retries() {
    let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let now = Instant::now();
    app.scope_cleanup_registry
        .budget
        .configure(now, now + Duration::from_secs(1));
    let output = app
        .message_dispatch_registry
        .spawn_owner(Uuid::new_v4(), |slot| async move {
            drop(slot);
            panic!("message owner panic probe");
        });
    assert!(output.await.is_err());
    for _ in 0..2 {
        let error = app
            .reconcile_framework_tasks()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("panic"), "{error}");
        assert!(
            app.framework_tasks_terminal(),
            "termination differs from successful cleanup"
        );
    }
    app.container.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_close_callers_observe_an_installed_and_joined_root() {
    let app = Arc::new(WsAppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let mut waiters = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let closing = app.clone();
        waiters.spawn(async move {
            closing.close().await.unwrap();
            let root = closing
                .lifecycle
                .root_task
                .get()
                .expect("root receipt must precede publication");
            assert!(root.clone().now_or_never().is_some());
            assert!(closing.framework_tasks_terminal());
        });
    }
    while let Some(result) = waiters.join_next().await {
        result.unwrap();
    }
}
