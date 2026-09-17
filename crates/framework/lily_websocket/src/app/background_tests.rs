//! Real AppBuilder/listener/shutdown ownership, with user-supplied workers.
use super::*;
use crate::BackgroundCancellation as ExecutionCancellation;
use crate::reporting::DependencyState;
use lily_injection::{
    ApplicationScopeFactory, Injectable, InjectionError, ProcessContext, ServiceTrait,
};
use lily_shutdown::FrameworkShutdownCompletion;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct BackgroundProbe {
    events: Mutex<Vec<(&'static str, u64)>>,
    changed: Notify,
    release: CancellationToken,
    dispose_release: CancellationToken,
    block_disposal: AtomicBool,
    fail_disposal: AtomicBool,
    subscription_release: CancellationToken,
    active: AtomicUsize,
    drop_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
impl BackgroundProbe {
    fn event(&self, name: &'static str) {
        self.events
            .lock()
            .unwrap()
            .push((name, ProcessContext::current().map_or(0, |c| c.process_id)));
        self.changed.notify_waiters();
    }
    fn count(&self, name: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == name)
            .count()
    }
    async fn until(&self, name: &str, count: usize) {
        bounded(async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.count(name) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await;
    }
    fn before(&self, first: &str, second: &str) {
        let events = self.events.lock().unwrap();
        assert!(
            events.iter().rposition(|e| e.0 == first).unwrap()
                < events.iter().position(|e| e.0 == second).unwrap(),
            "{events:?}"
        );
    }
}
#[async_trait::async_trait]
impl ServiceTrait for BackgroundProbe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(
            self.active.load(Ordering::Acquire),
            0,
            "DI disposed before background scope values were released"
        );
        self.event("di-disposed");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct BackgroundScoped {
    #[inject]
    probe: Arc<BackgroundProbe>,
    id: u64,
}
#[async_trait::async_trait]
impl ServiceTrait for BackgroundScoped {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = ProcessContext::current().unwrap().process_id;
        self.probe.active.fetch_add(1, Ordering::AcqRel);
        self.probe.event("scope-created");
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(ProcessContext::current().unwrap().process_id, self.id);
        self.probe.event("disposing");
        if self.probe.block_disposal.load(Ordering::Acquire) {
            self.probe.dispose_release.cancelled().await;
        }
        self.probe.event("disposed");
        if self.probe.fail_disposal.load(Ordering::Acquire) {
            return Err(InjectionError::DisposeError("test scope disposer".into()));
        }
        Ok(())
    }
}
impl Drop for BackgroundScoped {
    fn drop(&mut self) {
        if self.id != 0 {
            self.probe.active.fetch_sub(1, Ordering::AcqRel);
            self.probe.event("scope-dropped");
        }
    }
}
struct WorkerDrop(Arc<BackgroundProbe>);
impl Drop for WorkerDrop {
    fn drop(&mut self) {
        self.0.event("execution-dropped");
    }
}
struct BlockingDrop(Arc<BackgroundProbe>, std::sync::mpsc::Receiver<()>);
impl Drop for BlockingDrop {
    fn drop(&mut self) {
        self.0.event("drop-blocked");
        self.1.recv().unwrap();
        self.0.event("drop-released");
    }
}

struct BackgroundWorker<const MODE: u8, const ID: u8 = 0> {
    scopes: Arc<ApplicationScopeFactory>,
    probe: Arc<BackgroundProbe>,
}
#[async_trait::async_trait]
impl<const MODE: u8, const ID: u8> BackgroundServiceTrait for BackgroundWorker<MODE, ID> {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        let probe = scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move { extensions.get_service::<BackgroundProbe>(None).await })
            })
            .await?;
        probe.event("constructed");
        if MODE == 5 {
            return Err(InjectionError::DisposeError("test constructor".into()));
        }
        if MODE == 6 {
            scopes
                .create_scope(ProcessContext::new())?
                .run(|extensions| {
                    Box::pin(async move {
                        extensions.get_service::<BackgroundScoped>(None).await?;
                        std::future::pending::<Result<(), InjectionError>>().await
                    })
                })
                .await?;
        }
        Ok(Self { scopes, probe })
    }
    async fn execute_async(&mut self, token: ExecutionCancellation) -> Result<(), Self::Error> {
        let _guard = WorkerDrop(self.probe.clone());
        let _blocking = if MODE == 7 {
            Some(BlockingDrop(
                self.probe.clone(),
                self.probe.drop_gate.lock().unwrap().take().unwrap(),
            ))
        } else {
            None
        };
        assert!(!token.is_cancelled());
        self.probe.event("executing");
        if MODE == 4 {
            return Ok(());
        }
        if MODE == 7 {
            std::future::pending::<()>().await;
        }
        if MODE == 2 || MODE == 3 {
            self.probe.release.cancelled().await;
            if MODE == 3 {
                panic!("test worker panic");
            }
            return Err(InjectionError::DisposeError("test worker".into()));
        }
        self.scopes
            .create_scope(ProcessContext::new())?
            .run(move |extensions| {
                Box::pin(async move {
                    let service = extensions.get_service::<BackgroundScoped>(None).await?;
                    let repeated = extensions.get_service::<BackgroundScoped>(None).await?;
                    assert!(Arc::ptr_eq(&service, &repeated));
                    if MODE == 1 {
                        std::future::pending::<()>().await;
                    }
                    token.cancelled().await;
                    service.probe.event("cancel-observed");
                    Ok::<_, InjectionError>(())
                })
            })
            .await?;
        if MODE == 9 {
            assert_eq!(self.probe.count("cancel-observed"), 1);
            self.scopes
                .create_scope(ProcessContext::new())?
                .run(|extensions| {
                    Box::pin(async move {
                        let service = extensions.get_service::<BackgroundScoped>(None).await?;
                        service.probe.event("finalization-created");
                        tokio::task::yield_now().await;
                        Ok::<_, InjectionError>(())
                    })
                })
                .await?;
        }
        Ok(())
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("background WebSocket barrier")
}

async fn application<const MODE: u8>() -> (Arc<WsApp>, Arc<BackgroundProbe>) {
    let mut app = WsAppBuilder::new("127.0.0.1:0")
        .add_background_service::<BackgroundWorker<MODE>>()
        .build()
        .await
        .unwrap();
    app.shutdown_timeout = Duration::from_secs(2);
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    (Arc::new(app), probe)
}

fn start(app: &Arc<WsApp>) -> tokio::task::JoinHandle<Result<(), ServerError>> {
    let app = app.clone();
    tokio::spawn(async move { app.start_with_cancellation(CancellationToken::new()).await })
}

async fn ready(app: &WsApp) {
    bounded(async {
        while !app.health_snapshot().unwrap().ready {
            assert!(app.lifecycle.terminal_result.lock().await.is_none());
            tokio::task::yield_now().await;
        }
    })
    .await;
}

fn report(app: &WsApp) -> reporting::ShutdownReport {
    app.lifecycle.shutdown_report.get().unwrap().clone()
}

async fn assert_closed(app: &WsApp, probe: &BackgroundProbe) {
    let report = report(app);
    assert!(report.evidence.quiescent(), "{report:?}");
    assert!(report.evidence.reconciles(), "{report:?}");
    assert_eq!(report.evidence.background.outstanding, 0);
    assert_eq!(report.evidence.background.scopes.outstanding, 0);
    assert!(
        app.lifecycle
            .root_task
            .get()
            .unwrap()
            .clone()
            .now_or_never()
            .is_some()
    );
    assert_eq!(app.container.active_scope_count(), 0);
    assert_eq!(probe.active.load(Ordering::Acquire), 0);
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn build_close_and_bind_failure_never_execute_prepared_workers() {
    let (app, probe) = application::<0>().await;
    assert_eq!(probe.count("constructed"), 1);
    assert_eq!(probe.count("executing"), 0);
    bounded(app.close()).await.unwrap();
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(report(&app).evidence.background.stopped_before_start, 1);
    assert_closed(&app, &probe).await;

    let (app, probe) = application::<0>().await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    bounded(app.start_with_cancellation(cancellation))
        .await
        .unwrap();
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(report(&app).evidence.background.stopped_before_start, 1);
    assert_closed(&app, &probe).await;

    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = WsAppBuilder::new(&occupied.local_addr().unwrap().to_string())
        .add_background_service::<BackgroundWorker<0>>()
        .build()
        .await
        .unwrap();
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    assert!(bounded(app.start()).await.is_err());
    assert_eq!(probe.count("constructed"), 1);
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(report(&app).evidence.background.stopped_before_start, 1);
    assert_closed(&app, &probe).await;
}

static OWNED_BUILD_PROBE: tokio::sync::OnceCell<Arc<BackgroundProbe>> =
    tokio::sync::OnceCell::const_new();
struct PendingOwnedConstructor;
#[async_trait::async_trait]
impl BackgroundServiceTrait for PendingOwnedConstructor {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move {
                    let scoped = extensions.get_service::<BackgroundScoped>(None).await?;
                    assert!(OWNED_BUILD_PROBE.set(scoped.probe.clone()).is_ok());
                    std::future::pending::<Result<Self, InjectionError>>().await
                })
            })
            .await
    }
    async fn execute_async(&mut self, _: ExecutionCancellation) -> Result<(), Self::Error> {
        panic!("cancelled constructor must never execute")
    }
}

#[tokio::test]
async fn cancelled_owned_build_stops_constructor_before_disposing_its_container() {
    let build = tokio::spawn(async {
        WsAppBuilder::new("127.0.0.1:0")
            .add_background_service::<PendingOwnedConstructor>()
            .build()
            .await
    });
    let probe = bounded(async {
        loop {
            if let Some(probe) = OWNED_BUILD_PROBE.get() {
                break probe.clone();
            }
            assert!(!build.is_finished());
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(probe.count("scope-created"), 1);
    build.abort();
    assert!(matches!(build.await, Err(error) if error.is_cancelled()));
    probe.until("di-disposed", 1).await;
    assert_eq!(probe.count("scope-dropped"), 1);
    assert_eq!(probe.active.load(Ordering::Acquire), 0);
    probe.before("scope-dropped", "di-disposed");
}

#[tokio::test]
async fn scope_disposal_is_a_barrier_before_websocket_dependencies() {
    let (app, probe) = application::<0>().await;
    probe.block_disposal.store(true, Ordering::Release);
    let running = start(&app);
    ready(&app).await;
    probe.until("scope-created", 1).await;
    assert_eq!(probe.count("cancel-observed"), 0);
    let closing = app.clone();
    let close = tokio::spawn(async move { closing.close().await });
    probe.until("disposing", 1).await;
    assert_eq!(probe.count("di-disposed"), 0);
    assert!(!app.dispatcher.close_terminal());
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    assert_eq!(app.background_snapshot().scopes.outstanding, 1);
    probe.dispose_release.cancel();
    bounded(close).await.unwrap().unwrap();
    bounded(running).await.unwrap().unwrap();
    assert_eq!(
        report(&app).completion,
        FrameworkShutdownCompletion::GracefulCompleted
    );
    assert_closed(&app, &probe).await;
    probe.before("cancel-observed", "disposing");
    probe.before("disposed", "scope-dropped");
    probe.before("scope-dropped", "di-disposed");
}

#[tokio::test]
async fn error_and_panic_stop_host_but_one_shot_completion_keeps_it_ready() {
    let (app, probe) = application::<2>().await;
    let running = start(&app);
    probe.until("executing", 1).await;
    probe.release.cancel();
    assert!(bounded(running).await.unwrap().is_err());
    assert_eq!(report(&app).evidence.background.failed, 1);
    assert_eq!(
        report(&app).completion,
        FrameworkShutdownCompletion::Incomplete
    );
    assert!(!app.health_snapshot().unwrap().live);
    assert_closed(&app, &probe).await;

    let (app, probe) = application::<3>().await;
    let running = start(&app);
    probe.until("executing", 1).await;
    probe.release.cancel();
    assert!(bounded(running).await.unwrap().is_err());
    assert_eq!(report(&app).evidence.background.panicked, 1);
    assert_eq!(
        report(&app).completion,
        FrameworkShutdownCompletion::Incomplete
    );
    assert_closed(&app, &probe).await;

    let (app, probe) = application::<4>().await;
    let running = start(&app);
    probe.until("execution-dropped", 1).await;
    ready(&app).await;
    // Poll the durable failure signal after the completed join was recorded.
    bounded(async {
        while app.background_snapshot().joined != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.wait_for_background_failure().now_or_never().is_none());
    assert!(!running.is_finished());
    app.close().await.unwrap();
    bounded(running).await.unwrap().unwrap();
    assert_eq!(report(&app).evidence.background.completed, 1);
    assert_closed(&app, &probe).await;
}

#[tokio::test]
async fn duplicate_workers_share_one_budget_and_cancelled_close_waiters_keep_ownership() {
    let mut app = WsAppBuilder::new("127.0.0.1:0")
        .add_background_service::<BackgroundWorker<1, 0>>()
        .add_background_service::<BackgroundWorker<1, 0>>()
        .add_background_service::<BackgroundWorker<1, 1>>()
        .build()
        .await
        .unwrap();
    app.shutdown_timeout = Duration::from_secs(2);
    let app = Arc::new(app);
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    let running = start(&app);
    probe.until("scope-created", 2).await;
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    let background = app.lifecycle.background.as_ref().unwrap();
    bounded(async {
        while background.deadlines.get().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let limits = *background.deadlines.get().unwrap();
    close.abort();
    assert!(close.await.unwrap_err().is_cancelled());
    app.close().await.unwrap();
    bounded(running).await.unwrap().unwrap();
    let report = report(&app);
    assert_eq!(
        report.completion,
        FrameworkShutdownCompletion::ForcedCompleted,
        "{report:?}"
    );
    assert_eq!(report.evidence.background.registered, 2);
    assert_eq!(report.evidence.background.aborted, 2);
    assert_eq!(report.evidence.background.joined, 2);
    assert!(Instant::now() >= limits.cooperative);
    assert!(Instant::now() < limits.reconcile);
    assert_eq!(
        background.deadlines.get().unwrap().reconcile,
        limits.reconcile
    );
    let contexts = probe
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.0 == "scope-created")
        .map(|event| event.1)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        contexts.len(),
        2,
        "workers must have independent scoped services"
    );
    assert_closed(&app, &probe).await;
}

#[tokio::test]
async fn force_reaches_workers_while_another_shutdown_component_is_waiting() {
    let (app, probe) = application::<1>().await;
    let running = start(&app);
    probe.until("scope-created", 1).await;
    let (release, wait) = tokio::sync::oneshot::channel();
    app.lifecycle
        .connection_tasks
        .track(tokio::spawn(async move {
            let _ = wait.await;
        }));
    let closing = app.clone();
    let close = tokio::spawn(async move { closing.close().await });
    let background = app.lifecycle.background.as_ref().unwrap();
    bounded(async {
        while background.deadlines.get().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let cutoff = background.deadlines.get().unwrap().cooperative;
    app.lifecycle.shutdown_state.request_force();
    probe.until("execution-dropped", 1).await;
    assert!(
        Instant::now() < cutoff,
        "worker abort waited for an unrelated component"
    );
    let _ = release.send(());
    bounded(close).await.unwrap().unwrap();
    bounded(running).await.unwrap().unwrap();
    assert_eq!(report(&app).evidence.background.aborted, 1);
    assert_closed(&app, &probe).await;
}

#[tokio::test]
async fn cancelled_start_and_abandoned_unstarted_app_keep_cleanup_owned() {
    let (app, probe) = application::<0>().await;
    let running = start(&app);
    probe.until("scope-created", 1).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    bounded(app.close()).await.unwrap();
    assert_closed(&app, &probe).await;
    probe.before("scope-dropped", "di-disposed");

    let (app, probe) = application::<0>().await;
    let background = app.lifecycle.background.as_ref().unwrap().clone();
    let container = app.container.clone();
    drop(app);
    probe.until("di-disposed", 1).await;
    bounded(async {
        while background.retained.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(background.is_terminal());
    assert_eq!(background.runtime.snapshot().stopped_before_start, 1);
    assert_eq!(probe.count("executing"), 0);
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &container
    ));
}

#[tokio::test]
async fn constructor_failure_and_cancelled_build_leave_external_container_usable() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let probe = container.resolve::<BackgroundProbe>(None).await.unwrap();
    let result = WsAppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .add_background_service::<BackgroundWorker<5>>()
        .build()
        .await;
    assert!(matches!(result, Err(ServerError::BackgroundService(_))));
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(probe.count("di-disposed"), 0);
    let mut building = Box::pin(
        WsAppBuilder::new("127.0.0.1:0")
            .container(container.clone())
            .add_background_service::<BackgroundWorker<6>>()
            .build(),
    );
    tokio::select! {
        result = &mut building => panic!("constructor must be pending: {:?}", result.err()),
        _ = probe.until("scope-created", 1) => {},
    }
    drop(building);
    probe.until("scope-dropped", 1).await;
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(probe.count("di-disposed"), 0);
    // The very same provider remains usable for a new application.
    let app = WsAppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    app.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 0);
    container.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn only_worker_scopes_close_in_a_shared_external_container() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let probe = container.resolve::<BackgroundProbe>(None).await.unwrap();
    let mut unrelated = container.create_scope(ProcessContext::new()).unwrap();
    unrelated
        .run(async {
            container
                .services()
                .get_service::<BackgroundScoped>(None)
                .await
                .unwrap();
        })
        .await
        .unwrap();
    let app = Arc::new(
        WsAppBuilder::new("127.0.0.1:0")
            .container(container.clone())
            .add_background_service::<BackgroundWorker<0>>()
            .build()
            .await
            .unwrap(),
    );
    let running = start(&app);
    probe.until("scope-created", 2).await;
    app.close().await.unwrap();
    bounded(running).await.unwrap().unwrap();
    assert_eq!(container.active_scope_count(), 1);
    assert_eq!(probe.active.load(Ordering::Acquire), 1);
    assert_eq!(probe.count("di-disposed"), 0);
    assert!(report(&app).evidence.background.is_terminal());
    assert_eq!(report(&app).evidence.container, DependencyState::NotOwned);
    unrelated.close().await.unwrap();
    container.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn cooperative_worker_can_open_a_finalization_scope_before_dependency_shutdown() {
    let (app, probe) = application::<9>().await;
    let running = start(&app);
    probe.until("scope-created", 1).await;
    app.close().await.unwrap();
    bounded(running).await.unwrap().unwrap();
    assert_eq!(probe.count("finalization-created"), 1);
    assert_eq!(probe.count("scope-created"), 2);
    assert_eq!(probe.count("scope-dropped"), 2);
    probe.before("cancel-observed", "finalization-created");
    probe.before("scope-dropped", "di-disposed");
    assert_eq!(report(&app).evidence.background.aborted, 0);
    assert_closed(&app, &probe).await;
}

#[tokio::test]
async fn scope_cleanup_failure_and_timeout_are_not_successful_shutdowns() {
    for timeout_cleanup in [false, true] {
        let (app, probe) = application::<0>().await;
        probe
            .fail_disposal
            .store(!timeout_cleanup, Ordering::Release);
        probe
            .block_disposal
            .store(timeout_cleanup, Ordering::Release);
        let running = start(&app);
        probe.until("scope-created", 1).await;
        assert!(bounded(app.close()).await.is_err());
        assert!(bounded(running).await.unwrap().is_err());
        let report = report(&app);
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::Incomplete,
            "{report:?}"
        );
        assert_eq!(report.evidence.background.scopes.failed, 1);
        assert!(!report.evidence.background.cleanup_succeeded());
        assert_closed(&app, &probe).await;
    }
}

struct ProbeBackplane<const FAIL: bool> {
    probe: Arc<BackgroundProbe>,
    ready_sent: AtomicBool,
}

#[async_trait::async_trait]
impl<const FAIL: bool> WebSocketBackplane for ProbeBackplane<FAIL> {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, crate::WebSocketBackplaneInitError> {
        Ok(Self {
            probe: extensions
                .get_service::<BackgroundProbe>(None)
                .await
                .unwrap(),
            ready_sent: AtomicBool::new(false),
        })
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
        if self.ready_sent.swap(true, Ordering::AcqRel) {
            return std::future::pending().await;
        }
        self.probe.event("backplane-subscribing");
        self.probe.subscription_release.cancelled().await;
        if FAIL {
            return Err(crate::WebSocketBackplaneError::new(
                crate::WebSocketBackplaneErrorKind::Shutdown,
            ));
        }
        self.probe.event("backplane-ready");
        Ok(crate::WebSocketBackplaneEvent::SubscriptionReady)
    }
    async fn close(&self) -> Result<(), crate::WebSocketBackplaneError> {
        assert_eq!(self.probe.active.load(Ordering::Acquire), 0);
        self.probe.event("backplane-closed");
        Ok(())
    }
}

#[tokio::test]
async fn required_backplane_readiness_gates_worker_execution_and_optional_does_not() {
    for requirement in [
        BackplaneRequirement::Required,
        BackplaneRequirement::Optional,
    ] {
        let app = Arc::new(
            WsAppBuilder::new("127.0.0.1:0")
                .backplane::<ProbeBackplane<false>>(requirement)
                .add_background_service::<BackgroundWorker<0>>()
                .build()
                .await
                .unwrap(),
        );
        let probe = app
            .container
            .resolve::<BackgroundProbe>(None)
            .await
            .unwrap();
        let running = start(&app);
        probe.until("backplane-subscribing", 1).await;
        if requirement == BackplaneRequirement::Required {
            assert_eq!(probe.count("executing"), 0);
            assert!(!app.health_snapshot().unwrap().ready);
            probe.subscription_release.cancel();
        }
        probe.until("scope-created", 1).await;
        ready(&app).await;
        if requirement == BackplaneRequirement::Required {
            probe.before("backplane-ready", "executing");
        } else {
            assert_eq!(probe.count("backplane-ready"), 0);
        }
        app.close().await.unwrap();
        bounded(running).await.unwrap().unwrap();
        probe.before("scope-dropped", "backplane-closed");
        probe.before("backplane-closed", "di-disposed");
        assert_closed(&app, &probe).await;
    }
}

#[tokio::test]
async fn failed_or_cancelled_required_backplane_never_executes_worker() {
    for fail in [true, false] {
        let app = Arc::new(
            WsAppBuilder::new("127.0.0.1:0")
                .backplane::<ProbeBackplane<true>>(BackplaneRequirement::Required)
                .add_background_service::<BackgroundWorker<0>>()
                .build()
                .await
                .unwrap(),
        );
        let probe = app
            .container
            .resolve::<BackgroundProbe>(None)
            .await
            .unwrap();
        let running = start(&app);
        probe.until("backplane-subscribing", 1).await;
        assert_eq!(probe.count("executing"), 0);
        if fail {
            probe.subscription_release.cancel();
            assert!(bounded(running).await.unwrap().is_err());
        } else {
            app.close().await.unwrap();
            bounded(running).await.unwrap().unwrap();
        }
        assert_eq!(probe.count("executing"), 0);
        assert_eq!(report(&app).evidence.background.stopped_before_start, 1);
        assert_closed(&app, &probe).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_destructor_retains_dependencies_and_freezes_incomplete_report() {
    let (app, probe) = application::<7>().await;
    let (release, wait) = std::sync::mpsc::channel();
    *probe.drop_gate.lock().unwrap() = Some(wait);
    let emergency = release.clone();
    let (done, done_rx) = std::sync::mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(Duration::from_secs(8)).is_err() {
            let _ = emergency.send(());
            true
        } else {
            false
        }
    });
    let running = start(&app);
    probe.until("executing", 1).await;
    app.lifecycle.close_cancellation.cancel();
    app.lifecycle.shutdown_state.request_force();
    probe.until("drop-blocked", 1).await;
    let closed = bounded(app.close()).await;
    let root = bounded(running).await.unwrap();
    let frozen = report(&app);
    let di_started = lily_injection::__private::container_shutdown_started(&app.container);
    let background = app.lifecycle.background.as_ref().unwrap();
    release.send(()).unwrap();
    done.send(()).unwrap();
    assert!(!watchdog.join().unwrap(), "test emergency watchdog fired");
    assert!(
        background
            .runtime
            .wait_stopped_before(Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(closed.is_err() && root.is_err());
    assert!(!di_started);
    assert_eq!(frozen.completion, FrameworkShutdownCompletion::Incomplete);
    assert_eq!(frozen.evidence.background.outstanding, 1);
    assert!(frozen.evidence.background.execution_deadline_missed);
    assert!(!frozen.evidence.quiescent());
    assert_eq!(probe.count("di-disposed"), 0);
    assert!(app.close().await.is_err());
    assert_eq!(report(&app), frozen);
    assert!(background.retained.lock().unwrap().is_some());
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    // Explicit fixture disposal after the real join; it does not rewrite or
    // renew the application's already-frozen shutdown attempt.
    let _ = app.dispatcher.close_backplane().await;
    app.container.close().await.unwrap();
    background.release();
    probe.before("drop-released", "di-disposed");
}
